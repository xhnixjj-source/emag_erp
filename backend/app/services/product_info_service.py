"""Product information service - reverse lookup from existing tables"""
from typing import Optional, Dict, Any
from urllib.parse import urlparse
from sqlalchemy.orm import Session
from app.models.listing import ListingPool, ProfitCalculation
from app.models.monitor_pool import MonitorPool
from app.models.product import FilterPool
from app.models.keyword import KeywordLink
from app.models.profit_config_models import CommissionConfig


def extract_category_name_from_url(category_url: str) -> Optional[str]:
    """
    从类目URL中提取类目名称
    
    例如：
    - https://www.emag.ro/laptopuri/c → "laptopuri"
    - https://www.emag.ro/telefoane-mobile/c → "telefoane-mobile"
    
    Args:
        category_url: 类目URL
    
    Returns:
        类目名称，如果无法提取则返回 None
    """
    if not category_url:
        return None
    
    try:
        parsed = urlparse(category_url)
        path = parsed.path.strip('/')
        
        # 提取路径中的类目部分（通常是 /xxx/c 格式）
        parts = path.split('/')
        if len(parts) >= 2 and parts[-1] == 'c':
            # 返回倒数第二个部分作为类目名称
            return parts[-2]
        elif len(parts) >= 1:
            # 如果没有 /c 后缀，返回最后一个部分
            return parts[-1]
    except Exception:
        pass
    
    return None


def get_product_info_from_listing(
    listing_pool_id: int,
    db: Session
) -> Dict[str, Any]:
    """
    从 ListingPool 反查产品信息（价格、类目等）
    通过 ListingPool -> MonitorPool -> FilterPool 链路反查
    
    Args:
        listing_pool_id: ListingPool ID
        db: 数据库会话
    
    Returns:
        包含产品信息的字典：
        {
            'frontend_price_ron': float,  # 从 FilterPool.price 反查
            'best_price_ron': float,  # 从 KeywordLink 或其他表反查（如存在）
            'category_name': str,  # 从 FilterPool.category_url 解析
            'category_url': str,  # 从 FilterPool 反查
        }
    """
    result = {}
    
    listing = db.query(ListingPool).filter(
        ListingPool.id == listing_pool_id
    ).first()
    
    if not listing:
        return result
    
    # 通过 monitor_pool_id 反查到 FilterPool
    if listing.monitor_pool_id:
        monitor = db.query(MonitorPool).filter(
            MonitorPool.id == listing.monitor_pool_id
        ).first()
        
        if monitor and monitor.filter_pool_id:
            filter_pool = db.query(FilterPool).filter(
                FilterPool.id == monitor.filter_pool_id
            ).first()
            
            if filter_pool:
                # 反查前端售价
                result['frontend_price_ron'] = filter_pool.price
                result['category_url'] = filter_pool.category_url
                
                # 从 category_url 解析类目名称
                if filter_pool.category_url:
                    result['category_name'] = extract_category_name_from_url(
                        filter_pool.category_url
                    )
    
    # 反查 best_price（从 KeywordLink 表，如果存在）
    keyword_link = db.query(KeywordLink).filter(
        KeywordLink.product_url == listing.product_url
    ).order_by(KeywordLink.crawled_at.desc()).first()
    
    # 注意：如果 KeywordLink 表有 best_price 字段，可以在这里获取
    # 目前 KeywordLink 表可能没有这个字段，所以暂时不处理
    
    return result


def get_commission_from_category(
    category_name: Optional[str],
    db: Session
) -> Optional[float]:
    """
    根据类目名称从配置表获取佣金费率
    
    Args:
        category_name: 类目名称（从 FilterPool.category_url 解析得到）
        db: 数据库会话
    
    Returns:
        佣金费率（百分比格式，如 15.0 表示 15%），如果未找到返回 None
    """
    if not category_name:
        return None
    
    # 从 CommissionConfig 表查询
    commission_config = db.query(CommissionConfig).filter(
        CommissionConfig.category_or_group == category_name,
        CommissionConfig.effective_to.is_(None)  # 只查询当前生效的
    ).order_by(CommissionConfig.effective_from.desc()).first()
    
    if commission_config:
        # 转换为百分比格式（配置表中存储的是小数格式，如 0.15）
        return float(commission_config.commission_rate * 100)
    
    return None


def populate_profit_calculation_from_listing(
    calc: ProfitCalculation,
    listing: ListingPool,
    db: Session,
    force_update: bool = False
) -> None:
    """
    从 ListingPool 反查并填充 ProfitCalculation 的缺失字段
    
    Args:
        calc: ProfitCalculation 对象
        listing: ListingPool 对象
        db: 数据库会话
        force_update: 是否强制更新已有字段
    """
    from datetime import datetime
    
    # 反查产品信息
    product_info = get_product_info_from_listing(listing.id, db)
    
    # 填充前端售价
    if (force_update or not calc.frontend_price_ron) and product_info.get('frontend_price_ron'):
        calc.frontend_price_ron = product_info['frontend_price_ron']
        calc.price_source = 'crawler'
        calc.price_last_updated_at = datetime.utcnow()
    
    # 填充 best_price
    if (force_update or not calc.best_price_ron) and product_info.get('best_price_ron'):
        calc.best_price_ron = product_info['best_price_ron']
    
    # 填充类目名称
    if (force_update or not calc.category_name) and product_info.get('category_name'):
        calc.category_name = product_info['category_name']
    
    # 如果类目名称存在但佣金为空，尝试自动匹配佣金
    if calc.category_name and (force_update or not calc.platform_commission):
        auto_commission = get_commission_from_category(calc.category_name, db)
        if auto_commission:
            calc.platform_commission = auto_commission
            calc.commission_source = 'default'
            calc.commission_last_updated_at = datetime.utcnow()


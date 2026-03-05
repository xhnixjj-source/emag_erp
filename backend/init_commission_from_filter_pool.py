"""
从链接初筛（KeywordLink）中提取类目和佣金率信息，去重后初始化到佣金配置表
"""
import os
from sqlalchemy import create_engine, distinct, func
from sqlalchemy.orm import sessionmaker
from app.config import config
from app.database import SessionLocal
from app.models.keyword import KeywordLink
from app.models.profit_config_models import CommissionConfig
from datetime import datetime
from collections import defaultdict


def init_commission_from_keyword_links():
    """从KeywordLink中提取类目和佣金率信息并初始化到CommissionConfig表"""
    
    with SessionLocal() as db:
        print("开始从链接初筛（KeywordLink）提取类目和佣金率信息...")
        
        # 1. 查询所有有category和commission_rate的记录
        keyword_links = db.query(KeywordLink).filter(
            KeywordLink.category.isnot(None),
            KeywordLink.category != '',
            KeywordLink.commission_rate.isnot(None)
        ).all()
        
        print(f"找到 {len(keyword_links)} 条有类目和佣金率的记录")
        
        # 2. 按类目分组，计算每个类目的平均佣金率
        category_commissions = defaultdict(list)
        for kl in keyword_links:
            if kl.category and kl.commission_rate is not None:
                # commission_rate 在 KeywordLink 中是百分比（如 15 表示 15%）
                # 需要转换为小数（0.15）用于 CommissionConfig
                category_commissions[kl.category].append(kl.commission_rate)
        
        print(f"提取到 {len(category_commissions)} 个唯一类目")
        
        # 3. 计算每个类目的平均佣金率（转换为小数格式）
        category_avg_commissions = {}
        for category, rates in category_commissions.items():
            avg_rate = sum(rates) / len(rates)
            # 转换为小数格式（15% -> 0.15）
            category_avg_commissions[category] = avg_rate / 100.0
            print(f"  - 类目: {category}, 平均佣金率: {avg_rate}% (转换为小数: {category_avg_commissions[category]})")
        
        # 4. 检查哪些类目已经在CommissionConfig中存在
        existing_categories = {}
        existing_configs = db.query(CommissionConfig).filter(
            CommissionConfig.site == 'emag_ro',
            CommissionConfig.effective_to.is_(None)  # 只检查当前生效的
        ).all()
        
        for config in existing_configs:
            existing_categories[config.category_or_group] = config
        
        print(f"已有 {len(existing_categories)} 个类目在佣金配置表中")
        
        # 5. 更新或创建佣金配置
        updated_count = 0
        added_count = 0
        
        for category_name, avg_commission_rate in category_avg_commissions.items():
            try:
                if category_name in existing_categories:
                    # 更新现有配置
                    existing_config = existing_categories[category_name]
                    old_rate = existing_config.commission_rate
                    existing_config.commission_rate = avg_commission_rate
                    updated_count += 1
                    print(f"  - 更新类目: {category_name} (佣金率: {old_rate*100}% -> {avg_commission_rate*100}%)")
                else:
                    # 创建新配置
                    commission_config = CommissionConfig(
                        site='emag_ro',
                        category_or_group=category_name,
                        commission_rate=avg_commission_rate,
                        effective_from=datetime.utcnow()
                    )
                    db.add(commission_config)
                    added_count += 1
                    print(f"  - 添加类目: {category_name} (佣金率: {avg_commission_rate*100}%)")
            except Exception as e:
                print(f"  - 处理类目失败 {category_name}: {e}")
                continue
        
        # 6. 提交事务
        try:
            db.commit()
            print(f"\n成功初始化佣金配置:")
            print(f"  - 新增: {added_count} 个类目")
            print(f"  - 更新: {updated_count} 个类目")
            print(f"  - 总计: {added_count + updated_count} 个类目")
        except Exception as e:
            db.rollback()
            print(f"\n初始化失败: {e}")
            raise


if __name__ == "__main__":
    init_commission_from_keyword_links()


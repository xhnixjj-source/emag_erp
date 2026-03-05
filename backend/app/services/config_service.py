"""Configuration service for profit calculation"""
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from app.models.profit_config_models import (
    VatConfig,
    ExchangeRate,
    LogisticsPrice,
    GeniusRule,
    GeniusRuleStep,
    PackagingTemplate
)


def get_current_vat(site: str = "emag_ro", db: Session = None) -> Decimal:
    """
    获取当前生效的VAT率
    
    Args:
        site: 站点标识
        db: 数据库会话
    
    Returns:
        VAT率（小数格式，如 0.21 表示 21%），如果未找到返回默认值 0.21
    """
    if not db:
        return Decimal("0.21")
    
    config = db.query(VatConfig).filter(
        VatConfig.site == site,
        VatConfig.effective_to.is_(None)
    ).order_by(VatConfig.effective_from.desc()).first()
    
    return Decimal(str(config.vat_rate)) if config else Decimal("0.21")


def get_current_exchange_rate(
    from_currency: str = "RON",
    to_currency: str = "CNY",
    db: Session = None
) -> Decimal:
    """
    获取当前生效的汇率
    
    Args:
        from_currency: 源货币（默认 RON）
        to_currency: 目标货币（默认 CNY）
        db: 数据库会话
    
    Returns:
        汇率（1 RON = rate CNY），如果未找到返回默认值 1.6
    """
    if not db:
        return Decimal("1.6")
    
    rate = db.query(ExchangeRate).filter(
        ExchangeRate.from_currency == from_currency,
        ExchangeRate.to_currency == to_currency,
        ExchangeRate.effective_to.is_(None)
    ).order_by(ExchangeRate.effective_from.desc()).first()
    
    return Decimal(str(rate.rate)) if rate else Decimal("1.6")


def get_logistics_price(transport_mode: str, db: Session = None) -> Decimal:
    """
    获取物流单价（人民币/公斤）
    
    Args:
        transport_mode: 运输方式（'air' 或 'land'）
        db: 数据库会话
    
    Returns:
        物流单价（人民币/公斤），如果未找到返回 0
    """
    if not db:
        return Decimal("0")
    
    price = db.query(LogisticsPrice).filter(
        LogisticsPrice.transport_mode == transport_mode,
        LogisticsPrice.effective_to.is_(None)
    ).order_by(LogisticsPrice.effective_from.desc()).first()
    
    return Decimal(str(price.price_per_kg_rmb)) if price else Decimal("0")


def get_active_genius_rule(db: Session = None):
    """
    获取当前激活的genius规则
    
    Args:
        db: 数据库会话
    
    Returns:
        GeniusRuleDomain 对象，如果未找到返回 None
    """
    if not db:
        return None
    
    rule = db.query(GeniusRule).filter(
        GeniusRule.is_active == True
    ).first()
    
    if not rule:
        return None
    
    steps = db.query(GeniusRuleStep).filter(
        GeniusRuleStep.rule_id == rule.id
    ).order_by(GeniusRuleStep.min_sales_amount).all()
    
    # 返回规则和步骤列表，由调用方创建 GeniusRuleDomain
    return {
        'rule': rule,
        'steps': steps
    }


def get_packaging_cost(
    packaging_template_id: Optional[int],
    db: Session = None
) -> Decimal:
    """
    获取包材成本
    
    Args:
        packaging_template_id: 包材模板ID
        db: 数据库会话
    
    Returns:
        包材成本（人民币），如果未找到返回默认值 0.2
    """
    if not db:
        return Decimal("0.2")
    
    if packaging_template_id:
        template = db.query(PackagingTemplate).filter(
            PackagingTemplate.id == packaging_template_id
        ).first()
        if template:
            return Decimal(str(template.cost_rmb))
    
    # 如果没有指定模板，查找默认模板
    default_template = db.query(PackagingTemplate).filter(
        PackagingTemplate.is_default == True
    ).first()
    
    if default_template:
        return Decimal(str(default_template.cost_rmb))
    
    # 如果都没有，返回默认值 0.2 RMB
    return Decimal("0.2")


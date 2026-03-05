"""Profit configuration models for enhanced profit calculation"""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class LogisticsPrice(Base):
    """物流单价表"""
    __tablename__ = "logistics_price"
    
    id = Column(Integer, primary_key=True, index=True)
    transport_mode = Column(String(16), nullable=False)  # 'air' / 'land'
    price_per_kg_rmb = Column(Float, nullable=False)  # 人民币/公斤
    effective_from = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    effective_to = Column(DateTime(timezone=True), nullable=True)
    remark = Column(String, nullable=True)


class VatConfig(Base):
    """VAT 配置表"""
    __tablename__ = "vat_config"
    
    id = Column(Integer, primary_key=True, index=True)
    site = Column(String(32), nullable=False, default="emag_ro", index=True)
    vat_rate = Column(Float, nullable=False)  # 小数格式，如 0.21 表示 21%
    effective_from = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    effective_to = Column(DateTime(timezone=True), nullable=True)


class ExchangeRate(Base):
    """汇率配置表"""
    __tablename__ = "exchange_rate"
    
    id = Column(Integer, primary_key=True, index=True)
    from_currency = Column(String(8), nullable=False, default="RON")
    to_currency = Column(String(8), nullable=False, default="CNY")
    rate = Column(Float, nullable=False)  # 1 RON = rate CNY（如 1.6）
    source = Column(String(32), nullable=False, default="manual")
    effective_from = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    effective_to = Column(DateTime(timezone=True), nullable=True)


class GeniusRule(Base):
    """Genius 费用规则主表"""
    __tablename__ = "genius_rule"
    
    id = Column(Integer, primary_key=True, index=True)
    rule_name = Column(String(64), nullable=False)
    currency = Column(String(8), nullable=False, default="RON")
    is_active = Column(Boolean, nullable=False, default=True)
    
    # Relationships
    steps = relationship("GeniusRuleStep", back_populates="rule", cascade="all, delete-orphan")


class GeniusRuleStep(Base):
    """Genius 费用规则阶梯明细表"""
    __tablename__ = "genius_rule_step"
    
    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer, ForeignKey("genius_rule.id", ondelete="CASCADE"), nullable=False)
    min_sales_amount = Column(Float, nullable=False)  # 最小销售额（含）
    max_sales_amount = Column(Float, nullable=True)  # 最大销售额（不含，NULL表示无上限）
    fee_amount = Column(Float, nullable=False)  # 费用（列伊）
    
    # Relationships
    rule = relationship("GeniusRule", back_populates="steps")


class PackagingTemplate(Base):
    """包材配置表"""
    __tablename__ = "packaging_template"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(64), nullable=False)
    cost_rmb = Column(Float, nullable=False)  # 人民币
    apply_scope = Column(String(64), nullable=True)
    is_default = Column(Boolean, nullable=False, default=False)


class CommissionConfig(Base):
    """平台佣金配置表（按类目）"""
    __tablename__ = "commission_config"
    
    id = Column(Integer, primary_key=True, index=True)
    site = Column(String(32), nullable=False, default="emag_ro", index=True)
    category_or_group = Column(String(128), nullable=False)  # 类目名称或佣金组
    commission_rate = Column(Float, nullable=False)  # 小数格式，如 0.15 表示 15%
    effective_from = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    effective_to = Column(DateTime(timezone=True), nullable=True)


class FeeTemplate(Base):
    """费用模板表（订单处理费、仓储费）"""
    __tablename__ = "fee_template"
    
    id = Column(Integer, primary_key=True, index=True)
    template_name = Column(String(64), nullable=False)
    fee_type = Column(String(32), nullable=False)  # 'order_handling' / 'storage'
    currency = Column(String(8), nullable=False)  # 'CNY' / 'RON'
    calculation_method = Column(String(32), nullable=False)  # 'per_order', 'per_item', 'per_weight', 'fixed'
    base_amount = Column(Float, nullable=False, default=0.0)
    rate = Column(Float, nullable=False, default=0.0)  # 比例费率（如按金额的百分比）
    unit = Column(String(16), nullable=True)  # 单位（如 'kg', 'm3', 'day'）


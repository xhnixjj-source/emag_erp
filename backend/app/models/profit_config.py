"""Profit configuration model"""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class ProfitConfig(Base):
    """Profit configuration model for storing default fee settings"""
    __tablename__ = "profit_config"
    
    id = Column(Integer, primary_key=True, index=True)
    site = Column(String, nullable=False, default="emag_ro", index=True)  # 站点标识，为多站点预留
    default_shipping_cost = Column(Float, nullable=False, default=0.0)  # 头程物流基础费用/每单
    default_order_fee = Column(Float, nullable=False, default=0.0)  # 订单处理费
    default_storage_fee = Column(Float, nullable=False, default=0.0)  # 仓储费
    default_platform_commission = Column(Float, nullable=False, default=0.0)  # 平台佣金率(%)
    default_vat_rate = Column(Float, nullable=False, default=0.0)  # VAT率(%)
    shipping_price_per_kg = Column(Float, nullable=True)  # 按重量计费时的单价(€/kg)，可选
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    updated_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    
    # Relationships
    updated_by = relationship("User", foreign_keys=[updated_by_user_id])


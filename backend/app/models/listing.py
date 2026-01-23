"""Listing models"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Boolean, JSON, Float
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum
from app.database import Base


class ListingStatus(str, enum.Enum):
    """Listing status enum"""
    PENDING_CALC = "pending_calc"  # 待测算
    APPROVED = "approved"  # 已通过
    LISTED = "listed"  # 已上架
    PURCHASED = "purchased"  # 已采购
    REJECTED = "rejected"  # 已放弃


class ListingPool(Base):
    """Listing pool model"""
    __tablename__ = "listing_pool"
    
    id = Column(Integer, primary_key=True, index=True)
    monitor_pool_id = Column(Integer, ForeignKey("monitor_pool.id"), nullable=True)
    product_url = Column(String, nullable=False, index=True)
    status = Column(Enum(ListingStatus), default=ListingStatus.PENDING_CALC, nullable=False)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    is_locked = Column(Boolean, default=False, nullable=False)
    locked_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    user = relationship("User", foreign_keys=[created_by_user_id], backref="listing_pools")
    locked_by = relationship("User", foreign_keys=[locked_by_user_id])
    profit_calc = relationship("ProfitCalculation", back_populates="listing_pool", uselist=False, cascade="all, delete-orphan")
    listing_details = relationship("ListingDetails", back_populates="listing_pool", uselist=False, cascade="all, delete-orphan")


class ProfitCalculation(Base):
    """Profit calculation model"""
    __tablename__ = "profit_calculation"
    
    id = Column(Integer, primary_key=True, index=True)
    listing_pool_id = Column(Integer, ForeignKey("listing_pool.id"), unique=True, nullable=False)
    purchase_price = Column(Float, nullable=True)
    length = Column(Float, nullable=True)
    width = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    shipping_cost = Column(Float, nullable=True)
    order_fee = Column(Float, nullable=True)
    storage_fee = Column(Float, nullable=True)
    platform_commission = Column(Float, nullable=True)
    vat = Column(Float, nullable=True)
    profit_margin = Column(Float, nullable=True)
    profit_amount = Column(Float, nullable=True)
    chinese_name = Column(String, nullable=True)  # 中文名
    model_number = Column(String, nullable=True)  # 型号
    category_name = Column(String, nullable=True)  # 类目名称
    calculated_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    listing_pool = relationship("ListingPool", back_populates="profit_calc")


class ListingDetails(Base):
    """Listing details model"""
    __tablename__ = "listing_details"
    
    id = Column(Integer, primary_key=True, index=True)
    listing_pool_id = Column(Integer, ForeignKey("listing_pool.id"), unique=True, nullable=False)
    image_urls = Column(JSON, nullable=True)
    competitor_urls = Column(JSON, nullable=True)
    listing_html = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    listing_pool = relationship("ListingPool", back_populates="listing_details")


"""Product models"""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class FilterPool(Base):
    """Filter pool model"""
    __tablename__ = "filter_pool"
    
    id = Column(Integer, primary_key=True, index=True)
    product_url = Column(String, unique=True, nullable=False, index=True)
    product_name = Column(String, nullable=True)
    thumbnail_image = Column(String, nullable=True)  # 产品缩略图URL
    brand = Column(String, nullable=True)  # 品牌
    shop_name = Column(String, nullable=True)  # 店铺名称
    price = Column(Float, nullable=True)
    listed_at = Column(DateTime(timezone=True), nullable=True)
    stock = Column(Integer, nullable=True)
    review_count = Column(Integer, nullable=True)
    latest_review_at = Column(DateTime(timezone=True), nullable=True)
    earliest_review_at = Column(DateTime(timezone=True), nullable=True)
    shop_rank = Column(Integer, nullable=True)
    category_rank = Column(Integer, nullable=True)
    ad_rank = Column(Integer, nullable=True)
    is_fbe = Column(Boolean, nullable=True, default=False)  # 是否是FBE（Fulfilled by eMAG）
    competitor_count = Column(Integer, nullable=True, default=0)  # 跟卖数
    crawled_at = Column(DateTime(timezone=True), server_default=func.now())


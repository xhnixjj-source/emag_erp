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
    # 店铺介绍页完整URL（/xxx/v?ref=see_vendor_page）
    shop_intro_url = Column(String, nullable=True)
    # 店铺商品列表页完整URL（用于后续复用店铺排名/访问店铺页）
    shop_url = Column(String, nullable=True)
    # 产品主类目页完整URL（用于后续复用类目排名/访问类目页）
    category_url = Column(String, nullable=True)
    price = Column(Float, nullable=True)
    # 评分（1-5分）
    rating = Column(Float, nullable=True)
    # 上架日期（如果能从 Istoric Preturi 获取到）
    listed_at = Column(DateTime(timezone=True), nullable=True)
    # 上架日期获取状态：pending / success / not_found / error
    listed_at_status = Column(String, nullable=True, default="pending")
    # 上架日期获取错误类型：timeout / host_disconnected / http_error / parse_failed / unknown 等，仅在 status=error 时有意义
    listed_at_error_type = Column(String, nullable=True)
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


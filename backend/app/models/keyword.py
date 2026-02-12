"""Keyword models"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Float
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum
from app.database import Base


class KeywordStatus(str, enum.Enum):
    """Keyword status enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class Keyword(Base):
    """Keyword model"""
    __tablename__ = "keywords"
    
    id = Column(Integer, primary_key=True, index=True)
    keyword = Column(String, nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    status = Column(Enum(KeywordStatus), default=KeywordStatus.PENDING, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    user = relationship("User", backref="keywords")
    links = relationship("KeywordLink", back_populates="keyword", cascade="all, delete-orphan")


class KeywordLink(Base):
    """Keyword link model"""
    __tablename__ = "keyword_links"
    
    id = Column(Integer, primary_key=True, index=True)
    keyword_id = Column(Integer, ForeignKey("keywords.id"), nullable=False)
    product_url = Column(String, nullable=False, index=True)
    pnk_code = Column("PNK_CODE", String, nullable=True)  # PNK_CODE（产品编码）
    thumbnail_image = Column(String, nullable=True)  # 产品缩略图URL
    price = Column(Float, nullable=True)  # 售价
    review_count = Column(Integer, nullable=True)  # 评论数
    rating = Column(Float, nullable=True)  # 评分
    crawled_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String, default="active", nullable=False)
    
    # Chrome 插件扩展字段
    product_title = Column(String, nullable=True)  # 产品标题
    brand = Column(String, nullable=True)  # 品牌
    category = Column(String, nullable=True)  # 类目
    commission_rate = Column(Float, nullable=True)  # 佣金比例(%)
    offer_count = Column(Integer, nullable=True)  # 跟卖数
    purchase_price = Column(Float, nullable=True)  # 采购价
    last_offer_period = Column(String, nullable=True)  # 最近offer周期
    tag = Column(String, nullable=True)  # 标签（如 Super Hot）
    source = Column(String, default="keyword_search", nullable=False)  # 来源: keyword_search / chrome_extension
    
    # Relationships
    keyword = relationship("Keyword", back_populates="links")


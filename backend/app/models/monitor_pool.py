"""Monitor pool models"""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Enum
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum
from app.database import Base


class MonitorStatus(str, enum.Enum):
    """Monitor status enum"""
    ACTIVE = "active"
    INACTIVE = "inactive"


class MonitorPool(Base):
    """Monitor pool model"""
    __tablename__ = "monitor_pool"
    
    id = Column(Integer, primary_key=True, index=True)
    filter_pool_id = Column(Integer, ForeignKey("filter_pool.id"), nullable=True)
    product_url = Column(String, nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    last_monitored_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(Enum(MonitorStatus), default=MonitorStatus.ACTIVE, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    user = relationship("User", backref="monitor_pools")
    history = relationship("MonitorHistory", back_populates="monitor_pool", cascade="all, delete-orphan")


class MonitorHistory(Base):
    """Monitor history model"""
    __tablename__ = "monitor_history"
    
    id = Column(Integer, primary_key=True, index=True)
    monitor_pool_id = Column(Integer, ForeignKey("monitor_pool.id"), nullable=False)
    price = Column(Float, nullable=True)
    stock = Column(Integer, nullable=True)
    review_count = Column(Integer, nullable=True)
    shop_rank = Column(Integer, nullable=True)
    category_rank = Column(Integer, nullable=True)
    ad_rank = Column(Integer, nullable=True)
    monitored_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    monitor_pool = relationship("MonitorPool", back_populates="history")


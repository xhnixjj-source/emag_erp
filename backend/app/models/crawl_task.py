"""Crawl task models"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Text, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum
from app.database import Base


class TaskType(str, enum.Enum):
    """Task type enum"""
    KEYWORD_SEARCH = "keyword_search"
    PRODUCT_CRAWL = "product_crawl"
    MONITOR_CRAWL = "monitor_crawl"


class TaskStatus(str, enum.Enum):
    """Task status enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"


class TaskPriority(str, enum.Enum):
    """Task priority enum"""
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class ErrorType(str, enum.Enum):
    """Error type enum"""
    CAPTCHA = "captcha"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    DISCONNECT = "disconnect"
    OTHER = "other"


class CrawlTask(Base):
    """Crawl task model"""
    __tablename__ = "crawl_tasks"
    
    id = Column(Integer, primary_key=True, index=True)
    task_type = Column(Enum(TaskType), nullable=False, index=True)
    keyword_id = Column(Integer, ForeignKey("keywords.id"), nullable=True)
    product_url = Column(String, nullable=True)
    status = Column(Enum(TaskStatus), default=TaskStatus.PENDING, nullable=False, index=True)
    priority = Column(Enum(TaskPriority), default=TaskPriority.NORMAL, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    retry_count = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=5, nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    queue_position = Column(Integer, nullable=True)
    progress = Column(Integer, default=0, nullable=False)  # 0-100
    
    # Relationships
    user = relationship("User", backref="crawl_tasks")
    keyword = relationship("Keyword", backref="tasks")
    error_logs = relationship("ErrorLog", back_populates="task", cascade="all, delete-orphan")


class ErrorLog(Base):
    """Error log model"""
    __tablename__ = "error_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("crawl_tasks.id"), nullable=True)
    error_type = Column(String, nullable=False, index=True)  # captcha/timeout/connection/disconnect/other
    error_message = Column(Text, nullable=True)
    error_detail = Column(JSON, nullable=True)
    occurred_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolution_action = Column(String, nullable=True)
    
    # Relationships
    task = relationship("CrawlTask", back_populates="error_logs")


"""Operation log model"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class OperationLog(Base):
    """Operation log model"""
    __tablename__ = "operation_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    operation_type = Column(String, nullable=False, index=True)
    target_type = Column(String, nullable=True, index=True)
    target_id = Column(Integer, nullable=True, index=True)
    operation_detail = Column(JSON, nullable=True)
    ip_address = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    
    # Relationships
    user = relationship("User", backref="operation_logs")


"""Operation log service"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from app.models.operation_log import OperationLog


def create_operation_log(
    db: Session,
    user_id: int,
    operation_type: str,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    operation_detail: Optional[Dict[str, Any]] = None,
    ip_address: Optional[str] = None
) -> OperationLog:
    """Create operation log"""
    log = OperationLog(
        user_id=user_id,
        operation_type=operation_type,
        target_type=target_type,
        target_id=target_id,
        operation_detail=operation_detail,
        ip_address=ip_address
    )
    db.add(log)
    db.commit()
    db.refresh(log)
    return log


def get_operation_logs(
    db: Session,
    user_id: Optional[int] = None,
    operation_type: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    skip: int = 0,
    limit: int = 100
) -> List[OperationLog]:
    """Get operation logs with filters"""
    query = db.query(OperationLog)
    
    if user_id:
        query = query.filter(OperationLog.user_id == user_id)
    if operation_type:
        query = query.filter(OperationLog.operation_type == operation_type)
    if target_type:
        query = query.filter(OperationLog.target_type == target_type)
    if target_id:
        query = query.filter(OperationLog.target_id == target_id)
    if start_date:
        query = query.filter(OperationLog.created_at >= start_date)
    if end_date:
        query = query.filter(OperationLog.created_at <= end_date)
    
    return query.order_by(OperationLog.created_at.desc()).offset(skip).limit(limit).all()


def get_operation_log_count(
    db: Session,
    user_id: Optional[int] = None,
    operation_type: Optional[str] = None,
    target_type: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> int:
    """Get operation log count"""
    query = db.query(OperationLog)
    
    if user_id:
        query = query.filter(OperationLog.user_id == user_id)
    if operation_type:
        query = query.filter(OperationLog.operation_type == operation_type)
    if target_type:
        query = query.filter(OperationLog.target_type == target_type)
    if start_date:
        query = query.filter(OperationLog.created_at >= start_date)
    if end_date:
        query = query.filter(OperationLog.created_at <= end_date)
    
    return query.count()


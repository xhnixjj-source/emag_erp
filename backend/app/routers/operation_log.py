"""Operation log query API"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Response
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
import csv
import io
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.operation_log import OperationLog
from app.models.user import User
from app.services.operation_log_service import get_operation_logs, get_operation_log_count
from app.services.permission import is_admin

router = APIRouter(prefix="/api/operation-logs", tags=["operation-logs"])


class OperationLogResponse(BaseModel):
    """Operation log response model"""
    id: int
    user_id: int
    username: Optional[str] = None
    operation_type: str
    target_type: Optional[str]
    target_id: Optional[int]
    operation_detail: Optional[dict]
    ip_address: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class OperationLogListResponse(BaseModel):
    """Operation log list response model"""
    logs: List[OperationLogResponse]
    total: int
    skip: int
    limit: int


@router.get("", response_model=OperationLogListResponse)
async def get_operation_logs_api(
    user_id: Optional[int] = None,
    operation_type: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Get operation logs with filters"""
    # Non-admin users can only see their own logs
    if not is_admin(db, current_user["id"]):
        user_id = current_user["id"]
    
    # Parse dates
    start_datetime = None
    end_datetime = None
    if start_date:
        try:
            start_datetime = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use ISO format."
            )
    if end_date:
        try:
            end_datetime = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use ISO format."
            )
    
    logs = get_operation_logs(
        db=db,
        user_id=user_id,
        operation_type=operation_type,
        target_type=target_type,
        target_id=target_id,
        start_date=start_datetime,
        end_date=end_datetime,
        skip=skip,
        limit=limit
    )
    
    total = get_operation_log_count(
        db=db,
        user_id=user_id,
        operation_type=operation_type,
        target_type=target_type,
        start_date=start_datetime,
        end_date=end_datetime
    )
    
    # Load user information for each log
    log_responses = []
    for log in logs:
        user = db.query(User).filter(User.id == log.user_id).first()
        log_dict = {
            "id": log.id,
            "user_id": log.user_id,
            "username": user.username if user else None,
            "operation_type": log.operation_type,
            "target_type": log.target_type,
            "target_id": log.target_id,
            "operation_detail": log.operation_detail,
            "ip_address": log.ip_address,
            "created_at": log.created_at
        }
        log_responses.append(OperationLogResponse(**log_dict))
    
    return {
        "logs": log_responses,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@router.get("/stats", response_model=dict)
async def get_operation_log_stats(
    user_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get operation log statistics"""
    # Non-admin users can only see their own stats
    if not is_admin(db, current_user["id"]):
        user_id = current_user["id"]
    
    # Parse dates
    start_datetime = None
    end_datetime = None
    if start_date:
        try:
            start_datetime = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use ISO format."
            )
    if end_date:
        try:
            end_datetime = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use ISO format."
            )
    
    # Get logs for stats
    logs = get_operation_logs(
        db=db,
        user_id=user_id,
        start_date=start_datetime,
        end_date=end_datetime,
        skip=0,
        limit=10000  # Get all for stats
    )
    
    # Calculate statistics
    stats = {
        "total_operations": len(logs),
        "by_operation_type": {},
        "by_target_type": {},
        "by_user": {}
    }
    
    for log in logs:
        # Count by operation type
        op_type = log.operation_type
        stats["by_operation_type"][op_type] = stats["by_operation_type"].get(op_type, 0) + 1
        
        # Count by target type
        if log.target_type:
            target_type = log.target_type
            stats["by_target_type"][target_type] = stats["by_target_type"].get(target_type, 0) + 1
        
        # Count by user
        user_id_str = str(log.user_id)
        stats["by_user"][user_id_str] = stats["by_user"].get(user_id_str, 0) + 1
    
    return stats


@router.get("/export")
async def export_operation_logs(
    user_id: Optional[int] = None,
    operation_type: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Export operation logs to CSV"""
    # Non-admin users can only export their own logs
    if not is_admin(db, current_user["id"]):
        user_id = current_user["id"]
    
    # Parse dates
    start_datetime = None
    end_datetime = None
    if start_date:
        try:
            start_datetime = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use ISO format."
            )
    if end_date:
        try:
            end_datetime = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use ISO format."
            )
    
    # Get all logs (no pagination for export)
    logs = get_operation_logs(
        db=db,
        user_id=user_id,
        operation_type=operation_type,
        target_type=target_type,
        target_id=target_id,
        start_date=start_datetime,
        end_date=end_datetime,
        skip=0,
        limit=100000  # Large limit for export
    )
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow([
        "ID", "用户ID", "用户名", "操作类型", "目标类型", "目标ID", 
        "操作详情", "IP地址", "操作时间"
    ])
    
    # Write data
    for log in logs:
        user = db.query(User).filter(User.id == log.user_id).first()
        username = user.username if user else ""
        operation_detail_str = ""
        if log.operation_detail:
            import json
            operation_detail_str = json.dumps(log.operation_detail, ensure_ascii=False)
        
        writer.writerow([
            log.id,
            log.user_id,
            username,
            log.operation_type,
            log.target_type or "",
            log.target_id or "",
            operation_detail_str,
            log.ip_address or "",
            log.created_at.isoformat() if log.created_at else ""
        ])
    
    # Convert to bytes with UTF-8 BOM for Excel compatibility
    csv_content = output.getvalue()
    csv_bytes = csv_content.encode('utf-8-sig')
    
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=operation_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        }
    )

"""Failed tasks management API - list and batch retry"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel
from datetime import datetime
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.crawl_task import CrawlTask, TaskType, TaskStatus, TaskPriority, ErrorLog
from app.models.keyword import Keyword
from app.services.task_manager import task_manager
from app.services.operation_log_service import create_operation_log

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/failed-tasks", tags=["failed-tasks"])


class FailedTaskResponse(BaseModel):
    """Failed task response model"""
    id: int
    task_type: str
    status: str
    priority: str
    progress: int
    retry_count: int
    max_retries: int
    product_url: Optional[str] = None
    keyword_id: Optional[int] = None
    keyword_name: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class FailedTaskListResponse(BaseModel):
    """Failed task list response model"""
    items: List[FailedTaskResponse]
    total: int
    skip: int
    limit: int


class BatchRetryRequest(BaseModel):
    """Batch retry request model"""
    task_ids: List[int]


class BatchRetryResponse(BaseModel):
    """Batch retry response model"""
    success_count: int
    fail_count: int
    total: int
    message: str
    failed_task_ids: List[int] = []


@router.get("", response_model=FailedTaskListResponse)
async def list_failed_tasks(
    task_type: Optional[str] = None,
    error_keyword: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 50
):
    """
    List failed tasks for the current user.
    Supports filtering by task_type and searching error_message.
    """
    query = db.query(CrawlTask).filter(
        CrawlTask.user_id == current_user["id"],
        CrawlTask.status == TaskStatus.FAILED
    )

    # Filter by task type
    if task_type:
        try:
            task_type_enum = TaskType(task_type)
            query = query.filter(CrawlTask.task_type == task_type_enum)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid task_type: {task_type}"
            )

    # Search by error message keyword
    if error_keyword:
        query = query.filter(CrawlTask.error_message.ilike(f"%{error_keyword}%"))

    # Get total count
    total = query.count()

    # Get paginated results, newest first
    tasks = query.order_by(CrawlTask.updated_at.desc()).offset(skip).limit(limit).all()

    # Build keyword name cache
    keyword_ids = [t.keyword_id for t in tasks if t.keyword_id]
    keyword_map = {}
    if keyword_ids:
        keywords = db.query(Keyword).filter(Keyword.id.in_(keyword_ids)).all()
        keyword_map = {k.id: k.keyword for k in keywords}

    # Serialize
    items = []
    for task in tasks:
        items.append(FailedTaskResponse(
            id=task.id,
            task_type=task.task_type.value if hasattr(task.task_type, 'value') else str(task.task_type),
            status=task.status.value if hasattr(task.status, 'value') else str(task.status),
            priority=task.priority.value if hasattr(task.priority, 'value') else str(task.priority),
            progress=task.progress,
            retry_count=task.retry_count,
            max_retries=task.max_retries,
            product_url=task.product_url,
            keyword_id=task.keyword_id,
            keyword_name=keyword_map.get(task.keyword_id),
            error_message=task.error_message,
            created_at=task.created_at,
            updated_at=task.updated_at,
            completed_at=task.completed_at,
        ))

    return FailedTaskListResponse(
        items=items,
        total=total,
        skip=skip,
        limit=limit
    )


@router.post("/batch-retry", response_model=BatchRetryResponse)
async def batch_retry_failed_tasks(
    request: BatchRetryRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """
    Batch retry selected failed tasks.
    Resets retry_count to 0 and re-queues the tasks.
    """
    if not request.task_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="task_ids cannot be empty"
        )

    # Verify all tasks belong to the current user and are failed
    tasks = db.query(CrawlTask).filter(
        CrawlTask.id.in_(request.task_ids),
        CrawlTask.user_id == current_user["id"],
        CrawlTask.status == TaskStatus.FAILED
    ).all()

    if not tasks:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No valid failed tasks found for retry"
        )

    # Ensure task manager is running
    if not task_manager.running:
        task_manager.start()

    success_count = 0
    fail_count = 0
    failed_task_ids = []

    for task in tasks:
        try:
            # Reset task state
            task.status = TaskStatus.PENDING
            task.retry_count = 0
            task.error_message = None
            task.progress = 0
            task.updated_at = datetime.utcnow()
            db.commit()

            # Re-add to queue with high priority
            import queue as queue_module
            priority_value = task_manager.task_queue._get_priority_value(TaskPriority.HIGH)
            queue_item = (priority_value, datetime.utcnow().timestamp(), task.id)

            with task_manager.task_queue._lock:
                task_manager.task_queue._task_map[task.id] = task
                task_manager.task_queue._queue.put_nowait(queue_item)

            success_count += 1
            logger.info(f"[批量重试] 任务 {task.id} 已重新入队")

        except queue_module.Full:
            # Queue full, revert task status
            task.status = TaskStatus.FAILED
            task.error_message = "Retry failed: queue is full"
            db.commit()
            fail_count += 1
            failed_task_ids.append(task.id)
            logger.warning(f"[批量重试] 队列已满，无法重试任务 {task.id}")

        except Exception as e:
            fail_count += 1
            failed_task_ids.append(task.id)
            logger.error(f"[批量重试] 重试任务 {task.id} 失败: {e}", exc_info=True)

    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="batch_retry_failed_tasks",
        target_type="crawl_task",
        operation_detail={
            "task_ids": request.task_ids,
            "success_count": success_count,
            "fail_count": fail_count
        }
    )

    return BatchRetryResponse(
        success_count=success_count,
        fail_count=fail_count,
        total=len(request.task_ids),
        message=f"成功重试 {success_count} 个任务" + (f"，{fail_count} 个失败" if fail_count > 0 else ""),
        failed_task_ids=failed_task_ids
    )


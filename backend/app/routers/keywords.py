"""Keywords and link library management API"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.keyword import Keyword, KeywordLink, KeywordStatus
from app.models.crawl_task import CrawlTask, TaskType, TaskStatus, TaskPriority, ErrorLog
from app.models.user import User
from app.services.task_manager import task_manager
from app.services.operation_log_service import create_operation_log
from app.services.crawler import crawl_keyword_search

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/keywords", tags=["keywords"])

class KeywordCreate(BaseModel):
    """Keyword create model"""
    keyword: str

class KeywordResponse(BaseModel):
    """Keyword response model"""
    id: int
    keyword: str
    status: str
    created_at: datetime
    created_by_user_id: int

    class Config:
        from_attributes = True

class KeywordLinkResponse(BaseModel):
    """Keyword link response model"""
    id: int
    keyword_id: int
    product_url: str
    pnk_code: Optional[str] = None  # PNK_CODE（产品编码）
    thumbnail_image: Optional[str] = None  # 产品缩略图URL
    price: Optional[float] = None  # 售价
    crawled_at: datetime
    status: str

    class Config:
        from_attributes = True

class TaskResponse(BaseModel):
    """Task response model"""
    id: int
    task_type: str
    status: str
    priority: str
    progress: int
    created_at: datetime
    updated_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]

    class Config:
        from_attributes = True

class ErrorLogResponse(BaseModel):
    """Error log response model"""
    id: int
    task_id: Optional[int]
    error_type: str
    error_message: Optional[str]
    occurred_at: datetime
    resolved_at: Optional[datetime]

    class Config:
        from_attributes = True

@router.post("", response_model=KeywordResponse)
async def add_keyword(
    keyword_data: KeywordCreate,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Add keyword and start search task"""
    # Create keyword
    keyword = Keyword(
        keyword=keyword_data.keyword,
        created_by_user_id=current_user["id"],
        status=KeywordStatus.PENDING
    )
    db.add(keyword)
    db.commit()
    db.refresh(keyword)
    
    # Create task
    
    try:
        task = task_manager.add_task(
            db=db,
            task_type=TaskType.KEYWORD_SEARCH,
            user_id=current_user["id"],
            keyword_id=keyword.id,
            priority=TaskPriority.NORMAL
        )
        
    except Exception as e:
        raise
    
    # Update keyword status
    keyword.status = KeywordStatus.PROCESSING
    db.commit()
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="keyword_add",
        target_type="keyword",
        target_id=keyword.id,
        operation_detail={"keyword": keyword_data.keyword}
    )
    
    # Start background task (placeholder - actual implementation would use task queue)
    # background_tasks.add_task(process_keyword_search, keyword.id, task.id)
    
    return keyword

class BatchKeywordsRequest(BaseModel):
    """Batch keywords request model"""
    keywords: List[str]

@router.post("/batch", response_model=List[KeywordResponse])
async def batch_add_keywords(
    request: BatchKeywordsRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Batch add keywords"""
    
    keywords = request.keywords
    created_keywords = []
    
    
    try:
        for keyword_str in keywords:
            keyword = Keyword(
                keyword=keyword_str,
                created_by_user_id=current_user["id"],
                status=KeywordStatus.PENDING
            )
            db.add(keyword)
            created_keywords.append(keyword)
        
        
        db.commit()
        
        
        # Create tasks for each keyword
        for keyword in created_keywords:
            
            try:
                task_manager.add_task(
                    db=db,
                    task_type=TaskType.KEYWORD_SEARCH,
                    user_id=current_user["id"],
                    keyword_id=keyword.id,
                    priority=TaskPriority.NORMAL
                )
                keyword.status = KeywordStatus.PROCESSING
            except Exception as task_error:
                raise
        
        
        db.commit()
        
        # Log operation
        create_operation_log(
            db=db,
            user_id=current_user["id"],
            operation_type="keyword_add",
            target_type="keyword",
            operation_detail={"keywords": keywords, "count": len(keywords)}
        )
        
        
        return created_keywords
    
    except Exception as e:
        db.rollback()
        raise

@router.get("", response_model=List[KeywordResponse])
async def list_keywords(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """List keywords"""
    keywords = db.query(Keyword).filter(
        Keyword.created_by_user_id == current_user["id"]
    ).offset(skip).limit(limit).all()
    return keywords

@router.get("/links")
async def get_keyword_links(
    keyword_id: Optional[int] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None
):
    """Get keyword links with optional filters"""
    query = db.query(KeywordLink)
    if keyword_id:
        query = query.filter(KeywordLink.keyword_id == keyword_id)
    if price_min is not None:
        query = query.filter(KeywordLink.price >= price_min)
    if price_max is not None:
        query = query.filter(KeywordLink.price <= price_max)
    
    # 获取总数
    total = query.count()
    
    # 获取分页数据
    links = query.order_by(KeywordLink.crawled_at.desc()).offset(skip).limit(limit).all()
    
    return {
        "items": links,
        "total": total,
        "skip": skip,
        "limit": limit
    }

class BatchCrawlLinksRequest(BaseModel):
    """Batch crawl links request model"""
    link_ids: List[int]

@router.post("/links/batch-crawl")
async def batch_crawl_links(
    request: BatchCrawlLinksRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """批量创建链接爬取任务"""
    logger.info(f"[批量爬取] 开始批量创建链接爬取任务 - 用户ID: {current_user['id']}, 链接数: {len(request.link_ids)}")
    try:
        # 获取链接
        links = db.query(KeywordLink).filter(
            KeywordLink.id.in_(request.link_ids),
            KeywordLink.status == "active"
        ).all()
        
        if not links:
            logger.warning(f"[批量爬取] 没有找到有效的链接 - 用户ID: {current_user['id']}, 链接ID: {request.link_ids}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="没有找到有效的链接"
            )
        
        logger.info(f"[批量爬取] 找到有效链接 - 用户ID: {current_user['id']}, 链接数: {len(links)}")
        
        
        # ⚠️ 关键修复：确保任务管理器在创建任务之前已启动
        if not task_manager.running:
            logger.info(f"[批量爬取] 启动任务管理器 - 用户ID: {current_user['id']}")
            task_manager.start()
        else:
            logger.info(f"[批量爬取] 任务管理器已运行 - 用户ID: {current_user['id']}")
        
        # 为每个链接创建爬取任务
        created_count = 0
        skipped_count = 0
        product_urls = []
        for link in links:
            # 检查是否已有进行中的任务
            existing_task = db.query(CrawlTask).filter(
                CrawlTask.product_url == link.product_url,
                CrawlTask.status.in_([TaskStatus.PENDING, TaskStatus.PROCESSING])
            ).first()
            
            if not existing_task:
                # 使用task_manager.add_task()方法，确保任务被添加到队列
                try:
                    task_id = task_manager.add_task(
                        task_type=TaskType.PRODUCT_CRAWL,
                        product_url=link.product_url,
                        keyword_id=link.keyword_id,
                        user_id=current_user["id"],
                        priority=TaskPriority.NORMAL,
                        db=db
                    )
                    created_count += 1
                    product_urls.append(link.product_url)
                    logger.info(f"[批量爬取] 创建任务成功 - 任务ID: {task_id}, 产品URL: {link.product_url}")
                except Exception as e:
                    logger.error(f"[批量爬取] 创建任务失败 - 产品URL: {link.product_url}, 错误: {str(e)}")
                    skipped_count += 1
            else:
                skipped_count += 1
                logger.debug(f"[批量爬取] 跳过已有任务 - 产品URL: {link.product_url}, 现有任务ID: {existing_task.id}")
        
        logger.info(f"[批量爬取] 任务创建完成 - 用户ID: {current_user['id']}, 创建任务数: {created_count}, 跳过任务数: {skipped_count}, 总链接数: {len(links)}")
        
        
        # 记录操作日志
        create_operation_log(
            db=db,
            user_id=current_user["id"],
            operation_type="batch_crawl_links",
            target_type="keyword_link",
            operation_detail={
                "link_ids": request.link_ids,
                "created_count": created_count
            }
        )
        
        logger.info(f"[批量爬取] 批量爬取任务创建完成 - 用户ID: {current_user['id']}, 成功创建: {created_count}/{len(links)}")
        
        return {
            "created_count": created_count,
            "total_links": len(links),
            "message": f"成功创建 {created_count} 个爬取任务"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating batch crawl tasks: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建批量爬取任务失败: {str(e)}"
        )

@router.get("/tasks", response_model=List[TaskResponse])
async def get_tasks(
    status: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Get user's tasks"""
    task_status = None
    if status:
        try:
            task_status = TaskStatus(status)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status}"
            )
    
    tasks = task_manager.get_user_tasks(
        db=db,
        user_id=current_user["id"],
        status=task_status,
        skip=skip,
        limit=limit
    )
    return tasks

@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get task by ID"""
    task = task_manager.get_task(db, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    
    if task.user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to view this task"
        )
    
    return task

@router.post("/tasks/{task_id}/retry", response_model=TaskResponse)
async def retry_task(
    task_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Retry failed task"""
    task = task_manager.get_task(db, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    
    if task.user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to retry this task"
        )
    
    if task.status not in [TaskStatus.FAILED, TaskStatus.RETRY]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only failed or retry tasks can be retried"
        )
    
    # Use task manager's retry method (thread-safe and proper queue handling)
    retry_success = task_manager.retry_task(
        task_id=task_id,
        user_id=current_user["id"],
        db=db
    )
    
    if not retry_success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to retry task. Task may have exceeded max retries or queue is full."
        )
    
    # Refresh task to get updated status
    db.refresh(task)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="task_retry",
        target_type="crawl_task",
        target_id=task_id
    )
    
    return task

@router.get("/error-logs", response_model=List[ErrorLogResponse])
async def get_error_logs(
    task_id: Optional[int] = None,
    error_type: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Get error logs"""
    query = db.query(ErrorLog)
    
    if task_id:
        query = query.filter(ErrorLog.task_id == task_id)
        # Verify task belongs to user
        task = task_manager.get_task(db, task_id)
        if task and task.user_id != current_user["id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to view this task's error logs"
            )
    else:
        # Only show error logs for user's tasks
        user_task_ids = [t.id for t in task_manager.get_user_tasks(db, current_user["id"])]
        query = query.filter(ErrorLog.task_id.in_(user_task_ids))
    
    if error_type:
        query = query.filter(ErrorLog.error_type == error_type)
    
    logs = query.order_by(ErrorLog.occurred_at.desc()).offset(skip).limit(limit).all()
    return logs


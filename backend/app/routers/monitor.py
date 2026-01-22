"""Monitor pool management API"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.monitor_pool import MonitorPool, MonitorHistory, MonitorStatus
from app.models.product import FilterPool
from sqlalchemy import func, desc
from app.services.operation_log_service import create_operation_log
from app.services.scheduler import scheduler, trigger_monitor_manual
from app.services.crawler import crawl_monitor_product
from app.config import config

router = APIRouter(prefix="/api/monitor-pool", tags=["monitor"])

class MonitorPoolResponse(BaseModel):
    """Monitor pool response model"""
    id: int
    filter_pool_id: Optional[int]
    product_url: str
    status: str
    created_by_user_id: int
    last_monitored_at: Optional[str]
    created_at: str
    # 来自 filter_pool 的字段
    thumbnail_image: Optional[str] = None
    product_name: Optional[str] = None
    brand: Optional[str] = None
    shop_name: Optional[str] = None
    is_fbe: Optional[bool] = None
    competitor_count: Optional[int] = None
    # 最新监控数据
    price: Optional[float] = None
    stock: Optional[int] = None
    review_count: Optional[int] = None
    shop_rank: Optional[int] = None
    category_rank: Optional[int] = None
    ad_rank: Optional[int] = None

    class Config:
        from_attributes = True

class MonitorHistoryResponse(BaseModel):
    """Monitor history response model"""
    id: int
    monitor_pool_id: int
    price: Optional[float]
    stock: Optional[int]
    review_count: Optional[int]
    shop_rank: Optional[int]
    category_rank: Optional[int]
    ad_rank: Optional[int]
    monitored_at: str

    class Config:
        from_attributes = True

class MonitorPoolListResponse(BaseModel):
    """Monitor pool list response with pagination"""
    items: List[MonitorPoolResponse]
    total: int
    skip: int
    limit: int

@router.get("", response_model=MonitorPoolListResponse)
async def get_monitor_pool(
    status: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    page: Optional[int] = None,
    page_size: Optional[int] = None
):
    """Get monitor pool"""
    
    # 支持前端传递的 page 和 page_size 参数
    if page is not None and page_size is not None:
        skip = (page - 1) * page_size
        limit = page_size
    
    query = db.query(MonitorPool).filter(
        MonitorPool.created_by_user_id == current_user["id"]
    )
    
    
    if status:
        try:
            monitor_status = MonitorStatus(status)
            query = query.filter(MonitorPool.status == monitor_status)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status}"
            )
    
    # 获取总数
    total = query.count()
    
    
    monitors = query.order_by(MonitorPool.created_at.desc()).offset(skip).limit(limit).all()
    
    # 获取所有 monitor_ids
    monitor_ids = [m.id for m in monitors]
    filter_pool_ids = [m.filter_pool_id for m in monitors if m.filter_pool_id]
    
    # 批量获取 filter_pool 数据
    filter_pool_map = {}
    if filter_pool_ids:
        filter_pools = db.query(FilterPool).filter(FilterPool.id.in_(filter_pool_ids)).all()
        filter_pool_map = {fp.id: fp for fp in filter_pools}
    
    # 批量获取每个 monitor 的最新历史数据（使用子查询获取每个 monitor 的最新记录 id）
    latest_history_map = {}
    if monitor_ids:
        # 子查询：获取每个 monitor_pool_id 的最新 history id
        subquery = db.query(
            MonitorHistory.monitor_pool_id,
            func.max(MonitorHistory.id).label('max_id')
        ).filter(
            MonitorHistory.monitor_pool_id.in_(monitor_ids)
        ).group_by(MonitorHistory.monitor_pool_id).subquery()
        
        # 主查询：获取这些最新记录的详细数据
        latest_histories = db.query(MonitorHistory).join(
            subquery,
            (MonitorHistory.monitor_pool_id == subquery.c.monitor_pool_id) &
            (MonitorHistory.id == subquery.c.max_id)
        ).all()
        
        latest_history_map = {h.monitor_pool_id: h for h in latest_histories}
    
    # 转换 datetime 字段为字符串
    converted_monitors = []
    for monitor in monitors:
        # 获取关联的 filter_pool 数据
        fp = filter_pool_map.get(monitor.filter_pool_id) if monitor.filter_pool_id else None
        # 获取最新的监控历史数据
        latest_history = latest_history_map.get(monitor.id)
        
        monitor_dict = {
            "id": monitor.id,
            "filter_pool_id": monitor.filter_pool_id,
            "product_url": monitor.product_url,
            "status": monitor.status.value if hasattr(monitor.status, 'value') else str(monitor.status),
            "created_by_user_id": monitor.created_by_user_id,
            "last_monitored_at": monitor.last_monitored_at.isoformat() if monitor.last_monitored_at and isinstance(monitor.last_monitored_at, datetime) else (str(monitor.last_monitored_at) if monitor.last_monitored_at else None),
            "created_at": monitor.created_at.isoformat() if monitor.created_at and isinstance(monitor.created_at, datetime) else (str(monitor.created_at) if monitor.created_at else ""),
            # 来自 filter_pool 的字段
            "thumbnail_image": fp.thumbnail_image if fp else None,
            "product_name": fp.product_name if fp else None,
            "brand": fp.brand if fp else None,
            "shop_name": fp.shop_name if fp else None,
            "is_fbe": fp.is_fbe if fp else None,
            "competitor_count": fp.competitor_count if fp else None,
            # 最新监控数据（优先使用 history，否则使用 filter_pool）
            "price": latest_history.price if latest_history else (fp.price if fp else None),
            "stock": latest_history.stock if latest_history else (fp.stock if fp else None),
            "review_count": latest_history.review_count if latest_history else (fp.review_count if fp else None),
            "shop_rank": latest_history.shop_rank if latest_history else (fp.shop_rank if fp else None),
            "category_rank": latest_history.category_rank if latest_history else (fp.category_rank if fp else None),
            "ad_rank": latest_history.ad_rank if latest_history else (fp.ad_rank if fp else None),
        }
        converted_monitors.append(MonitorPoolResponse(**monitor_dict))
    
    response = MonitorPoolListResponse(
        items=converted_monitors,
        total=total,
        skip=skip,
        limit=limit
    )
    
    
    return response

@router.get("/{monitor_id}/history", response_model=List[MonitorHistoryResponse])
async def get_monitor_history(
    monitor_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Get monitor history"""
    # Verify monitor belongs to user
    monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found"
        )
    
    if monitor.created_by_user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to view this monitor"
        )
    
    history = db.query(MonitorHistory).filter(
        MonitorHistory.monitor_pool_id == monitor_id
    ).order_by(MonitorHistory.monitored_at.desc()).offset(skip).limit(limit).all()
    
    # 转换 datetime 字段为字符串
    converted_history = []
    for h in history:
        history_dict = {
            "id": h.id,
            "monitor_pool_id": h.monitor_pool_id,
            "price": h.price,
            "stock": h.stock,
            "review_count": h.review_count,
            "shop_rank": h.shop_rank,
            "category_rank": h.category_rank,
            "ad_rank": h.ad_rank,
            "monitored_at": h.monitored_at.isoformat() if h.monitored_at and isinstance(h.monitored_at, datetime) else (str(h.monitored_at) if h.monitored_at else "")
        }
        converted_history.append(MonitorHistoryResponse(**history_dict))
    
    return converted_history

@router.post("/{monitor_id}/trigger", response_model=dict)
async def trigger_monitor(
    monitor_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Manually trigger monitor crawl"""
    monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found"
        )
    
    if monitor.created_by_user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to trigger this monitor"
        )
    
    if monitor.status != MonitorStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Monitor is not active"
        )
    
    # Crawl product data
    product_data = crawl_monitor_product(monitor_id, monitor.product_url, db)
    
    if not product_data:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to crawl product data"
        )
    
    # Create monitor history record
    history = MonitorHistory(
        monitor_pool_id=monitor_id,
        price=product_data.get('price'),
        stock=product_data.get('stock'),
        review_count=product_data.get('review_count'),
        shop_rank=product_data.get('shop_rank'),
        category_rank=product_data.get('category_rank'),
        ad_rank=product_data.get('ad_rank'),
        monitored_at=datetime.utcnow()
    )
    
    db.add(history)
    
    # Update monitor's last_monitored_at
    monitor.last_monitored_at = datetime.utcnow()
    db.commit()
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="monitor_trigger",
        target_type="monitor_pool",
        target_id=monitor_id,
        operation_detail={"product_url": monitor.product_url}
    )
    
    return {
        "message": "Monitor triggered successfully",
        "monitor_id": monitor_id,
        "history_id": history.id
    }

@router.put("/{monitor_id}/status", response_model=MonitorPoolResponse)
async def update_monitor_status(
    monitor_id: int,
    new_status: str,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Update monitor status"""
    monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found"
        )
    
    if monitor.created_by_user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to update this monitor"
        )
    
    try:
        monitor.status = MonitorStatus(new_status)
        db.commit()
        db.refresh(monitor)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid status: {new_status}"
        )
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="status_change",
        target_type="monitor_pool",
        target_id=monitor_id,
        operation_detail={"new_status": new_status}
    )
    
    return monitor

class TriggerBatchRequest(BaseModel):
    """Trigger batch request model"""
    monitor_ids: Optional[List[int]] = None

@router.post("/trigger-batch", response_model=dict)
async def trigger_monitor_batch(
    request: TriggerBatchRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Manually trigger monitoring for multiple monitors"""
    # Trigger monitoring (this runs in background)
    result = trigger_monitor_manual(request.monitor_ids)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="monitor_trigger_batch",
        target_type="monitor_pool",
        operation_detail={
            "monitor_ids": request.monitor_ids,
            "processed": result.get("processed", 0),
            "success": result.get("success", 0),
            "failed": result.get("failed", 0)
        }
    )
    
    return result

class ScheduleConfigResponse(BaseModel):
    """Schedule config response model"""
    enabled: bool
    schedule_time: Optional[str] = None
    timezone: str

class ScheduleConfigRequest(BaseModel):
    """Schedule config request model"""
    enabled: bool
    schedule_time: Optional[str] = None
    timezone: Optional[str] = None

@router.get("/schedule", response_model=ScheduleConfigResponse)
async def get_schedule_config(
    current_user: dict = Depends(require_auth)
):
    """Get monitor schedule configuration"""
    
    # 从配置中获取调度器设置
    from app.config import config
    schedule_hour = config.MONITOR_SCHEDULE_HOUR
    schedule_minute = config.MONITOR_SCHEDULE_MINUTE
    schedule_time = f"{schedule_hour:02d}:{schedule_minute:02d}"
    
    return ScheduleConfigResponse(
        enabled=True,  # 调度器默认启用
        schedule_time=schedule_time,
        timezone=config.SCHEDULER_TIMEZONE
    )

@router.put("/schedule", response_model=ScheduleConfigResponse)
async def update_schedule_config(
    request: ScheduleConfigRequest,
    current_user: dict = Depends(require_auth)
):
    """Update monitor schedule configuration"""
    
    # 注意：这里只是返回配置，实际的调度器配置在config.py中
    # 如果需要动态修改，需要重新配置scheduler
    from app.config import config
    
    if request.schedule_time:
        try:
            # 解析时间字符串 "HH:mm"
            time_parts = request.schedule_time.split(':')
            if len(time_parts) == 2:
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                # 更新调度器任务
                scheduler.reschedule_job(
                    "daily_monitor",
                    trigger="cron",
                    hour=hour,
                    minute=minute,
                    timezone=request.timezone or config.SCHEDULER_TIMEZONE
                )
        except Exception as e:
            logger.warning(f"Failed to update schedule: {e}")
    
    schedule_hour = config.MONITOR_SCHEDULE_HOUR
    schedule_minute = config.MONITOR_SCHEDULE_MINUTE
    schedule_time = f"{schedule_hour:02d}:{schedule_minute:02d}"
    
    return ScheduleConfigResponse(
        enabled=request.enabled,
        schedule_time=request.schedule_time or schedule_time,
        timezone=request.timezone or config.SCHEDULER_TIMEZONE
    )

class AddToMonitorRequest(BaseModel):
    """Add to monitor request model"""
    product_url: str

@router.post("", response_model=MonitorPoolResponse)
async def add_to_monitor_pool(
    request: AddToMonitorRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Add product to monitor pool"""
    product_url = request.product_url
    if not product_url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="product_url is required"
        )
    
    # Check if already exists
    existing = db.query(MonitorPool).filter(
        MonitorPool.product_url == product_url,
        MonitorPool.created_by_user_id == current_user["id"]
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Product already in monitor pool"
        )
    
    monitor = MonitorPool(
        product_url=product_url,
        created_by_user_id=current_user["id"],
        status=MonitorStatus.ACTIVE
    )
    db.add(monitor)
    db.commit()
    db.refresh(monitor)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="monitor_add",
        target_type="monitor_pool",
        target_id=monitor.id,
        operation_detail={"product_url": product_url}
    )
    
    # Convert datetime to string
    monitor_dict = {
        "id": monitor.id,
        "filter_pool_id": monitor.filter_pool_id,
        "product_url": monitor.product_url,
        "status": monitor.status.value if hasattr(monitor.status, 'value') else str(monitor.status),
        "created_by_user_id": monitor.created_by_user_id,
        "last_monitored_at": monitor.last_monitored_at.isoformat() if monitor.last_monitored_at and isinstance(monitor.last_monitored_at, datetime) else (str(monitor.last_monitored_at) if monitor.last_monitored_at else None),
        "created_at": monitor.created_at.isoformat() if monitor.created_at and isinstance(monitor.created_at, datetime) else (str(monitor.created_at) if monitor.created_at else "")
    }
    
    return MonitorPoolResponse(**monitor_dict)

@router.delete("/{monitor_id}", response_model=dict)
async def remove_from_monitor_pool(
    monitor_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Remove product from monitor pool"""
    monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found"
        )
    
    if monitor.created_by_user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to remove this monitor"
        )
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="monitor_remove",
        target_type="monitor_pool",
        target_id=monitor_id,
        operation_detail={"product_url": monitor.product_url}
    )
    
    db.delete(monitor)
    db.commit()
    
    return {"message": "Monitor removed successfully"}

class TriggerMonitorRequest(BaseModel):
    """Trigger monitor request model"""
    product_ids: List[int]

@router.post("/trigger", response_model=dict)
async def trigger_monitor_batch_simple(
    request: TriggerMonitorRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Manually trigger monitoring for multiple monitors (simplified endpoint)"""
    product_ids = request.product_ids
    if not product_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="product_ids is required"
        )
    
    # Trigger monitoring (this runs in background)
    result = trigger_monitor_manual(product_ids)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="monitor_trigger_batch",
        target_type="monitor_pool",
        operation_detail={
            "monitor_ids": product_ids,
            "processed": result.get("processed", 0),
            "success": result.get("success", 0),
            "failed": result.get("failed", 0)
        }
    )
    
    return result


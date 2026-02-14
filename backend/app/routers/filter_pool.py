"""Filter pool management API"""
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from pydantic import BaseModel
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.product import FilterPool
from app.models.monitor_pool import MonitorPool, MonitorStatus
from app.services.operation_log_service import create_operation_log

router = APIRouter(prefix="/api/filter-pool", tags=["filter-pool"])

class FilterPoolResponse(BaseModel):
    """Filter pool response model"""
    id: int
    product_url: str
    product_name: Optional[str]
    thumbnail_image: Optional[str]  # 产品缩略图URL
    brand: Optional[str]  # 品牌
    shop_name: Optional[str]  # 店铺名称
    price: Optional[float]
    rating: Optional[float]
    listed_at: Optional[str]
    stock: Optional[int]
    review_count: Optional[int]
    latest_review_at: Optional[str]
    earliest_review_at: Optional[str]
    shop_rank: Optional[int]
    category_rank: Optional[int]
    ad_rank: Optional[int]
    is_fbe: Optional[bool]  # 是否是FBE
    competitor_count: Optional[int]  # 跟卖数
    crawled_at: str

    class Config:
        from_attributes = True

class FilterRequest(BaseModel):
    """Filter request model"""
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_review_count: Optional[int] = None
    max_review_count: Optional[int] = None
    min_rating: Optional[float] = None
    max_rating: Optional[float] = None
    min_shop_rank: Optional[int] = None
    max_shop_rank: Optional[int] = None
    min_category_rank: Optional[int] = None
    max_category_rank: Optional[int] = None
    has_stock: Optional[bool] = None

class MoveToMonitorRequest(BaseModel):
    """Move to monitor request model"""
    filter_pool_ids: List[int]

class FilterPoolListResponse(BaseModel):
    """Filter pool list response with pagination"""
    items: List[FilterPoolResponse]
    total: int
    skip: int
    limit: int

    class Config:
        from_attributes = True

class MoveToMonitorResponse(BaseModel):
    """Move to monitor response model"""
    message: str
    created_count: int
    skipped_count: int

class FilterCountResponse(BaseModel):
    """Filter count response model"""
    count: int

@router.get("", response_model=FilterPoolListResponse)
async def get_filter_pool(
    request: Request,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_review_count: Optional[int] = None,
    max_review_count: Optional[int] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    min_shop_rank: Optional[int] = None,
    max_shop_rank: Optional[int] = None,
    min_category_rank: Optional[int] = None,
    max_category_rank: Optional[int] = None,
    has_stock: Optional[bool] = None,
    listed_at_period: Optional[str] = None,
    # 品牌/店铺剔除与链接初筛保持兼容：既支持 exclude_brands=a&exclude_brands=b 也支持 exclude_brands[]=a&exclude_brands[]=b
    exclude_brands: Optional[List[str]] = Query(None),
    exclude_brands_brackets: Optional[List[str]] = Query(None, alias="exclude_brands[]"),
    exclude_shops: Optional[List[str]] = Query(None),
    exclude_shops_brackets: Optional[List[str]] = Query(None, alias="exclude_shops[]"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Get filter pool with filters"""
    
    try:
        query = db.query(FilterPool)

        # 兼容 axios 对数组参数使用 exclude_xxx[] 的情况
        if (not exclude_brands) and exclude_brands_brackets:
            exclude_brands = exclude_brands_brackets
        if (not exclude_shops) and exclude_shops_brackets:
            exclude_shops = exclude_shops_brackets
        
        # Apply filters（所有条件联动按 AND 过滤）
        if min_price is not None:
            query = query.filter(FilterPool.price >= min_price)
        if max_price is not None:
            query = query.filter(FilterPool.price <= max_price)
        if min_review_count is not None:
            query = query.filter(FilterPool.review_count >= min_review_count)
        if max_review_count is not None:
            query = query.filter(FilterPool.review_count <= max_review_count)
        if min_rating is not None:
            query = query.filter(FilterPool.rating >= min_rating)
        if max_rating is not None:
            query = query.filter(FilterPool.rating <= max_rating)
        if min_shop_rank is not None:
            query = query.filter(FilterPool.shop_rank >= min_shop_rank)
        if max_shop_rank is not None:
            query = query.filter(FilterPool.shop_rank <= max_shop_rank)
        if min_category_rank is not None:
            query = query.filter(FilterPool.category_rank >= min_category_rank)
        if max_category_rank is not None:
            query = query.filter(FilterPool.category_rank <= max_category_rank)
        if has_stock is not None:
            if has_stock:
                query = query.filter(FilterPool.stock > 0)
            else:
                query = query.filter(or_(FilterPool.stock == 0, FilterPool.stock.is_(None)))

        # 上架日期筛选：逻辑与链接初筛保持一致
        if listed_at_period:
            now = datetime.utcnow()
            start_date = None
            if listed_at_period == "6months":
                start_date = now - timedelta(days=180)
            elif listed_at_period == "1year":
                start_date = now - timedelta(days=365)
            elif listed_at_period == "1.5years":
                start_date = now - timedelta(days=547)

            if start_date:
                # 成功获取且在时间范围内，或已去爬但未获取到（not_found/error），排除 pending
                query = query.filter(
                    or_(
                        and_(
                            FilterPool.listed_at_status == "success",
                            FilterPool.listed_at >= start_date,
                        ),
                        FilterPool.listed_at_status.in_(["not_found", "error"]),
                    )
                )

        # 品牌剔除：逻辑与链接初筛保持一致，保留 brand 为 NULL 的记录
        if exclude_brands:
            query = query.filter(
                or_(
                    FilterPool.brand.is_(None),
                    ~FilterPool.brand.in_(exclude_brands),
                )
            )

        # 店铺剔除：与品牌剔除逻辑一致，保留 shop_name 为 NULL 的记录
        if exclude_shops:
            query = query.filter(
                or_(
                    FilterPool.shop_name.is_(None),
                    ~FilterPool.shop_name.in_(exclude_shops),
                )
            )
    
        
        # Get total count
        total = query.count()
        
        
        # Get paginated results
        products = query.order_by(FilterPool.crawled_at.desc()).offset(skip).limit(limit).all()
        
        
        # Convert datetime fields to strings for response
        converted_products = []
        for product in products:
            product_dict = {
                "id": product.id,
                "product_url": product.product_url,
                "product_name": product.product_name,
                "thumbnail_image": product.thumbnail_image,
                "brand": product.brand,
                "shop_name": product.shop_name,
                "price": product.price,
                "rating": product.rating,
                "stock": product.stock,
                "review_count": product.review_count,
                "shop_rank": product.shop_rank,
                "category_rank": product.category_rank,
                "ad_rank": product.ad_rank,
                "is_fbe": product.is_fbe,
                "competitor_count": product.competitor_count,
                "listed_at": product.listed_at.isoformat() if product.listed_at and isinstance(product.listed_at, datetime) else (str(product.listed_at) if product.listed_at else None),
                "latest_review_at": product.latest_review_at.isoformat() if product.latest_review_at and isinstance(product.latest_review_at, datetime) else (str(product.latest_review_at) if product.latest_review_at else None),
                "earliest_review_at": product.earliest_review_at.isoformat() if product.earliest_review_at and isinstance(product.earliest_review_at, datetime) else (str(product.earliest_review_at) if product.earliest_review_at else None),
                "crawled_at": product.crawled_at.isoformat() if product.crawled_at and isinstance(product.crawled_at, datetime) else (str(product.crawled_at) if product.crawled_at else "")
            }
            converted_products.append(FilterPoolResponse(**product_dict))
        
        
        response = FilterPoolListResponse(
            items=converted_products,
            total=total,
            skip=skip,
            limit=limit
        )
        
        
        return response
    except Exception as e:
        # 调试：记录筛选池列表加载失败的详细原因到 debug.log
        # #region agent log
        import json as _json_fp, time as _time_fp
        try:
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_fp.dumps({
                    "timestamp": int(_time_fp.time() * 1000),
                    "location": "filter_pool.py:get_filter_pool:exception",
                    "message": "筛选池加载产品列表失败",
                    "data": {
                        "error": str(e)[:300],
                        "error_type": type(e).__name__
                    },
                    "hypothesisId": "H_filter_pool_load_fail",
                    "runId": "filter-pool-debug"
                }, ensure_ascii=False) + "\n")
        except Exception:
            # 日志失败不影响主流程
            pass
        # #endregion
        raise

@router.post("/filter", response_model=FilterPoolListResponse)
async def filter_products(
    request: Request,
    filter_data: FilterRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Filter products with custom criteria"""
    query = db.query(FilterPool)
    
    # Apply filters
    if filter_data.min_price is not None:
        query = query.filter(FilterPool.price >= filter_data.min_price)
    if filter_data.max_price is not None:
        query = query.filter(FilterPool.price <= filter_data.max_price)
    if filter_data.min_review_count is not None:
        query = query.filter(FilterPool.review_count >= filter_data.min_review_count)
    if filter_data.max_review_count is not None:
        query = query.filter(FilterPool.review_count <= filter_data.max_review_count)
    if filter_data.min_rating is not None:
        query = query.filter(FilterPool.rating >= filter_data.min_rating)
    if filter_data.max_rating is not None:
        query = query.filter(FilterPool.rating <= filter_data.max_rating)
    if filter_data.min_shop_rank is not None:
        query = query.filter(FilterPool.shop_rank >= filter_data.min_shop_rank)
    if filter_data.max_shop_rank is not None:
        query = query.filter(FilterPool.shop_rank <= filter_data.max_shop_rank)
    if filter_data.min_category_rank is not None:
        query = query.filter(FilterPool.category_rank >= filter_data.min_category_rank)
    if filter_data.max_category_rank is not None:
        query = query.filter(FilterPool.category_rank <= filter_data.max_category_rank)
    if filter_data.has_stock is not None:
        if filter_data.has_stock:
            query = query.filter(FilterPool.stock > 0)
        else:
            query = query.filter(or_(FilterPool.stock == 0, FilterPool.stock.is_(None)))
    
    # Get total count
    total = query.count()
    
    # Get paginated results
    products = query.order_by(FilterPool.crawled_at.desc()).offset(skip).limit(limit).all()
    
    # Get IP address
    ip_address = request.client.host if request.client else None
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="filter_select",
        target_type="filter_pool",
        operation_detail={
            "filter": filter_data.dict(),
            "result_count": len(products),
            "total_count": total,
            "skip": skip,
            "limit": limit
        },
        ip_address=ip_address
    )
    
    return FilterPoolListResponse(
        items=products,
        total=total,
        skip=skip,
        limit=limit
    )

@router.post("/move-to-monitor", response_model=MoveToMonitorResponse)
async def move_to_monitor(
    http_request: Request,
    request: MoveToMonitorRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Move products from filter pool to monitor pool"""
    
    try:
        if not request.filter_pool_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="filter_pool_ids cannot be empty"
            )
        
        # Get filter pool products
        filter_products = db.query(FilterPool).filter(
            FilterPool.id.in_(request.filter_pool_ids)
        ).all()
        
        if not filter_products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No products found with the provided IDs"
            )
        
        # Create monitor pool entries
        created_count = 0
        skipped_count = 0
        created_ids = []
        
        for fp in filter_products:
            # Check if already in monitor pool
            existing = db.query(MonitorPool).filter(
                MonitorPool.product_url == fp.product_url
            ).first()
            
            if not existing:
                monitor = MonitorPool(
                    filter_pool_id=fp.id,
                    product_url=fp.product_url,
                    created_by_user_id=current_user["id"],
                    status=MonitorStatus.ACTIVE
                )
                db.add(monitor)
                created_count += 1
                created_ids.append(fp.id)
            else:
                skipped_count += 1
        
        db.commit()
        
        # Get IP address
        ip_address = http_request.client.host if http_request.client else None
        
        # Log operation
        create_operation_log(
            db=db,
            user_id=current_user["id"],
            operation_type="monitor_add",
            target_type="filter_pool",
            target_id=None,  # Multiple targets, so None
            operation_detail={
                "filter_pool_ids": request.filter_pool_ids,
                "created_count": created_count,
                "skipped_count": skipped_count,
                "created_filter_pool_ids": created_ids
            },
            ip_address=ip_address
        )
        
        return MoveToMonitorResponse(
            message=f"Successfully moved {created_count} products to monitor pool" + 
                    (f", {skipped_count} products were already in monitor pool" if skipped_count > 0 else ""),
            created_count=created_count,
            skipped_count=skipped_count
        )
    except Exception as e:
        raise

@router.get("/count", response_model=FilterCountResponse)
async def get_filter_pool_count(
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_review_count: Optional[int] = None,
    max_review_count: Optional[int] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    min_shop_rank: Optional[int] = None,
    max_shop_rank: Optional[int] = None,
    min_category_rank: Optional[int] = None,
    max_category_rank: Optional[int] = None,
    has_stock: Optional[bool] = None,
    listed_at_period: Optional[str] = None,
    exclude_brands: Optional[List[str]] = Query(None),
    exclude_brands_brackets: Optional[List[str]] = Query(None, alias="exclude_brands[]"),
    exclude_shops: Optional[List[str]] = Query(None),
    exclude_shops_brackets: Optional[List[str]] = Query(None, alias="exclude_shops[]"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get count of products matching filter criteria"""
    query = db.query(FilterPool)

    # 兼容 axios 对数组参数使用 exclude_xxx[] 的情况
    if (not exclude_brands) and exclude_brands_brackets:
        exclude_brands = exclude_brands_brackets
    if (not exclude_shops) and exclude_shops_brackets:
        exclude_shops = exclude_shops_brackets
    
    # Apply filters (same as get_filter_pool，保持联动一致)
    if min_price is not None:
        query = query.filter(FilterPool.price >= min_price)
    if max_price is not None:
        query = query.filter(FilterPool.price <= max_price)
    if min_review_count is not None:
        query = query.filter(FilterPool.review_count >= min_review_count)
    if max_review_count is not None:
        query = query.filter(FilterPool.review_count <= max_review_count)
    if min_rating is not None:
        query = query.filter(FilterPool.rating >= min_rating)
    if max_rating is not None:
        query = query.filter(FilterPool.rating <= max_rating)
    if min_shop_rank is not None:
        query = query.filter(FilterPool.shop_rank >= min_shop_rank)
    if max_shop_rank is not None:
        query = query.filter(FilterPool.shop_rank <= max_shop_rank)
    if min_category_rank is not None:
        query = query.filter(FilterPool.category_rank >= min_category_rank)
    if max_category_rank is not None:
        query = query.filter(FilterPool.category_rank <= max_category_rank)
    if has_stock is not None:
        if has_stock:
            query = query.filter(FilterPool.stock > 0)
        else:
            query = query.filter(or_(FilterPool.stock == 0, FilterPool.stock.is_(None)))

    # 上架日期筛选
    if listed_at_period:
        now = datetime.utcnow()
        start_date = None
        if listed_at_period == "6months":
            start_date = now - timedelta(days=180)
        elif listed_at_period == "1year":
            start_date = now - timedelta(days=365)
        elif listed_at_period == "1.5years":
            start_date = now - timedelta(days=547)

        if start_date:
            query = query.filter(
                or_(
                    and_(
                        FilterPool.listed_at_status == "success",
                        FilterPool.listed_at >= start_date,
                    ),
                    FilterPool.listed_at_status.in_(["not_found", "error"]),
                )
            )

    # 品牌剔除
    if exclude_brands:
        query = query.filter(
            or_(
                FilterPool.brand.is_(None),
                ~FilterPool.brand.in_(exclude_brands),
            )
        )

    # 店铺剔除
    if exclude_shops:
        query = query.filter(
            or_(
                FilterPool.shop_name.is_(None),
                ~FilterPool.shop_name.in_(exclude_shops),
            )
        )
    
    count = query.count()
    return FilterCountResponse(count=count)


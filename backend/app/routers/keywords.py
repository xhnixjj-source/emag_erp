"""Keywords and link library management API"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.keyword import Keyword, KeywordLink, KeywordStatus
from app.models.crawl_task import CrawlTask, TaskType, TaskStatus, TaskPriority, ErrorLog
from app.models.user import User, UserRole
from app.services.task_manager import task_manager
from app.services.operation_log_service import create_operation_log
from app.services.crawler import crawl_keyword_search
from app.services.istoric_preturi_client import get_listed_at_via_browser
from app.utils.playwright_manager import get_playwright_pool
from app.utils.bitbrowser_manager import bitbrowser_manager
from app.config import config
import time

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
    review_count: Optional[int] = None  # 评论数
    rating: Optional[float] = None  # 评分
    crawled_at: datetime
    status: str
    # Chrome 插件扩展字段
    product_title: Optional[str] = None  # 产品标题
    brand: Optional[str] = None  # 品牌
    category: Optional[str] = None  # 类目
    commission_rate: Optional[float] = None  # 佣金比例(%)
    offer_count: Optional[int] = None  # 跟卖数
    purchase_price: Optional[float] = None  # 采购价
    last_offer_period: Optional[str] = None  # 最近offer周期
    tag: Optional[str] = None  # 标签
    source: Optional[str] = "keyword_search"  # 来源
    # 上架日期相关字段
    listed_at: Optional[datetime] = None  # 上架日期
    listed_at_status: Optional[str] = None  # 上架日期获取状态
    listed_at_error_type: Optional[str] = None  # 上架日期获取错误类型

    class Config:
        from_attributes = True

class ChromeExtensionLinkItem(BaseModel):
    """Chrome 插件提交的单条链接数据"""
    brand: Optional[str] = None
    category: Optional[str] = None
    commission_rate: Optional[float] = None
    image_url: Optional[str] = None
    keyword: str
    last_offer_period: Optional[str] = None
    min_price: Optional[float] = None
    offer_count: Optional[int] = None
    pnk: Optional[str] = None
    product_title: Optional[str] = None
    product_url: str
    purchase_price: Optional[float] = None
    scraped_at: Optional[str] = None
    tag: Optional[str] = None

class ChromeExtensionLinksRequest(BaseModel):
    """Chrome 插件提交链接请求 - 支持单条或批量"""
    items: List[ChromeExtensionLinkItem]

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

@router.get("/brands")
async def get_brands(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get distinct brands from keyword links"""
    from sqlalchemy import distinct, func
    brands = db.query(distinct(KeywordLink.brand)).filter(
        KeywordLink.brand.isnot(None),
        KeywordLink.brand != ""
    ).order_by(KeywordLink.brand).all()
    # Extract brand strings from tuples
    brand_list = [brand[0] for brand in brands if brand[0]]
    
    # #region agent log
    try:
        import json as _json_debug, time as _time_debug
        _entry = {
            "id": f"brands_{int(_time_debug.time() * 1000)}",
            "timestamp": int(_time_debug.time() * 1000),
            "location": "keywords.py:get_brands",
            "message": "brands query result",
            "data": {
                "count": len(brand_list),
                "sample": brand_list[:5]
            },
            "runId": "pre-fix-1",
            "hypothesisId": "H1,H2"
        }
        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
            _f.write(_json_debug.dumps(_entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass
    # #endregion
    
    return {"brands": brand_list}

@router.get("/links")
async def get_keyword_links(
    keyword_id: Optional[int] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    review_count_min: Optional[int] = None,
    review_count_max: Optional[int] = None,
    rating_min: Optional[float] = None,
    rating_max: Optional[float] = None,
    crawled_at_start: Optional[str] = None,
    crawled_at_end: Optional[str] = None,
    source: Optional[str] = None,
    tag: Optional[str] = None,
    offer_count_min: Optional[int] = None,
    offer_count_max: Optional[int] = None,
    listed_at_period: Optional[str] = None,
    # 前端 axios 可能会以 exclude_brands[]=x&exclude_brands[]=y 形式传参，这里同时兼容两种写法
    exclude_brands: Optional[List[str]] = Query(None),
    exclude_brands_brackets: Optional[List[str]] = Query(None, alias="exclude_brands[]")
):
    """Get keyword links with optional filters"""
    # #region agent log
    import time as _time_query
    import json as _json_query
    import traceback as _traceback
    _query_start_time = _time_query.time()
    _query_debug_log_path = r"d:\emag_erp\.cursor\debug.log"
    def _dbg_query(location, message, data=None, hypothesis=""):
        try:
            entry = {
                "timestamp": int(_time_query.time() * 1000),
                "location": location,
                "message": message,
                "data": data or {},
                "hypothesisId": hypothesis,
                "runId": "debug-500-error"
            }
            with open(_query_debug_log_path, "a", encoding="utf-8") as _f:
                _f.write(_json_query.dumps(entry, ensure_ascii=False, default=str) + "\n")
        except Exception:
            pass
    try:
        _dbg_query("keywords.py:query_start", "查询请求开始", {
            "user_id": current_user["id"],
            "db_session_id": id(db),
            "skip": skip,
            "limit": limit,
            "keyword_id": keyword_id,
            "filters": {
                "price_min": price_min, "price_max": price_max,
                "review_count_min": review_count_min, "review_count_max": review_count_max,
                "rating_min": rating_min, "rating_max": rating_max,
                "source": source, "tag": tag,
                "offer_count_min": offer_count_min, "offer_count_max": offer_count_max,
                "listed_at_period": listed_at_period,
                "exclude_brands": exclude_brands,
                "exclude_brands_brackets": exclude_brands_brackets,
            }
        }, "H1,H2,H3,H4,H5")
    except Exception as _log_err:
        pass
    # #endregion
    
    try:
        query = db.query(KeywordLink)
        # 兼容 axios 对数组参数使用 exclude_brands[] 的情况
        if (not exclude_brands) and exclude_brands_brackets:
            exclude_brands = exclude_brands_brackets

        if keyword_id:
            query = query.filter(KeywordLink.keyword_id == keyword_id)
        if price_min is not None:
            query = query.filter(KeywordLink.price >= price_min)
        if price_max is not None:
            query = query.filter(KeywordLink.price <= price_max)
        if review_count_min is not None:
            query = query.filter(KeywordLink.review_count >= review_count_min)
        if review_count_max is not None:
            query = query.filter(KeywordLink.review_count <= review_count_max)
        if rating_min is not None:
            query = query.filter(KeywordLink.rating >= rating_min)
        if rating_max is not None:
            query = query.filter(KeywordLink.rating <= rating_max)
        if source:
            query = query.filter(KeywordLink.source == source)
        if tag:
            query = query.filter(KeywordLink.tag == tag)
        if crawled_at_start:
            try:
                start_date = datetime.fromisoformat(crawled_at_start.replace('Z', '+00:00'))
                query = query.filter(KeywordLink.crawled_at >= start_date)
            except ValueError:
                pass
        if crawled_at_end:
            try:
                end_date = datetime.fromisoformat(crawled_at_end.replace('Z', '+00:00'))
                query = query.filter(KeywordLink.crawled_at <= end_date)
            except ValueError:
                pass
        
        # 跟卖数筛选
        if offer_count_min is not None:
            query = query.filter(KeywordLink.offer_count >= offer_count_min)
        if offer_count_max is not None:
            query = query.filter(KeywordLink.offer_count <= offer_count_max)
        
        # 上架日期筛选
        if listed_at_period:
            from datetime import timedelta
            from sqlalchemy import or_, and_
            now = datetime.utcnow()
            
            # 计算时间范围起始日期
            start_date = None
            if listed_at_period == "6months":
                # 使用 timedelta 近似 6 个月（约 180 天）
                start_date = now - timedelta(days=180)
            elif listed_at_period == "1year":
                # 使用 timedelta 近似 1 年（365 天）
                start_date = now - timedelta(days=365)
            elif listed_at_period == "1.5years":
                # 使用 timedelta 近似 1.5 年（约 547 天）
                start_date = now - timedelta(days=547)
            
            # 筛选逻辑：包含成功获取上架日期且在时间范围内的记录，以及去爬过但没有爬取到的记录（not_found/error）
            if start_date:
                query = query.filter(
                    or_(
                        # 成功获取上架日期且在时间范围内
                        and_(
                            KeywordLink.listed_at_status == 'success',
                            KeywordLink.listed_at >= start_date
                        ),
                        # 去爬过但没有爬取到的记录（not_found 或 error，不包括 pending）
                        KeywordLink.listed_at_status.in_(['not_found', 'error'])
                    )
                )
        
        # 品牌剔除筛选
        if exclude_brands:
            from sqlalchemy import or_
            # 排除指定品牌，同时保留brand为NULL的记录（因为NULL品牌不应该被排除）
            query = query.filter(
                or_(
                    KeywordLink.brand.is_(None),
                    ~KeywordLink.brand.in_(exclude_brands)
                )
            )
        
        # #region agent log
        _time_before_count = _time_query.time()
        _dbg_query("keywords.py:before_count", "准备执行count查询", {
            "elapsed_since_start": round(_time_before_count - _query_start_time, 3)
        }, "H2,H4")
        # #endregion
        
        # 获取总数
        total = query.count()
        
        # #region agent log
        _time_after_count = _time_query.time()
        _dbg_query("keywords.py:after_count", "count查询完成", {
            "total": total,
            "count_query_duration_s": round(_time_after_count - _time_before_count, 3),
            "elapsed_since_start": round(_time_after_count - _query_start_time, 3)
        }, "H2,H4")
        # #endregion
        
        # #region agent log
        _time_before_fetch = _time_query.time()
        _dbg_query("keywords.py:before_fetch", "准备获取分页数据", {
            "elapsed_since_start": round(_time_before_fetch - _query_start_time, 3)
        }, "H2,H4")
        # #endregion
        
        # 获取分页数据
        links = query.order_by(KeywordLink.crawled_at.desc()).offset(skip).limit(limit).all()
        
        # #region agent log
        _time_after_fetch = _time_query.time()
        try:
            _sample_link = links[0] if links else None
            _sample_dict = {}
            if _sample_link:
                try:
                    _sample_dict = {
                        "id": _sample_link.id,
                        "has_keyword_relation": hasattr(_sample_link, 'keyword'),
                        "keyword_relation_type": str(type(_sample_link.keyword)) if hasattr(_sample_link, 'keyword') else None,
                        "listed_at_type": str(type(_sample_link.listed_at)) if _sample_link.listed_at else None,
                        "crawled_at_type": str(type(_sample_link.crawled_at)) if _sample_link.crawled_at else None
                    }
                except Exception as _sample_err:
                    _sample_dict = {"error": str(_sample_err)}
            _dbg_query("keywords.py:after_fetch", "分页数据获取完成", {
                "items_count": len(links),
                "fetch_duration_s": round(_time_after_fetch - _time_before_fetch, 3),
                "total_duration_s": round(_time_after_fetch - _query_start_time, 3),
                "sample_link_info": _sample_dict
            }, "H1,H2,H4")
        except Exception as _log_err:
            pass
        # #endregion
        
        # #region agent log
        _time_before_serialize = _time_query.time()
        try:
            _dbg_query("keywords.py:before_serialize", "准备序列化返回数据", {
                "links_count": len(links),
                "links_type": str(type(links))
            }, "H1,H2,H4")
        except Exception:
            pass
        # #endregion
        
        # 手动序列化 SQLAlchemy 对象，避免关系对象序列化问题
        try:
            # #region agent log
            try:
                _dbg_query("keywords.py:serialize_start", "开始序列化", {
                    "links_count": len(links)
                }, "H1,H2,H4")
            except Exception:
                pass
            # #endregion
            
            serialized_links = []
            for idx, link in enumerate(links):
                try:
                    # #region agent log
                    if idx == 0 or idx % 20 == 0:
                        try:
                            _dbg_query("keywords.py:serialize_link", "序列化链接", {
                                "link_index": idx,
                                "link_id": getattr(link, 'id', None)
                            }, "H1,H2,H4")
                        except Exception:
                            pass
                    # #endregion
                    
                    # 直接手动构建字典，避免关系对象序列化问题
                    # 安全处理日期时间字段
                    def safe_isoformat(dt):
                        if dt is None:
                            return None
                        try:
                            if isinstance(dt, datetime):
                                return dt.isoformat()
                            return str(dt)
                        except Exception:
                            return None
                    
                    link_dict = {
                        "id": link.id,
                        "keyword_id": link.keyword_id,
                        "product_url": link.product_url,
                        "pnk_code": link.pnk_code,
                        "thumbnail_image": link.thumbnail_image,
                        "price": link.price,
                        "review_count": link.review_count,
                        "rating": link.rating,
                        "crawled_at": safe_isoformat(link.crawled_at),
                        "status": link.status,
                        "product_title": link.product_title,
                        "brand": link.brand,
                        "category": link.category,
                        "commission_rate": link.commission_rate,
                        "offer_count": link.offer_count,
                        "purchase_price": link.purchase_price,
                        "last_offer_period": link.last_offer_period,
                        "tag": link.tag,
                        "source": link.source,
                        "listed_at": safe_isoformat(link.listed_at),
                        "listed_at_status": link.listed_at_status,
                        "listed_at_error_type": link.listed_at_error_type
                    }
                    serialized_links.append(link_dict)
                except Exception as link_err:
                    # #region agent log
                    try:
                        _dbg_query("keywords.py:serialize_link_error", "序列化单个链接失败", {
                            "link_id": getattr(link, 'id', None),
                            "error": str(link_err),
                            "error_type": type(link_err).__name__,
                            "traceback": _traceback.format_exc()
                        }, "H1,H2,H4")
                    except Exception:
                        pass
                    # #endregion
                    logger.error(f"序列化链接 {getattr(link, 'id', 'unknown')} 失败: {link_err}", exc_info=True)
                    raise
            
            # #region agent log
            _time_after_serialize = _time_query.time()
            try:
                _dbg_query("keywords.py:after_serialize", "序列化完成", {
                    "serialized_count": len(serialized_links),
                    "serialize_duration_s": round(_time_after_serialize - _time_before_serialize, 3)
                }, "H1,H2,H4")
            except Exception:
                pass
            # #endregion
            
            return {
                "items": serialized_links,
                "total": total,
                "skip": skip,
                "limit": limit
            }
        except Exception as serialize_err:
            # #region agent log
            try:
                _dbg_query("keywords.py:serialize_error", "序列化过程发生异常", {
                    "error": str(serialize_err),
                    "error_type": type(serialize_err).__name__,
                    "traceback": _traceback.format_exc()
                }, "H1,H2,H4,H5")
            except Exception:
                pass
            # #endregion
            logger.error(f"序列化链接数据失败: {serialize_err}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"序列化数据失败: {str(serialize_err)}")
    except Exception as query_err:
        # #region agent log
        try:
            _dbg_query("keywords.py:query_execution_error", "查询执行过程发生异常", {
                "error": str(query_err),
                "error_type": type(query_err).__name__,
                "traceback": _traceback.format_exc()
            }, "H3,H5")
        except Exception:
            pass
        # #endregion
        logger.error(f"查询链接数据失败: {query_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"查询数据失败: {str(query_err)}")

@router.post("/links/chrome-extension")
async def import_chrome_extension_links(
    request: ChromeExtensionLinksRequest,
    db: Session = Depends(get_db)
):
    """
    Chrome 插件提交链接到链接初筛（无需认证，局域网直接调用）
    - 如果关键字已存在，则新建一个 'keyword super hot' 的关键字以示区别
    - 将链接数据写入 keyword_links 表，标记来源为 chrome_extension
    """
    # 获取默认用户（第一个管理员用户）
    default_user = db.query(User).filter(User.role == UserRole.ADMIN).first()
    if not default_user:
        default_user = db.query(User).first()
    if not default_user:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="系统中没有可用用户，请先创建用户"
        )
    user_id = default_user.id
    
    logger.info(f"[Chrome插件] 收到链接导入请求 - 默认用户ID: {user_id}, 链接数: {len(request.items)}")
    
    created_count = 0
    skipped_count = 0
    keyword_cache = {}  # 缓存已查找/创建的关键字，避免重复查询
    
    try:
        for item in request.items:
            # 检查是否已存在相同 product_url 的链接（去重）
            existing_link = db.query(KeywordLink).filter(
                KeywordLink.product_url == item.product_url
            ).first()
            if existing_link:
                skipped_count += 1
                logger.debug(f"[Chrome插件] 跳过已存在的链接: {item.product_url}")
                continue
            
            # 查找或创建关键字
            keyword_str = item.keyword.strip()
            if keyword_str in keyword_cache:
                keyword_obj = keyword_cache[keyword_str]
            else:
                # 检查数据库中是否已有该关键字（不限用户）
                existing_keyword = db.query(Keyword).filter(
                    Keyword.keyword == keyword_str
                ).first()
                
                if existing_keyword:
                    # 关键字已存在，使用 "keyword super hot" 作为新关键字名
                    super_hot_keyword_str = f"{keyword_str} super hot"
                    # 检查 super hot 版本是否也已存在
                    existing_super_hot = db.query(Keyword).filter(
                        Keyword.keyword == super_hot_keyword_str
                    ).first()
                    
                    if existing_super_hot:
                        keyword_obj = existing_super_hot
                    else:
                        keyword_obj = Keyword(
                            keyword=super_hot_keyword_str,
                            created_by_user_id=user_id,
                            status=KeywordStatus.COMPLETED
                        )
                        db.add(keyword_obj)
                        db.flush()  # 获取 ID
                        logger.info(f"[Chrome插件] 创建新关键字: '{super_hot_keyword_str}' (原关键字 '{keyword_str}' 已存在)")
                else:
                    # 关键字不存在，直接创建
                    keyword_obj = Keyword(
                        keyword=keyword_str,
                        created_by_user_id=user_id,
                        status=KeywordStatus.COMPLETED
                    )
                    db.add(keyword_obj)
                    db.flush()
                    logger.info(f"[Chrome插件] 创建新关键字: '{keyword_str}'")
                
                keyword_cache[keyword_str] = keyword_obj
            
            # 处理 scraped_at 时间
            crawled_at = None
            if item.scraped_at:
                try:
                    crawled_at = datetime.fromisoformat(item.scraped_at.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    crawled_at = None
            
            # 创建链接记录
            link = KeywordLink(
                keyword_id=keyword_obj.id,
                product_url=item.product_url,
                pnk_code=item.pnk,
                thumbnail_image=item.image_url,
                price=item.min_price,
                product_title=item.product_title,
                brand=item.brand,
                category=item.category,
                commission_rate=item.commission_rate,
                offer_count=item.offer_count,
                purchase_price=item.purchase_price,
                last_offer_period=item.last_offer_period,
                tag=item.tag,
                source="chrome_extension",
                status="active"
            )
            if crawled_at:
                link.crawled_at = crawled_at
            
            db.add(link)
            created_count += 1
        
        db.commit()
        
        # 记录操作日志
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="chrome_extension_import",
            target_type="keyword_link",
            operation_detail={
                "created_count": created_count,
                "skipped_count": skipped_count,
                "total_items": len(request.items)
            }
        )
        
        logger.info(f"[Chrome插件] 导入完成 - 创建: {created_count}, 跳过(已存在): {skipped_count}")
        
        return {
            "success": True,
            "created_count": created_count,
            "skipped_count": skipped_count,
            "total_items": len(request.items),
            "message": f"成功导入 {created_count} 条链接，跳过 {skipped_count} 条已存在链接"
        }
    
    except Exception as e:
        db.rollback()
        logger.error(f"[Chrome插件] 导入失败: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导入失败: {str(e)}"
        )


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


class BatchGetListedAtRequest(BaseModel):
    """Batch get listed at request model"""
    link_ids: List[int]


def _process_batch_get_listed_at(link_ids: List[int], user_id: int):
    """后台任务：批量获取上架日期"""
    from app.database import SessionLocal
    
    db = SessionLocal()
    try:
        logger.info(f"[批量获取上架日期] 后台任务开始 - 用户ID: {user_id}, 链接数: {len(link_ids)}")
        
        # 获取链接
        links = db.query(KeywordLink).filter(
            KeywordLink.id.in_(link_ids)
        ).all()
        
        if not links:
            logger.warning(f"[批量获取上架日期] 没有找到链接 - 用户ID: {user_id}, 链接ID: {link_ids}")
            return
        
        # #region agent log
        import json as _json_debug
        import time as _time_debug
        _debug_log_path = r"d:\emag_erp\.cursor\debug.log"
        def _dbg(location, message, data=None, hypothesis=""):
            try:
                entry = {
                    "timestamp": int(_time_debug.time() * 1000),
                    "location": location,
                    "message": message,
                    "data": data or {},
                    "hypothesisId": hypothesis,
                    "runId": "batch-listed-at-debug"
                }
                with open(_debug_log_path, "a", encoding="utf-8") as _f:
                    _f.write(_json_debug.dumps(entry, ensure_ascii=False, default=str) + "\n")
            except Exception:
                pass
        # #endregion
        
        success_count = 0
        error_count = 0
        skipped_count = 0
        request_times = []
        consecutive_errors = 0
        commit_interval = 5
        
        # 获取 Playwright 上下文池
        playwright_pool = get_playwright_pool()
        context = None
        page = None
        
        # 获取浏览器上下文（优先使用 BitBrowser，否则使用无代理模式）
        try:
            if config.BITBROWSER_ENABLED:
                window_info = bitbrowser_manager.acquire_window()
                if window_info:
                    context = playwright_pool.acquire_context(
                        cdp_url=window_info['ws'],
                        window_id=window_info['id'],
                    )
                    logger.info(f"[批量获取上架日期] 使用 BitBrowser 窗口: {window_info['id']}")
            else:
                # 不使用代理，直接创建浏览器上下文（模拟人工点击）
                context = playwright_pool.acquire_context(proxy=None)
                logger.info(f"[批量获取上架日期] 使用无代理浏览器上下文（模拟人工点击）")
            
            if not context:
                raise RuntimeError("无法获取浏览器上下文")
            
            # 创建页面
            page = context.new_page()
            logger.info(f"[批量获取上架日期] 浏览器页面创建成功")
            
            # #region agent log
            _dbg("keywords.py:batch_start", "批量处理开始", {
                "total_links": len(links),
                "commit_interval": commit_interval,
                "db_session_id": id(db),
                "browser_context_acquired": context is not None,
                "page_created": page is not None
            }, "H1,H3")
            # #endregion
            
            for idx, link in enumerate(links):
                # #region agent log
                _dbg("keywords.py:loop_iteration_start", "循环迭代开始", {
                    "index": idx + 1,
                    "total": len(links),
                    "url": link.product_url
                }, "H1,H5")
                # #endregion
                
                try:
                    # #region agent log
                    _dbg("keywords.py:batch_start_item", "开始处理链接", {
                        "index": idx + 1,
                        "total": len(links),
                        "url": link.product_url,
                        "consecutive_errors": consecutive_errors,
                        "time_since_last": request_times[-1] - request_times[-2] if len(request_times) >= 2 else None
                    }, "H1,H5")
                    # #endregion
                    
                    # 如果已经有成功的上架日期，跳过
                    if link.listed_at_status == 'success' and link.listed_at:
                        skipped_count += 1
                        continue
                    
                    # #region agent log
                    current_time = _time_debug.time()
                    request_times.append(current_time)
                    if len(request_times) > 1:
                        delay_since_last = current_time - request_times[-2]
                        _dbg("keywords.py:request_timing", "请求时间间隔", {
                            "delay_since_last_s": round(delay_since_last, 2),
                            "request_index": idx + 1
                        }, "H1")
                    # #endregion
                    
                    # 使用浏览器方式获取上架日期（模拟人工点击）
                    try:
                        listed_at = get_listed_at_via_browser(page, link.product_url)
                    except Exception as api_err:
                        # 如果 get_listed_at 抛出异常，记录并继续
                        error_count += 1
                        consecutive_errors += 1
                        error_type = 'unknown'
                        error_str = str(api_err).lower()
                        if 'timeout' in error_str or 'timed out' in error_str:
                            error_type = 'timeout'
                        elif 'http' in error_str or 'status' in error_str:
                            error_type = 'http_error'
                        elif 'parse' in error_str or 'json' in error_str:
                            error_type = 'parse_failed'
                        
                        link.listed_at_status = 'error'
                        link.listed_at_error_type = error_type
                        logger.warning(f"[批量获取上架日期] API调用异常 - URL: {link.product_url}, 错误: {api_err}")
                        # #region agent log
                        _dbg("keywords.py:api_exception", "API调用异常", {
                            "url": link.product_url,
                            "error": str(api_err)[:200],
                            "error_type": error_type,
                            "consecutive_errors": consecutive_errors
                        }, "H1,H4,H5")
                        # #endregion
                        listed_at = None
                    
                    if listed_at:
                        link.listed_at = listed_at
                        link.listed_at_status = 'success'
                        link.listed_at_error_type = None
                        success_count += 1
                        consecutive_errors = 0
                        logger.info(f"[批量获取上架日期] 成功 - URL: {link.product_url}, 上架日期: {listed_at.isoformat()}")
                        # #region agent log
                        _dbg("keywords.py:success", "获取上架日期成功", {
                            "url": link.product_url,
                            "listed_at": listed_at.isoformat(),
                            "consecutive_errors_reset": True
                        }, "H5")
                        # #endregion
                    else:
                        link.listed_at_status = 'not_found'
                        link.listed_at_error_type = None
                        error_count += 1
                        consecutive_errors += 1
                        logger.info(f"[批量获取上架日期] 未找到 - URL: {link.product_url}")
                        # #region agent log
                        _dbg("keywords.py:not_found", "未找到上架日期", {
                            "url": link.product_url,
                            "consecutive_errors": consecutive_errors
                        }, "H5")
                        # #endregion
                    
                    # 动态延迟
                    base_delay = 0.5
                    if consecutive_errors > 0:
                        delay = base_delay * (1 + consecutive_errors * 0.5)
                        delay = min(delay, 5.0)
                    else:
                        delay = base_delay
                    
                    # #region agent log
                    _dbg("keywords.py:delay_before_next", "延迟设置", {
                        "delay_s": round(delay, 2),
                        "consecutive_errors": consecutive_errors,
                        "base_delay": base_delay
                    }, "H1,H5")
                    # #endregion
                    
                    time.sleep(delay)
                    
                    # #region agent log
                    _dbg("keywords.py:after_sleep", "延迟完成，准备检查提交", {
                        "index": idx + 1,
                        "total": len(links),
                        "should_commit": (idx + 1) % commit_interval == 0 or (idx + 1) == len(links),
                        "commit_interval": commit_interval
                    }, "H1,H3")
                    # #endregion
                    
                    # 定期提交
                    if (idx + 1) % commit_interval == 0 or (idx + 1) == len(links):
                        # #region agent log
                        _dbg("keywords.py:periodic_commit", "定期提交数据库", {
                            "processed_count": idx + 1,
                            "total": len(links),
                            "success_count": success_count,
                            "error_count": error_count
                        }, "H1,H3")
                        # #endregion
                        try:
                            db.commit()
                            # #region agent log
                            _dbg("keywords.py:commit_success", "数据库提交成功", {
                                "processed_count": idx + 1
                            }, "H1,H3")
                            # #endregion
                        except Exception as commit_err:
                            # #region agent log
                            _dbg("keywords.py:commit_error", "数据库提交失败", {
                                "error": str(commit_err)[:200],
                                "processed_count": idx + 1
                            }, "H1,H3")
                            # #endregion
                            db.rollback()
                            raise
                
                except Exception as e:
                    error_count += 1
                    consecutive_errors += 1
                    error_type = 'unknown'
                    error_str = str(e).lower()
                    if 'timeout' in error_str or 'timed out' in error_str:
                        error_type = 'timeout'
                    elif 'http' in error_str or 'status' in error_str:
                        error_type = 'http_error'
                    elif 'parse' in error_str or 'json' in error_str:
                        error_type = 'parse_failed'
                    
                    link.listed_at_status = 'error'
                    link.listed_at_error_type = error_type
                    logger.warning(f"[批量获取上架日期] 失败 - URL: {link.product_url}, 错误: {e}")
                    
                    # #region agent log
                    _dbg("keywords.py:exception_caught", "捕获到异常", {
                        "index": idx + 1,
                        "url": link.product_url,
                        "error": str(e)[:200],
                        "error_type": type(e).__name__
                    }, "H1,H4,H5")
                    # #endregion
                    
                    if consecutive_errors >= 3:
                        extra_delay = min(consecutive_errors * 2, 10)
                        # #region agent log
                        _dbg("keywords.py:backoff_delay", "连续错误过多，增加延迟", {
                            "consecutive_errors": consecutive_errors,
                            "extra_delay_s": extra_delay
                        }, "H5")
                        # #endregion
                        time.sleep(extra_delay)
                    else:
                        time.sleep(0.5)
                    
                    # 定期提交（异常情况下）
                    if (idx + 1) % commit_interval == 0 or (idx + 1) == len(links):
                        # #region agent log
                        _dbg("keywords.py:periodic_commit_exception", "异常后定期提交数据库", {
                            "processed_count": idx + 1,
                            "total": len(links),
                            "success_count": success_count,
                            "error_count": error_count
                        }, "H1,H3")
                        # #endregion
                        try:
                            db.commit()
                            # #region agent log
                            _dbg("keywords.py:commit_success_exception", "异常后数据库提交成功", {
                                "processed_count": idx + 1
                            }, "H1,H3")
                            # #endregion
                        except Exception as commit_err:
                            # #region agent log
                            _dbg("keywords.py:commit_error_exception", "异常后数据库提交失败", {
                                "error": str(commit_err)[:200],
                                "processed_count": idx + 1
                            }, "H1,H3")
                            # #endregion
                            db.rollback()
                            logger.error(f"[批量获取上架日期] 提交失败，继续处理: {commit_err}")
                
                # #region agent log
                _dbg("keywords.py:loop_iteration_end", "循环迭代结束", {
                    "index": idx + 1,
                    "total": len(links),
                    "url": link.product_url,
                    "success_count": success_count,
                    "error_count": error_count,
                    "skipped_count": skipped_count
                }, "H1,H5")
                # #endregion
            
            # #region agent log
            _dbg("keywords.py:loop_completed", "循环处理完成", {
                "total_processed": len(links),
                "success_count": success_count,
                "error_count": error_count,
                "skipped_count": skipped_count
            }, "H1,H3")
            # #endregion
            
        except Exception as ctx_err:
            logger.error(f"[批量获取上架日期] 获取浏览器上下文失败: {ctx_err}", exc_info=True)
            # #region agent log
            _dbg("keywords.py:browser_context_error", "浏览器上下文获取失败", {
                "error": str(ctx_err)[:200],
                "error_type": type(ctx_err).__name__
            }, "H1,H3")
            # #endregion
            return
        finally:
            # 清理浏览器资源
            try:
                if page:
                    page.close()
                    logger.info(f"[批量获取上架日期] 浏览器页面已关闭")
                if context:
                    playwright_pool.release_context(context)
                    logger.info(f"[批量获取上架日期] 浏览器上下文已释放")
            except Exception as cleanup_err:
                logger.error(f"[批量获取上架日期] 清理浏览器资源失败: {cleanup_err}", exc_info=True)
        
        # 最终提交
        try:
            db.commit()
            # #region agent log
            _dbg("keywords.py:final_commit", "最终提交数据库", {
                "total_processed": len(links)
            }, "H1,H3")
            # #endregion
        except Exception as commit_err:
            # #region agent log
            _dbg("keywords.py:final_commit_error", "最终提交失败", {
                "error": str(commit_err)[:200]
            }, "H1,H3")
            # #endregion
            db.rollback()
            raise
        
        # 记录操作日志
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="batch_get_listed_at",
            target_type="keyword_link",
            operation_detail={
                "total": len(links),
                "success_count": success_count,
                "error_count": error_count,
                "skipped_count": skipped_count
            }
        )
        
        logger.info(f"[批量获取上架日期] 后台任务完成 - 用户ID: {user_id}, 成功: {success_count}, 失败: {error_count}, 跳过: {skipped_count}")
        
    except Exception as e:
        logger.error(f"[批量获取上架日期] 后台任务失败: {str(e)}", exc_info=True)
        if db:
            db.rollback()
    finally:
        if db:
            db.close()


@router.post("/links/batch-get-listed-at")
async def batch_get_listed_at(
    request: BatchGetListedAtRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """批量获取上架日期（异步后台任务）"""
    logger.info(f"[批量获取上架日期] 接收请求 - 用户ID: {current_user['id']}, 链接数: {len(request.link_ids)}")
    
    try:
        # 验证链接是否存在
        links = db.query(KeywordLink).filter(
            KeywordLink.id.in_(request.link_ids)
        ).limit(1).all()
        
        if not links:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="没有找到链接"
            )
        
        # 添加到后台任务
        background_tasks.add_task(
            _process_batch_get_listed_at,
            request.link_ids,
            current_user["id"]
        )
        
        logger.info(f"[批量获取上架日期] 已添加到后台任务 - 用户ID: {current_user['id']}, 链接数: {len(request.link_ids)}")
        
        return {
            "success": True,
            "message": f"已开始批量获取 {len(request.link_ids)} 个链接的上架日期，处理将在后台进行",
            "total": len(request.link_ids)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[批量获取上架日期] 请求处理失败: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"启动批量获取上架日期失败: {str(e)}"
        )


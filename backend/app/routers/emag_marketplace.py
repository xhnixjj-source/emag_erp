import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload

from app.middleware.auth_middleware import require_auth
from app.services.emag_marketplace_login_service import emag_marketplace_login_service
from app.services.emag_ads_service import sync_ads_data, query_ads_performance
from app.database import get_db, SessionLocal
from app.models.emag_sync import EmagInboundShipment, EmagInboundShipmentDetail

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/emag-marketplace", tags=["emag-marketplace"])


class MarketplaceLoginRequest(BaseModel):
    username: Optional[str] = None  # 手动登录方式，不需要用户名密码
    password: Optional[str] = None


class MarketplaceSmsCodeRequest(BaseModel):
    code: str


class AdsSyncRequest(BaseModel):
    date_start: str   # YYYY-MM-DD
    date_end: str     # YYYY-MM-DD
    marketplace: str = "ro"  # ro / bg / hu


@router.post("/login")
async def marketplace_login(
    payload: MarketplaceLoginRequest,
    current_user: dict = Depends(require_auth),
):
    """
    启动 eMAG 后台登录（独立线程执行）。
    必须用 threading.Thread 而非 background_tasks，
    因为 sync_playwright() 不能在 asyncio 事件循环中调用。
    """

    def _run():
        try:
            emag_marketplace_login_service.login(payload.username, payload.password)
        except Exception as e:
            logger.error("marketplace login background task failed: %s", e, exc_info=True)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"success": True, "message": "login_started", **emag_marketplace_login_service.get_login_status()}


@router.get("/login-status")
async def marketplace_login_status(
    current_user: dict = Depends(require_auth),
):
    """查询当前登录状态（轮询用）"""
    return {"success": True, **emag_marketplace_login_service.get_login_status()}


@router.post("/captcha-done")
async def marketplace_captcha_done(
    current_user: dict = Depends(require_auth),
):
    """用户在弹窗浏览器内手动完成验证码后调用"""
    return {"success": True, **emag_marketplace_login_service.captcha_done()}


@router.post("/sms-code")
async def marketplace_submit_sms_code(
    payload: MarketplaceSmsCodeRequest,
    current_user: dict = Depends(require_auth),
):
    """提交手机验证码"""
    return {"success": True, **emag_marketplace_login_service.submit_sms_code(payload.code)}


@router.post("/logout")
async def marketplace_logout(
    current_user: dict = Depends(require_auth),
):
    """注销并关闭浏览器会话"""
    emag_marketplace_login_service.logout()
    return {"success": True, "status": "not_logged_in"}


@router.post("/inbound-shipments/sync")
async def sync_inbound_shipments(
    current_user: dict = Depends(require_auth),
):
    """
    同步入仓运单（独立线程执行，自动翻页）
    返回所有运单 ID 的拼接字符串，记录到日志
    """
    
    def _run():
        try:
            # 设置同步状态为进行中
            with emag_marketplace_login_service._lock:
                emag_marketplace_login_service._sync_status = "syncing"
            
            result = emag_marketplace_login_service.fetch_inbound_shipments_all_pages()
            # 记录到操作日志
            logger.info(f"入仓运单同步完成：共 {len(result['all_ids'])} 条")
            logger.info(f"运单 ID 列表: {result['ids_string']}")
        except Exception as e:
            logger.error(f"入仓运单同步失败: {e}", exc_info=True)
            with emag_marketplace_login_service._lock:
                emag_marketplace_login_service._sync_status = "error"
                emag_marketplace_login_service._last_sync_result = {"error": str(e)}
    
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {
        "success": True,
        "message": "入仓运单同步已启动，正在后台进行..."
    }


@router.get("/inbound-shipments/sync-status")
async def get_inbound_shipments_sync_status(
    current_user: dict = Depends(require_auth),
):
    """查询入仓运单同步状态和结果"""
    return {
        "success": True,
        **emag_marketplace_login_service.get_sync_status()
    }


@router.post("/inbound-shipments/sync-details")
async def sync_inbound_shipments_details(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """
    同步 finalized 运单详情到数据库（独立线程执行）
    流程：
    1. 获取 finalized 状态的运单列表
    2. 遍历每个运单，获取详情
    3. 存入数据库
    """
    
    def _run():
        try:
            result = emag_marketplace_login_service.sync_finalized_shipments_to_db(db)
            logger.info(f"运单详情同步完成: {result}")
        except Exception as e:
            logger.error(f"运单详情同步失败: {e}", exc_info=True)
    
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {
        "success": True,
        "message": "运单详情同步已启动，正在后台进行..."
    }


@router.get("/inbound-shipments")
async def get_inbound_shipments(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    reception_id: Optional[int] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """查询已同步的入仓运单列表（含详情展开）"""
    query = db.query(EmagInboundShipment)

    if reception_id is not None:
        query = query.filter(EmagInboundShipment.reception_id == reception_id)
    if status_filter:
        query = query.filter(EmagInboundShipment.status == status_filter)

    total = query.count()
    shipments = (
        query
        .options(joinedload(EmagInboundShipment.details))
        .order_by(EmagInboundShipment.reception_id.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    items = []
    for s in shipments:
        detail_list = []
        for d in s.details:
            detail_list.append({
                "id": d.id,
                "vendor_product_id": d.vendor_product_id,
                "transferred_to_storage_quantity": d.transferred_to_storage_quantity,
                "expiration_date": d.expiration_date.isoformat() if d.expiration_date else None,
                "producer_lot": d.producer_lot,
                "synced_at": d.synced_at.isoformat() if d.synced_at else None,
            })
        items.append({
            "id": s.id,
            "reception_id": s.reception_id,
            "status": s.status,
            "detail_count": len(detail_list),
            "total_quantity": sum(d["transferred_to_storage_quantity"] for d in detail_list),
            "synced_at": s.synced_at.isoformat() if s.synced_at else None,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "details": detail_list,
        })

    return {"items": items, "total": total, "skip": skip, "limit": limit}


# ---------------------------------------------------------------------------
# 广告数据同步
# ---------------------------------------------------------------------------

@router.post("/ads/sync")
async def sync_ads(
    payload: AdsSyncRequest,
    current_user: dict = Depends(require_auth),
):
    """
    启动广告数据三层递归同步（独立线程执行）。
    Campaign → Adset → Product Performance
    """

    def _run():
        db = SessionLocal()
        try:
            result = sync_ads_data(
                db=db,
                date_start=payload.date_start,
                date_end=payload.date_end,
                login_service=emag_marketplace_login_service,
                marketplace=payload.marketplace,
            )
            logger.info(f"广告数据同步完成: {result}")
        except Exception as e:
            logger.error(f"广告数据同步失败: {e}", exc_info=True)
        finally:
            db.close()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {
        "success": True,
        "message": "广告数据同步已启动，正在后台进行..."
    }


@router.get("/ads/performance")
async def get_ads_performance(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    campaign_id: Optional[int] = Query(None),
    adset_id: Optional[int] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    marketplace: Optional[str] = Query(None),  # ro / bg / hu
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """查询广告产品表现数据（分页）"""
    return query_ads_performance(
        db=db,
        skip=skip,
        limit=limit,
        campaign_id=campaign_id,
        adset_id=adset_id,
        date_start=date_start,
        date_end=date_end,
        marketplace=marketplace,
    )


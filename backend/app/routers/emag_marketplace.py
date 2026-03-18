import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session, selectinload
from datetime import datetime
import io
import csv

from app.middleware.auth_middleware import require_auth
from app.services.emag_marketplace_login_service import emag_marketplace_login_service
from app.services.emag_ads_service import sync_ads_data, query_ads_performance, get_ads_sync_progress, _split_into_iso_weeks
from app.database import get_db, SessionLocal
from app.models.emag_sync import EmagInboundShipment, EmagInboundShipmentDetail

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/emag-marketplace", tags=["emag-marketplace"])


class MarketplaceLoginRequest(BaseModel):
    username: Optional[str] = None  # eMAG 登录邮箱
    password: Optional[str] = None  # eMAG 登录密码
    shop_id: Optional[int] = None   # 关联店铺 ID


class MarketplaceSmsCodeRequest(BaseModel):
    code: str


class AdsSyncRequest(BaseModel):
    date_start: str   # YYYY-MM-DD
    date_end: str     # YYYY-MM-DD
    marketplace: str = "ro"  # ro / bg / hu
    shop_id: Optional[int] = None  # 关联店铺 ID


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
    # 记住当前登录的 shop_id，以便后续同步数据关联到该店铺
    emag_marketplace_login_service.set_current_shop_id(payload.shop_id)

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
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """查询已同步的入仓运单列表（含详情展开）"""
    query = db.query(EmagInboundShipment)

    if shop_id is not None:
        query = query.filter(EmagInboundShipment.shop_id == shop_id)
    if reception_id is not None:
        query = query.filter(EmagInboundShipment.reception_id == reception_id)
    if status_filter:
        query = query.filter(EmagInboundShipment.status == status_filter)

    total = query.count()
    shipments = (
        query
        .options(selectinload(EmagInboundShipment.details))
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
            "number_of_units": s.number_of_units,
            "detail_count": len(detail_list),
            "total_quantity": sum(d["transferred_to_storage_quantity"] for d in detail_list),
            "synced_at": s.synced_at.isoformat() if s.synced_at else None,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "details": detail_list,
        })

    return {"items": items, "total": total, "skip": skip, "limit": limit}


@router.get("/inbound-shipments/export-summary")
async def export_inbound_shipments_summary(
    reception_id: Optional[int] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Export inbound shipments summary as CSV"""
    query = db.query(EmagInboundShipment)

    if shop_id is not None:
        query = query.filter(EmagInboundShipment.shop_id == shop_id)
    if reception_id is not None:
        query = query.filter(EmagInboundShipment.reception_id == reception_id)
    if status_filter:
        query = query.filter(EmagInboundShipment.status == status_filter)

    query = query.options(selectinload(EmagInboundShipment.details)).order_by(EmagInboundShipment.reception_id.desc())

    def iter_csv():
        yield b'\xef\xbb\xbf'
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Shop ID", "Reception ID", "Status", "Number of Units", "Total Actual Quantity", "SKU Count", "Synced At", "Created At"])
        yield output.getvalue().encode('utf-8')
        output.seek(0)
        output.truncate(0)

        for s in query.yield_per(500):
            total_qty = sum(d.transferred_to_storage_quantity for d in s.details) if s.details else 0
            writer.writerow([
                s.id, s.shop_id, s.reception_id, s.status, s.number_of_units, total_qty, len(s.details) if s.details else 0,
                s.synced_at.strftime('%Y-%m-%d %H:%M:%S') if s.synced_at else "",
                s.created_at.strftime('%Y-%m-%d %H:%M:%S') if s.created_at else ""
            ])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)

    response = StreamingResponse(iter_csv(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=inbound_shipments_summary_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    return response


@router.get("/inbound-shipments/export-details")
async def export_inbound_shipments_details(
    reception_id: Optional[int] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Export inbound shipments details (flat structure) as CSV"""
    query = db.query(EmagInboundShipment)

    if shop_id is not None:
        query = query.filter(EmagInboundShipment.shop_id == shop_id)
    if reception_id is not None:
        query = query.filter(EmagInboundShipment.reception_id == reception_id)
    if status_filter:
        query = query.filter(EmagInboundShipment.status == status_filter)

    query = query.options(selectinload(EmagInboundShipment.details)).order_by(EmagInboundShipment.reception_id.desc())

    def iter_csv():
        yield b'\xef\xbb\xbf'
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Reception ID", "Status", "Vendor Product ID", "Quantity", "Expiration Date", "Producer Lot"])
        yield output.getvalue().encode('utf-8')
        output.seek(0)
        output.truncate(0)

        for s in query.yield_per(500):
            if s.details:
                for d in s.details:
                    writer.writerow([
                        s.reception_id, s.status, d.vendor_product_id, d.transferred_to_storage_quantity,
                        d.expiration_date.isoformat() if d.expiration_date else "",
                        d.producer_lot
                    ])
            else:
                writer.writerow([s.reception_id, s.status, "", "", "", ""])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)

    response = StreamingResponse(iter_csv(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=inbound_shipments_details_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    return response


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
    自动按 ISO 标准周（周一~周日）切割日期范围，逐周同步。
    Campaign → Adset → Product Performance
    """
    from datetime import datetime as _dt

    current_shop_id = emag_marketplace_login_service.get_current_shop_id()

    # 预计算周数，用于前端展示
    ds = _dt.strptime(payload.date_start, "%Y-%m-%d").date()
    de = _dt.strptime(payload.date_end, "%Y-%m-%d").date()
    weeks = _split_into_iso_weeks(ds, de)
    total_weeks = len(weeks)

    def _run():
        db = SessionLocal()
        try:
            result = sync_ads_data(
                db=db,
                date_start=payload.date_start,
                date_end=payload.date_end,
                login_service=emag_marketplace_login_service,
                marketplace=payload.marketplace,
                shop_id=current_shop_id,
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
        "message": f"广告数据同步已启动（共 {total_weeks} 个 ISO 周），正在后台逐周进行...",
        "total_weeks": total_weeks,
    }


@router.get("/ads/sync-progress")
async def get_ads_sync_progress_api(
    current_user: dict = Depends(require_auth),
):
    """查询广告数据同步进度（前端轮询用）"""
    return {"success": True, **get_ads_sync_progress()}


@router.get("/ads/performance")
async def get_ads_performance(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    campaign_id: Optional[int] = Query(None),
    adset_id: Optional[int] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    marketplace: Optional[str] = Query(None),  # ro / bg / hu
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
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
        shop_id=shop_id,
    )


@router.get("/ads/performance/export")
async def export_ads_performance(
    campaign_id: Optional[int] = Query(None),
    adset_id: Optional[int] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    marketplace: Optional[str] = Query(None),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Export ads performance as CSV"""
    from app.models.emag_ads import AdsProductPerformance, AdsAdset
    
    query = db.query(
        AdsProductPerformance,
        AdsAdset.status.label("adset_status")
    ).outerjoin(
        AdsAdset,
        (AdsProductPerformance.adset_id == AdsAdset.adset_id) &
        (AdsProductPerformance.marketplace == AdsAdset.marketplace) &
        (AdsProductPerformance.campaign_id == AdsAdset.campaign_id)
    )

    if shop_id is not None:
        query = query.filter(AdsProductPerformance.shop_id == shop_id)
    if marketplace:
        query = query.filter(AdsProductPerformance.marketplace == marketplace)
    if campaign_id is not None:
        query = query.filter(AdsProductPerformance.campaign_id == campaign_id)
    if adset_id is not None:
        query = query.filter(AdsProductPerformance.adset_id == adset_id)
    if date_start:
        ds = datetime.strptime(date_start, "%Y-%m-%d").date()
        query = query.filter(AdsProductPerformance.date_start >= ds)
    if date_end:
        de = datetime.strptime(date_end, "%Y-%m-%d").date()
        query = query.filter(AdsProductPerformance.date_end <= de)

    query = query.order_by(AdsProductPerformance.cost.desc())

    def iter_csv():
        yield b'\xef\xbb\xbf'
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Marketplace", "Campaign ID", "Campaign Name", "Adset ID", "Adset Name", "Status",
            "Product ID", "Product Name", "Part Number", "PNK", "Date Start", "Date End",
            "Clicks", "Impressions", "CTR", "Actual CPC", "Cost", "Sales", "Products Sold", "CPS", "Cost Percentage"
        ])
        yield output.getvalue().encode('utf-8')
        output.seek(0)
        output.truncate(0)

        for row in query.yield_per(500):
            perf_obj = row[0]
            adset_status = row[1]
            writer.writerow([
                perf_obj.marketplace, perf_obj.campaign_id, perf_obj.campaign_name, perf_obj.adset_id, perf_obj.adset_name, adset_status,
                perf_obj.product_id, perf_obj.product_name, perf_obj.part_number, perf_obj.part_number_key,
                perf_obj.date_start.isoformat() if perf_obj.date_start else "",
                perf_obj.date_end.isoformat() if perf_obj.date_end else "",
                perf_obj.clicks, perf_obj.impressions, perf_obj.ctr, perf_obj.actual_cpc, perf_obj.cost,
                perf_obj.sales, perf_obj.products_sold, perf_obj.cps, perf_obj.cost_percentage
            ])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)

    response = StreamingResponse(iter_csv(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=ads_performance_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    return response


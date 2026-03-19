import logging
import threading
from typing import Optional
import json
import time

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
from app.models.keyword import Keyword, KeywordLink, KeywordStatus

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


class OpportunitiesImportRequest(BaseModel):
    category_doc_id: int
    category_name: Optional[str] = None
    per_page: int = 100
    max_pages: int = 30


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


@router.post("/opportunities/import-by-category")
async def import_opportunities_by_category(
    payload: OpportunitiesImportRequest,
    shop_id: Optional[int] = Query(None, description="店铺 ID（用于加载对应的登录态 storage_state）"),
    current_user: dict = Depends(require_auth),
):
    """
    导入 opportunities 链接到链接初筛（keyword_links）。
    - 使用卖家中心登录态（storage_state/cookies）访问 api-ui/opportunities
    - 每次最多请求 max_pages（默认 30）页候选（per_page 默认 100）
    - 自动复用/创建 Keyword: CAT:<category_doc_id> <category_name>
    - 全局按 product_url 去重，已存在则跳过
    """

    def _dbg(loc: str, msg: str, data: dict, hyp: str):
        try:
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "runId": "pre-fix",
                    "hypothesisId": hyp,
                    "location": loc,
                    "message": msg,
                    "data": data,
                    "timestamp": int(time.time() * 1000),
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _run():
        db = SessionLocal()
        old_shop_id = emag_marketplace_login_service.get_current_shop_id()
        pw = browser = context = page_obj = None
        try:
            if shop_id is not None:
                emag_marketplace_login_service.set_current_shop_id(shop_id)

            cat_id = int(payload.category_doc_id)
            cat_name = (payload.category_name or "").strip()
            if not cat_name:
                # fallback: use ID only to avoid empty keyword
                cat_name = f"Category {cat_id}"

            keyword_str = f"CAT:{cat_id} {cat_name}"
            _dbg("emag_marketplace.py:_run:entry", "start import opportunities", {
                "shop_id": shop_id,
                "old_shop_id": old_shop_id,
                "category_doc_id": cat_id,
                "category_name_len": len(cat_name),
                "keyword_str_len": len(keyword_str),
                "per_page": payload.per_page,
                "max_pages": payload.max_pages,
            }, "H_backend_thread_start")

            # Find or create keyword
            keyword = db.query(Keyword).filter(Keyword.keyword == keyword_str).first()
            created_keyword = False
            if not keyword:
                keyword = Keyword(
                    keyword=keyword_str,
                    created_by_user_id=current_user["id"],
                    status=KeywordStatus.COMPLETED,
                )
                db.add(keyword)
                db.flush()
                created_keyword = True
            _dbg("emag_marketplace.py:_run:keyword", "keyword ready", {
                "keyword_id": getattr(keyword, "id", None),
                "created_keyword": created_keyword,
            }, "H_keyword_create")

            created_count = 0
            skipped_count = 0

            per_page = min(int(payload.per_page), 1000)
            max_pages = min(int(payload.max_pages), 30)

            # Create ONE authed playwright page for both opportunities + images calls
            _dbg("emag_marketplace.py:_run:fetch:before", "create authed page", {
                "category_doc_id": cat_id,
            }, "H_fetch_begin")
            pw, browser, context, page_obj = emag_marketplace_login_service._create_authed_page()

            for page in range(1, max_pages + 1):
                # ---- fetch opportunities page ----
                opp_body = {
                    "page": page,
                    "per_page": per_page,
                    "duplicated_documentation": 2,
                    "sort": [{"field": "performance", "direction": "asc"}],
                    "category_doc_id": cat_id,
                }
                opp_res = page_obj.evaluate(f"""
                    async () => {{
                        try {{
                            const res = await fetch('https://marketplace.emag.ro/api-ui/opportunities/', {{
                                method: 'POST',
                                headers: {{
                                    'content-type': 'application/json',
                                    'x-requested-with': 'XMLHttpRequest'
                                }},
                                body: JSON.stringify({json.dumps(opp_body)})
                            }});
                            const status = res.status;
                            const text = await res.text();
                            try {{ return JSON.parse(text); }} catch(pe) {{ return "ERROR_JS_HTTP" + status + "_" + text.substring(0, 500); }}
                        }} catch (e) {{
                            return "ERROR_JS_" + e.message;
                        }}
                    }}
                """)
                if isinstance(opp_res, str) and opp_res.startswith("ERROR_JS_"):
                    raise RuntimeError(f"Fetch opportunities failed: {opp_res}")
                if not isinstance(opp_res, dict):
                    raise RuntimeError(f"Unexpected opportunities response: {type(opp_res).__name__}")

                data = opp_res.get("data") or {}
                meta = data.get("meta") or {}
                products = data.get("products") or []
                if not isinstance(products, list) or not products:
                    _dbg("emag_marketplace.py:_run:opp_empty", "no products in page", {"page": page, "meta": meta}, "H_commit_page")
                    break

                # ---- prepare batch + dedupe ----
                batch = []
                for p in products:
                    if not isinstance(p, dict):
                        continue
                    url = (p.get("url") or "").strip()
                    if not url:
                        continue
                    batch.append((url, p))

                urls = [u for u, _ in batch]
                existing = set()
                if urls:
                    existing_rows = db.query(KeywordLink.product_url).filter(KeywordLink.product_url.in_(urls)).all()
                    existing = {r[0] for r in existing_rows}

                page_created = 0
                page_skipped = 0
                created_pnks = []

                for url, p in batch:
                    if url in existing:
                        page_skipped += 1
                        continue

                    pnk = (p.get("part_number_key") or "").strip() or None
                    best_price = p.get("best_price")
                    active_offers = p.get("active_offers")
                    try:
                        purchase_price = float(best_price) if best_price is not None else 0.0
                    except Exception:
                        purchase_price = 0.0
                    try:
                        offer_count = int(active_offers) if active_offers is not None else 0
                    except Exception:
                        offer_count = 0

                    link = KeywordLink(
                        keyword_id=keyword.id,
                        product_url=url,
                        pnk_code=pnk,
                        purchase_price=purchase_price,
                        offer_count=offer_count,
                        product_title=p.get("product_name"),
                        brand=p.get("brand_name"),
                        category=p.get("category_name"),
                        tag=p.get("product_performance_label"),
                        source="category_opportunities",
                        status="active",
                    )
                    db.add(link)
                    page_created += 1
                    if pnk:
                        created_pnks.append(pnk)

                db.commit()
                created_count += page_created
                skipped_count += page_skipped

                _dbg("emag_marketplace.py:_run:page_commit", "page committed", {
                    "page": page,
                    "products_len": len(products),
                    "batch_len": len(batch),
                    "page_created": page_created,
                    "page_skipped": page_skipped,
                    "total_created": created_count,
                    "total_skipped": skipped_count,
                    "meta": meta,
                    "created_pnks_count": len(created_pnks),
                }, "H_commit_page")

                # ---- fetch images + update thumbnail_image (global by PNK) ----
                # Deduplicate pnks and chunk to 100
                pnks_unique = []
                seen = set()
                for x in created_pnks:
                    if x and x not in seen:
                        seen.add(x)
                        pnks_unique.append(x)

                if pnks_unique:
                    chunk_size = 100
                    for i in range(0, len(pnks_unique), chunk_size):
                        chunk = pnks_unique[i:i+chunk_size]
                        img_body = {"products": chunk, "resolution": "150x150"}
                        img_res = page_obj.evaluate(f"""
                            async () => {{
                                try {{
                                    const res = await fetch('https://marketplace.emag.ro/ui/offer/images', {{
                                        method: 'POST',
                                        headers: {{
                                            'content-type': 'application/json',
                                            'x-requested-with': 'XMLHttpRequest'
                                        }},
                                        body: JSON.stringify({json.dumps(img_body)})
                                    }});
                                    const status = res.status;
                                    const text = await res.text();
                                    try {{ return JSON.parse(text); }} catch(pe) {{ return "ERROR_JS_HTTP" + status + "_" + text.substring(0, 500); }}
                                }} catch (e) {{
                                    return "ERROR_JS_" + e.message;
                                }}
                            }}
                        """)
                        if isinstance(img_res, str) and img_res.startswith("ERROR_JS_"):
                            _dbg("emag_marketplace.py:_run:images_error", "images fetch error", {
                                "page": page,
                                "chunk_len": len(chunk),
                                "error": img_res[:200],
                            }, "H_images_error")
                            continue
                        if not isinstance(img_res, dict):
                            _dbg("emag_marketplace.py:_run:images_error", "images response not dict", {
                                "page": page,
                                "chunk_len": len(chunk),
                                "type": type(img_res).__name__,
                            }, "H_images_error")
                            continue

                        if img_res.get("isError"):
                            _dbg("emag_marketplace.py:_run:images_error", "images isError", {
                                "page": page,
                                "chunk_len": len(chunk),
                                "messages": img_res.get("messages"),
                            }, "H_images_error")
                            continue

                        results = img_res.get("results") or []
                        updated = 0
                        if isinstance(results, list):
                            for r in results:
                                if not isinstance(r, dict):
                                    continue
                                pnk = r.get("pnk")
                                url = r.get("imageURL")
                                if not pnk or not url:
                                    continue
                                # global update, only fill empty thumbnail_image
                                updated += (
                                    db.query(KeywordLink)
                                    .filter(
                                        KeywordLink.pnk_code == pnk,
                                        (KeywordLink.thumbnail_image.is_(None)) | (KeywordLink.thumbnail_image == "")
                                    )
                                    .update({"thumbnail_image": url})
                                )
                            db.commit()

                        _dbg("emag_marketplace.py:_run:images_update", "images updated", {
                            "page": page,
                            "chunk_len": len(chunk),
                            "results_len": len(results) if isinstance(results, list) else None,
                            "rows_updated": updated,
                        }, "H_images_update")

                        time.sleep(0.4)

            _dbg("emag_marketplace.py:_run:commit", "import finished", {
                "created": created_count,
                "skipped": skipped_count,
                "per_page": per_page,
                "max_pages": max_pages,
            }, "H_commit_ok")
            logger.info(
                f"[opportunities import] keyword='{keyword_str}' created_keyword={created_keyword} "
                f"created={created_count} skipped={skipped_count} shop_id={shop_id}"
            )
        except Exception as e:
            db.rollback()
            _dbg("emag_marketplace.py:_run:exception", "import failed", {
                "error_type": type(e).__name__,
                "error": str(e)[:500],
            }, "H_backend_exception")
            logger.error(f"[opportunities import] failed: {e}", exc_info=True)
        finally:
            try:
                if browser:
                    browser.close()
            except Exception:
                pass
            try:
                if pw:
                    pw.stop()
            except Exception:
                pass
            try:
                if shop_id is not None:
                    emag_marketplace_login_service.set_current_shop_id(old_shop_id)
            except Exception:
                pass
            db.close()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"success": True, "message": "opportunities_import_started"}


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


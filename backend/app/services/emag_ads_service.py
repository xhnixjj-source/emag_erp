"""
eMAG Ads 广告数据抓取服务。

复用 emag_marketplace_login_service 的 _create_authed_page() 创建认证的 Playwright 会话，
通过 page.evaluate(fetch(...)) 调用 eMAG 内部 API 进行三层递归抓取：
  Campaign → Adset → Product Performance
"""
import json
import logging
import time
import urllib.parse
from datetime import datetime, date
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.emag_ads import AdsCampaign, AdsAdset, AdsProductPerformance

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Marketplace URL 映射
# ---------------------------------------------------------------------------

MARKETPLACE_BASE_URLS: Dict[str, str] = {
    "ro": "https://marketplace.emag.ro",
    "bg": "https://marketplace.emag.bg",
    "hu": "https://marketplace.emag.hu",
}

MARKETPLACE_LABELS: Dict[str, str] = {
    "ro": "Romania (RO)",
    "bg": "Bulgaria (BG)",
    "hu": "Hungary (HU)",
}

# ---------------------------------------------------------------------------
# 辅助：构建 query-string
# ---------------------------------------------------------------------------

def _build_qs(params: Dict[str, Any]) -> str:
    """将参数字典转为 query string，支持 key[] 格式。"""
    parts = []
    for k, v in params.items():
        parts.append(f"{urllib.parse.quote(k, safe='[]')}={urllib.parse.quote(str(v), safe='')}")
    return "&".join(parts)


# ---------------------------------------------------------------------------
# 核心：通过 Playwright page.evaluate(fetch) 调用 API
# ---------------------------------------------------------------------------

_FETCH_JS_TEMPLATE = """
    async () => {{
        try {{
            const res = await fetch('{url}', {{
                method: 'GET',
                headers: {{ 'x-requested-with': 'XMLHttpRequest' }}
            }});
            const text = await res.text();
            let parsed = null;
            try {{ parsed = JSON.parse(text); }} catch(e) {{}}
            return {{ "__status": res.status, "__ok": res.ok, "__text_preview": text.substring(0, 500), "__parsed": parsed }};
        }} catch (e) {{
            return {{ "__error": true, "message": e.message }};
        }}
    }}
"""

def _fetch_json(page, url: str, max_retries: int = 3) -> Any:
    """在 Playwright page 中执行 fetch 并返回 JSON（带重试）。"""
    js_code = _FETCH_JS_TEMPLATE.format(url=url)

    for attempt in range(1, max_retries + 1):
        try:
            # 确保页面处于稳定状态
            page.wait_for_load_state("domcontentloaded", timeout=10000)
            result = page.evaluate(js_code)
        except Exception as e:
            err_msg = str(e)
            logger.warning(f"  fetch attempt {attempt}/{max_retries} 失败: {err_msg[:120]}")
            if attempt >= max_retries:
                raise
            # 页面导航/上下文被销毁 → 等待页面重新稳定后重试
            if "context was destroyed" in err_msg or "navigation" in err_msg.lower():
                logger.info(f"  等待页面重新加载后重试...")
                time.sleep(1)
                try:
                    page.wait_for_load_state("load", timeout=10000)
                except Exception:
                    pass
                continue
            raise

        # JS fetch 内部错误（如 "Failed to fetch"）→ 也重试
        if isinstance(result, dict) and result.get("__error"):
            err_msg = result.get("message", "")
            logger.warning(f"  JS fetch attempt {attempt}/{max_retries} 错误: {err_msg}")
            if attempt >= max_retries:
                raise RuntimeError(f"Fetch 失败: {result}")
            time.sleep(1.5)
            try:
                page.wait_for_load_state("load", timeout=10000)
            except Exception:
                pass
            continue

        # 提取实际 parsed JSON
        if isinstance(result, dict) and "__parsed" in result:
            if not result.get("__ok"):
                raise RuntimeError(f"Fetch HTTP {result.get('__status')}: {result.get('__text_preview', '')[:200]}")
            return result["__parsed"] if result["__parsed"] is not None else {}
        return result

    raise RuntimeError("_fetch_json: 不应到达此处")


# ---------------------------------------------------------------------------
# 分页通用
# ---------------------------------------------------------------------------

def _fetch_all_pages(page, base_url: str, extra_params: Dict[str, Any],
                     items_key: str = "items",
                     per_page: int = 25, max_pages: int = 100) -> List[dict]:
    """
    通用分页拉取，返回所有 items。

    eMAG API 返回结构为:
      { "message": "OK", "data": { "meta": { "page", "limit", "page_count", "total_count" }, "<items_key>": [...] } }

    Parameters
    ----------
    items_key : 在 data.data 下存放记录列表的 key 名称，例如 "campaigns" / "adsets" / "products"
    """
    all_items: List[dict] = []
    current_page = 1

    while current_page <= max_pages:
        params = {**extra_params, "page": current_page, "per_page": per_page}
        qs = _build_qs(params)
        url = f"{base_url}?{qs}"

        logger.info(f"  分页请求 page={current_page}: {url[:200]}")
        raw = _fetch_json(page, url)

        # 解开 eMAG 的 {"message":"OK","data":{...}} 外层
        inner = raw.get("data", raw) if isinstance(raw, dict) else raw
        if not isinstance(inner, dict):
            logger.warning(f"  API 返回非 dict 内层: {type(inner)}")
            break

        meta = inner.get("meta", {}) or {}
        total = meta.get("total_count", 0)
        page_count = meta.get("page_count", 1)

        items = inner.get(items_key)  # None if key doesn't exist
        if items is None or not isinstance(items, list):
            # items_key 不存在或值不是列表 → 自动发现：找内层中第一个非空 list 值
            found = False
            for k, v in inner.items():
                if isinstance(v, list) and len(v) > 0:
                    logger.warning(f"  items_key '{items_key}' 未找到，使用自动发现 key='{k}' ({len(v)} 条)")
                    items = v
                    found = True
                    break
            if not found:
                items = []

        all_items.extend(items)
        logger.info(f"  本页 {len(items)} 条，累计 {len(all_items)}/{total} (page_count={page_count})")

        if current_page >= page_count or len(items) == 0:
            break
        current_page += 1
        time.sleep(0.3)

    return all_items


# ---------------------------------------------------------------------------
# 三层递归抓取 + 入库
# ---------------------------------------------------------------------------

def sync_ads_data(
    db: Session,
    date_start: str,
    date_end: str,
    login_service,
    marketplace: str = "ro",
) -> Dict[str, Any]:
    """
    主入口：同步广告数据到数据库。

    Parameters
    ----------
    db : Session
    date_start : str  "YYYY-MM-DD"
    date_end : str    "YYYY-MM-DD"
    login_service : EmagMarketplaceLoginService 实例（用来调 _create_authed_page）
    marketplace : str  "ro" / "bg" / "hu"

    Returns
    -------
    统计摘要 dict
    """
    base_url = MARKETPLACE_BASE_URLS.get(marketplace)
    if not base_url:
        raise ValueError(f"不支持的 marketplace: {marketplace}，可选: {list(MARKETPLACE_BASE_URLS.keys())}")

    pw, browser, context, page_obj = None, None, None, None
    stats = {
        "marketplace": marketplace,
        "campaigns": 0,
        "adsets": 0,
        "products": 0,
        "errors": [],
    }

    try:
        pw, browser, context, page_obj = login_service._create_authed_page()

        # 先导航到目标 marketplace，确保 cookie 域名正确
        logger.info(f"广告数据同步开始 marketplace={marketplace} date_start={date_start} date_end={date_end}")
        page_obj.goto(f"{base_url}/dashboard", wait_until="domcontentloaded", timeout=15000)
        time.sleep(1)

        # ---- 第 1 层: Campaign 列表 ----
        campaign_params = {
            "date_start": date_start,
            "date_end": date_end,
            "inherited_status[]": "active",
            "sort[]": json.dumps({"field": "id", "direction": "desc"}),
        }
        campaigns = _fetch_all_pages(
            page_obj,
            f"{base_url}/api-ui/ads/campaign",
            campaign_params,
            items_key="campaigns",
        )
        logger.info(f"[{marketplace.upper()}] 获取到 {len(campaigns)} 个广告活动")

        for camp in campaigns:
            camp_id = camp.get("id")
            if not camp_id:
                continue
            camp_name = camp.get("name", "")

            # upsert campaign
            _upsert_campaign(db, camp, marketplace)
            stats["campaigns"] += 1

            # ---- 第 2 层: Adset 列表 ----
            try:
                adset_params = {
                    "date_start": date_start,
                    "date_end": date_end,
                    "status[]": "active",
                }
                adsets = _fetch_all_pages(
                    page_obj,
                    f"{base_url}/api-ui/ads/campaign/{camp_id}/adsets",
                    adset_params,
                    items_key="ad_sets",
                )
                logger.info(f"  活动 {camp_id} ({camp_name}): {len(adsets)} 个广告组")
            except Exception as e:
                logger.error(f"  获取活动 {camp_id} 的广告组失败: {e}")
                stats["errors"].append({"campaign_id": camp_id, "error": str(e)})
                continue

            for adset in adsets:
                adset_id = adset.get("id")
                adset_name = adset.get("name", "")
                if not adset_id:
                    continue

                # upsert adset
                _upsert_adset(db, adset, camp_id, marketplace)
                stats["adsets"] += 1

                # ---- 第 3 层: Product Performance ----
                try:
                    product_params = {
                        "adset_id": adset_id,
                        "adset_name": adset_name,
                        "date_start": date_start,
                        "date_end": date_end,
                        "status[]": "active",
                    }
                    products = _fetch_all_pages(
                        page_obj,
                        f"{base_url}/api-ui/ads/campaign/{camp_id}/products",
                        product_params,
                        items_key="offers",
                    )
                    logger.info(f"    广告组 {adset_id} ({adset_name}): {len(products)} 个产品")
                except Exception as e:
                    logger.error(f"    获取广告组 {adset_id} 的产品失败: {e}")
                    stats["errors"].append({
                        "campaign_id": camp_id,
                        "adset_id": adset_id,
                        "error": str(e),
                    })
                    continue

                for prod in products:
                    try:
                        _upsert_product_performance(
                            db, prod, camp_id, camp_name,
                            adset_id, adset_name,
                            date_start, date_end,
                            marketplace,
                        )
                        stats["products"] += 1
                    except Exception as e:
                        logger.error(f"      产品入库失败: {e}")
                        stats["errors"].append({
                            "campaign_id": camp_id,
                            "adset_id": adset_id,
                            "product": prod.get("id"),
                            "error": str(e),
                        })

                time.sleep(0.3)  # 广告组间延迟

            time.sleep(0.5)  # 活动间延迟

        logger.info(
            f"[{marketplace.upper()}] 广告数据同步完成: {stats['campaigns']} 活动, "
            f"{stats['adsets']} 广告组, {stats['products']} 产品, "
            f"{len(stats['errors'])} 错误"
        )
        return stats

    except Exception as e:
        logger.exception("广告数据同步失败")
        raise
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


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def _upsert_campaign(db: Session, camp: dict, marketplace: str = "ro"):
    camp_id = camp["id"]
    existing = db.query(AdsCampaign).filter(
        AdsCampaign.campaign_id == camp_id,
        AdsCampaign.marketplace == marketplace,
    ).first()
    # API 返回 daily_budget 而非 budget
    budget_val = camp.get("daily_budget") or camp.get("budget")
    budget_type_val = "daily" if camp.get("daily_budget") is not None else camp.get("budget_type")
    if existing:
        existing.name = camp.get("name", existing.name)
        existing.status = camp.get("inherited_status", camp.get("status", existing.status))
        existing.budget = budget_val if budget_val is not None else existing.budget
        existing.budget_type = budget_type_val or existing.budget_type
        existing.synced_at = datetime.utcnow()
    else:
        obj = AdsCampaign(
            campaign_id=camp_id,
            marketplace=marketplace,
            name=camp.get("name"),
            status=camp.get("inherited_status", camp.get("status")),
            budget=budget_val,
            budget_type=budget_type_val,
            synced_at=datetime.utcnow(),
        )
        db.add(obj)
    db.commit()


def _upsert_adset(db: Session, adset: dict, campaign_id: int, marketplace: str = "ro"):
    adset_id = adset["id"]
    existing = db.query(AdsAdset).filter(
        AdsAdset.adset_id == adset_id,
        AdsAdset.campaign_id == campaign_id,
        AdsAdset.marketplace == marketplace,
    ).first()
    if existing:
        existing.name = adset.get("name", existing.name)
        existing.status = adset.get("status", existing.status)
        existing.bid = adset.get("bid", existing.bid)
        existing.synced_at = datetime.utcnow()
    else:
        obj = AdsAdset(
            adset_id=adset_id,
            campaign_id=campaign_id,
            marketplace=marketplace,
            name=adset.get("name"),
            status=adset.get("status"),
            bid=adset.get("bid"),
            synced_at=datetime.utcnow(),
        )
        db.add(obj)
    db.commit()


def _upsert_product_performance(
    db: Session,
    prod: dict,
    campaign_id: int,
    campaign_name: str,
    adset_id: int,
    adset_name: str,
    date_start: str,
    date_end: str,
    marketplace: str = "ro",
):
    product_id = prod.get("id")
    if not product_id:
        return

    # API 返回的性能数据在 "summary" 字段下（非 "analytics"）
    # 字段映射: spent→cost, effective_cpc→actual_cpc, sold_units→products_sold,
    #          average_cost_of_sale→cps, spent_percentage→cost_percentage
    summary = prod.get("summary", {})
    ds = datetime.strptime(date_start, "%Y-%m-%d").date()
    de = datetime.strptime(date_end, "%Y-%m-%d").date()

    existing = db.query(AdsProductPerformance).filter(
        AdsProductPerformance.product_id == product_id,
        AdsProductPerformance.adset_id == adset_id,
        AdsProductPerformance.date_start == ds,
        AdsProductPerformance.date_end == de,
        AdsProductPerformance.marketplace == marketplace,
    ).first()

    fields = dict(
        marketplace=marketplace,
        campaign_id=campaign_id,
        campaign_name=campaign_name,
        adset_id=adset_id,
        adset_name=adset_name,
        product_id=product_id,
        product_name=prod.get("name"),
        part_number=prod.get("part_number"),             # PNK
        part_number_key=prod.get("part_number_key"),     # Prd_Code
        date_start=ds,
        date_end=de,
        clicks=_safe_int(summary.get("clicks", 0)),
        impressions=_safe_int(summary.get("impressions", 0)),
        ctr=_safe_float(summary.get("ctr", 0)),
        actual_cpc=_safe_float(summary.get("effective_cpc", 0)),        # API: effective_cpc
        cost=_safe_float(summary.get("spent", 0)),                      # API: spent
        sales=_safe_float(summary.get("sales", 0)),
        products_sold=_safe_int(summary.get("sold_units", 0)),          # API: sold_units
        cps=_safe_float(summary.get("average_cost_of_sale", 0)),        # API: average_cost_of_sale
        cost_percentage=_safe_float(summary.get("spent_percentage", 0)), # API: spent_percentage
        synced_at=datetime.utcnow(),
    )

    if existing:
        for k, v in fields.items():
            setattr(existing, k, v)
    else:
        obj = AdsProductPerformance(**fields)
        db.add(obj)
    db.commit()


def _safe_int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _safe_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# 查询：供 GET 路由使用
# ---------------------------------------------------------------------------

def query_ads_performance(
    db: Session,
    skip: int = 0,
    limit: int = 50,
    campaign_id: Optional[int] = None,
    adset_id: Optional[int] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    marketplace: Optional[str] = None,
) -> Dict[str, Any]:
    """查询广告产品表现数据（分页）。"""
    query = db.query(AdsProductPerformance)

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

    total = query.count()
    items = (
        query
        .order_by(AdsProductPerformance.cost.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    result_items = []
    for row in items:
        result_items.append({
            "id": row.id,
            "marketplace": row.marketplace,
            "campaign_id": row.campaign_id,
            "campaign_name": row.campaign_name,
            "adset_id": row.adset_id,
            "adset_name": row.adset_name,
            "product_id": row.product_id,
            "product_name": row.product_name,
            "part_number": row.part_number,
            "part_number_key": row.part_number_key,
            "date_start": row.date_start.isoformat() if row.date_start else None,
            "date_end": row.date_end.isoformat() if row.date_end else None,
            "clicks": row.clicks,
            "impressions": row.impressions,
            "ctr": row.ctr,
            "actual_cpc": row.actual_cpc,
            "cost": row.cost,
            "sales": row.sales,
            "products_sold": row.products_sold,
            "cps": row.cps,
            "cost_percentage": row.cost_percentage,
            "synced_at": row.synced_at.isoformat() if row.synced_at else None,
        })

    return {"items": result_items, "total": total, "skip": skip, "limit": limit}


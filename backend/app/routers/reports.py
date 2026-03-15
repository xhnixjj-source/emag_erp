"""Reports router - product summary & ads weekly performance"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, case, literal_column, text
from datetime import datetime, date

from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.emag_sync import EmagProduct, EmagOrder, EmagReturn, EmagInboundShipment, EmagInboundShipmentDetail
from app.models.emag_ads import AdsProductPerformance

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/reports", tags=["reports"])


# ---------------------------------------------------------------------------
# Report 1 – Product Summary (发货数 / 订单数 / 退货数量 / 库存数)
# ---------------------------------------------------------------------------

@router.get("/product-summary")
async def product_summary(
    search: Optional[str] = Query(None, description="Search by product name / PNK / EAN"),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """
    每个产品一行，聚合以下指标:
    - 发货数 (inbound shipment transferred_to_storage_quantity)
    - 订单数 (order quantity, 排除取消的订单 status!=0)
    - 退货数量 (return quantity)
    - 库存数 (product stock)
    """

    # Sub-queries ----------------------------------------------------------

    # 订单数: 按 product_id 汇总 quantity (排除取消订单 status=0)
    order_q = (
        db.query(
            EmagOrder.product_id.label("product_id"),
            func.count(EmagOrder.id).label("order_count"),
            func.coalesce(func.sum(EmagOrder.quantity), 0).label("order_quantity"),
        )
        .filter(EmagOrder.order_status != 0)  # 排除取消
    )
    if shop_id is not None:
        order_q = order_q.filter(EmagOrder.shop_id == shop_id)
    order_sub = order_q.group_by(EmagOrder.product_id).subquery("order_sub")

    # 退货数量: 按 product_id 汇总 quantity
    return_q = (
        db.query(
            EmagReturn.product_id.label("product_id"),
            func.coalesce(func.sum(EmagReturn.quantity), 0).label("return_quantity"),
        )
    )
    if shop_id is not None:
        return_q = return_q.filter(EmagReturn.shop_id == shop_id)
    return_sub = return_q.group_by(EmagReturn.product_id).subquery("return_sub")

    # 发货数: EmagInboundShipmentDetail.vendor_product_id == product.product_id
    shipment_q = (
        db.query(
            EmagInboundShipmentDetail.vendor_product_id.label("product_id"),
            func.coalesce(func.sum(EmagInboundShipmentDetail.transferred_to_storage_quantity), 0).label("shipment_quantity"),
        )
    )
    if shop_id is not None:
        shipment_q = shipment_q.join(
            EmagInboundShipment, EmagInboundShipmentDetail.shipment_id == EmagInboundShipment.id
        ).filter(EmagInboundShipment.shop_id == shop_id)
    shipment_sub = shipment_q.group_by(EmagInboundShipmentDetail.vendor_product_id).subquery("shipment_sub")

    # Main query -----------------------------------------------------------
    query = (
        db.query(
            EmagProduct.product_id,
            EmagProduct.pnk_code,
            EmagProduct.ean,
            EmagProduct.name,
            EmagProduct.brand,
            EmagProduct.sale_price,
            func.coalesce(EmagProduct.stock, 0).label("stock"),
            func.coalesce(shipment_sub.c.shipment_quantity, 0).label("shipment_quantity"),
            func.coalesce(order_sub.c.order_count, 0).label("order_count"),
            func.coalesce(order_sub.c.order_quantity, 0).label("order_quantity"),
            func.coalesce(return_sub.c.return_quantity, 0).label("return_quantity"),
        )
        .outerjoin(order_sub, EmagProduct.product_id == order_sub.c.product_id)
        .outerjoin(return_sub, EmagProduct.product_id == return_sub.c.product_id)
        .outerjoin(shipment_sub, EmagProduct.product_id == shipment_sub.c.product_id)
    )

    # shop_id filter on main product table
    if shop_id is not None:
        query = query.filter(EmagProduct.shop_id == shop_id)

    # Optional search
    if search:
        like_pattern = f"%{search}%"
        query = query.filter(
            (EmagProduct.name.ilike(like_pattern))
            | (EmagProduct.pnk_code.ilike(like_pattern))
            | (EmagProduct.ean.ilike(like_pattern))
        )

    rows = query.order_by(EmagProduct.product_id.desc()).all()

    items = []
    for r in rows:
        items.append({
            "product_id": r.product_id,
            "pnk_code": r.pnk_code,
            "ean": r.ean,
            "name": r.name,
            "brand": r.brand,
            "sale_price": r.sale_price,
            "stock": r.stock,
            "shipment_quantity": r.shipment_quantity,
            "order_count": r.order_count,
            "order_quantity": r.order_quantity,
            "return_quantity": r.return_quantity,
        })

    return {"items": items, "total": len(items)}


# ---------------------------------------------------------------------------
# Report 2 – Ads Weekly Performance (按周聚合广告表现)
# ---------------------------------------------------------------------------

@router.get("/ads-weekly")
async def ads_weekly(
    week: Optional[str] = Query(None, description="ISO week string, e.g. 2026-W11. If empty, return all weeks."),
    search: Optional[str] = Query(None, description="Search by product name / PNK"),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """
    每个产品每周一行，聚合:
    - 曝光 (impressions)
    - 点击 (clicks)
    - CTR (clicks / impressions * 100)
    - 成交 (products_sold)
    - CPS (cost / products_sold)  -- 单次成交成本
    - 广告成本 (cost)
    """

    # Use strftime to derive ISO week: '%Y-W%W' (SQLite compatible)
    # SQLite: strftime('%Y-W%W', date_start)
    week_expr = func.strftime('%Y-W%W', AdsProductPerformance.date_start).label("week")

    base_q = db.query(
        AdsProductPerformance.product_id,
        func.max(AdsProductPerformance.product_name).label("product_name"),
        func.max(AdsProductPerformance.part_number).label("part_number"),
        week_expr,
        func.coalesce(func.sum(AdsProductPerformance.impressions), 0).label("impressions"),
        func.coalesce(func.sum(AdsProductPerformance.clicks), 0).label("clicks"),
        func.coalesce(func.sum(AdsProductPerformance.cost), 0).label("cost"),
        func.coalesce(func.sum(AdsProductPerformance.sales), 0).label("sales"),
        func.coalesce(func.sum(AdsProductPerformance.products_sold), 0).label("products_sold"),
    )
    if shop_id is not None:
        base_q = base_q.filter(AdsProductPerformance.shop_id == shop_id)
    query = base_q.group_by(AdsProductPerformance.product_id, week_expr)

    # Optional week filter
    if week:
        query = query.having(week_expr == week)

    # Optional search
    if search:
        like_pattern = f"%{search}%"
        query = query.having(
            (func.max(AdsProductPerformance.product_name).ilike(like_pattern))
            | (func.max(AdsProductPerformance.part_number).ilike(like_pattern))
        )

    rows = query.order_by(week_expr.desc(), AdsProductPerformance.product_id.desc()).all()

    items = []
    for r in rows:
        impressions = r.impressions or 0
        clicks = r.clicks or 0
        cost = r.cost or 0
        products_sold = r.products_sold or 0
        ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
        cps = round(cost / products_sold, 2) if products_sold > 0 else 0

        items.append({
            "product_id": r.product_id,
            "product_name": r.product_name,
            "part_number": r.part_number,
            "week": r.week,
            "impressions": impressions,
            "clicks": clicks,
            "ctr": ctr,
            "cost": round(cost, 2),
            "sales": round(r.sales or 0, 2),
            "products_sold": products_sold,
            "cps": cps,
        })

    # Collect distinct weeks for filter dropdown
    week_dropdown_q = db.query(
        func.strftime('%Y-W%W', AdsProductPerformance.date_start).label("week")
    )
    if shop_id is not None:
        week_dropdown_q = week_dropdown_q.filter(AdsProductPerformance.shop_id == shop_id)
    week_query = (
        week_dropdown_q
        .distinct()
        .order_by(func.strftime('%Y-W%W', AdsProductPerformance.date_start).desc())
        .all()
    )
    weeks = [w.week for w in week_query if w.week]

    return {"items": items, "total": len(items), "weeks": weeks}


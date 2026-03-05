"""Profit calculation API"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_
from pydantic import BaseModel
from datetime import datetime
import json
import os
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.listing import ListingPool, ProfitCalculation, ListingStatus
from app.models.monitor_pool import MonitorPool
from app.models.product import FilterPool
from app.models.user import User
from app.models.profit_config import ProfitConfig
from app.services.permission import require_product_edit_permission
from app.services.operation_log_service import create_operation_log
from app.services.profit_engine import ProfitEngine, GeniusRuleDomain
from app.services.config_service import (
    get_current_vat,
    get_current_exchange_rate,
    get_logistics_price,
    get_active_genius_rule,
    get_packaging_cost
)
from app.services.product_info_service import (
    get_product_info_from_listing,
    populate_profit_calculation_from_listing
)
from decimal import Decimal



router = APIRouter(prefix="/api/profit", tags=["profit"])

class ProfitCalculationRequest(BaseModel):
    """Profit calculation request model"""
    purchase_price: Optional[float] = None
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    weight: Optional[float] = None
    shipping_cost: Optional[float] = None
    order_fee: Optional[float] = None
    storage_fee: Optional[float] = None
    platform_commission: Optional[float] = None
    vat: Optional[float] = None
    chinese_name: Optional[str] = None
    model_number: Optional[str] = None
    category_name: Optional[str] = None
    # 新增字段
    frontend_price_ron: Optional[float] = None
    transport_mode: Optional[str] = None  # 'air' / 'land'
    participate_genius: Optional[bool] = False
    packaging_template_id: Optional[int] = None
    override_vat: Optional[float] = None
    override_exchange_rate: Optional[float] = None

    model_config = {"protected_namespaces": ()}



class ProfitCalculationResponse(BaseModel):
    """Profit calculation response model"""
    id: int
    listing_pool_id: int
    purchase_price: Optional[float]
    length: Optional[float]
    width: Optional[float]
    height: Optional[float]
    weight: Optional[float]
    shipping_cost: Optional[float]
    order_fee: Optional[float]
    storage_fee: Optional[float]
    platform_commission: Optional[float]
    vat: Optional[float]
    profit_margin: Optional[float]
    profit_amount: Optional[float]
    chinese_name: Optional[str] = None
    model_number: Optional[str] = None
    category_name: Optional[str] = None
    calculated_at: Optional[str] = None
    # 新增字段
    commission_source: Optional[str] = None
    commission_last_updated_at: Optional[str] = None
    frontend_price_ron: Optional[float] = None
    price_source: Optional[str] = None
    price_last_updated_at: Optional[str] = None
    best_price_ron: Optional[float] = None
    packaging_template_id: Optional[int] = None
    default_transport_mode: Optional[str] = None
    is_genius_eligible: Optional[bool] = False

    model_config = {"protected_namespaces": (), "from_attributes": True}


class EnhancedProfitCalculationResponse(BaseModel):
    """Enhanced profit calculation response with all cost items"""
    # 基础信息
    listing_pool_id: int
    frontend_price_ron: float
    purchase_price_rmb: float
    
    # 收入
    revenue_ex_vat_rmb: float
    revenue_inc_vat_rmb: float
    
    # 成本项明细
    volumetric_weight_kg: float
    chargeable_weight_kg: float
    first_leg_logistics_cost_rmb: float
    commission_fee_ron: float
    commission_fee_rmb: float
    genius_fee_ron: float
    genius_fee_rmb: float
    order_handling_fee_rmb: float
    storage_fee_rmb: float
    packaging_cost_rmb: float
    
    # 结果
    profit_rmb: float
    margin_ex_vat: float  # 利润率（去除VAT）
    margin_inc_vat: float  # 利润率（含VAT）





class ProfitListResponse(BaseModel):
    """Profit calculation list item response model"""
    id: int
    listing_pool_id: int
    operator_name: Optional[str] = None
    competitor_image: Optional[str] = None
    product_name_ro: Optional[str] = None
    chinese_name: Optional[str] = None
    model_number: Optional[str] = None
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    weight: Optional[float] = None
    purchase_price: Optional[float] = None
    sale_price: Optional[float] = None  # 售价（含VAT）
    profit_amount: Optional[float] = None
    profit_margin: Optional[float] = None
    profit_margin_without_vat: Optional[float] = None
    roi: Optional[float] = None  # ROI (%)
    category_name: Optional[str] = None
    platform_commission: Optional[float] = None
    auto_commission_rate: Optional[float] = None  # 自动获取的佣金费率
    platform_commission_amount: Optional[float] = None
    vat_amount: Optional[float] = None  # VAT金额
    logistics_cost: Optional[float] = None  # 物流成本
    domestic_logistics: Optional[float] = None
    shipping_cost: Optional[float] = None
    status: str
    created_at: str

    model_config = {"protected_namespaces": ()}



class ProfitListResponseWrapper(BaseModel):
    """Profit calculation list response wrapper"""
    items: list[ProfitListResponse]
    total: int
    page: int
    page_size: int

def get_commission_rate_by_category(category_name: Optional[str]) -> Optional[float]:
    """
    根据类目名称获取佣金费率
    如果没有找到或没有类目名称，返回None
    """
    if not category_name:
        return None
    
    # 这里可以根据实际的类目和佣金费率映射关系来获取
    # 目前先返回None，后续可以根据实际需求添加映射表
    # 例如：
    # category_commission_map = {
    #     "Electronics": 5.0,
    #     "Clothing": 8.0,
    #     ...
    # }
    # return category_commission_map.get(category_name)
    
    return None

def calculate_profit(
    listing_pool_id: int,
    purchase_price: float,
    shipping_cost: float,
    order_fee: float,
    storage_fee: float,
    platform_commission: float,
    vat: float,
    db: Session
) -> tuple[float, float, float, float]:
    """
    Calculate profit amount, margin, margin without VAT, and platform commission amount
    Returns: (profit_amount, profit_margin, profit_margin_without_vat, platform_commission_amount)
    
    This function now uses ProfitEngine for calculation, but maintains backward compatibility
    with the existing return signature.
    """
    # Get product price from filter pool
    listing = db.query(ListingPool).filter(ListingPool.id == listing_pool_id).first()
    if not listing:
        return 0.0, 0.0, 0.0, 0.0
    
    # Get price from filter pool via monitor pool
    from app.models.monitor_pool import MonitorPool
    from app.models.product import FilterPool
    
    price = None
    if listing.monitor_pool_id:
        monitor = db.query(MonitorPool).filter(MonitorPool.id == listing.monitor_pool_id).first()
        if monitor and monitor.filter_pool_id:
            filter_product = db.query(FilterPool).filter(FilterPool.id == monitor.filter_pool_id).first()
            if filter_product:
                price = filter_product.price
    
    if not price:
        return 0.0, 0.0, 0.0, 0.0
    
    # Get profit calculation record for dimensions and weight
    calc = db.query(ProfitCalculation).filter(
        ProfitCalculation.listing_pool_id == listing_pool_id
    ).first()
    
    # Get default config from ProfitConfig
    config = get_or_create_profit_config(db)
    
    # Use values from calc if available, otherwise use defaults or provided values
    weight_kg = Decimal(str(calc.weight)) if calc and calc.weight is not None else Decimal("0")
    length_cm = Decimal(str(calc.length)) if calc and calc.length is not None else Decimal("0")
    width_cm = Decimal(str(calc.width)) if calc and calc.width is not None else Decimal("0")
    height_cm = Decimal(str(calc.height)) if calc and calc.height is not None else Decimal("0")
    
    # Use provided values or defaults from config
    shipping_cost_fixed = Decimal(str(shipping_cost)) if shipping_cost is not None else Decimal(str(config.default_shipping_cost))
    order_fee_val = Decimal(str(order_fee)) if order_fee is not None else Decimal(str(config.default_order_fee))
    storage_fee_val = Decimal(str(storage_fee)) if storage_fee is not None else Decimal(str(config.default_storage_fee))
    commission_rate = Decimal(str(platform_commission / 100)) if platform_commission is not None else Decimal(str(config.default_platform_commission / 100))
    vat_rate = Decimal(str(vat / 100)) if vat is not None else Decimal(str(config.default_vat_rate / 100))
    
    # Optional weight-based shipping
    shipping_price_per_kg = None
    if config.shipping_price_per_kg is not None:
        shipping_price_per_kg = Decimal(str(config.shipping_price_per_kg))
    
    # Call ProfitEngine
    result = ProfitEngine.calculate_profit(
        sale_price_gross=Decimal(str(price)),
        purchase_cost=Decimal(str(purchase_price)),
        weight_kg=weight_kg,
        length_cm=length_cm,
        width_cm=width_cm,
        height_cm=height_cm,
        vat_rate=vat_rate,
        commission_rate=commission_rate,
        shipping_cost_fixed=shipping_cost_fixed,
        order_fee=order_fee_val,
        storage_fee=storage_fee_val,
        shipping_price_per_kg=shipping_price_per_kg,
    )
    
    # Calculate profit margin without VAT for backward compatibility
    # This matches the old calculation: profit_amount / (price - vat_amount) * 100
    price_without_vat = Decimal(str(price)) - result.vat_amount
    profit_margin_without_vat = (
        float(result.net_profit / price_without_vat * 100) if price_without_vat > 0 else 0.0
    )
    
    # Return in the expected format (profit_amount, profit_margin, profit_margin_without_vat, platform_commission_amount)
    return (
        float(result.net_profit),
        float(result.profit_margin * 100),  # Convert to percentage
        profit_margin_without_vat,
        float(result.commission_amount)
    )

# Fee settings endpoints must be defined BEFORE /{listing_id} to avoid path matching conflicts
class FeeSettingsRequest(BaseModel):
    """Fee settings request model"""
    shipping_cost: Optional[float] = None
    order_fee: Optional[float] = None
    storage_fee: Optional[float] = None
    platform_commission: Optional[float] = None
    vat: Optional[float] = None

class FeeSettingsResponse(BaseModel):
    """Fee settings response model"""
    shipping_cost: float
    order_fee: float
    storage_fee: float
    platform_commission: float
    vat: float

def get_or_create_profit_config(db: Session, site: str = "emag_ro") -> ProfitConfig:
    """Get or create profit config for a site"""
    config = db.query(ProfitConfig).filter(ProfitConfig.site == site).first()
    if not config:
        config = ProfitConfig(
            site=site,
            default_shipping_cost=0.0,
            default_order_fee=0.0,
            default_storage_fee=0.0,
            default_platform_commission=0.0,
            default_vat_rate=0.0
        )
        db.add(config)
        db.commit()
        db.refresh(config)
    return config

@router.get("/fee-settings", response_model=FeeSettingsResponse)
async def get_fee_settings(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    site: Optional[str] = Query("emag_ro", description="Site identifier")
):
    """Get fee settings from database"""
    config = get_or_create_profit_config(db, site)
    return FeeSettingsResponse(
        shipping_cost=config.default_shipping_cost,
        order_fee=config.default_order_fee,
        storage_fee=config.default_storage_fee,
        platform_commission=config.default_platform_commission,
        vat=config.default_vat_rate
    )

@router.get("", response_model=ProfitListResponseWrapper)
async def get_profit_list(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    operator_id: Optional[int] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get profit calculation list"""
    from app.services.permission import is_admin
    
    
    
    # Build query - use LEFT JOIN to show all listings, even without profit calculations
    # This allows showing listings that haven't been calculated yet
    query = db.query(ListingPool).outerjoin(ProfitCalculation, ListingPool.id == ProfitCalculation.listing_pool_id)
    
    
    
    # Filter to only show listings that have profit calculations OR are pending calculation
    # This ensures we show listings that are ready for profit calculation
    query = query.filter(
        or_(
            ProfitCalculation.id.isnot(None),  # Has profit calculation
            ListingPool.status == ListingStatus.PENDING_CALC  # Or is pending calculation
        )
    )
    
    
    
    # Filter by user if not admin
    if not is_admin(db, current_user["id"]):
        query = query.filter(ListingPool.created_by_user_id == current_user["id"])
        
    
    # Filter by status
    if status:
        try:
            listing_status = ListingStatus(status)
            query = query.filter(ListingPool.status == listing_status)
            
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status}"
            )
    
    # Filter by operator
    if operator_id:
        query = query.filter(ListingPool.created_by_user_id == operator_id)
    
    # Get total count (before pagination)
    total = query.count()
    
    
    
    # Apply pagination
    skip = (page - 1) * page_size
    listings = query.options(
        joinedload(ListingPool.profit_calc),
        joinedload(ListingPool.user)
    ).order_by(ListingPool.created_at.desc()).offset(skip).limit(page_size).all()
    
    # Build response items
    items = []
    
    for listing in listings:
        calc = listing.profit_calc
        
        # If no profit calculation exists, create one with auto-populated data
        if not calc:
            from app.services.product_info_service import (
                get_product_info_from_listing,
                get_commission_from_category
            )
            from datetime import datetime
            
            # 反查产品信息
            product_info = get_product_info_from_listing(listing.id, db)
            
            # 创建 ProfitCalculation 并填充反查到的信息
            calc = ProfitCalculation(
                listing_pool_id=listing.id,
                category_name=product_info.get('category_name'),
                frontend_price_ron=product_info.get('frontend_price_ron'),
                best_price_ron=product_info.get('best_price_ron'),
            )
            
            # 设置价格来源
            if product_info.get('frontend_price_ron'):
                calc.price_source = 'crawler'
                calc.price_last_updated_at = datetime.utcnow()
            
            # 如果类目名称存在，尝试自动匹配佣金
            if calc.category_name:
                auto_commission = get_commission_from_category(calc.category_name, db)
                if auto_commission:
                    calc.platform_commission = auto_commission
                    calc.commission_source = 'default'
                    calc.commission_last_updated_at = datetime.utcnow()
            
            db.add(calc)
            db.flush()
        
        # 如果已有 ProfitCalculation，但某些字段缺失，尝试自动填充
        else:
            from app.services.product_info_service import populate_profit_calculation_from_listing
            populate_profit_calculation_from_listing(calc, listing, db, force_update=False)
        
        # Get product info via monitor pool
        competitor_image = None
        product_name_ro = None
        price = None
        
        # 优先使用 frontend_price_ron（前端售价），如果没有则从 FilterPool 获取
        # 但图片和名称始终从 FilterPool 获取
        if listing.monitor_pool_id:
            monitor = db.query(MonitorPool).filter(MonitorPool.id == listing.monitor_pool_id).first()
            
            if monitor and monitor.filter_pool_id:
                filter_product = db.query(FilterPool).filter(FilterPool.id == monitor.filter_pool_id).first()
                
                if filter_product:
                    competitor_image = filter_product.thumbnail_image
                    product_name_ro = filter_product.product_name
                    # 价格优先使用 frontend_price_ron，如果没有则使用 FilterPool 的价格
                    if calc and calc.frontend_price_ron:
                        price = calc.frontend_price_ron
                    else:
                        price = filter_product.price
        elif calc and calc.frontend_price_ron:
            # 如果没有 monitor_pool_id，但有 frontend_price_ron，使用它作为价格
            price = calc.frontend_price_ron
        
        # Calculate derived fields using ProfitEngine if we have all required data
        profit_margin_without_vat = None
        profit_margin_calc = None
        platform_commission_amount = None
        vat_amount_calc = None
        logistics_cost_calc = None
        roi_calc = None
        profit_amount_calc = None
        
        # Use ProfitEngine to calculate detailed metrics if we have all required data
        # 注意：price 可能是 None，需要检查 frontend_price_ron
        sale_price_for_calc = price
        if not sale_price_for_calc and calc and calc.frontend_price_ron:
            sale_price_for_calc = calc.frontend_price_ron
        
        if calc and sale_price_for_calc and calc.purchase_price is not None:
            try:
                config = get_or_create_profit_config(db)
                
                # Prepare inputs for ProfitEngine
                weight_kg = Decimal(str(calc.weight)) if calc.weight is not None else Decimal("0")
                length_cm = Decimal(str(calc.length)) if calc.length is not None else Decimal("0")
                width_cm = Decimal(str(calc.width)) if calc.width is not None else Decimal("0")
                height_cm = Decimal(str(calc.height)) if calc.height is not None else Decimal("0")
                
                shipping_cost_fixed = Decimal(str(calc.shipping_cost)) if calc.shipping_cost is not None else Decimal(str(config.default_shipping_cost))
                order_fee_val = Decimal(str(calc.order_fee)) if calc.order_fee is not None else Decimal(str(config.default_order_fee))
                storage_fee_val = Decimal(str(calc.storage_fee)) if calc.storage_fee is not None else Decimal(str(config.default_storage_fee))
                commission_rate = Decimal(str(calc.platform_commission / 100)) if calc.platform_commission is not None else Decimal(str(config.default_platform_commission / 100))
                vat_rate = Decimal(str(calc.vat / 100)) if calc.vat is not None else Decimal(str(config.default_vat_rate / 100))
                
                shipping_price_per_kg = None
                if config.shipping_price_per_kg is not None:
                    shipping_price_per_kg = Decimal(str(config.shipping_price_per_kg))
                
                # Calculate using ProfitEngine
                result = ProfitEngine.calculate_profit(
                    sale_price_gross=Decimal(str(sale_price_for_calc)),
                    purchase_cost=Decimal(str(calc.purchase_price)),
                    weight_kg=weight_kg,
                    length_cm=length_cm,
                    width_cm=width_cm,
                    height_cm=height_cm,
                    vat_rate=vat_rate,
                    commission_rate=commission_rate,
                    shipping_cost_fixed=shipping_cost_fixed,
                    order_fee=order_fee_val,
                    storage_fee=storage_fee_val,
                    shipping_price_per_kg=shipping_price_per_kg,
                )
                
                # Extract calculated values
                platform_commission_amount = float(result.commission_amount)
                vat_amount_calc = float(result.vat_amount)
                logistics_cost_calc = float(result.logistics_cost)
                roi_calc = float(result.roi * 100)  # Convert to percentage
                profit_amount_calc = float(result.net_profit)
                
                # Calculate profit margin (with VAT) - percentage format
                profit_margin_calc = float(result.profit_margin * 100)  # Convert to percentage
                
                # Calculate profit margin without VAT
                price_without_vat = Decimal(str(sale_price_for_calc)) - result.vat_amount
                if price_without_vat > 0:
                    profit_margin_without_vat = float(result.net_profit / price_without_vat * 100)
                
                # Update database with calculated values
                calc.profit_amount = profit_amount_calc
                calc.profit_margin = profit_margin_calc
                db.flush()
            except Exception as e:
                # Log the error for debugging
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Profit calculation failed for listing {listing.id}: {str(e)}", exc_info=True)
                
                # Fallback to simple calculation if ProfitEngine fails
                if calc.platform_commission is not None and sale_price_for_calc:
                    platform_commission_amount = sale_price_for_calc * (calc.platform_commission / 100)
                    if calc.vat is not None:
                        vat_amount_calc = sale_price_for_calc * (calc.vat / 100)
                        price_without_vat = sale_price_for_calc - vat_amount_calc
                        if price_without_vat > 0 and calc.profit_amount is not None:
                            profit_margin_without_vat = (calc.profit_amount / price_without_vat * 100)
                
                # Fallback logistics cost calculation
                if calc.shipping_cost is not None:
                    logistics_cost_calc = calc.shipping_cost
        
        # 尝试自动获取佣金费率
        auto_commission_rate = None
        category_name_for_commission = None
        
        # 优先从calc中获取类目名称
        if calc and calc.category_name:
            category_name_for_commission = calc.category_name
        
        # 根据类目名称获取佣金费率
        if category_name_for_commission:
            auto_commission_rate = get_commission_rate_by_category(category_name_for_commission)
        
        # Handle case where calc is None - use listing.id as temporary id
        if not calc:
            # Create a placeholder response for listings without profit calculation
            items.append(ProfitListResponse(
                id=-listing.id,  # Use negative id to indicate placeholder
                listing_pool_id=listing.id,
                operator_name=listing.user.username if listing.user else None,
                competitor_image=competitor_image,
                product_name_ro=product_name_ro,
                chinese_name=None,
                model_number=None,
                length=None,
                width=None,
                height=None,
                weight=None,
                purchase_price=None,
                sale_price=price,
                profit_amount=None,
                profit_margin=None,
                profit_margin_without_vat=None,
                roi=None,
                category_name=None,
                platform_commission=None,
                auto_commission_rate=None,
                platform_commission_amount=None,
                vat_amount=None,
                logistics_cost=None,
                domestic_logistics=None,
                shipping_cost=None,
                status=listing.status.value if hasattr(listing.status, 'value') else str(listing.status),
                created_at=listing.created_at.isoformat() if listing.created_at else ""
            ))
        else:
            items.append(ProfitListResponse(
                id=calc.id,
                listing_pool_id=calc.listing_pool_id,
                operator_name=listing.user.username if listing.user else None,
                competitor_image=competitor_image,
                product_name_ro=product_name_ro,
                chinese_name=calc.chinese_name,
                model_number=calc.model_number,
                length=calc.length,
                width=calc.width,
                height=calc.height,
                weight=calc.weight,
                purchase_price=calc.purchase_price,
                sale_price=sale_price_for_calc if sale_price_for_calc else price,
                profit_amount=profit_amount_calc if profit_amount_calc is not None else calc.profit_amount,
                profit_margin=profit_margin_calc if profit_margin_calc is not None else calc.profit_margin,
                profit_margin_without_vat=profit_margin_without_vat,
                roi=roi_calc,
                category_name=calc.category_name,
                platform_commission=calc.platform_commission,
                auto_commission_rate=auto_commission_rate,
                platform_commission_amount=platform_commission_amount if platform_commission_amount is not None else None,
                vat_amount=vat_amount_calc if vat_amount_calc is not None else None,
                logistics_cost=logistics_cost_calc if logistics_cost_calc is not None else None,
                domestic_logistics=calc.shipping_cost,
                shipping_cost=calc.shipping_cost,
                status=listing.status.value if hasattr(listing.status, 'value') else str(listing.status),
                created_at=listing.created_at.isoformat() if listing.created_at else ""
            ))
    
    
    return ProfitListResponseWrapper(
        items=items,
        total=total,
        page=page,
        page_size=page_size
    )

@router.put("/fee-settings", response_model=FeeSettingsResponse)
async def update_fee_settings(
    request: FeeSettingsRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    site: Optional[str] = Query("emag_ro", description="Site identifier")
):
    """Update fee settings in database"""
    config = get_or_create_profit_config(db, site)
    
    # Update fee settings
    if request.shipping_cost is not None:
        config.default_shipping_cost = request.shipping_cost
    if request.order_fee is not None:
        config.default_order_fee = request.order_fee
    if request.storage_fee is not None:
        config.default_storage_fee = request.storage_fee
    if request.platform_commission is not None:
        config.default_platform_commission = request.platform_commission
    if request.vat is not None:
        config.default_vat_rate = request.vat
    
    config.updated_by_user_id = current_user["id"]
    db.commit()
    db.refresh(config)
    
    return FeeSettingsResponse(
        shipping_cost=config.default_shipping_cost,
        order_fee=config.default_order_fee,
        storage_fee=config.default_storage_fee,
        platform_commission=config.default_platform_commission,
        vat=config.default_vat_rate
    )

@router.post("/{listing_id}/calculate-enhanced", response_model=EnhancedProfitCalculationResponse)
async def calculate_profit_enhanced(
    listing_id: int,
    request: ProfitCalculationRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """
    增强版利润计算，支持所有成本项和两种利润率
    """
    
    # 获取 listing
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    # 获取或创建 ProfitCalculation
    calc = db.query(ProfitCalculation).filter(
        ProfitCalculation.listing_pool_id == listing_id
    ).first()
    
    if not calc:
        calc = ProfitCalculation(listing_pool_id=listing_id)
        db.add(calc)
        db.flush()
    
    # 自动填充缺失字段
    populate_profit_calculation_from_listing(calc, listing, db, force_update=False)
    
    # 更新字段（如果请求中提供了）
    if request.purchase_price is not None:
        calc.purchase_price = request.purchase_price
    if request.length is not None:
        calc.length = request.length
    if request.width is not None:
        calc.width = request.width
    if request.height is not None:
        calc.height = request.height
    if request.weight is not None:
        calc.weight = request.weight
    if request.frontend_price_ron is not None:
        calc.frontend_price_ron = request.frontend_price_ron
        calc.price_source = 'manual'
        calc.price_last_updated_at = datetime.utcnow()
    if request.transport_mode is not None:
        calc.default_transport_mode = request.transport_mode
    if request.participate_genius is not None:
        calc.is_genius_eligible = request.participate_genius
    if request.packaging_template_id is not None:
        calc.packaging_template_id = request.packaging_template_id
    
    db.commit()
    db.refresh(calc)
    
    # 获取配置
    vat_rate = Decimal(str(request.override_vat)) if request.override_vat else get_current_vat(db=db)
    exchange_rate = Decimal(str(request.override_exchange_rate)) if request.override_exchange_rate else get_current_exchange_rate(db=db)
    transport_mode = calc.default_transport_mode or request.transport_mode or 'air'
    logistics_price_per_kg_rmb = get_logistics_price(transport_mode, db=db)
    
    # 获取佣金费率
    commission_rate = Decimal("0")
    if calc.platform_commission is not None:
        commission_rate = Decimal(str(calc.platform_commission / 100))
    elif calc.category_name:
        auto_commission = get_commission_from_category(calc.category_name, db)
        if auto_commission:
            commission_rate = Decimal(str(auto_commission / 100))
    
    # 获取包材成本
    packaging_cost_rmb = get_packaging_cost(calc.packaging_template_id, db=db)
    
    # 获取 genius 规则
    genius_rule_data = get_active_genius_rule(db=db)
    genius_rule = None
    if genius_rule_data and genius_rule_data.get('steps'):
        genius_rule = GeniusRuleDomain(genius_rule_data['steps'])
    
    # 获取前端售价
    frontend_price_ron = Decimal(str(calc.frontend_price_ron)) if calc.frontend_price_ron else Decimal("0")
    if frontend_price_ron == 0:
        # 如果前端售价为空，尝试从 FilterPool 反查
        product_info = get_product_info_from_listing(listing_id, db)
        if product_info.get('frontend_price_ron'):
            frontend_price_ron = Decimal(str(product_info['frontend_price_ron']))
    
    if frontend_price_ron == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Frontend price is required"
        )
    
    # 获取采购价
    purchase_price_rmb = Decimal(str(calc.purchase_price)) if calc.purchase_price else Decimal("0")
    if purchase_price_rmb == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Purchase price is required"
        )
    
    # 获取尺寸和重量
    weight_kg = Decimal(str(calc.weight)) if calc.weight else Decimal("0")
    length_cm = Decimal(str(calc.length)) if calc.length else Decimal("0")
    width_cm = Decimal(str(calc.width)) if calc.width else Decimal("0")
    height_cm = Decimal(str(calc.height)) if calc.height else Decimal("0")
    
    # 获取订单处理费和仓储费（暂时使用 calc 中的值，后续可以扩展为模板）
    order_handling_fee_rmb = Decimal(str(calc.order_fee)) if calc.order_fee else Decimal("0")
    storage_fee_rmb = Decimal(str(calc.storage_fee)) if calc.storage_fee else Decimal("0")
    
    # 调用增强版利润计算
    result = ProfitEngine.calculate_profit_enhanced(
        sale_price_gross_ron=frontend_price_ron,
        purchase_price_rmb=purchase_price_rmb,
        weight_kg=weight_kg,
        length_cm=length_cm,
        width_cm=width_cm,
        height_cm=height_cm,
        vat_rate=vat_rate,
        commission_rate=commission_rate,
        exchange_rate=exchange_rate,
        logistics_price_per_kg_rmb=logistics_price_per_kg_rmb,
        packaging_cost_rmb=packaging_cost_rmb,
        participate_genius=calc.is_genius_eligible or False,
        genius_rule=genius_rule,
        order_handling_fee_rmb=order_handling_fee_rmb,
        storage_fee_rmb=storage_fee_rmb,
    )
    
    return EnhancedProfitCalculationResponse(
        listing_pool_id=listing_id,
        frontend_price_ron=float(frontend_price_ron),
        purchase_price_rmb=float(purchase_price_rmb),
        revenue_ex_vat_rmb=float(result.revenue_ex_vat_rmb),
        revenue_inc_vat_rmb=float(result.revenue_inc_vat_rmb),
        volumetric_weight_kg=float(result.volumetric_weight_kg),
        chargeable_weight_kg=float(result.chargeable_weight_kg),
        first_leg_logistics_cost_rmb=float(result.first_leg_logistics_cost_rmb),
        commission_fee_ron=float(result.commission_fee_ron),
        commission_fee_rmb=float(result.commission_fee_rmb),
        genius_fee_ron=float(result.genius_fee_ron),
        genius_fee_rmb=float(result.genius_fee_rmb),
        order_handling_fee_rmb=float(result.order_handling_fee_rmb),
        storage_fee_rmb=float(result.storage_fee_rmb),
        packaging_cost_rmb=float(packaging_cost_rmb),
        profit_rmb=float(result.profit_rmb),
        margin_ex_vat=float(result.margin_ex_vat),
        margin_inc_vat=float(result.margin_inc_vat),
    )


@router.get("/{listing_id}", response_model=ProfitCalculationResponse)
async def get_profit_calculation(
    listing_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get profit calculation"""
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    if listing.created_by_user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to view this profit calculation"
        )
    
    calc = db.query(ProfitCalculation).filter(
        ProfitCalculation.listing_pool_id == listing_id
    ).first()
    
    if not calc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Profit calculation not found"
        )
    
    return calc

@router.post("/{listing_id}", response_model=ProfitCalculationResponse)
async def create_profit_calculation(
    listing_id: int,
    request: ProfitCalculationRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create or update profit calculation"""
    # Check permission
    require_product_edit_permission(db, listing_id, current_user["id"])
    
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    # Get or create profit calculation
    calc = db.query(ProfitCalculation).filter(
        ProfitCalculation.listing_pool_id == listing_id
    ).first()
    
    if not calc:
        calc = ProfitCalculation(listing_pool_id=listing_id)
        db.add(calc)
    
    # Update fields
    if request.purchase_price is not None:
        calc.purchase_price = request.purchase_price
    if request.length is not None:
        calc.length = request.length
    if request.width is not None:
        calc.width = request.width
    if request.height is not None:
        calc.height = request.height
    if request.weight is not None:
        calc.weight = request.weight
    if request.shipping_cost is not None:
        calc.shipping_cost = request.shipping_cost
    if request.order_fee is not None:
        calc.order_fee = request.order_fee
    if request.storage_fee is not None:
        calc.storage_fee = request.storage_fee
    if request.platform_commission is not None:
        calc.platform_commission = request.platform_commission
    if request.vat is not None:
        calc.vat = request.vat
    if request.chinese_name is not None:
        calc.chinese_name = request.chinese_name
    if request.model_number is not None:
        calc.model_number = request.model_number
    if request.category_name is not None:
        calc.category_name = request.category_name
    
    # Calculate profit if all required fields are present
    if (
        calc.purchase_price and
        calc.shipping_cost is not None and
        calc.order_fee is not None and
        calc.storage_fee is not None and
        calc.platform_commission is not None and
        calc.vat is not None
    ):
        profit_amount, profit_margin, profit_margin_without_vat, platform_commission_amount = calculate_profit(
            listing_id=listing_id,
            purchase_price=calc.purchase_price,
            shipping_cost=calc.shipping_cost or 0,
            order_fee=calc.order_fee or 0,
            storage_fee=calc.storage_fee or 0,
            platform_commission=calc.platform_commission or 0,
            vat=calc.vat or 0,
            db=db
        )
        calc.profit_amount = profit_amount
        calc.profit_margin = profit_margin
    
    calc.calculated_at = datetime.utcnow()
    db.commit()
    db.refresh(calc)
    
    # Log operation
    try:
        create_operation_log(
            db=db,
            user_id=current_user["id"],
            operation_type="profit_calc",
            target_type="listing_pool",
            target_id=listing_id,
            operation_detail={
                "purchase_price": calc.purchase_price,
                "profit_amount": calc.profit_amount,
                "profit_margin": calc.profit_margin
            }
        )
    except Exception as e:
        # 如果操作日志记录失败，不应该影响主流程
        import traceback
        import json
        log_file = r"d:\emag_erp\.cursor\debug.log"
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"location":"profit.py:543","message":"Error creating operation log","data":{"error_message":str(e),"error_type":type(e).__name__},"timestamp":int(__import__('time').time()*1000),"runId":"initial","hypothesisId":"E"}) + "\n")
    
    # 构造响应对象，确保 calculated_at 是字符串
    return ProfitCalculationResponse(
        id=calc.id,
        listing_pool_id=calc.listing_pool_id,
        purchase_price=calc.purchase_price,
        length=calc.length,
        width=calc.width,
        height=calc.height,
        weight=calc.weight,
        shipping_cost=calc.shipping_cost,
        order_fee=calc.order_fee,
        storage_fee=calc.storage_fee,
        platform_commission=calc.platform_commission,
        vat=calc.vat,
        profit_margin=calc.profit_margin,
        profit_amount=calc.profit_amount,
        chinese_name=calc.chinese_name,
        model_number=calc.model_number,
        category_name=calc.category_name,
        calculated_at=calc.calculated_at.isoformat() if calc.calculated_at else None
    )

@router.put("/{listing_id}", response_model=ProfitCalculationResponse)
async def update_profit_calculation(
    listing_id: int,
    request: ProfitCalculationRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Update profit calculation"""
    import traceback
    import json
    log_file = r"d:\emag_erp\.cursor\debug.log"
    try:
        # #region agent log
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"location":"profit.py:567","message":"Starting update_profit_calculation","data":{"listing_id":listing_id,"request_data":request.dict()},"timestamp":int(__import__('time').time()*1000),"runId":"initial","hypothesisId":"D"}) + "\n")
        # #endregion
        # Check permission
        require_product_edit_permission(db, listing_id, current_user["id"])
        
        result = await create_profit_calculation(listing_id, request, current_user, db)
        # #region agent log
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"location":"profit.py:572","message":"create_profit_calculation success","data":{"listing_id":listing_id,"calc_id":result.id if result else None},"timestamp":int(__import__('time').time()*1000),"runId":"initial","hypothesisId":"D"}) + "\n")
        # #endregion
        return result
    except Exception as e:
        # #region agent log
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"location":"profit.py:576","message":"Error in update_profit_calculation","data":{"error_message":str(e),"error_type":type(e).__name__,"traceback":traceback.format_exc()},"timestamp":int(__import__('time').time()*1000),"runId":"initial","hypothesisId":"D"}) + "\n")
        # #endregion
        raise

@router.put("/{listing_id}/reject", response_model=dict)
async def reject_profit_calculation(
    listing_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Reject profit calculation (mark as rejected)"""
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    # Check permission
    if listing.created_by_user_id != current_user["id"]:
        from app.services.permission import is_admin
        if not is_admin(db, current_user["id"]):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to reject this profit calculation"
            )
    
    # Update status to REJECTED
    old_status = listing.status
    listing.status = ListingStatus.REJECTED
    db.commit()
    db.refresh(listing)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="profit_reject",
        target_type="listing_pool",
        target_id=listing_id,
        operation_detail={
            "old_status": old_status.value if hasattr(old_status, 'value') else str(old_status),
            "new_status": "rejected"
        }
    )
    
    return {"message": "Profit calculation rejected successfully", "status": "rejected"}

"""Profit calculation API"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.listing import ListingPool, ProfitCalculation
from app.services.permission import require_product_edit_permission
from app.services.operation_log_service import create_operation_log

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
    calculated_at: str

    class Config:
        from_attributes = True

def calculate_profit(
    listing_pool_id: int,
    purchase_price: float,
    shipping_cost: float,
    order_fee: float,
    storage_fee: float,
    platform_commission: float,
    vat: float,
    db: Session
) -> tuple[float, float]:
    """
    Calculate profit amount and margin
    Returns: (profit_amount, profit_margin)
    """
    # Get product price from filter pool
    listing = db.query(ListingPool).filter(ListingPool.id == listing_pool_id).first()
    if not listing:
        return 0.0, 0.0
    
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
        return 0.0, 0.0
    
    # Calculate total costs
    total_costs = (
        purchase_price +
        shipping_cost +
        order_fee +
        storage_fee +
        platform_commission +
        vat
    )
    
    # Calculate profit
    profit_amount = price - total_costs
    profit_margin = (profit_amount / price * 100) if price > 0 else 0.0
    
    return profit_amount, profit_margin

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

# In-memory fee settings storage (can be moved to database later)
_fee_settings = {
    "shipping_cost": 0.0,
    "order_fee": 0.0,
    "storage_fee": 0.0,
    "platform_commission": 0.0,
    "vat": 0.0
}

@router.get("/fee-settings", response_model=FeeSettingsResponse)
async def get_fee_settings(
    current_user: dict = Depends(require_auth)
):
    """Get fee settings"""
    
    
    return FeeSettingsResponse(**_fee_settings)

@router.put("/fee-settings", response_model=FeeSettingsResponse)
async def update_fee_settings(
    request: FeeSettingsRequest,
    current_user: dict = Depends(require_auth)
):
    """Update fee settings"""
    
    # Update fee settings
    if request.shipping_cost is not None:
        _fee_settings["shipping_cost"] = request.shipping_cost
    if request.order_fee is not None:
        _fee_settings["order_fee"] = request.order_fee
    if request.storage_fee is not None:
        _fee_settings["storage_fee"] = request.storage_fee
    if request.platform_commission is not None:
        _fee_settings["platform_commission"] = request.platform_commission
    if request.vat is not None:
        _fee_settings["vat"] = request.vat
    
    
    return FeeSettingsResponse(**_fee_settings)

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
    
    # Calculate profit if all required fields are present
    if (
        calc.purchase_price and
        calc.shipping_cost is not None and
        calc.order_fee is not None and
        calc.storage_fee is not None and
        calc.platform_commission is not None and
        calc.vat is not None
    ):
        profit_amount, profit_margin = calculate_profit(
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
    
    return calc

@router.put("/{listing_id}", response_model=ProfitCalculationResponse)
async def update_profit_calculation(
    listing_id: int,
    request: ProfitCalculationRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Update profit calculation"""
    # Check permission
    require_product_edit_permission(db, listing_id, current_user["id"])
    
    return await create_profit_calculation(listing_id, request, current_user, db)

"""Listing pool management API"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.listing import ListingPool, ListingStatus, ListingDetails
from app.models.monitor_pool import MonitorPool
from app.services.permission import require_product_edit_permission, check_product_lock
from app.services.operation_log_service import create_operation_log

router = APIRouter(prefix="/api/listing", tags=["listing"])

class ListingPoolResponse(BaseModel):
    """Listing pool response model"""
    id: int
    monitor_pool_id: Optional[int]
    product_url: str
    status: str
    created_by_user_id: int
    is_locked: bool
    locked_by_user_id: Optional[int]
    locked_at: Optional[str]
    created_at: str

    class Config:
        from_attributes = True

class AddToListingRequest(BaseModel):
    """Add to listing request model"""
    monitor_pool_ids: List[int]

class UpdateStatusRequest(BaseModel):
    """Update status request model"""
    status: str

class ListingDetailsRequest(BaseModel):
    """Listing details request model"""
    image_urls: Optional[List[str]] = None
    competitor_urls: Optional[List[str]] = None
    listing_html: Optional[str] = None

class ListingDetailsResponse(BaseModel):
    """Listing details response model"""
    id: int
    listing_pool_id: int
    image_urls: Optional[List[str]]
    competitor_urls: Optional[List[str]]
    listing_html: Optional[str]
    created_at: str
    updated_at: Optional[str]

    class Config:
        from_attributes = True

class ListingPoolListResponse(BaseModel):
    """Listing pool list response model"""
    items: List[ListingPoolResponse]
    total: int
    skip: int
    limit: int

@router.get("")
async def get_listing_pool(
    status: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    page: int = 1,
    page_size: int = 100,
    skip: Optional[int] = None,
    limit: Optional[int] = None
):
    """Get listing pool"""
    
    # Convert page/page_size to skip/limit if needed
    if skip is None:
        skip = (page - 1) * page_size
    if limit is None:
        limit = page_size
    
    query = db.query(ListingPool).filter(
        ListingPool.created_by_user_id == current_user["id"]
    )
    
    if status:
        try:
            listing_status = ListingStatus(status)
            query = query.filter(ListingPool.status == listing_status)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status}"
            )
    
    # Get total count
    total = query.count()
    
    # Get paginated results
    listings = query.order_by(ListingPool.created_at.desc()).offset(skip).limit(limit).all()
    
    # Serialize listings to ensure datetime fields are converted to strings
    serialized_listings = []
    for listing in listings:
        listing_dict = {
            "id": listing.id,
            "monitor_pool_id": listing.monitor_pool_id,
            "product_url": listing.product_url,
            "status": listing.status.value if hasattr(listing.status, 'value') else str(listing.status),
            "created_by_user_id": listing.created_by_user_id,
            "is_locked": listing.is_locked,
            "locked_by_user_id": listing.locked_by_user_id,
            "locked_at": listing.locked_at.isoformat() if listing.locked_at else None,
            "created_at": listing.created_at.isoformat() if listing.created_at else ""
        }
        serialized_listings.append(listing_dict)
    
    
    return ListingPoolListResponse(
        items=[ListingPoolResponse(**item) for item in serialized_listings],
        total=total,
        skip=skip,
        limit=limit
    )

@router.post("/add", response_model=dict)
async def add_to_listing(
    request: AddToListingRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Add products from monitor pool to listing pool"""
    
    try:
        # Get monitor pool products
        monitors = db.query(MonitorPool).filter(
            MonitorPool.id.in_(request.monitor_pool_ids),
            MonitorPool.created_by_user_id == current_user["id"]
        ).all()
        
        
        if not monitors:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No monitors found"
            )
        
        created_count = 0
        for monitor in monitors:
            # Check if already in listing pool
            existing = db.query(ListingPool).filter(
                ListingPool.product_url == monitor.product_url
            ).first()
            
            if not existing:
                listing = ListingPool(
                    monitor_pool_id=monitor.id,
                    product_url=monitor.product_url,
                    created_by_user_id=current_user["id"],
                    status=ListingStatus.PENDING_CALC
                )
                db.add(listing)
                created_count += 1
        
        db.commit()
        
        
        result = {
            "message": f"Successfully added {created_count} products to listing pool",
            "created_count": created_count
        }
        
        
        # Note: Operation log will be handled by OperationLogMiddleware
        # No need to call create_operation_log here to avoid database session conflicts
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise

@router.put("/{listing_id}/status", response_model=ListingPoolResponse)
async def update_listing_status(
    listing_id: int,
    request: UpdateStatusRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Update listing status"""
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    if listing.created_by_user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to update this listing"
        )
    
    try:
        new_status = ListingStatus(request.status)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid status: {request.status}"
        )
    
    old_status = listing.status
    listing.status = new_status
    
    # Auto-lock when status changes to "listed"
    if new_status == ListingStatus.LISTED and not listing.is_locked:
        listing.is_locked = True
        listing.locked_by_user_id = current_user["id"]
        listing.locked_at = datetime.utcnow()
    
    db.commit()
    db.refresh(listing)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="status_change",
        target_type="listing_pool",
        target_id=listing_id,
        operation_detail={
            "old_status": old_status.value,
            "new_status": new_status.value
        }
    )
    
    return listing

@router.get("/{listing_id}/lock-status", response_model=dict)
async def get_lock_status(
    listing_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get product lock status"""
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    can_edit, error_message = check_product_lock(db, listing_id, current_user["id"])
    
    result = {
        "is_locked": listing.is_locked,
        "can_edit": can_edit,
        "locked_by_user_id": listing.locked_by_user_id,
        "locked_at": listing.locked_at.isoformat() if listing.locked_at else None
    }
    
    if listing.locked_by_user_id:
        from app.models.user import User
        locker = db.query(User).filter(User.id == listing.locked_by_user_id).first()
        result["locked_by_username"] = locker.username if locker else None
    
    if error_message:
        result["error_message"] = error_message
    
    return result

@router.post("/{listing_id}/unlock", response_model=dict)
async def unlock_listing(
    listing_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Unlock listing (admin only)"""
    from app.services.permission import is_admin
    
    if not is_admin(db, current_user["id"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can unlock products"
        )
    
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    listing.is_locked = False
    listing.locked_by_user_id = None
    listing.locked_at = None
    db.commit()
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="product_unlock",
        target_type="listing_pool",
        target_id=listing_id
    )
    
    return {"message": "Product unlocked successfully"}

@router.get("/{listing_id}/details", response_model=ListingDetailsResponse)
async def get_listing_details(
    listing_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get listing details"""
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    if listing.created_by_user_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to view this listing"
        )
    
    details = db.query(ListingDetails).filter(
        ListingDetails.listing_pool_id == listing_id
    ).first()
    
    if not details:
        # Create empty details if not exists
        details = ListingDetails(
            listing_pool_id=listing_id,
            image_urls=[],
            competitor_urls=[],
            listing_html=""
        )
        db.add(details)
        db.commit()
        db.refresh(details)
    
    return details

@router.put("/{listing_id}/details", response_model=ListingDetailsResponse)
async def update_listing_details(
    listing_id: int,
    request: ListingDetailsRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Update listing details"""
    # Check permission
    require_product_edit_permission(db, listing_id, current_user["id"])
    
    listing = db.query(ListingPool).filter(ListingPool.id == listing_id).first()
    if not listing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    
    details = db.query(ListingDetails).filter(
        ListingDetails.listing_pool_id == listing_id
    ).first()
    
    if not details:
        details = ListingDetails(listing_pool_id=listing_id)
        db.add(details)
    
    if request.image_urls is not None:
        details.image_urls = request.image_urls
    if request.competitor_urls is not None:
        details.competitor_urls = request.competitor_urls
    if request.listing_html is not None:
        details.listing_html = request.listing_html
    
    db.commit()
    db.refresh(details)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="listing_edit",
        target_type="listing_pool",
        target_id=listing_id,
        operation_detail={
            "updated_fields": [
                "image_urls" if request.image_urls is not None else None,
                "competitor_urls" if request.competitor_urls is not None else None,
                "listing_html" if request.listing_html is not None else None
            ]
        }
    )
    
    return details


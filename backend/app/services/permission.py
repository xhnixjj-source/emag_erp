"""Permission checking service"""
from typing import Optional
from sqlalchemy.orm import Session
from app.models.user import User, UserRole
from app.models.listing import ListingPool
from fastapi import HTTPException, status


def check_product_lock(db: Session, listing_pool_id: int, current_user_id: int):
    """
    Check if product is locked and if current user can edit it
    Returns: (can_edit, error_message)
    """
    listing = db.query(ListingPool).filter(ListingPool.id == listing_pool_id).first()
    if not listing:
        return False, "Product not found"
    
    if not listing.is_locked:
        return True, None
    
    # Check if current user is the locker or admin
    current_user = db.query(User).filter(User.id == current_user_id).first()
    if not current_user:
        return False, "User not found"
    
    if current_user.role == UserRole.ADMIN:
        return True, None
    
    if listing.locked_by_user_id == current_user_id:
        return True, None
    
    # Get locker username
    locker = db.query(User).filter(User.id == listing.locked_by_user_id).first()
    locker_name = locker.username if locker else "Unknown"
    
    return False, f"该产品已被{locker_name}锁定，无法编辑"


def require_product_edit_permission(db: Session, listing_pool_id: int, current_user_id: int):
    """Require product edit permission, raise exception if not allowed"""
    can_edit, error_message = check_product_lock(db, listing_pool_id, current_user_id)
    if not can_edit:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=error_message or "You don't have permission to edit this product"
        )


def is_admin(db: Session, user_id: int) -> bool:
    """Check if user is admin"""
    user = db.query(User).filter(User.id == user_id).first()
    return user and user.role == UserRole.ADMIN


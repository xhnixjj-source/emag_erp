"""eMAG Marketplace API sync router"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func
from pydantic import BaseModel
from datetime import datetime, date
import io
import csv

from app.database import get_db, SessionLocal
from app.middleware.auth_middleware import require_auth
from app.models.emag_sync import EmagAccount, EmagProduct, EmagOrder, EmagReturn, EmagCategory
from app.services.emag_api_client import EmagAPIClient
from app.services.emag_sync_service import EmagSyncService
from app.services.operation_log_service import create_operation_log

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/emag-sync", tags=["emag-sync"])

# Platform base URLs mapping
PLATFORM_URLS = {
    "ro": "https://marketplace-api.emag.ro/api-3",
    "bg": "https://marketplace-api.emag.bg/api-3",
    "hu": "https://marketplace-api.emag.hu/api-3",
    "fashiondays-ro": "https://marketplace-ro-api.fashiondays.com/api-3",
    "fashiondays-bg": "https://marketplace-bg-api.fashiondays.com/api-3",
}


# Request/Response models
class EmagAccountRequest(BaseModel):
    """eMAG account configuration request"""
    platform: str  # ro, bg, hu, fashiondays-ro, fashiondays-bg
    username: str
    password: str
    shop_id: Optional[int] = None  # 关联店铺 ID（可选）


class EmagAccountResponse(BaseModel):
    """eMAG account configuration response (without password)"""
    id: int
    platform: str
    username: str
    base_url: str
    is_active: int
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True


class SyncResponse(BaseModel):
    """Sync operation response"""
    success: bool
    records_count: int
    error: Optional[str] = None
    message: Optional[str] = None


class SyncAllResponse(BaseModel):
    """Sync all response"""
    success: bool
    results: dict


# API Auth endpoints
@router.post("/auth", response_model=EmagAccountResponse)
async def save_account(
    account_data: EmagAccountRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Save or update eMAG API account configuration"""
    if account_data.platform not in PLATFORM_URLS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid platform. Must be one of: {', '.join(PLATFORM_URLS.keys())}"
        )
    
    base_url = PLATFORM_URLS[account_data.platform]
    
    # Check if account exists for this platform
    account = db.query(EmagAccount).filter(EmagAccount.platform == account_data.platform).first()
    
    if account:
        # Update existing
        account.username = account_data.username
        account.password = account_data.password
        account.base_url = base_url
        account.is_active = 1
        account.updated_at = datetime.utcnow()
    else:
        # Create new
        account = EmagAccount(
            platform=account_data.platform,
            username=account_data.username,
            password=account_data.password,
            base_url=base_url,
            is_active=1
        )
        db.add(account)
    
    db.commit()
    db.refresh(account)
    
    # Log operation
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="emag_account_save",
        target_type="emag_account",
        target_id=account.id,
        operation_detail={"platform": account_data.platform}
    )
    
    return account


@router.get("/auth", response_model=Optional[EmagAccountResponse])
async def get_account(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get eMAG API account configuration (without password)"""
    account = db.query(EmagAccount).filter(EmagAccount.is_active == 1).first()
    return account


@router.post("/auth/test")
async def test_connection(
    account_data: Optional[EmagAccountRequest] = None,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Test eMAG API connection"""
    try:
        if account_data:
            # Test with provided credentials
            if account_data.platform not in PLATFORM_URLS:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid platform"
                )
            base_url = PLATFORM_URLS[account_data.platform]
            client = EmagAPIClient(
                base_url=base_url,
                username=account_data.username,
                password=account_data.password
            )
        else:
            # Test with saved credentials
            account = db.query(EmagAccount).filter(EmagAccount.is_active == 1).first()
            if not account:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No eMAG account configured"
                )
            client = EmagAPIClient(
                base_url=account.base_url,
                username=account.username,
                password=account.password
            )
        
        success, error_message = client.authenticate()
        if success:
            return {"success": True, "message": "Connection successful"}
        else:
            return {"success": False, "message": f"Authentication failed: {error_message}"}
    
    except Exception as e:
        logger.error(f"Connection test failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connection test failed: {str(e)}"
        )


# Background task functions
def run_sync_products(user_id: int, shop_id: int = None):
    """Background task to sync products"""
    db = SessionLocal()
    try:
        service = EmagSyncService(db, shop_id=shop_id)
        result = service.sync_products()
        
        # Log operation
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_products",
            target_type="emag_product",
            operation_detail={**result, "shop_id": shop_id}
        )
        logger.info(f"Product sync completed (shop_id={shop_id}): {result}")
    except Exception as e:
        logger.error(f"Product sync failed: {e}", exc_info=True)
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_products",
            target_type="emag_product",
            operation_detail={"success": False, "error": str(e), "shop_id": shop_id}
        )
    finally:
        db.close()


def run_sync_orders(user_id: int, shop_id: int = None):
    """Background task to sync orders"""
    db = SessionLocal()
    try:
        service = EmagSyncService(db, shop_id=shop_id)
        result = service.sync_orders()
        
        # Log operation
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_orders",
            target_type="emag_order",
            operation_detail={**result, "shop_id": shop_id}
        )
        logger.info(f"Order sync completed (shop_id={shop_id}): {result}")
    except Exception as e:
        logger.error(f"Order sync failed: {e}", exc_info=True)
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_orders",
            target_type="emag_order",
            operation_detail={"success": False, "error": str(e), "shop_id": shop_id}
        )
    finally:
        db.close()


def run_sync_returns(user_id: int, shop_id: int = None):
    """Background task to sync returns"""
    db = SessionLocal()
    try:
        service = EmagSyncService(db, shop_id=shop_id)
        result = service.sync_returns()
        
        # Log operation
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_returns",
            target_type="emag_return",
            operation_detail={**result, "shop_id": shop_id}
        )
        logger.info(f"Return sync completed (shop_id={shop_id}): {result}")
    except Exception as e:
        logger.error(f"Return sync failed: {e}", exc_info=True)
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_returns",
            target_type="emag_return",
            operation_detail={"success": False, "error": str(e), "shop_id": shop_id}
        )
    finally:
        db.close()


def run_sync_all(user_id: int, shop_id: int = None):
    """Background task to sync all data"""
    db = SessionLocal()
    try:
        service = EmagSyncService(db, shop_id=shop_id)
        result = service.sync_all()
        
        # Log operation
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_all",
            target_type="emag_sync",
            operation_detail={**result, "shop_id": shop_id}
        )
        logger.info(f"Full sync completed (shop_id={shop_id}): {result}")
    except Exception as e:
        logger.error(f"Full sync failed: {e}", exc_info=True)
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_all",
            target_type="emag_sync",
            operation_detail={"success": False, "error": str(e), "shop_id": shop_id}
        )
    finally:
        db.close()


def run_sync_categories(user_id: int):
    """Background task to sync categories (RO + EN)"""
    db = SessionLocal()
    try:
        service = EmagSyncService(db)
        result = service.sync_categories()

        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_categories",
            target_type="emag_category",
            operation_detail=result,
        )
        logger.info(f"Category sync completed: {result}")
    except Exception as e:
        logger.error(f"Category sync failed: {e}", exc_info=True)
        create_operation_log(
            db=db,
            user_id=user_id,
            operation_type="emag_sync_categories",
            target_type="emag_category",
            operation_detail={"success": False, "error": str(e)},
        )
    finally:
        db.close()


# Sync endpoints
@router.post("/products", response_model=SyncResponse)
async def sync_products(
    background_tasks: BackgroundTasks,
    shop_id: Optional[int] = Query(None, description="店铺 ID"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Sync products from eMAG API (runs in background)"""
    try:
        background_tasks.add_task(run_sync_products, current_user["id"], shop_id)
        return {
            "success": True,
            "records_count": 0,
            "error": None,
            "message": f"Product sync started in background (shop_id={shop_id})"
        }
    except Exception as e:
        logger.error(f"Failed to start product sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start sync: {str(e)}"
        )


@router.post("/orders", response_model=SyncResponse)
async def sync_orders(
    background_tasks: BackgroundTasks,
    shop_id: Optional[int] = Query(None, description="店铺 ID"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Sync orders from eMAG API (runs in background)"""
    try:
        background_tasks.add_task(run_sync_orders, current_user["id"], shop_id)
        return {
            "success": True,
            "records_count": 0,
            "error": None,
            "message": f"Order sync started in background (shop_id={shop_id})"
        }
    except Exception as e:
        logger.error(f"Failed to start order sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start sync: {str(e)}"
        )


@router.post("/returns", response_model=SyncResponse)
async def sync_returns(
    background_tasks: BackgroundTasks,
    shop_id: Optional[int] = Query(None, description="店铺 ID"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Sync returns from eMAG API (runs in background)"""
    try:
        background_tasks.add_task(run_sync_returns, current_user["id"], shop_id)
        return {
            "success": True,
            "records_count": 0,
            "error": None,
            "message": f"Return sync started in background (shop_id={shop_id})"
        }
    except Exception as e:
        logger.error(f"Failed to start return sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start sync: {str(e)}"
        )


@router.post("/all", response_model=SyncAllResponse)
async def sync_all(
    background_tasks: BackgroundTasks,
    shop_id: Optional[int] = Query(None, description="店铺 ID"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Sync all data (products, orders, returns) - runs in background"""
    try:
        background_tasks.add_task(run_sync_all, current_user["id"], shop_id)
        return {
            "success": True,
            "results": {
                "products": {"success": True, "records_count": 0},
                "orders": {"success": True, "records_count": 0},
                "returns": {"success": True, "records_count": 0}
            },
            "message": f"Full sync started in background (shop_id={shop_id})"
        }
    except Exception as e:
        logger.error(f"Failed to start full sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start sync: {str(e)}"
        )


@router.post("/categories", response_model=SyncResponse)
async def sync_categories(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Sync categories (RO + EN) from eMAG API (runs in background)"""
    try:
        background_tasks.add_task(run_sync_categories, current_user["id"])
        return {
            "success": True,
            "records_count": 0,
            "error": None,
            "message": "Category sync started in background",
        }
    except Exception as e:
        logger.error(f"Failed to start category sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start sync: {str(e)}",
        )


# Data query endpoints
@router.get("/products")
async def get_products(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    pnk_code: Optional[str] = Query(None),
    ean: Optional[str] = Query(None),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get products list with optional filters"""
    query = db.query(EmagProduct)
    
    # Apply filters
    if shop_id is not None:
        query = query.filter(EmagProduct.shop_id == shop_id)
    if pnk_code:
        query = query.filter(EmagProduct.pnk_code == pnk_code)
    if ean:
        query = query.filter(EmagProduct.ean == ean)
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    products = query.order_by(EmagProduct.product_id.desc()).offset(skip).limit(limit).all()
    
    return {
        "items": products,
        "total": total,
        "skip": skip,
        "limit": limit
    }


def _build_category_tree(categories: List[EmagCategory]) -> List[dict]:
    """Build nested tree JSON from flat EmagCategory rows."""
    node_map = {}
    tree: List[dict] = []

    # Prepare nodes
    for c in categories:
        node = {
            "id": c.id,
            "parent_id": c.parent_id,
            "name_ro": c.name_ro,
            "name_en": c.name_en,
            "is_allowed": c.is_allowed,
            "is_ean_mandatory": c.is_ean_mandatory,
            "is_warranty_mandatory": c.is_warranty_mandatory,
            "children": [],
        }
        node_map[c.id] = node

    # Build tree
    for node in node_map.values():
        pid = node["parent_id"]
        if pid is None or pid == 0 or pid not in node_map:
            tree.append(node)
        else:
            node_map[pid]["children"].append(node)

    return tree


@router.get("/categories/tree")
async def get_categories_tree(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Get category tree as nested JSON."""
    categories = db.query(EmagCategory).order_by(EmagCategory.id.asc()).all()
    tree = _build_category_tree(categories)
    return tree


@router.get("/products/export")
async def export_products(
    pnk_code: Optional[str] = Query(None),
    ean: Optional[str] = Query(None),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Export products as CSV"""
    query = db.query(EmagProduct)
    if shop_id is not None:
        query = query.filter(EmagProduct.shop_id == shop_id)
    if pnk_code:
        query = query.filter(EmagProduct.pnk_code == pnk_code)
    if ean:
        query = query.filter(EmagProduct.ean == ean)
        
    def iter_csv():
        # BOM for Excel to recognize UTF-8
        yield b'\xef\xbb\xbf'
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Shop ID", "Product ID", "PNK Code", "EAN", "Part Number", "Name", "Brand", "Sale Price", "Stock", "Status", "Synced At"])
        yield output.getvalue().encode('utf-8')
        output.seek(0)
        output.truncate(0)
        
        for p in query.yield_per(500):
            writer.writerow([
                p.id, p.shop_id, p.product_id, p.pnk_code, p.ean, p.part_number, 
                p.name, p.brand, p.sale_price, p.stock, p.status, 
                p.synced_at.strftime('%Y-%m-%d %H:%M:%S') if p.synced_at else ""
            ])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)
            
    response = StreamingResponse(iter_csv(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=products_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    return response


@router.get("/orders")
async def get_orders(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    pnk_code: Optional[str] = Query(None),
    ean: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    order_status: Optional[int] = Query(None),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get orders list with optional filters"""
    query = db.query(EmagOrder)
    
    # Apply filters
    if shop_id is not None:
        query = query.filter(EmagOrder.shop_id == shop_id)
    if pnk_code:
        query = query.filter(EmagOrder.pnk_code == pnk_code)
    if ean:
        query = query.filter(EmagOrder.ean == ean)
    if order_status is not None:
        query = query.filter(EmagOrder.order_status == order_status)
    if date_start:
        try:
            start_date = datetime.fromisoformat(date_start.replace('Z', '+00:00'))
            query = query.filter(EmagOrder.order_date >= start_date)
        except ValueError:
            pass
    if date_end:
        try:
            end_date = datetime.fromisoformat(date_end.replace('Z', '+00:00'))
            query = query.filter(EmagOrder.order_date <= end_date)
        except ValueError:
            pass
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    orders = query.order_by(EmagOrder.order_date.desc()).offset(skip).limit(limit).all()
    
    return {
        "items": orders,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@router.get("/orders/export")
async def export_orders(
    pnk_code: Optional[str] = Query(None),
    ean: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    order_status: Optional[int] = Query(None),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Export orders as CSV"""
    query = db.query(EmagOrder)
    
    if shop_id is not None:
        query = query.filter(EmagOrder.shop_id == shop_id)
    if pnk_code:
        query = query.filter(EmagOrder.pnk_code == pnk_code)
    if ean:
        query = query.filter(EmagOrder.ean == ean)
    if order_status is not None:
        query = query.filter(EmagOrder.order_status == order_status)
    if date_start:
        try:
            start_date = datetime.fromisoformat(date_start.replace('Z', '+00:00'))
            query = query.filter(EmagOrder.order_date >= start_date)
        except ValueError:
            pass
    if date_end:
        try:
            end_date = datetime.fromisoformat(date_end.replace('Z', '+00:00'))
            query = query.filter(EmagOrder.order_date <= end_date)
        except ValueError:
            pass
            
    def iter_csv():
        yield b'\xef\xbb\xbf'
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "ID", "Shop ID", "Order ID", "Order Date", "Status", "Payment Mode", 
            "Customer Name", "Customer Email", "Customer Phone", "Billing City", "Shipping City", 
            "PNK Code", "EAN", "Product Name", "Quantity", "Sale Price", "Total Amount", "Synced At"
        ])
        yield output.getvalue().encode('utf-8')
        output.seek(0)
        output.truncate(0)
        
        for p in query.yield_per(500):
            writer.writerow([
                p.id, p.shop_id, p.order_id, 
                p.order_date.strftime('%Y-%m-%d %H:%M:%S') if p.order_date else "",
                p.order_status, p.payment_mode_id, p.customer_name, p.customer_email, p.customer_phone,
                p.billing_city, p.shipping_city, p.pnk_code, p.ean, p.product_name, 
                p.quantity, p.sale_price, p.total_amount,
                p.synced_at.strftime('%Y-%m-%d %H:%M:%S') if p.synced_at else ""
            ])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)
            
    response = StreamingResponse(iter_csv(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=orders_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    return response


@router.get("/returns")
async def get_returns(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    pnk_code: Optional[str] = Query(None),
    ean: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    return_status: Optional[int] = Query(None),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get returns list with optional filters"""
    query = db.query(EmagReturn)
    
    # Apply filters
    if shop_id is not None:
        query = query.filter(EmagReturn.shop_id == shop_id)
    if pnk_code:
        query = query.filter(EmagReturn.pnk_code == pnk_code)
    if ean:
        query = query.filter(EmagReturn.ean == ean)
    if return_status is not None:
        query = query.filter(EmagReturn.return_status == return_status)
    if date_start:
        try:
            start_date = datetime.fromisoformat(date_start.replace('Z', '+00:00'))
            query = query.filter(EmagReturn.return_date >= start_date)
        except ValueError:
            pass
    if date_end:
        try:
            end_date = datetime.fromisoformat(date_end.replace('Z', '+00:00'))
            query = query.filter(EmagReturn.return_date <= end_date)
        except ValueError:
            pass
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    returns = query.order_by(EmagReturn.return_date.desc()).offset(skip).limit(limit).all()
    
    return {
        "items": returns,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@router.get("/returns/export")
async def export_returns(
    pnk_code: Optional[str] = Query(None),
    ean: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    return_status: Optional[int] = Query(None),
    shop_id: Optional[int] = Query(None, description="店铺 ID 筛选"),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Export returns as CSV"""
    query = db.query(EmagReturn)
    
    if shop_id is not None:
        query = query.filter(EmagReturn.shop_id == shop_id)
    if pnk_code:
        query = query.filter(EmagReturn.pnk_code == pnk_code)
    if ean:
        query = query.filter(EmagReturn.ean == ean)
    if return_status is not None:
        query = query.filter(EmagReturn.return_status == return_status)
    if date_start:
        try:
            start_date = datetime.fromisoformat(date_start.replace('Z', '+00:00'))
            query = query.filter(EmagReturn.return_date >= start_date)
        except ValueError:
            pass
    if date_end:
        try:
            end_date = datetime.fromisoformat(date_end.replace('Z', '+00:00'))
            query = query.filter(EmagReturn.return_date <= end_date)
        except ValueError:
            pass
            
    def iter_csv():
        yield b'\xef\xbb\xbf'
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "ID", "Shop ID", "RMA ID", "Order ID", "Return Date", "Status", 
            "PNK Code", "EAN", "Product Name", "Quantity", "Sale Price", 
            "Reason", "Synced At"
        ])
        yield output.getvalue().encode('utf-8')
        output.seek(0)
        output.truncate(0)
        
        for p in query.yield_per(500):
            writer.writerow([
                p.id, p.shop_id, p.rma_id, p.order_id,
                p.return_date.strftime('%Y-%m-%d %H:%M:%S') if p.return_date else "",
                p.return_status, p.pnk_code, p.ean, p.product_name, 
                p.quantity, p.sale_price, p.reason,
                p.synced_at.strftime('%Y-%m-%d %H:%M:%S') if p.synced_at else ""
            ])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)
            
    response = StreamingResponse(iter_csv(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=returns_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    return response


@router.get("/sync-status")
async def get_sync_status(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get sync status and statistics"""
    # Get account status
    account = db.query(EmagAccount).filter(EmagAccount.is_active == 1).first()
    
    # Get record counts
    product_count = db.query(EmagProduct).count()
    order_count = db.query(EmagOrder).count()
    return_count = db.query(EmagReturn).count()
    
    # Get last sync times
    last_product_sync = db.query(func.max(EmagProduct.synced_at)).scalar()
    last_order_sync = db.query(func.max(EmagOrder.synced_at)).scalar()
    last_return_sync = db.query(func.max(EmagReturn.synced_at)).scalar()
    
    return {
        "account_configured": account is not None,
        "account_platform": account.platform if account else None,
        "product_count": product_count,
        "order_count": order_count,
        "return_count": return_count,
        "last_product_sync": last_product_sync.isoformat() if last_product_sync else None,
        "last_order_sync": last_order_sync.isoformat() if last_order_sync else None,
        "last_return_sync": last_return_sync.isoformat() if last_return_sync else None,
    }


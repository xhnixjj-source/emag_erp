"""eMAG Marketplace API sync service"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.emag_sync import EmagShop, EmagAccount, EmagProduct, EmagOrder, EmagReturn
from app.services.emag_api_client import EmagAPIClient

logger = logging.getLogger(__name__)


class EmagSyncService:
    """Service for syncing data from eMAG Marketplace API"""
    
    def __init__(self, db: Session, shop_id: Optional[int] = None):
        self.db = db
        self.shop_id = shop_id
        self._client: Optional[EmagAPIClient] = None
    
    def _get_client(self) -> EmagAPIClient:
        """Get or create API client from shop or legacy account configuration"""
        if self._client is None:
            # 优先使用 shop_id 获取凭据
            if self.shop_id:
                shop = self.db.query(EmagShop).filter(EmagShop.id == self.shop_id).first()
                if not shop:
                    raise Exception(f"店铺不存在 (id={self.shop_id})")
                if not shop.api_username or not shop.api_password:
                    raise Exception(f"店铺 '{shop.name}' 未配置 API 凭据")
                self._client = EmagAPIClient(
                    base_url=shop.api_base_url,
                    username=shop.api_username,
                    password=shop.api_password
                )
            else:
                # fallback: 旧的 EmagAccount 表
                account = self.db.query(EmagAccount).filter(EmagAccount.is_active == 1).first()
                if not account:
                    raise Exception("No active eMAG account configured")
                self._client = EmagAPIClient(
                    base_url=account.base_url,
                    username=account.username,
                    password=account.password
                )
        
        return self._client
    
    def _parse_float(self, value) -> Optional[float]:
        """Parse float value, handling empty strings and None"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip()
            if not value or value == '':
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        return None
    
    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from API response"""
        if not date_str:
            return None
        
        try:
            # Try ISO format first (with T separator)
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            
            # Try datetime format: 'YYYY-MM-DD HH:MM:SS'
            if ' ' in date_str and ':' in date_str:
                try:
                    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    # Try with microseconds if present
                    try:
                        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        pass
            
            # Try date only format: 'YYYY-MM-DD'
            return datetime.strptime(date_str, '%Y-%m-%d')
            
        except Exception as e:
            logger.warning(f"Failed to parse datetime '{date_str}': {e}")
            return None
    
    def sync_products(self) -> Dict[str, Any]:
        """
        Full sync products (clear old data for this shop and re-insert)
        
        Returns:
            Dict with sync results: {success: bool, records_count: int, error: str}
        """
        try:
            client = self._get_client()
            
            # Clear old data (only for this shop)
            q = self.db.query(EmagProduct)
            if self.shop_id:
                q = q.filter(EmagProduct.shop_id == self.shop_id)
            deleted_count = q.delete()
            logger.info(f"Deleted {deleted_count} old product records (shop_id={self.shop_id})")
            
            # Sync all pages
            page = 1
            total_records = 0
            
            while True:
                response = client.get_products(current_page=page, items_per_page=100)
                products = response.get('results', [])
                
                if not products:
                    break
                
                for product_data in products:
                    # Extract stock from stock array
                    stock_data = product_data.get('stock', [])
                    stock = 0
                    warehouse_id = None
                    
                    if stock_data and len(stock_data) > 0:
                        first_stock = stock_data[0]
                        stock = first_stock.get('value', 0) if isinstance(first_stock, dict) else 0
                        warehouse_id = first_stock.get('warehouse_id') if isinstance(first_stock, dict) else None
                    
                    # Extract PNK and EAN (may be in part_number or other fields)
                    # Handle cases where these might be lists or strings
                    pnk_code_raw = product_data.get('pnk_code') or product_data.get('part_number')
                    if isinstance(pnk_code_raw, list):
                        pnk_code = str(pnk_code_raw[0]) if pnk_code_raw else None
                    elif pnk_code_raw is not None:
                        pnk_code = str(pnk_code_raw)
                    else:
                        pnk_code = None
                    
                    ean_raw = product_data.get('ean') or product_data.get('barcode')
                    if isinstance(ean_raw, list):
                        ean = str(ean_raw[0]) if ean_raw else None
                    elif ean_raw is not None:
                        ean = str(ean_raw)
                    else:
                        ean = None
                    
                    product = EmagProduct(
                        shop_id=self.shop_id,
                        product_id=product_data.get('id'),
                        pnk_code=pnk_code,
                        ean=ean,
                        part_number=product_data.get('part_number'),
                        name=product_data.get('name'),
                        brand=product_data.get('brand'),
                        category_id=product_data.get('category_id'),
                        sale_price=self._parse_float(product_data.get('sale_price')),
                        vat_id=product_data.get('vat_id'),
                        stock=stock,
                        status=product_data.get('status'),
                        warehouse_id=warehouse_id,
                        synced_at=datetime.utcnow()
                    )
                    self.db.add(product)
                    total_records += 1
                
                # Check if there are more pages
                if len(products) < 100:
                    break
                
                page += 1
            
            self.db.commit()
            logger.info(f"Synced {total_records} products")
            
            return {
                "success": True,
                "records_count": total_records,
                "error": None
            }
        
        except Exception as e:
            self.db.rollback()
            logger.error(f"Product sync failed: {e}", exc_info=True)
            return {
                "success": False,
                "records_count": 0,
                "error": str(e)
            }
    
    def sync_orders(self) -> Dict[str, Any]:
        """
        Full sync orders (clear old data for this shop and re-insert)
        
        Returns:
            Dict with sync results: {success: bool, records_count: int, error: str}
        """
        try:
            client = self._get_client()
            
            # Clear old data (only for this shop)
            q = self.db.query(EmagOrder)
            if self.shop_id:
                q = q.filter(EmagOrder.shop_id == self.shop_id)
            deleted_count = q.delete()
            logger.info(f"Deleted {deleted_count} old order records (shop_id={self.shop_id})")
            
            # Sync all pages
            page = 1
            total_records = 0
            
            while True:
                response = client.get_orders(current_page=page, items_per_page=100)
                orders = response.get('results', [])
                
                if not orders:
                    break
                
                for order_data in orders:
                    order_id = order_data.get('id')
                    
                    # Extract order time fields
                    order_date = self._parse_datetime(order_data.get('date') or order_data.get('created_at'))
                    order_updated_at = self._parse_datetime(order_data.get('updated_at'))
                    order_finalized_at = None
                    order_canceled_at = None
                    
                    # Set time fields based on status
                    status = order_data.get('status')
                    if status == 4:  # Finalized
                        order_finalized_at = order_updated_at
                    elif status == 0:  # Canceled
                        order_canceled_at = order_updated_at
                    
                    # Process products in order
                    for product_data in order_data.get('products', []):
                        product_id = product_data.get('product_id')
                        
                        # Get product info for PNK/EAN
                        product = self.db.query(EmagProduct).filter_by(product_id=product_id).first()
                        pnk_code = product.pnk_code if product else None
                        ean = product.ean if product else None
                        product_name = product.name if product else None
                        
                        # Parse price fields safely
                        sale_price = self._parse_float(product_data.get('sale_price'))
                        quantity = product_data.get('quantity') or 0
                        
                        # Calculate total_amount safely
                        if sale_price is not None and quantity:
                            total_amount = sale_price * quantity
                        else:
                            total_amount = None
                        
                        order = EmagOrder(
                            shop_id=self.shop_id,
                            order_id=order_id,
                            order_product_id=product_data.get('id'),
                            product_id=product_id,
                            pnk_code=pnk_code,
                            ean=ean,
                            order_status=status,
                            payment_mode_id=order_data.get('payment_mode_id'),
                            customer_id=order_data.get('customer', {}).get('id'),
                            customer_name=order_data.get('customer', {}).get('name'),
                            customer_email=order_data.get('customer', {}).get('email'),
                            customer_phone=order_data.get('customer', {}).get('phone'),
                            billing_city=order_data.get('customer', {}).get('billing_city'),
                            shipping_city=order_data.get('customer', {}).get('shipping_city'),
                            product_name=product_name,
                            quantity=quantity,
                            sale_price=sale_price,
                            product_status=product_data.get('status'),
                            total_amount=total_amount,
                            order_date=order_date,
                            order_updated_at=order_updated_at,
                            order_finalized_at=order_finalized_at,
                            order_canceled_at=order_canceled_at,
                            synced_at=datetime.utcnow()
                        )
                        self.db.add(order)
                        total_records += 1
                
                # Check if there are more pages
                if len(orders) < 100:
                    break
                
                page += 1
            
            self.db.commit()
            logger.info(f"Synced {total_records} order records")
            
            return {
                "success": True,
                "records_count": total_records,
                "error": None
            }
        
        except Exception as e:
            self.db.rollback()
            logger.error(f"Order sync failed: {e}", exc_info=True)
            return {
                "success": False,
                "records_count": 0,
                "error": str(e)
            }
    
    def sync_returns(self) -> Dict[str, Any]:
        """
        Full sync returns (clear old data for this shop and re-insert)
        
        Returns:
            Dict with sync results: {success: bool, records_count: int, error: str}
        """
        try:
            client = self._get_client()
            
            # Clear old data (only for this shop)
            q = self.db.query(EmagReturn)
            if self.shop_id:
                q = q.filter(EmagReturn.shop_id == self.shop_id)
            deleted_count = q.delete()
            logger.info(f"Deleted {deleted_count} old return records (shop_id={self.shop_id})")
            
            # Sync all pages
            page = 1
            total_records = 0
            
            while True:
                response = client.get_returns(current_page=page, items_per_page=100)
                returns = response.get('results', [])
                
                if not returns:
                    break
                
                for return_data in returns:
                    # Get rma_id - try different possible field names
                    # API returns 'emag_id' as the RMA ID, not 'id'
                    rma_id = return_data.get('emag_id') or return_data.get('id') or return_data.get('rma_id') or return_data.get('rma')
                    # Also check status_history for rma_id
                    if rma_id is None and 'status_history' in return_data:
                        for status_entry in return_data.get('status_history', []):
                            if 'requests' in status_entry:
                                for request in status_entry.get('requests', []):
                                    if 'rma_id' in request:
                                        rma_id = request.get('rma_id')
                                        break
                            if rma_id:
                                break
                    
                    # Convert to int if it's a string
                    if rma_id is not None:
                        try:
                            rma_id = int(rma_id)
                        except (ValueError, TypeError):
                            rma_id = None
                    
                    # Get order_id and convert to int if it's a string
                    order_id_raw = return_data.get('order_id')
                    order_id = None
                    if order_id_raw is not None:
                        try:
                            order_id = int(order_id_raw)
                        except (ValueError, TypeError):
                            order_id = None
                    
                    # Skip if rma_id is missing (required field)
                    if rma_id is None:
                        logger.warning(f"Skipping return record without rma_id: {return_data}")
                        continue
                    
                    # Extract return time fields
                    return_date = self._parse_datetime(return_data.get('date') or return_data.get('created_at'))
                    return_updated_at = self._parse_datetime(return_data.get('updated_at'))
                    return_acknowledged_at = None
                    return_received_at = None
                    return_resolved_at = None
                    return_rejected_at = None
                    
                    # Set time fields based on status
                    status = return_data.get('status')
                    if status == 2:  # Acknowledged
                        return_acknowledged_at = return_updated_at
                    elif status == 3:  # Received
                        return_received_at = return_updated_at
                    elif status == 4:  # Resolved
                        return_resolved_at = return_updated_at
                    elif status == 5:  # Rejected
                        return_rejected_at = return_updated_at
                    
                    # Process products in return
                    products_list = return_data.get('products', [])
                    if not products_list:
                        # If no products, create a single record with return-level data
                        return_record = EmagReturn(
                            shop_id=self.shop_id,
                            rma_id=rma_id,
                            order_id=order_id,
                            order_product_id=None,
                            product_id=None,
                            pnk_code=None,
                            ean=None,
                            return_status=status,
                            reason=return_data.get('reason'),
                            product_name=None,
                            quantity=None,
                            sale_price=None,
                            return_date=return_date,
                            return_acknowledged_at=return_acknowledged_at,
                            return_received_at=return_received_at,
                            return_resolved_at=return_resolved_at,
                            return_rejected_at=return_rejected_at,
                            return_updated_at=return_updated_at,
                            synced_at=datetime.utcnow()
                        )
                        self.db.add(return_record)
                        total_records += 1
                    else:
                        # Process each product in return
                        for product_data in products_list:
                            # Get product_id and convert to int if it's a string
                            product_id_raw = product_data.get('product_id')
                            product_id = None
                            if product_id_raw is not None:
                                try:
                                    product_id = int(product_id_raw)
                                except (ValueError, TypeError):
                                    product_id = None
                            
                            # Get product info for PNK/EAN
                            product = None
                            if product_id is not None:
                                product = self.db.query(EmagProduct).filter_by(product_id=product_id).first()
                            pnk_code = product.pnk_code if product else None
                            ean = product.ean if product else None
                            product_name = product.name if product else None
                            
                            # Get order_product_id and convert to int if it's a string
                            order_product_id_raw = product_data.get('order_product_id')
                            order_product_id = None
                            if order_product_id_raw is not None:
                                try:
                                    order_product_id = int(order_product_id_raw)
                                except (ValueError, TypeError):
                                    order_product_id = None
                            
                            return_record = EmagReturn(
                                shop_id=self.shop_id,
                                rma_id=rma_id,
                                order_id=order_id,
                                order_product_id=order_product_id,
                                product_id=product_id,
                                pnk_code=pnk_code,
                                ean=ean,
                                return_status=status,
                                reason=return_data.get('reason'),
                                product_name=product_name,
                                quantity=product_data.get('quantity'),
                                sale_price=self._parse_float(product_data.get('sale_price')),
                                return_date=return_date,
                                return_acknowledged_at=return_acknowledged_at,
                                return_received_at=return_received_at,
                                return_resolved_at=return_resolved_at,
                                return_rejected_at=return_rejected_at,
                                return_updated_at=return_updated_at,
                                synced_at=datetime.utcnow()
                            )
                            self.db.add(return_record)
                            total_records += 1
                
                # Check if there are more pages
                if len(returns) < 100:
                    break
                
                page += 1
            
            self.db.commit()
            logger.info(f"Synced {total_records} return records")
            
            return {
                "success": True,
                "records_count": total_records,
                "error": None
            }
        
        except Exception as e:
            self.db.rollback()
            logger.error(f"Return sync failed: {e}", exc_info=True)
            return {
                "success": False,
                "records_count": 0,
                "error": str(e)
            }
    
    def sync_all(self) -> Dict[str, Any]:
        """
        Sync all data (products, orders, returns)
        
        Returns:
            Dict with sync results for each type
        """
        results = {
            "products": self.sync_products(),
            "orders": self.sync_orders(),
            "returns": self.sync_returns()
        }
        
        overall_success = all(r["success"] for r in results.values())
        
        return {
            "success": overall_success,
            "results": results
        }


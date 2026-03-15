"""eMAG Marketplace API Client"""
import base64
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class EmagAPIClient:
    """eMAG Marketplace API Client with rate limiting"""
    
    # Rate limiting constants
    ORDER_RATE_LIMIT = 12  # requests per second
    OTHER_RATE_LIMIT = 3   # requests per second
    
    def __init__(self, base_url: str, username: str, password: str):
        """
        Initialize eMAG API client
        
        Args:
            base_url: API base URL (e.g., https://marketplace-api.emag.ro/api-3)
            username: API username
            password: API password
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.auth = HTTPBasicAuth(username, password)
        
        # Rate limiting tracking
        self._order_request_times = []
        self._other_request_times = []
        self._lock = None  # Could use threading.Lock if needed
    
    def _wait_for_rate_limit(self, is_order_request: bool = False):
        """Wait if necessary to respect rate limits"""
        now = time.time()
        request_times = self._order_request_times if is_order_request else self._other_request_times
        rate_limit = self.ORDER_RATE_LIMIT if is_order_request else self.OTHER_RATE_LIMIT
        
        # Remove requests older than 1 second
        request_times[:] = [t for t in request_times if now - t < 1.0]
        
        # If we've hit the limit, wait
        if len(request_times) >= rate_limit:
            sleep_time = 1.0 - (now - request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                # Clean up again after sleep
                now = time.time()
                request_times[:] = [t for t in request_times if now - t < 1.0]
        
        # Record this request
        request_times.append(time.time())
    
    def _make_request(self, resource: str, action: str, data: Optional[Dict] = None, 
                     method: str = "POST", is_order_request: bool = False) -> Dict[str, Any]:
        """
        Make API request with rate limiting
        
        Args:
            resource: API resource (e.g., 'product_offer', 'order', 'rma')
            action: API action (e.g., 'read', 'save')
            data: Request data (will be wrapped in 'data' key)
            method: HTTP method (default: POST)
            is_order_request: Whether this is an order-related request (for rate limiting)
        
        Returns:
            API response as dictionary
        """
        # Apply rate limiting
        self._wait_for_rate_limit(is_order_request=is_order_request)
        
        url = f"{self.base_url}/{resource}/{action}"
        
        # Prepare request body
        if data is None:
            data = {}
        
        # Wrap data in 'data' key as per API requirement
        payload = {"data": data}
        
        try:
            if method.upper() == "POST":
                response = requests.post(
                    url,
                    json=payload,
                    auth=self.auth,
                    timeout=30
                )
            elif method.upper() == "PATCH":
                response = requests.patch(
                    url,
                    json=payload,
                    auth=self.auth,
                    timeout=30
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            result = response.json()
            
            # Check for API errors
            if result.get('isError', False):
                error_messages = result.get('messages', [])
                error_msg = '; '.join(error_messages) if error_messages else 'Unknown API error'
                raise Exception(f"eMAG API error: {error_msg}")
            
            return result
        
        except requests.exceptions.RequestException as e:
            logger.error(f"eMAG API request failed: {e}")
            raise Exception(f"API request failed: {str(e)}")
    
    def authenticate(self) -> tuple:
        """
        Test API connection
        
        Returns:
            Tuple of (success: bool, error_message: str)
        """
        try:
            # Try to get categories count as a simple test
            result = self._make_request('category', 'count', {})
            return (True, "")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Authentication failed: {e}")
            return (False, error_msg)
    
    def get_products(self, current_page: int = 1, items_per_page: int = 100) -> Dict[str, Any]:
        """
        Get products list
        
        Args:
            current_page: Page number (default: 1)
            items_per_page: Items per page (default: 100, max: 100)
        
        Returns:
            API response with products
        """
        data = {
            "currentPage": current_page,
            "itemsPerPage": min(items_per_page, 100)
        }
        return self._make_request('product_offer', 'read', data)
    
    def get_orders(self, current_page: int = 1, items_per_page: int = 100,
                   order_id: Optional[int] = None, status: Optional[int] = None,
                   date_start: Optional[str] = None, date_end: Optional[str] = None) -> Dict[str, Any]:
        """
        Get orders list
        
        Args:
            current_page: Page number (default: 1)
            items_per_page: Items per page (default: 100, max: 100)
            order_id: Filter by order ID
            status: Filter by status (0=Canceled, 1=New, 2=In Progress, 3=Prepared, 4=Finalized, 5=Returned)
            date_start: Start date (ISO format string)
            date_end: End date (ISO format string)
        
        Returns:
            API response with orders
        """
        data = {
            "currentPage": current_page,
            "itemsPerPage": min(items_per_page, 100)
        }
        
        if order_id:
            data["id"] = order_id
        if status is not None:
            data["status"] = status
        if date_start:
            data["date_start"] = date_start
        if date_end:
            data["date_end"] = date_end
        
        return self._make_request('order', 'read', data, is_order_request=True)
    
    def acknowledge_order(self, order_id: int) -> Dict[str, Any]:
        """
        Acknowledge an order
        
        Args:
            order_id: Order ID to acknowledge
        
        Returns:
            API response
        """
        data = [{"id": order_id}]
        return self._make_request('order', 'acknowledge', data, is_order_request=True)
    
    def get_returns(self, current_page: int = 1, items_per_page: int = 100) -> Dict[str, Any]:
        """
        Get returns (RMA) list
        
        Args:
            current_page: Page number (default: 1)
            items_per_page: Items per page (default: 100, max: 100)
        
        Returns:
            API response with returns
        """
        data = {
            "currentPage": current_page,
            "itemsPerPage": min(items_per_page, 100)
        }
        return self._make_request('rma', 'read', data)
    
    def update_stock(self, product_id: int, warehouse_id: int, value: int) -> Dict[str, Any]:
        """
        Update product stock (PATCH request)
        
        Args:
            product_id: Product ID
            warehouse_id: Warehouse ID
            value: Stock value
        
        Returns:
            API response
        """
        payload = {
            "value": value,
            "warehouse_id": warehouse_id
        }
        return self._make_request(f'offer_stock/{product_id}', '', payload, method='PATCH')


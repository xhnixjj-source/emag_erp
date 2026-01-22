"""Operation log middleware"""
from typing import Callable, Optional
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from app.database import get_db
from app.services.operation_log_service import create_operation_log
from app.middleware.auth_middleware import get_current_user


class OperationLogMiddleware(BaseHTTPMiddleware):
    """Middleware to automatically log API operations"""
    
    # Operations that should be logged
    LOGGED_OPERATIONS = {
        "POST": ["keyword", "filter", "monitor", "listing", "profit"],
        "PUT": ["keyword", "filter", "monitor", "listing", "profit"],
        "DELETE": ["keyword", "filter", "monitor", "listing"],
    }
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and log operation"""
        # Skip logging for GET requests and certain endpoints
        if request.method == "GET" or request.url.path.startswith("/docs") or request.url.path.startswith("/openapi"):
            return await call_next(request)
        
        # Get current user from authorization header
        authorization = request.headers.get("Authorization")
        if not authorization or not authorization.startswith("Bearer "):
            return await call_next(request)
        
        token = authorization.split(" ")[1]
        try:
            from jose import jwt
            import os
            SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("sub")
            if not user_id:
                return await call_next(request)
        except:
            return await call_next(request)
        
        # Get user from database - wrap in try-except to prevent blocking
        user = None
        try:
            db = next(get_db())
            try:
                from app.services.auth_service import get_user_by_id
                user_obj = get_user_by_id(db, user_id)
                if user_obj:
                    user = {"id": user_obj.id, "username": user_obj.username, "role": user_obj.role.value}
            finally:
                db.close()
        except Exception:
            # If user lookup fails, still allow request to proceed
            return await call_next(request)
        
        if not user:
            return await call_next(request)
        
        # Check if this operation should be logged
        path_parts = request.url.path.strip("/").split("/")
        should_log = any(
            part in self.LOGGED_OPERATIONS.get(request.method, [])
            for part in path_parts
        )
        
        if not should_log:
            return await call_next(request)
        
        # Execute request first
        response = await call_next(request)
        
        # Log operation if successful (in background, don't block response)
        if response.status_code < 400:
            try:
                # Use separate db session for logging to avoid conflicts
                db = next(get_db())
                try:
                    # Extract operation details from path and method
                    operation_type = self._extract_operation_type(request.method, path_parts)
                    # Don't try to read request body after response - it may be consumed
                    body = {}
                    target_type, target_id = self._extract_target_info(path_parts, body)
                    
                    create_operation_log(
                        db=db,
                        user_id=user["id"],
                        operation_type=operation_type,
                        target_type=target_type,
                        target_id=target_id,
                        operation_detail={"path": request.url.path, "method": request.method},
                        ip_address=request.client.host if request.client else None
                    )
                except Exception as log_error:
                    # Don't fail request if logging fails
                    pass
                finally:
                    db.close()
            except Exception:
                # Don't fail request if logging fails
                pass
        
        return response
    
    def _extract_operation_type(self, method: str, path_parts: list) -> str:
        """Extract operation type from request"""
        if "keyword" in path_parts:
            return "keyword_add" if method == "POST" else "keyword_update"
        elif "filter" in path_parts:
            return "filter_select" if method == "POST" else "filter_update"
        elif "monitor" in path_parts:
            return "monitor_add" if method == "POST" else "monitor_update"
        elif "listing" in path_parts:
            return "listing_add" if method == "POST" else "listing_edit"
        elif "profit" in path_parts:
            return "profit_calc" if method == "POST" else "profit_update"
        return "unknown"
    
    def _extract_target_info(self, path_parts: list, body: dict) -> tuple[Optional[str], Optional[int]]:
        """Extract target type and ID from request"""
        target_type = None
        target_id = None
        
        if "keyword" in path_parts:
            target_type = "keyword"
            target_id = body.get("id") or body.get("keyword_id")
        elif "filter" in path_parts or "filter_pool" in path_parts:
            target_type = "filter_pool"
            target_id = body.get("id") or body.get("filter_pool_id")
        elif "monitor" in path_parts or "monitor_pool" in path_parts:
            target_type = "monitor_pool"
            target_id = body.get("id") or body.get("monitor_pool_id")
        elif "listing" in path_parts or "listing_pool" in path_parts:
            target_type = "listing_pool"
            target_id = body.get("id") or body.get("listing_pool_id")
        
        return target_type, target_id


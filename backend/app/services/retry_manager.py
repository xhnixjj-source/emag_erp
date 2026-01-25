"""Retry manager with exponential backoff, max retry count control, and error classification"""
import time
import logging
from typing import Optional, Callable, Any, Type
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from app.database import ErrorLog, ErrorType, SessionLocal
from app.config import config

logger = logging.getLogger(__name__)


class RetryManager:
    """Retry manager with exponential backoff and error classification"""
    
    def __init__(
        self,
        max_retries: int = None,
        backoff_base: int = None,
        backoff_max: int = None
    ):
        self.max_retries = max_retries or config.MAX_RETRY_COUNT
        self.backoff_base = backoff_base or config.RETRY_BACKOFF_BASE
        self.backoff_max = backoff_max or config.RETRY_BACKOFF_MAX
    
    def calculate_backoff(self, retry_count: int) -> float:
        """
        Calculate exponential backoff delay
        
        Args:
            retry_count: Current retry count (0-indexed)
        
        Returns:
            Delay in seconds
        """
        delay = min(
            (self.backoff_base ** retry_count),
            self.backoff_max
        )
        return float(delay)
    
    def classify_error(self, error: Exception) -> ErrorType:
        """
        Classify error type
        
        支持Playwright错误类型映射：
        - TimeoutError -> TIMEOUT
        - ConnectionError -> CONNECTION
        - BrowserError -> DISCONNECT
        - 人机验证 -> CAPTCHA
        
        Args:
            error: Exception to classify
        
        Returns:
            ErrorType enum
        """
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        # 导入Playwright错误类型（如果可用）
        try:
            from playwright.sync_api import (
                TimeoutError as PlaywrightTimeoutError,
                Error as PlaywrightError,
            )
            
            # 检查Playwright TimeoutError
            if isinstance(error, PlaywrightTimeoutError):
                return ErrorType.TIMEOUT
            
            # 检查Playwright BrowserError和其他Playwright错误
            if isinstance(error, PlaywrightError):
                error_name = error.name if hasattr(error, 'name') else error_type
                if 'timeout' in error_name.lower() or 'timeout' in error_str:
                    return ErrorType.TIMEOUT
                elif 'connection' in error_name.lower() or 'connection' in error_str:
                    return ErrorType.CONNECTION
                elif 'browser' in error_name.lower() or 'disconnect' in error_str or 'disconnected' in error_str:
                    return ErrorType.DISCONNECT
        except ImportError:
            # Playwright未安装，使用字符串匹配
            pass
        
        # Check for timeout errors
        if "timeout" in error_str or "Timeout" in error_type:
            return ErrorType.TIMEOUT
        
        # Check for connection errors
        if "connection" in error_str or "Connection" in error_type:
            return ErrorType.CONNECTION
        
        # Check for disconnect errors
        if "disconnect" in error_str or "Disconnect" in error_type or "chunked" in error_str:
            return ErrorType.DISCONNECT
        
        # Check for captcha (usually handled separately, but catch here too)
        if "captcha" in error_str or "verification" in error_str:
            return ErrorType.CAPTCHA
        
        return ErrorType.OTHER
    
    def should_retry(
        self,
        error: Exception,
        retry_count: int,
        error_type: Optional[ErrorType] = None
    ) -> bool:
        """
        Determine if task should be retried
        
        Args:
            error: Exception that occurred
            retry_count: Current retry count
            error_type: Classified error type (optional)
        
        Returns:
            True if should retry, False otherwise
        """
        if retry_count >= self.max_retries:
            return False
        
        if error_type is None:
            error_type = self.classify_error(error)
        
        # 验证码错误：允许重试（切换代理后可能可以绕过验证码）
        # 但限制重试次数，避免无限重试
        if error_type == ErrorType.CAPTCHA:
            # 验证码错误最多重试 max_retries 次（通过切换代理）
            return True
        
        return True
    
    def get_retry_delay(
        self,
        error_type: ErrorType,
        retry_count: int
    ) -> float:
        """
        Get retry delay based on error type and retry count
        
        Args:
            error_type: Classified error type
            retry_count: Current retry count
        
        Returns:
            Delay in seconds
        """
        base_delay = self.calculate_backoff(retry_count)
        
        # Adjust delay based on error type
        if error_type == ErrorType.TIMEOUT:
            # Timeout errors: use exponential backoff
            return base_delay
        elif error_type == ErrorType.CONNECTION:
            # Connection errors: slightly longer delay
            return base_delay * 1.5
        elif error_type == ErrorType.DISCONNECT:
            # Disconnect errors: immediate retry with short delay
            return min(base_delay, 2.0)
        elif error_type == ErrorType.CAPTCHA:
            # Captcha errors: longer delay before retry (allow time for proxy rotation)
            # 验证码错误：使用较长的延迟，给代理切换留出时间
            return min(base_delay * 2.0, 10.0)  # 最多等待10秒
        else:
            # Other errors: standard exponential backoff
            return base_delay
    
    def log_error(
        self,
        task_id: Optional[int],
        error: Exception,
        error_type: Optional[ErrorType] = None,
        error_detail: Optional[dict] = None,
        db: Optional[Session] = None
    ) -> int:
        """
        Log error to database
        
        Args:
            task_id: Associated task ID
            error: Exception that occurred
            error_type: Classified error type (optional)
            error_detail: Additional error details (optional)
            db: Database session (optional)
        
        Returns:
            Error log ID
        """
        if error_type is None:
            error_type = self.classify_error(error)
        
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            error_log = ErrorLog(
                task_id=task_id,
                error_type=error_type,
                error_message=str(error),
                error_detail=error_detail or {},
                occurred_at=datetime.utcnow()
            )
            
            db.add(error_log)
            db.commit()
            db.refresh(error_log)
            
            return error_log.id
        except Exception as e:
            logger.error(f"Failed to log error: {e}")
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def resolve_error(
        self,
        error_log_id: int,
        resolution_action: str,
        db: Optional[Session] = None
    ):
        """
        Mark error as resolved
        
        Args:
            error_log_id: Error log ID
            resolution_action: Description of resolution action
            db: Database session (optional)
        """
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            error_log = db.query(ErrorLog).filter(ErrorLog.id == error_log_id).first()
            if error_log:
                error_log.resolved_at = datetime.utcnow()
                error_log.resolution_action = resolution_action
                db.commit()
        except Exception as e:
            logger.error(f"Failed to resolve error: {e}")
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def execute_with_retry(
        self,
        fn: Callable,
        *args,
        task_id: Optional[int] = None,
        error_callback: Optional[Callable] = None,
        **kwargs
    ) -> Any:
        """
        Execute function with retry logic
        
        Args:
            fn: Function to execute
            *args: Function arguments
            task_id: Associated task ID for logging
            error_callback: Callback function called on each retry (optional)
            **kwargs: Function keyword arguments
        
        Returns:
            Function result
        
        Raises:
            Exception: If max retries exceeded
        """
        retry_count = 0
        last_error = None
        
        while retry_count <= self.max_retries:
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_error = e
                error_type = self.classify_error(e)
                
                # Log error
                try:
                    self.log_error(task_id, e, error_type, db=None)
                except Exception as log_error:
                    logger.error(f"Failed to log error: {log_error}")
                
                # Check if should retry
                if not self.should_retry(e, retry_count, error_type):
                    logger.error(
                        f"Task {task_id} failed after {retry_count} retries: {e}"
                    )
                    raise
                
                # Calculate delay
                delay = self.get_retry_delay(error_type, retry_count)
                retry_count += 1
                
                logger.warning(
                    f"Task {task_id} failed (attempt {retry_count}/{self.max_retries + 1}): {e}. "
                    f"Retrying in {delay:.2f}s..."
                )
                
                # Call error callback if provided
                if error_callback:
                    try:
                        error_callback(e, error_type, retry_count, delay)
                    except Exception as cb_error:
                        logger.error(f"Error callback failed: {cb_error}")
                
                # Wait before retry
                time.sleep(delay)
        
        # Max retries exceeded
        logger.error(
            f"Task {task_id} exceeded max retries ({self.max_retries}): {last_error}"
        )
        raise last_error


# Global retry manager instance
retry_manager = RetryManager()


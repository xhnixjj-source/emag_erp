"""Captcha handler for detecting and managing captcha challenges"""
import logging
import time
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from app.database import ErrorLog, ErrorType, CrawlTask, TaskStatus, SessionLocal
from app.config import config

logger = logging.getLogger(__name__)

class CaptchaHandler:
    """Handler for detecting and managing captcha challenges"""
    
    def __init__(self):
        self.detection_enabled = config.CAPTCHA_DETECTION_ENABLED
        self.wait_time = config.CAPTCHA_WAIT_TIME
        self._paused_tasks: Dict[int, datetime] = {}  # task_id -> pause_time
    
    def detect_captcha(self, html_content: str, response_text: str = "") -> bool:
        """
        Detect if page contains captcha challenge
        
        Args:
            html_content: HTML content of the page
            response_text: Response text (optional)
        
        Returns:
            True if captcha detected, False otherwise
        """
        if not self.detection_enabled:
            return False
        
        # Combine content for detection
        content = (html_content + " " + response_text).lower()
        
        # 优先检查HTML title中的验证码标识（最可靠）
        # eMAG验证码页面的title通常是 "eMAG Captcha"
        if '<title>' in content and '</title>' in content:
            title_start = content.find('<title>')
            title_end = content.find('</title>', title_start)
            if title_start >= 0 and title_end > title_start:
                title_text = content[title_start + 7:title_end]
                # 检查title中是否包含验证码关键词
                if 'emag captcha' in title_text or title_text.strip() == 'emag captcha':
                    logger.warning(f"Captcha detected in title: '{title_text}'")
                    return True
        
        # 检查验证码特定的HTML结构（更精确）
        captcha_indicators = [
            "data-sitekey",  # reCAPTCHA site key
            "g-recaptcha-response",  # reCAPTCHA response field
            "hcaptcha-container",  # hCaptcha container
            "cf-challenge",  # Cloudflare challenge
            "challenge-platform",  # Cloudflare challenge platform
            "static.captcha.aws",  # AWS Captcha (eMAG使用)
        ]
        
        for indicator in captcha_indicators:
            if indicator in content:
                logger.warning(f"Captcha detected: found indicator '{indicator}'")
                return True
        
        # 检查特定短语（更严格，避免误报）
        specific_phrases = [
            "verify you're human",
            "i'm not a robot",
            "cloudflare challenge",
            "please complete the security check",
        ]
        
        for phrase in specific_phrases:
            if phrase in content:
                logger.warning(f"Captcha detected: found phrase '{phrase}'")
                return True
        
        return False
    
    def handle_captcha(
        self,
        task_id: int,
        html_content: str = "",
        response_text: str = "",
        error_detail: Optional[Dict[str, Any]] = None,
        db: Optional[Session] = None
    ) -> int:
        """
        Handle captcha detection: pause task and log error
        
        Args:
            task_id: Task ID that encountered captcha
            html_content: HTML content (optional)
            response_text: Response text (optional)
            error_detail: Additional error details (optional)
            db: Database session (optional)
        
        Returns:
            Error log ID
        """
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            # Update task status to paused/retry
            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
            if task:
                task.status = TaskStatus.RETRY
                task.error_message = "Captcha challenge detected"
                task.updated_at = datetime.utcnow()
            
            # Log error
            error_log = ErrorLog(
                task_id=task_id,
                error_type=ErrorType.CAPTCHA,
                error_message="Captcha challenge detected",
                error_detail=error_detail or {
                    "html_snippet": html_content[:500] if html_content else "",
                    "response_snippet": response_text[:500] if response_text else ""
                },
                occurred_at=datetime.utcnow()
            )
            
            db.add(error_log)
            db.commit()
            db.refresh(error_log)
            
            # Track paused task
            self._paused_tasks[task_id] = datetime.utcnow()
            
            logger.warning(
                f"Task {task_id} paused due to captcha challenge. "
                f"Waiting {self.wait_time}s before retry..."
            )
            
            return error_log.id
            
        except Exception as e:
            logger.error(f"Failed to handle captcha for task {task_id}: {e}")
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def is_task_paused(self, task_id: int) -> bool:
        """Check if task is paused due to captcha"""
        return task_id in self._paused_tasks
    
    def can_retry_after_captcha(self, task_id: int) -> bool:
        """
        Check if task can be retried after captcha pause
        
        Args:
            task_id: Task ID to check
        
        Returns:
            True if wait time has elapsed, False otherwise
        """
        if task_id not in self._paused_tasks:
            return True
        
        pause_time = self._paused_tasks[task_id]
        elapsed = (datetime.utcnow() - pause_time).total_seconds()
        
        return elapsed >= self.wait_time
    
    def resume_task_after_captcha(
        self,
        task_id: int,
        db: Optional[Session] = None
    ):
        """
        Resume task after captcha wait period
        
        Args:
            task_id: Task ID to resume
            db: Database session (optional)
        """
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
            if task and task.status == TaskStatus.RETRY:
                # Reset status to pending for retry
                task.status = TaskStatus.PENDING
                task.error_message = None
                task.updated_at = datetime.utcnow()
                db.commit()
            
            # Remove from paused tasks
            self._paused_tasks.pop(task_id, None)
            
            logger.info(f"Task {task_id} resumed after captcha wait period")
            
        except Exception as e:
            logger.error(f"Failed to resume task {task_id}: {e}")
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def resolve_captcha_error(
        self,
        error_log_id: int,
        resolution_action: str = "manual_resolution",
        db: Optional[Session] = None
    ):
        """
        Mark captcha error as resolved
        
        Args:
            error_log_id: Error log ID
            resolution_action: Description of resolution
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
                
                # Resume associated task if exists
                if error_log.task_id:
                    self.resume_task_after_captcha(error_log.task_id, db)
        except Exception as e:
            logger.error(f"Failed to resolve captcha error {error_log_id}: {e}")
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def check_and_resume_paused_tasks(self, db: Optional[Session] = None):
        """
        Check all paused tasks and resume those that have waited long enough
        
        Args:
            db: Database session (optional)
        """
        tasks_to_resume = []
        
        for task_id, pause_time in self._paused_tasks.items():
            if self.can_retry_after_captcha(task_id):
                tasks_to_resume.append(task_id)
        
        for task_id in tasks_to_resume:
            try:
                self.resume_task_after_captcha(task_id, db)
            except Exception as e:
                logger.error(f"Failed to resume task {task_id}: {e}")

# Global captcha handler instance
captcha_handler = CaptchaHandler()


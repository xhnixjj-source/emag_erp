"""Task queue system with priority, state persistence, and resume capability"""
import queue
import threading
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum
from sqlalchemy.orm import Session
from app.database import CrawlTask, TaskStatus, TaskPriority, TaskType, SessionLocal, get_db
from app.config import config


class TaskQueue:
    """Task queue with priority support and state persistence"""
    
    def __init__(self, maxsize: int = None):
        self.maxsize = maxsize or config.TASK_QUEUE_SIZE
        # Priority queue: lower number = higher priority
        # Priority mapping: high=1, normal=2, low=3
        self._queue = queue.PriorityQueue(maxsize=self.maxsize)
        self._lock = threading.Lock()
        self._task_map: Dict[int, CrawlTask] = {}  # task_id -> task
    
    def _get_priority_value(self, priority: TaskPriority) -> int:
        """Convert priority enum to numeric value for queue ordering"""
        priority_map = {
            TaskPriority.HIGH: 1,
            TaskPriority.NORMAL: 2,
            TaskPriority.LOW: 3
        }
        return priority_map.get(priority, 2)
    
    def add_task(
        self,
        task_type: TaskType,
        user_id: int,
        priority: TaskPriority = TaskPriority.NORMAL,
        keyword_id: Optional[int] = None,
        product_url: Optional[str] = None,
        max_retries: int = 5,
        db: Optional[Session] = None
    ) -> int:
        """
        Add a task to the queue and persist to database
        
        Returns:
            task_id: The ID of the created task
        """
        # Create task in database
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            # Calculate queue position
            queue_position = self._queue.qsize() + 1
            
            # Create database record
            task = CrawlTask(
                task_type=task_type,
                keyword_id=keyword_id,
                product_url=product_url,
                status=TaskStatus.PENDING,
                priority=priority,
                user_id=user_id,
                max_retries=max_retries,
                queue_position=queue_position,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            db.add(task)
            db.commit()
            db.refresh(task)
            
            task_id = task.id
            
            # Add to in-memory queue
            priority_value = self._get_priority_value(priority)
            # Use (priority, created_at, task_id) as tuple for ordering
            # Same priority tasks are ordered by creation time (FIFO)
            queue_item = (priority_value, task.created_at.timestamp(), task_id)
            
            with self._lock:
                self._task_map[task_id] = task
                try:
                    self._queue.put_nowait(queue_item)
                except queue.Full:
                    # Queue is full, but task is already in database
                    # Update status to indicate queue full issue
                    task.status = TaskStatus.PENDING
                    task.error_message = "Queue is full"
                    db.commit()
                    raise Exception("Task queue is full")
            
            return task_id
            
        except Exception as e:
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def get_task(self, timeout: Optional[float] = None) -> Optional[int]:
        """
        Get next task from queue (blocking)
        
        Returns:
            task_id or None if timeout
        """
        try:
            queue_item = self._queue.get(timeout=timeout)
            _, _, task_id = queue_item
            
            with self._lock:
                task = self._task_map.get(task_id)
                if task:
                    return task_id
            return None
        except queue.Empty:
            return None
    
    def get_task_non_blocking(self) -> Optional[int]:
        """Get next task from queue (non-blocking)"""
        try:
            queue_item = self._queue.get_nowait()
            _, _, task_id = queue_item
            
            with self._lock:
                task = self._task_map.get(task_id)
                if task:
                    return task_id
            return None
        except queue.Empty:
            return None
    
    def update_task_status(
        self,
        task_id: int,
        status: TaskStatus,
        error_message: Optional[str] = None,
        db: Optional[Session] = None
    ):
        """Update task status in database"""
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
            if task:
                task.status = status
                task.updated_at = datetime.utcnow()
                if error_message:
                    task.error_message = error_message
                if status == TaskStatus.COMPLETED:
                    task.completed_at = datetime.utcnow()
                
                db.commit()
                
                # Update in-memory map
                with self._lock:
                    if task_id in self._task_map:
                        self._task_map[task_id] = task
        except Exception as e:
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def increment_retry_count(self, task_id: int, db: Optional[Session] = None):
        """Increment retry count for a task"""
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
            if task:
                task.retry_count += 1
                task.updated_at = datetime.utcnow()
                db.commit()
                
                with self._lock:
                    if task_id in self._task_map:
                        self._task_map[task_id] = task
        except Exception as e:
            if should_close:
                db.rollback()
            raise
        finally:
            if should_close:
                db.close()
    
    def get_task_info(self, task_id: int, db: Optional[Session] = None) -> Optional[CrawlTask]:
        """Get task information from database"""
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
            return task
        finally:
            if should_close:
                db.close()
    
    def resume_pending_tasks(self, db: Optional[Session] = None):
        """
        Resume pending/retry tasks from database (for recovery after restart)
        This implements the resume capability
        """
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            # Get all pending and retry tasks
            pending_tasks = db.query(CrawlTask).filter(
                CrawlTask.status.in_([TaskStatus.PENDING, TaskStatus.RETRY])
            ).order_by(
                CrawlTask.priority,
                CrawlTask.created_at
            ).all()
            
            resumed_count = 0
            for task in pending_tasks:
                priority_value = self._get_priority_value(task.priority)
                queue_item = (priority_value, task.created_at.timestamp(), task.id)
                
                try:
                    with self._lock:
                        self._task_map[task.id] = task
                        self._queue.put_nowait(queue_item)
                    resumed_count += 1
                except queue.Full:
                    # Queue is full, skip remaining tasks
                    break
            
            return resumed_count
        finally:
            if should_close:
                db.close()
    
    def size(self) -> int:
        """Get current queue size"""
        return self._queue.qsize()
    
    def empty(self) -> bool:
        """Check if queue is empty"""
        return self._queue.empty()
    
    def full(self) -> bool:
        """Check if queue is full"""
        return self._queue.full()
    
    def clear_task_from_map(self, task_id: int):
        """Remove task from in-memory map (after completion)"""
        with self._lock:
            self._task_map.pop(task_id, None)


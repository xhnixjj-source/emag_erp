"""Global task manager with singleton pattern, task scheduling, priority queue, multi-user isolation, and thread safety"""
import threading
import logging
import time
import queue
from typing import Optional, Dict, Callable, Any, List
from datetime import datetime
from sqlalchemy.orm import Session
from app.database import CrawlTask, TaskStatus, TaskPriority, TaskType, SessionLocal
from app.services.task_queue import TaskQueue
from app.utils.thread_pool import thread_pool_manager
from app.config import config

logger = logging.getLogger(__name__)

class TaskManager:
    """Global task manager (singleton pattern)"""
    
    _instance: Optional['TaskManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern implementation"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(TaskManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize task manager (only once)"""
        if self._initialized:
            return
        
        self._initialized = True
        self.task_queue = TaskQueue(maxsize=config.TASK_QUEUE_SIZE)
        self.running = False
        self._worker_threads: Dict[str, threading.Thread] = {}
        self._stop_event = threading.Event()
        self._task_handlers: Dict[TaskType, Callable] = {}
        self._active_tasks: Dict[int, threading.Thread] = {}
        self._active_tasks_lock = threading.Lock()
        self._max_concurrent = config.MAX_CONCURRENT_TASKS
        self._init_lock = threading.Lock()  # Lock for initialization operations
        
        # Resume pending tasks on startup (but don't auto-start workers here)
        # Workers should be started after handlers are registered in startup_event
        if config.TASK_MANAGER_ENABLED:
            self._resume_pending_tasks()
            # Note: Workers will be started in startup_event after handlers are registered
            # or when first task is added via add_task()
    
    def _resume_pending_tasks(self):
        """Resume pending tasks from database (resume capability)"""
        try:
            resumed_count = self.task_queue.resume_pending_tasks()
            if resumed_count > 0:
                logger.info(f"Resumed {resumed_count} pending tasks from database")
        except Exception as e:
            logger.error(f"Failed to resume pending tasks: {e}")
    
    def register_handler(self, task_type: TaskType, handler: Callable):
        """
        Register task handler for specific task type (thread-safe)
        
        Args:
            task_type: Task type enum
            handler: Handler function that takes (task_id, task_info, db) and returns result
        """
        with self._init_lock:
            self._task_handlers[task_type] = handler
            logger.info(f"Registered handler for task type: {task_type}")
    
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
        Add a task to the queue
        
        Args:
            task_type: Type of task
            user_id: User ID who created the task
            priority: Task priority
            keyword_id: Keyword ID (for keyword_search tasks)
            product_url: Product URL (for product_crawl tasks)
            max_retries: Maximum retry count
            db: Database session (optional)
        
        Returns:
            task_id: The ID of the created task
        """
        task_id = self.task_queue.add_task(
            task_type=task_type,
            user_id=user_id,
            priority=priority,
            keyword_id=keyword_id,
            product_url=product_url,
            max_retries=max_retries,
            db=db
        )
        
        print(f"[任务添加] 任务已添加到队列 - 任务ID: {task_id}, 类型: {task_type}, 优先级: {priority}, 用户ID: {user_id}, 产品URL: {product_url}")
        logger.info(
            f"Added task {task_id} (type={task_type}, priority={priority}, user={user_id})"
        )
        
        # Start worker if not running
        if not self.running and config.TASK_MANAGER_ENABLED:
            print(f"[任务管理器] 启动任务管理器 - 当前运行状态: {self.running}, 启用状态: {config.TASK_MANAGER_ENABLED}")
            self.start()
        else:
            print(f"[任务管理器] 任务管理器已运行或未启用 - 运行状态: {self.running}, 启用状态: {config.TASK_MANAGER_ENABLED}")
        
        return task_id
    
    def _execute_task(self, task_id: int):
        """Execute a single task"""
        start_time = time.time()
        # 强制输出到控制台，确保日志可见
        print(f"[任务执行] 开始执行任务 - 任务ID: {task_id}")
        logger.info(f"[任务执行] 开始执行任务 - 任务ID: {task_id}")
        
        db = SessionLocal()
        try:
            # Get task info
            task = self.task_queue.get_task_info(task_id, db)
            if not task:
                logger.error(f"[任务执行] 任务未找到 - 任务ID: {task_id}")
                return
            
            # Check if handler exists
            
            if task.task_type not in self._task_handlers:
                logger.error(f"No handler registered for task type: {task.task_type}")
                self.task_queue.update_task_status(
                    task_id, TaskStatus.FAILED,
                    error_message=f"No handler for task type: {task.task_type}",
                    db=db
                )
                return
            
            # Update status to processing
            self.task_queue.update_task_status(task_id, TaskStatus.PROCESSING, db=db)
            
            # Get handler
            handler = self._task_handlers[task.task_type]
            
            
            # Execute task
            try:
                task_url = getattr(task, 'product_url', None) or (f"关键字ID: {task.keyword_id}" if task.keyword_id else "未知")
                print(f"[任务执行] 调用任务处理器 - 任务ID: {task_id}, 任务类型: {task.task_type}, 目标: {task_url}")
                logger.info(f"[任务执行] 调用任务处理器 - 任务ID: {task_id}, 任务类型: {task.task_type}, 目标: {task_url}")
                result = handler(task_id, task, db)
                
                
                # Mark as completed
                self.task_queue.update_task_status(task_id, TaskStatus.COMPLETED, db=db)
                elapsed = time.time() - start_time
                logger.info(f"[任务执行] 任务执行完成 - 任务ID: {task_id}, 任务类型: {task.task_type}, 耗时: {elapsed:.2f}秒")
                
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"[任务执行] 任务执行失败 - 任务ID: {task_id}, 任务类型: {task.task_type}, 错误: {str(e)}, 耗时: {elapsed:.2f}秒", exc_info=True)
                
                # 记录错误到ErrorLog（如果还没有记录）
                try:
                    from app.services.retry_manager import retry_manager
                    from app.models.crawl_task import ErrorType
                    error_type = retry_manager.classify_error(e)
                    retry_manager.log_error(task_id, e, error_type, db=db)
                except Exception as log_err:
                    logger.error(f"记录任务错误失败: {log_err}")
                
                # 不立即重试，标记为FAILED，等待批量重试
                self.task_queue.update_task_status(
                    task_id, TaskStatus.FAILED,
                    error_message=f"Task failed: {str(e)[:500]}",
                    db=db
                )
                logger.info(f"Task {task_id} marked as FAILED, will be retried in batch retry phase")
        
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {e}", exc_info=True)
        finally:
            # Remove from active tasks
            with self._active_tasks_lock:
                self._active_tasks.pop(task_id, None)
            
            # Remove from task map
            self.task_queue.clear_task_from_map(task_id)
            
            db.close()
    
    def _worker_loop(self, worker_name: str):
        """Worker thread loop"""
        print(f"[工作线程] 工作线程启动 - {worker_name}")
        logger.info(f"Worker thread {worker_name} started")
        
        # 批量重试检查间隔（秒），避免频繁检查
        last_batch_retry_check = 0
        batch_retry_check_interval = 10  # 每10秒检查一次
        
        while not self._stop_event.is_set():
            try:
                # Check active task count
                with self._active_tasks_lock:
                    active_count = len(self._active_tasks)
                
                if active_count >= self._max_concurrent:
                    # Wait a bit if too many active tasks
                    time.sleep(0.1)
                    continue
                
                # Get next task (non-blocking)
                task_id = self.task_queue.get_task_non_blocking()
                
                if task_id is None:
                    # No tasks available, check if we should retry failed tasks
                    # Only retry if queue is empty AND no active tasks
                    current_time = time.time()
                    with self._active_tasks_lock:
                        active_count = len(self._active_tasks)
                    
                    # 定期检查批量重试（避免频繁检查）
                    if (active_count == 0 and 
                        self.task_queue.empty() and 
                        current_time - last_batch_retry_check >= batch_retry_check_interval):
                        # All tasks completed, try to retry failed tasks
                        try:
                            db = SessionLocal()
                            retried_count = self.task_queue.retry_failed_tasks(db=db, max_tasks=50)
                            if retried_count > 0:
                                logger.info(f"[批量重试] 重试了 {retried_count} 个失败的任务")
                                print(f"[批量重试] 重试了 {retried_count} 个失败的任务")
                                last_batch_retry_check = current_time
                                # Continue immediately to process retried tasks
                                continue
                            db.close()
                            last_batch_retry_check = current_time
                        except Exception as retry_err:
                            logger.error(f"批量重试失败任务时出错: {retry_err}", exc_info=True)
                            last_batch_retry_check = current_time
                    
                    # No tasks available, wait a bit
                    time.sleep(0.5)
                    continue
                
                
                print(f"[任务调度] 工作线程获取到任务 - 工作线程: {worker_name}, 任务ID: {task_id}")
                logger.info(f"[任务调度] 工作线程获取到任务 - 工作线程: {worker_name}, 任务ID: {task_id}")
                
                # Add to active tasks
                with self._active_tasks_lock:
                    self._active_tasks[task_id] = threading.current_thread()
                
                # Execute task in thread pool
                pool_name = "task_execution"
                logger.debug(f"[任务调度] 提交任务到线程池 - 任务ID: {task_id}, 线程池: {pool_name}")
                thread_pool_manager.submit(
                    pool_name,
                    self._execute_task,
                    task_id
                )
                
            except Exception as e:
                logger.error(f"Error in worker loop {worker_name}: {e}", exc_info=True)
                time.sleep(1)
        
        logger.info(f"Worker thread {worker_name} stopped")
    
    def start(self, num_workers: int = 3):
        """
        Start task manager workers (thread-safe)
        
        Args:
            num_workers: Number of worker threads
        """
        with self._init_lock:
            if self.running:
                print(f"[任务管理器] 任务管理器已在运行")
                logger.warning("Task manager is already running")
                return
            
            self.running = True
            self._stop_event.clear()
            
            print(f"[任务管理器] 启动任务管理器 - 工作线程数: {num_workers}")
            for i in range(num_workers):
                worker_name = f"worker-{i+1}"
                thread = threading.Thread(
                    target=self._worker_loop,
                    args=(worker_name,),
                    daemon=True,
                    name=worker_name
                )
                thread.start()
                self._worker_threads[worker_name] = thread
                print(f"[任务管理器] 工作线程已启动 - {worker_name}")
            
            print(f"[任务管理器] 任务管理器启动完成 - 工作线程数: {num_workers}")
            logger.info(f"Task manager started with {num_workers} workers")
    
    def stop(self, wait: bool = True):
        """
        Stop task manager (thread-safe)
        
        Args:
            wait: Whether to wait for tasks to complete
        """
        with self._init_lock:
            if not self.running:
                return
            
            logger.info("Stopping task manager...")
            self.running = False
            self._stop_event.set()
            
            if wait:
                # Wait for worker threads
                for name, thread in list(self._worker_threads.items()):
                    thread.join(timeout=5)
                    logger.info(f"Worker thread {name} stopped")
            
            self._worker_threads.clear()
            logger.info("Task manager stopped")
    
    def get_task_status(self, task_id: int, db: Optional[Session] = None) -> Optional[CrawlTask]:
        """Get task status"""
        return self.task_queue.get_task_info(task_id, db)
    
    def get_task(self, db: Session, task_id: int) -> Optional[CrawlTask]:
        """
        Get a single task by ID
        
        Args:
            db: Database session
            task_id: Task ID
            
        Returns:
            CrawlTask or None if not found
        """
        return db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
    
    def get_user_tasks(
        self,
        db: Session,
        user_id: int,
        status: Optional[TaskStatus] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[CrawlTask]:
        """
        Get tasks for a specific user (multi-user isolation)
        
        Args:
            db: Database session
            user_id: User ID to filter tasks
            status: Optional status filter
            skip: Number of records to skip (pagination)
            limit: Maximum number of records to return
            
        Returns:
            List of CrawlTask objects
        """
        query = db.query(CrawlTask).filter(CrawlTask.user_id == user_id)
        
        if status:
            query = query.filter(CrawlTask.status == status)
        
        # Order by priority and creation time
        query = query.order_by(
            CrawlTask.priority,
            CrawlTask.created_at.desc()
        )
        
        return query.offset(skip).limit(limit).all()
    
    def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.task_queue.size()
    
    def get_active_task_count(self) -> int:
        """Get number of active tasks"""
        with self._active_tasks_lock:
            return len(self._active_tasks)
    
    def retry_task(
        self,
        task_id: int,
        user_id: int,
        db: Optional[Session] = None
    ) -> bool:
        """
        Retry a failed or retry task (with user ownership check)
        
        Args:
            task_id: Task ID to retry
            user_id: User ID requesting retry
            db: Database session (optional)
            
        Returns:
            True if retry was successful, False otherwise
        """
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            task = self.get_task(db, task_id)
            if not task:
                logger.warning(f"Task {task_id} not found for retry")
                return False
            
            # Check ownership (multi-user isolation)
            if task.user_id != user_id:
                logger.warning(
                    f"User {user_id} attempted to retry task {task_id} owned by {task.user_id}"
                )
                return False
            
            # Only retry failed or retry tasks
            if task.status not in [TaskStatus.FAILED, TaskStatus.RETRY]:
                logger.warning(
                    f"Cannot retry task {task_id} with status {task.status}"
                )
                return False
            
            # Check if max retries exceeded
            if task.retry_count >= task.max_retries:
                logger.warning(
                    f"Cannot retry task {task_id}: max retries ({task.max_retries}) exceeded"
                )
                return False
            
            # Reset task status and increment retry count
            task.status = TaskStatus.PENDING
            task.retry_count += 1
            task.error_message = None
            task.updated_at = datetime.utcnow()
            db.commit()
            
            # Re-add to queue with high priority
            priority_value = self.task_queue._get_priority_value(TaskPriority.HIGH)
            queue_item = (priority_value, datetime.utcnow().timestamp(), task.id)
            
            try:
                with self.task_queue._lock:
                    self.task_queue._task_map[task.id] = task
                    self.task_queue._queue.put_nowait(queue_item)
                logger.info(f"Task {task_id} queued for retry ({task.retry_count}/{task.max_retries})")
                return True
            except queue.Full:
                logger.error(f"Queue is full, cannot retry task {task_id}")
                task.status = TaskStatus.FAILED
                task.error_message = "Retry queue is full"
                db.commit()
                return False
                
        except Exception as e:
            logger.error(f"Failed to retry task {task_id}: {e}", exc_info=True)
            if should_close:
                db.rollback()
            return False
        finally:
            if should_close:
                db.close()
    
    def cancel_task(self, task_id: int, user_id: int, db: Optional[Session] = None) -> bool:
        """
        Cancel a task (only if pending and owned by user)
        
        Args:
            task_id: Task ID to cancel
            user_id: User ID requesting cancellation
            db: Database session (optional)
        
        Returns:
            True if cancelled, False otherwise
        """
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            task = self.task_queue.get_task_info(task_id, db)
            if not task:
                return False
            
            # Check ownership
            if task.user_id != user_id:
                logger.warning(f"User {user_id} attempted to cancel task {task_id} owned by {task.user_id}")
                return False
            
            # Only cancel pending tasks
            if task.status not in [TaskStatus.PENDING, TaskStatus.RETRY]:
                logger.warning(f"Cannot cancel task {task_id} with status {task.status}")
                return False
            
            # Update status
            self.task_queue.update_task_status(
                task_id, TaskStatus.CANCELLED,
                error_message="Cancelled by user",
                db=db
            )
            
            logger.info(f"Task {task_id} cancelled by user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel task {task_id}: {e}")
            return False
        finally:
            if should_close:
                db.close()

# Global task manager instance (singleton)
task_manager = TaskManager()

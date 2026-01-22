"""Thread pool management utility"""
import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Callable, Optional, Any, Dict, List
from app.config import config


class ThreadPoolManager:
    """Thread pool manager with thread-safe control"""
    
    def __init__(self):
        self._pools: Dict[str, ThreadPoolExecutor] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._active_tasks: Dict[str, List[Future]] = {}
    
    def get_pool(
        self,
        pool_name: str,
        max_workers: Optional[int] = None
    ) -> ThreadPoolExecutor:
        """
        Get or create a thread pool
        
        Args:
            pool_name: Name of the pool (e.g., 'keyword_search', 'product_crawl')
            max_workers: Maximum number of worker threads
        
        Returns:
            ThreadPoolExecutor instance
        """
        if pool_name not in self._pools:
            if max_workers is None:
                # Use default based on pool name
                max_workers = self._get_default_workers(pool_name)
            
            with self._get_lock(pool_name):
                if pool_name not in self._pools:
                    self._pools[pool_name] = ThreadPoolExecutor(max_workers=max_workers)
                    self._active_tasks[pool_name] = []
        
        return self._pools[pool_name]
    
    def _get_default_workers(self, pool_name: str) -> int:
        """Get default worker count for pool name"""
        defaults = {
            "keyword_search": config.KEYWORD_SEARCH_THREADS,
            "product_crawl": config.PRODUCT_CRAWL_THREADS,
            "monitor": config.MONITOR_THREADS,
        }
        return defaults.get(pool_name, config.MAX_WORKER_THREADS)
    
    def _get_lock(self, pool_name: str) -> threading.Lock:
        """Get lock for pool name"""
        if pool_name not in self._locks:
            self._locks[pool_name] = threading.Lock()
        return self._locks[pool_name]
    
    def submit(
        self,
        pool_name: str,
        fn: Callable,
        *args,
        **kwargs
    ) -> Future:
        """
        Submit a task to thread pool
        
        Args:
            pool_name: Name of the pool
            fn: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
        
        Returns:
            Future object
        """
        pool = self.get_pool(pool_name)
        future = pool.submit(fn, *args, **kwargs)
        
        # Track active tasks
        with self._get_lock(pool_name):
            self._active_tasks[pool_name].append(future)
            # Clean up completed futures
            self._active_tasks[pool_name] = [
                f for f in self._active_tasks[pool_name] if not f.done()
            ]
        
        return future
    
    def submit_batch(
        self,
        pool_name: str,
        fn: Callable,
        args_list: List[tuple],
        **kwargs
    ) -> List[Future]:
        """
        Submit multiple tasks to thread pool
        
        Args:
            pool_name: Name of the pool
            fn: Function to execute
            args_list: List of argument tuples
            **kwargs: Common keyword arguments for all tasks
        
        Returns:
            List of Future objects
        """
        futures = []
        for args in args_list:
            if isinstance(args, tuple):
                future = self.submit(pool_name, fn, *args, **kwargs)
            else:
                future = self.submit(pool_name, fn, args, **kwargs)
            futures.append(future)
        
        return futures
    
    def wait_for_completion(
        self,
        pool_name: str,
        futures: Optional[List[Future]] = None,
        timeout: Optional[float] = None
    ) -> List[Any]:
        """
        Wait for futures to complete and return results
        
        Args:
            pool_name: Name of the pool
            futures: List of futures to wait for (if None, wait for all active)
            timeout: Maximum time to wait
        
        Returns:
            List of results (exceptions are raised)
        """
        if futures is None:
            with self._get_lock(pool_name):
                futures = self._active_tasks[pool_name].copy()
        
        results = []
        for future in as_completed(futures, timeout=timeout):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                # Re-raise exception
                raise
        
        return results
    
    def get_active_count(self, pool_name: str) -> int:
        """Get number of active tasks in pool"""
        with self._get_lock(pool_name):
            active = [f for f in self._active_tasks.get(pool_name, []) if not f.done()]
            return len(active)
    
    def shutdown(self, pool_name: Optional[str] = None, wait: bool = True):
        """
        Shutdown thread pool(s)
        
        Args:
            pool_name: Name of pool to shutdown (None for all)
            wait: Whether to wait for tasks to complete
        """
        if pool_name:
            pools_to_shutdown = [pool_name]
        else:
            pools_to_shutdown = list(self._pools.keys())
        
        for name in pools_to_shutdown:
            if name in self._pools:
                with self._get_lock(name):
                    self._pools[name].shutdown(wait=wait)
                    del self._pools[name]
                    if name in self._active_tasks:
                        del self._active_tasks[name]
    
    def shutdown_all(self, wait: bool = True):
        """Shutdown all thread pools"""
        self.shutdown(wait=wait)


# Global thread pool manager instance
thread_pool_manager = ThreadPoolManager()


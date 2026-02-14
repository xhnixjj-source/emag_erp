"""FastAPI application main file"""
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.database import init_db

# 配置日志系统 - 同时输出到文件和控制台
def setup_logging():
    """配置日志系统，同时输出到文件和控制台"""
    # 创建logs目录（如果不存在）
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 日志文件路径
    log_file = os.path.join(log_dir, 'crawler.log')
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 清除已有的处理器
    root_logger.handlers.clear()
    
    # 文件处理器 - 使用轮转文件处理器，每个文件最大10MB，保留5个备份
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到根日志记录器
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 设置特定库的日志级别
    logging.getLogger('uvicorn').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    logging.getLogger('playwright').setLevel(logging.WARNING)
    
    logging.info(f"日志系统初始化完成 - 日志文件: {log_file}")

# 初始化日志系统
setup_logging()

from app.routers import auth, keywords, filter_pool, monitor, listing, profit, operation_log, failed_tasks
from app.middleware.operation_log_middleware import OperationLogMiddleware
from app.services.scheduler import start_scheduler

app = FastAPI(
    title="EMAG ERP System",
    description="EMAG选品上架管理系统",
    version="1.0.0"
)

# #region agent log - request logging middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
class RequestLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # #region agent log
        try:
            import json as _json_req, time as _time_req
            _entry = {
                "id": f"req_{int(_time_req.time() * 1000)}",
                "timestamp": int(_time_req.time() * 1000),
                "location": "main.py:RequestLogMiddleware",
                "message": "incoming request",
                "data": {
                    "method": request.method,
                    "path": str(request.url.path),
                    "query": str(request.url.query)
                },
                "runId": "pre-fix-1",
                "hypothesisId": "H2"
            }
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_req.dumps(_entry, ensure_ascii=False, default=str) + "\n")
        except Exception:
            pass
        # #endregion
        response = await call_next(request)
        # #region agent log
        try:
            import json as _json_resp, time as _time_resp
            _entry = {
                "id": f"resp_{int(_time_resp.time() * 1000)}",
                "timestamp": int(_time_resp.time() * 1000),
                "location": "main.py:RequestLogMiddleware",
                "message": "response",
                "data": {
                    "method": request.method,
                    "path": str(request.url.path),
                    "status_code": response.status_code
                },
                "runId": "pre-fix-1",
                "hypothesisId": "H2"
            }
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_resp.dumps(_entry, ensure_ascii=False, default=str) + "\n")
        except Exception:
            pass
        # #endregion
        return response
app.add_middleware(RequestLogMiddleware)
# #endregion

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Operation log middleware
app.add_middleware(OperationLogMiddleware)

# Include routers
app.include_router(auth.router)
app.include_router(keywords.router)
app.include_router(filter_pool.router)
app.include_router(monitor.router)
app.include_router(listing.router)
app.include_router(profit.router)
app.include_router(operation_log.router)
app.include_router(failed_tasks.router)

@app.on_event("startup")
async def startup_event():
    """Initialize database, register task handlers, and start scheduler on startup"""
    init_db()
    
    # Register crawler task handlers with task manager
    from app.services.task_manager import task_manager
    from app.services.crawler import handle_keyword_search_task, handle_product_crawl_task
    from app.database import TaskType
    from app.config import config
    
    # Register handlers FIRST before starting workers
    task_manager.register_handler(TaskType.KEYWORD_SEARCH, handle_keyword_search_task)
    task_manager.register_handler(TaskType.PRODUCT_CRAWL, handle_product_crawl_task)
    
    # Start TaskManager workers AFTER handlers are registered
    # This ensures handlers are available when tasks are executed
    if config.TASK_MANAGER_ENABLED and not task_manager.running:
        # Check if there are pending tasks to process
        if task_manager.get_queue_size() > 0:
            task_manager.start()
    
    # Start scheduler (which includes setting up daily monitor task)
    start_scheduler()
    
    # ⚠️ 关键修复：预初始化Playwright（在后台线程中）
    # 这样可以避免首次爬取时的延迟
    def _pre_init_playwright():
        """预初始化Playwright，避免首次使用时延迟"""
        try:
            import logging
            logger = logging.getLogger(__name__)
            logger.info("开始预初始化Playwright...")
            from app.utils.playwright_manager import get_playwright_pool
            pool = get_playwright_pool()
            # 触发浏览器初始化（通过获取Playwright实例）
            # 注意：现在使用线程本地存储，每个线程独立初始化
            # 这里只是触发当前线程的初始化
            pool._get_or_init_playwright()
            logger.info("Playwright预初始化完成")
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Playwright预初始化失败（不影响使用）: {e}")
    
    import threading
    pre_init_thread = threading.Thread(target=_pre_init_playwright, daemon=True, name="playwright-preinit")
    pre_init_thread.start()

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown scheduler on shutdown"""
    from app.services.scheduler import stop_scheduler
    stop_scheduler()

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "EMAG ERP System API", "version": "1.0.0"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}


"""Playwright 浏览器上下文池管理器

提供线程安全的浏览器上下文池，支持上下文复用、健康检查、资源管理
"""
import logging
import threading
import time
import random
import sys
import platform
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import datetime
from playwright.sync_api import (
    sync_playwright,
    Playwright,
    Browser,
    BrowserContext,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)
from app.config import config, get_debug_log_path
from app.utils.proxy import proxy_manager
from app.database import ErrorType

logger = logging.getLogger(__name__)

# 获取debug.log路径（缓存以避免重复计算）
_debug_log_path = None
def _get_debug_log_path():
    """获取debug.log路径（带缓存）"""
    global _debug_log_path
    if _debug_log_path is None:
        _debug_log_path = get_debug_log_path()
    return _debug_log_path

# Windows上Playwright需要ProactorEventLoop
# 在导入时设置事件循环策略（仅Windows）
if platform.system() == 'Windows':
    import asyncio
    # 设置事件循环策略为WindowsProactorEventLoopPolicy
    if sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

@dataclass
class ContextInfo:
    """浏览器上下文信息"""
    context: BrowserContext
    proxy: Optional[str]
    created_at: float
    last_used_at: float
    reuse_count: int
    owner_thread_id: Optional[int] = None
    is_valid: bool = True
    cdp_browser: Optional[Browser] = None  # CDP 连接的浏览器引用（BitBrowser 模式）
    window_id: Optional[str] = None  # BitBrowser 窗口ID

class PlaywrightContextPool:
    """线程安全的Playwright浏览器上下文池"""
    
    # User-Agent池（用于轮换）
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    _instance: Optional['PlaywrightContextPool'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(PlaywrightContextPool, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化上下文池"""
        try:
            
            if self._initialized:
                return
            
            self._lock = threading.RLock()
            # 使用线程本地存储，每个线程有独立的Playwright和Browser实例
            self._thread_local = threading.local()
            self._contexts: Dict[str, ContextInfo] = {}  # context_id -> ContextInfo
            self._available_contexts: List[str] = []  # 可用上下文ID列表
            self._max_contexts = config.PLAYWRIGHT_MAX_CONTEXTS
            self._context_reuse = config.PLAYWRIGHT_CONTEXT_REUSE
            self._max_reuse_count = config.PLAYWRIGHT_CONTEXT_MAX_REUSE_COUNT
            self._health_check_interval = config.PLAYWRIGHT_HEALTH_CHECK_INTERVAL
            self._last_health_check = time.time()
            self._pending_close_by_thread: Dict[int, List[ContextInfo]] = {}
            self._orphan_contexts: Dict[str, ContextInfo] = {}
            self._initialized = True
            
            # 不再在__init__中初始化Playwright，改为延迟初始化（每个线程独立初始化）
            
        except Exception as e:
            raise
    
    def _get_or_init_playwright(
        self,
        require_proxy: bool = False,
        proxy_server: Optional[str] = None
    ) -> tuple[Playwright, Browser]:
        """
        获取或初始化当前线程的Playwright浏览器实例
        
        Returns:
            (Playwright, Browser) 元组
        """
        # 检查线程本地存储中是否已有实例
        if hasattr(self._thread_local, 'playwright') and hasattr(self._thread_local, 'browser'):
            if self._thread_local.playwright is not None and self._thread_local.browser is not None:
                proxy_enabled = getattr(self._thread_local, 'proxy_enabled', False)
                if require_proxy and not proxy_enabled:
                    try:
                        # 关闭无代理浏览器，重新初始化带全局代理的实例
                        self._thread_local.browser.close()
                        self._thread_local.playwright.stop()
                    except Exception:
                        pass
                    self._thread_local.playwright = None
                    self._thread_local.browser = None
                else:
                    return self._thread_local.playwright, self._thread_local.browser
        
        # 为每个线程创建独立的初始化锁（避免多线程同时初始化导致的资源竞争）
        if not hasattr(self._thread_local, 'init_lock'):
            self._thread_local.init_lock = threading.Lock()
        
        # 使用线程本地锁保护初始化过程
        with self._thread_local.init_lock:
            # 双重检查：在获取锁后再次检查，避免重复初始化
            if hasattr(self._thread_local, 'playwright') and hasattr(self._thread_local, 'browser'):
                if self._thread_local.playwright is not None and self._thread_local.browser is not None:
                    proxy_enabled = getattr(self._thread_local, 'proxy_enabled', False)
                    if require_proxy and not proxy_enabled:
                        try:
                            self._thread_local.browser.close()
                            self._thread_local.playwright.stop()
                        except Exception:
                            pass
                        self._thread_local.playwright = None
                        self._thread_local.browser = None
                    else:
                        return self._thread_local.playwright, self._thread_local.browser
            
            # 初始化当前线程的Playwright实例
            try:
                
                
                # Windows上确保使用正确的事件循环
                if platform.system() == 'Windows':
                    import asyncio
                    try:
                        
                        # 尝试获取当前事件循环
                        try:
                            loop = asyncio.get_event_loop()
                            loop_type = type(loop).__name__
                            
                            if not isinstance(loop, asyncio.ProactorEventLoop):
                                # 如果不是ProactorEventLoop，创建新的
                                
                                try:
                                    loop.close()
                                except:
                                    pass
                                
                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                        except RuntimeError:
                            # 如果没有事件循环，创建新的
                            
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                    except Exception as e:
                        # 即使事件循环设置失败，也继续尝试初始化Playwright
                        pass
                
                playwright = sync_playwright().start()
                
                
                browser_type = getattr(playwright, config.PLAYWRIGHT_BROWSER_TYPE)
                
                
                launch_options: Dict[str, Any] = {
                    "headless": config.PLAYWRIGHT_HEADLESS,
                    "args": ['--no-sandbox', '--disable-setuid-sandbox']
                }
                if require_proxy:
                    proxy_dict = self._format_proxy(proxy_server or "http://per-context")
                    if proxy_dict:
                        print(f"[Playwright代理配置] 浏览器启动代理配置: {proxy_dict}, 原始代理: {proxy_server}")
                        launch_options["proxy"] = proxy_dict
                    else:
                        print(f"[Playwright代理配置警告] 无法格式化浏览器启动代理: {proxy_server}")

                browser = browser_type.launch(**launch_options)
                
                # 存储到线程本地存储
                self._thread_local.playwright = playwright
                self._thread_local.browser = browser
                self._thread_local.proxy_enabled = require_proxy
                
                
                logger.info(f"Playwright browser initialized for thread {threading.current_thread().ident}: {config.PLAYWRIGHT_BROWSER_TYPE}")
                return playwright, browser
            except Exception as e:
                logger.error(f"Failed to initialize Playwright: {e}")
                raise
    
    def _ensure_browser_initialized(self):
        """
        确保浏览器已初始化（线程安全）
        注意：此方法已废弃，因为现在使用线程本地存储，每个线程独立初始化
        保留此方法是为了向后兼容，但实际不做任何操作
        """
        # 不再需要初始化，因为每个线程会在需要时通过_get_or_init_playwright自动初始化
        pass
    
    def acquire_context(
        self,
        proxy: Optional[str] = None,
        cdp_url: Optional[str] = None,
        window_id: Optional[str] = None,
    ) -> BrowserContext:
        """
        获取可用的浏览器上下文
        
        Args:
            proxy: 代理地址（格式：ip:port 或 http://ip:port）— 传统代理模式
            cdp_url: CDP WebSocket URL — BitBrowser CDP 连接模式
            window_id: BitBrowser 窗口ID（用于日志和追踪）
            
        Returns:
            BrowserContext对象
        """
        
        with self._lock:
            self._close_pending_for_current_thread()
            # 健康检查
            self._health_check_if_needed()
            
            # CDP 连接模式（BitBrowser）
            if cdp_url:
                return self._create_context_cdp(cdp_url, window_id)
            
            # 传统代理模式 — 如果启用上下文复用，尝试复用现有上下文
            if self._context_reuse and proxy:
                context_id = self._find_available_context(proxy)
                if context_id:
                    context_info = self._contexts[context_id]
                    if context_info.is_valid:
                        # 检查是否超过最大复用次数
                        if context_info.reuse_count < self._max_reuse_count:
                            context_info.last_used_at = time.time()
                            context_info.reuse_count += 1
                            logger.debug(f"Reusing context {context_id} (reuse count: {context_info.reuse_count})")
                            return context_info.context
            
            # 创建新上下文
            return self._create_context(proxy)
    
    def release_context(self, context: BrowserContext):
        """
        释放浏览器上下文（标记为可用，不关闭）
        
        Args:
            context: 要释放的BrowserContext对象
        """
        with self._lock:
            self._close_pending_for_current_thread()
            # 查找上下文ID
            context_id = None
            for cid, info in self._contexts.items():
                if info.context == context:
                    context_id = cid
                    break
            
            if context_id:
                context_info = self._contexts[context_id]
                if context_info.is_valid and context_id not in self._available_contexts:
                    self._available_contexts.append(context_id)
                    context_info.last_used_at = time.time()
                    logger.debug(f"Context {context_id} released and marked as available")
            else:
                logger.warning("Attempted to release unknown context")

    def _close_pending_for_current_thread(self):
        """关闭当前线程待关闭的上下文"""
        current_thread_id = threading.current_thread().ident
        if current_thread_id is None:
            return
        pending_list = self._pending_close_by_thread.pop(current_thread_id, [])
        if not pending_list:
            return
        for context_id, context_info in pending_list:
            try:
                context_info.context.close()
                logger.info(f"Closed pending context {context_id} for thread {current_thread_id}")
            except Exception as e:
                logger.warning(f"Error closing pending context {context_id}: {e}")
    
    def _create_context(self, proxy: Optional[str] = None) -> BrowserContext:
        """
        创建新的浏览器上下文
        
        Args:
            proxy: 代理地址
            
        Returns:
            BrowserContext对象
        """
        
        try:
            # 检查是否超过最大上下文数
            if len(self._contexts) >= self._max_contexts:
                # 清理无效上下文
                self._cleanup_invalid_contexts()
                
                # 如果仍然超过限制，关闭最旧的上下文
                if len(self._contexts) >= self._max_contexts:
                    self._close_oldest_context()
            
            # 准备上下文选项
            context_options: Dict[str, Any] = {
                'user_agent': random.choice(self.USER_AGENTS),
                'viewport': {'width': 1920, 'height': 1080},
                'locale': 'ro-RO',
                'timezone_id': 'Europe/Bucharest',
                'accept_downloads': False,
                'ignore_https_errors': True,
            }
            
            # 配置代理
            if proxy:
                proxy_dict = self._format_proxy(proxy)
                if proxy_dict:
                    print(f"[Playwright代理配置] 上下文代理配置: {proxy_dict}, 原始代理: {proxy}")
                    context_options['proxy'] = proxy_dict
                else:
                    print(f"[Playwright代理配置警告] 无法格式化代理: {proxy}")
                    
            
            # 获取或初始化当前线程的Playwright和Browser实例
            # 每个线程有独立的实例，避免greenlet跨线程切换问题
            context_create_start = time.time()
            _, browser = self._get_or_init_playwright(
                require_proxy=bool(proxy),
                proxy_server=proxy
            )
            
            
            
            
            try:
                context = browser.new_context(**context_options)

                # 在上下文层面统一屏蔽静态资源，减少带宽占用并提升加载速度
                def _should_block(route_request) -> bool:
                    r_type = route_request.resource_type
                    url = route_request.url
                    # 屏蔽图片 / 媒体 / 字体 / 样式表等静态资源
                    if r_type in ("image", "media", "font", "stylesheet"):
                        return True
                    # 可选：屏蔽常见广告和统计域名（如后续需要可扩展）
                    block_keywords = [
                        "doubleclick.net",
                        "googlesyndication.com",
                        "google-analytics.com",
                        "facebook.net",
                    ]
                    for kw in block_keywords:
                        if kw in url:
                            return True
                    return False

                def _route_handler(route):
                    try:
                        if _should_block(route.request):
                            return route.abort()
                    except Exception:
                        # 出现异常时回退为正常放行，避免影响主流程
                        pass
                    return route.continue_()

                # 对该上下文下的所有页面生效
                context.route("**/*", _route_handler)

                
            except Exception as e:
                
                raise
            context.set_default_timeout(config.PLAYWRIGHT_TIMEOUT)
            context.set_default_navigation_timeout(config.PLAYWRIGHT_NAVIGATION_TIMEOUT)
            
            # 生成上下文ID
            context_id = f"ctx_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
            
            # 保存上下文信息
            context_info = ContextInfo(
                context=context,
                proxy=proxy,
                created_at=time.time(),
                last_used_at=time.time(),
                reuse_count=1,
                owner_thread_id=threading.current_thread().ident,
                is_valid=True
            )
            
            self._contexts[context_id] = context_info
            if self._context_reuse:
                self._available_contexts.append(context_id)
            
            logger.debug(f"Created new context {context_id} with proxy {proxy}")
            return context
            
        except Exception as e:
            logger.error(f"Failed to create context: {e}")
            raise
    
    def _get_playwright_for_cdp(self) -> Playwright:
        """
        获取当前线程的 Playwright 实例（CDP 模式专用，不启动浏览器）
        
        Returns:
            Playwright 实例
        """
        if hasattr(self._thread_local, 'playwright') and self._thread_local.playwright is not None:
            return self._thread_local.playwright
        
        # Windows 上确保使用正确的事件循环
        if platform.system() == 'Windows':
            import asyncio
            try:
                try:
                    loop = asyncio.get_event_loop()
                    if not isinstance(loop, asyncio.ProactorEventLoop):
                        try:
                            loop.close()
                        except Exception:
                            pass
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except Exception:
                pass
        
        playwright = sync_playwright().start()
        self._thread_local.playwright = playwright
        logger.info(
            f"Playwright instance initialized for thread {threading.current_thread().ident} (CDP mode)"
        )
        return playwright
    
    def _create_context_cdp(self, cdp_url: str, window_id: Optional[str] = None) -> BrowserContext:
        """
        通过 CDP 连接创建浏览器上下文（BitBrowser 模式）
        
        Args:
            cdp_url: CDP WebSocket URL
            window_id: BitBrowser 窗口ID
            
        Returns:
            BrowserContext对象
        """
        try:
            # 检查是否超过最大上下文数
            if len(self._contexts) >= self._max_contexts:
                self._cleanup_invalid_contexts()
                if len(self._contexts) >= self._max_contexts:
                    self._close_oldest_context()
            
            # 获取 Playwright 实例（不启动浏览器）
            playwright = self._get_playwright_for_cdp()
            
            # 通过 CDP 连接到已有浏览器
            logger.info(
                f"[CDP连接] 正在连接到 BitBrowser 窗口 - window_id: {window_id}, cdp_url: {cdp_url}"
            )
            cdp_browser = playwright.chromium.connect_over_cdp(cdp_url)
            
            # 准备上下文选项（CDP 模式下不设置 proxy，由 BitBrowser 窗口自带）
            context_options: Dict[str, Any] = {
                'user_agent': random.choice(self.USER_AGENTS),
                'viewport': {'width': 1920, 'height': 1080},
                'locale': 'ro-RO',
                'timezone_id': 'Europe/Bucharest',
                'accept_downloads': False,
                'ignore_https_errors': True,
            }
            
            context = cdp_browser.new_context(**context_options)
            
            # 在上下文层面统一屏蔽静态资源，减少带宽占用并提升加载速度
            def _should_block(route_request) -> bool:
                r_type = route_request.resource_type
                url = route_request.url
                if r_type in ("image", "media", "font", "stylesheet"):
                    return True
                block_keywords = [
                    "doubleclick.net",
                    "googlesyndication.com",
                    "google-analytics.com",
                    "facebook.net",
                ]
                for kw in block_keywords:
                    if kw in url:
                        return True
                return False

            def _route_handler(route):
                try:
                    if _should_block(route.request):
                        return route.abort()
                except Exception:
                    pass
                return route.continue_()

            context.route("**/*", _route_handler)
            
            context.set_default_timeout(config.PLAYWRIGHT_TIMEOUT)
            context.set_default_navigation_timeout(config.PLAYWRIGHT_NAVIGATION_TIMEOUT)
            
            # 生成上下文ID
            context_id = f"cdp_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
            
            # 保存上下文信息
            context_info = ContextInfo(
                context=context,
                proxy=None,
                created_at=time.time(),
                last_used_at=time.time(),
                reuse_count=1,
                owner_thread_id=threading.current_thread().ident,
                is_valid=True,
                cdp_browser=cdp_browser,
                window_id=window_id,
            )
            
            self._contexts[context_id] = context_info
            
            logger.info(
                f"[CDP连接] 浏览器上下文创建成功 - context_id: {context_id}, window_id: {window_id}"
            )
            return context
            
        except Exception as e:
            logger.error(f"[CDP连接] 创建CDP上下文失败 - window_id: {window_id}, error: {e}")
            raise
    
    def _find_available_context(self, proxy: Optional[str] = None) -> Optional[str]:
        """
        查找可用的上下文（匹配代理）
        
        Args:
            proxy: 代理地址
            
        Returns:
            上下文ID或None
        """
        current_thread_id = threading.current_thread().ident
        for context_id in self._available_contexts:
            context_info = self._contexts[context_id]
            if (
                context_info.is_valid
                and context_info.proxy == proxy
                and context_info.owner_thread_id == current_thread_id
            ):
                return context_id
        return None
    
    def _format_proxy(self, proxy: str) -> Optional[Dict[str, str]]:
        """
        格式化代理地址为Playwright格式
        
        Args:
            proxy: 代理地址（格式：ip:port 或 http://ip:port）
            
        Returns:
            代理字典或None
        """
        if not proxy:
            return None
        
        
        
        try:
            # 处理不同格式的代理
            # 统一使用配置的 PROXY_SCHEME（例如 socks5），支持 SOCKS 代理
            if '://' in proxy:
                proxy_url = proxy
            else:
                scheme = getattr(config, "PROXY_SCHEME", "socks5")
                proxy_url = f"{scheme}://{proxy}"

            from urllib.parse import urlparse
            parsed = urlparse(proxy_url)
            if not parsed.hostname or not parsed.port:
                logger.warning(f"Failed to parse proxy address: {proxy}")
                return None

            proxy_dict: Dict[str, str] = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
            if parsed.username or parsed.password:
                proxy_dict["username"] = parsed.username or ""
                proxy_dict["password"] = parsed.password or ""
            
            

            return proxy_dict
        except Exception as e:
            logger.warning(f"Failed to format proxy {proxy}: {e}")
            
            return None
    
    def _close_oldest_context(self):
        """关闭最旧的上下文"""
        if not self._contexts:
            return
        
        # 找到最旧的上下文
        oldest_id = min(
            self._contexts.keys(),
            key=lambda cid: self._contexts[cid].created_at
        )
        
        self._close_context(oldest_id)
    
    def _close_context(self, context_id: str):
        """
        关闭指定的上下文
        
        Args:
            context_id: 上下文ID
        """
        if context_id not in self._contexts:
            return
        
        context_info = self._contexts[context_id]
        current_thread_id = threading.current_thread().ident
        if context_info.owner_thread_id is None:
            # 无法确定归属线程，避免跨线程关闭
            self._orphan_contexts[context_id] = context_info
            # 从字典中移除，避免继续复用
            if context_id in self._contexts:
                del self._contexts[context_id]
            if context_id in self._available_contexts:
                self._available_contexts.remove(context_id)
            return
        if context_info.owner_thread_id != current_thread_id:
            self._pending_close_by_thread.setdefault(context_info.owner_thread_id, []).append((context_id, context_info))
            # 从字典中移除，避免继续复用
            if context_id in self._contexts:
                del self._contexts[context_id]
            if context_id in self._available_contexts:
                self._available_contexts.remove(context_id)
            return
        try:
            context_info.context.close()
            logger.debug(f"Closed context {context_id}")
        except Exception as e:
            logger.warning(f"Error closing context {context_id}: {e}")
        finally:
            # CDP 模式下断开与远程浏览器的连接（不会关闭实际的 BitBrowser 窗口）
            if context_info.cdp_browser:
                try:
                    context_info.cdp_browser.close()
                    logger.debug(f"Disconnected CDP browser for context {context_id} (window: {context_info.window_id})")
                except Exception as e:
                    logger.warning(f"Error disconnecting CDP browser for context {context_id}: {e}")
            # 从字典中移除
            if context_id in self._contexts:
                del self._contexts[context_id]
            if context_id in self._available_contexts:
                self._available_contexts.remove(context_id)
    
    def cleanup_invalid_contexts(self):
        """清理无效的上下文"""
        with self._lock:
            self._cleanup_invalid_contexts()
    
    def _cleanup_invalid_contexts(self):
        """清理无效的上下文（内部方法，不加锁）"""
        invalid_ids = []
        current_thread_id = threading.current_thread().ident
        
        for context_id, context_info in self._contexts.items():
            if context_info.owner_thread_id is not None and context_info.owner_thread_id != current_thread_id:
                continue
            if not context_info.is_valid:
                invalid_ids.append(context_id)
                continue
            
            # 检查上下文是否仍然有效（通过检查浏览器是否连接）
            try:
                # 尝试创建一个页面来测试上下文是否有效
                pages = context_info.context.pages
                # 如果上下文中没有页面，尝试创建一个临时页面
                if len(pages) == 0:
                    test_page = context_info.context.new_page()
                    test_page.close()
            except Exception:
                # 上下文无效，标记为无效
                context_info.is_valid = False
                invalid_ids.append(context_id)
        
        # 关闭无效的上下文
        for context_id in invalid_ids:
            self._close_context(context_id)
        
        if invalid_ids:
            logger.info(f"Cleaned up {len(invalid_ids)} invalid contexts")
    
    def _health_check_if_needed(self):
        """如果需要，执行健康检查"""
        current_time = time.time()
        if current_time - self._last_health_check >= self._health_check_interval:
            self._last_health_check = current_time
            self._cleanup_invalid_contexts()
    
    def get_context_count(self) -> int:
        """获取当前活跃上下文数量"""
        with self._lock:
            return len([c for c in self._contexts.values() if c.is_valid])
    
    def shutdown(self):
        """关闭所有上下文和浏览器"""
        with self._lock:
            # 关闭所有上下文
            for context_id in list(self._contexts.keys()):
                self._close_context(context_id)
            
            # 关闭所有线程的浏览器实例
            # 注意：由于使用线程本地存储，无法直接访问所有线程的实例
            # 这里只清理当前线程的实例（如果有的话）
            if hasattr(self._thread_local, 'browser') and self._thread_local.browser:
                try:
                    self._thread_local.browser.close()
                    logger.info("Browser closed for current thread")
                except Exception as e:
                    logger.warning(f"Error closing browser: {e}")
            
            # 停止当前线程的Playwright实例
            if hasattr(self._thread_local, 'playwright') and self._thread_local.playwright:
                try:
                    self._thread_local.playwright.stop()
                    logger.info("Playwright stopped for current thread")
                except Exception as e:
                    logger.warning(f"Error stopping Playwright: {e}")
    
    @staticmethod
    def classify_playwright_error(error: Exception) -> ErrorType:
        """
        将Playwright错误映射到ErrorType
        
        Args:
            error: Playwright异常对象
            
        Returns:
            ErrorType枚举值
        """
        error_type = type(error).__name__
        error_str = str(error).lower()
        
        # Timeout错误
        if isinstance(error, PlaywrightTimeoutError) or 'timeout' in error_type.lower():
            return ErrorType.TIMEOUT
        
        # 连接错误
        if 'connection' in error_type.lower() or 'connection' in error_str:
            return ErrorType.CONNECTION
        
        # 浏览器错误/断开连接
        if 'browser' in error_type.lower() or 'disconnect' in error_str or 'disconnected' in error_str:
            return ErrorType.DISCONNECT
        
        # 其他错误
        return ErrorType.OTHER

# 全局单例实例
playwright_pool: Optional[PlaywrightContextPool] = None

def get_playwright_pool() -> PlaywrightContextPool:
    """获取Playwright上下文池单例"""
    global playwright_pool
    try:
        
        if playwright_pool is None:
            
            playwright_pool = PlaywrightContextPool()
            
        
        return playwright_pool
    except Exception as e:
        raise


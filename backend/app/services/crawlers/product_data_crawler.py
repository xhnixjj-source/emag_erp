"""产品数据爬取器

从产品详情页提取产品数据
"""
import json
import logging
import time
import threading
import requests
import socket
import ssl
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from app.config import config
from app.utils.proxy import proxy_manager
from app.utils.captcha_handler import captcha_handler
from app.utils.playwright_manager import get_playwright_pool
from app.services.extractors import BaseInfoExtractor, DynamicDataExtractor
from app.services.istoric_preturi_client import get_listed_at as get_istoric_listed_at
from app.database import ErrorType

logger = logging.getLogger(__name__)

class ProductDataCrawler:
    """从产品详情页爬取产品数据的爬取器"""
    
    BASE_URL = "https://www.emag.ro"
    
    def __init__(self):
        """初始化产品数据爬取器"""
        # 延迟初始化playwright_pool，避免在导入时就初始化
        self.base_url = self.BASE_URL
        self.base_info_extractor = BaseInfoExtractor(self.base_url)
        self.dynamic_data_extractor = DynamicDataExtractor(self.base_url)
        self._playwright_pool = None  # 延迟初始化
    
    @property
    def playwright_pool(self):
        """延迟获取Playwright上下文池"""
        if self._playwright_pool is None:
            self._playwright_pool = get_playwright_pool()
        return self._playwright_pool
    
    def crawl_full_data(
        self,
        product_url: str,
        include_base_info: bool = True,
        task_id: Optional[int] = None,
        db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        爬取完整数据（筛选池使用，不包含重试逻辑）
        
        Args:
            product_url: 产品URL
            include_base_info: 是否包含基础信息（默认True）
            task_id: 任务ID（用于更新进度）
            db: 数据库会话（用于更新任务状态）
            
        Returns:
            包含完整产品数据的字典
            
        Raises:
            Exception: 爬取失败时抛出异常，由retry_manager处理重试
        """
        logger.info(f"[爬取开始] 产品完整数据爬取 - URL: {product_url}, 包含基础信息: {include_base_info}, 任务ID: {task_id}")
        return self._crawl_with_context(
            product_url=product_url,
            extract_base_info=include_base_info,
            extract_dynamic_data=True,
            extract_rankings=True,
            task_id=task_id,
            db=db
        )
    
    def crawl_dynamic_data(
        self,
        product_url: str,
        task_id: Optional[int] = None,
        db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        仅爬取动态数据（监控池使用，不包含重试逻辑）
        
        Args:
            product_url: 产品URL
            task_id: 任务ID（用于更新进度）
            db: 数据库会话（用于更新任务状态）
            
        Returns:
            包含动态产品数据的字典（仅包含动态字段）
            
        Raises:
            Exception: 爬取失败时抛出异常，由retry_manager处理重试
        """
        logger.info(f"[爬取开始] 产品动态数据爬取 - URL: {product_url}, 任务ID: {task_id}")
        return self._crawl_with_context(
            product_url=product_url,
            extract_base_info=False,
            extract_dynamic_data=True,
            extract_rankings=True,  # 监控时也提取排名
            task_id=task_id,
            db=db
        )
    
    def _crawl_with_context(
        self,
        product_url: str,
        extract_base_info: bool,
        extract_dynamic_data: bool,
        extract_rankings: bool,
        task_id: Optional[int] = None,
        db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        内部爬取方法，统一处理上下文获取和释放
        
        Args:
            product_url: 产品URL
            extract_base_info: 是否提取基础信息
            extract_dynamic_data: 是否提取动态数据
            extract_rankings: 是否提取排名（需要遍历多个页面）
            task_id: 任务ID
            db: 数据库会话
            
        Returns:
            包含产品数据的字典
            
        Raises:
            Exception: 爬取失败时抛出异常
        """
        start_time = time.time()
        result: Dict[str, Any] = {
            'product_url': product_url
        }
        stage = "start"
        
        context = None
        page = None
        proxy_str = None
        
        try:
            # 获取代理
            # 这里直接使用 proxy_manager 返回的 URL，避免写死 http/https，支持 socks 协议
            proxy_dict = proxy_manager.get_random_proxy()
            if proxy_dict:
                proxy_url = proxy_dict.get('http', '') or proxy_dict.get('https', '')
                if proxy_url:
                    proxy_str = proxy_url
            
            # #region agent log
            try:
                import json
                from app.config import get_debug_log_path
                debug_log_path = get_debug_log_path()
                with open(debug_log_path, 'a', encoding='utf-8') as f:
                    log_entry = {
                        "sessionId": "debug-session",
                        "runId": "run2",
                        "hypothesisId": "F",
                        "location": "product_data_crawler.py:_crawl_with_context",
                        "message": "获取代理后 - 准备创建上下文",
                        "data": {
                            "proxy_dict": proxy_dict,
                            "proxy_str": proxy_str,
                            "product_url": product_url,
                            "has_proxy": proxy_str is not None
                        },
                        "timestamp": int(time.time() * 1000)
                    }
                    f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            logger.info(f"[爬取进行中] 开始爬取产品数据 - URL: {product_url}, 代理: {proxy_str if proxy_str else '无'}, 提取基础信息: {extract_base_info}, 提取动态数据: {extract_dynamic_data}, 提取排名: {extract_rankings}")

            
            # 获取浏览器上下文
            context = self.playwright_pool.acquire_context(proxy=proxy_str)
            
            # #region agent log
            try:
                import json
                from app.config import get_debug_log_path
                debug_log_path = get_debug_log_path()
                with open(debug_log_path, 'a', encoding='utf-8') as f:
                    log_entry = {
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "D",
                        "location": "product_data_crawler.py:_crawl_with_context",
                        "message": "上下文创建完成 - 准备加载页面",
                        "data": {
                            "proxy_str": proxy_str,
                            "context_created": context is not None,
                            "timeout_setting": config.PLAYWRIGHT_NAVIGATION_TIMEOUT
                        },
                        "timestamp": int(time.time() * 1000)
                    }
                    f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            # 创建新页面
            page = context.new_page()


            
            # 加载产品详情页
            load_start = time.time()
            stage = "page_goto"
            
            # #region agent log
            try:
                import json
                from app.config import get_debug_log_path
                debug_log_path = get_debug_log_path()
                with open(debug_log_path, 'a', encoding='utf-8') as f:
                    log_entry = {
                        "sessionId": "debug-session",
                        "runId": "run2",
                        "hypothesisId": "G",
                        "location": "product_data_crawler.py:_crawl_with_context",
                        "message": "开始页面加载",
                        "data": {
                            "product_url": product_url,
                            "proxy_str": proxy_str,
                            "timeout": config.PLAYWRIGHT_NAVIGATION_TIMEOUT,
                            "wait_until": "domcontentloaded"
                        },
                        "timestamp": int(time.time() * 1000)
                    }
                    f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            try:
                # 使用更宽松的 load_state，避免网络长连接导致 networkidle 超时
                page.goto(product_url, wait_until='domcontentloaded', timeout=config.PLAYWRIGHT_NAVIGATION_TIMEOUT)
                
                # #region agent log
                try:
                    import json
                    from app.config import get_debug_log_path
                    debug_log_path = get_debug_log_path()
                    with open(debug_log_path, 'a', encoding='utf-8') as f:
                        log_entry = {
                            "sessionId": "debug-session",
                            "runId": "run2",
                            "hypothesisId": "G",
                            "location": "product_data_crawler.py:_crawl_with_context",
                            "message": "页面加载成功",
                            "data": {
                                "product_url": product_url,
                                "proxy_str": proxy_str,
                                "elapsed_time": time.time() - load_start
                            },
                            "timestamp": int(time.time() * 1000)
                        }
                        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
            except Exception as e:
                # #region agent log
                try:
                    import json
                    from app.config import get_debug_log_path
                    debug_log_path = get_debug_log_path()
                    with open(debug_log_path, 'a', encoding='utf-8') as f:
                        log_entry = {
                            "sessionId": "debug-session",
                            "runId": "run2",
                            "hypothesisId": "G",
                            "location": "product_data_crawler.py:_crawl_with_context",
                            "message": "页面加载异常",
                            "data": {
                                "error_type": type(e).__name__,
                                "error_message": str(e),
                                "proxy_str": proxy_str,
                                "elapsed_time": time.time() - load_start,
                                "timeout_setting": config.PLAYWRIGHT_NAVIGATION_TIMEOUT
                            },
                            "timestamp": int(time.time() * 1000)
                        }
                        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                raise
            load_elapsed = time.time() - load_start
            logger.debug(f"[页面加载] 产品页面加载完成 - URL: {product_url}, 加载耗时: {load_elapsed:.2f}秒")
            
            # 检查验证码
            page_content = page.content()
            if captcha_handler.detect_captcha(page_content, page_content):
                logger.warning(f"[验证码检测] 检测到验证码 - URL: {product_url}, 代理: {proxy_str if proxy_str else '无'}")
                # 标记当前代理失败，以便重试时使用新代理
                if proxy_str:
                    proxy_manager.mark_proxy_failed(proxy_str)
                    logger.warning(f"[代理标记失败] 验证码检测，标记代理为失败 - 代理: {proxy_str}")
                if task_id and db:
                    captcha_handler.handle_captcha(
                        task_id,
                        html_content=page_content[:1000],
                        response_text=page_content[:1000],
                        db=db
                    )
                # 抛出异常，让 retry_manager 处理重试（会使用新的代理IP）
                raise ValueError(f"Captcha detected for {product_url}")
            
            # 提取基础信息
            if extract_base_info:
                extract_start = time.time()
                base_info = self.base_info_extractor.extract(page, product_url)
                extract_elapsed = time.time() - extract_start
                result.update(base_info)
                logger.info(f"[数据提取] 基础信息提取完成 - URL: {product_url}, 字段数: {len(base_info)}, 耗时: {extract_elapsed:.2f}秒")
            
            # 提取动态数据
            if extract_dynamic_data:
                stage = "extract_dynamic"
                extract_start = time.time()
                dynamic_data = self.dynamic_data_extractor.extract_basic_fields(page)
                extract_elapsed = time.time() - extract_start
                result.update(dynamic_data)
                logger.info(f"[数据提取] 动态数据提取完成 - URL: {product_url}, 字段数: {len(dynamic_data)}, 耗时: {extract_elapsed:.2f}秒")
                
                # 提取排名（需要遍历多个页面）
                if extract_rankings:
                    ranking_start = time.time()
                    rankings = self.dynamic_data_extractor.extract_rankings(
                        page=page,
                        product_url=product_url,
                        context=context
                    )
                    ranking_elapsed = time.time() - ranking_start
                    result.update(rankings)
                    logger.info(f"[数据提取] 排名信息提取完成 - URL: {product_url}, 排名数据: {rankings}, 耗时: {ranking_elapsed:.2f}秒")

            # 通过 Istoric Preturi 接口补充上架日期（优先于页面 DOM 解析）
            try:
                listed_at = get_istoric_listed_at(product_url)
                if listed_at:
                    # 只有在未提前填充 listed_at 时才覆盖，避免意外覆盖更可信的来源
                    if not result.get("listed_at"):
                        result["listed_at"] = listed_at
                        logger.info(f"[上架日期] 通过 Istoric Preturi 获取上架日期成功 - URL: {product_url}, listed_at: {listed_at.isoformat()}")
                    else:
                        logger.debug(
                            f"[上架日期] 已存在 listed_at 字段，跳过 Istoric Preturi 覆盖 - URL: {product_url}, "
                            f"existing={result.get('listed_at')}, istoric={listed_at.isoformat()}"
                        )
                else:
                    logger.debug(f"[上架日期] Istoric Preturi 未返回上架日期 - URL: {product_url}")
            except Exception as e:
                # 该接口失败不影响整体验证，只记录 warning 级别日志
                logger.warning(f"[上架日期] 调用 Istoric Preturi 接口失败 - URL: {product_url}, 错误: {e}")
            
            total_elapsed = time.time() - start_time
            logger.info(f"[爬取完成] 产品数据爬取完成 - URL: {product_url}, 总字段数: {len(result)}, 总耗时: {total_elapsed:.2f}秒")
            return result
            
        except PlaywrightTimeoutError as e:
            # 连接超时：标记代理失败
            if proxy_str:
                proxy_manager.mark_proxy_failed(proxy_str)
                logger.warning(f"[代理标记失败] 连接超时，标记代理为失败 - 代理: {proxy_str}")
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 产品数据爬取超时 - URL: {product_url}, 错误: {str(e)}, 总耗时: {total_elapsed:.2f}秒")
            raise
        except PlaywrightError as e:
            # 浏览器错误/主机断开：标记代理失败
            if proxy_str:
                proxy_manager.mark_proxy_failed(proxy_str)
                logger.warning(f"[代理标记失败] 浏览器错误，标记代理为失败 - 代理: {proxy_str}")
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 产品数据爬取浏览器错误 - URL: {product_url}, 错误: {str(e)}, 总耗时: {total_elapsed:.2f}秒")
            raise
        except Exception as e:
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 产品数据爬取错误 - URL: {product_url}, 错误: {str(e)}, 错误类型: {type(e).__name__}, 总耗时: {total_elapsed:.2f}秒", exc_info=True)
            raise
        finally:
            # 确保资源清理
            if page:
                try:
                    page.close()
                except Exception:
                    pass
            
            if context:
                try:
                    self.playwright_pool.release_context(context)
                except Exception:
                    pass


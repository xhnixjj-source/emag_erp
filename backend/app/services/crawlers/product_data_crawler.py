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
from app.utils.bitbrowser_manager import bitbrowser_manager
from app.services.extractors import BaseInfoExtractor, DynamicDataExtractor
from app.services.istoric_preturi_client import get_listed_at as get_istoric_listed_at, get_listed_at_via_browser
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
        db: Optional[Session] = None,
        shop_url: Optional[str] = None,
        category_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        仅爬取动态数据（监控池使用，不包含重试逻辑）
        
        Args:
            product_url: 产品URL
            task_id: 任务ID（用于更新进度）
            db: 数据库会话（用于更新任务状态）
            shop_url: 店铺链接（可选，从FilterPool获取，避免重复爬取）
            category_url: 类目链接（可选，从FilterPool获取，避免重复爬取）
            
        Returns:
            包含动态产品数据的字典（仅包含动态字段）
            
        Raises:
            Exception: 爬取失败时抛出异常，由retry_manager处理重试
        """
        logger.info(f"[爬取开始] 产品动态数据爬取 - URL: {product_url}, 任务ID: {task_id}, shop_url: {shop_url}, category_url: {category_url}")
        return self._crawl_with_context(
            product_url=product_url,
            extract_base_info=False,
            extract_dynamic_data=True,
            extract_rankings=True,  # 监控时也提取排名
            task_id=task_id,
            db=db,
            shop_url=shop_url,
            category_url=category_url
        )
    
    def _crawl_with_context(
        self,
        product_url: str,
        extract_base_info: bool,
        extract_dynamic_data: bool,
        extract_rankings: bool,
        task_id: Optional[int] = None,
        db: Optional[Session] = None,
        shop_url: Optional[str] = None,
        category_url: Optional[str] = None
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
        proxy_dict = None  # 初始化 proxy_dict，确保 finally 块中可以使用
        window_info = None  # BitBrowser 窗口信息
        
        try:
            # 提取产品 PNK 用于日志
            import re as _re
            _pnk_match = _re.search(r'/pd/([^/]+)', product_url)
            _pnk = _pnk_match.group(1) if _pnk_match else 'unknown'
            
            if config.BITBROWSER_ENABLED:
                # ── BitBrowser 模式：获取独占窗口 ──
                try:
                    window_info = bitbrowser_manager.acquire_exclusive_window()
                except Exception as win_error:
                    logger.error(f"[BitBrowser窗口获取失败] 获取独占窗口时出错: {str(win_error)}, 错误类型: {type(win_error).__name__}", exc_info=True)
                    window_info = None
                
                if not window_info:
                    raise RuntimeError("无法获取可用的 BitBrowser 窗口")
                
                print(f"[BitBrowser分配] PNK: {_pnk}, 窗口ID: {window_info['id']}")
                logger.info(f"[BitBrowser分配] PNK: {_pnk}, 窗口ID: {window_info['id']}")
                
                print(f"[爬取进行中] 开始爬取产品数据 - URL: {product_url}, 窗口: {window_info['id']}, 提取基础信息: {extract_base_info}, 提取动态数据: {extract_dynamic_data}, 提取排名: {extract_rankings}")
                logger.info(f"[爬取进行中] 开始爬取产品数据 - URL: {product_url}, 窗口: {window_info['id']}, 提取基础信息: {extract_base_info}, 提取动态数据: {extract_dynamic_data}, 提取排名: {extract_rankings}")
                
                # 获取浏览器上下文（CDP 连接模式）
                context = self.playwright_pool.acquire_context(
                    cdp_url=window_info['ws'],
                    window_id=window_info['id'],
                )
            else:
                # ── 传统代理模式 ──
                try:
                    proxy_dict = proxy_manager.acquire_exclusive_proxy()
                    if proxy_dict:
                        proxy_url = proxy_dict.get('http', '') or proxy_dict.get('https', '')
                        if proxy_url:
                            proxy_str = proxy_url
                except Exception as proxy_error:
                    logger.error(f"[代理获取失败] 获取独占代理时出错: {str(proxy_error)}, 错误类型: {type(proxy_error).__name__}", exc_info=True)
                    proxy_dict = None
                
                print(f"[代理分配] PNK: {_pnk}, 代理IP: {proxy_str if proxy_str else '无'}")
                logger.info(f"[代理分配] PNK: {_pnk}, 代理IP: {proxy_str if proxy_str else '无'}")
                
                print(f"[爬取进行中] 开始爬取产品数据 - URL: {product_url}, 代理: {proxy_str if proxy_str else '无'}, 提取基础信息: {extract_base_info}, 提取动态数据: {extract_dynamic_data}, 提取排名: {extract_rankings}")
                logger.info(f"[爬取进行中] 开始爬取产品数据 - URL: {product_url}, 代理: {proxy_str if proxy_str else '无'}, 提取基础信息: {extract_base_info}, 提取动态数据: {extract_dynamic_data}, 提取排名: {extract_rankings}")
                
                # 获取浏览器上下文（传统代理模式）
                context = self.playwright_pool.acquire_context(proxy=proxy_str)
            
            
            
            # 创建新页面
            page = context.new_page()


            
            # 加载产品详情页
            load_start = time.time()
            stage = "page_goto"
            
            # #region agent log
            import json as _json_page_goto
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_page_goto.dumps({
                        "timestamp": int(time.time() * 1000),
                        "location": "product_data_crawler.py:before_page_goto",
                        "message": "准备加载产品页面",
                        "data": {
                            "url": product_url,
                            "timeout_ms": config.PLAYWRIGHT_NAVIGATION_TIMEOUT,
                            "wait_until": "domcontentloaded"
                        },
                        "hypothesisId": "H1",
                        "runId": "timeout-debug"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            # ── 内部重试：ERR_EMPTY_RESPONSE 等瞬时网络错误重试一次 ──
            _MAX_PAGE_GOTO = 2
            for _page_attempt in range(_MAX_PAGE_GOTO):
                try:
                    page.goto(product_url, wait_until='domcontentloaded', timeout=config.PLAYWRIGHT_NAVIGATION_TIMEOUT)
                    
                    # #region agent log
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_page_goto.dumps({
                                "timestamp": int(time.time() * 1000),
                                "location": "product_data_crawler.py:after_page_goto",
                                "message": "产品页面加载完成",
                                "data": {
                                    "url": product_url,
                                    "elapsed_ms": int((time.time() - load_start) * 1000),
                                    "attempt": _page_attempt + 1
                                },
                                "hypothesisId": "H1",
                                "runId": "timeout-debug"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    break  # goto 成功
                except Exception as e:
                    # #region agent log
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_page_goto.dumps({
                                "timestamp": int(time.time() * 1000),
                                "location": "product_data_crawler.py:page_goto_error",
                                "message": "产品页面加载失败",
                                "data": {
                                    "url": product_url,
                                    "error_type": type(e).__name__,
                                    "error_message": str(e)[:300],
                                    "elapsed_ms": int((time.time() - load_start) * 1000),
                                    "timeout_ms": config.PLAYWRIGHT_NAVIGATION_TIMEOUT,
                                    "attempt": _page_attempt + 1,
                                    "will_retry": _page_attempt < _MAX_PAGE_GOTO - 1 and not isinstance(e, PlaywrightTimeoutError)
                                },
                                "hypothesisId": "H10_product_goto_transient",
                                "runId": "retry-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    # 超时不重试（已消耗完整超时时间），非超时瞬时错误重试一次
                    if _page_attempt < _MAX_PAGE_GOTO - 1 and not isinstance(e, PlaywrightTimeoutError):
                        logger.warning(
                            f"[产品页] goto 失败(attempt {_page_attempt+1})，3秒后重试: {e}"
                        )
                        time.sleep(3)
                        load_start = time.time()  # 重置计时
                    else:
                        raise
            load_elapsed = time.time() - load_start
            logger.debug(f"[页面加载] 产品页面加载完成 - URL: {product_url}, 加载耗时: {load_elapsed:.2f}秒")
            
            # 检查验证码
            page_content = page.content()
            if captcha_handler.detect_captcha(page_content, page_content):
                logger.warning(f"[验证码检测] 检测到验证码 - URL: {product_url}, 代理/窗口: {window_info['id'] if window_info else (proxy_str or '无')}")
                # 标记当前代理/窗口失败，以便重试时使用新IP
                if config.BITBROWSER_ENABLED and window_info:
                    bitbrowser_manager.restart_window(window_info['id'])
                    logger.warning(f"[BitBrowser窗口重启] 验证码检测，重启窗口 - 窗口ID: {window_info['id']}")
                elif proxy_str:
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
                # #region agent log
                import json as _json_base_start
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_base_start.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "product_data_crawler.py:before_base_info_extract",
                            "message": "准备提取基础信息",
                            "data": {
                                "url": product_url
                            },
                            "hypothesisId": "H2",
                            "runId": "timeout-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                try:
                    base_info = self.base_info_extractor.extract(page, product_url)
                    extract_elapsed = time.time() - extract_start
                    result.update(base_info)
                    logger.info(f"[数据提取] 基础信息提取完成 - URL: {product_url}, 字段数: {len(base_info)}, 耗时: {extract_elapsed:.2f}秒")
                    # #region agent log
                    import json as _json_base
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_base.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "product_data_crawler.py:base_info_extracted",
                            "message": "基础信息提取结果",
                            "data": {
                                "url": product_url,
                                "field_count": len(base_info),
                                "keys_sample": list(base_info.keys())[:20],
                            },
                            "hypothesisId": "H_base_missing",
                            "runId": "incomplete-debug"
                        }, default=str) + "\n")
                        # 当基础信息字段数为 0 时，额外记录一小段页面 HTML 片段用于诊断
                        if len(base_info) == 0:
                            try:
                                _html_snippet = page.content()[:800]
                            except Exception as _html_err:
                                _html_snippet = f"<page.content() error: {type(_html_err).__name__}: {str(_html_err)[:200]}>"
                            _f.write(_json_base.dumps({
                                "timestamp": int(time.time() * 1000),
                                "location": "product_data_crawler.py:base_info_html_zero",
                                "message": "基础信息字段为0时的HTML片段",
                                "data": {
                                    "url": product_url,
                                    "html_snippet": _html_snippet
                                },
                                "hypothesisId": "H_base_missing_html",
                                "runId": "incomplete-debug"
                            }, default=str) + "\n")
                    # #endregion
                except Exception as _base_err:
                    extract_elapsed = time.time() - extract_start
                    # #region agent log
                    import json as _json_base_err
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_base_err.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "product_data_crawler.py:base_info_exception",
                            "message": "基础信息提取异常",
                            "data": {
                                "url": product_url,
                                "error": str(_base_err)[:200],
                                "error_type": type(_base_err).__name__,
                                "elapsed": round(extract_elapsed, 2),
                            },
                            "hypothesisId": "H_base_missing",
                            "runId": "incomplete-debug"
                        }, default=str) + "\n")
                    # #endregion
                    raise
            
            # 提取动态数据
            if extract_dynamic_data:
                stage = "extract_dynamic"
                extract_start = time.time()
                # #region agent log
                import json as _json_dyn_start
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_dyn_start.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "product_data_crawler.py:before_dynamic_info_extract",
                            "message": "准备提取动态信息",
                            "data": {
                                "url": product_url
                            },
                            "hypothesisId": "H3",
                            "runId": "timeout-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                try:
                    dynamic_data = self.dynamic_data_extractor.extract_basic_fields(page)
                    extract_elapsed = time.time() - extract_start
                    result.update(dynamic_data)
                    logger.info(f"[数据提取] 动态数据提取完成 - URL: {product_url}, 字段数: {len(dynamic_data)}, 耗时: {extract_elapsed:.2f}秒")
                    # #region agent log
                    import json as _json_dyn
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_dyn.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "product_data_crawler.py:dynamic_data_extracted",
                            "message": "动态数据提取结果",
                            "data": {
                                "url": product_url,
                                "field_count": len(dynamic_data),
                                "keys_sample": list(dynamic_data.keys())[:20],
                            },
                            "hypothesisId": "H_dynamic_missing",
                            "runId": "incomplete-debug"
                        }, default=str) + "\n")
                        # 当动态数据字段数为 0 时，同样记录一小段 HTML 片段
                        if len(dynamic_data) == 0:
                            try:
                                _html_snippet_dyn = page.content()[:800]
                            except Exception as _html_dyn_err:
                                _html_snippet_dyn = f"<page.content() error: {type(_html_dyn_err).__name__}: {str(_html_dyn_err)[:200]}>"
                            _f.write(_json_dyn.dumps({
                                "timestamp": int(time.time() * 1000),
                                "location": "product_data_crawler.py:dynamic_html_zero",
                                "message": "动态数据字段为0时的HTML片段",
                                "data": {
                                    "url": product_url,
                                    "html_snippet": _html_snippet_dyn
                                },
                                "hypothesisId": "H_dynamic_missing_html",
                                "runId": "incomplete-debug"
                            }, default=str) + "\n")
                    # #endregion
                except Exception as _dyn_err:
                    extract_elapsed = time.time() - extract_start
                    # #region agent log
                    import json as _json_dyn_err
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_dyn_err.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "product_data_crawler.py:dynamic_data_exception",
                            "message": "动态数据提取异常",
                            "data": {
                                "url": product_url,
                                "error": str(_dyn_err)[:200],
                                "error_type": type(_dyn_err).__name__,
                                "elapsed": round(extract_elapsed, 2),
                            },
                            "hypothesisId": "H_dynamic_missing",
                            "runId": "incomplete-debug"
                        }, default=str) + "\n")
                    # #endregion
                    raise
                
                # 提取排名（需要遍历多个页面）
                if extract_rankings:
                    
                    ranking_start = time.time()
                    try:
                        rankings = self.dynamic_data_extractor.extract_rankings(
                            page=page,
                            product_url=product_url,
                            context=context,
                            shop_url=shop_url,  # 传入从FilterPool获取的shop_url
                            category_url=category_url,  # 传入从FilterPool获取的category_url
                            task_id=task_id,  # 传递任务ID用于错误记录
                            db=db  # 传递数据库会话用于错误记录
                        )
                        ranking_elapsed = time.time() - ranking_start
                        result.update(rankings)
                        
                        logger.info(f"[数据提取] 排名信息提取完成 - URL: {product_url}, 排名数据: {rankings}, 耗时: {ranking_elapsed:.2f}秒")
                    except Exception as ranking_error:
                        ranking_elapsed = time.time() - ranking_start
                        
                        # 所有从 extract_rankings 抛出的异常都应该是关键错误，因为排名提取失败意味着数据不完整
                        # 直接抛出异常，不进行任何检查，确保任务失败并触发重试
                        error_msg = str(ranking_error)
                        error_type = type(ranking_error).__name__
                        
                        logger.error(f"[数据提取] 排名信息提取失败（关键错误）- URL: {product_url}, 错误: {error_msg}, 错误类型: {error_type}, 耗时: {ranking_elapsed:.2f}秒")
                        # #region agent log
                        import json as _json_rank_fail
                        try:
                            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                _f.write(_json_rank_fail.dumps({
                                    "timestamp": int(time.time() * 1000),
                                    "location": "product_data_crawler.py:extract_rankings_failed_critical",
                                    "message": "排名提取失败（关键错误），抛出异常",
                                    "data": {
                                        "url": product_url,
                                        "error": error_msg[:300],
                                        "error_type": error_type,
                                        "elapsed": round(ranking_elapsed, 2),
                                        "ranking_keys_before_error": list(result.get('category_rank', result.get('store_rank', result.get('ad_category_rank', 'N/A'))))
                                    },
                                    "hypothesisId": "H_rank_fail_critical",
                                    "runId": "ranking-fix"
                                }, ensure_ascii=False) + "\n")
                        except Exception:
                            pass
                        # #endregion
                        # 重新抛出异常，确保任务失败并触发重试
                        raise

            # 通过 Istoric Preturi 接口补充上架日期（优先于页面 DOM 解析）
            # 注意：根据 config.DISABLE_LISTED_AT 可临时整体屏蔽该步骤，后续可通过修改配置重新开启
            if not config.DISABLE_LISTED_AT:
                try:
                    # 在 BitBrowser 模式下，只使用浏览器网络通道调用，避免 Python requests 在当前网络环境下反复超时
                    if config.BITBROWSER_ENABLED:
                        # 优先通过浏览器网络通道调用（绕过 Proxifier 对 Python 进程的代理限制）
                        listed_at = get_listed_at_via_browser(page, product_url)
                    else:
                        # 非 BitBrowser 模式下，仍然可以使用 requests 直连方式
                        listed_at = get_istoric_listed_at(product_url)

                    if listed_at:
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
            else:
                logger.debug(f"[上架日期] 已根据配置 DISABLE_LISTED_AT 屏蔽 Istoric Preturi 上架日期获取 - URL: {product_url}")
            
            total_elapsed = time.time() - start_time
            # #region agent log
            import json as _json_ret
            _rank_vals = {k: result.get(k) for k in ['category_rank', 'ad_category_rank', 'store_rank', 'shop_rank', 'ad_rank']}
            _basic_vals = {k: str(result.get(k))[:50] if result.get(k) is not None else None for k in ['title', 'product_name', 'price', 'stock_count', 'stock', 'review_count', 'listed_at']}
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_ret.dumps({"timestamp": int(time.time()*1000), "location": "product_data_crawler.py:return_result", "message": "爬取函数返回结果", "data": {"url": product_url, "all_keys": list(result.keys()), "ranking": _rank_vals, "basic": _basic_vals, "total_fields": len(result)}, "hypothesisId": "G2,G3", "runId": "field-debug"}, default=str) + "\n")
            # 额外标记字段数是否异常少，用于定位“爬取不完整”的商品
            try:
                _too_few_fields = len(result) < 15
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_ret.dumps({
                        "timestamp": int(time.time() * 1000),
                        "location": "product_data_crawler.py:field_count_check",
                        "message": "字段数量检查",
                        "data": {
                            "url": product_url,
                            "total_fields": len(result),
                            "too_few_fields": _too_few_fields,
                            "keys_sample": list(result.keys())[:20],
                        },
                        "hypothesisId": "H_incomplete_fields",
                        "runId": "incomplete-debug"
                    }, default=str) + "\n")
            except Exception:
                # 调试日志本身失败时静默忽略
                pass
            # #endregion
            logger.info(f"[爬取完成] 产品数据爬取完成 - URL: {product_url}, 总字段数: {len(result)}, 总耗时: {total_elapsed:.2f}秒")
            return result
            
        except PlaywrightTimeoutError as e:
            # 连接超时：标记代理失败 / 重启 BitBrowser 窗口
            if config.BITBROWSER_ENABLED and window_info:
                bitbrowser_manager.restart_window(window_info['id'])
                logger.warning(f"[BitBrowser窗口重启] 连接超时，重启窗口 - 窗口ID: {window_info['id']}")
            elif proxy_str:
                proxy_manager.mark_proxy_failed(proxy_str)
                logger.warning(f"[代理标记失败] 连接超时，标记代理为失败 - 代理: {proxy_str}")
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 产品数据爬取超时 - URL: {product_url}, 错误: {str(e)}, 总耗时: {total_elapsed:.2f}秒")
            raise
        except PlaywrightError as e:
            # 浏览器错误/主机断开：标记代理失败 / 重启 BitBrowser 窗口
            error_msg = str(e)
            if config.BITBROWSER_ENABLED and window_info:
                print(f"[BitBrowser错误详情] 浏览器错误 - 窗口ID: {window_info['id']}, 错误: {error_msg}")
                bitbrowser_manager.restart_window(window_info['id'])
                logger.warning(f"[BitBrowser窗口重启] 浏览器错误，重启窗口 - 窗口ID: {window_info['id']}, 错误: {error_msg}")
            elif proxy_str:
                print(f"[代理错误详情] 浏览器错误 - 代理: {proxy_str}, 错误: {error_msg}")
                proxy_manager.mark_proxy_failed(proxy_str)
                logger.warning(f"[代理标记失败] 浏览器错误，标记代理为失败 - 代理: {proxy_str}, 错误: {error_msg}")
            total_elapsed = time.time() - start_time
            print(f"[爬取失败详情] 产品数据爬取浏览器错误 - URL: {product_url}, 错误: {str(e)}, 总耗时: {total_elapsed:.2f}秒")
            logger.error(f"[爬取失败] 产品数据爬取浏览器错误 - URL: {product_url}, 错误: {str(e)}, 总耗时: {total_elapsed:.2f}秒")
            raise
        except Exception as e:
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 产品数据爬取错误 - URL: {product_url}, 错误: {str(e)}, 错误类型: {type(e).__name__}, 总耗时: {total_elapsed:.2f}秒", exc_info=True)
            raise
        finally:
            # #region agent log
            import json as _json
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json.dumps({"timestamp": int(time.time()*1000), "location": "product_data_crawler.py:finally", "message": "Finally block entered", "data": {"url": product_url, "has_window_info": window_info is not None, "window_id": window_info.get('id') if window_info else None, "has_context": context is not None, "bitbrowser_enabled": config.BITBROWSER_ENABLED}, "hypothesisId": "H2"}) + "\n")
            # #endregion
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
            
            # 释放独占资源（无论成功还是失败都要释放）
            if config.BITBROWSER_ENABLED and window_info:
                try:
                    print(f"[BitBrowser释放] 准备释放窗口: {window_info['id']}")
                    bitbrowser_manager.release_window(window_info['id'])
                    print(f"[BitBrowser释放] 窗口释放成功")
                except Exception as release_error:
                    print(f"[BitBrowser释放错误] 释放独占窗口时出错: {str(release_error)}, 错误类型: {type(release_error).__name__}")
                    logger.error(f"[BitBrowser释放失败] 释放独占窗口时出错: {str(release_error)}, 错误类型: {type(release_error).__name__}", exc_info=True)
            elif proxy_dict:
                try:
                    print(f"[代理释放] 准备释放代理: {proxy_dict.get('_raw', 'unknown')}")
                    proxy_manager.release_proxy(proxy_dict)
                    print(f"[代理释放] 代理释放成功")
                except Exception as release_error:
                    print(f"[代理释放错误] 释放独占代理时出错: {str(release_error)}, 错误类型: {type(release_error).__name__}")
                    logger.error(f"[代理释放失败] 释放独占代理时出错: {str(release_error)}, 错误类型: {type(release_error).__name__}", exc_info=True)


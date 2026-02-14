"""动态数据提取器

从产品详情页提取动态数据（可变字段）
"""
import logging
import re
import time
import threading
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin, urlparse
from datetime import datetime
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError
from app.config import config

logger = logging.getLogger(__name__)

# 延迟导入，避免循环依赖
def _get_error_log_imports():
    """延迟导入ErrorLog相关模块"""
    try:
        from app.models.crawl_task import ErrorLog, ErrorType
        from sqlalchemy.orm import Session
        return ErrorLog, ErrorType, Session
    except ImportError:
        return None, None, None

# ── 类目排名页缓存 ─────────────────────────────────────────────────
# 同一类目下多个产品共用同一次页面加载结果，消除每次加载返回不同排序的问题
# key: page_url -> {"data": {pid: [{"rank":int,"is_ad":bool}, ...]}, "ts": float}
_category_rank_cache: Dict[str, dict] = {}
_category_rank_locks: Dict[str, threading.Lock] = {}
_category_rank_global_lock = threading.Lock()
_CATEGORY_CACHE_TTL = 300  # 5 分钟


def _get_category_page_lock(page_url: str) -> threading.Lock:
    """获取每个类目页 URL 对应的独占锁，保证同一页面只加载一次"""
    with _category_rank_global_lock:
        if page_url not in _category_rank_locks:
            _category_rank_locks[page_url] = threading.Lock()
        return _category_rank_locks[page_url]


# ── 店铺排名页缓存 ─────────────────────────────────────────────────
# 同一店铺下多个产品共用同一次页面加载结果，避免重复加载导致超时/验证码
# key: page_url -> {"data": {pid: rank(int)}, "ts": float}
_store_rank_cache: Dict[str, dict] = {}
_store_rank_locks: Dict[str, threading.Lock] = {}
_store_rank_global_lock = threading.Lock()
_STORE_CACHE_TTL = 300  # 5 分钟


def _get_store_page_lock(page_url: str) -> threading.Lock:
    """获取每个店铺页 URL 对应的独占锁，保证同一页面只加载一次"""
    with _store_rank_global_lock:
        if page_url not in _store_rank_locks:
            _store_rank_locks[page_url] = threading.Lock()
        return _store_rank_locks[page_url]


def _get_store_rank_from_cache_by_vendor_slug(vendor_slug: str, product_id: str) -> Optional[tuple]:
    """
    尝试在已加载的店铺缓存中，根据 vendor slug 直接获取某个产品的店铺排名。
    
    Returns:
        (shop_url, rank) 或 None
    """
    try:
        with _store_rank_global_lock:
            for page_url, entry in _store_rank_cache.items():
                try:
                    parsed = urlparse(page_url)
                except Exception:
                    continue
                # 只匹配当前店铺的 vendors/vendor/{slug} 路径
                if f"/vendors/vendor/{vendor_slug}" not in parsed.path:
                    continue
                data = entry.get("data") or {}
                if product_id in data:
                    rank = data[product_id]
                    # 还原基础 shop_url（去掉 /p{n}/c 分页后缀）
                    base_path = parsed.path.rstrip("/")
                    base_path = re.sub(r"/p\d+/c$", "", base_path)
                    shop_url = f"{parsed.scheme}://{parsed.netloc}{base_path}?ref=seller-page-see-all-products"
                    return shop_url, rank
    except Exception:
        return None



class DynamicDataExtractor:
    """从产品详情页提取动态数据的提取器"""
    
    BASE_URL = "https://www.emag.ro"
    
    def __init__(self, base_url: str = BASE_URL):
        """
        初始化动态数据提取器
        
        Args:
            base_url: 基础URL，用于解析相对链接
        """
        self.base_url = base_url
    
    def extract_basic_fields(self, page: Page) -> Dict[str, Any]:
        """
        提取基础动态字段（价格、库存、评论等）
        
        Args:
            page: Playwright Page 对象
            
        Returns:
            包含基础动态字段的字典：
            - price: 价格
            - review_count: 评论数量
            - is_fbe: 是否FBE配送
            - latest_review_date: 最新评论日期
            - reviews_score: 评论评分
            - stock_count: 库存数量
            - has_resellers: 是否有转售商
        """
        result: Dict[str, Any] = {}
        
        try:
            # 等待页面加载：超时或验证码时抛出异常，不继续执行
            # #region agent log
            import json as _json_dyn_wait, time as _time_dyn_wait
            _wait_start = _time_dyn_wait.time()
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_dyn_wait.dumps({
                        "timestamp": int(_time_dyn_wait.time() * 1000),
                        "location": "dynamic_data_extractor.py:before_wait_domcontentloaded",
                        "message": "准备等待domcontentloaded+价格元素",
                        "data": {
                            "timeout_ms": 20000
                        },
                        "hypothesisId": "H3",
                        "runId": "timeout-debug"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            try:
                # 使用 domcontentloaded + 价格元素等待替代 networkidle
                page.wait_for_load_state('domcontentloaded', timeout=20000)
                # 等待价格元素出现，确保动态数据已加载
                try:
                    page.wait_for_selector('.product-new-price, [itemprop="price"], .product-price', timeout=8000)
                except Exception:
                    pass  # 元素等待失败不中断
                
                # #region agent log
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_dyn_wait.dumps({
                            "timestamp": int(_time_dyn_wait.time() * 1000),
                            "location": "dynamic_data_extractor.py:after_wait_domcontentloaded",
                            "message": "domcontentloaded+价格元素等待完成",
                            "data": {
                                "elapsed_ms": int((_time_dyn_wait.time() - _wait_start) * 1000)
                            },
                            "hypothesisId": "H3",
                            "runId": "networkidle-opt"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
            except PlaywrightTimeoutError as e:
                # 超时：抛出异常，不继续执行
                # #region agent log
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_dyn_wait.dumps({
                            "timestamp": int(_time_dyn_wait.time() * 1000),
                            "location": "dynamic_data_extractor.py:wait_domcontentloaded_timeout",
                            "message": "domcontentloaded等待超时",
                            "data": {
                                "error_type": type(e).__name__,
                                "error_message": str(e)[:300],
                                "elapsed_ms": int((_time_dyn_wait.time() - _wait_start) * 1000),
                                "timeout_ms": 20000
                            },
                            "hypothesisId": "H3",
                            "runId": "networkidle-opt"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                logger.error(f"DynamicDataExtractor wait_for_load_state('domcontentloaded') 超时: {e}")
                raise
            
            # 检查验证码
            try:
                from app.utils.captcha_handler import captcha_handler
                page_content = page.content()
                if captcha_handler.detect_captcha(page_content, page_content):
                    logger.warning(f"[动态信息提取] 检测到验证码")
                    raise ValueError("Captcha detected during dynamic info extraction")
            except ValueError:
                # 验证码异常直接抛出
                raise
            except Exception as captcha_check_err:
                # 其他异常记录但不中断
                logger.debug(f"验证码检测异常（可忽略）: {captcha_check_err}")
            
            # 提取价格
            result['price'] = self._extract_price(page)
            
            # 提取评论数量
            result['review_count'] = self._extract_review_count(page)
            
            # 提取是否FBE
            result['is_fbe'] = self._extract_is_fbe(page)
            
            # 提取最新评论日期
            result['latest_review_date'] = self._extract_latest_review_date(page)
            
            # 提取评论评分
            result['reviews_score'] = self._extract_reviews_score(page)
            
            # 提取库存数量
            result['stock_count'] = self._extract_stock_count(page)
            
            # 提取是否有转售商
            result['has_resellers'] = self._extract_has_resellers(page)
            
            logger.debug(f"Extracted dynamic basic fields: price={result.get('price')}, review_count={result.get('review_count')}")
            
        except (PlaywrightTimeoutError, ValueError) as e:
            # 超时或验证码：抛出异常，不继续执行
            logger.error(f"Error extracting dynamic basic fields (timeout/captcha): {e}")
            raise
        except Exception as e:
            # 其他异常：也抛出，确保数据完整性
            logger.error(f"Error extracting dynamic basic fields: {e}")
            raise
        
        return result
    
    def extract_rankings(
        self,
        page: Page,
        product_url: str,
        context=None,  # BrowserContext，用于遍历页面
        shop_url: Optional[str] = None,  # 外部传入的店铺链接（从FilterPool获取）
        category_url: Optional[str] = None,  # 外部传入的类目链接（从FilterPool获取）
        task_id: Optional[int] = None,  # 任务ID，用于记录错误
        db=None  # 数据库会话，用于记录错误
    ) -> Dict[str, Any]:
        """
        提取排名字段（需要遍历多个页面）
        
        Args:
            page: Playwright Page 对象（产品详情页）
            product_url: 产品URL（用于提取data-availability-id）
            context: BrowserContext对象（用于打开新页面）
            shop_url: 外部传入的店铺链接（可选，从FilterPool获取，避免重复爬取）
            category_url: 外部传入的类目链接（可选，从FilterPool获取，避免重复爬取）
            task_id: 任务ID（可选，用于记录排名提取失败的错误）
            db: 数据库会话（可选，用于记录排名提取失败的错误）
            
        Returns:
            包含排名字段的字典：
            - ad_category_rank: 广告类目排名
            - category_rank: 类目排名
            - store_rank: 店铺排名
        """
        result: Dict[str, Any] = {}
        
        if not context:
            logger.warning("No context provided, skipping rankings extraction")
            # 记录错误
            if task_id and db:
                self._log_ranking_error(
                    task_id, db, "no_context",
                    f"排名提取跳过: 无context - URL: {product_url}"
                )
            # #region agent log
            import json as _json_rank_ctx
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_rank_ctx.dumps({
                    "timestamp": int(time.time() * 1000),
                    "location": "dynamic_data_extractor.py:extract_rankings:skip_no_context",
                    "message": "排名提取跳过: 无context",
                    "data": {"product_url": product_url},
                    "hypothesisId": "H_rank_early_return"
                }, ensure_ascii=False) + "\n")
            # #endregion
            return result
        
        try:
            # 从产品URL中提取data-availability-id
            product_id = self._extract_product_id_from_url(product_url)
            
            if not product_id:
                logger.warning(f"Could not extract product ID from URL: {product_url}")
                # #region agent log
                import json as _json_rank_pid
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_rank_pid.dumps({
                        "timestamp": int(time.time() * 1000),
                        "location": "dynamic_data_extractor.py:extract_rankings:skip_no_product_id",
                        "message": "排名提取跳过: 无product_id",
                        "data": {"product_url": product_url},
                        "hypothesisId": "H_rank_early_return"
                    }, ensure_ascii=False) + "\n")
                # #endregion
                return result
            
            # 优先使用外部传入的类目URL，如果没有则从页面提取
            if not category_url:
                category_url = self._extract_category_url_from_page(page)
            else:
                logger.info(f"[排名提取] 使用外部传入的category_url: {category_url}")
            
            if not category_url:
                logger.warning("Could not extract category URL")
                # #region agent log
                import json as _json_rank_cat
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_rank_cat.dumps({
                        "timestamp": int(time.time() * 1000),
                        "location": "dynamic_data_extractor.py:extract_rankings:skip_no_category_url",
                        "message": "排名提取跳过: 无category_url",
                        "data": {"product_url": product_url, "product_id": product_id},
                        "hypothesisId": "H_rank_early_return"
                    }, ensure_ascii=False) + "\n")
                # #endregion
                return result
            brand_category_url = None
            # 将类目URL写入结果，便于上游保存到筛选池
            result["category_url"] = category_url
            try:
                breadcrumb_links = page.locator('.breadcrumb-inner a').all()
            except Exception:
                breadcrumb_links = []
            for link in breadcrumb_links:
                try:
                    href = link.get_attribute('href')
                except Exception:
                    href = None
                if not href:
                    continue
                if 'ref=back-breadcrumb' in href:
                    continue
                if '/brand/' in href and '/c' in href:
                    brand_category_url = href
            
            # 先从商品页保存店铺介绍页URL（dotted-link），无论后续店铺页是否成功
            shop_intro_url = None
            try:
                intro_link = page.locator('a.dotted-link').first
                if intro_link.count() > 0:
                    intro_href = intro_link.get_attribute("href")
                    if intro_href:
                        shop_intro_url = self._normalize_url(intro_href)
                        result["shop_intro_url"] = shop_intro_url
                        # #region agent log
                        import json as _json_intro_found, time as _time_intro_found
                        try:
                            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                _f.write(_json_intro_found.dumps({
                                    "timestamp": int(_time_intro_found.time() * 1000),
                                    "location": "dynamic_data_extractor.py:extract_rankings:shop_intro_url_found",
                                    "message": "找到店铺介绍页URL",
                                    "data": {
                                        "product_url": product_url,
                                        "product_id": product_id,
                                        "shop_intro_url": shop_intro_url
                                    },
                                    "hypothesisId": "H_shop_intro_found",
                                    "runId": "shop-url-fix"
                                }, ensure_ascii=False) + "\n")
                        except Exception:
                            pass
                        # #endregion
            except Exception:
                # 介绍页URL获取失败不影响主流程
                shop_intro_url = None

            # ── 检测是否为eMAG官方自营店 ──
            # eMAG自营店无法获取店铺商品列表链接，也无需提取店铺排名
            is_emag_official = False
            try:
                _dotted_link_el = page.locator('a.dotted-link')
                if _dotted_link_el.count() > 0:
                    _seller_name = _dotted_link_el.first.inner_text().strip()
                    if _seller_name and 'emag' in _seller_name.lower():
                        is_emag_official = True
                        logger.info(f"[排名提取] 检测到eMAG官方自营店，跳过店铺排名 - URL: {product_url}, seller: {_seller_name}")
                else:
                    # 没有 dotted-link，也可能是eMAG自营（无卖家链接）
                    # 检查页面上是否有eMAG自营标识
                    pass
            except Exception:
                pass
            # #region agent log
            import json as _json_emag_detect
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_emag_detect.dumps({
                        "timestamp": int(time.time() * 1000),
                        "location": "dynamic_data_extractor.py:extract_rankings:emag_official_detect",
                        "message": "eMAG官方自营店检测结果",
                        "data": {
                            "product_url": product_url,
                            "product_id": product_id,
                            "is_emag_official": is_emag_official,
                            "shop_intro_url": shop_intro_url
                        },
                        "hypothesisId": "H_emag_detect",
                        "runId": "emag-official-fix"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion

            # ── eMAG官方自营店：跳过所有排名提取（类目+广告+店铺） ──
            if is_emag_official:
                result["is_emag_official"] = True
                logger.info(f"[排名提取] eMAG官方自营店，跳过所有排名提取（类目+广告+店铺）- URL: {product_url}")
                # #region agent log
                import json as _json_emag_skip_all
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_emag_skip_all.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "dynamic_data_extractor.py:extract_rankings:emag_skip_all_ranks",
                            "message": "eMAG官方自营店，跳过所有排名提取",
                            "data": {
                                "product_url": product_url,
                                "product_id": product_id,
                                "category_url": category_url,
                                "shop_intro_url": shop_intro_url
                            },
                            "hypothesisId": "H_emag_skip_all",
                            "runId": "emag-official-fix-v2"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                return result

            # 优先尝试从已缓存的店铺数据中直接获取店铺排名（同一店铺且已加载过店铺页时生效）
            if shop_intro_url:
                try:
                    from urllib.parse import urlparse as _urlparse_vendor
                    _parsed_intro = _urlparse_vendor(shop_intro_url)
                    _parts = [p for p in _parsed_intro.path.split("/") if p]
                    vendor_slug = _parts[0] if _parts else None
                except Exception:
                    vendor_slug = None

                if vendor_slug:
                    cached = _get_store_rank_from_cache_by_vendor_slug(vendor_slug, product_id)
                    if cached:
                        cached_shop_url, cached_rank = cached
                        result["shop_url"] = cached_shop_url
                        result["store_rank"] = cached_rank
                        # #region agent log
                        import json as _json_rank_shop_cache, time as _time_rank_shop_cache
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_rank_shop_cache.dumps({
                                "timestamp": int(_time_rank_shop_cache.time() * 1000),
                                "location": "dynamic_data_extractor.py:extract_rankings:store_from_cache",
                                "message": "店铺排名直接命中缓存, 跳过店铺URL加载",
                                "data": {
                                    "product_url": product_url,
                                    "product_id": product_id,
                                    "vendor_slug": vendor_slug,
                                    "shop_url": cached_shop_url,
                                    "store_rank": cached_rank
                                },
                                "hypothesisId": "H_rank_store_cache"
                            }, ensure_ascii=False) + "\n")
                        # #endregion
                        shop_url = cached_shop_url
                    else:
                        shop_url = None
                else:
                    shop_url = None
            else:
                shop_url = None

            # 优先使用外部传入的shop_url，如果没有则从页面提取
            if not shop_url:
                # 如果缓存未命中，再通过介绍页 + 店铺页网络请求获取 shop_url 和 store_rank
                if not result.get("store_rank"):
                    try:
                        shop_url = self._extract_shop_url_from_page(page, context=context)
                    except (PlaywrightTimeoutError, ValueError) as e:
                        # 店铺介绍页/店铺页超时或验证码：如果已获取到shop_intro_url，说明应该能获取到shop_url，此时失败应抛出异常
                        if shop_intro_url:
                            logger.error(f"[店铺URL提取失败] 已获取到店铺介绍页URL但未获取到店铺商品列表URL - URL: {product_url}, shop_intro_url: {shop_intro_url}, 错误: {e}")
                            # #region agent log
                            import json as _json_rank_shop_fail
                            try:
                                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                    _f.write(_json_rank_shop_fail.dumps({
                                        "timestamp": int(time.time() * 1000),
                                        "location": "dynamic_data_extractor.py:extract_rankings:shop_url_extraction_failed",
                                        "message": "店铺URL提取失败，抛出异常",
                                        "data": {
                                            "product_url": product_url,
                                            "product_id": product_id,
                                            "category_url": category_url,
                                            "shop_intro_url": shop_intro_url,
                                            "error": str(e)[:200],
                                            "error_type": type(e).__name__
                                        },
                                        "hypothesisId": "H_rank_shop_extraction_failed",
                                        "runId": "shop-url-fix"
                                    }, ensure_ascii=False) + "\n")
                            except Exception:
                                pass
                            # #endregion
                            # 抛出异常，确保任务失败并触发重试
                            raise
                        else:
                            # 如果没有shop_intro_url，说明可能是产品页本身的问题，只记录警告
                            logger.warning(f"[店铺URL超时] 未获取到店铺介绍页URL，仅跳过店铺排名 - URL: {product_url}, 错误: {e}")
                            # #region agent log
                            import json as _json_rank_shop_to
                            try:
                                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                    _f.write(_json_rank_shop_to.dumps({
                                        "timestamp": int(time.time() * 1000),
                                        "location": "dynamic_data_extractor.py:extract_rankings:shop_timeout_no_intro",
                                        "message": "店铺URL获取超时（无shop_intro_url），仅跳过store_rank",
                                        "data": {
                                            "product_url": product_url,
                                            "product_id": product_id,
                                            "category_url": category_url,
                                            "error": str(e)[:200]
                                        },
                                        "hypothesisId": "H_rank_shop_timeout_no_intro",
                                        "runId": "shop-url-fix"
                                    }, ensure_ascii=False) + "\n")
                            except Exception:
                                pass
                            # #endregion
                            shop_url = None
                    except Exception as e:
                        # 其他异常：如果已获取到shop_intro_url，说明应该能获取到shop_url，此时失败应抛出异常
                        if shop_intro_url:
                            logger.error(f"[店铺URL提取失败] 已获取到店铺介绍页URL但未获取到店铺商品列表URL - URL: {product_url}, shop_intro_url: {shop_intro_url}, 错误: {e}")
                            # #region agent log
                            import json as _json_rank_shop_err
                            try:
                                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                    _f.write(_json_rank_shop_err.dumps({
                                        "timestamp": int(time.time() * 1000),
                                        "location": "dynamic_data_extractor.py:extract_rankings:shop_url_extraction_error",
                                        "message": "店铺URL提取失败（其他异常），抛出异常",
                                        "data": {
                                            "product_url": product_url,
                                            "product_id": product_id,
                                            "category_url": category_url,
                                            "shop_intro_url": shop_intro_url,
                                            "error": str(e)[:200],
                                            "error_type": type(e).__name__
                                        },
                                        "hypothesisId": "H_rank_shop_extraction_error",
                                        "runId": "shop-url-fix"
                                    }, ensure_ascii=False) + "\n")
                            except Exception:
                                pass
                            # #endregion
                            # 抛出异常，确保任务失败并触发重试
                            raise
                        else:
                            # 如果没有shop_intro_url，说明可能是产品页本身的问题，只记录警告
                            logger.warning(f"[店铺URL提取失败] 未获取到店铺介绍页URL，仅跳过店铺排名 - URL: {product_url}, 错误: {e}")
                            shop_url = None

                    if not shop_url:
                        # 如果已获取到shop_intro_url但未获取到shop_url，说明提取失败，应抛出异常
                        if shop_intro_url:
                            error_msg = f"已获取到店铺介绍页URL ({shop_intro_url}) 但未获取到店铺商品列表URL"
                            logger.error(f"[店铺URL提取失败] {error_msg} - URL: {product_url}, 产品ID: {product_id}")
                            # #region agent log
                            import json as _json_rank_shop_missing
                            try:
                                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                    _f.write(_json_rank_shop_missing.dumps({
                                        "timestamp": int(time.time() * 1000),
                                        "location": "dynamic_data_extractor.py:extract_rankings:shop_url_missing_after_intro",
                                        "message": "店铺URL缺失（已有shop_intro_url），抛出异常",
                                        "data": {
                                            "product_url": product_url,
                                            "product_id": product_id,
                                            "category_url": category_url,
                                            "shop_intro_url": shop_intro_url,
                                            "extract_shop_url_called": True,
                                            "extract_shop_url_returned_none": True
                                        },
                                        "hypothesisId": "H_rank_shop_missing_after_intro",
                                        "runId": "shop-url-fix"
                                    }, ensure_ascii=False) + "\n")
                            except Exception:
                                pass
                            # #endregion
                            # 抛出异常，确保任务失败并触发重试
                            raise ValueError(error_msg)
                        else:
                            # 如果没有shop_intro_url，说明可能是产品页本身的问题，只记录警告
                            logger.warning("Could not extract shop URL, skipping store_rank only")
                            # #region agent log
                            import json as _json_rank_shop
                            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                _f.write(_json_rank_shop.dumps({
                                    "timestamp": int(time.time() * 1000),
                                    "location": "dynamic_data_extractor.py:extract_rankings:no_shop_url",
                                    "message": "店铺URL缺失（无shop_intro_url），仅跳过store_rank",
                                    "data": {"product_url": product_url, "product_id": product_id, "category_url": category_url},
                                    "hypothesisId": "H_rank_no_store",
                                    "runId": "shop-url-fix"
                                }, ensure_ascii=False) + "\n")
                            # #endregion
                    else:
                        # 成功拿到店铺商品列表URL，写入结果
                        result["shop_url"] = shop_url
            else:
                # 使用外部传入的shop_url
                logger.info(f"[排名提取] 使用外部传入的shop_url: {shop_url}")
                result["shop_url"] = shop_url

            
            
            # ── 清除浏览器状态，消除个性化推荐对排名的影响 ──
            try:
                context.clear_cookies()
                logger.debug("[排名提取] 已清除 context cookies")
            except Exception as _clear_err:
                logger.debug(f"[排名提取] 清除 cookies 失败（可忽略）: {_clear_err}")
            # 清除 localStorage / sessionStorage（通过当前 page 执行）
            try:
                page.evaluate("() => { try { localStorage.clear(); sessionStorage.clear(); } catch(e) {} }")
                logger.debug("[排名提取] 已清除 localStorage/sessionStorage")
            except Exception:
                pass
            
            # 提取类目排名（遍历类目页前3页，仅在总类目下查找）
            try:
                category_ranks = self._extract_category_rank(context, category_url, product_id, max_pages=3)
                result['category_rank'] = category_ranks.get('category_rank')
                result['ad_category_rank'] = category_ranks.get('ad_category_rank')
            except PlaywrightTimeoutError as e:
                # 类目排名加载超时：抛出异常，确保任务失败并触发重试
                logger.error(f"[类目排名超时] URL: {product_url}, 错误: {e}")
                if task_id and db:
                    self._log_ranking_error(
                        task_id, db, "category_rank_timeout",
                        f"类目排名提取超时 - URL: {product_url}, category_url: {category_url}, 错误: {str(e)[:200]}"
                    )
                # #region agent log
                import json as _json_cat_timeout
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_cat_timeout.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "dynamic_data_extractor.py:extract_rankings:category_rank_timeout",
                            "message": "类目排名提取超时，抛出异常",
                            "data": {
                                "product_url": product_url,
                                "product_id": product_id,
                                "category_url": category_url,
                                "error": str(e)[:200]
                            },
                            "hypothesisId": "H_rank_cat_timeout",
                            "runId": "ranking-fix"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                # 抛出异常，确保任务失败并触发重试
                raise
            except ValueError as e:
                # 验证码异常：抛出异常，确保任务失败并触发重试
                if "Captcha" in str(e):
                    logger.error(f"[类目排名验证码] URL: {product_url}, 错误: {e}")
                    if task_id and db:
                        self._log_ranking_error(
                            task_id, db, "category_rank_error",
                            f"类目排名提取遇到验证码 - URL: {product_url}, category_url: {category_url}, 错误: {str(e)[:200]}"
                        )
                    # #region agent log
                    import json as _json_cat_captcha
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_cat_captcha.dumps({
                                "timestamp": int(time.time() * 1000),
                                "location": "dynamic_data_extractor.py:extract_rankings:category_rank_captcha",
                                "message": "类目排名提取遇到验证码，抛出异常",
                                "data": {
                                    "product_url": product_url,
                                    "product_id": product_id,
                                    "category_url": category_url,
                                    "error": str(e)[:200]
                                },
                                "hypothesisId": "H_rank_cat_captcha",
                                "runId": "ranking-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    # 抛出异常，确保任务失败并触发重试
                    raise
                else:
                    # 其他ValueError也抛出
                    raise
            except Exception as e:
                # 其他异常（网络错误等）：抛出异常，确保任务失败并触发重试
                logger.error(f"[类目排名错误] URL: {product_url}, 错误: {e}")
                if task_id and db:
                    self._log_ranking_error(
                        task_id, db, "category_rank_error",
                        f"类目排名提取失败 - URL: {product_url}, category_url: {category_url}, 错误: {str(e)[:200]}"
                    )
                # #region agent log
                import json as _json_cat_error
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_cat_error.dumps({
                            "timestamp": int(time.time() * 1000),
                            "location": "dynamic_data_extractor.py:extract_rankings:category_rank_error",
                            "message": "类目排名提取失败，抛出异常",
                            "data": {
                                "product_url": product_url,
                                "product_id": product_id,
                                "category_url": category_url,
                                "error": str(e)[:200],
                                "error_type": type(e).__name__
                            },
                            "hypothesisId": "H_rank_cat_error",
                            "runId": "ranking-fix"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                # 抛出异常，确保任务失败并触发重试
                raise
            
            

            # 提取店铺排名（遍历店铺商品列表前2页），仅在成功提取 shop_url 时执行
            if shop_url:
                try:
                    # 再次清除 cookies 避免类目页浏览产生的个性化数据影响店铺排名
                    try:
                        context.clear_cookies()
                    except Exception:
                        pass
                    result['store_rank'] = self._extract_store_rank(context, shop_url, product_id, max_pages=2)
                except PlaywrightTimeoutError as store_to:
                    # 店铺排名访问超时：抛出异常，确保任务失败并触发重试
                    logger.error(f"[店铺排名超时] URL: {product_url}, 错误: {store_to}")
                    if task_id and db:
                        self._log_ranking_error(
                            task_id, db, "store_rank_timeout",
                            f"店铺排名提取超时 - URL: {product_url}, shop_url: {shop_url}, 错误: {str(store_to)[:200]}"
                        )
                    # #region agent log
                    import json as _json_store_to
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_store_to.dumps({
                                "timestamp": int(time.time() * 1000),
                                "location": "dynamic_data_extractor.py:extract_rankings:store_timeout",
                                "message": "店铺排名提取超时，抛出异常",
                                "data": {"product_url": product_url, "product_id": product_id, "shop_url": shop_url, "error": str(store_to)[:200]},
                                "hypothesisId": "H_rank_store_timeout",
                                "runId": "ranking-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    # 抛出异常，确保任务失败并触发重试
                    raise
                except ValueError as e:
                    # 验证码异常：抛出异常，确保任务失败并触发重试
                    if "Captcha" in str(e):
                        logger.error(f"[店铺排名验证码] URL: {product_url}, 错误: {e}")
                        if task_id and db:
                            self._log_ranking_error(
                                task_id, db, "store_rank_error",
                                f"店铺排名提取遇到验证码 - URL: {product_url}, shop_url: {shop_url}, 错误: {str(e)[:200]}"
                            )
                        # #region agent log
                        import json as _json_store_captcha
                        try:
                            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                _f.write(_json_store_captcha.dumps({
                                    "timestamp": int(time.time() * 1000),
                                    "location": "dynamic_data_extractor.py:extract_rankings:store_rank_captcha",
                                    "message": "店铺排名提取遇到验证码，抛出异常",
                                    "data": {"product_url": product_url, "product_id": product_id, "shop_url": shop_url, "error": str(e)[:200]},
                                    "hypothesisId": "H_rank_store_captcha",
                                    "runId": "ranking-fix"
                                }, ensure_ascii=False) + "\n")
                        except Exception:
                            pass
                        # #endregion
                        # 抛出异常，确保任务失败并触发重试
                        raise
                    else:
                        # 其他ValueError也抛出
                        raise
                except Exception as store_rank_error:
                    # 其他异常（网络错误等）：抛出异常，确保任务失败并触发重试
                    logger.error(f"店铺排名提取失败 - URL: {product_url}, 错误: {str(store_rank_error)}")
                    if task_id and db:
                        self._log_ranking_error(
                            task_id, db, "store_rank_error",
                            f"店铺排名提取失败 - URL: {product_url}, shop_url: {shop_url}, 错误: {str(store_rank_error)[:200]}"
                        )
                    # #region agent log
                    import json as _json_store_error
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_store_error.dumps({
                                "timestamp": int(time.time() * 1000),
                                "location": "dynamic_data_extractor.py:extract_rankings:store_rank_error",
                                "message": "店铺排名提取失败，抛出异常",
                                "data": {
                                    "product_url": product_url,
                                    "product_id": product_id,
                                    "shop_url": shop_url,
                                    "error": str(store_rank_error)[:200],
                                    "error_type": type(store_rank_error).__name__
                                },
                                "hypothesisId": "H_rank_store_error",
                                "runId": "ranking-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    # 抛出异常，确保任务失败并触发重试
                    raise
            
            logger.debug(f"Extracted rankings: category_rank={result.get('category_rank')}, "
                        f"ad_category_rank={result.get('ad_category_rank')}, store_rank={result.get('store_rank')}")
            
        except PlaywrightTimeoutError as e:
            # 排名阶段的超时：抛出异常，确保任务失败并触发重试
            logger.error(f"[排名提取阶段超时] 抛出异常 - URL: {product_url}, 错误: {e}")
            if task_id and db:
                self._log_ranking_error(
                    task_id, db, "ranking_timeout",
                    f"排名提取阶段超时 - URL: {product_url}, 错误: {str(e)[:200]}"
                )
            # #region agent log
            import json as _json_rank_to
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_rank_to.dumps({
                        "timestamp": int(time.time() * 1000),
                        "location": "dynamic_data_extractor.py:extract_rankings:timeout_outer",
                        "message": "排名阶段超时, 抛出异常",
                        "data": {"product_url": product_url, "error": str(e)[:200]},
                        "hypothesisId": "H_rank_outer_timeout",
                        "runId": "ranking-fix"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            # 抛出异常，确保任务失败并触发重试
            raise
        except Exception as e:
            # 其他异常（网络错误等）：抛出异常，确保任务失败并触发重试
            logger.error(f"Error extracting rankings: {e}")
            if task_id and db:
                self._log_ranking_error(
                    task_id, db, "ranking_error",
                    f"排名提取失败 - URL: {product_url}, 错误: {str(e)[:200]}"
                )
            # #region agent log
            import json as _json_rank_err_outer
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_rank_err_outer.dumps({
                        "timestamp": int(time.time() * 1000),
                        "location": "dynamic_data_extractor.py:extract_rankings:error_outer",
                        "message": "排名提取失败（外层异常），抛出异常",
                        "data": {
                            "product_url": product_url,
                            "error": str(e)[:200],
                            "error_type": type(e).__name__
                        },
                        "hypothesisId": "H_rank_outer_error",
                        "runId": "ranking-fix"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            # 抛出异常，确保任务失败并触发重试
            raise
        
        return result
    
    def _extract_price(self, page: Page) -> Optional[float]:
        """提取价格"""
        try:
            selectors = [
                '[itemprop="price"]',
                '.product-new-price',
                '.product-old-price',
                '.price',
                '[data-price]',
                '.product-price',
            ]
            
            for selector in selectors:
                try:
                    price_elem = page.locator(selector).first
                    if price_elem.count() > 0:
                        price_text = price_elem.inner_text()
                        if price_text:
                            price = self._parse_price(price_text)
                            if price:
                                return price
                except Exception:
                    continue
            
            # 尝试从属性中提取
            try:
                price_elem = page.locator('[itemprop="price"]').first
                if price_elem.count() > 0:
                    price_attr = price_elem.get_attribute('content')
                    if price_attr:
                        return float(price_attr)
            except Exception:
                pass
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract price: {e}")
            return None
    
    def _extract_review_count(self, page: Page) -> Optional[int]:
        """提取评论数量"""
        try:
            # 查找评论链接
            review_selectors = [
                'a[href*="/reviews"]',
                'a[href*="#reviews"]',
                '.reviews-count',
                '.rating-count',
                '[data-reviews]',
            ]
            
            for selector in review_selectors:
                try:
                    review_elem = page.locator(selector).first
                    if review_elem.count() > 0:
                        review_text = review_elem.inner_text()
                        if review_text:
                            review_text_normalized = review_text.strip()
                            review_text_lower = review_text_normalized.lower()
                            # 优先匹配“xx de review-uri / review-uri”
                            count_match = re.search(r'(\d+)\s*(?:de\s+)?review(?:-|\s)?uri', review_text_lower)
                            if count_match:
                                parsed_count = int(count_match.group(1))
                                return parsed_count
                            # 使用正则表达式提取数字
                            match = re.search(r'\([^)]*?(\d+(?:[,\s]\d+)*)[^)]*?\)', review_text_normalized)
                            if not match:
                                # 如果没有括号，直接提取数字
                                match = re.search(r'(\d+(?:[,\s]\d+)*)', review_text_normalized.replace(',', ''))
                            if match:
                                count_str = match.group(1).replace(',', '').replace(' ', '')
                                if '.' in count_str:
                                    # 跳过可能是评分的小数
                                    continue
                                parsed_count = int(count_str)
                            return parsed_count
                except Exception:
                    continue
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract review count: {e}")
            return None
    
    def _extract_is_fbe(self, page: Page) -> bool:
        """提取是否FBE配送"""
        try:
            # 检查是否存在FBE标识
            fbe_selectors = [
                'div.product-highlight.not-own-delivery',
                '.product-highlight.not-own-delivery',
                '[data-fbe]',
            ]
            
            for selector in fbe_selectors:
                try:
                    fbe_elem = page.locator(selector).first
                    if fbe_elem.count() > 0:
                        return True
                except Exception:
                    continue
            
            # 检查文本内容
            try:
                page_text = page.inner_text('body').lower()
                if 'fulfilled by emag' in page_text or 'livrare din stoc emag' in page_text:
                    return True
            except Exception:
                pass
            
            return False
        except Exception as e:
            logger.debug(f"Failed to extract is_fbe: {e}")
            return False
    
    def _extract_latest_review_date(self, page: Page) -> Optional[datetime]:
        """提取最新评论日期"""
        try:
            # 查找评论列表
            review_list = page.locator('.product-conversations-list.js-reviews-list').first
            if review_list.count() > 0:
                # 获取第一个评论的日期
                first_review = review_list.locator('.review').first
                if first_review.count() > 0:
                    date_elem = first_review.locator('.review-date, [data-review-date]').first
                    if date_elem.count() > 0:
                        date_text = date_elem.inner_text()
                        if date_text:
                            return self._parse_date(date_text)
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract latest review date: {e}")
            return None
    
    def _extract_reviews_score(self, page: Page) -> Optional[float]:
        """提取评论评分"""
        try:
            score_selectors = [
                'div.reviews-general-rating.py-2',
                '.reviews-general-rating',
                '[itemprop="ratingValue"]',
                '.rating-value',
            ]
            
            for selector in score_selectors:
                try:
                    score_elem = page.locator(selector).first
                    if score_elem.count() > 0:
                        score_text = score_elem.inner_text()
                        if score_text:
                            # 提取数字
                            match = re.search(r'(\d+\.?\d*)', score_text)
                            if match:
                                parsed_score = float(match.group(1))
                                return parsed_score
                except Exception:
                    continue
            
            # 尝试从属性中提取
            try:
                score_elem = page.locator('[itemprop="ratingValue"]').first
                if score_elem.count() > 0:
                    score_attr = score_elem.get_attribute('content')
                    if score_attr:
                        return float(score_attr)
            except Exception:
                pass
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract reviews score: {e}")
            return None
    
    def _extract_stock_count(self, page: Page) -> Optional[int]:
        """提取库存数量"""
        try:
            em_offer_max = None
            max_attr = None
            try:
                em_offer_max = page.evaluate(
                    "() => (window.EM && EM.offer && EM.offer.buying_options && EM.offer.buying_options.max) ? EM.offer.buying_options.max : null"
                )
            except Exception:
                em_offer_max = None

            # 查找数量输入框的max属性
            stock_input = page.locator('input[max]').first
            if stock_input.count() > 0:
                max_attr = stock_input.get_attribute('max')
                if max_attr:
                    return int(max_attr)
            if em_offer_max is not None:
                try:
                    parsed_em_offer = int(em_offer_max)
                except Exception:
                    parsed_em_offer = None
                if parsed_em_offer is not None:
                    return parsed_em_offer
            
            # 备用：查找库存文本
            stock_selectors = [
                '.stock-info',
                '.availability',
                '[data-stock]',
            ]
            
            for selector in stock_selectors:
                try:
                    stock_elem = page.locator(selector).first
                    if stock_elem.count() > 0:
                        stock_text = stock_elem.inner_text().lower()
                        # 查找数字
                        match = re.search(r'(\d+)', stock_text)
                        if match:
                            return int(match.group(1))
                except Exception:
                    continue
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract stock count: {e}")
            return None
    
    def _extract_has_resellers(self, page: Page) -> bool:
        """提取是否有转售商"""
        try:
            # 查找"vezi toate ofertele"文本（罗马尼亚语：查看所有报价）
            has_text = page.locator('text=/vezi toate ofertele/i').first
            if has_text.count() > 0:
                return True
            
            # 备用：检查是否存在转售商相关的元素
            reseller_selectors = [
                '[data-resellers]',
                '.resellers',
                '.alternative-offers',
            ]
            
            for selector in reseller_selectors:
                try:
                    reseller_elem = page.locator(selector).first
                    if reseller_elem.count() > 0:
                        return True
                except Exception:
                    continue
            
            return False
        except Exception as e:
            logger.debug(f"Failed to extract has_resellers: {e}")
            return False
    
    # ── 类目排名：公共方法 ────────────────────────────────────────────

    def _build_category_page_url(self, base_url: str, page_num: int) -> Optional[str]:
        """构建类目页URL（使用emag默认排序 — 最受欢迎）"""
        try:
            parsed = urlparse(base_url)
            path = parsed.path or ""
            if re.search(r'/p\d+/c$', path):
                new_path = re.sub(r'/p\d+/c$', f'/p{page_num}/c', path)
                return parsed._replace(path=new_path, query='').geturl()
            elif path.endswith('/c'):
                base_path = path[:-2]
                return parsed._replace(path=f'{base_path}/p{page_num}/c', query='').geturl()
            else:
                return f"{base_url}?p={page_num}"
        except Exception:
            return f"{base_url}?p={page_num}"

    def _get_or_load_category_page(
        self,
        context,
        page_url: str,
        page_num: int,
        page_size: int,
    ) -> Dict[str, list]:
        """
        从缓存获取类目页产品位置，缓存未命中则加载页面并缓存。
        返回 {product_id: [{"rank": int, "is_ad": bool}, ...]}
        同一 page_url 只会被一个线程加载，其余线程等待。
        """
        key_lock = _get_category_page_lock(page_url)
        with key_lock:
            entry = _category_rank_cache.get(page_url)
            if entry and time.time() - entry["ts"] < _CATEGORY_CACHE_TTL:
                # #region agent log
                import json as _json
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json.dumps({"timestamp": int(time.time()*1000), "location": "dynamic_data_extractor.py:_get_or_load_category_page", "message": "Cache HIT", "data": {"page_url": page_url, "cached_products": len(entry["data"])}, "hypothesisId": "H7-fix"}) + "\n")
                # #endregion
                return entry["data"]

            # 缓存未命中 → 加载页面（此时持有 per-key 锁，其他线程排队等待）
            data = self._load_and_parse_category_page(context, page_url, page_num, page_size)
            _category_rank_cache[page_url] = {"data": data, "ts": time.time()}
            return data

    def _load_and_parse_category_page(
        self,
        context,
        page_url: str,
        page_num: int,
        page_size: int,
    ) -> Dict[str, list]:
        """加载类目页并提取所有产品的排名位置（缓存写入方）"""
        result: Dict[str, list] = {}
        category_page = context.new_page()
        try:
            # 拦截请求：移除 Cookie 和 Referer，消除个性化
            def _strip_tracking_headers(route):
                try:
                    headers = dict(route.request.headers)
                    for h in ('cookie', 'Cookie', 'referer', 'Referer'):
                        headers.pop(h, None)
                    route.continue_(headers=headers)
                except Exception:
                    route.continue_()

            category_page.route("**/*", _strip_tracking_headers)
            
            # #region agent log
            import json as _json_cat_goto, time as _time_cat_goto
            _cat_goto_start = _time_cat_goto.time()
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_cat_goto.dumps({
                        "timestamp": int(_time_cat_goto.time() * 1000),
                        "location": "dynamic_data_extractor.py:before_category_page_goto",
                        "message": "准备加载类目页",
                        "data": {
                            "page_url": page_url,
                            "page_num": page_num,
                            "timeout_ms": config.RANKING_PAGE_TIMEOUT
                        },
                        "hypothesisId": "H4",
                        "runId": "timeout-debug"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            # ── 内部重试：类目页 goto 遇到瞬时网络错误时重试 ──
            _MAX_CAT_GOTO = 3
            for _cat_attempt in range(_MAX_CAT_GOTO):
                try:
                    category_page.goto(page_url, wait_until='domcontentloaded', timeout=config.RANKING_PAGE_TIMEOUT)
                    
                    # #region agent log
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_cat_goto.dumps({
                                "timestamp": int(_time_cat_goto.time() * 1000),
                                "location": "dynamic_data_extractor.py:after_category_page_goto",
                                "message": "类目页加载完成",
                                "data": {
                                    "page_url": page_url,
                                    "elapsed_ms": int((_time_cat_goto.time() - _cat_goto_start) * 1000),
                                    "attempt": _cat_attempt + 1
                                },
                                "hypothesisId": "H4",
                                "runId": "timeout-debug"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    break  # goto 成功
                except Exception as _cat_goto_err:
                    # #region agent log
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json_cat_goto.dumps({
                                "timestamp": int(_time_cat_goto.time() * 1000),
                                "location": "dynamic_data_extractor.py:category_page_goto_error",
                                "message": "类目页加载失败",
                                "data": {
                                    "page_url": page_url,
                                    "error_type": type(_cat_goto_err).__name__,
                                    "error_message": str(_cat_goto_err)[:300],
                                    "elapsed_ms": int((_time_cat_goto.time() - _cat_goto_start) * 1000),
                                    "timeout_ms": config.RANKING_PAGE_TIMEOUT,
                                    "attempt": _cat_attempt + 1,
                                    "will_retry": _cat_attempt < _MAX_CAT_GOTO - 1 and not isinstance(_cat_goto_err, PlaywrightTimeoutError)
                                },
                                "hypothesisId": "H9_category_no_retry",
                                "runId": "retry-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    # 超时不重试（已消耗完整超时时间），非超时瞬时错误重试一次
                    if _cat_attempt < _MAX_CAT_GOTO - 1 and not isinstance(_cat_goto_err, PlaywrightTimeoutError):
                        logger.warning(
                            f"[类目页] goto 失败(attempt {_cat_attempt+1})，3秒后重试: {_cat_goto_err}"
                        )
                        try:
                            category_page.close()
                        except Exception:
                            pass
                        _time_cat_goto.sleep(5)
                        category_page = context.new_page()
                        category_page.route("**/*", _strip_tracking_headers)
                        _cat_goto_start = _time_cat_goto.time()  # 重置计时
                    else:
                        raise

            # 检测验证码
            try:
                from app.utils.captcha_handler import captcha_handler
                page_content = category_page.content()
                if captcha_handler.detect_captcha(page_content, page_content):
                    logger.warning(f"[类目页验证码] 检测到验证码 - URL: {page_url}")
                    raise ValueError(f"Captcha detected on category page: {page_url}")
            except ValueError:
                # 验证码异常直接抛出
                raise
            except Exception as e:
                # 其他异常记录但不中断
                logger.debug(f"验证码检测异常（可忽略）: {e}")

            # 等待页面完全加载（软失败：超时后仍尝试提取已加载的内容）
            _element_wait_ok = True
            try:
                # 等待card_grid容器出现（goto 后 JS 渲染仍需时间）
                card_grid = category_page.locator('#card_grid')
                card_grid.wait_for(state='attached', timeout=20000)
                
                # 等待至少一个产品卡片加载完成
                category_page.wait_for_selector(
                    '.card-item.card-standard.js-product-data.js-card-clickable',
                    timeout=20000
                )
                
                # 额外等待500ms确保动态内容加载完成
                category_page.wait_for_timeout(500)
            except PlaywrightTimeoutError as e:
                # 元素等待超时：不抛出异常，尝试用已加载的 DOM 继续提取
                _element_wait_ok = False
                logger.warning(f"类目页元素等待超时（软失败，继续提取）: {page_url}, 错误: {e}")
                # #region agent log
                try:
                    import json as _j_sw, time as _t_sw
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_j_sw.dumps({"timestamp": int(_t_sw.time()*1000), "location": "dynamic_data_extractor.py:element_wait_soft_fail", "message": "元素等待超时，软失败继续提取", "data": {"page_url": page_url, "error": str(e)[:200]}, "hypothesisId": "H_soft_fail", "runId": "round3-fix"}, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
            except Exception as e:
                # 其他异常（网络错误等）：抛出异常，确保任务失败并触发重试
                logger.error(f"等待类目页加载失败: {page_url}, 错误: {e}")
                raise

            # 定位 card_grid
            card_grid = category_page.locator('#card_grid')

            if card_grid.count() > 0:
                all_cards = card_grid.locator(
                    '.card-item.card-standard.js-product-data.js-card-clickable').all()
            else:
                all_cards = category_page.locator(
                    '.card-item.card-standard.js-product-data.js-card-clickable').all()

            # 遍历所有卡片，提取每个产品的排名
            ad_counter = 0  # 广告计数器
            normal_counter = 0  # 普通产品计数器
            for card in all_cards:
                availability_id = None
                try:
                    availability_id = card.get_attribute('data-availability-id')
                    if availability_id is None:
                        continue
                except Exception:
                    continue

                href_code = None
                try:
                    href = card.locator('a[href*="/pd/"]').first.get_attribute('href')
                    if href:
                        m = re.search(r'/pd/([^/]+)', href)
                        href_code = m.group(1) if m else None
                except Exception:
                    pass

                if not href_code:
                    continue

                is_ad = availability_id == "0"
                
                # 根据类型分别计数
                if is_ad:
                    ad_counter += 1
                    rank = (page_num - 1) * page_size + ad_counter
                else:
                    normal_counter += 1
                    rank = (page_num - 1) * page_size + normal_counter

                if href_code not in result:
                    result[href_code] = []
                result[href_code].append({"rank": rank, "is_ad": is_ad})

            # #region agent log
            import json as _json
            _first5 = list(result.keys())[:5]
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json.dumps({"timestamp": int(time.time()*1000), "location": "dynamic_data_extractor.py:_load_and_parse_category_page", "message": "Category page loaded & cached", "data": {"page_url": page_url, "page_num": page_num, "total_products": len(result), "total_cards": len(all_cards), "first_5_product_ids": _first5}, "hypothesisId": "H7-fix"}) + "\n")
            # #endregion

        finally:
            category_page.close()

        return result

    def _extract_category_rank(
        self,
        context,
        category_url: str,
        product_id: str,
        max_pages: int = 3
    ) -> Dict[str, Optional[int]]:
        """
        提取类目排名（遍历类目页）。
        同一类目 URL 的页面数据会被缓存，所有同类目产品共享同一次加载结果，
        避免每次加载返回不同排序导致多个产品拿到相同排名。
        """
        result: Dict[str, Optional[int]] = {'category_rank': None, 'ad_category_rank': None}
        page_size_for_rank = 60  # 每页固定60个商品

        try:
            for page_num in range(1, max_pages + 1):
                if result.get('category_rank') is not None and result.get('ad_category_rank') is not None:
                    break

                page_url = self._build_category_page_url(category_url, page_num)
                if not page_url:
                    continue

                # 从缓存获取或加载页面
                page_data = self._get_or_load_category_page(
                    context, page_url, page_num, page_size_for_rank)

                if not page_data or product_id not in page_data:
                    continue

                # 在缓存数据中查找该产品
                for entry in page_data[product_id]:
                    rank = entry['rank']
                    is_ad = entry['is_ad']

                    # #region agent log
                    import json as _json
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json.dumps({"timestamp": int(time.time()*1000), "location": "dynamic_data_extractor.py:_extract_category_rank:found", "message": "Product found (cached)", "data": {"product_id": product_id, "page_num": page_num, "rank": rank, "is_ad": is_ad, "page_url": page_url, "source": "cache"}, "hypothesisId": "H7-fix"}) + "\n")
                    # #endregion

                    if is_ad:
                        if result.get('ad_category_rank') is None:
                            result['ad_category_rank'] = rank
                    else:
                        # 取最小值（最小排名 = 最好位置）
                        if result.get('category_rank') is None or rank < result.get('category_rank'):
                            result['category_rank'] = rank

        except (PlaywrightTimeoutError, ValueError) as e:
            # 网络超时或验证码异常：向上抛出，让上层处理
            logger.error(f"类目排名提取失败（超时/验证码）: {e}")
            raise
        except Exception as e:
            # 其他异常（网络错误等）：向上抛出，让上层处理
            logger.error(f"类目排名提取失败（其他错误）: {e}")
            raise

        # 如果在前 max_pages 页中都没有找到商品，则分别记为 200（这是正常情况，表示排名很靠后）
        if result.get('category_rank') is None:
            result['category_rank'] = 200
        if result.get('ad_category_rank') is None:
            result['ad_category_rank'] = 200

        return result
    
    def _get_or_load_store_page(self, context, page_url: str) -> Dict[str, int]:
        """
        获取或加载店铺页面的产品排名数据（带缓存）
        
        返回: {product_id: rank} 字典
        """
        import json as _json_sp, time as _time_sp
        
        # 检查缓存
        now = time.time()
        cached = _store_rank_cache.get(page_url)
        if cached and (now - cached["ts"]) < _STORE_CACHE_TTL:
            # #region agent log
            _log_payload = {"timestamp": int(_time_sp.time() * 1000), "location": "dynamic_data_extractor.py:_get_or_load_store_page", "message": "Store cache HIT", "data": {"page_url": page_url, "cached_products": len(cached["data"])}, "hypothesisId": "H16-fix"}
            with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sp.dumps(_log_payload, ensure_ascii=False) + '\n')
            # #endregion
            return cached["data"]
        
        # 缓存未命中，获取锁加载
        lock = _get_store_page_lock(page_url)
        with lock:
            # double-check：另一个线程可能已经加载完成
            cached = _store_rank_cache.get(page_url)
            if cached and (now - cached["ts"]) < _STORE_CACHE_TTL:
                return cached["data"]
            
            # 实际加载页面
            product_ranks: Dict[str, int] = {}
            try:
                shop_page = context.new_page()
                # 禁用 Cookie 和 Referer 避免个性化推荐
                def _remove_tracking_shop(route):
                    try:
                        headers = dict(route.request.headers)
                        for h in ('cookie', 'Cookie', 'referer', 'Referer'):
                            headers.pop(h, None)
                        route.continue_(headers=headers)
                    except Exception:
                        route.continue_()
                shop_page.route("**/*", _remove_tracking_shop)
                
                # #region agent log
                import json as _json_shop_goto, time as _time_shop_goto
                _shop_goto_start = _time_shop_goto.time()
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_shop_goto.dumps({
                            "timestamp": int(_time_shop_goto.time() * 1000),
                            "location": "dynamic_data_extractor.py:before_store_page_goto",
                            "message": "准备加载店铺页",
                            "data": {
                                "page_url": page_url,
                                "timeout_ms": config.RANKING_PAGE_TIMEOUT
                            },
                            "hypothesisId": "H4",
                            "runId": "timeout-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                
                # ── 内部重试：店铺页 goto 瞬时网络错误重试 ──
                _MAX_STORE_GOTO = 3
                for _store_attempt in range(_MAX_STORE_GOTO):
                    try:
                        shop_page.goto(page_url, wait_until='domcontentloaded', timeout=config.RANKING_PAGE_TIMEOUT)
                        
                        # #region agent log
                        try:
                            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                _f.write(_json_shop_goto.dumps({
                                    "timestamp": int(_time_shop_goto.time() * 1000),
                                    "location": "dynamic_data_extractor.py:after_store_page_goto",
                                    "message": "店铺页加载完成",
                                    "data": {
                                        "page_url": page_url,
                                        "elapsed_ms": int((_time_shop_goto.time() - _shop_goto_start) * 1000),
                                        "attempt": _store_attempt + 1
                                    },
                                    "hypothesisId": "H4",
                                    "runId": "timeout-debug"
                                }, ensure_ascii=False) + "\n")
                        except Exception:
                            pass
                        # #endregion
                        break  # goto 成功
                    except Exception as _shop_goto_err:
                        # #region agent log
                        try:
                            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                                _f.write(_json_shop_goto.dumps({
                                    "timestamp": int(_time_shop_goto.time() * 1000),
                                    "location": "dynamic_data_extractor.py:store_page_goto_error",
                                    "message": "店铺页加载失败",
                                    "data": {
                                        "page_url": page_url,
                                        "error_type": type(_shop_goto_err).__name__,
                                        "error_message": str(_shop_goto_err)[:300],
                                        "elapsed_ms": int((_time_shop_goto.time() - _shop_goto_start) * 1000),
                                        "timeout_ms": config.RANKING_PAGE_TIMEOUT,
                                        "attempt": _store_attempt + 1,
                                        "will_retry": _store_attempt < _MAX_STORE_GOTO - 1 and not isinstance(_shop_goto_err, PlaywrightTimeoutError)
                                    },
                                    "hypothesisId": "H9_store_no_retry",
                                    "runId": "retry-fix"
                                }, ensure_ascii=False) + "\n")
                        except Exception:
                            pass
                        # #endregion
                        if _store_attempt < _MAX_STORE_GOTO - 1 and not isinstance(_shop_goto_err, PlaywrightTimeoutError):
                            logger.warning(
                                f"[店铺页] goto 失败(attempt {_store_attempt+1})，3秒后重试: {_shop_goto_err}"
                            )
                            try:
                                shop_page.close()
                            except Exception:
                                pass
                            _time_shop_goto.sleep(5)
                            shop_page = context.new_page()
                            shop_page.route("**/*", _remove_tracking_shop)
                            _shop_goto_start = _time_shop_goto.time()
                        else:
                            raise
                
                # 检测验证码
                try:
                    from app.utils.captcha_handler import captcha_handler
                    page_content = shop_page.content()
                    if captcha_handler.detect_captcha(page_content, page_content):
                        logger.warning(f"[店铺页验证码] 检测到验证码 - URL: {page_url}")
                        raise ValueError(f"Captcha detected on store page: {page_url}")
                except ValueError:
                    # 验证码异常直接抛出
                    raise
                except Exception as e:
                    # 其他异常记录但不中断
                    logger.debug(f"验证码检测异常（可忽略）: {e}")
                
                # 查找所有产品卡片
                products = shop_page.locator('.card-item.card-standard.js-product-data.js-card-clickable[data-availability-id]').all()
                if len(products) == 0:
                    products = shop_page.locator('[data-availability-id]').all()
                
                page_title = ""
                try:
                    page_title = shop_page.title()[:80]
                except Exception:
                    pass
                
                # 解析所有产品的 PNK_CODE 和 data-position
                _products_without_position = []
                _products_with_position = []
                for product in products:
                    try:
                        href = product.locator('a[href*="/pd/"]').first.get_attribute('href')
                        if not href:
                            continue
                        m = re.search(r'/pd/([^/]+)', href)
                        if not m:
                            continue
                        pid = m.group(1)
                        dp = product.get_attribute('data-position')
                        if dp:
                            try:
                                rank_val = int(dp)
                                product_ranks[pid] = rank_val
                                _products_with_position.append({"pid": pid, "rank": rank_val})
                            except ValueError:
                                _products_without_position.append({"pid": pid, "data_position": dp})
                        else:
                            _products_without_position.append({"pid": pid, "data_position": None})
                    except Exception as e:
                        # #region agent log
                        try:
                            _log_payload = {"timestamp": int(_time_sp.time() * 1000), "location": "dynamic_data_extractor.py:_get_or_load_store_page:parse_error", "message": "Error parsing product card", "data": {"page_url": page_url, "error": str(e)[:200]}, "hypothesisId": "H17"}
                            with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sp.dumps(_log_payload, ensure_ascii=False) + '\n')
                        except Exception:
                            pass
                        # #endregion
                        continue
                
                shop_page.close()
                
                # #region agent log
                _first_20_with_rank = _products_with_position[:20]
                _first_10_without_rank = _products_without_position[:10]
                _log_payload = {"timestamp": int(_time_sp.time() * 1000), "location": "dynamic_data_extractor.py:_get_or_load_store_page:loaded", "message": "Store page loaded & cached", "data": {"page_url": page_url, "total_products": len(product_ranks), "total_cards_found": len(products), "page_title": page_title, "first_20_with_rank": _first_20_with_rank, "first_10_without_rank": _first_10_without_rank, "all_product_ids": list(product_ranks.keys())[:30]}, "hypothesisId": "H17"}
                with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sp.dumps(_log_payload, ensure_ascii=False) + '\n')
                # #endregion
                
            except Exception as e:
                # #region agent log
                _log_payload = {"timestamp": int(_time_sp.time() * 1000), "location": "dynamic_data_extractor.py:_get_or_load_store_page:error", "message": "Store page load error", "data": {"page_url": page_url, "error": str(e)[:200], "error_type": type(e).__name__}, "hypothesisId": "H16-fix"}
                with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sp.dumps(_log_payload, ensure_ascii=False) + '\n')
                # #endregion
                logger.warning(f"加载店铺页面失败: {page_url}, 错误: {e}")
                if 'shop_page' in locals():
                    try:
                        shop_page.close()
                    except Exception:
                        pass
            
            # 缓存结果（即使为空也缓存，避免重复尝试失败的页面）
            _store_rank_cache[page_url] = {"data": product_ranks, "ts": time.time()}
            return product_ranks
    
    def _extract_store_rank(
        self,
        context,
        shop_url: str,
        product_id: str,
        max_pages: int = 2
    ) -> Optional[int]:
        """
        提取店铺排名（遍历店铺商品列表，使用缓存）
        
        流程：
        1. 对每页使用缓存：同一店铺页面只加载一次，所有产品共享结果
        2. 匹配到后返回 data-position 排名值
        3. 店铺排名获取前 max_pages 页数据
        4. 如获取不到记录200
        """
        import json as _json_sr, time as _time_sr
        try:
            # #region agent log
            _log_payload = {"timestamp": int(_time_sr.time() * 1000), "location": "dynamic_data_extractor.py:_extract_store_rank:entry", "message": "Store rank extraction started", "data": {"shop_url": shop_url, "product_id": product_id, "product_id_type": type(product_id).__name__, "product_id_len": len(product_id) if product_id else 0, "max_pages": max_pages}, "hypothesisId": "H17"}
            with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sr.dumps(_log_payload, ensure_ascii=False) + '\n')
            # #endregion
            
            # 预处理 shop_url：去掉 query 参数，构建 path-based 分页
            _parsed_shop = urlparse(shop_url)
            _base_shop_path = _parsed_shop.path.rstrip('/')
            _base_shop_path = re.sub(r'/p\d+/c$', '', _base_shop_path)
            _shop_base_url = f"{_parsed_shop.scheme}://{_parsed_shop.netloc}{_base_shop_path}"
            
            for page_num in range(1, max_pages + 1):
                page_url = f"{_shop_base_url}/p{page_num}/c"
                
                # 使用缓存获取页面数据
                page_data = self._get_or_load_store_page(context, page_url)
                
                # #region agent log
                _all_product_ids = list(page_data.keys())
                _first_20_ids = _all_product_ids[:20]
                _log_payload = {"timestamp": int(_time_sr.time() * 1000), "location": "dynamic_data_extractor.py:_extract_store_rank:page_check", "message": "Checking page for product", "data": {"shop_url": shop_url, "product_id": product_id, "page_num": page_num, "page_url": page_url, "total_products_in_page": len(page_data), "first_20_product_ids": _first_20_ids, "product_id_in_page": product_id in page_data}, "hypothesisId": "H17"}
                with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sr.dumps(_log_payload, ensure_ascii=False) + '\n')
                # #endregion
                
                if product_id in page_data:
                    rank = page_data[product_id]
                    # #region agent log
                    _log_payload = {"timestamp": int(_time_sr.time() * 1000), "location": "dynamic_data_extractor.py:_extract_store_rank:found", "message": "Product found in store", "data": {"product_id": product_id, "rank": rank, "page_num": page_num, "page_url": page_url, "source": "cache" if len(page_data) > 0 else "load"}, "hypothesisId": "H16-fix"}
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sr.dumps(_log_payload, ensure_ascii=False) + '\n')
                    # #endregion
                    logger.debug(f"通过缓存找到产品 {product_id}，店铺排名: {rank}")
                    return rank
            
            # 如果在前 max_pages 页中都没有找到商品，则记录200
            # #region agent log
            # 收集所有页面的产品 ID 列表用于调试
            _all_pages_product_ids = []
            for _pnum in range(1, max_pages + 1):
                _purl = f"{_shop_base_url}/p{_pnum}/c"
                _pdata = self._get_or_load_store_page(context, _purl)
                _all_pages_product_ids.extend(list(_pdata.keys())[:30])  # 每页最多取前30个
            _log_payload = {"timestamp": int(_time_sr.time() * 1000), "location": "dynamic_data_extractor.py:_extract_store_rank:not_found", "message": "Product not found in store pages", "data": {"shop_url": shop_url, "product_id": product_id, "max_pages": max_pages, "all_product_ids_sample": _all_pages_product_ids[:50], "product_id_lower": product_id.lower() if product_id else None, "product_ids_lower_sample": [pid.lower() for pid in _all_pages_product_ids[:20]]}, "hypothesisId": "H17"}
            with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f: _f.write(_json_sr.dumps(_log_payload, ensure_ascii=False) + '\n')
            # #endregion
            logger.warning(f"在前 {max_pages} 页店铺商品列表中未找到产品 {product_id}，记录排名为 200")
            return 200
            
        except Exception as e:
            logger.warning(f"提取店铺排名时发生错误: {e}", exc_info=True)
            return 200
    
    def _extract_product_id_from_url(self, product_url: str) -> Optional[str]:
        """从产品URL中提取product ID（data-availability-id）"""
        try:
            match = re.search(r'/pd/([^/]+)', product_url)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None
    
    def _extract_category_url_from_page(self, page: Page) -> Optional[str]:
        """从页面中提取类目URL"""
        try:
            category_link = page.locator('.breadcrumb-inner li:nth-last-child(3) a').first
            if category_link.count() > 0:
                category_url = category_link.get_attribute('href')
                if category_url:
                    if '/pd/' in category_url:
                        return None
                    return self._normalize_url(category_url)
            return None
        except Exception:
            return None
    
    def _extract_shop_url_from_page(self, page: Page, context=None) -> Optional[str]:
        """
        从页面中提取店铺商品列表URL
        
        流程：
        1. 先从商品页获取 dotted-link 链接（这是店铺介绍页）
        2. 再从介绍页获取 vendor-subtitle 中的链接，这才是店铺商品页列表
        """
        try:
            # 第一步：从商品页获取 dotted-link 链接（店铺介绍页）
            shop_intro_link = page.locator('a.dotted-link').first
            # #region agent log
            import json as _json_shop, time as _time_shop
            try:
                with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                    _f.write(_json_shop.dumps({
                        "timestamp": int(_time_shop.time() * 1000),
                        "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:start",
                        "message": "开始提取店铺URL",
                        "data": {
                            "product_url": page.url,
                            "has_dotted_link": shop_intro_link.count() > 0
                        },
                        "hypothesisId": "H_shop_url",
                        "runId": "shop-url-debug"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion

            if shop_intro_link.count() == 0:
                logger.warning("未找到店铺介绍页链接 (a.dotted-link)")
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:no_dotted_link",
                            "message": "未找到 a.dotted-link",
                            "data": {"product_url": page.url},
                            "hypothesisId": "H_shop_url_no_link",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                return None
            
            shop_intro_url = shop_intro_link.get_attribute('href')
            
            if not shop_intro_url:
                logger.warning("店铺介绍页链接为空")
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:empty_intro_href",
                            "message": "店铺介绍页链接为空",
                            "data": {"product_url": page.url},
                            "hypothesisId": "H_shop_url_empty_href",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                return None
            
            raw_intro_url = shop_intro_url
            # 规范化店铺介绍页URL
            shop_intro_url = self._normalize_url(shop_intro_url)
            if not shop_intro_url:
                logger.warning(f"无法规范化店铺介绍页URL: {raw_intro_url}")
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_normalize_failed",
                            "message": "无法规范化店铺介绍页URL",
                            "data": {"product_url": page.url, "raw_intro_url": raw_intro_url},
                            "hypothesisId": "H_shop_url_normalize",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                return None
            
            logger.debug(f"找到店铺介绍页URL: {shop_intro_url}")
            # #region agent log
            try:
                with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                    _f.write(_json_shop.dumps({
                        "timestamp": int(_time_shop.time() * 1000),
                        "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_url_ok",
                        "message": "已找到并规范化店铺介绍页URL",
                        "data": {"product_url": page.url, "shop_intro_url": shop_intro_url},
                        "hypothesisId": "H_shop_url_intro_ok",
                        "runId": "shop-url-debug"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            # 如果没有 context，无法访问介绍页，返回 None
            if not context:
                logger.warning("没有提供 context，无法访问店铺介绍页")
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:no_context",
                            "message": "缺少浏览器上下文, 无法访问店铺介绍页",
                            "data": {"product_url": page.url, "shop_intro_url": shop_intro_url},
                            "hypothesisId": "H_shop_url_no_context",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                return None
            
            # 第二步：访问店铺介绍页，获取真正的店铺商品列表页URL
            try:
                intro_page = context.new_page()
                from app.config import config
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_goto_start",
                            "message": "开始访问店铺介绍页",
                            "data": {
                                "product_url": page.url,
                                "shop_intro_url": shop_intro_url,
                                "timeout_ms": config.PLAYWRIGHT_NAVIGATION_TIMEOUT
                            },
                            "hypothesisId": "H_shop_url_intro_goto",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                # ── 内部重试：ERR_EMPTY_RESPONSE 等瞬时网络错误重试 ──
                _MAX_INTRO_GOTO = 3
                for _intro_attempt in range(_MAX_INTRO_GOTO):
                    try:
                        intro_page.goto(
                            shop_intro_url,
                            wait_until='domcontentloaded',
                            timeout=config.PLAYWRIGHT_NAVIGATION_TIMEOUT
                        )
                        break  # goto 成功，跳出重试循环
                    except PlaywrightTimeoutError:
                        raise  # 超时直接抛出，由外层 PlaywrightTimeoutError handler 处理
                    except Exception as _goto_err:
                        # #region agent log
                        try:
                            with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                                _f.write(_json_shop.dumps({
                                    "timestamp": int(_time_shop.time() * 1000),
                                    "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_goto_retry",
                                    "message": "店铺介绍页goto失败",
                                    "data": {
                                        "product_url": page.url,
                                        "shop_intro_url": shop_intro_url,
                                        "attempt": _intro_attempt + 1,
                                        "max_attempts": _MAX_INTRO_GOTO,
                                        "error": str(_goto_err)[:200],
                                        "will_retry": _intro_attempt < _MAX_INTRO_GOTO - 1
                                    },
                                    "hypothesisId": "H7_shop_intro_no_retry",
                                    "runId": "retry-fix"
                                }, ensure_ascii=False) + "\n")
                        except Exception:
                            pass
                        # #endregion
                        if _intro_attempt < _MAX_INTRO_GOTO - 1:
                            logger.warning(
                                f"[店铺介绍页] goto 失败(attempt {_intro_attempt+1})，3秒后重试: {_goto_err}"
                            )
                            try:
                                intro_page.close()
                            except Exception:
                                pass
                            _time_shop.sleep(5)
                            intro_page = context.new_page()
                        else:
                            raise  # 最后一次仍失败，抛出给外层
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_goto_ok",
                            "message": "店铺介绍页访问成功",
                            "data": {"product_url": page.url, "shop_intro_url": shop_intro_url},
                            "hypothesisId": "H_shop_url_intro_ok2",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                
                # 查找 vendor-subtitle 中的链接
                vendor_subtitle = intro_page.locator('span.vendor-subtitle').first
                
                if vendor_subtitle.count() == 0:
                    logger.error("店铺介绍页中未找到 vendor-subtitle")
                    # #region agent log
                    try:
                        with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                            _f.write(_json_shop.dumps({
                                "timestamp": int(_time_shop.time() * 1000),
                                "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:no_vendor_subtitle",
                                "message": "介绍页无 vendor-subtitle，抛出异常",
                                "data": {"product_url": page.url, "shop_intro_url": shop_intro_url},
                                "hypothesisId": "H_shop_url_vendor_subtitle",
                                "runId": "shop-url-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    intro_page.close()
                    # 如果已成功访问店铺介绍页但未找到vendor-subtitle，说明页面结构异常，应抛出异常
                    raise ValueError(f"店铺介绍页中未找到 vendor-subtitle: {shop_intro_url}")
                
                # 查找 vendor-subtitle 中的链接（包含 "/vendors/vendor/" 的链接）
                product_list_link = vendor_subtitle.locator('a[href*="/vendors/vendor/"]').first
                
                if product_list_link.count() == 0:
                    logger.error("vendor-subtitle 中未找到店铺商品列表链接")
                    # #region agent log
                    try:
                        with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                            _f.write(_json_shop.dumps({
                                "timestamp": int(_time_shop.time() * 1000),
                                "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:no_vendor_link",
                                "message": "vendor-subtitle 中未找到 /vendors/vendor/ 链接，抛出异常",
                                "data": {"product_url": page.url, "shop_intro_url": shop_intro_url},
                                "hypothesisId": "H_shop_url_vendor_link",
                                "runId": "shop-url-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    intro_page.close()
                    # 如果已找到vendor-subtitle但未找到链接，说明页面结构异常，应抛出异常
                    raise ValueError(f"vendor-subtitle 中未找到店铺商品列表链接: {shop_intro_url}")
                
                product_list_url = product_list_link.get_attribute('href')
                
                if not product_list_url:
                    logger.error("店铺商品列表链接为空")
                    # #region agent log
                    try:
                        with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                            _f.write(_json_shop.dumps({
                                "timestamp": int(_time_shop.time() * 1000),
                                "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:empty_product_list_href",
                                "message": "店铺商品列表链接为空，抛出异常",
                                "data": {"product_url": page.url, "shop_intro_url": shop_intro_url},
                                "hypothesisId": "H_shop_url_empty_product_list",
                                "runId": "shop-url-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    intro_page.close()
                    # 如果已找到链接但href为空，说明页面结构异常，应抛出异常
                    raise ValueError(f"店铺商品列表链接为空: {shop_intro_url}")
                
                raw_product_list_url = product_list_url
                # 规范化店铺商品列表URL
                product_list_url = self._normalize_url(product_list_url)
                intro_page.close()
                
                if product_list_url:
                    logger.debug(f"找到店铺商品列表URL: {product_list_url}")
                    # #region agent log
                    try:
                        with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                            _f.write(_json_shop.dumps({
                                "timestamp": int(_time_shop.time() * 1000),
                                "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:success",
                                "message": "成功获取店铺商品列表URL",
                                "data": {
                                    "product_url": page.url,
                                    "shop_intro_url": shop_intro_url,
                                    "raw_product_list_url": raw_product_list_url,
                                    "product_list_url": product_list_url
                                },
                                "hypothesisId": "H_shop_url_success",
                                "runId": "shop-url-debug"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    return product_list_url
                else:
                    logger.error(f"无法规范化店铺商品列表URL: {raw_product_list_url}")
                    # #region agent log
                    try:
                        with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                            _f.write(_json_shop.dumps({
                                "timestamp": int(_time_shop.time() * 1000),
                                "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:product_list_normalize_failed",
                                "message": "无法规范化店铺商品列表URL，抛出异常",
                                "data": {
                                    "product_url": page.url,
                                    "shop_intro_url": shop_intro_url,
                                    "raw_product_list_url": raw_product_list_url
                                },
                                "hypothesisId": "H_shop_url_product_list_normalize",
                                "runId": "shop-url-fix"
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    # 如果已找到链接但无法规范化，说明URL格式异常，应抛出异常
                    raise ValueError(f"无法规范化店铺商品列表URL: {raw_product_list_url}")
                    
            except PlaywrightTimeoutError as e:
                # 店铺介绍页访问超时：向上传递 Timeout，让上层触发窗口重启 + 重试
                logger.warning(f"访问店铺介绍页超时: {e}")
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_timeout",
                            "message": "店铺介绍页访问超时",
                            "data": {
                                "product_url": page.url,
                                "shop_intro_url": shop_intro_url,
                                "timeout_ms": config.PLAYWRIGHT_NAVIGATION_TIMEOUT,
                                "error": str(e)[:200]
                            },
                            "hypothesisId": "H_shop_url_intro_timeout",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                if 'intro_page' in locals():
                    try:
                        intro_page.close()
                    except Exception:
                        pass
                raise
            except ValueError as e:
                # 验证码异常：向上抛出，让上层处理
                if 'intro_page' in locals():
                    try:
                        intro_page.close()
                    except Exception:
                        pass
                raise
            except Exception as e:
                # 网络错误或其他异常：向上抛出，确保任务失败并触发重试
                logger.error(f"访问店铺介绍页失败: {e}")
                # #region agent log
                try:
                    with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                        _f.write(_json_shop.dumps({
                            "timestamp": int(_time_shop.time() * 1000),
                            "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_exception",
                            "message": "访问店铺介绍页异常，抛出异常",
                            "data": {
                                "product_url": page.url,
                                "shop_intro_url": shop_intro_url,
                                "error": str(e)[:200],
                                "error_type": type(e).__name__
                            },
                            "hypothesisId": "H_shop_url_intro_exception",
                            "runId": "shop-url-debug"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                if 'intro_page' in locals():
                    try:
                        intro_page.close()
                    except Exception:
                        pass
                # 抛出异常，确保任务失败并触发重试
                raise
                
        except Exception as e:
            logger.warning(f"提取店铺URL时发生错误: {e}")
            # #region agent log
            try:
                with open('d:\\emag_erp\\.cursor\\debug.log', 'a', encoding='utf-8') as _f:
                    _f.write(_json_shop.dumps({
                        "timestamp": int(_time_shop.time() * 1000),
                        "location": "dynamic_data_extractor.py:_extract_shop_url_from_page:outer_exception",
                        "message": "提取店铺URL外层异常",
                        "data": {
                            "product_url": page.url if hasattr(page, 'url') else None,
                            "error": str(e)[:200],
                            "error_type": type(e).__name__
                        },
                        "hypothesisId": "H_shop_url_outer_exception",
                        "runId": "shop-url-debug"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            return None
    
    def _parse_price(self, price_text: str) -> Optional[float]:
        """解析价格文本为浮点数"""
        if not price_text:
            return None
        
        try:
            # 移除货币符号和多余空格
            price_text = re.sub(r'[^\d,.\s]', '', price_text.strip())
            price_text = price_text.replace(' ', '')
            
            # 处理罗马尼亚数字格式（1.234,56）
            if ',' in price_text and '.' in price_text:
                # 格式：1.234,56（千位分隔符是点，小数点是逗号）
                price_text = price_text.replace('.', '').replace(',', '.')
            elif ',' in price_text:
                parts = price_text.split(',')
                if len(parts) == 2 and len(parts[0]) > 3:
                    price_text = ''.join(parts)
                else:
                    price_text = price_text.replace(',', '.')
            
            return float(price_text)
        except (ValueError, AttributeError):
            return None
    
    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """解析日期文本为datetime对象"""
        if not date_text:
            return None
        
        try:
            # 常见的日期格式
            date_formats = [
                '%Y-%m-%d',
                '%d-%m-%Y',
                '%d/%m/%Y',
                '%Y/%m/%d',
                '%d.%m.%Y',
                '%Y.%m.%d',
            ]
            
            date_text = date_text.strip()
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_text, fmt)
                except ValueError:
                    continue
            
            return None
        except Exception:
            return None
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """规范化URL"""
        if not url:
            return None
        
        try:
            if url.startswith('/'):
                return urljoin(self.base_url, url)
            elif url.startswith('http://') or url.startswith('https://'):
                if 'emag.ro' in url:
                    return url.replace('http://', 'https://', 1) if url.startswith('http://') else url
            else:
                return urljoin(self.base_url, '/' + url)
            
            return url
        except Exception:
            return None
    
    def _log_ranking_error(
        self,
        task_id: int,
        db,
        error_type: str,
        error_message: str
    ):
        """
        记录排名提取错误到任务错误日志
        
        Args:
            task_id: 任务ID
            db: 数据库会话（Session对象）
            error_type: 错误类型字符串（category_rank_timeout, store_rank_timeout等）
            error_message: 错误消息
        """
        if not task_id or not db:
            return
        
        try:
            ErrorLog, ErrorType, Session = _get_error_log_imports()
            if not ErrorLog or not ErrorType:
                logger.warning("无法导入ErrorLog模块，跳过错误记录")
                return
            
            # 映射错误类型
            error_type_map = {
                "no_context": ErrorType.OTHER,
                "category_rank_timeout": ErrorType.TIMEOUT,
                "category_rank_error": ErrorType.OTHER,
                "store_rank_timeout": ErrorType.TIMEOUT,
                "store_rank_error": ErrorType.OTHER,
                "ranking_timeout": ErrorType.TIMEOUT,
                "ranking_error": ErrorType.OTHER,
            }
            
            mapped_error_type = error_type_map.get(error_type, ErrorType.OTHER)
            
            error_log = ErrorLog(
                task_id=task_id,
                error_type=mapped_error_type,
                error_message=error_message,
                error_detail={
                    "ranking_error_type": error_type,
                    "timestamp": datetime.utcnow().isoformat()
                },
                occurred_at=datetime.utcnow()
            )
            
            db.add(error_log)
            db.commit()
            
            logger.debug(f"已记录排名提取错误 - 任务ID: {task_id}, 错误类型: {error_type}")
        except Exception as e:
            logger.error(f"记录排名提取错误失败 - 任务ID: {task_id}, 错误: {e}")
            try:
                db.rollback()
            except Exception:
                pass


"""产品链接爬取器

从搜索结果页面提取产品链接
"""
import logging
import time
from typing import List, Dict, Any, Optional
from urllib.parse import quote
from sqlalchemy.orm import Session
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from app.config import config
from app.utils.proxy import proxy_manager
from app.utils.captcha_handler import captcha_handler
from app.utils.playwright_manager import get_playwright_pool
from app.services.extractors import LinkExtractor
from app.database import ErrorType

logger = logging.getLogger(__name__)

class ProductLinkCrawler:
    """从搜索结果页爬取产品链接的爬取器"""
    
    BASE_URL = "https://www.emag.ro"
    
    def __init__(self):
        """初始化产品链接爬取器"""
        # 延迟初始化playwright_pool，避免在导入时就初始化
        self.base_url = self.BASE_URL
        self.extractor = LinkExtractor(self.base_url)
        self._playwright_pool = None  # 延迟初始化
    
    @property
    def playwright_pool(self):
        """延迟获取Playwright上下文池"""
        if self._playwright_pool is None:
            try:
                
                self._playwright_pool = get_playwright_pool()
                
            except Exception as e:
                raise
        return self._playwright_pool
    
    def crawl_search_results(
        self,
        keyword: str,
        max_pages: int,
        task_id: Optional[int] = None,
        db: Optional[Session] = None
    ) -> List[Dict[str, Any]]:
        """
        爬取搜索结果（不包含重试逻辑，由调用方使用retry_manager包装）
        
        Args:
            keyword: 搜索关键字
            max_pages: 最大爬取页数
            task_id: 任务ID（用于更新进度）
            db: 数据库会话（用于更新任务状态）
            
        Returns:
            产品信息列表，每个包含 url, pnk_code, thumbnail_image, price
            
        Raises:
            Exception: 爬取失败时抛出异常，由retry_manager处理重试
        """
        start_time = time.time()
        logger.info(f"[爬取开始] 关键字搜索爬取 - 关键字: {keyword}, 最大页数: {max_pages}, 任务ID: {task_id}")
        
        all_products: List[Dict[str, Any]] = []
        processed_urls: set = set()
        
        # 编码关键字用于URL
        encoded_keyword = quote(keyword)
        
        context = None
        try:
            # 获取代理
            # 直接使用 proxy_manager 返回的完整代理 URL（支持 socks 协议），无需去掉前缀
            proxy_dict = proxy_manager.get_random_proxy()
            proxy_str = None
            if proxy_dict:
                # 从代理字典中提取代理字符串
                proxy_url = proxy_dict.get('http', '') or proxy_dict.get('https', '')
                if proxy_url:
                    proxy_str = proxy_url
            
            
            # 获取浏览器上下文
            context = self.playwright_pool.acquire_context(proxy=proxy_str)
            logger.info(f"[爬取进行中] 已获取浏览器上下文 - 关键字: {keyword}, 代理: {proxy_str if proxy_str else '无'}")
            
            # 爬取每一页
            for page_num in range(1, max_pages + 1):
                page_start_time = time.time()
                logger.info(f"[爬取页面] 开始爬取第 {page_num}/{max_pages} 页 - 关键字: {keyword}")
                
                
                try:
                    page_products = self._crawl_single_page(
                        keyword,
                        page_num,
                        encoded_keyword,
                        context,
                        proxy_str,  # 传递代理字符串，用于标记失败代理
                        task_id,
                        db
                    )
                    
                    # 去重并添加到结果列表
                    page_added = 0
                    page_duplicates = 0
                    for product in page_products:
                        url = product.get('url')
                        if url and url not in processed_urls:
                            processed_urls.add(url)
                            all_products.append(product)
                            page_added += 1
                        else:
                            page_duplicates += 1
                    
                    
                    page_elapsed = time.time() - page_start_time
                    logger.info(f"[爬取页面完成] 第 {page_num}/{max_pages} 页爬取完成 - 关键字: {keyword}, 本页产品数: {len(page_products)}, 累计产品数: {len(all_products)}, 耗时: {page_elapsed:.2f}秒")
                    
                    # 更新任务进度
                    if task_id and db:
                        self._update_progress(task_id, page_num, max_pages, db)
                    
                except Exception as e:
                    page_elapsed = time.time() - page_start_time
                    error_msg = str(e)
                    
                    # 如果是验证码异常，重新抛出以触发整个任务的重试（使用新代理）
                    if isinstance(e, ValueError) and "Captcha detected" in error_msg:
                        logger.warning(f"[验证码异常] 第 {page_num}/{max_pages} 页检测到验证码，将触发整个任务重试 - 关键字: {keyword}, 耗时: {page_elapsed:.2f}秒")
                        raise  # 重新抛出异常，让 retry_manager 处理重试
                    
                    # 其他异常：记录日志并继续爬取下一页
                    logger.warning(f"[爬取页面失败] 第 {page_num}/{max_pages} 页爬取失败 - 关键字: {keyword}, 错误: {error_msg}, 耗时: {page_elapsed:.2f}秒")
                    
                    
                    # 继续爬取下一页，不中断整个任务
                    continue
            
            total_elapsed = time.time() - start_time
            logger.info(f"[爬取完成] 关键字搜索爬取完成 - 关键字: {keyword}, 总页数: {max_pages}, 总产品数: {len(all_products)}, 总耗时: {total_elapsed:.2f}秒")
            return all_products
            
        except PlaywrightTimeoutError as e:
            # 连接超时：标记代理失败
            if proxy_str:
                proxy_manager.mark_proxy_failed(proxy_str)
                logger.warning(f"[代理标记失败] 连接超时，标记代理为失败 - 代理: {proxy_str}")
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 关键字搜索爬取超时 - 关键字: {keyword}, 已爬取产品数: {len(all_products)}, 错误: {str(e)}, 总耗时: {total_elapsed:.2f}秒")
            raise
        except PlaywrightError as e:
            # 浏览器错误/主机断开：标记代理失败
            if proxy_str:
                proxy_manager.mark_proxy_failed(proxy_str)
                logger.warning(f"[代理标记失败] 浏览器错误，标记代理为失败 - 代理: {proxy_str}")
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 关键字搜索爬取浏览器错误 - 关键字: {keyword}, 已爬取产品数: {len(all_products)}, 错误: {str(e)}, 总耗时: {total_elapsed:.2f}秒")
            raise
        except Exception as e:
            total_elapsed = time.time() - start_time
            logger.error(f"[爬取失败] 关键字搜索爬取错误 - 关键字: {keyword}, 已爬取产品数: {len(all_products)}, 错误: {str(e)}, 错误类型: {type(e).__name__}, 总耗时: {total_elapsed:.2f}秒", exc_info=True)
            raise
        finally:
            # 释放上下文
            if context:
                self.playwright_pool.release_context(context)
    
    def _crawl_single_page(
        self,
        keyword: str,
        page: int,
        encoded_keyword: str,
        context,
        proxy_str: Optional[str] = None,
        task_id: Optional[int] = None,
        db: Optional[Session] = None
    ) -> List[Dict[str, Any]]:
        """
        爬取单个搜索页面
        
        Args:
            keyword: 搜索关键字
            page: 页码（从1开始）
            encoded_keyword: URL编码后的关键字
            context: BrowserContext对象
            proxy_str: 代理字符串（用于标记失败代理）
            task_id: 任务ID
            db: 数据库会话
            
        Returns:
            产品信息列表
            
        Raises:
            Exception: 爬取失败时抛出异常
        """
        # 构建搜索URL
        if page == 1:
            search_url = f"{self.base_url}/search/{encoded_keyword}"
        else:
            search_url = f"{self.base_url}/search/{encoded_keyword}/p{page}"
        
        logger.debug(f"[页面爬取] 开始加载页面 - URL: {search_url}, 关键字: {keyword}, 页码: {page}")
        
        
        page_obj = None
        try:
            # 创建新页面
            page_obj = context.new_page()
            
            # 加载页面
            load_start = time.time()
            # 使用 domcontentloaded 而非 networkidle，避免等待广告/分析脚本导致超时
            page_obj.goto(search_url, wait_until='domcontentloaded', timeout=config.PLAYWRIGHT_NAVIGATION_TIMEOUT)
            load_elapsed = time.time() - load_start
            
            # 获取实际 URL（检查是否被重定向）
            actual_url = page_obj.url
            logger.debug(f"[页面加载] 页面加载完成 - 请求URL: {search_url}, 实际URL: {actual_url}, 加载耗时: {load_elapsed:.2f}秒")
            
            
            # 检查验证码
            page_content = page_obj.content()
            
            if captcha_handler.detect_captcha(page_content, page_content):
                logger.warning(f"[验证码检测] 检测到验证码 - 关键字: {keyword}, 页码: {page}, URL: {search_url}, 代理: {proxy_str if proxy_str else '无'}")
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
                raise ValueError(f"Captcha detected for keyword '{keyword}', page {page}")
            
            # 使用提取器提取产品链接
            extract_start = time.time()
            products = self.extractor.extract_from_search_page(page_obj)
            extract_elapsed = time.time() - extract_start
            logger.info(f"[链接提取] 提取完成 - 关键字: {keyword}, 页码: {page}, 提取产品数: {len(products)}, 提取耗时: {extract_elapsed:.2f}秒")
            
            return products
            
        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout while crawling page {page} for keyword '{keyword}': {e}")
            raise
        except Exception as e:
            logger.error(f"Error crawling page {page} for keyword '{keyword}': {e}")
            raise
        finally:
            # 关闭页面
            if page_obj:
                try:
                    page_obj.close()
                except Exception:
                    pass
    
    def _update_progress(self, task_id: int, current_page: int, max_pages: int, db: Session):
        """
        更新任务进度
        
        Args:
            task_id: 任务ID
            current_page: 当前页码
            max_pages: 总页数
            db: 数据库会话
        """
        try:
            from app.database import CrawlTask
            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
            if task:
                # 计算进度：5% - 85% 用于爬取
                progress = 5 + int((current_page / max_pages) * 80)
                task.progress = progress
                db.commit()
        except Exception as e:
            logger.warning(f"Failed to update task progress: {e}")
            try:
                db.rollback()
            except Exception:
                pass


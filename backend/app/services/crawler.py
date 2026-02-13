"""Crawler service for crawling product data with multi-threading, batch operations, and error handling"""
import logging
import time
import random
import re
import json
import requests
from typing import List, Optional, Dict, Any, Set
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from datetime import datetime
from urllib.parse import urljoin, urlencode, quote
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from app.config import config
from app.utils.proxy import proxy_manager
from app.utils.captcha_handler import captcha_handler
from app.utils.thread_pool import thread_pool_manager
from app.services.retry_manager import retry_manager
from app.services.crawlers import ProductLinkCrawler, ProductDataCrawler
from app.services.istoric_preturi_client import get_listed_at as get_istoric_listed_at
from app.database import CrawlTask, TaskStatus, ErrorType, ErrorLog, TaskType, TaskPriority, SessionLocal
from app.models.keyword import Keyword, KeywordLink, KeywordStatus
from app.models.product import FilterPool

logger = logging.getLogger(__name__)

# User-Agent pool for rotation (增强的浏览器指纹池)
USER_AGENTS = [
    # Chrome on Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    # Chrome on macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    # Firefox on Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    # Safari on macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    # Chrome on Linux
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# Accept-Language 池（模拟不同地区的浏览器）
ACCEPT_LANGUAGES = [
    'en-US,en;q=0.9,ro;q=0.8',
    'ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7',
    'en-GB,en;q=0.9,ro;q=0.8',
    'en-US,en;q=0.9',
    'ro,en-US;q=0.9,en;q=0.8',
]

# 屏幕分辨率池（用于生成真实的浏览器指纹）
SCREEN_RESOLUTIONS = [
    (1920, 1080),
    (1366, 768),
    (1536, 864),
    (1440, 900),
    (1600, 900),
    (1280, 720),
    (2560, 1440),
]

def generate_browser_fingerprint() -> Dict[str, Any]:
    """
    生成浏览器指纹信息（用于更真实的模拟）
    
    Returns:
        包含浏览器指纹信息的字典
    """
    resolution = random.choice(SCREEN_RESOLUTIONS)
    return {
        'user_agent': random.choice(USER_AGENTS),
        'accept_language': random.choice(ACCEPT_LANGUAGES),
        'screen_width': resolution[0],
        'screen_height': resolution[1],
        'timezone_offset': random.randint(-12, 12),
    }

def get_random_headers(referer: Optional[str] = None, base_url: Optional[str] = None) -> Dict[str, str]:
    """
    获取随机HTTP头以模拟真实浏览器（增强版）
    
    Args:
        referer: Referer URL
        base_url: 基础URL（用于设置Referer）
        
    Returns:
        包含HTTP头的字典
    """
    fingerprint = generate_browser_fingerprint()
    user_agent = fingerprint['user_agent']
    
    # 检测浏览器类型以设置对应的HTTP头
    is_chrome = 'Chrome' in user_agent and 'Safari' in user_agent
    is_firefox = 'Firefox' in user_agent
    is_safari = 'Safari' in user_agent and 'Chrome' not in user_agent
    
    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': fingerprint['accept_language'],
        'Accept-Encoding': 'gzip, deflate',  # 移除 br (Brotli) 支持，因为 requests 需要额外库
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none' if not referer else ('same-origin' if base_url and base_url in referer else 'cross-site'),
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'DNT': '1',  # Do Not Track
    }
    
    # Chrome特定头
    if is_chrome:
        headers['sec-ch-ua'] = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
        headers['sec-ch-ua-mobile'] = '?0'
        headers['sec-ch-ua-platform'] = '"Windows"' if 'Windows' in user_agent else '"macOS"' if 'Macintosh' in user_agent else '"Linux"'
    
    # Firefox特定头
    if is_firefox:
        headers['TE'] = 'trailers'
    
    # 设置Referer
    if referer:
        headers['Referer'] = referer
    elif base_url:
        # 如果没有指定referer但有base_url，使用base_url作为referer
        headers['Referer'] = base_url
    
    return headers

def _crawl_single_search_page(
    keyword: str,
    page: int,
    encoded_keyword: str,
    base_url: str,
    task_id: Optional[int] = None,
    db: Optional[Session] = None
) -> tuple[int, List[Dict[str, Any]], Optional[Exception]]:
    """
    爬取单个搜索页面（用于多线程）
    
    Args:
        keyword: 搜索关键字
        page: 页码（从1开始）
        encoded_keyword: URL编码后的关键字
        base_url: 基础URL
        task_id: 任务ID
        db: 数据库会话
        
    Returns:
        (页码, 产品URL列表, 异常对象或None)
    """
    try:
        # 构建搜索URL（优化emag.ro搜索URL格式）
        if page == 1:
            # 第一页通常没有分页参数
            search_url = f"{base_url}/search/{encoded_keyword}"
        else:
            # emag.ro的分页格式：/search/keyword?p=2
            search_url = f"{base_url}/search/{encoded_keyword}?p={page}"
        
        logger.info(f"Crawling search page {page} for keyword '{keyword}': {search_url}")
        
        # 获取独立代理（每个线程使用不同的代理）
        proxies = proxy_manager.get_random_proxy()
        
        # 随机延迟（避免请求过于频繁）
        delay = random.uniform(config.CRAWLER_DELAY_MIN, config.CRAWLER_DELAY_MAX)
        time.sleep(delay)
        
        # 生成增强的HTTP头（模拟真实浏览器）
        headers = get_random_headers(base_url=base_url)
        
        # 发送请求
        
        response = requests.get(
            search_url,
            headers=headers,
            proxies=proxies,
            timeout=config.CRAWLER_TIMEOUT,
            stream=False,
            allow_redirects=True
        )
        
        
        # 检查状态码511（Network Authentication Required - 通常是验证码）
        if response.status_code == 511:
            logger.warning(f"Status code 511 (Network Authentication Required) for page {page} of keyword '{keyword}' - likely captcha")
            if task_id and db:
                captcha_handler.handle_captcha(
                    task_id,
                    html_content=response.text[:1000] if response.text else "",
                    response_text=response.text[:1000] if response.text else "",
                    db=db
                )
            return (page, [], None)
        
        response.raise_for_status()
        
        # 确保响应文本正确解码（requests通常会自动处理，但有时需要手动设置编码）
        if not response.encoding or response.encoding == 'ISO-8859-1':
            response.encoding = response.apparent_encoding or 'utf-8'
        
        # 检查HTML title中是否有"eMAG Captcha"（在解压和解码后检查）
        if response.text:
            # 检查title标签
            title_match = False
            try:
                if '<title>' in response.text and '</title>' in response.text:
                    title_start = response.text.find('<title>')
                    title_end = response.text.find('</title>', title_start)
                    if title_start >= 0 and title_end > title_start:
                        title_text = response.text[title_start + 7:title_end].lower()
                        if 'captcha' in title_text or 'emag captcha' in title_text:
                            title_match = True
                            logger.warning(f"Captcha detected in title: '{response.text[title_start + 7:title_end]}' for page {page} of keyword '{keyword}'")
            except Exception as e:
                logger.debug(f"Error checking title: {e}")
            
            if title_match:
                if task_id and db:
                    captcha_handler.handle_captcha(
                        task_id,
                        html_content=response.text[:1000],
                        response_text=response.text[:1000],
                        db=db
                    )
                return (page, [], None)
        
        # 检查验证码（使用验证码检测器）
        captcha_detected = captcha_handler.detect_captcha(response.text, response.text)
        
        
        if captcha_detected:
            logger.warning(f"Captcha detected on search page {page} for keyword '{keyword}'")
            if task_id and db:
                captcha_handler.handle_captcha(
                    task_id,
                    html_content=response.text[:1000],
                    response_text=response.text[:1000],
                    db=db
                )
            # 验证码检测时不抛出异常，返回空列表
            return (page, [], None)
        
        # 解析HTML
        
        try:
            soup = BeautifulSoup(response.text, 'lxml')
        except Exception as parse_error:
            # 如果解析失败，尝试使用html.parser
            logger.warning(f"Failed to parse with lxml, trying html.parser: {parse_error}")
            soup = BeautifulSoup(response.text, 'html.parser')
        
        
        # 提取产品链接
        page_urls = _extract_product_links_from_search(soup, base_url)
        
        
        if page_urls:
            logger.info(f"Found {len(page_urls)} products on page {page} for keyword '{keyword}'")
        else:
            logger.info(f"No products found on page {page} for keyword '{keyword}'")
        
        return (page, page_urls, None)
        
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout while crawling page {page} for keyword '{keyword}': {e}")
        if proxies:
            # 标记代理失败
            proxy_str = str(proxies.get('http', ''))
            proxy_manager.mark_proxy_failed(proxy_str)
        if task_id and db:
            retry_manager.log_error(task_id, e, ErrorType.TIMEOUT, db=db)
        return (page, [], e)
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while crawling page {page} for keyword '{keyword}': {e}")
        if proxies:
            # 标记代理失败
            proxy_str = str(proxies.get('http', ''))
            proxy_manager.mark_proxy_failed(proxy_str)
        if task_id and db:
            error_type = retry_manager.classify_error(e)
            retry_manager.log_error(task_id, e, error_type, db=db)
        return (page, [], e)
    except Exception as e:
        logger.error(f"Error crawling page {page} for keyword '{keyword}': {e}", exc_info=True)
        if task_id and db:
            retry_manager.log_error(task_id, e, ErrorType.OTHER, db=db)
        return (page, [], e)

def crawl_keyword_search(
    keyword: str,
    max_pages: int = None,
    task_id: Optional[int] = None,
    db: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """
    爬取关键字搜索结果（多线程版本）
    
    Args:
        keyword: 搜索关键字
        max_pages: 最大爬取页数（默认使用配置值）
        task_id: 任务ID
        db: 数据库会话
        
    Returns:
        产品信息列表（已去重），每个包含 url, pnk_code, thumbnail_image, price
        
    Note:
        - 检测到验证码时返回空列表，不抛出异常
        - 网络错误时抛出异常
        - 部分页面失败不影响其他页面
    """
    # 使用配置的默认页数
    if max_pages is None:
        max_pages = config.KEYWORD_SEARCH_MAX_PAGES
    
    all_products: List[Dict[str, Any]] = []
    processed_urls: Set[str] = set()  # 用于去重
    base_url = "https://www.emag.ro"
    
    # 编码关键字用于URL
    encoded_keyword = quote(keyword)
    
    # 更新任务进度
    if task_id and db:
        try:
            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
            if task:
                task.progress = 5
                db.commit()
        except Exception as e:
            logger.warning(f"Failed to update task progress: {e}")
    
    executor = None
    try:
        # 使用ThreadPoolExecutor实现多线程爬取
        # 每页使用一个独立线程
        executor = ThreadPoolExecutor(max_workers=max_pages)
        # 提交所有页面的爬取任务
        futures = {
            executor.submit(
                _crawl_single_search_page,
                keyword,
                page,
                encoded_keyword,
                base_url,
                task_id,
                db
            ): page
            for page in range(1, max_pages + 1)
        }
        
        # 收集结果（线程安全）
        page_results: Dict[int, List[Dict[str, Any]]] = {}
        page_errors: Dict[int, Optional[Exception]] = {}
        
        try:
            for future in as_completed(futures):
                page = futures[future]
                try:
                    result_page, page_products, error = future.result()
                    page_results[result_page] = page_products
                    page_errors[result_page] = error
                    
                    # 更新进度
                    if task_id and db:
                        try:
                            task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
                            if task:
                                progress = int((len(page_results) / max_pages) * 85)  # 85%用于爬取，15%用于处理
                                task.progress = progress
                                db.commit()
                        except Exception as db_error:
                            logger.warning(f"Failed to update task progress: {db_error}")
                            try:
                                db.rollback()
                            except Exception:
                                pass
                    
                except Exception as e:
                    logger.error(f"Unexpected error in future for page {page}: {e}")
                    page_results[page] = []
                    page_errors[page] = e
        except Exception as e:
            logger.warning(f"Error waiting for futures: {e}")
            # 尝试取消未完成的任务
            for future in futures:
                if not future.done():
                    future.cancel()
        finally:
            # 确保executor正确关闭
            if executor:
                executor.shutdown(wait=False)  # 不等待，避免阻塞
        
        # 合并结果（按页码排序），去重
        for page in sorted(page_results.keys()):
            page_products = page_results[page]
            error = page_errors.get(page)
            
            if page_products:
                for product in page_products:
                    url = product.get('url')
                    if url and url not in processed_urls:
                        processed_urls.add(url)
                        all_products.append(product)
            elif error:
                logger.warning(f"Page {page} failed with error: {error}")
        
        logger.info(
            f"Total {len(all_products)} unique products found for keyword '{keyword}' "
            f"across {len([p for p in page_results.values() if p])} successful pages"
        )
        
        # 返回去重后的产品列表（按URL排序）
        return sorted(all_products, key=lambda x: x['url'])
        
    except Exception as e:
        logger.error(f"Error in crawl_keyword_search for keyword '{keyword}': {e}", exc_info=True)
        if task_id and db:
            retry_manager.log_error(task_id, e, ErrorType.OTHER, db=db)
        raise

def _validate_product_url(url: str, base_url: str) -> Optional[str]:
    """
    验证并规范化产品URL
    
    Args:
        url: 原始URL
        base_url: 基础URL
        
    Returns:
        规范化后的URL或None（如果无效）
    """
    if not url or not isinstance(url, str):
        return None
    
    # 必须包含 /pd/ 路径（emag产品页面的标识）
    if '/pd/' not in url:
        return None
    
    # 必须包含 emag.ro 域名
    if 'emag.ro' not in url and not url.startswith('/'):
        return None
    
    # 解析和规范化URL
    try:
        if url.startswith('/'):
            full_url = urljoin(base_url, url)
        elif url.startswith('http://') or url.startswith('https://'):
            # 确保是emag.ro的URL
            if 'emag.ro' not in url:
                return None
            full_url = url
        else:
            full_url = urljoin(base_url, '/' + url)
        
        # 确保使用https协议
        if full_url.startswith('http://www.emag.ro'):
            full_url = full_url.replace('http://', 'https://', 1)
        elif full_url.startswith('http://emag.ro'):
            full_url = full_url.replace('http://', 'https://', 1)
        
        # 移除查询参数和锚点
        full_url = full_url.split('?')[0].split('#')[0]
        
        # 确保URL格式正确
        if not full_url.startswith('https://www.emag.ro') and not full_url.startswith('https://emag.ro'):
            return None
        
        # 确保URL以/结尾或者格式正确
        if '/pd/' not in full_url:
            return None
        
        return full_url
    except Exception as e:
        logger.debug(f"URL validation failed for {url}: {e}")
        return None

def _extract_pnk_code_from_url(url: str) -> Optional[str]:
    """
    从产品URL中提取PNK_CODE
    """
    if not url or not isinstance(url, str):
        return None

    match = re.search(r"/pd/([^/?#]+)", url)
    if not match:
        return None

    return match.group(1)

def _extract_product_links_from_search(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
    """
    从搜索结果页面提取产品链接、缩略图和价格
    
    Args:
        soup: BeautifulSoup解析的HTML
        base_url: 用于解析相对链接的基础URL
        
    Returns:
        产品信息列表，每个包含 url, pnk_code, thumbnail_image, price, review_count, rating
    """
    
    
    products: List[Dict[str, Any]] = []
    processed_urls: Set[str] = set()  # 使用Set自动去重
    
    try:
        
        # Emag搜索结果的常见选择器（按优先级排序）
        # 优化后的选择器，更准确地匹配产品链接
        selectors = [
            # 优先选择器：emag常见的产品卡片结构
            'a[href*="/pd/"]',
            '.card-item a[href*="/pd/"]',
            '.product-item a[href*="/pd/"]',
            '.card-body a[href*="/pd/"]',
            'a.card-v2-title[href*="/pd/"]',
            'a.card-v2-wrapper[href*="/pd/"]',
            # 备用选择器
            'a[data-product-id][href*="/pd/"]',
            '.product-title a[href*="/pd/"]',
            '.product-box a[href*="/pd/"]',
        ]
        
        price_selectors = [
            '.product-new-price',
            '.product-old-price',
            '.price',
            '[data-price]',
            '.product-price',
            'p.product-new-price',
            'span.product-new-price',
        ]
        
        # 评论数选择器（优先匹配 emag 实际结构）
        review_count_selectors = [
            '.star-rating-text',  # emag 实际结构：包含评分和评论数的容器
            '.star-rating-text .hidden-xs',  # 包含 "398 de review-uri" 的元素
            '.star-rating-text .visible-xs-inline-block',  # 移动端显示 "(398)"
            '.reviews-count',
            '.rating-count',
            '[data-reviews]',
            '.product-reviews-count',
            '.reviews-summary .count',
            'span[itemprop="reviewCount"]',
            'a[href*="/reviews"]',
            'a[href*="#reviews"]',
        ]
        
        # 评分选择器（优先匹配 emag 实际结构）
        rating_selectors = [
            '.star-rating-text .average-rating',  # emag 实际结构：评分元素
            '.average-rating',  # 备用：直接查找评分元素
            '.rating-value',
            '[itemprop="ratingValue"]',
            '.star-rating',
            '[data-rating]',
            '.product-rating',
            '.reviews-general-rating',
        ]
        
        
        links_found = False
        selector_results = {}
        debug_samples = 0
        for selector in selectors:
            try:
                links = soup.select(selector)
                selector_results[selector] = len(links)
                if links:
                    links_found = True
                    raw_hrefs = []
                    valid_count = 0
                    for link in links:
                        href = link.get('href', '')
                        if not href:
                            continue
                        raw_hrefs.append(href[:100])  # 只记录前100字符
                        
                        # 验证并规范化URL
                        valid_url = _validate_product_url(href, base_url)
                        if not valid_url or valid_url in processed_urls:
                            continue
                        
                        processed_urls.add(valid_url)
                        
                        
                        
                        # 提取缩略图
                        thumbnail_image = None
                        # 查找产品卡片中的图片
                        card = link.find_parent(class_=lambda x: x and ('card' in x.lower() or 'product' in x.lower()))
                        
                        
                        if not card:
                            # 尝试查找父元素中的图片
                            parent = link.parent
                            if parent:
                                img = parent.find('img', src=True) or parent.find('img', {'data-src': True})
                                if img:
                                    img_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                                    if img_url:
                                        if img_url.startswith('http'):
                                            thumbnail_image = img_url
                                        elif img_url.startswith('//'):
                                            thumbnail_image = 'https:' + img_url
                                        elif img_url.startswith('/'):
                                            thumbnail_image = base_url + img_url
                        else:
                            img = card.find('img', src=True) or card.find('img', {'data-src': True}) or card.find('img', {'data-lazy-src': True})
                            if img:
                                img_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                                if img_url:
                                    if img_url.startswith('http'):
                                        thumbnail_image = img_url
                                    elif img_url.startswith('//'):
                                        thumbnail_image = 'https:' + img_url
                                    elif img_url.startswith('/'):
                                        thumbnail_image = base_url + img_url
                        
                        
                        # 提取价格
                        price = None
                        price_selector_used = None
                        price_text_raw = None
                        price_text_clean = None
                        price_match_text = None
                        price_parse_error = None
                        if card:
                            # 查找价格元素
                            for price_sel in price_selectors:
                                price_elem = card.select_one(price_sel)
                                if price_elem:
                                    price_selector_used = price_sel
                                    price_text = price_elem.get_text(strip=True)
                                    price_text_raw = price_text
                                    # 移除货币符号，提取数字
                                    price_text_clean = price_text.replace('Lei', '').replace('RON', '').replace('lei', '').replace('€', '').replace('EUR', '').strip()
                                    # 提取数字（处理逗号和点作为小数分隔符）
                                    price_match = re.search(r'[\d\s.,]+', price_text_clean)
                                    if price_match:
                                        price_match_text = price_match.group()
                                        price_str = price_match.group().replace(' ', '').replace(',', '.')
                                        # 处理千位分隔符
                                        if price_str.count('.') > 1:
                                            price_str = price_str.replace('.', '', price_str.count('.') - 1)
                                        try:
                                            price = float(price_str)
                                            break
                                        except ValueError as parse_error:
                                            price_parse_error = str(parse_error)
                                            continue
                        
                        # 提取评论数和评分（优先从 .star-rating-text 容器中提取）
                        review_count = None
                        rating = None
                        
                        
                        
                        # 查找 .star-rating-text 容器（不限于 card 内部，也在 link 的父元素中查找）
                        star_rating_container = None
                        if card:
                            star_rating_container = card.select_one('.star-rating-text')
                        # 如果 card 中没有找到，尝试在 link 的父元素中查找
                        if not star_rating_container:
                            parent = link.parent
                            if parent:
                                star_rating_container = parent.select_one('.star-rating-text')
                                # 如果直接父元素中没有，向上查找
                                if not star_rating_container:
                                    for ancestor in link.parents:
                                        star_rating_container = ancestor.select_one('.star-rating-text')
                                        if star_rating_container:
                                            break
                        
                        
                        
                        if star_rating_container:
                            # 提取评分：从 .average-rating 中提取
                            rating_elem = star_rating_container.select_one('.average-rating')
                            
                            if rating_elem:
                                rating_text = rating_elem.get_text(strip=True)
                                if rating_text:
                                    # 提取数字，处理 "4.66" 格式
                                    rating_match = re.search(r'(\d+[.,]?\d*)', rating_text.replace(',', '.'))
                                    if rating_match:
                                        try:
                                            rating = float(rating_match.group(1))
                                            # 确保评分在合理范围内（0-5）
                                            if rating > 5:
                                                rating = rating / 10  # 可能是10分制
                                            
                                        except ValueError:
                                            pass
                                
                            # 提取评论数：从包含 "de review-uri" 的文本中提取，或从括号中提取
                            # 方法1：优先查找包含 "de review-uri" 文本的 span（桌面端）
                            review_elem = None
                            for elem in star_rating_container.find_all('span'):
                                text = elem.get_text(strip=True)
                                # 查找包含 "de review-uri" 的元素
                                if 'de review-uri' in text.lower():
                                    review_elem = elem
                                    break
                                # 或者查找括号格式 "(398)"
                                elif text.startswith('(') and text.endswith(')') and text[1:-1].isdigit():
                                    review_elem = elem
                                    break
                            
                            # 方法2：如果没找到特定元素，从整个容器中提取
                            if review_elem:
                                review_text = review_elem.get_text(strip=True)
                            else:
                                # 从整个容器中提取文本，但排除评分部分
                                container_text = star_rating_container.get_text(strip=True)
                                # 如果已经提取到评分，从容器文本中移除评分部分
                                if rating is not None:
                                    # 移除评分数字（可能是 "4.66" 格式）
                                    rating_str = str(rating)
                                    if rating_str in container_text:
                                        review_text = container_text.replace(rating_str, '', 1).strip()
                                    else:
                                        review_text = container_text
                                else:
                                    review_text = container_text
                            
                            if review_text:
                                
                                # 匹配 "398 de review-uri" 或 "(398)" 格式
                                # 优先匹配 "数字 de review-uri" 格式（不区分大小写）
                                review_match = re.search(r'(\d+)\s*de\s*review-uri', review_text, re.IGNORECASE)
                                if not review_match:
                                    # 备用：匹配括号中的数字 "(398)"
                                    review_match = re.search(r'\((\d+)\)', review_text)
                                if not review_match:
                                    # 最后：直接提取第一个数字（但排除评分）
                                    # 先移除评分部分（通常是第一个数字，可能是小数）
                                    text_without_rating = review_text
                                    if rating is not None:
                                        # 移除评分数字
                                        text_without_rating = re.sub(r'^\s*\d+[.,]?\d*\s*', '', text_without_rating)
                                    review_match = re.search(r'(\d+)', text_without_rating.replace(',', '').replace('.', ''))
                                
                                if review_match:
                                    try:
                                        review_count = int(review_match.group(1))
                                        
                                    except ValueError:
                                        pass
                            
                            # 如果从容器中未提取到，使用原来的选择器方法（不依赖 card）
                            if review_count is None:
                                search_context = card if card else link.parent if link.parent else None
                                if search_context:
                                    for review_sel in review_count_selectors:
                                        # 跳过已经尝试过的 .star-rating-text
                                        if review_sel.startswith('.star-rating-text'):
                                            continue
                                        review_elem = search_context.select_one(review_sel)
                                        if review_elem:
                                            review_text = review_elem.get_text(strip=True)
                                            if review_text:
                                                # 提取数字，处理 "123 reviews" 或 "123 recenzii" 等格式
                                                review_match = re.search(r'(\d+)', review_text.replace(',', '').replace('.', ''))
                                                if review_match:
                                                    try:
                                                        review_count = int(review_match.group(1))
                                                        break
                                                    except ValueError:
                                                        continue
                            
                            # 如果评分未提取到，使用原来的选择器方法（不依赖 card）
                            if rating is None:
                                search_context = card if card else link.parent if link.parent else None
                                if search_context:
                                    for rating_sel in rating_selectors:
                                        # 跳过已经尝试过的选择器
                                        if rating_sel.startswith('.star-rating-text') or rating_sel == '.average-rating':
                                            continue
                                        rating_elem = search_context.select_one(rating_sel)
                                        if rating_elem:
                                            rating_text = rating_elem.get_text(strip=True)
                                            if rating_text:
                                                # 提取数字，处理 "4.5" 或 "4,5" 等格式
                                                rating_match = re.search(r'(\d+[.,]?\d*)', rating_text.replace(',', '.'))
                                                if rating_match:
                                                    try:
                                                        rating = float(rating_match.group(1))
                                                        # 确保评分在合理范围内（0-5）
                                                        if rating > 5:
                                                            rating = rating / 10  # 可能是10分制
                                                        break
                                                    except ValueError:
                                                        continue
                        
                        
                        products.append({
                            'url': valid_url,
                            'pnk_code': _extract_pnk_code_from_url(valid_url),
                            'thumbnail_image': thumbnail_image,
                            'price': price,
                            'review_count': review_count,
                            'rating': rating
                        })
                        valid_count += 1
                    
                    
                    # 如果找到链接，继续使用其他选择器收集更多链接
                    # 不立即break，因为不同选择器可能找到不同的链接
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        
        # 如果没有找到链接，尝试使用正则表达式搜索所有链接
        if not products:
            all_links = soup.find_all('a', href=True)
            pd_links_count = 0
            pd_links_valid = 0
            for link in all_links:
                href = link.get('href', '')
                if not href:
                    continue
                
                # 必须包含/pd/路径
                if '/pd/' in href:
                    pd_links_count += 1
                    valid_url = _validate_product_url(href, base_url)
                    if valid_url and valid_url not in processed_urls:
                        processed_urls.add(valid_url)
                        products.append({
                            'url': valid_url,
                            'pnk_code': _extract_pnk_code_from_url(valid_url),
                            'thumbnail_image': None,
                            'price': None,
                            'review_count': None,
                            'rating': None
                        })
                        pd_links_valid += 1
            
        
        # 按URL排序（保持一致性）
        result = sorted(products, key=lambda x: x['url'])
        
        if result:
            logger.debug(f"Extracted {len(result)} product links with metadata from search results")
        else:
            logger.warning("No product links found in search results")
        
        return result
        
    except Exception as e:
        logger.error(f"Error extracting product links: {e}", exc_info=True)
        return []

def crawl_product_details(
    product_url: str,
    task_id: Optional[int] = None,
    db: Optional[Session] = None
) -> Optional[dict]:
    """
    Crawl product details from emag product page with retry and error handling
    
    Args:
        product_url: URL of the product page
        task_id: Task ID for logging (optional)
        db: Database session (optional)
        
    Returns:
        Dictionary with product data or None if failed
    """
    try:
        # Get proxy if enabled
        proxies = proxy_manager.get_random_proxy()
        
        # Random delay to avoid rate limiting
        delay = random.uniform(config.CRAWLER_DELAY_MIN, config.CRAWLER_DELAY_MAX)
        time.sleep(delay)
        
        # Set headers to mimic browser
        headers = get_random_headers()
        
        # Make request with retry mechanism
        def _make_request():
            response = requests.get(
                product_url,
                headers=headers,
                proxies=proxies,
                timeout=config.CRAWLER_TIMEOUT,
                stream=False,  # Disable streaming to avoid downloading images
                allow_redirects=True
            )
            response.raise_for_status()
            return response
        
        # Use retry manager for request
        try:
            response = retry_manager.execute_with_retry(
                _make_request,
                task_id=task_id
            )
        except Exception as e:
            logger.error(f"Failed to fetch {product_url} after retries: {e}")
            if task_id and db:
                error_type = retry_manager.classify_error(e)
                retry_manager.log_error(task_id, e, error_type, db=db)
            return None
        
        # Check for captcha
        if captcha_handler.detect_captcha(response.text, response.text):
            logger.warning(f"Captcha detected on {product_url}")
            if task_id and db:
                captcha_handler.handle_captcha(
                    task_id,
                    html_content=response.text[:1000],
                    response_text=response.text[:1000],
                    db=db
                )
            return None
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Extract product data
        product_data = _extract_product_data(soup, product_url)
        
        return product_data
        
    except Exception as e:
        logger.error(f"Error crawling {product_url}: {e}", exc_info=True)
        if task_id and db:
            error_type = retry_manager.classify_error(e)
            retry_manager.log_error(task_id, e, error_type, db=db)
        return None

def _extract_product_data(soup: BeautifulSoup, product_url: str) -> Dict[str, Any]:
    """
    Extract product data from parsed HTML
    
    Args:
        soup: BeautifulSoup parsed HTML
        product_url: Product URL
        
    Returns:
        Dictionary with extracted product data
    """
    data = {
        'product_url': product_url,
        'product_name': None,
        'thumbnail_image': None,  # 产品缩略图
        'brand': None,  # 品牌
        'shop_name': None,  # 店铺名称
        'price': None,
        'stock': None,
        'review_count': None,
        'shop_rank': None,
        'category_rank': None,
        'ad_rank': None,
        'listed_at': None,
        'latest_review_at': None,
        'earliest_review_at': None,
        'is_fbe': False,  # 是否是FBE
        'competitor_count': 0,  # 跟卖数
    }
    
    try:
        # 优先通过 Istoric Preturi 接口获取“上架日期”，DOM 解析作为兜底
        try:
            listed_at = get_istoric_listed_at(product_url)
            if listed_at:
                data['listed_at'] = listed_at
                logger.info(
                    f"[上架日期] 通过 Istoric Preturi 获取上架日期成功 - URL: {product_url}, "
                    f"listed_at: {listed_at.isoformat()}"
                )
        except Exception as e:
            logger.warning(f"[上架日期] 调用 Istoric Preturi 接口失败 - URL: {product_url}, 错误: {e}")

        # Extract product name
        # Try multiple selectors for product name
        name_selectors = [
            'h1.page-title',
            'h1.product-title',
            '.product-title',
            'h1[itemprop="name"]',
            '.product-title-container h1',
            'h1.page-title-container',
            'h1.product-page-title',
        ]
        for selector in name_selectors:
            name_elem = soup.select_one(selector)
            if name_elem:
                data['product_name'] = name_elem.get_text(strip=True)
                break
        
        # Extract price
        # Emag price selectors
        price_selectors = [
            '.product-new-price',
            '.product-old-price',
            '.price',
            '[data-price]',
            '.product-price',
            '.product-price-container .price',
            'p.product-new-price',
            'span.product-new-price',
        ]
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                # Remove currency symbols and extract numeric value
                # Romanian Lei format: "1.234,56 Lei" or "1,234.56 RON"
                price_text_clean = price_text.replace('Lei', '').replace('RON', '').replace('lei', '').strip()
                # Try to extract number (handle both comma and dot as decimal separator)
                price_match = re.search(r'[\d\s.,]+', price_text_clean)
                if price_match:
                    price_str = price_match.group().replace(' ', '').replace(',', '.')
                    # Handle cases where comma is thousand separator
                    if price_str.count('.') > 1:
                        price_str = price_str.replace('.', '', price_str.count('.') - 1)
                    try:
                        data['price'] = float(price_str)
                        break
                    except ValueError:
                        continue
        
        # Extract stock information
        stock_selectors = [
            '.stock-info',
            '.availability',
            '[data-stock]',
            '.product-availability',
            '.stock-status',
            '.product-stock',
        ]
        stock_found = False
        for selector in stock_selectors:
            stock_elem = soup.select_one(selector)
            if stock_elem:
                stock_text = stock_elem.get_text(strip=True).lower()
                # Check for stock indicators (Romanian: "disponibil", "in stoc", "stoc epuizat")
                if any(keyword in stock_text for keyword in ['disponibil', 'in stock', 'in stoc', 'disponibila']):
                    # Try to extract stock number
                    stock_match = re.search(r'(\d+)\s*(buc|pcs|pieces|unit)', stock_text, re.IGNORECASE)
                    if stock_match:
                        data['stock'] = int(stock_match.group(1))
                    else:
                        data['stock'] = 1  # Assume in stock if no number
                    stock_found = True
                    break
                elif any(keyword in stock_text for keyword in ['epuizat', 'out of stock', 'stoc epuizat', 'indisponibil']):
                    data['stock'] = 0
                    stock_found = True
                    break
        
        if not stock_found:
            # Try to find stock from other indicators
            stock_indicators = soup.find_all(string=re.compile(r'(disponibil|in stock|epuizat|out of stock)', re.IGNORECASE))
            if stock_indicators:
                stock_text = ' '.join(stock_indicators).lower()
                if 'disponibil' in stock_text or 'in stock' in stock_text:
                    data['stock'] = 1
                else:
                    data['stock'] = 0
        
        # Extract review count
        review_selectors = [
            '.reviews-count',
            '.rating-count',
            '[data-reviews]',
            '.product-reviews-count',
            '.reviews-summary .count',
            'span[itemprop="reviewCount"]',
        ]
        for selector in review_selectors:
            review_elem = soup.select_one(selector)
            if review_elem:
                review_text = review_elem.get_text(strip=True)
                # Extract number from text like "123 reviews" or "123 recenzii"
                review_match = re.search(r'(\d+)', review_text.replace(',', '').replace('.', ''))
                if review_match:
                    try:
                        data['review_count'] = int(review_match.group(1))
                        break
                    except ValueError:
                        continue
        
        # Extract review dates (if available)
        # Latest review date
        latest_review_selectors = [
            '.latest-review-date',
            '.reviews .review:first-child .review-date',
            '[data-latest-review]',
        ]
        for selector in latest_review_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                parsed_date = _parse_date(date_text)
                if parsed_date:
                    data['latest_review_at'] = parsed_date
                    break
        
        # Earliest review date
        earliest_review_selectors = [
            '.earliest-review-date',
            '.reviews .review:last-child .review-date',
            '[data-earliest-review]',
        ]
        for selector in earliest_review_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                parsed_date = _parse_date(date_text)
                if parsed_date:
                    data['earliest_review_at'] = parsed_date
                    break
        
        # Extract rankings (these are typically harder to extract)
        # Shop rank, category rank, ad rank may be in JavaScript or require API calls
        # For now, we'll try to extract from data attributes or specific elements
        rank_elem = soup.select_one('[data-shop-rank], [data-category-rank], [data-ad-rank]')
        if rank_elem:
            shop_rank = rank_elem.get('data-shop-rank')
            category_rank = rank_elem.get('data-category-rank')
            ad_rank = rank_elem.get('data-ad-rank')
            if shop_rank:
                try:
                    data['shop_rank'] = int(shop_rank)
                except ValueError:
                    pass
            if category_rank:
                try:
                    data['category_rank'] = int(category_rank)
                except ValueError:
                    pass
            if ad_rank:
                try:
                    data['ad_rank'] = int(ad_rank)
                except ValueError:
                    pass
        
        # 若 Istoric Preturi 未获取到，则继续尝试从页面 DOM 中解析 listed_at 作为兜底
        if not data.get('listed_at'):
            listed_at_selectors = [
                '[data-listed-at]',
                '.product-info .listed-date',
                '.product-details .date',
            ]
            for selector in listed_at_selectors:
                date_elem = soup.select_one(selector)
                if date_elem:
                    date_text = date_elem.get_text(strip=True) or date_elem.get('data-listed-at', '')
                    parsed_date = _parse_date(date_text)
                    if parsed_date:
                        data['listed_at'] = parsed_date
                        break
        
        # Extract thumbnail image
        thumbnail_selectors = [
            'img.product-gallery-image',
            '.product-gallery img',
            '.product-images img',
            'img[itemprop="image"]',
            '.product-photo img',
            'img.product-main-image',
            '.thumbnail img',
        ]
        for selector in thumbnail_selectors:
            img_elem = soup.select_one(selector)
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy-src')
                if img_url:
                    # 确保是完整URL
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = 'https://www.emag.ro' + img_url
                    data['thumbnail_image'] = img_url
                    break
        
        # Extract brand
        brand_selectors = [
            '[itemprop="brand"]',
            '.product-brand',
            '.brand-name',
            '.manufacturer',
            'span.brand',
            'a.brand-link',
        ]
        for selector in brand_selectors:
            brand_elem = soup.select_one(selector)
            if brand_elem:
                brand_text = brand_elem.get_text(strip=True)
                if brand_text:
                    data['brand'] = brand_text
                    break
        
        # Extract shop name
        shop_selectors = [
            '.seller-name',
            '.shop-name',
            '.vendor-name',
            '[data-seller-name]',
            '.product-seller',
            '.seller-info .name',
        ]
        for selector in shop_selectors:
            shop_elem = soup.select_one(selector)
            if shop_elem:
                shop_text = shop_elem.get_text(strip=True)
                if shop_text:
                    data['shop_name'] = shop_text
                    break
        
        # Extract FBE (Fulfilled by eMAG) indicator
        # FBE通常在eMAG上有特殊标识，比如"Livrare din stoc eMAG"或"Fulfilled by eMAG"
        fbe_indicators = soup.find_all(string=re.compile(r'(fulfilled by emag|livrare din stoc emag|vandut si livrat de emag)', re.IGNORECASE))
        if fbe_indicators:
            data['is_fbe'] = True
        else:
            # 也可以通过seller信息判断，如果seller是eMAG则是FBE
            seller_elem = soup.select_one('.seller-name, .shop-name, .vendor-name')
            if seller_elem:
                seller_text = seller_elem.get_text(strip=True).lower()
                if 'emag' in seller_text and 'marketplace' not in seller_text:
                    data['is_fbe'] = True
        
        # Extract competitor count (跟卖数)
        # 需要查找有多少个卖家在销售同一产品
        competitor_selectors = [
            '.other-sellers',
            '.alternative-sellers',
            '.sellers-list',
            '[data-seller-count]',
            '.offers-count',
        ]
        competitor_count = 0
        for selector in competitor_selectors:
            competitor_elem = soup.select_one(selector)
            if competitor_elem:
                # 尝试从data属性获取
                count_attr = competitor_elem.get('data-seller-count') or competitor_elem.get('data-offers-count')
                if count_attr:
                    try:
                        competitor_count = int(count_attr)
                        break
                    except ValueError:
                        pass
                
                # 尝试从文本中提取数字
                count_text = competitor_elem.get_text(strip=True)
                count_match = re.search(r'(\d+)', count_text)
                if count_match:
                    try:
                        competitor_count = int(count_match.group(1))
                        break
                    except ValueError:
                        pass
                
                # 尝试统计列表中的卖家数量
                seller_items = competitor_elem.select('.seller-item, .offer-item, li')
                if seller_items:
                    competitor_count = len(seller_items)
                    break
        
        # 如果找到了其他卖家，competitor_count应该是其他卖家数量（不包括自己）
        # 如果只有一个卖家，competitor_count为0
        data['competitor_count'] = max(0, competitor_count - 1) if competitor_count > 0 else 0
        
    except Exception as e:
        logger.warning(f"Error extracting product data from {product_url}: {e}", exc_info=True)
    
    return data

def _parse_date(date_text: str) -> Optional[datetime]:
    """
    Parse date string to datetime object
    
    Args:
        date_text: Date string in various formats
        
    Returns:
        datetime object or None if parsing fails
    """
    if not date_text:
        return None
    
    # Common date formats
    date_formats = [
        '%Y-%m-%d',
        '%d.%m.%Y',
        '%d/%m/%Y',
        '%Y-%m-%d %H:%M:%S',
        '%d.%m.%Y %H:%M:%S',
        '%d %B %Y',
        '%d %b %Y',
        '%B %d, %Y',
        '%b %d, %Y',
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_text.strip(), fmt)
        except ValueError:
            continue
    
    # Try to extract date from text using regex
    date_match = re.search(r'(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})', date_text)
    if date_match:
        day, month, year = date_match.groups()
        if len(year) == 2:
            year = '20' + year if int(year) < 50 else '19' + year
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            pass
    
    return None

def crawl_monitor_product(
    monitor_id: int,
    product_url: str,
    db: Session
) -> Optional[Dict[str, Any]]:
    """
    Crawl product data for monitoring and save to history
    
    使用ProductDataCrawler.crawl_dynamic_data()仅爬取动态数据
    从FilterPool获取shop_url和category_url，避免重复爬取
    
    Args:
        monitor_id: Monitor pool ID
        product_url: Product URL to crawl
        db: Database session
        
    Returns:
        Crawled product data (仅包含6个核心监控字段) or None if failed
    """
    try:
        # 从MonitorPool获取filter_pool_id，然后从FilterPool获取链接
        from app.models.monitor_pool import MonitorPool
        monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
        
        shop_url = None
        category_url = None
        
        if monitor and monitor.filter_pool_id:
            filter_pool = db.query(FilterPool).filter(FilterPool.id == monitor.filter_pool_id).first()
            if filter_pool:
                shop_url = filter_pool.shop_url
                category_url = filter_pool.category_url
                if shop_url or category_url:
                    logger.info(f"[监控爬取] 从FilterPool获取链接 - monitor_id: {monitor_id}, shop_url: {shop_url}, category_url: {category_url}")
        
        # 使用新的Playwright爬取器，仅爬取动态数据
        crawler = ProductDataCrawler()
        
        def _crawl():
            return crawler.crawl_dynamic_data(
                product_url=product_url,
                task_id=None,  # Monitor tasks don't use crawl_tasks table
                db=db,
                shop_url=shop_url,  # 传入从FilterPool获取的shop_url
                category_url=category_url  # 传入从FilterPool获取的category_url
            )
        
        # 使用retry_manager包装爬取调用，自动处理重试和错误记录
        product_data = retry_manager.execute_with_retry(
            _crawl,
            task_id=None  # Monitor tasks don't use crawl_tasks table
        )
        
        if product_data:
            # 只返回6个核心监控字段：价格、库存、评分、店铺排名、类目排名、广告排名
            filtered_data = {
                'price': product_data.get('price'),
                'stock': product_data.get('stock_count') or product_data.get('stock'),
                'reviews_score': product_data.get('reviews_score'),  # 评分
                'shop_rank': product_data.get('store_rank') or product_data.get('shop_rank'),
                'category_rank': product_data.get('category_rank'),
                'ad_rank': product_data.get('ad_category_rank') or product_data.get('ad_rank'),
            }
            return filtered_data
        else:
            logger.warning(f"Failed to crawl product data for {product_url}")
            return None
            
    except Exception as e:
        logger.error(f"Error in crawl_monitor_product for {product_url}: {e}")
        return None

# ============================================================================
# Task Handler Functions (for TaskManager integration)
# ============================================================================

def _validate_product_list(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    验证产品列表的有效性
    
    Args:
        products: 产品列表，每个包含 url, pnk_code, thumbnail_image, price
        
    Returns:
        有效产品列表
    """
    if not products:
        return []
    
    valid_products = []
    base_url = "https://www.emag.ro"
    
    for product in products:
        # 跳过无效产品
        if not product or not isinstance(product, dict):
            continue
        
        url = product.get('url')
        # 跳过空URL
        if not url or not isinstance(url, str):
            continue
        
        # 验证URL格式
        validated_url = _validate_product_url(url, base_url)
        if validated_url:
            # 更新URL为验证后的URL
            product['url'] = validated_url
            if not product.get('pnk_code'):
                product['pnk_code'] = _extract_pnk_code_from_url(validated_url)
            valid_products.append(product)
        else:
            logger.debug(f"Invalid URL filtered: {url}")
    
    return valid_products

def handle_keyword_search_task(task_id: int, task: CrawlTask, db: Session) -> Dict[str, Any]:
    """
    关键字搜索任务处理器（增强版：数据验证和写入控制）
    
    此函数由TaskManager调用来执行关键字搜索任务。
    它爬取搜索结果，提取产品链接，并将它们保存到数据库。
    
    重要：
    - 只有在成功获取到有效数据时才写入数据库
    - 检测到验证码时不写入空数据，标记任务为RETRY
    - 网络错误时不写入空数据，抛出异常
    - 部分成功时只写入有效数据
    
    Args:
        task_id: 任务ID
        task: CrawlTask对象
        db: 数据库会话
        
    Returns:
        包含任务结果的字典
    """
    start_time = time.time()
    logger.info(f"[任务开始] 关键字搜索任务处理 - 任务ID: {task_id}, 关键字ID: {task.keyword_id}")
    
    try:
        # 获取关键字
        keyword_obj = db.query(Keyword).filter(Keyword.id == task.keyword_id).first()
        if not keyword_obj:
            logger.error(f"[任务失败] 关键字未找到 - 任务ID: {task_id}, 关键字ID: {task.keyword_id}")
            raise ValueError(f"Keyword {task.keyword_id} not found")
        
        keyword = keyword_obj.keyword
        logger.info(f"[任务进行中] 开始处理关键字 - 任务ID: {task_id}, 关键字: {keyword}, 最大页数: {config.KEYWORD_SEARCH_MAX_PAGES}")
        
        
        # 更新关键字状态
        keyword_obj.status = KeywordStatus.PROCESSING
        db.commit()
        
        # 更新任务进度
        task.progress = 5
        db.commit()
        
        # 爬取关键字搜索（使用配置的默认页数：5页）
        logger.info(f"Starting keyword search for '{keyword}' (task {task_id})")
        try:
            
            # 使用新的Playwright爬取器和retry_manager
            
            crawler = ProductLinkCrawler()
            
            
            def _crawl():
                try:
                    result = crawler.crawl_search_results(
                        keyword=keyword,
                        max_pages=config.KEYWORD_SEARCH_MAX_PAGES,
                        task_id=task_id,
                        db=db
                    )
                    return result
                except Exception as e:
                    raise
            
            # 使用retry_manager包装爬取调用，自动处理重试和错误记录
            
            product_urls = retry_manager.execute_with_retry(
                _crawl,
                task_id=task_id
            )
            
        except Exception as e:
            # 网络错误：不写入空数据，抛出异常
            logger.error(f"Network error during keyword search for '{keyword}': {e}")
            keyword_obj.status = KeywordStatus.FAILED
            task.status = TaskStatus.FAILED
            task.error_message = f"Network error: {str(e)}"
            db.commit()
            raise
        except Exception as e:
            # 其他错误：不写入空数据，抛出异常
            logger.error(f"Error during keyword search for '{keyword}': {e}")
            keyword_obj.status = KeywordStatus.FAILED
            task.status = TaskStatus.FAILED
            task.error_message = f"Error: {str(e)}"
            db.commit()
            raise
        
        # ⭐ 数据验证：检查是否获取到有效数据
        if not product_urls or len(product_urls) == 0:
            # 没有获取到数据：可能是验证码或其他原因
            logger.warning(f"No product URLs found for keyword '{keyword}' (task {task_id})")
            
            # 检查是否是验证码导致
            error_logs = db.query(ErrorLog).filter(
                ErrorLog.task_id == task_id,
                ErrorLog.error_type == ErrorType.CAPTCHA
            ).order_by(ErrorLog.occurred_at.desc()).first()
            
            if error_logs:
                # 验证码导致：不写入空数据，标记任务为RETRY
                logger.warning(f"Captcha detected for keyword '{keyword}', marking task as RETRY")
                keyword_obj.status = KeywordStatus.PENDING  # 保持PENDING状态，不标记为COMPLETED
                task.status = TaskStatus.RETRY
                task.error_message = "Captcha challenge detected, no data retrieved"
                db.commit()
                
                return {
                    "keyword": keyword,
                    "total_urls": 0,
                    "saved_count": 0,
                    "skipped_count": 0,
                    "status": "retry_captcha",
                    "message": "Captcha detected, no data saved"
                }
            else:
                # 其他原因导致无数据：标记为失败，不写入空数据
                logger.warning(f"No data retrieved for keyword '{keyword}', marking as failed")
                keyword_obj.status = KeywordStatus.FAILED
                task.status = TaskStatus.FAILED
                task.error_message = "No product URLs retrieved"
                db.commit()
                
                return {
                    "keyword": keyword,
                    "total_urls": 0,
                    "saved_count": 0,
                    "skipped_count": 0,
                    "status": "failed",
                    "message": "No data retrieved"
                }
        
        # ⭐ 数据验证：验证产品列表有效性
        validated_products = _validate_product_list(product_urls)
        
        if not validated_products or len(validated_products) == 0:
            # 所有产品都无效：不写入空数据
            logger.warning(f"All products invalid for keyword '{keyword}', no data saved")
            keyword_obj.status = KeywordStatus.FAILED
            task.status = TaskStatus.FAILED
            task.error_message = "All retrieved products are invalid"
            db.commit()
            
            return {
                "keyword": keyword,
                "total_urls": len(product_urls),
                "valid_urls": 0,
                "saved_count": 0,
                "skipped_count": 0,
                "status": "failed",
                "message": "All products invalid"
            }
        
        # 更新任务进度
        task.progress = 90
        db.commit()
        
        # ⭐ 数据完整性验证：基础数据（URL、价格）必须完整才允许写入
        # 分离完整数据和不完整数据
        complete_products = []
        incomplete_products = []
        
        for product_info in validated_products:
            url = product_info.get('url')
            price = product_info.get('price')
            pnk_code = product_info.get('pnk_code')
            
            # 必须字段：url 和 price（基础数据在网络正常情况下一定会有）
            if url and price is not None:
                complete_products.append(product_info)
            else:
                incomplete_products.append({
                    'url': url,
                    'price': price,
                    'pnk_code': pnk_code,
                    'thumbnail_image': product_info.get('thumbnail_image'),
                    'missing_fields': [f for f in ['url', 'price'] if not product_info.get(f) and f != 'url' or (f == 'price' and product_info.get(f) is None)]
                })
        
        
        # 记录不完整数据的警告日志
        if incomplete_products:
            logger.warning(
                f"[数据不完整] 任务ID: {task_id}, 关键字: {keyword}, "
                f"不完整数据数: {len(incomplete_products)}/{len(validated_products)}, "
                f"缺失字段示例: {incomplete_products[0] if incomplete_products else 'N/A'}"
            )
            # 将不完整数据记录到错误日志（使用已导入的全局 retry_manager）
            try:
                from app.services.retry_manager import retry_manager as rm_logger
                rm_logger.log_error(
                    task_id=task_id,
                    error=Exception(f"数据不完整: {len(incomplete_products)} 条产品缺少必要字段(price)"),
                    error_type=ErrorType.OTHER,
                    error_detail={
                        "incomplete_count": len(incomplete_products),
                        "incomplete_samples": incomplete_products[:10],
                        "keyword": keyword
                    },
                    db=db
                )
            except Exception as log_err:
                logger.error(f"记录不完整数据错误失败: {log_err}")
        
        # 只保存完整数据到数据库
        logger.info(f"[数据保存] 开始保存产品链接到数据库 - 任务ID: {task_id}, 关键字: {keyword}, 完整数据数: {len(complete_products)}, 不完整数据数: {len(incomplete_products)}")
        saved_count = 0
        skipped_count = 0
        
        for product_info in complete_products:
            url = product_info.get('url')
            # 检查链接是否已存在
            existing = db.query(KeywordLink).filter(
                KeywordLink.keyword_id == task.keyword_id,
                KeywordLink.product_url == url
            ).first()
            
            if not existing:
                link = KeywordLink(
                    keyword_id=task.keyword_id,
                    product_url=url,
                    pnk_code=product_info.get('pnk_code'),
                    thumbnail_image=product_info.get('thumbnail_image'),
                    price=product_info.get('price'),
                    review_count=product_info.get('review_count'),
                    rating=product_info.get('rating'),
                    status="active"
                )
                
                db.add(link)
                saved_count += 1
            else:
                # 更新现有链接的缩略图和价格（如果为空）
                if not existing.pnk_code and product_info.get('pnk_code'):
                    existing.pnk_code = product_info.get('pnk_code')
                if not existing.thumbnail_image and product_info.get('thumbnail_image'):
                    existing.thumbnail_image = product_info.get('thumbnail_image')
                if existing.price is None and product_info.get('price') is not None:
                    existing.price = product_info.get('price')
                # 更新评论数和评分（如果为空或需要更新）
                updated_fields = []
                if existing.review_count is None and product_info.get('review_count') is not None:
                    existing.review_count = product_info.get('review_count')
                    updated_fields.append('review_count')
                if existing.rating is None and product_info.get('rating') is not None:
                    existing.rating = product_info.get('rating')
                    updated_fields.append('rating')
                
                skipped_count += 1
        
        # 提交数据库更改
        db.commit()
        
        # 更新关键字状态为完成（只有成功保存数据才标记为完成）
        keyword_obj.status = KeywordStatus.COMPLETED
        db.commit()
        
        # 更新任务进度
        task.progress = 100
        task.status = TaskStatus.COMPLETED
        db.commit()
        
        total_elapsed = time.time() - start_time
        logger.info(
            f"[任务完成] 关键字搜索任务完成 - 任务ID: {task_id}, 关键字: {keyword}, "
            f"保存链接数: {saved_count}, 跳过重复数: {skipped_count}, "
            f"完整数据: {len(complete_products)}, 不完整数据(已跳过): {len(incomplete_products)}, "
            f"有效产品数: {len(validated_products)}/{len(product_urls)}, 总耗时: {total_elapsed:.2f}秒"
        )
        
        return {
            "keyword": keyword,
            "total_urls": len(product_urls),
            "valid_urls": len(validated_products),
            "complete_count": len(complete_products),
            "incomplete_count": len(incomplete_products),
            "saved_count": saved_count,
            "skipped_count": skipped_count,
            "status": "completed"
        }
        
    except requests.exceptions.RequestException as e:
        # 网络错误：不写入空数据，抛出异常
        logger.error(f"Network error in handle_keyword_search_task for task {task_id}: {e}")
        
        # 更新关键字状态为失败
        if task.keyword_id:
            keyword_obj = db.query(Keyword).filter(Keyword.id == task.keyword_id).first()
            if keyword_obj:
                keyword_obj.status = KeywordStatus.FAILED
                db.commit()
        
        task.status = TaskStatus.FAILED
        task.error_message = f"Network error: {str(e)}"
        db.commit()
        
        raise
    except Exception as e:
        # 其他错误：不写入空数据，抛出异常
        logger.error(f"Error in handle_keyword_search_task for task {task_id}: {e}", exc_info=True)
        
        # 更新关键字状态为失败
        if task.keyword_id:
            keyword_obj = db.query(Keyword).filter(Keyword.id == task.keyword_id).first()
            if keyword_obj:
                keyword_obj.status = KeywordStatus.FAILED
                db.commit()
        
        task.status = TaskStatus.FAILED
        task.error_message = f"Error: {str(e)}"
        db.commit()
        
        raise

def handle_product_crawl_task(task_id: int, task: CrawlTask, db: Session) -> Dict[str, Any]:
    """
    Task handler for product crawl tasks
    
    This function is called by TaskManager to execute product crawl tasks.
    It crawls product details and saves them to filter_pool.
    
    Args:
        task_id: Task ID
        task: CrawlTask object
        db: Database session
        
    Returns:
        Dictionary with task results
    """
    start_time = time.time()
    try:
        if not task.product_url:
            logger.error(f"[任务失败] 产品URL未指定 - 任务ID: {task_id}")
            raise ValueError(f"Product URL not specified for task {task_id}")
        
        product_url = task.product_url
        print(f"[任务开始] 产品爬取任务处理 - 任务ID: {task_id}, 产品URL: {product_url}")
        logger.info(f"[任务开始] 产品爬取任务处理 - 任务ID: {task_id}, 产品URL: {product_url}")
        
        # Update task progress
        task.progress = 10
        db.commit()
        
        # Crawl product details
        logger.info(f"[爬取进行中] 开始爬取产品详情 - 任务ID: {task_id}, 产品URL: {product_url}")
        
        # 使用新的Playwright爬取器
        crawler = ProductDataCrawler()
        
        # 直接调用爬取，不进行重试（失败的任务将在批量重试阶段处理）
        # 注意：错误记录在task_manager中统一处理，这里只抛出异常
        product_data = crawler.crawl_full_data(
            product_url=product_url,
            include_base_info=True,
            task_id=task_id,
            db=db
        )
        
        if not product_data:
            logger.error(f"[任务失败] 爬取产品数据失败 - 任务ID: {task_id}, 产品URL: {product_url}")
            raise ValueError(f"Failed to crawl product data for {product_url}")
        
        logger.info(f"[爬取完成] 产品数据爬取完成 - 任务ID: {task_id}, 产品URL: {product_url}, 数据字段数: {len(product_data)}")
        
        # #region agent log
        import json as _json_save
        _ranking_keys = ['category_rank', 'ad_category_rank', 'store_rank', 'shop_rank', 'ad_rank']
        _basic_keys = ['title', 'product_name', 'price', 'stock_count', 'stock', 'review_count', 'brand', 'shop_name', 'latest_review_date', 'latest_review_at', 'listed_at']
        _ranking_data = {k: product_data.get(k) for k in _ranking_keys}
        _basic_data = {k: str(product_data.get(k))[:50] if product_data.get(k) is not None else None for k in _basic_keys}
        _is_emag = product_data.get('is_emag_official', False)
        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
            _f.write(_json_save.dumps({"timestamp": int(time.time()*1000), "location": "crawler.py:product_data_received", "message": "爬取结果数据", "data": {"task_id": task_id, "url": product_url, "all_keys": list(product_data.keys()), "ranking": _ranking_data, "basic": _basic_data, "total_fields": len(product_data), "is_emag_official": _is_emag}, "hypothesisId": "G1,G2,H_emag_detect", "runId": "emag-official-fix"}, default=str) + "\n")
        # #endregion
        
        # Update task progress
        task.progress = 80
        db.commit()
        
        # 从 keyword_link 获取上架日期（如果存在）
        keyword_link_listed_at = None
        if task.keyword_id:
            keyword_link = db.query(KeywordLink).filter(
                KeywordLink.product_url == product_url,
                KeywordLink.keyword_id == task.keyword_id
            ).first()
            if keyword_link and keyword_link.listed_at:
                keyword_link_listed_at = keyword_link.listed_at
                logger.info(f"[数据保存] 从 keyword_link 获取上架日期 - 任务ID: {task_id}, 产品URL: {product_url}, 上架日期: {keyword_link_listed_at}")
        
        # 优先使用 keyword_link 中的上架日期，否则使用爬取结果中的
        final_listed_at = keyword_link_listed_at or product_data.get('listed_at')
        
        # Check if product already exists in filter_pool
        logger.info(f"[数据保存] 开始保存产品数据到数据库 - 任务ID: {task_id}, 产品URL: {product_url}")
        existing = db.query(FilterPool).filter(
            FilterPool.product_url == product_url
        ).first()
        
        if existing:
            # Update existing record
            # #region agent log
            _matched = [k for k in product_data.keys() if k != 'product_url' and hasattr(existing, k)]
            _dropped = [k for k in product_data.keys() if k != 'product_url' and not hasattr(existing, k)]
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_save.dumps({"timestamp": int(time.time()*1000), "location": "crawler.py:update_existing", "message": "更新现有记录-字段匹配", "data": {"task_id": task_id, "url": product_url, "is_new": False, "matched_fields": _matched, "dropped_fields": _dropped, "existing_shop_rank": existing.shop_rank, "existing_category_rank": existing.category_rank, "existing_ad_rank": existing.ad_rank, "existing_stock": existing.stock, "existing_product_name": str(existing.product_name)[:50] if existing.product_name else None, "listed_at_from_keyword_link": keyword_link_listed_at is not None, "final_listed_at": str(final_listed_at) if final_listed_at else None}, "hypothesisId": "G1,G4", "runId": "field-debug"}, default=str) + "\n")
            # #endregion
            
            for key, value in product_data.items():
                if key != 'product_url' and hasattr(existing, key):
                    setattr(existing, key, value)
            # 更新上架日期（优先使用 keyword_link 中的）
            if final_listed_at:
                existing.listed_at = final_listed_at
            existing.crawled_at = datetime.utcnow()
            db.commit()
            
            logger.info(f"[数据保存] 更新现有产品数据 - 任务ID: {task_id}, 产品URL: {product_url}, 上架日期: {final_listed_at}")
        else:
            # Create new record
            # 注意：ProductDataCrawler返回的字段名可能与FilterPool不同，需要映射
            shop_rank_value = product_data.get('store_rank') or product_data.get('shop_rank')
            category_rank_value = product_data.get('category_rank')
            ad_rank_value = product_data.get('ad_category_rank') or product_data.get('ad_rank')
            
            filter_pool_item = FilterPool(
                product_url=product_data['product_url'],
                product_name=product_data.get('title') or product_data.get('product_name'),  # 新格式使用title
                thumbnail_image=product_data.get('thumbnail_image'),
                brand=product_data.get('brand'),
                shop_name=product_data.get('shop_name'),
                # 在筛选池中同时保存店铺介绍页、完整店铺URL和类目URL，便于后续复用
                shop_intro_url=product_data.get('shop_intro_url'),
                shop_url=product_data.get('shop_url'),
                category_url=product_data.get('category_url'),
                price=product_data.get('price'),
                listed_at=final_listed_at,  # 优先使用 keyword_link 中的上架日期
                stock=product_data.get('stock_count') or product_data.get('stock'),  # 新格式使用stock_count
                review_count=product_data.get('review_count'),
                latest_review_at=product_data.get('latest_review_date') or product_data.get('latest_review_at'),  # 新格式使用latest_review_date
                earliest_review_at=product_data.get('earliest_review_at'),
                shop_rank=shop_rank_value,  # 新格式使用store_rank
                category_rank=category_rank_value,
                ad_rank=ad_rank_value,  # 新格式使用ad_category_rank
                is_fbe=product_data.get('is_fbe', False),
                competitor_count=product_data.get('competitor_count', 0),
                crawled_at=datetime.utcnow()
            )
            db.add(filter_pool_item)
            db.commit()
            
            # #region agent log
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_save.dumps({"timestamp": int(time.time()*1000), "location": "crawler.py:insert_new", "message": "插入新记录", "data": {"task_id": task_id, "url": product_url, "is_new": True, "shop_rank": shop_rank_value, "category_rank": category_rank_value, "ad_rank": ad_rank_value, "price": product_data.get('price'), "stock": product_data.get('stock_count') or product_data.get('stock'), "product_name": str(product_data.get('title') or product_data.get('product_name'))[:50]}, "hypothesisId": "G1,G4", "runId": "field-debug"}, default=str) + "\n")
            # #endregion
            logger.info(f"[数据保存] 添加新产品数据 - 任务ID: {task_id}, 产品URL: {product_url}")
        
        
        # Update task progress
        task.progress = 100
        task.status = TaskStatus.COMPLETED
        db.commit()
        
        total_elapsed = time.time() - start_time
        logger.info(f"[任务完成] 产品爬取任务完成 - 任务ID: {task_id}, 产品URL: {product_url}, 总耗时: {total_elapsed:.2f}秒")
        
        return {
            "product_url": product_url,
            "product_name": product_data.get('product_name'),
            "price": product_data.get('price'),
            "updated": existing is not None
        }
        
    except Exception as e:
        total_elapsed = time.time() - start_time if 'start_time' in locals() else 0
        logger.error(f"[任务失败] 产品爬取任务失败 - 任务ID: {task_id}, 产品URL: {task.product_url if task else 'N/A'}, 错误: {str(e)}, 错误类型: {type(e).__name__}, 耗时: {total_elapsed:.2f}秒", exc_info=True)
        # #region agent log
        import json as _json_fail, time as _time_fail
        try:
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json_fail.dumps({
                    "timestamp": int(_time_fail.time() * 1000),
                    "location": "crawler.py:task_failed",
                    "message": "产品爬取任务最终失败",
                    "data": {
                        "task_id": task_id,
                        "product_url": task.product_url if task else None,
                        "error_type": type(e).__name__,
                        "error_message": str(e)[:300],
                        "elapsed_sec": round(total_elapsed, 2),
                    },
                    "hypothesisId": "H_timeout_captcha",
                    "runId": "retry-debug"
                }, ensure_ascii=False) + "\n")
        except Exception:
            # 调试日志失败不影响主流程
            pass
        # #endregion

        # 不在这里更新任务状态，让task_manager统一处理
        # task_manager会记录错误并标记任务为FAILED
        raise

# ============================================================================
# Batch Crawling Functions (Multi-threaded)
# ============================================================================

def batch_crawl_keywords(
    keyword_ids: List[int],
    db: Session,
    max_workers: Optional[int] = None
) -> Dict[int, Dict[str, Any]]:
    """
    Batch crawl multiple keywords concurrently using thread pool
    
    Args:
        keyword_ids: List of keyword IDs to crawl
        db: Database session
        max_workers: Maximum number of worker threads (default from config)
        
    Returns:
        Dictionary mapping keyword_id to results
    """
    results = {}
    
    def _crawl_single_keyword(keyword_id: int) -> tuple:
        """Crawl a single keyword"""
        db_session = SessionLocal()
        try:
            keyword = db_session.query(Keyword).filter(Keyword.id == keyword_id).first()
            if not keyword:
                return keyword_id, {"error": "Keyword not found"}
            
            # Create a temporary task for progress tracking
            task = CrawlTask(
                task_type=TaskType.KEYWORD_SEARCH,
                keyword_id=keyword_id,
                user_id=keyword.created_by_user_id,
                status=TaskStatus.PROCESSING,
                priority=TaskPriority.NORMAL
            )
            db_session.add(task)
            db_session.commit()
            db_session.refresh(task)
            
            try:
                result = handle_keyword_search_task(task.id, task, db_session)
                return keyword_id, result
            except Exception as e:
                logger.error(f"Error crawling keyword {keyword_id}: {e}")
                return keyword_id, {"error": str(e)}
        finally:
            db_session.close()
    
    # Use thread pool for concurrent crawling
    pool_name = "keyword_search"
    if max_workers is None:
        max_workers = config.KEYWORD_SEARCH_THREADS
    
    futures = []
    for keyword_id in keyword_ids:
        future = thread_pool_manager.submit(pool_name, _crawl_single_keyword, keyword_id)
        futures.append(future)
    
    # Collect results
    for future in as_completed(futures):
        try:
            keyword_id, result = future.result()
            results[keyword_id] = result
        except Exception as e:
            logger.error(f"Error getting result from keyword crawl: {e}")
    
    return results

def batch_crawl_products(
    product_urls: List[str],
    db: Session,
    max_workers: Optional[int] = None,
    task_id: Optional[int] = None
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    Batch crawl multiple products concurrently using thread pool
    
    Args:
        product_urls: List of product URLs to crawl
        db: Database session
        max_workers: Maximum number of worker threads (default from config)
        task_id: Optional parent task ID for progress tracking
        
    Returns:
        Dictionary mapping product_url to crawled data (or None if failed)
    """
    results = {}
    
    def _crawl_single_product(product_url: str) -> tuple:
        """Crawl a single product"""
        db_session = SessionLocal()
        try:
            product_data = crawl_product_details(
                product_url=product_url,
                task_id=task_id,
                db=db_session
            )
            return product_url, product_data
        except Exception as e:
            logger.error(f"Error crawling product {product_url}: {e}")
            return product_url, None
        finally:
            db_session.close()
    
    # Use thread pool for concurrent crawling
    pool_name = "product_crawl"
    if max_workers is None:
        max_workers = config.PRODUCT_CRAWL_THREADS
    
    futures = []
    for url in product_urls:
        future = thread_pool_manager.submit(pool_name, _crawl_single_product, url)
        futures.append(future)
    
    # Collect results and save to database
    successful_count = 0
    failed_count = 0
    
    for future in as_completed(futures):
        try:
            product_url, product_data = future.result()
            results[product_url] = product_data
            
            if product_data:
                # Save to filter_pool
                db_session = SessionLocal()
                try:
                    existing = db_session.query(FilterPool).filter(
                        FilterPool.product_url == product_url
                    ).first()
                    
                    if existing:
                        # Update existing
                        for key, value in product_data.items():
                            if key != 'product_url' and hasattr(existing, key):
                                setattr(existing, key, value)
                        existing.crawled_at = datetime.utcnow()
                    else:
                        # Create new
                        filter_pool_item = FilterPool(
                            product_url=product_data['product_url'],
                            product_name=product_data.get('product_name'),
                            thumbnail_image=product_data.get('thumbnail_image'),
                            brand=product_data.get('brand'),
                            shop_name=product_data.get('shop_name'),
                            price=product_data.get('price'),
                            listed_at=product_data.get('listed_at'),
                            stock=product_data.get('stock'),
                            review_count=product_data.get('review_count'),
                            latest_review_at=product_data.get('latest_review_at'),
                            earliest_review_at=product_data.get('earliest_review_at'),
                            shop_rank=product_data.get('shop_rank'),
                            category_rank=product_data.get('category_rank'),
                            ad_rank=product_data.get('ad_rank'),
                            is_fbe=product_data.get('is_fbe', False),
                            competitor_count=product_data.get('competitor_count', 0),
                            crawled_at=datetime.utcnow()
                        )
                        db_session.add(filter_pool_item)
                    
                    db_session.commit()
                    successful_count += 1
                except Exception as e:
                    logger.error(f"Error saving product {product_url} to database: {e}")
                    db_session.rollback()
                    failed_count += 1
                finally:
                    db_session.close()
            else:
                failed_count += 1
                
        except Exception as e:
            logger.error(f"Error getting result from product crawl: {e}")
            failed_count += 1
    
    logger.info(
        f"Batch product crawl completed: {successful_count} successful, "
        f"{failed_count} failed out of {len(product_urls)} total"
    )
    
    return results

def batch_crawl_keyword_links(
    keyword_id: int,
    db: Session,
    max_workers: Optional[int] = None,
    batch_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Batch crawl all product links for a keyword
    
    Args:
        keyword_id: Keyword ID
        db: Database session
        max_workers: Maximum number of worker threads
        batch_size: Number of URLs to process in each batch (for memory management)
        
    Returns:
        Dictionary with crawl statistics
    """
    # Get all links for keyword
    links = db.query(KeywordLink).filter(
        KeywordLink.keyword_id == keyword_id,
        KeywordLink.status == "active"
    ).all()
    
    if not links:
        return {
            "keyword_id": keyword_id,
            "total_links": 0,
            "crawled": 0,
            "failed": 0
        }
    
    product_urls = [link.product_url for link in links]
    
    # Use batch processing if batch_size is specified
    if batch_size:
        all_results = {}
        for i in range(0, len(product_urls), batch_size):
            batch = product_urls[i:i + batch_size]
            batch_results = batch_crawl_products(batch, db, max_workers=max_workers)
            all_results.update(batch_results)
        results = all_results
    else:
        results = batch_crawl_products(product_urls, db, max_workers=max_workers)
    
    # Calculate statistics
    successful = sum(1 for v in results.values() if v is not None)
    failed = len(results) - successful
    
    return {
        "keyword_id": keyword_id,
        "total_links": len(product_urls),
        "crawled": successful,
        "failed": failed
    }


# ============================================================================
# Batch Crawling Functions (Multi-threaded)
# ============================================================================

def batch_crawl_keywords(
    keyword_ids: List[int],
    db: Session,
    max_workers: Optional[int] = None
) -> Dict[int, Dict[str, Any]]:
    """
    Batch crawl multiple keywords concurrently using thread pool
    
    Args:
        keyword_ids: List of keyword IDs to crawl
        db: Database session
        max_workers: Maximum number of worker threads (default from config)
        
    Returns:
        Dictionary mapping keyword_id to results
    """
    results = {}
    
    def _crawl_single_keyword(keyword_id: int) -> tuple:
        """Crawl a single keyword"""
        db_session = SessionLocal()
        try:
            keyword = db_session.query(Keyword).filter(Keyword.id == keyword_id).first()
            if not keyword:
                return keyword_id, {"error": "Keyword not found"}
            
            # Create a temporary task for progress tracking
            task = CrawlTask(
                task_type=TaskType.KEYWORD_SEARCH,
                keyword_id=keyword_id,
                user_id=keyword.created_by_user_id,
                status=TaskStatus.PROCESSING,
                priority=TaskPriority.NORMAL
            )
            db_session.add(task)
            db_session.commit()
            db_session.refresh(task)
            
            try:
                result = handle_keyword_search_task(task.id, task, db_session)
                return keyword_id, result
            except Exception as e:
                logger.error(f"Error crawling keyword {keyword_id}: {e}")
                return keyword_id, {"error": str(e)}
        finally:
            db_session.close()
    
    # Use thread pool for concurrent crawling
    pool_name = "keyword_search"
    if max_workers is None:
        max_workers = config.KEYWORD_SEARCH_THREADS
    
    futures = []
    for keyword_id in keyword_ids:
        future = thread_pool_manager.submit(pool_name, _crawl_single_keyword, keyword_id)
        futures.append(future)
    
    # Collect results
    for future in as_completed(futures):
        try:
            keyword_id, result = future.result()
            results[keyword_id] = result
        except Exception as e:
            logger.error(f"Error getting result from keyword crawl: {e}")
    
    return results

def batch_crawl_products(
    product_urls: List[str],
    db: Session,
    max_workers: Optional[int] = None,
    task_id: Optional[int] = None
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    Batch crawl multiple products concurrently using thread pool
    
    Args:
        product_urls: List of product URLs to crawl
        db: Database session
        max_workers: Maximum number of worker threads (default from config)
        task_id: Optional parent task ID for progress tracking
        
    Returns:
        Dictionary mapping product_url to crawled data (or None if failed)
    """
    results = {}
    
    def _crawl_single_product(product_url: str) -> tuple:
        """Crawl a single product"""
        db_session = SessionLocal()
        try:
            product_data = crawl_product_details(
                product_url=product_url,
                task_id=task_id,
                db=db_session
            )
            return product_url, product_data
        except Exception as e:
            logger.error(f"Error crawling product {product_url}: {e}")
            return product_url, None
        finally:
            db_session.close()
    
    # Use thread pool for concurrent crawling
    pool_name = "product_crawl"
    if max_workers is None:
        max_workers = config.PRODUCT_CRAWL_THREADS
    
    futures = []
    for url in product_urls:
        future = thread_pool_manager.submit(pool_name, _crawl_single_product, url)
        futures.append(future)
    
    # Collect results and save to database
    successful_count = 0
    failed_count = 0
    
    for future in as_completed(futures):
        try:
            product_url, product_data = future.result()
            results[product_url] = product_data
            
            if product_data:
                # Save to filter_pool
                db_session = SessionLocal()
                try:
                    existing = db_session.query(FilterPool).filter(
                        FilterPool.product_url == product_url
                    ).first()
                    
                    if existing:
                        # Update existing
                        for key, value in product_data.items():
                            if key != 'product_url' and hasattr(existing, key):
                                setattr(existing, key, value)
                        existing.crawled_at = datetime.utcnow()
                    else:
                        # Create new
                        filter_pool_item = FilterPool(
                            product_url=product_data['product_url'],
                            product_name=product_data.get('product_name'),
                            thumbnail_image=product_data.get('thumbnail_image'),
                            brand=product_data.get('brand'),
                            shop_name=product_data.get('shop_name'),
                            price=product_data.get('price'),
                            listed_at=product_data.get('listed_at'),
                            stock=product_data.get('stock'),
                            review_count=product_data.get('review_count'),
                            latest_review_at=product_data.get('latest_review_at'),
                            earliest_review_at=product_data.get('earliest_review_at'),
                            shop_rank=product_data.get('shop_rank'),
                            category_rank=product_data.get('category_rank'),
                            ad_rank=product_data.get('ad_rank'),
                            is_fbe=product_data.get('is_fbe', False),
                            competitor_count=product_data.get('competitor_count', 0),
                            crawled_at=datetime.utcnow()
                        )
                        db_session.add(filter_pool_item)
                    
                    db_session.commit()
                    successful_count += 1
                except Exception as e:
                    logger.error(f"Error saving product {product_url} to database: {e}")
                    db_session.rollback()
                    failed_count += 1
                finally:
                    db_session.close()
            else:
                failed_count += 1
                
        except Exception as e:
            logger.error(f"Error getting result from product crawl: {e}")
            failed_count += 1
    
    logger.info(
        f"Batch product crawl completed: {successful_count} successful, "
        f"{failed_count} failed out of {len(product_urls)} total"
    )
    
    return results

def batch_crawl_keyword_links(
    keyword_id: int,
    db: Session,
    max_workers: Optional[int] = None,
    batch_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Batch crawl all product links for a keyword
    
    Args:
        keyword_id: Keyword ID
        db: Database session
        max_workers: Maximum number of worker threads
        batch_size: Number of URLs to process in each batch (for memory management)
        
    Returns:
        Dictionary with crawl statistics
    """
    # Get all links for keyword
    links = db.query(KeywordLink).filter(
        KeywordLink.keyword_id == keyword_id,
        KeywordLink.status == "active"
    ).all()
    
    if not links:
        return {
            "keyword_id": keyword_id,
            "total_links": 0,
            "crawled": 0,
            "failed": 0
        }
    
    product_urls = [link.product_url for link in links]
    
    # Use batch processing if batch_size is specified
    if batch_size:
        all_results = {}
        for i in range(0, len(product_urls), batch_size):
            batch = product_urls[i:i + batch_size]
            batch_results = batch_crawl_products(batch, db, max_workers=max_workers)
            all_results.update(batch_results)
        results = all_results
    else:
        results = batch_crawl_products(product_urls, db, max_workers=max_workers)
    
    # Calculate statistics
    successful = sum(1 for v in results.values() if v is not None)
    failed = len(results) - successful
    
    return {
        "keyword_id": keyword_id,
        "total_links": len(product_urls),
        "crawled": successful,
        "failed": failed
    }


"""基础信息提取器

从产品详情页提取基础信息（固定字段）
"""
import logging
from typing import Dict, Any, Optional
from urllib.parse import urljoin
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)

class BaseInfoExtractor:
    """从产品详情页提取基础信息的提取器"""
    
    BASE_URL = "https://www.emag.ro"
    
    def __init__(self, base_url: str = BASE_URL):
        """
        初始化基础信息提取器
        
        Args:
            base_url: 基础URL，用于解析相对链接
        """
        self.base_url = base_url
    
    def extract(self, page: Page, product_url: str) -> Dict[str, Any]:
        """
        从产品详情页提取基础信息
        
        Args:
            page: Playwright Page 对象
            product_url: 产品URL（用于参考）
            
        Returns:
            包含基础信息的字典，字段包括：
            - title: 产品标题
            - thumbnail_image: 缩略图URL
            - brand: 品牌名称
            - shop_name: 店铺名称
            - seller_url: 店铺链接
            - category_url: 类目链接
        """
        result: Dict[str, Any] = {}
        
        try:
            # 等待页面加载：超时或验证码时抛出异常，不继续执行
            # #region agent log
            import json as _json_base_wait, time as _time_base_wait
            _wait_start = _time_base_wait.time()
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_base_wait.dumps({
                        "timestamp": int(_time_base_wait.time() * 1000),
                        "location": "base_info_extractor.py:before_wait_domcontentloaded",
                        "message": "准备等待domcontentloaded+元素",
                        "data": {
                            "url": product_url,
                            "timeout_ms": 30000
                        },
                        "hypothesisId": "H2",
                        "runId": "timeout-debug"
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            try:
                # 使用 domcontentloaded + 关键元素等待替代 networkidle，避免长轮询/广告脚本导致超时
                page.wait_for_load_state('domcontentloaded', timeout=30000)
                # 等待产品标题元素出现，确保核心内容已加载
                try:
                    page.wait_for_selector('.page-title, h1[class*="title"], .product-title, [class*="product-name"]', timeout=10000)
                except Exception:
                    pass  # 元素等待失败不中断，DOM已加载即可
                
                # #region agent log
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_base_wait.dumps({
                            "timestamp": int(_time_base_wait.time() * 1000),
                            "location": "base_info_extractor.py:after_wait_domcontentloaded",
                            "message": "domcontentloaded+元素等待完成",
                            "data": {
                                "url": product_url,
                                "elapsed_ms": int((_time_base_wait.time() - _wait_start) * 1000)
                            },
                            "hypothesisId": "H2",
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
                        _f.write(_json_base_wait.dumps({
                            "timestamp": int(_time_base_wait.time() * 1000),
                            "location": "base_info_extractor.py:wait_domcontentloaded_timeout",
                            "message": "domcontentloaded等待超时",
                            "data": {
                                "url": product_url,
                                "error_type": type(e).__name__,
                                "error_message": str(e)[:300],
                                "elapsed_ms": int((_time_base_wait.time() - _wait_start) * 1000),
                                "timeout_ms": 30000
                            },
                            "hypothesisId": "H2",
                            "runId": "networkidle-opt"
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                logger.error(f"BaseInfoExtractor wait_for_load_state('domcontentloaded') 超时: {e}")
                raise
            
            # 检查验证码
            try:
                from app.utils.captcha_handler import captcha_handler
                page_content = page.content()
                if captcha_handler.detect_captcha(page_content, page_content):
                    logger.warning(f"[基础信息提取] 检测到验证码")
                    raise ValueError("Captcha detected during base info extraction")
            except ValueError:
                # 验证码异常直接抛出
                raise
            except Exception as captcha_check_err:
                # 其他异常记录但不中断
                logger.debug(f"验证码检测异常（可忽略）: {captcha_check_err}")
            
            # 提取产品标题
            result['title'] = self._extract_title(page)
            
            # 提取缩略图
            result['thumbnail_image'] = self._extract_thumbnail_image(page)
            
            # 提取品牌
            result['brand'] = self._extract_brand(page)
            
            # 提取店铺信息
            shop_info = self._extract_shop_info(page)
            result['shop_name'] = shop_info.get('shop_name')
            result['seller_url'] = shop_info.get('seller_url')
            
            # 提取类目链接
            result['category_url'] = self._extract_category_url(page)
            
            logger.debug(f"Extracted base info: title={result.get('title')}, shop={result.get('shop_name')}")
            
        except (PlaywrightTimeoutError, ValueError) as e:
            # 超时或验证码：抛出异常，不继续执行
            logger.error(f"Error extracting base info (timeout/captcha): {e}")
            raise
        except Exception as e:
            # 其他异常：也抛出，确保数据完整性
            logger.error(f"Error extracting base info: {e}")
            raise
        
        return result
    
    def _extract_title(self, page: Page) -> Optional[str]:
        """提取产品标题"""
        try:
            # 尝试多个选择器
            selectors = [
                'h1',
                'h1.page-title',
                '.page-title h1',
                '[itemprop="name"]',
                '.product-title',
            ]
            
            for selector in selectors:
                try:
                    title_elem = page.locator(selector).first
                    if title_elem.count() > 0:
                        title = title_elem.inner_text()
                        if title and title.strip():
                            return title.strip()
                except Exception:
                    continue
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract title: {e}")
            return None
    
    def _extract_thumbnail_image(self, page: Page) -> Optional[str]:
        """提取缩略图URL"""
        try:
            # 尝试多个选择器
            selectors = [
                'img.product-gallery-image',
                '.product-gallery img',
                '[itemprop="image"]',
                '.product-images img',
                '.product-photo img',
                'img.product-image',
            ]
            
            for selector in selectors:
                try:
                    img_elem = page.locator(selector).first
                    if img_elem.count() > 0:
                        img_src = img_elem.get_attribute('src') or img_elem.get_attribute('data-src')
                        if img_src:
                            return self._normalize_image_url(img_src)
                except Exception:
                    continue
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract thumbnail image: {e}")
            return None
    
    def _extract_brand(self, page: Page) -> Optional[str]:
        """提取品牌名称"""
        try:
            # 尝试多个选择器
            selectors = [
                '[itemprop="brand"]',
                '.disclaimer-section [itemprop="brand"]',
                '.product-brand',
                '.brand-name',
            ]
            
            for selector in selectors:
                try:
                    brand_elem = page.locator(selector).first
                    if brand_elem.count() > 0:
                        brand = brand_elem.inner_text()
                        if brand and brand.strip():
                            return brand.strip()
                except Exception:
                    continue
            
            # 尝试从disclaimer-section中提取
            try:
                disclaimer = page.locator('.disclaimer-section').first
                if disclaimer.count() > 0:
                    disclaimer_text = disclaimer.inner_text()
                    # 查找品牌相关的文本
                    # 这里可以根据实际页面结构进行更精确的提取
                    # 例如：从 "Brand: XYZ" 或 "Marca: XYZ" 中提取
                    if disclaimer_text:
                        # 简单的文本提取逻辑，可根据实际情况优化
                        lines = disclaimer_text.split('\n')
                        for line in lines:
                            line = line.strip()
                            if 'brand' in line.lower() or 'marca' in line.lower():
                                # 提取品牌名称（简单实现）
                                parts = line.split(':')
                                if len(parts) > 1:
                                    return parts[-1].strip()
            except Exception:
                pass
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract brand: {e}")
            return None
    
    def _extract_shop_info(self, page: Page) -> Dict[str, Optional[str]]:
        """提取店铺信息（店铺名称和链接）"""
        result = {'shop_name': None, 'seller_url': None}
        
        try:
            # 查找店铺链接
            shop_link = page.locator('a.dotted-link').first
            if shop_link.count() > 0:
                shop_name = shop_link.inner_text()
                seller_url = shop_link.get_attribute('href')
                
                if shop_name and shop_name.strip():
                    result['shop_name'] = shop_name.strip()
                
                if seller_url:
                    result['seller_url'] = self._normalize_url(seller_url)
        except Exception as e:
            logger.debug(f"Failed to extract shop info: {e}")
        
        return result
    
    def _extract_category_url(self, page: Page) -> Optional[str]:
        """提取类目链接"""
        try:
            # 查找面包屑导航中的类目链接
            # .breadcrumb-inner li:nth-last-child(3) a 选择倒数第三个面包屑项
            category_link = page.locator('.breadcrumb-inner li:nth-last-child(3) a').first
            if category_link.count() > 0:
                category_url = category_link.get_attribute('href')
                if category_url:
                    return self._normalize_url(category_url)
            
            # 备用选择器：查找所有面包屑链接
            breadcrumb_links = page.locator('.breadcrumb-inner a').all()
            if len(breadcrumb_links) >= 3:
                # 取倒数第三个链接作为类目链接
                category_link = breadcrumb_links[-3]
                category_url = category_link.get_attribute('href')
                if category_url:
                    return self._normalize_url(category_url)
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract category URL: {e}")
            return None
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """规范化URL"""
        if not url:
            return None
        
        try:
            # 处理相对URL
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
    
    def _normalize_image_url(self, img_url: str) -> Optional[str]:
        """规范化图片URL"""
        if not img_url:
            return None
        
        try:
            if img_url.startswith('//'):
                return 'https:' + img_url
            elif img_url.startswith('/'):
                return urljoin(self.base_url, img_url)
            elif img_url.startswith('http://') or img_url.startswith('https://'):
                return img_url
            
            return None
        except Exception:
            return None


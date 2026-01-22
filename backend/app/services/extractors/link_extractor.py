"""产品链接提取器

从搜索结果页面提取产品链接、缩略图和价格
"""
import logging
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from playwright.sync_api import Page

logger = logging.getLogger(__name__)

class LinkExtractor:
    """从搜索结果页提取产品链接的提取器"""
    
    BASE_URL = "https://www.emag.ro"
    
    def __init__(self, base_url: str = BASE_URL):
        """
        初始化链接提取器
        
        Args:
            base_url: 基础URL，用于解析相对链接
        """
        self.base_url = base_url
        # 控制缩略图调试日志数量，避免日志过多
        self._thumb_debug_count = 0
    
    def extract_from_search_page(self, page: Page) -> List[Dict[str, Any]]:
        """
        从搜索结果页面提取产品链接、缩略图和价格
        
        Args:
            page: Playwright Page 对象
            
        Returns:
            产品信息列表，每个包含 url, pnk_code, thumbnail_image, price
        """
        products: List[Dict[str, Any]] = []
        processed_urls: set = set()
        
        try:
            # 等待产品列表加载
            page.wait_for_selector('a[href*="/pd/"]', timeout=10000)
            
            # 使用多种选择器查找产品链接
            selectors = [
                'a[href*="/pd/"]',
                '.card-item a[href*="/pd/"]',
                '.product-item a[href*="/pd/"]',
                'a.card-v2-title[href*="/pd/"]',
            ]
            
            all_links = []
            for selector in selectors:
                try:
                    links = page.locator(selector).all()
                    for link in links:
                        href = link.get_attribute('href')
                        if href and '/pd/' in href:
                            # 避免重复
                            normalized_url = self._normalize_url(href)
                            if normalized_url and normalized_url not in processed_urls:
                                all_links.append(link)
                                processed_urls.add(normalized_url)
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            # 提取每个产品的详细信息
            for link in all_links[:50]:  # 限制最多50个产品，避免性能问题
                try:
                    product_info = self._extract_product_info(link)
                    if product_info:
                        products.append(product_info)
                except Exception as e:
                    logger.debug(f"Failed to extract product info: {e}")
                    continue
            
            logger.info(f"Extracted {len(products)} products from search page")
            
        except Exception as e:
            logger.warning(f"Error extracting links from search page: {e}")
        
        return products
    
    def _extract_product_info(self, link_locator) -> Optional[Dict[str, Any]]:
        """
        从链接元素提取产品信息
        
        Args:
            link_locator: Playwright Locator 对象
            
        Returns:
            产品信息字典，包含 url, pnk_code, thumbnail_image, price
        """
        try:
            # 获取URL
            href = link_locator.get_attribute('href')
            if not href:
                return None
            
            url = self._normalize_url(href)
            if not url:
                return None
            
            pnk_code = self._extract_pnk_code_from_url(url)

            # 获取缩略图（尝试从多层祖先元素中查找）
            thumbnail_image = None
            try:
                # 尝试向上查找多层祖先元素（最多5层）
                current = link_locator
                thumb_debug_info = {"attempts": [], "found": False}
                for level in range(5):
                    try:
                        parent = current.locator('..').first
                        if parent.count() == 0:
                            break
                        
                        # 在当前父元素中查找图片
                        img = parent.locator('img').first
                        if img.count() > 0:
                            # 尝试多种属性获取图片URL
                            img_src = (
                                img.get_attribute('src') or 
                                img.get_attribute('data-src') or 
                                img.get_attribute('data-lazy-src') or
                                img.get_attribute('data-original')
                            )
                            thumb_debug_info["attempts"].append({
                                "level": level,
                                "img_src": img_src[:120] if img_src else None
                            })
                            if img_src and not img_src.startswith('data:'):  # 排除 data URI
                                normalized_img = self._normalize_image_url(img_src)
                                if normalized_img and not self._is_placeholder_image(normalized_img):
                                    thumbnail_image = normalized_img
                                    thumb_debug_info["found"] = True
                                    break
                        
                        current = parent
                    except Exception:
                        break
                
            except Exception:
                pass
            
            # 获取价格
            price = None
            price_debug_info = {"attempts": [], "found": False}
            try:
                # 价格选择器列表
                price_selectors = [
                    '.product-new-price',
                    'p.product-new-price',
                    '.price',
                    '[itemprop="price"]',
                    '.product-price',
                ]
                
                # 尝试向上查找多层祖先元素（最多5层）
                current = link_locator
                for level in range(5):
                    try:
                        # 获取父元素
                        parent = current.locator('..').first
                        if parent.count() == 0:
                            break
                        
                        # 在当前父元素中查找价格
                        for price_selector in price_selectors:
                            try:
                                price_elem = parent.locator(price_selector).first
                                if price_elem.count() > 0:
                                    price_text = price_elem.inner_text()
                                    price_debug_info["attempts"].append({
                                        "level": level,
                                        "selector": price_selector,
                                        "price_text": price_text[:100] if price_text else None
                                    })
                                    if price_text:
                                        price = self._parse_price(price_text)
                                        if price:
                                            price_debug_info["found"] = True
                                            price_debug_info["final_price"] = price
                                            break
                            except Exception:
                                continue
                        
                        if price:
                            break
                        
                        current = parent
                    except Exception:
                        break
                
                
            except Exception:
                pass
            
            return {
                'url': url,
                'pnk_code': pnk_code,
                'thumbnail_image': thumbnail_image,
                'price': price,
            }
            
        except Exception as e:
            logger.debug(f"Failed to extract product info: {e}")
            return None
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """
        规范化URL
        
        Args:
            url: 原始URL
            
        Returns:
            规范化后的URL或None
        """
        if not url:
            return None
        
        try:
            # 移除查询参数和锚点
            url = url.split('?')[0].split('#')[0]
            
            # 处理相对URL
            if url.startswith('/'):
                full_url = urljoin(self.base_url, url)
            elif url.startswith('http://') or url.startswith('https://'):
                if 'emag.ro' not in url:
                    return None
                full_url = url
            else:
                full_url = urljoin(self.base_url, '/' + url)
            
            # 确保使用https
            if full_url.startswith('http://www.emag.ro'):
                full_url = full_url.replace('http://', 'https://', 1)
            elif full_url.startswith('http://emag.ro'):
                full_url = full_url.replace('http://', 'https://', 1)
            
            # 验证URL格式
            if not full_url.startswith('https://www.emag.ro') and not full_url.startswith('https://emag.ro'):
                return None
            
            if '/pd/' not in full_url:
                return None
            
            return full_url
            
        except Exception as e:
            logger.debug(f"URL normalization failed for {url}: {e}")
            return None

    def _extract_pnk_code_from_url(self, url: str) -> Optional[str]:
        """
        从产品URL中提取PNK_CODE
        """
        if not url:
            return None

        match = re.search(r"/pd/([^/?#]+)", url)
        if not match:
            return None

        return match.group(1)
    
    def _normalize_image_url(self, img_url: str) -> Optional[str]:
        """
        规范化图片URL
        
        Args:
            img_url: 原始图片URL
            
        Returns:
            规范化后的图片URL
        """
        if not img_url:
            return None
        
        try:
            # 处理相对URL
            if img_url.startswith('//'):
                return 'https:' + img_url
            elif img_url.startswith('/'):
                return urljoin(self.base_url, img_url)
            elif img_url.startswith('http://') or img_url.startswith('https://'):
                return img_url
            
            return None
        except Exception:
            return None
    
    def _parse_price(self, price_text: str) -> Optional[float]:
        """
        解析价格文本为浮点数
        
        Args:
            price_text: 价格文本（如 "1.234,56 Lei" 或 "1234.56"）
            
        Returns:
            价格浮点数或None
        """
        if not price_text:
            return None
        
        try:
            # 移除货币符号和多余空格
            price_text = re.sub(r'[^\d,.\s]', '', price_text.strip())
            # 移除空格
            price_text = price_text.replace(' ', '')
            
            # 处理罗马尼亚数字格式（1.234,56）
            if ',' in price_text and '.' in price_text:
                # 格式：1.234,56（千位分隔符是点，小数点是逗号）
                price_text = price_text.replace('.', '').replace(',', '.')
            elif ',' in price_text:
                # 可能是小数，也可能是千位分隔符
                # 假设如果逗号前超过3位数字，则是千位分隔符
                parts = price_text.split(',')
                if len(parts) == 2 and len(parts[0]) > 3:
                    price_text = ''.join(parts)
                else:
                    price_text = price_text.replace(',', '.')
            
            return float(price_text)
        except (ValueError, AttributeError):
            return None

    def _is_placeholder_image(self, img_url: str) -> bool:
        """判断是否为占位图或过滤器图片"""
        if not img_url:
            return True
        lower_url = img_url.lower()
        if 'layout/ro/images/filters/' in lower_url:
            return True
        if 'layout/ro/static-upload/' in lower_url:
            return True
        if 'res_db66567abaaaa58862d48d05705d406f' in lower_url:
            return True
        if 'user-wallet-info-budget.png' in lower_url:
            return True
        return False


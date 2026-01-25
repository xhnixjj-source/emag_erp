"""动态数据提取器

从产品详情页提取动态数据（可变字段）
"""
import json
import logging
import re
import time
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin, urlparse
from datetime import datetime
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)


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
            # 等待页面加载
            page.wait_for_load_state('networkidle', timeout=30000)
            
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
            
        except Exception as e:
            logger.warning(f"Error extracting dynamic basic fields: {e}")
        
        return result
    
    def extract_rankings(
        self,
        page: Page,
        product_url: str,
        context=None  # BrowserContext，用于遍历页面
    ) -> Dict[str, Any]:
        """
        提取排名字段（需要遍历多个页面）
        
        Args:
            page: Playwright Page 对象（产品详情页）
            product_url: 产品URL（用于提取data-availability-id）
            context: BrowserContext对象（用于打开新页面）
            
        Returns:
            包含排名字段的字典：
            - ad_category_rank: 广告类目排名
            - category_rank: 类目排名
            - store_rank: 店铺排名
        """
        result: Dict[str, Any] = {}
        
        if not context:
            logger.warning("No context provided, skipping rankings extraction")
            return result
        
        try:
            # 从产品URL中提取data-availability-id
            product_id = self._extract_product_id_from_url(product_url)
            if not product_id:
                logger.warning(f"Could not extract product ID from URL: {product_url}")
                return result
            
            # 提取类目URL（用于排名计算）
            category_url = self._extract_category_url_from_page(page)
            if not category_url:
                logger.warning("Could not extract category URL")
                return result
            brand_category_url = None
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
            
            # 提取店铺URL（用于排名计算）
            shop_url = self._extract_shop_url_from_page(page)
            if not shop_url:
                logger.warning("Could not extract shop URL")
                return result

            # #region agent log
            # 记录本次排名计算的关键上下文，方便对比不同商品（例如同类目下两个商品排名不一致问题）
            try:
                import json
                import time
                from app.config import get_debug_log_path
                with open(get_debug_log_path(), 'a', encoding='utf-8') as f:
                    log_data = {
                        "location": "dynamic_data_extractor.py:extract_rankings:context",
                        "message": "Rankings extraction context",
                        "data": {
                            "product_url": product_url,
                            "product_id": product_id,
                            "category_url": category_url,
                            "brand_category_url": brand_category_url,
                            "shop_url": shop_url
                        },
                        "timestamp": int(time.time() * 1000),
                        "sessionId": "debug-session",
                        "runId": "rank-compare",
                        "hypothesisId": "H_CTX"
                    }
                    f.write(json.dumps(log_data, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            # 提取类目排名（遍历类目页前3页，仅在总类目下查找）
            category_ranks = self._extract_category_rank(context, category_url, product_id, max_pages=3)
            result['category_rank'] = category_ranks.get('category_rank')
            result['ad_category_rank'] = category_ranks.get('ad_category_rank')

            # 提取店铺排名（遍历店铺商品列表前2页）
            result['store_rank'] = self._extract_store_rank(context, shop_url, product_id, max_pages=2)
            
            logger.debug(f"Extracted rankings: category_rank={result.get('category_rank')}, "
                        f"ad_category_rank={result.get('ad_category_rank')}, store_rank={result.get('store_rank')}")
            
        except Exception as e:
            logger.warning(f"Error extracting rankings: {e}")
        
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
    
    def _extract_category_rank(
        self,
        context,
        category_url: str,
        product_id: str,
        max_pages: int = 3
    ) -> Dict[str, Optional[int]]:
        """提取类目排名（遍历类目页）"""
        result = {'category_rank': None, 'ad_category_rank': None}
        
        # #region agent log
        import json
        import time
        from app.config import get_debug_log_path
        try:
            with open(get_debug_log_path(), 'a', encoding='utf-8') as f:
                log_data = {
                    "location": "dynamic_data_extractor.py:_extract_category_rank:entry",
                    "message": "Starting category rank extraction",
                    "data": {"category_url": category_url, "product_id": product_id, "max_pages": max_pages},
                    "timestamp": int(time.time() * 1000),
                    "sessionId": "debug-session",
                    "runId": "rank-debug",
                    "hypothesisId": "H40"
                }
                f.write(json.dumps(log_data, ensure_ascii=False) + '\n')
        except:
            pass
        # #endregion
        
        try:
            page_size_for_rank = 60  # 每页固定60个商品
            def _build_page_urls(base_url: str, page_num: int) -> List[str]:
                urls: List[str] = []
                try:
                    parsed = urlparse(base_url)
                    path = parsed.path or ""
                    query = parsed.query or ""
                    # 1) 兼容 /p{n}/c 形式
                    match = re.search(r'/p\d+/c$', path)
                    if match:
                        new_path = re.sub(r'/p\d+/c$', f'/p{page_num}/c', path)
                        urls.append(parsed._replace(path=new_path).geturl())
                    # 2) 兼容 /c 形式 -> /p{n}/c
                    if path.endswith('/c'):
                        base_path = path[:-2]
                        urls.append(parsed._replace(path=f'{base_path}/p{page_num}/c', query="").geturl())
                    # 3) 兼容 ?p= 形式（保留原查询参数）
                    if query:
                        urls.append(f"{base_url}&p={page_num}")
                    else:
                        urls.append(f"{base_url}?p={page_num}")
                except Exception:
                    urls.append(f"{base_url}?p={page_num}")
                # 去重保持顺序
                deduped = []
                for u in urls:
                    if u not in deduped:
                        deduped.append(u)
                return deduped
            for page_num in range(1, max_pages + 1):
                page_urls = _build_page_urls(category_url, page_num)
                for page_url in page_urls:
                    # 打开新页面
                    category_page = context.new_page()
                    # 使用 domcontentloaded 而非 networkidle，避免等待广告/分析脚本导致超时
                    category_page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
                    
                    # 查找所有产品卡片（使用 .card-item 确保获取正确元素）
                    products = category_page.locator('.card-item[data-availability-id]').all()
                    
                    # #region agent log
                    import json
                    import time
                    from app.config import get_debug_log_path
                    try:
                        # 获取前10个卡片的availability_id和href_code样本
                        sample_cards = []
                        for idx, p in enumerate(products[:10]):
                            try:
                                av_id = p.get_attribute('data-availability-id')
                                href = p.locator('a[href*="/pd/"]').first.get_attribute('href') or ""
                                m = re.search(r'/pd/([^/]+)', href)
                                code = m.group(1) if m else None
                                sample_cards.append({"i": idx+1, "avail_id": av_id, "code": code, "is_target": code == product_id})
                            except:
                                pass
                        with open(get_debug_log_path(), 'a', encoding='utf-8') as f:
                            log_data = {
                                "location": "dynamic_data_extractor.py:_extract_category_rank:page_loaded",
                                "message": "Category page loaded with card samples",
                                "data": {
                                    "page_url": page_url,
                                    "page_num": page_num,
                                    "products_count": len(products),
                                    "product_id": product_id,
                                    "sample_cards": sample_cards
                                },
                                "timestamp": int(time.time() * 1000),
                                "sessionId": "debug-session",
                                "runId": "rank-fix",
                                "hypothesisId": "H40"
                            }
                            f.write(json.dumps(log_data, ensure_ascii=False) + '\n')
                    except:
                        pass
                    # #endregion
                    
                    for i, product in enumerate(products, start=1):
                        availability_id = product.get_attribute('data-availability-id')
                        href_code = None
                        try:
                            href = product.locator('a[href*="/pd/"]').first.get_attribute('href')
                            if href:
                                match = re.search(r'/pd/([^/]+)', href)
                                href_code = match.group(1) if match else None
                        except Exception:
                            href_code = None
                        is_match = href_code and href_code == product_id
                        if is_match:
                            data_position = None
                            try:
                                data_position = product.get_attribute('data-position')
                            except Exception:
                                data_position = None
                            
                            # 计算排名：(页码-1) * 每页数量 + 当前位置
                            computed_rank = i  # 默认使用枚举序号
                            if data_position:
                                try:
                                    pos_int = int(data_position)
                                    if pos_int > 0:
                                        computed_rank = (page_num - 1) * page_size_for_rank + pos_int
                                except Exception:
                                    computed_rank = (page_num - 1) * page_size_for_rank + i
                            else:
                                computed_rank = (page_num - 1) * page_size_for_rank + i
                            
                            # 使用 data-availability-id 判断广告
                            is_ad_by_availability = availability_id == "0"
                            
                            # #region agent log
                            try:
                                with open(get_debug_log_path(), 'a', encoding='utf-8') as f:
                                    log_data = {
                                        "location": "dynamic_data_extractor.py:_extract_category_rank:match_found",
                                        "message": "Product match found",
                                        "data": {
                                            "product_id": product_id,
                                            "href_code": href_code,
                                            "availability_id": availability_id,
                                            "is_ad": is_ad_by_availability,
                                            "data_position": data_position,
                                            "computed_rank": computed_rank,
                                            "page_num": page_num,
                                            "enumerate_i": i
                                        },
                                        "timestamp": int(time.time() * 1000),
                                        "sessionId": "debug-session",
                                        "runId": "rank-fix",
                                        "hypothesisId": "H41"
                                    }
                                    f.write(json.dumps(log_data, ensure_ascii=False) + '\n')
                            except:
                                pass
                            # #endregion
                            
                            if is_ad_by_availability:
                                result['ad_category_rank'] = computed_rank
                            else:
                                result['category_rank'] = computed_rank

                            category_page.close()
                            return result
                    
                    # fallback: 通过卡片链接匹配 /pd/ 码，同时获取 availability_id 判断广告
                    try:
                        card_items = category_page.locator('.card-item[data-availability-id]').all()
                    except Exception:
                        card_items = []
                    if card_items:
                        seen_codes = set()
                        rank_offset = 0
                        for card in card_items:
                            try:
                                href = card.locator('a[href*="/pd/"]').first.get_attribute('href')
                                if not href:
                                    continue
                                match = re.search(r'/pd/([^/]+)', href)
                                href_code = match.group(1) if match else None
                            except Exception:
                                href_code = None
                            if not href_code or href_code in seen_codes:
                                continue
                            seen_codes.add(href_code)
                            rank_offset += 1
                            if href_code == product_id:
                                # 使用 data-availability-id 判断广告
                                card_availability_id = None
                                try:
                                    card_availability_id = card.get_attribute('data-availability-id')
                                except Exception:
                                    pass
                                is_ad_by_availability = card_availability_id == "0"
                                card_rank = (page_num - 1) * page_size_for_rank + rank_offset
                                if is_ad_by_availability:
                                    result['ad_category_rank'] = card_rank
                                else:
                                    result['category_rank'] = card_rank
                                category_page.close()
                                return result
                    
                    category_page.close()
            
        except Exception as e:
            logger.warning(f"Error extracting category rank: {e}")
        
        # 如果在前 max_pages 页中都没有找到商品，则统一记为 200（类目排名和广告排名都是 200）
        if result.get('category_rank') is None and result.get('ad_category_rank') is None:
            result['category_rank'] = 200
            result['ad_category_rank'] = 200

        # #region agent log
        import json
        import time
        from app.config import get_debug_log_path
        try:
            with open(get_debug_log_path(), 'a', encoding='utf-8') as f:
                log_data = {
                    "location": "dynamic_data_extractor.py:_extract_category_rank:result",
                    "message": "Category rank extraction completed",
                    "data": {
                        "product_id": product_id,
                        "category_url": category_url,
                        "category_rank": result.get('category_rank'),
                        "ad_category_rank": result.get('ad_category_rank')
                    },
                    "timestamp": int(time.time() * 1000),
                    "sessionId": "debug-session",
                    "runId": "rank-fix",
                    "hypothesisId": "H40"
                }
                f.write(json.dumps(log_data, ensure_ascii=False) + '\n')
        except:
            pass
        # #endregion
        
        return result
    
    def _extract_store_rank(
        self,
        context,
        shop_url: str,
        product_id: str,
        max_pages: int = 2
    ) -> Optional[int]:
        """提取店铺排名（遍历店铺商品列表）"""
        try:
            rank = 1
            for page_num in range(1, max_pages + 1):
                # 构建店铺页URL（添加页码）
                if '?' in shop_url:
                    page_url = f"{shop_url}&p={page_num}"
                else:
                    page_url = f"{shop_url}?p={page_num}"
                
                try:
                    # 打开新页面
                    shop_page = context.new_page()
                    # 使用 domcontentloaded 而非 networkidle，避免等待广告/分析脚本导致超时
                    shop_page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
                    
                    # 查找所有产品
                    products = shop_page.locator('[data-availability-id]').all()
                    
                    for product in products:
                        availability_id = product.get_attribute('data-availability-id')
                        href_code = None
                        try:
                            href = product.locator('a[href*="/pd/"]').first.get_attribute('href')
                            if href:
                                match = re.search(r'/pd/([^/]+)', href)
                                href_code = match.group(1) if match else None
                        except Exception:
                            href_code = None
                        if availability_id == product_id or (href_code and href_code == product_id):
                            shop_page.close()
                            return rank
                        rank += 1
                    
                    shop_page.close()
                except Exception as e:
                    logger.debug(f"Failed to crawl shop page {page_num}: {e}")
                    continue
            
        except Exception as e:
            logger.warning(f"Error extracting store rank: {e}")
        
        return None
    
    def _extract_product_id_from_url(self, product_url: str) -> Optional[str]:
        """从产品URL中提取product ID（data-availability-id）"""
        try:
            # eMAG产品URL格式：https://www.emag.ro/product-name/pd/XXXXX/
            # 或者可以从页面中提取data-availability-id
            match = re.search(r'/pd/([^/]+)', product_url)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None
    
    def _extract_category_url_from_page(self, page: Page) -> Optional[str]:
        """从页面中提取类目URL"""
        try:
            # 按需求：取面包屑倒数第三个<li>中的链接
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
    
    def _extract_shop_url_from_page(self, page: Page) -> Optional[str]:
        """从页面中提取店铺URL"""
        try:
            shop_link = page.locator('a.dotted-link').first
            if shop_link.count() > 0:
                shop_url = shop_link.get_attribute('href')
                if shop_url:
                    return self._normalize_url(shop_url)
            return None
        except Exception:
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


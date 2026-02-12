"""Proxy management utility"""
import random
import logging
import requests
import time
import json
import threading
from typing import Optional, List, Dict
from app.config import config, get_debug_log_path

logger = logging.getLogger(__name__)


class ProxyManager:
    """
    Proxy manager for rotating IP addresses with API support
    
    功能说明：
    - 启动时从 lunaproxy API 获取动态代理 IP 列表
    - 定期刷新 IP 池（默认 60 秒），保持 IP 池新鲜
    - 自动清理过期和失败的代理
    - 支持代理轮询和随机选择
    """
    
    def __init__(self):
        self.proxies: List[str] = []
        self.failed_proxies: set = set()  # 追踪失败的代理
        self.occupied_proxies: set = set()  # 追踪已占用的代理（独占式分配）
        self.current_index: int = 0
        self.enabled: bool = config.PROXY_ENABLED
        self.last_api_fetch_time: Optional[float] = None
        self.api_fetch_interval: int = getattr(config, 'PROXY_API_FETCH_INTERVAL', 60)
        
        # 代理获取时间戳（用于过期清理）
        self.proxy_timestamps: Dict[str, float] = {}
        
        # 后台刷新线程
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_refresh: bool = False
        self._lock = threading.Lock()
        
        # 初始化加载代理
        if self.enabled:
            self._initial_load()
        
        # 启动后台刷新线程
        if self.enabled and config.PROXY_API_URL:
            self._start_refresh_thread()
    
    def _initial_load(self):
        """初始化加载代理 IP"""
        # 加载静态配置的代理列表
        if config.PROXY_LIST:
            self.proxies = [p.strip() for p in config.PROXY_LIST if p.strip()]
            logger.info(f"从配置加载了 {len(self.proxies)} 个静态代理")
        
        # 从 API 获取动态代理
        if config.PROXY_API_URL:
            try:
                api_proxies = self._fetch_proxy_from_api()
                if api_proxies:
                    current_time = time.time()
                    for proxy in api_proxies:
                        if proxy and proxy not in self.proxies:
                            self.proxies.append(proxy)
                            self.proxy_timestamps[proxy] = current_time
                    logger.info(f"从 API 加载了 {len(api_proxies)} 个动态代理")
            except Exception as e:
                logger.warning(f"初始化时从 API 获取代理失败: {e}")
        
        # 去重
        self.proxies = list(dict.fromkeys(self.proxies))
        
        if self.proxies:
            logger.info(f"ProxyManager 初始化完成，共有 {len(self.proxies)} 个代理")
        else:
            logger.warning("没有可用的代理 IP")
    
    def _start_refresh_thread(self):
        """启动后台刷新线程"""
        if self._refresh_thread and self._refresh_thread.is_alive():
            return
        
        self._stop_refresh = False
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            daemon=True,
            name="ProxyRefreshThread"
        )
        self._refresh_thread.start()
        logger.info(f"启动代理刷新线程，间隔 {self.api_fetch_interval} 秒")
    
    def _refresh_loop(self):
        """后台刷新循环"""
        while not self._stop_refresh:
            try:
                # 等待刷新间隔
                time.sleep(self.api_fetch_interval)
                
                if self._stop_refresh:
                    break
                
                # 刷新代理
                self.refresh_proxies_from_api()
                
                # 清理过期代理
                self._cleanup_expired_proxies()
                
            except Exception as e:
                logger.error(f"代理刷新循环出错: {e}")
    
    def _cleanup_expired_proxies(self):
        """清理过期的代理"""
        # IP 有效期（从配置获取，转换为秒）
        ip_lifetime_seconds = getattr(config, 'PROXY_API_IP_SI', 6) * 60
        current_time = time.time()
        
        with self._lock:
            expired_proxies = []
            for proxy, timestamp in list(self.proxy_timestamps.items()):
                if current_time - timestamp > ip_lifetime_seconds:
                    expired_proxies.append(proxy)
            
            for proxy in expired_proxies:
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                self.proxy_timestamps.pop(proxy, None)
                self.failed_proxies.discard(proxy)
            
            if expired_proxies:
                logger.info(f"清理了 {len(expired_proxies)} 个过期代理")
    
    def stop(self):
        """停止后台刷新线程"""
        self._stop_refresh = True
        if self._refresh_thread and self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=5)
            logger.info("代理刷新线程已停止")
    
    def _fetch_proxy_from_api(self) -> List[str]:
        """
        从 API 获取代理 IP 列表
        
        支持 LunaProxy GET 接口：
        https://tq.lunaproxy.com/get_dynamic_ip?neek={user_id}&num={count}&regions={country}&ip_si={ip_si}&sb={sb}
        返回格式：IP:PORT 列表（每行一个）
        
        Returns:
            代理 IP 列表（格式：["ip:port", ...]）
        """
        if not config.PROXY_API_URL:
            return []
        
        try:
            url = config.PROXY_API_URL
            headers = {}
            
            # 检查是否是 LunaProxy Unlocker API（POST 方式）
            if 'unlocker-api.lunaproxy.com' in url or '/request' in url:
                # Unlocker API 不用于获取 IP 列表
                if config.PROXY_API_KEY:
                    headers['Authorization'] = f"Bearer {config.PROXY_API_KEY}"
                    headers['content-type'] = "application/json"
                
                self.last_api_fetch_time = time.time()
                logger.info("检测到 LunaProxy Unlocker API (POST 模式)")
                logger.warning("Unlocker API 不返回 IP 列表，使用 API 端点作为代理标识符")
                
                api_endpoint = url.replace('https://', '').replace('http://', '')
                return [f"api://{api_endpoint}"]
            
            # GET 方式（获取 IP 列表）
            params = {}
            
            # 构建 LunaProxy API 参数（支持 lunaproxy.com 和 lunadataset.com）
            if 'lunaproxy.com' in url or 'lunadataset.com' in url or 'lunaproxy' in url.lower():
                if config.PROXY_API_USER_ID:
                    params['neek'] = config.PROXY_API_USER_ID
                if config.PROXY_API_IP_COUNT:
                    params['num'] = config.PROXY_API_IP_COUNT
                if config.PROXY_API_COUNTRY:
                    params['regions'] = config.PROXY_API_COUNTRY
                if config.PROXY_API_IP_SI:
                    params['ip_si'] = config.PROXY_API_IP_SI
                if config.PROXY_API_SB is not None:
                    params['sb'] = config.PROXY_API_SB
            
            if config.PROXY_API_KEY:
                headers['Authorization'] = f"Bearer {config.PROXY_API_KEY}"
            
            
            
            logger.debug(f"请求代理 API: {url}, params: {params}")
            
            response = requests.get(
                url,
                params=params if params else None,
                headers=headers,
                timeout=config.PROXY_API_TIMEOUT
            )
            
            response.raise_for_status()
            
            
            
            # 首先尝试解析 JSON 响应（检查是否是错误消息）
            try:
                data = response.json()
                
                
                # 检查是否是错误消息（包含 code 和 msg 字段，且 code 不是成功码）
                if isinstance(data, dict) and 'code' in data and 'msg' in data:
                    error_code = data.get('code')
                    error_msg = data.get('msg', '')
                    # 如果 code 不是 0 或 200（常见成功码），则认为是错误消息
                    if error_code != 0 and error_code != 200:
                        logger.warning(f"代理API返回错误: code={error_code}, msg={error_msg}")
                        
                        return []  # 返回空列表，不将错误消息当作代理
                
                # 正常解析代理列表
                if isinstance(data, dict):
                    proxies = data.get('proxies', data.get('data', data.get('list', [])))
                elif isinstance(data, list):
                    proxies = data
                else:
                    proxies = []
                
                proxy_list = [str(p).strip() for p in proxies if p and ':' in str(p) and not str(p).startswith('{')]
                self.last_api_fetch_time = time.time()
                
                if proxy_list:
                    logger.info(f"从 API 获取了 {len(proxy_list)} 个代理 (JSON 格式)")
                    return proxy_list
            except ValueError:
                # 如果不是 JSON，按行分割（纯文本格式）
                pass
            
            # 解析响应 - LunaProxy 返回纯文本，每行一个 IP:PORT
            text_content = response.text.strip()
            if text_content:
                # 检查是否包含JSON格式的错误消息（以 { 开头）
                if text_content.startswith('{') or text_content.startswith('['):
                    try:
                        # 尝试解析为JSON，如果是错误消息则忽略
                        error_data = json.loads(text_content)
                        if isinstance(error_data, dict) and 'code' in error_data and 'msg' in error_data:
                            error_code = error_data.get('code')
                            if error_code != 0 and error_code != 200:
                                logger.warning(f"代理API返回错误（文本格式）: {error_data.get('msg', '')}")
                                return []
                    except (ValueError, json.JSONDecodeError):
                        pass  # 不是JSON，继续处理
                
                proxy_list = [
                    line.strip() for line in text_content.split('\n')
                    if line.strip() and ':' in line.strip() and not line.strip().startswith('{')
                ]
                if proxy_list:
                    self.last_api_fetch_time = time.time()
                    logger.info(f"从 API 获取了 {len(proxy_list)} 个代理")
                    return proxy_list
            
            # 如果没有找到有效代理，返回空列表
            logger.warning(f"API响应中未找到有效代理: {response.text[:100]}")
            return []
                
        except requests.exceptions.Timeout as e:
            logger.error(f"API 请求超时: {e}")
            return []
        except requests.exceptions.ConnectionError as e:
            logger.error(f"API 连接错误: {e}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"API 请求异常: {e}")
            return []
        except Exception as e:
            logger.error(f"获取代理时发生未知错误: {e}", exc_info=True)
            return []
    
    def fetch_proxy_from_api(self) -> List[str]:
        """
        公共方法：从 API 获取代理 IP 列表（带速率限制）
        
        Returns:
            代理 IP 列表
        """
        # 检查是否需要获取（速率限制）
        current_time = time.time()
        if (self.last_api_fetch_time and 
            current_time - self.last_api_fetch_time < self.api_fetch_interval):
            return []
        
        return self._fetch_proxy_from_api()
    
    def validate_proxy(self, proxy_str: str) -> bool:
        """
        验证代理 IP 是否可用
        
        Args:
            proxy_str: 代理 IP 字符串，格式如 "ip:port" 或 "http://ip:port"
            
        Returns:
            True if proxy is valid, False otherwise
        """
        try:
            # 格式化代理 URL
            # 统一使用配置的 PROXY_SCHEME（例如 socks5），确保与实际代理协议一致
            if "://" not in proxy_str:
                test_url = f"{getattr(config, 'PROXY_SCHEME', 'socks5')}://{proxy_str}"
            else:
                test_url = proxy_str
            
            proxy_dict = {"http": test_url, "https": test_url}
            
            # 使用一个简单的测试 URL 验证代理
            test_response = requests.get(
                "http://httpbin.org/ip",
                proxies=proxy_dict,
                timeout=config.PROXY_VALIDATION_TIMEOUT
            )
            
            return test_response.status_code == 200
                
        except Exception as e:
            logger.debug(f"代理验证失败 {proxy_str}: {e}")
            return False
    
    def refresh_proxies_from_api(self) -> int:
        """
        从 API 刷新代理 IP 列表
        
        Returns:
            新增的代理数量
        """
        api_proxies = self._fetch_proxy_from_api()
        if not api_proxies:
            return 0
        
        current_time = time.time()
        new_count = 0
        
        with self._lock:
            for proxy in api_proxies:
                if proxy and proxy not in self.proxies:
                    self.proxies.append(proxy)
                    self.proxy_timestamps[proxy] = current_time
                    new_count += 1
                elif proxy in self.proxies:
                    # 更新已存在代理的时间戳
                    self.proxy_timestamps[proxy] = current_time
        
        if new_count > 0:
            logger.info(f"从 API 刷新代理: 新增 {new_count} 个")
        
        return new_count
    
    def get_proxy(self) -> Optional[dict]:
        """获取下一个代理（轮询方式）"""
        if not self.enabled or not self.proxies:
            return None
        
        # 如果代理池太小，尝试刷新
        if len(self.proxies) < 3 and config.PROXY_API_URL:
            self.refresh_proxies_from_api()
        
        with self._lock:
            # 过滤掉已知失败的代理
            available_proxies = [p for p in self.proxies if p not in self.failed_proxies]
            
            if not available_proxies:
                # 如果没有可用代理，重置失败列表并重试
                logger.warning("没有可用代理，重置失败列表")
                self.failed_proxies.clear()
                available_proxies = self.proxies
            
            if not available_proxies:
                return None
            
            # 选择代理
            proxy_str = available_proxies[self.current_index % len(available_proxies)]
            self.current_index = (self.current_index + 1) % len(available_proxies)
        
        # 格式化代理
        # 统一使用配置的 PROXY_SCHEME（默认 socks5），确保 requests 通过 SOCKS 代理访问
        scheme = getattr(config, "PROXY_SCHEME", "socks5")
        if "://" in proxy_str:
            formatted = proxy_str
        else:
            formatted = f"{scheme}://{proxy_str}"
        return {"http": formatted, "https": formatted}
    
    def get_random_proxy(self) -> Optional[dict]:
        """获取随机代理"""
        if not self.enabled or not self.proxies:
            return None
        
        # 如果代理池太小，尝试刷新
        if len(self.proxies) < 3 and config.PROXY_API_URL:
            self.refresh_proxies_from_api()
        
        with self._lock:
            # 过滤掉已知失败的代理
            available_proxies = [p for p in self.proxies if p not in self.failed_proxies]
            
            if not available_proxies:
                # 如果没有可用代理，重置失败列表并重试
                logger.warning("没有可用代理，重置失败列表")
                self.failed_proxies.clear()
                available_proxies = self.proxies
            
            if not available_proxies:
                return None
            
            proxy_str = random.choice(available_proxies)
        
        # 格式化代理
        # 与 get_proxy 保持一致，使用 PROXY_SCHEME 生成 SOCKS 代理 URL
        scheme = getattr(config, "PROXY_SCHEME", "socks5")
        if "://" in proxy_str:
            proxy_dict = {"http": proxy_str, "https": proxy_str, "_raw": proxy_str}
        else:
            proxy_dict = {"http": f"{scheme}://{proxy_str}", "https": f"{scheme}://{proxy_str}", "_raw": proxy_str}
        
        return proxy_dict
    
    def acquire_exclusive_proxy(self) -> Optional[dict]:
        """
        获取独占式代理（保证每个线程使用独立的 IP）
        
        Returns:
            代理字典，格式：{"http": "socks5://ip:port", "https": "socks5://ip:port"}
            如果没有可用代理，返回 None
            
        Note:
            使用完毕后必须调用 release_proxy() 释放代理
        """
        if not self.enabled or not self.proxies:
            return None
        
        # 如果代理池太小，尝试刷新
        if len(self.proxies) < 10 and config.PROXY_API_URL:
            self.refresh_proxies_from_api()
        
        with self._lock:
            # 过滤掉已知失败的和已占用的代理
            available_proxies = [
                p for p in self.proxies 
                if p not in self.failed_proxies and p not in self.occupied_proxies
            ]
            
            # 记录代理池状态
            print(f"[代理池检查] 可用代理数: {len(available_proxies)}, 已占用: {len(self.occupied_proxies)}, 失败: {len(self.failed_proxies)}, 总数: {len(self.proxies)}")
            
            if not available_proxies:
                # 如果没有可用代理，尝试刷新
                print(f"[代理池状态] 没有可用独占代理 - 已占用: {len(self.occupied_proxies)}, 失败: {len(self.failed_proxies)}, 总数: {len(self.proxies)}")
                logger.warning(f"没有可用独占代理，已占用: {len(self.occupied_proxies)}, 失败: {len(self.failed_proxies)}, 总数: {len(self.proxies)}")
                
                # 如果失败代理太多，清空占用列表（可能有些代理已经释放但没有从占用列表中移除）
                if len(self.occupied_proxies) > len(self.proxies) * 0.8:
                    print(f"[代理池清理] 占用代理过多，清空占用列表 - 已占用: {len(self.occupied_proxies)}, 总数: {len(self.proxies)}")
                    self.occupied_proxies.clear()
                    available_proxies = [
                        p for p in self.proxies 
                        if p not in self.failed_proxies
                    ]
                
                # 如果仍然没有可用代理，尝试刷新
                if not available_proxies and config.PROXY_API_URL:
                    self.refresh_proxies_from_api()
                    available_proxies = [
                        p for p in self.proxies 
                        if p not in self.failed_proxies and p not in self.occupied_proxies
                    ]
                
                if not available_proxies:
                    # 仍然没有可用代理，回退到随机选择（允许复用，类似 get_random_proxy 的行为）
                    print(f"[代理池耗尽] 回退到随机选择 - 已占用: {len(self.occupied_proxies)}, 失败: {len(self.failed_proxies)}, 总数: {len(self.proxies)}")
                    logger.warning("代理池耗尽，回退到随机选择")
                    # 回退到随机选择时，不标记为占用（允许复用）
                    random_proxy = self.get_random_proxy()
                    if random_proxy:
                        # 确保返回的代理字典包含 _raw 字段，以便后续释放
                        if '_raw' not in random_proxy:
                            http_proxy = random_proxy.get('http', '')
                            if '://' in http_proxy:
                                random_proxy['_raw'] = http_proxy.split('://', 1)[1]
                            else:
                                random_proxy['_raw'] = http_proxy
                    return random_proxy
            
            # 选择第一个可用代理并标记为已占用
            proxy_str = available_proxies[0]
            self.occupied_proxies.add(proxy_str)
            print(f"[代理分配详情] 分配独占代理: {proxy_str}, 剩余可用: {len(available_proxies) - 1}, 已占用: {len(self.occupied_proxies)}, 失败: {len(self.failed_proxies)}, 总数: {len(self.proxies)}")
            logger.debug(f"分配独占代理: {proxy_str}, 剩余可用: {len(available_proxies) - 1}")
        
        # 格式化代理
        scheme = getattr(config, "PROXY_SCHEME", "socks5")
        if "://" in proxy_str:
            proxy_dict = {"http": proxy_str, "https": proxy_str, "_raw": proxy_str}
        else:
            proxy_dict = {"http": f"{scheme}://{proxy_str}", "https": f"{scheme}://{proxy_str}", "_raw": proxy_str}
        
        return proxy_dict
    
    def release_proxy(self, proxy_dict: Optional[dict]):
        """
        释放独占代理
        
        Args:
            proxy_dict: acquire_exclusive_proxy() 返回的代理字典
        """
        if not proxy_dict:
            print(f"[代理释放] proxy_dict 为空，跳过释放")
            return
        
        # 获取原始代理字符串
        proxy_str = proxy_dict.get('_raw')
        if not proxy_str:
            # 尝试从 http URL 中提取
            http_proxy = proxy_dict.get('http', '')
            if '://' in http_proxy:
                proxy_str = http_proxy.split('://', 1)[1]
            else:
                proxy_str = http_proxy
        
        if proxy_str:
            with self._lock:
                was_occupied = proxy_str in self.occupied_proxies
                if was_occupied:
                    self.occupied_proxies.discard(proxy_str)
                    print(f"[代理释放] 释放独占代理: {proxy_str}, 释放前占用数: {len(self.occupied_proxies) + 1}, 释放后占用数: {len(self.occupied_proxies)}")
                    logger.debug(f"释放独占代理: {proxy_str}, 当前占用: {len(self.occupied_proxies)}")
                else:
                    print(f"[代理释放警告] 代理 {proxy_str} 不在占用列表中，可能已经释放或从未占用")
        else:
            print(f"[代理释放错误] 无法从 proxy_dict 中提取代理字符串: {proxy_dict}")
    
    def get_proxy_for_playwright(self) -> Optional[dict]:
        """
        获取用于 Playwright 的代理配置
        
        Returns:
            Playwright 代理配置字典，格式：{"server": "http://ip:port"}
        """
        if not self.enabled or not self.proxies:
            return None
        
        # 如果代理池太小，尝试刷新
        if len(self.proxies) < 3 and config.PROXY_API_URL:
            self.refresh_proxies_from_api()
        
        with self._lock:
            # 过滤掉已知失败的代理
            available_proxies = [p for p in self.proxies if p not in self.failed_proxies]
            
            if not available_proxies:
                logger.warning("没有可用代理，重置失败列表")
                self.failed_proxies.clear()
                available_proxies = self.proxies
            
            if not available_proxies:
                return None
            
            proxy_str = random.choice(available_proxies)
        
        # Playwright 代理格式
        # 这里同样尊重 PROXY_SCHEME，统一输出例如 socks5://ip:port
        scheme = getattr(config, "PROXY_SCHEME", "socks5")
        if "://" not in proxy_str:
            return {"server": f"{scheme}://{proxy_str}"}
        else:
            return {"server": proxy_str}
    
    def add_proxy(self, proxy: str):
        """添加一个新代理到池中"""
        with self._lock:
            if proxy and proxy not in self.proxies:
                self.proxies.append(proxy)
                self.proxy_timestamps[proxy] = time.time()
                self.failed_proxies.discard(proxy)
                logger.debug(f"添加代理: {proxy}")
    
    def remove_proxy(self, proxy: str):
        """从池中移除一个代理"""
        with self._lock:
            if proxy in self.proxies:
                self.proxies.remove(proxy)
                self.proxy_timestamps.pop(proxy, None)
                self.failed_proxies.discard(proxy)
                if self.current_index >= len(self.proxies):
                    self.current_index = 0
                logger.debug(f"移除代理: {proxy}")
    
    def mark_proxy_failed(self, proxy: str):
        """标记代理为失败"""
        if not proxy:
            return
        
        # 从代理字符串中提取实际的代理地址
        proxy_address = proxy
        if "://" in proxy_address:
            proxy_address = proxy_address.split("://")[1]
        
        with self._lock:
            # 查找匹配的代理
            matching_proxy = None
            for p in self.proxies:
                if proxy_address in p or p in proxy_address:
                    matching_proxy = p
                    break
            
            if matching_proxy:
                self.failed_proxies.add(matching_proxy)
                logger.warning(f"标记代理失败: {matching_proxy}")
    
    def get_proxy_count(self) -> int:
        """获取可用代理数量"""
        if not self.enabled:
            return 0
        with self._lock:
            available = [p for p in self.proxies if p not in self.failed_proxies]
            return len(available)
    
    def get_status(self) -> dict:
        """
        获取代理管理器状态
        
        Returns:
            状态信息字典
        """
        with self._lock:
            return {
                "enabled": self.enabled,
                "total_proxies": len(self.proxies),
                "available_proxies": len([p for p in self.proxies if p not in self.failed_proxies]),
                "failed_proxies": len(self.failed_proxies),
                "last_refresh_time": self.last_api_fetch_time,
                "refresh_interval": self.api_fetch_interval,
                "api_url": config.PROXY_API_URL if config.PROXY_API_URL else None
            }


# Global proxy manager instance
proxy_manager = ProxyManager()

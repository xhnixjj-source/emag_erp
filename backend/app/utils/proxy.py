"""Proxy management utility"""
import random
import logging
import requests
import time
import json
import threading
from typing import Optional, List, Dict
from app.config import config

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
            
            # 构建 LunaProxy API 参数
            if 'lunaproxy.com' in url or 'lunaproxy' in url.lower():
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
            
            # 解析响应 - LunaProxy 返回纯文本，每行一个 IP:PORT
            text_content = response.text.strip()
            if text_content:
                proxy_list = [
                    line.strip() for line in text_content.split('\n')
                    if line.strip() and ':' in line.strip()
                ]
                if proxy_list:
                    self.last_api_fetch_time = time.time()
                    logger.info(f"从 API 获取了 {len(proxy_list)} 个代理")
                    return proxy_list
            
            # 尝试解析 JSON 响应
            try:
                data = response.json()
                if isinstance(data, dict):
                    proxies = data.get('proxies', data.get('data', data.get('list', [])))
                elif isinstance(data, list):
                    proxies = data
                else:
                    proxies = []
                
                proxy_list = [str(p).strip() for p in proxies if p and ':' in str(p)]
                self.last_api_fetch_time = time.time()
                
                if proxy_list:
                    logger.info(f"从 API 获取了 {len(proxy_list)} 个代理 (JSON 格式)")
                
                return proxy_list
            except ValueError:
                # 如果不是 JSON，按行分割
                proxy_list = [
                    line.strip() for line in response.text.strip().split('\n')
                    if line.strip() and ':' in line.strip()
                ]
                self.last_api_fetch_time = time.time()
                
                if proxy_list:
                    logger.info(f"从 API 获取了 {len(proxy_list)} 个代理 (文本格式)")
                
                return proxy_list
                
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
            if "://" not in proxy_str:
                test_url = f"http://{proxy_str}"
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
        if "://" in proxy_str:
            return {"http": proxy_str, "https": proxy_str}
        else:
            return {"http": f"http://{proxy_str}", "https": f"https://{proxy_str}"}
    
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
        if "://" in proxy_str:
            return {"http": proxy_str, "https": proxy_str}
        else:
            return {"http": f"http://{proxy_str}", "https": f"https://{proxy_str}"}
    
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
        if "://" not in proxy_str:
            return {"server": f"http://{proxy_str}"}
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

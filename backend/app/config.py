"""Application configuration"""
import os
from typing import List, Optional
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# 获取项目根目录（backend的父目录）
def get_project_root() -> Path:
    """获取项目根目录路径"""
    # 直接计算：config.py 在 backend/app/ 下，所以项目根目录是 parent.parent.parent
    # backend/app/config.py -> backend/app -> backend -> emag_erp (项目根)
    config_path = Path(__file__).resolve()  # backend/app/config.py
    backend_app = config_path.parent        # backend/app
    backend = backend_app.parent            # backend
    project_root = backend.parent           # emag_erp (项目根目录)
    return project_root

# 获取debug.log文件的正确路径
def get_debug_log_path() -> str:
    """获取debug.log文件的正确路径"""
    project_root = get_project_root()
    debug_log_path = project_root / '.cursor' / 'debug.log'
    # 确保.cursor目录存在
    debug_log_path.parent.mkdir(parents=True, exist_ok=True)
    return str(debug_log_path)


class Config:
    """Application configuration"""
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./emag_erp.db")

    # Proxy configuration
    # 代理配置说明：
    # - 启用 lunaproxy 动态 IP 代理模式
    # - 通过 API 获取动态代理 IP 列表，定期刷新
    # - 需要配合 Proxifier 强制 Playwright 流量走 VPN，确保 lunaproxy 看到海外 IP
    # - 详细配置说明见：backend/docs/proxifier_setup_guide.md
    PROXY_ENABLED: bool = os.getenv("PROXY_ENABLED", "true").lower() == "true"
    # 代理列表（API 模式下自动从 API 获取，无需手动配置）
    PROXY_LIST: List[str] = os.getenv(
        "PROXY_LIST",
        ""
    ).split(",") if os.getenv("PROXY_LIST") else []
    # lunaproxy 动态 IP API URL
    # 默认使用 GET 方式获取 IP 列表，参数通过下方配置项指定
    PROXY_API_URL: str = os.getenv(
        "PROXY_API_URL", 
        "https://tq.lunadataset.com/get_dynamic_ip"
    )
    # PROXY_API_URL 支持两种格式：
    # 1. GET方式（获取IP列表）：https://tq.lunadataset.com/get_dynamic_ip
    # 2. POST方式（Unlocker API）：https://unlocker-api.lunaproxy.com/request
    #    Unlocker API用于页面解锁服务，不直接返回IP列表，需要通过API请求访问页面
    PROXY_API_KEY: Optional[str] = os.getenv("PROXY_API_KEY", None)
    # PROXY_API_KEY：用于Authorization Bearer token认证（Unlocker API必需）
    PROXY_API_TIMEOUT: int = int(os.getenv("PROXY_API_TIMEOUT", "10"))
    PROXY_VALIDATION_TIMEOUT: int = int(os.getenv("PROXY_VALIDATION_TIMEOUT", "5"))
    # 代理协议类型（http / socks4 / socks5 / socks5h 等）
    # 注意：
    # - 使用 socks 协议需要在环境中安装 requests[socks] / PySocks
    # - 建议使用 socks5 或 socks5h（带域名解析）
    PROXY_SCHEME: str = os.getenv("PROXY_SCHEME", "socks5")

    # BitBrowser configuration
    # 说明：
    # - 当 BITBROWSER_ENABLED=true 时，优先通过 BitBrowser 窗口进行所有 Playwright 爬取
    # - 推荐使用 BITBROWSER_GROUP_ID 指定窗口组，系统自动拉取该组下所有窗口
    # - 也可使用 BITBROWSER_WINDOW_IDS 手动指定窗口 ID（当 GROUP_ID 为空时的 fallback）
    BITBROWSER_ENABLED: bool = os.getenv("BITBROWSER_ENABLED", "false").lower() == "true"
    BITBROWSER_API_URL: str = os.getenv("BITBROWSER_API_URL", "http://127.0.0.1:54345")
    BITBROWSER_GROUP_ID: str = os.getenv("BITBROWSER_GROUP_ID", "")
    BITBROWSER_WINDOW_IDS: List[str] = os.getenv("BITBROWSER_WINDOW_IDS", "").split(",") if os.getenv("BITBROWSER_WINDOW_IDS") else []
    BITBROWSER_MAX_RESTART_COUNT: int = int(os.getenv("BITBROWSER_MAX_RESTART_COUNT", "10"))
    BITBROWSER_RESTART_DELAY: int = int(os.getenv("BITBROWSER_RESTART_DELAY", "5"))

    # Crawler configuration
    CRAWLER_DELAY_MIN: int = int(os.getenv("CRAWLER_DELAY_MIN", "1"))
    CRAWLER_DELAY_MAX: int = int(os.getenv("CRAWLER_DELAY_MAX", "3"))
    CRAWLER_TIMEOUT: int = int(os.getenv("CRAWLER_TIMEOUT", "60"))  # 增加到60秒，避免网络不稳定时的超时
    
    # Thread pool configuration
    MAX_WORKER_THREADS: int = int(os.getenv("MAX_WORKER_THREADS", "50"))
    KEYWORD_SEARCH_THREADS: int = int(os.getenv("KEYWORD_SEARCH_THREADS", "20"))
    PRODUCT_CRAWL_THREADS: int = int(os.getenv("PRODUCT_CRAWL_THREADS", "50"))
    MONITOR_THREADS: int = int(os.getenv("MONITOR_THREADS", "30"))
    
    # Retry configuration
    MAX_RETRY_COUNT: int = int(os.getenv("MAX_RETRY_COUNT", "3"))
    RETRY_BACKOFF_BASE: int = int(os.getenv("RETRY_BACKOFF_BASE", "2"))
    RETRY_BACKOFF_MAX: int = int(os.getenv("RETRY_BACKOFF_MAX", "60"))
    
    # Task queue configuration
    TASK_QUEUE_SIZE: int = int(os.getenv("TASK_QUEUE_SIZE", "1000"))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    TASK_MANAGER_ENABLED: bool = os.getenv("TASK_MANAGER_ENABLED", "true").lower() == "true"
    MAX_CONCURRENT_TASKS: int = int(os.getenv("MAX_CONCURRENT_TASKS", "10"))
    TASK_POLLING_INTERVAL: int = int(os.getenv("TASK_POLLING_INTERVAL", "2"))
    
    # Captcha configuration
    CAPTCHA_DETECTION_ENABLED: bool = os.getenv("CAPTCHA_DETECTION_ENABLED", "true").lower() == "true"
    CAPTCHA_WAIT_TIME: int = int(os.getenv("CAPTCHA_WAIT_TIME", "300"))
    
    # Keyword search configuration
    KEYWORD_SEARCH_MAX_PAGES: int = int(os.getenv("KEYWORD_SEARCH_MAX_PAGES", "5"))
    
    # Scheduler configuration
    SCHEDULER_TIMEZONE: str = os.getenv("SCHEDULER_TIMEZONE", "Asia/Shanghai")
    MONITOR_SCHEDULE_HOUR: int = int(os.getenv("MONITOR_SCHEDULE_HOUR", "2"))
    MONITOR_SCHEDULE_MINUTE: int = int(os.getenv("MONITOR_SCHEDULE_MINUTE", "0"))
    # Listed_at backfill task configuration
    LISTED_AT_BACKFILL_ENABLED: bool = os.getenv("LISTED_AT_BACKFILL_ENABLED", "true").lower() == "true"
    LISTED_AT_BACKFILL_INTERVAL_MINUTES: int = int(os.getenv("LISTED_AT_BACKFILL_INTERVAL_MINUTES", "5"))
    LISTED_AT_BATCH_SIZE: int = int(os.getenv("LISTED_AT_BATCH_SIZE", "50"))
    LISTED_AT_SLEEP_SECONDS: float = float(os.getenv("LISTED_AT_SLEEP_SECONDS", "0.5"))
    
    # Ranking page timeout (与主页超时对齐, 类目页实际加载常需 12-18 秒)
    RANKING_PAGE_TIMEOUT: int = int(os.getenv("RANKING_PAGE_TIMEOUT", "30000"))
    
    # Playwright configuration
    PLAYWRIGHT_BROWSER_TYPE: str = os.getenv("PLAYWRIGHT_BROWSER_TYPE", "chromium")
    PLAYWRIGHT_HEADLESS: bool = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
    PLAYWRIGHT_TIMEOUT: int = int(os.getenv("PLAYWRIGHT_TIMEOUT", "30000"))
    PLAYWRIGHT_MAX_CONTEXTS: int = int(os.getenv("PLAYWRIGHT_MAX_CONTEXTS", "10"))
    PLAYWRIGHT_CONTEXT_TIMEOUT: int = int(os.getenv("PLAYWRIGHT_CONTEXT_TIMEOUT", "60000"))
    PLAYWRIGHT_NAVIGATION_TIMEOUT: int = int(os.getenv("PLAYWRIGHT_NAVIGATION_TIMEOUT", "60000"))  # 增加到60秒，代理连接可能需要更长时间
    PLAYWRIGHT_CONTEXT_REUSE: bool = os.getenv("PLAYWRIGHT_CONTEXT_REUSE", "true").lower() == "true"
    PLAYWRIGHT_CONTEXT_MAX_REUSE_COUNT: int = int(os.getenv("PLAYWRIGHT_CONTEXT_MAX_REUSE_COUNT", "100"))
    PLAYWRIGHT_HEALTH_CHECK_INTERVAL: int = int(os.getenv("PLAYWRIGHT_HEALTH_CHECK_INTERVAL", "300"))
    
    # Dynamic IP proxy API configuration (LunaProxy)
    # lunaproxy 用户 ID（neek 参数），从 lunaproxy 控制台获取
    PROXY_API_USER_ID: Optional[str] = os.getenv("PROXY_API_USER_ID", "1866167")
    # 代理地区，all 表示全球，可指定如 "ro" 表示罗马尼亚
    PROXY_API_COUNTRY: str = os.getenv("PROXY_API_COUNTRY", "all")
    # 每次 API 请求获取的 IP 数量
    PROXY_API_IP_COUNT: int = int(os.getenv("PROXY_API_IP_COUNT", "1000"))
    # IP 有效期（分钟），lunaproxy 的 ip_si 参数：1-120 分钟
    PROXY_API_IP_SI: int = int(os.getenv("PROXY_API_IP_SI", "5"))
    # 分隔符参数，空字符串表示每行一个 IP:PORT
    PROXY_API_SB: Optional[str] = os.getenv("PROXY_API_SB", "")
    # API 刷新间隔（秒），建议 60-120 秒以保持 IP 池新鲜
    # 应小于 PROXY_API_IP_SI（IP有效期）换算成秒数
    PROXY_API_FETCH_INTERVAL: int = int(os.getenv("PROXY_API_FETCH_INTERVAL", "120"))

    # Istoric Preturi 插件后端接口配置
    # 说明：
    # - 通过逆向分析 Chrome 插件源码获取 API 地址
    # - POST /getProductInfo 返回包含 Chart.js 图表数据的 HTML
    # - 从图表 data.labels[0] 提取最早追踪日期（即上架日期）
    # 是否启用 Istoric Preturi 接口获取上架日期
    ISTORIC_PRETURI_ENABLED: bool = os.getenv("ISTORIC_PRETURI_ENABLED", "true").lower() == "true"
    # 临时总开关：允许在不改代码的情况下完全屏蔽上架日期获取逻辑
    # 默认改为 False，表示默认开启上架日期获取
    DISABLE_LISTED_AT: bool = os.getenv("DISABLE_LISTED_AT", "false").lower() == "true"
    # API 基础地址（不含路径），如 https://api.istoric-preturi.info
    ISTORIC_PRETURI_ENDPOINT: str = os.getenv("ISTORIC_PRETURI_ENDPOINT", "https://api.istoric-preturi.info")
    # 调用超时时间（秒）
    ISTORIC_PRETURI_TIMEOUT: int = int(os.getenv("ISTORIC_PRETURI_TIMEOUT", "15"))
    # 最大重试次数（独立于全局爬虫重试配置）
    ISTORIC_PRETURI_MAX_RETRIES: int = int(os.getenv("ISTORIC_PRETURI_MAX_RETRIES", "3"))


config = Config()


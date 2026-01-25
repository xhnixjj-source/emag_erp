import os
import sys
import time
from typing import Optional

import requests

"""
使用 SOCKS 代理测试访问 eMAG 产品页的简单脚本

用法示例（在 backend 目录下）：
    python -m scripts.test_socks_proxy_emag \
        --proxy socks5://ip:port \
        --url https://www.emag.ro/xxx/pd/PNKCODE/

说明：
- 依赖：requests[socks]（内部使用 PySocks），请确保已在 venv 中安装：
    pip install "requests[socks]"
- 本脚本不会依赖项目里的 proxy_manager，方便快速单独验证某个代理 IP 是否可用。
"""


def build_proxies(proxy_url: str) -> dict:
    """
    根据传入的 SOCKS 代理 URL 构建 requests 的 proxies 参数

    参数:
        proxy_url: 形如 "socks5://ip:port" 或 "socks5h://ip:port" 的字符串

    返回:
        可直接传给 requests.get(..., proxies=...) 的字典
    """
    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def test_socks_proxy(product_url: str, proxy_url: str, timeout: int = 30) -> bool:
    """
    使用 SOCKS 代理访问产品页，打印结果并返回是否成功

    参数:
        product_url: 目标产品页 URL（例如：https://www.emag.ro/.../pd/XXXX/）
        proxy_url:   SOCKS 代理 URL（例如：socks5://ip:port）
        timeout:     超时时间（秒）

    返回:
        True  表示 HTTP 状态码为 200
        False 表示请求失败或状态码不是 200
    """
    proxies = build_proxies(proxy_url)

    print("========== SOCKS 代理访问 eMAG 产品页测试 ==========")
    print(f"目标 URL : {product_url}")
    print(f"代理 URL : {proxy_url}")
    print(f"超时时间 : {timeout}s")
    print("=================================================")

    headers = {
        # 使用一个常见浏览器的 User-Agent，避免被直接拦截
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9,ro;q=0.8,zh-CN;q=0.7,zh;q=0.6",
    }

    start = time.time()
    try:
        resp = requests.get(
            product_url,
            headers=headers,
            proxies=proxies,
            timeout=timeout,
            allow_redirects=True,
        )
        elapsed = time.time() - start

        print(f"\n请求完成，耗时: {elapsed:.2f}s")
        print(f"HTTP 状态码: {resp.status_code}")
        print(f"最终 URL  : {resp.url}")

        # 打印前 500 字符，方便判断是否为正常页面 / Captcha / 错误页
        text_preview = resp.text[:500].replace("\n", " ").replace("\r", " ")
        print("\n响应内容预览（前 500 字符）:")
        print("-------------------------------------------------")
        print(text_preview)
        print("-------------------------------------------------")

        return resp.status_code == 200

    except Exception as e:
        elapsed = time.time() - start
        print(f"\n请求失败，耗时: {elapsed:.2f}s")
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        return False


def parse_args(argv: list[str]) -> tuple[Optional[str], Optional[str]]:
    """
    非常简单的命令行参数解析：
    --proxy <socks_url>   必填，例如：socks5://1.2.3.4:1080
    --url   <product_url> 必填，例如：https://www.emag.ro/.../pd/XXXX/
    """
    proxy_url: Optional[str] = None
    product_url: Optional[str] = None

    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--proxy" and i + 1 < len(argv):
            proxy_url = argv[i + 1]
            i += 2
        elif arg == "--url" and i + 1 < len(argv):
            product_url = argv[i + 1]
            i += 2
        else:
            i += 1

    return proxy_url, product_url


def main() -> None:
    proxy_url, product_url = parse_args(sys.argv[1:])

    if not proxy_url or not product_url:
        print("用法:")
        print("  python -m scripts.test_socks_proxy_emag "
              "--proxy socks5://ip:port "
              "--url https://www.emag.ro/.../pd/XXXX/")
        print("\n参数说明:")
        print("  --proxy  SOCKS 代理地址，例如：socks5://1.2.3.4:1080")
        print("  --url    eMAG 产品页 URL，例如：https://www.emag.ro/.../pd/XXXX/")
        sys.exit(1)

    success = test_socks_proxy(product_url=product_url, proxy_url=proxy_url)
    if success:
        print("\n结果：通过 SOCKS 代理访问产品页【成功】")
        sys.exit(0)
    else:
        print("\n结果：通过 SOCKS 代理访问产品页【失败】")
        sys.exit(2)


if __name__ == "__main__":
    main()



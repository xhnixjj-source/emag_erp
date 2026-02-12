"""Istoric Preturi 插件后端 HTTP client

通过调用 Istoric Preturi Chrome 插件使用的同一后端 API 接口，
获取产品的真实"上架日期"（listed_at）。

API 信息（通过逆向分析插件源码获取）：
- 基础地址: https://api.istoric-preturi.info
- 接口: POST /getProductInfo
- 请求体: { "link": "<encodeURIComponent 编码后的产品 URL>", "title": "", "inPage": false, "variant": "" }
- 请求头: content-type, extension-version, client-identifier
- 响应: HTML 页面，包含 Chart.js 图表数据
- 上架日期: 图表 data.labels 数组中的第一个元素（罗马尼亚语日期格式，如 "28 Noi 2020"）
"""

import json
import logging
import random
import re
import time as _time
from datetime import datetime
from typing import Dict, Optional
from urllib.parse import quote

import requests

from app.config import config
from app.services.retry_manager import RetryManager

logger = logging.getLogger(__name__)

# #region agent log
_DEBUG_LOG_PATH = r"d:\emag_erp\.cursor\debug.log"
def _dbg(location, message, data=None, hypothesis=""):
    try:
        import json as _j
        entry = {"timestamp": int(_time.time()*1000), "location": location, "message": message, "data": data or {}, "hypothesisId": hypothesis, "runId": "istoric-debug"}
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as _f:
            _f.write(_j.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass
# #endregion

# ── 罗马尼亚语月份名称映射 ──────────────────────────────────
# 插件返回的日期格式为 "DD Mon YYYY"，月份使用罗马尼亚语缩写
_RO_MONTH_MAP = {
    "ian": 1,   # Ianuarie
    "feb": 2,   # Februarie
    "mar": 3,   # Martie
    "apr": 4,   # Aprilie
    "mai": 5,   # Mai
    "iun": 6,   # Iunie
    "iul": 7,   # Iulie
    "aug": 8,   # August
    "sep": 9,   # Septembrie
    "oct": 10,  # Octombrie
    "noi": 11,  # Noiembrie
    "dec": 12,  # Decembrie
}

# ── 模拟插件 client-identifier ─────────────────────────────
_CLIENT_IDENTIFIER: Optional[str] = None


def _get_client_identifier() -> str:
    """生成或返回缓存的 client-identifier

    模拟插件安装时生成的 32 位随机标识（字符集 a-f + 0-9）。
    """
    global _CLIENT_IDENTIFIER
    if _CLIENT_IDENTIFIER is None:
        chars = "abcdef0123456789"
        _CLIENT_IDENTIFIER = "".join(random.choice(chars) for _ in range(32))
    return _CLIENT_IDENTIFIER


def _parse_romanian_date(date_str: str) -> Optional[datetime]:
    """解析罗马尼亚语日期字符串为 datetime

    支持格式：
    - "28 Noi 2020"  → 2020-11-28
    - "06 Feb 2021"  → 2021-02-06
    - "11 Mai 2022"  → 2022-05-11
    """
    if not date_str or not isinstance(date_str, str):
        return None

    parts = date_str.strip().split()
    if len(parts) != 3:
        return None

    try:
        day = int(parts[0])
        month_str = parts[1].lower()
        year = int(parts[2])

        month = _RO_MONTH_MAP.get(month_str)
        if month is None:
            return None

        return datetime(year, month, day)
    except (ValueError, TypeError):
        return None


def _extract_listed_at_from_html(html_content: str) -> Optional[datetime]:
    """从 Istoric Preturi API 返回的 HTML 中提取上架日期

    逻辑：
    1. 找到 <div id="__chart_options_onlySite"> 元素
    2. 提取其中的 Chart.js JSON 数据（格式为 <!-- {JSON} -->，需 slice(5, -4)）
    3. 解析 JSON，取 data.labels[0] 作为最早追踪日期（即上架日期）
    4. 将罗马尼亚语日期转换为 datetime
    """
    if not html_content:
        return None

    # 优先从 onlySite 图表提取（仅 eMAG 数据），兜底用 allSites
    for chart_id in ("__chart_options_onlySite", "__chart_options_allSites"):
        pattern = rf"id=['\"]?{chart_id}['\"]?[^>]*>(.*?)</div>"
        match = re.search(pattern, html_content, re.DOTALL)
        if not match:
            continue

        raw_content = match.group(1)
        if len(raw_content) < 10:
            continue

        # 插件 JS 中的解析方式: JSON.parse(element.innerHTML.slice(5, -4))
        # 内容格式为 "<!-- {JSON} -->"
        try:
            json_str = raw_content[5:-4]
            chart_data = json.loads(json_str)
        except (json.JSONDecodeError, IndexError):
            logger.debug(
                f"[IstoricPreturi] 解析 {chart_id} JSON 失败, raw_length={len(raw_content)}"
            )
            continue

        # 提取 data.labels 数组
        labels = None
        if isinstance(chart_data, dict):
            data_section = chart_data.get("data")
            if isinstance(data_section, dict):
                labels = data_section.get("labels")

        if not labels or not isinstance(labels, list) or len(labels) == 0:
            logger.debug(f"[IstoricPreturi] {chart_id} 中未找到 labels 数组")
            continue

        # 第一个 label 就是最早追踪日期（上架日期）
        first_label = labels[0]
        listed_at = _parse_romanian_date(first_label)
        if listed_at:
            return listed_at
        else:
            logger.debug(
                f"[IstoricPreturi] 无法解析 {chart_id} 首个 label: {first_label!r}"
            )

    return None


def get_listed_at_via_browser(page, product_url: str) -> Optional[datetime]:
    """通过 Playwright page（浏览器上下文）调用 Istoric Preturi API

    利用 BitBrowser 的网络通道发送请求，绕过 Proxifier 对 Python 进程的代理限制。
    通过在新标签页中导航到 API 同域，再用 same-origin fetch 避免 CORS 问题。

    Args:
        page: 当前 Playwright 页面对象（用于获取浏览器上下文）
        product_url: eMAG 产品详情页 URL

    Returns:
        datetime 或 None（获取失败时）
    """
    if not config.ISTORIC_PRETURI_ENABLED:
        logger.debug("[IstoricPreturi] 未启用，跳过浏览器方式获取")
        return None

    endpoint = config.ISTORIC_PRETURI_ENDPOINT
    if not endpoint:
        logger.warning("[IstoricPreturi] 未配置 ISTORIC_PRETURI_ENDPOINT")
        return None

    api_page = None
    try:
        # #region agent log
        t0 = _time.time()
        _dbg("client.py:browser_start", "浏览器方式API请求开始", {"url": product_url, "endpoint": endpoint}, "H6-browser")
        # #endregion

        # 在同一浏览器上下文中打开新标签页
        context = page.context
        # #region agent log
        _dbg("client.py:browser_new_page", "准备创建api_page", {"url": product_url, "context_pages": len(context.pages)}, "H4,H5")
        # #endregion
        api_page = context.new_page()
        # 为 api_page 设置较短的默认超时（Playwright 级安全网）
        api_page.set_default_timeout(config.ISTORIC_PRETURI_TIMEOUT * 1000 + 5000)

        # 导航到 API 域，建立 same-origin 上下文（必须成功才能执行 fetch）
        goto_ok = False
        try:
            api_page.goto(f"{endpoint}/", wait_until="commit", timeout=10000)
            goto_ok = True
            # #region agent log
            _dbg("client.py:browser_goto_ok", "api_page导航成功", {"url": product_url}, "H5")
            # #endregion
        except Exception as _goto_err:
            # #region agent log
            _dbg("client.py:browser_goto_err", "api_page导航失败-跳过evaluate", {"url": product_url, "error": str(_goto_err)[:200]}, "H5")
            # #endregion
            logger.info(
                f"[IstoricPreturi] 浏览器导航到API域失败，跳过fetch url={product_url}, "
                f"错误: {str(_goto_err)[:200]}"
            )

        # goto 失败时页面处于 about:blank，evaluate() 会永久挂起，必须跳过
        if not goto_ok:
            # #region agent log
            _dbg("client.py:browser_skip_evaluate", "goto失败,跳过evaluate直接返回None", {"url": product_url}, "F1")
            # #endregion
            return None

        # 在浏览器中执行 fetch 请求（same-origin，无 CORS 问题）
        # 使用 AbortController 设置 15 秒超时，避免 fetch 无限挂起
        fetch_timeout_ms = config.ISTORIC_PRETURI_TIMEOUT * 1000  # 转为毫秒
        js_result = api_page.evaluate("""
            async (params) => {
                try {
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), params.timeoutMs);
                    try {
                        const resp = await fetch(params.apiUrl + '/getProductInfo', {
                            method: 'POST',
                            signal: controller.signal,
                            headers: {
                                'content-type': 'application/json',
                                'extension-version': '2.39',
                                'client-identifier': params.clientId,
                            },
                            body: JSON.stringify({
                                link: encodeURIComponent(params.productUrl),
                                title: '',
                                inPage: false,
                                variant: '',
                            }),
                        });
                        clearTimeout(timeoutId);
                        if (!resp.ok) return { error: 'HTTP ' + resp.status, status: resp.status };
                        const text = await resp.text();
                        return { html: text, status: resp.status };
                    } catch (e) {
                        clearTimeout(timeoutId);
                        throw e;
                    }
                } catch (e) {
                    return { error: e.name === 'AbortError' ? 'fetch timeout' : e.message };
                }
            }
        """, {
            "apiUrl": endpoint,
            "productUrl": product_url,
            "clientId": _get_client_identifier(),
            "timeoutMs": fetch_timeout_ms,
        })

        # #region agent log
        elapsed = _time.time() - t0
        _dbg("client.py:browser_response", "浏览器方式API响应", {
            "url": product_url, "elapsed_s": round(elapsed, 2),
            "has_error": bool(js_result and js_result.get("error")),
            "error": js_result.get("error") if js_result else None,
            "html_len": len(js_result.get("html", "")) if js_result and js_result.get("html") else 0,
            "status": js_result.get("status") if js_result else None,
        }, "H6-browser")
        # #endregion

        if not js_result or js_result.get("error"):
            logger.info(
                f"[IstoricPreturi] 浏览器 fetch 失败 url={product_url}, "
                f"result={js_result}"
            )
            return None

        html_content = js_result.get("html", "")
        if not html_content or len(html_content) < 100:
            logger.info(
                f"[IstoricPreturi] 浏览器响应内容过短 url={product_url}, "
                f"length={len(html_content)}"
            )
            return None

        listed_at = _extract_listed_at_from_html(html_content)
        if listed_at:
            # #region agent log
            _dbg("client.py:browser_success", "浏览器方式成功获取上架日期", {"url": product_url, "listed_at": listed_at.isoformat()}, "H6-browser")
            # #endregion
            logger.info(
                f"[IstoricPreturi] 浏览器方式获取上架日期成功 url={product_url}, "
                f"listed_at={listed_at.isoformat()}"
            )
        else:
            # #region agent log
            _dbg("client.py:browser_parse_fail", "浏览器方式HTML解析失败", {"url": product_url, "html_len": len(html_content), "html_snippet": html_content[:300]}, "H6-browser")
            # #endregion
            logger.info(
                f"[IstoricPreturi] 浏览器方式未解析出上架日期 url={product_url}, "
                f"html_length={len(html_content)}"
            )
        return listed_at

    except Exception as e:
        # #region agent log
        elapsed = _time.time() - t0 if 't0' in dir() else -1
        _dbg("client.py:browser_exception", "浏览器方式获取异常", {"url": product_url, "error": str(e), "error_type": type(e).__name__, "elapsed_s": round(elapsed, 2)}, "H6-browser")
        # #endregion
        logger.warning(
            f"[IstoricPreturi] 浏览器方式获取上架日期异常 url={product_url}, 错误: {e}"
        )
        return None
    finally:
        if api_page:
            try:
                api_page.close()
                # #region agent log
                _dbg("client.py:browser_page_closed", "api_page已关闭", {"url": product_url, "remaining_pages": len(page.context.pages)}, "H4")
                # #endregion
            except Exception as _close_err:
                # #region agent log
                _dbg("client.py:browser_page_close_err", "api_page关闭失败", {"url": product_url, "error": str(_close_err)[:200]}, "H4")
                # #endregion
                pass


def get_listed_at(product_url: str) -> Optional[datetime]:
    """调用 Istoric Preturi API 获取产品上架日期

    Args:
        product_url: eMAG 产品详情页 URL

    Returns:
        datetime 或 None（获取失败时）
    """
    if not config.ISTORIC_PRETURI_ENABLED:
        logger.debug("[IstoricPreturi] 未启用（ISTORIC_PRETURI_ENABLED=false），跳过上架日期获取")
        return None

    endpoint = config.ISTORIC_PRETURI_ENDPOINT
    if not endpoint:
        logger.warning("[IstoricPreturi] 未配置 ISTORIC_PRETURI_ENDPOINT，无法调用接口")
        return None

    retry_manager = RetryManager(
        max_retries=config.ISTORIC_PRETURI_MAX_RETRIES,
        backoff_base=config.RETRY_BACKOFF_BASE,
        backoff_max=config.RETRY_BACKOFF_MAX,
    )

    def _request_once() -> Optional[datetime]:
        # 构建请求（模拟插件的 getProductInfo 调用）
        encoded_url = quote(product_url, safe="")
        payload = {
            "link": encoded_url,
            "title": "",
            "inPage": False,
            "variant": "",
        }

        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "text/html, application/json, text/plain, */*",
            "extension-version": "2.39",
            "client-identifier": _get_client_identifier(),
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        api_url = f"{endpoint}/getProductInfo"

        # #region agent log
        t0 = _time.time()
        _dbg("client.py:request_start", "API请求开始", {"url": product_url, "api_url": api_url, "client_id": _get_client_identifier()[:8]}, "H1,H2")
        # #endregion

        try:
            resp = requests.post(
                api_url,
                json=payload,
                headers=headers,
                timeout=config.ISTORIC_PRETURI_TIMEOUT,
            )
            # #region agent log
            elapsed = _time.time() - t0
            _dbg("client.py:response", "API响应", {"url": product_url, "status": resp.status_code, "length": len(resp.text), "elapsed_s": round(elapsed, 2), "first_200": resp.text[:200]}, "H1,H3,H5")
            # #endregion
            resp.raise_for_status()
        except Exception as e:
            # #region agent log
            elapsed = _time.time() - t0
            _dbg("client.py:request_error", "API请求异常", {"url": product_url, "error": str(e), "error_type": type(e).__name__, "elapsed_s": round(elapsed, 2)}, "H1,H2")
            # #endregion
            logger.warning(
                f"[IstoricPreturi] 请求失败 url={product_url}, api={api_url}, 错误: {e}"
            )
            raise

        html_content = resp.text
        if not html_content or len(html_content) < 100:
            # #region agent log
            _dbg("client.py:short_response", "响应内容过短", {"url": product_url, "length": len(html_content) if html_content else 0}, "H3,H5")
            # #endregion
            logger.info(
                f"[IstoricPreturi] 响应内容过短 url={product_url}, length={len(html_content) if html_content else 0}"
            )
            return None

        # #region agent log
        has_onlySite = "__chart_options_onlySite" in html_content
        has_allSites = "__chart_options_allSites" in html_content
        _dbg("client.py:html_check", "HTML图表检查", {"url": product_url, "html_len": len(html_content), "has_onlySite": has_onlySite, "has_allSites": has_allSites}, "H3,H5")
        # #endregion

        listed_at = _extract_listed_at_from_html(html_content)
        if not listed_at:
            # #region agent log
            _dbg("client.py:parse_fail", "HTML解析失败-无上架日期", {"url": product_url, "html_len": len(html_content), "has_onlySite": has_onlySite, "has_allSites": has_allSites, "html_snippet": html_content[:500]}, "H3,H5")
            # #endregion
            logger.info(
                f"[IstoricPreturi] 未能从 HTML 中解析出上架日期 url={product_url}, "
                f"html_length={len(html_content)}"
            )
            return None

        # #region agent log
        _dbg("client.py:success", "成功获取上架日期", {"url": product_url, "listed_at": listed_at.isoformat()}, "H1")
        # #endregion
        return listed_at

    try:
        result = retry_manager.execute_with_retry(_request_once, task_id=None)
        # #region agent log
        _dbg("client.py:get_listed_at_return", "get_listed_at返回", {"url": product_url, "result": result.isoformat() if result else None}, "H4")
        # #endregion
        return result
    except Exception as e:
        # #region agent log
        _dbg("client.py:get_listed_at_exception", "get_listed_at异常(重试耗尽)", {"url": product_url, "error": str(e), "error_type": type(e).__name__}, "H2,H4")
        # #endregion
        logger.warning(
            f"[IstoricPreturi] 多次重试仍然失败，放弃获取上架日期 url={product_url}, 错误: {e}"
        )
        return None

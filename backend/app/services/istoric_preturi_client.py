"""Istoric Preturi 插件后端 HTTP client

通过配置化的方式调用 Istoric Preturi 插件在浏览器中使用的同一后端接口，
用于获取产品的真实“上架日期”（listed_at）。

使用前置条件（需要你在浏览器中完成的工作）：
1. 打开任意 eMAG 产品详情页，点击 Istoric Preturi 插件；
2. 在 Chrome DevTools → Network 中抓包，找到插件产生的 HTTP 请求；
3. 记录：
   - 请求 URL（含 query 参数）；
   - 请求方法（GET/POST）；
   - Request Headers（是否需要特殊 Header / Token）；
   - 请求体格式（如有）；
   - 响应 JSON 中“上架日期”所在字段路径及日期格式；
4. 将上述信息填入环境变量，对应 Config 中的 ISTORIC_PRETURI_* 配置项。

本 client 的目标：
- 在不依赖浏览器插件的前提下，在后端直接调用相同接口获取上架日期；
- 尽量通过配置实现适配，避免频繁改动代码。
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import quote

import requests

from app.config import config
from app.services.retry_manager import RetryManager


def _agent_debug_log(hypothesis_id: str, location: str, message: str, data: Dict[str, Any]) -> None:
    """调试模式下，将关键状态写入 NDJSON 日志文件，便于排查问题"""
    try:
        import json  # 局部导入避免污染全局
        from app.config import get_debug_log_path

        debug_log_path = get_debug_log_path()
        log_entry = {
            "sessionId": "debug-session",
            "runId": "istoric-client",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(datetime.utcnow().timestamp() * 1000),
        }
        with open(debug_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception:
        # 调试日志失败时静默忽略，避免影响主流程
        pass

logger = logging.getLogger(__name__)


def _extract_pnk_code(product_url: str) -> Optional[str]:
    """从 eMAG 产品 URL 中提取 pnk_code（/pd/<pnk_code>/...）

    说明：
    - 逻辑与 crawler.py 中的 _extract_pnk_code_from_url 基本一致，这里做了轻量复制，
      避免 services 之间产生循环依赖。
    """
    if not product_url or "/pd/" not in product_url:
        return None
    try:
        # /pd/<code>/ 或 /pd/<code>
        part = product_url.split("/pd/", 1)[1]
        part = part.split("/", 1)[0]
        part = part.split("?", 1)[0].split("#", 1)[0]
        return part or None
    except Exception:
        return None


def _get_value_by_path(data: Dict[str, Any], path: str) -> Any:
    """根据点号分隔的路径从嵌套 dict 中取值，如 path='data.listed_at'"""
    if not path:
        return None
    current: Any = data
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        if key not in current:
            return None
        current = current[key]
    return current


def _parse_date(value: Any) -> Optional[datetime]:
    """将接口返回的日期字段解析为 datetime

    优先使用配置中的 ISTORIC_PRETURI_DATE_FORMAT，
    否则尝试内置的一些常见格式，最后兜底处理时间戳。
    """
    if value is None:
        return None

    # 已经是 datetime 直接返回
    if isinstance(value, datetime):
        return value

    # 数字类型：可能是 Unix 时间戳（秒或毫秒）
    if isinstance(value, (int, float)):
        try:
            # 优先按秒解析，若过小/过大可按毫秒重试
            if value > 10_000_000_000:  # 粗略判断为毫秒级
                return datetime.utcfromtimestamp(value / 1000.0)
            return datetime.utcfromtimestamp(value)
        except Exception:
            return None

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    # 若配置了明确的日期格式，则优先使用
    if config.ISTORIC_PRETURI_DATE_FORMAT:
        try:
            return datetime.strptime(text, config.ISTORIC_PRETURI_DATE_FORMAT)
        except Exception:
            logger.warning(
                f"[IstoricPreturi] 使用配置格式解析日期失败 value={text}, "
                f"format={config.ISTORIC_PRETURI_DATE_FORMAT}"
            )

    # 内置常见格式（覆盖 eMAG / 罗马尼亚常见格式）
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue

    return None


def get_listed_at(product_url: str) -> Optional[datetime]:
    """调用 Istoric Preturi 接口获取产品上架日期

    Args:
        product_url: eMAG 产品详情页 URL

    Returns:
        datetime 或 None（获取失败时）
    """
    # #region agent log
    _agent_debug_log(
        hypothesis_id="A",
        location="istoric_preturi_client.py:get_listed_at:entry",
        message="函数入口与基础配置",
        data={
            "product_url": product_url,
            "enabled": config.ISTORIC_PRETURI_ENABLED,
            "endpoint": config.ISTORIC_PRETURI_ENDPOINT,
            "url_param": config.ISTORIC_PRETURI_URL_PARAM,
            "pnk_param": config.ISTORIC_PRETURI_PNK_PARAM,
            "listed_at_path": config.ISTORIC_PRETURI_LISTED_AT_PATH,
            "date_format": config.ISTORIC_PRETURI_DATE_FORMAT,
        },
    )
    # #endregion

    if not config.ISTORIC_PRETURI_ENABLED:
        # 未开启开关时直接跳过，避免在本地开发或未配置阶段产生无意义请求
        logger.debug("[IstoricPreturi] 未启用（ISTORIC_PRETURI_ENABLED=false），跳过上架日期获取")
        # #region agent log
        _agent_debug_log(
            hypothesis_id="B",
            location="istoric_preturi_client.py:get_listed_at:disabled",
            message="Istoric Preturi 功能未启用",
            data={},
        )
        # #endregion
        return None

    if not config.ISTORIC_PRETURI_ENDPOINT:
        logger.warning("[IstoricPreturi] 未配置 ISTORIC_PRETURI_ENDPOINT，无法调用接口")
        # #region agent log
        _agent_debug_log(
            hypothesis_id="C",
            location="istoric_preturi_client.py:get_listed_at:no-endpoint",
            message="缺少 ISTORIC_PRETURI_ENDPOINT 配置",
            data={},
        )
        # #endregion
        return None

    retry_manager = RetryManager(
        max_retries=config.ISTORIC_PRETURI_MAX_RETRIES,
        backoff_base=config.RETRY_BACKOFF_BASE,
        backoff_max=config.RETRY_BACKOFF_MAX,
    )

    pnk_code = _extract_pnk_code(product_url)

    def _request_once() -> Optional[datetime]:
        # 大多数 Istoric Preturi 插件实现使用 POST JSON，正文中包含 link 等字段
        payload: Dict[str, Any] = {}

        # 根据配置决定是否传入 URL / pnk_code，到 JSON body 中
        if config.ISTORIC_PRETURI_URL_PARAM:
            # 插件通常会先对 URL 做 encodeURIComponent，这里进行同样的编码
            encoded_url = quote(product_url, safe="")
            payload[config.ISTORIC_PRETURI_URL_PARAM] = encoded_url
        if config.ISTORIC_PRETURI_PNK_PARAM and pnk_code:
            payload[config.ISTORIC_PRETURI_PNK_PARAM] = pnk_code

        headers: Dict[str, str] = {
            "User-Agent": "emag-erp-bot/1.0 (+https://emag.ro)",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
        }
        # 简单支持 Bearer Token / API Key 场景
        if config.ISTORIC_PRETURI_API_KEY:
            headers["Authorization"] = f"Bearer {config.ISTORIC_PRETURI_API_KEY}"

        # #region agent log
        _agent_debug_log(
            hypothesis_id="D",
            location="istoric_preturi_client.py:_request_once:before-request",
            message="准备发起 Istoric Preturi 请求",
            data={
                "endpoint": config.ISTORIC_PRETURI_ENDPOINT,
                "payload": payload,
                "has_api_key": bool(config.ISTORIC_PRETURI_API_KEY),
            },
        )
        # #endregion

        try:
            # 采用 POST JSON 方式，与浏览器插件保持一致
            resp = requests.post(
                config.ISTORIC_PRETURI_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=config.ISTORIC_PRETURI_TIMEOUT,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(
                f"[IstoricPreturi] 请求失败 url={product_url}, endpoint={config.ISTORIC_PRETURI_ENDPOINT}, "
                f"错误: {e}"
            )
            # #region agent log
            _agent_debug_log(
                hypothesis_id="E",
                location="istoric_preturi_client.py:_request_once:request-exception",
                message="Istoric Preturi 请求异常",
                data={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            # #endregion
            raise

        try:
            data = resp.json()
        except Exception as e:
            logger.warning(
                f"[IstoricPreturi] 响应解析 JSON 失败 url={product_url}, 错误: {e}, text={resp.text[:200]!r}"
            )
            # #region agent log
            _agent_debug_log(
                hypothesis_id="F",
                location="istoric_preturi_client.py:_request_once:json-error",
                message="Istoric Preturi 响应 JSON 解析失败",
                data={
                    "status_code": resp.status_code,
                    "text_sample": resp.text[:200],
                },
            )
            # #endregion
            return None

        # 根据配置的路径提取上架日期字段
        raw_value = _get_value_by_path(data, config.ISTORIC_PRETURI_LISTED_AT_PATH)
        if raw_value is None:
            logger.info(
                f"[IstoricPreturi] 响应中未找到上架日期字段 path={config.ISTORIC_PRETURI_LISTED_AT_PATH}, "
                f"url={product_url}"
            )
            # #region agent log
            _agent_debug_log(
                hypothesis_id="G",
                location="istoric_preturi_client.py:_request_once:no-listed-at",
                message="JSON 中未找到上架日期字段",
                data={
                    "path": config.ISTORIC_PRETURI_LISTED_AT_PATH,
                    "json_sample": data,
                },
            )
            # #endregion
            return None

        listed_at = _parse_date(raw_value)
        if not listed_at:
            logger.info(
                f"[IstoricPreturi] 无法解析上架日期 raw={raw_value!r}, "
                f"path={config.ISTORIC_PRETURI_LISTED_AT_PATH}, url={product_url}"
            )
            # #region agent log
            _agent_debug_log(
                hypothesis_id="H",
                location="istoric_preturi_client.py:_request_once:parse-failed",
                message="无法解析上架日期",
                data={
                    "raw_value": raw_value,
                    "path": config.ISTORIC_PRETURI_LISTED_AT_PATH,
                    "date_format": config.ISTORIC_PRETURI_DATE_FORMAT,
                },
            )
            # #endregion
            return None

        # #region agent log
        _agent_debug_log(
            hypothesis_id="I",
            location="istoric_preturi_client.py:_request_once:success",
            message="成功解析上架日期",
            data={
                "product_url": product_url,
                "listed_at_iso": listed_at.isoformat(),
            },
        )
        # #endregion
        return listed_at

    try:
        return retry_manager.execute_with_retry(_request_once, task_id=None)
    except Exception as e:
        logger.warning(
            f"[IstoricPreturi] 多次重试仍然失败，放弃获取上架日期 url={product_url}, 错误: {e}"
        )
        return None



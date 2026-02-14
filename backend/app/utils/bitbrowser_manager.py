"""BitBrowser 窗口管理器

负责：
- 管理通过 BitBrowser 创建的浏览器窗口
- 为爬虫分配/释放窗口（类似 proxy_manager 的独占代理）
- 通过 BitBrowser 本地 API 打开/关闭窗口，并返回 CDP WebSocket 地址

说明：
- 仅在 config.BITBROWSER_ENABLED = True 时启用
- 优先通过 BITBROWSER_GROUP_ID 从窗口组动态拉取窗口 ID
- 若 GROUP_ID 为空则 fallback 到 BITBROWSER_WINDOW_IDS 手动配置
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from app.config import config

logger = logging.getLogger(__name__)


@dataclass
class BitBrowserWindowInfo:
    """BitBrowser 窗口状态信息"""

    window_id: str
    ws_url: Optional[str] = None  # CDP WebSocket 地址
    in_use: bool = False
    restart_count: int = 0
    last_restart_at: float = 0.0
    cool_down_until: float = 0.0  # 窗口冷却截止时间（用于处理临时网络/代理问题）
    task_count: int = 0  # 自上次重启以来已完成的任务数


class BitBrowserManager:
    """BitBrowser 窗口管理器（线程安全单例）"""

    _instance: Optional["BitBrowserManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "BitBrowserManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(BitBrowserManager, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        # 避免多次初始化
        if getattr(self, "_initialized", False):
            return

        self._api_url: str = getattr(config, "BITBROWSER_API_URL", "http://127.0.0.1:54345")
        self._group_id: str = getattr(config, "BITBROWSER_GROUP_ID", "")
        self._max_restart: int = int(getattr(config, "BITBROWSER_MAX_RESTART_COUNT", 10))
        self._restart_delay: int = int(getattr(config, "BITBROWSER_RESTART_DELAY", 5))
        self._lock = threading.RLock()
        self._window_available = threading.Condition(self._lock)  # 窗口可用通知

        # ── 获取窗口 ID 列表 ──
        # 优先从窗口组动态拉取；fallback 到手动配置
        raw_ids: List[str] = []
        if self._group_id and config.BITBROWSER_ENABLED:
            try:
                raw_ids = self._fetch_window_ids_from_group(self._group_id)
                logger.info(
                    "[BitBrowser] 从窗口组 %s 获取到 %d 个窗口",
                    self._group_id,
                    len(raw_ids),
                )
            except Exception as e:
                logger.error(
                    "[BitBrowser] 从窗口组获取窗口失败: %s，将 fallback 到 WINDOW_IDS 配置", e
                )

        if not raw_ids:
            raw_ids = [
                wid.strip()
                for wid in (getattr(config, "BITBROWSER_WINDOW_IDS", []) or [])
                if wid.strip()
            ]

        self._windows: Dict[str, BitBrowserWindowInfo] = {
            wid: BitBrowserWindowInfo(window_id=wid) for wid in raw_ids
        }
        self._initialized = True

        if not self._windows:
            logger.warning(
                "BitBrowserManager 初始化时没有可用窗口 (GROUP_ID=%s, WINDOW_IDS 均为空)",
                self._group_id or "(未设置)",
            )
        else:
            logger.info(
                "BitBrowserManager 初始化完成，窗口数: %d, API: %s, GROUP_ID: %s",
                len(self._windows),
                self._api_url,
                self._group_id or "(未使用)",
            )

    # ------------------------------------------------------------------ #
    # 窗口组动态拉取
    # ------------------------------------------------------------------ #

    def _fetch_window_ids_from_group(self, group_id: str) -> List[str]:
        """
        调用 BitBrowser 本地 API 获取指定窗口组下的所有窗口 ID

        API: POST {API_URL}/browser/list
        Body: {"page": 0, "pageSize": 100, "groupId": "xxx"}
        Response: {"success": true, "data": {"list": [{"id": "...", ...}], "totalNum": N}}
        """
        url = self._api_url.rstrip("/") + "/browser/list"
        all_ids: List[str] = []
        page = 0
        page_size = 100

        while True:
            resp = requests.post(
                url,
                json={"page": page, "pageSize": page_size, "groupId": group_id},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            items: list = []
            # 兼容两种常见返回格式
            payload = data.get("data") if isinstance(data, dict) else None
            if isinstance(payload, dict):
                items = payload.get("list", [])
            elif isinstance(payload, list):
                items = payload

            for item in items:
                wid = ""
                if isinstance(item, dict):
                    wid = item.get("id", "").strip()
                if wid:
                    all_ids.append(wid)

            # 判断是否还有下一页
            total = 0
            if isinstance(payload, dict):
                total = payload.get("totalNum", 0)
            if len(all_ids) >= total or len(items) < page_size:
                break
            page += 1

        return all_ids

    def refresh_windows(self) -> int:
        """
        运行时重新拉取窗口组（可由外部调用来动态扩缩窗口池）

        Returns:
            当前可用窗口数
        """
        if not self._group_id or not config.BITBROWSER_ENABLED:
            return len(self._windows)

        with self._lock:
            try:
                new_ids = self._fetch_window_ids_from_group(self._group_id)
            except Exception as e:
                logger.error("[BitBrowser] refresh_windows 失败: %s", e)
                return len(self._windows)

            # 新增窗口
            for wid in new_ids:
                if wid not in self._windows:
                    self._windows[wid] = BitBrowserWindowInfo(window_id=wid)
                    logger.info("[BitBrowser] 新增窗口: %s", wid)

            # 移除已不在组中的窗口（只移除未在使用中的）
            removed = []
            for wid in list(self._windows.keys()):
                if wid not in new_ids and not self._windows[wid].in_use:
                    removed.append(wid)
                    del self._windows[wid]
            if removed:
                logger.info("[BitBrowser] 移除不在组中的窗口: %s", removed)

            logger.info("[BitBrowser] 窗口池刷新完成，当前窗口数: %d", len(self._windows))
            return len(self._windows)

    # ------------------------------------------------------------------ #
    # 对外公开方法
    # ------------------------------------------------------------------ #

    def acquire_exclusive_window(self, timeout: float = 120) -> Optional[Dict[str, str]]:
        """
        获取一个独占窗口（用于 ProductDataCrawler 等需要强隔离的场景）
        如果当前没有可用窗口，会阻塞等待直到有窗口释放或超时。

        Args:
            timeout: 最大等待时间（秒），默认120秒

        Returns:
            dict: {\"id\": window_id, \"ws\": cdp_ws_url} 或 None（超时）
        """
        if not config.BITBROWSER_ENABLED:
            return None

        deadline = time.time() + timeout

        with self._window_available:
            while True:
                # #region agent log
                import json as _json, time as _time
                _in_use_count = sum(1 for i in self._windows.values() if i.in_use)
                _free_count = len(self._windows) - _in_use_count
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json.dumps({"timestamp": int(_time.time()*1000), "location": "bitbrowser_manager.py:acquire_exclusive_window", "message": "Window pool state on acquire", "data": {"total": len(self._windows), "in_use": _in_use_count, "free": _free_count}, "hypothesisId": "H1", "runId": "post-fix"}) + "\n")
                # #endregion

                # 尝试找一个空闲窗口
                for wid, info in self._windows.items():
                    if not info.in_use:
                        try:
                            ws = self._ensure_window_open(info)
                            if not ws:
                                continue
                            info.in_use = True
                            logger.info("[BitBrowser] 分配独占窗口: %s, ws=%s", wid, ws)
                            return {"id": wid, "ws": ws}
                        except Exception as e:
                            logger.error("[BitBrowser] 分配独占窗口失败 - id=%s, error=%s", wid, e, exc_info=True)
                            continue

                # 没有空闲窗口，计算剩余等待时间
                remaining = deadline - time.time()
                if remaining <= 0:
                    logger.warning("[BitBrowser] 获取独占窗口超时（等待 %.0f 秒后仍无可用窗口）", timeout)
                    return None

                logger.debug("[BitBrowser] 所有窗口忙碌 (%d/%d)，等待释放... (剩余 %.1fs)",
                             _in_use_count, len(self._windows), remaining)
                # 阻塞等待，直到有窗口释放或超时
                self._window_available.wait(timeout=min(remaining, 10))

    def acquire_window(self) -> Optional[Dict[str, str]]:
        """
        获取一个共享窗口（用于 ProductLinkCrawler 等可复用场景）

        Returns:
            dict: {\"id\": window_id, \"ws\": cdp_ws_url} 或 None
        """
        if not config.BITBROWSER_ENABLED:
            return None

        with self._lock:
            # 允许共享时可以选择任意一个窗口（包括已经 in_use 的）
            for wid, info in self._windows.items():
                try:
                    ws = self._ensure_window_open(info)
                    if not ws:
                        continue
                    logger.debug("[BitBrowser] 获取共享窗口: %s, ws=%s", wid, ws)
                    return {"id": wid, "ws": ws}
                except Exception as e:
                    logger.error("[BitBrowser] 获取共享窗口失败 - id=%s, error=%s", wid, e, exc_info=True)
                    continue

            logger.warning("[BitBrowser] 没有可用的共享窗口")
            return None

    def release_window(self, window_id: str) -> None:
        """
        释放独占窗口。
        每个窗口连续完成 BITBROWSER_MAX_TASKS_PER_WINDOW 个任务后，
        自动关闭并重新打开窗口（相当于重启浏览器进程），清空内存/缓存/Cookie，
        保持每个窗口始终处于"刚启动"的最佳状态。
        """
        if not window_id:
            return
        with self._window_available:
            info = self._windows.get(window_id)
            if not info:
                logger.warning("[BitBrowser] 释放窗口失败，未知窗口ID: %s", window_id)
                return
            # #region agent log
            import json as _json, time as _time
            _was_in_use = info.in_use
            with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                _f.write(_json.dumps({"timestamp": int(_time.time()*1000), "location": "bitbrowser_manager.py:release_window", "message": "Releasing window", "data": {"window_id": window_id, "was_in_use": _was_in_use, "task_count": info.task_count}, "hypothesisId": "H2", "runId": "post-fix"}) + "\n")
            # #endregion
            if info.in_use:
                info.in_use = False
                info.task_count += 1
                logger.debug(
                    "[BitBrowser] 释放独占窗口: %s (task_count=%d)",
                    window_id, info.task_count,
                )

                # ── 主动轮换：达到任务上限后关闭并重新打开窗口 ──
                max_tasks = int(getattr(config, "BITBROWSER_MAX_TASKS_PER_WINDOW", 5))
                if info.task_count >= max_tasks:
                    logger.info(
                        "[BitBrowser] 窗口达到任务上限(%d/%d)，主动重启以清空浏览器状态 - id=%s",
                        info.task_count, max_tasks, window_id,
                    )
                    # #region agent log
                    try:
                        with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                            _f.write(_json.dumps({
                                "timestamp": int(_time.time() * 1000),
                                "location": "bitbrowser_manager.py:release_window:proactive_restart",
                                "message": "窗口达到任务上限，主动重启",
                                "data": {
                                    "window_id": window_id,
                                    "task_count": info.task_count,
                                    "max_tasks": max_tasks,
                                },
                                "hypothesisId": "H_proactive_restart",
                                "runId": "window-rotate",
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    try:
                        self._close_window_api(window_id)
                        time.sleep(2)  # 等待浏览器进程完全退出
                        ws = self._open_window_api(window_id)
                        info.ws_url = ws
                        info.task_count = 0
                        logger.info(
                            "[BitBrowser] 窗口主动重启成功 - id=%s, new_ws=%s",
                            window_id, ws,
                        )
                    except Exception as e:
                        logger.warning(
                            "[BitBrowser] 窗口主动重启失败，清除缓存待下次重新打开 - id=%s, error=%s",
                            window_id, e,
                        )
                        info.ws_url = None  # 清除缓存，下次 acquire 时 _ensure_window_open 会重新打开
                        info.task_count = 0  # 重置计数，避免反复尝试

                # 通知等待中的线程有窗口可用了
                self._window_available.notify()

    def restart_window(self, window_id: str) -> None:
        """重启指定窗口（关闭后重新打开）"""
        if not config.BITBROWSER_ENABLED or not window_id:
            return

        with self._lock:
            info = self._windows.get(window_id)
            if not info:
                logger.warning("[BitBrowser] 重启窗口失败，未知窗口ID: %s", window_id)
                return

            now = time.time()
            # 如果当前仍在冷却期内，直接跳过本次重启尝试
            if info.cool_down_until and now < info.cool_down_until:
                logger.debug(
                    "[BitBrowser] 窗口处于冷却期，跳过重启 - id=%s, remaining=%.2fs",
                    window_id,
                    info.cool_down_until - now,
                )
                return

            if info.restart_count >= self._max_restart:
                # 达到上限时，不永久废弃窗口，而是进入较长冷却期，稍后再尝试恢复
                info.cool_down_until = now + self._restart_delay * 12  # 例如 1 分钟冷却
                logger.error(
                    "[BitBrowser] 窗口重启次数已达上限，进入冷却期 - id=%s, count=%d, cool_down_until=%.0f",
                    window_id,
                    info.restart_count,
                    info.cool_down_until,
                )
                # 重置计数，冷却结束后允许再次尝试
                info.restart_count = 0
                return

            if now - info.last_restart_at < self._restart_delay:
                logger.debug(
                    "[BitBrowser] 两次重启间隔太近，跳过本次重启 - id=%s, interval=%.2f",
                    window_id,
                    now - info.last_restart_at,
                )
                return

            try:
                logger.info("[BitBrowser] 准备重启窗口: %s", window_id)
                self._close_window_api(window_id)
            except Exception as e:
                logger.warning("[BitBrowser] 关闭窗口时出错 (可忽略) - id=%s, error=%s", window_id, e)

            # 等待一小会再打开
            time.sleep(self._restart_delay)

            try:
                ws_url = self._open_window_api(window_id)
                info.ws_url = ws_url
                info.restart_count += 1
                info.last_restart_at = time.time()
                logger.info("[BitBrowser] 窗口重启成功 - id=%s, ws=%s", window_id, ws_url)
            except Exception as e:
                # 细分临时性错误与结构性错误
                err_msg = str(e)
                info.last_restart_at = time.time()

                # BitBrowser 返回“浏览器正在关闭中，请稍后操作”属于短暂状态，进入短冷却期后再试
                if "浏览器正在关闭中，请稍后操作" in err_msg:
                    info.cool_down_until = info.last_restart_at + self._restart_delay * 2
                    logger.warning(
                        "[BitBrowser] 窗口处于关闭中状态，进入短冷却期 - id=%s, cool_down_until=%.0f, error=%s",
                        window_id,
                        info.cool_down_until,
                        err_msg,
                    )
                    # 不增加 restart_count，认为是暂时性问题
                else:
                    # 结构性错误：增加失败计数，并视情况进入较长冷却
                    info.restart_count += 1
                    if info.restart_count >= self._max_restart:
                        info.cool_down_until = info.last_restart_at + self._restart_delay * 12
                        logger.error(
                            "[BitBrowser] 窗口重启多次失败，进入冷却期 - id=%s, count=%d, cool_down_until=%.0f, error=%s",
                            window_id,
                            info.restart_count,
                            info.cool_down_until,
                            err_msg,
                        )
                        # 重置计数，冷却结束后允许再次尝试
                        info.restart_count = 0
                    else:
                        logger.error(
                            "[BitBrowser] 窗口重启失败 - id=%s, count=%d, error=%s",
                            window_id,
                            info.restart_count,
                            err_msg,
                            exc_info=True,
                        )

    # ------------------------------------------------------------------ #
    # 内部辅助方法
    # ------------------------------------------------------------------ #

    def _ensure_window_open(self, info: BitBrowserWindowInfo) -> Optional[str]:
        """
        确保窗口已打开并有有效 ws_url。
        会通过 TCP 探活检测缓存的 ws_url 是否可达，不可达则清除缓存并重新打开。
        """
        # 如果处于冷却期，跳过
        if info.cool_down_until and time.time() < info.cool_down_until:
            return None

        if info.ws_url:
            if self._is_ws_alive(info.ws_url):
                return info.ws_url
            else:
                # #region agent log
                import json as _json_ws, time as _time_ws
                try:
                    with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                        _f.write(_json_ws.dumps({"timestamp": int(_time_ws.time()*1000), "location": "bitbrowser_manager.py:_ensure_window_open:stale_ws", "message": "ws_url不可达，清除缓存并重新打开", "data": {"window_id": info.window_id, "stale_ws_url": info.ws_url}, "hypothesisId": "H3_stale_ws", "runId": "p1p2-fix"}, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                logger.warning(
                    "[BitBrowser] ws_url 不可达，重新打开窗口 - id=%s, stale_ws=%s",
                    info.window_id, info.ws_url,
                )
                info.ws_url = None  # 清除陈旧缓存

        # 通过 API 重新打开
        try:
            ws = self._open_window_api(info.window_id)
            info.ws_url = ws
            # #region agent log
            import json as _json_ws2, time as _time_ws2
            try:
                with open(r"d:\emag_erp\.cursor\debug.log", "a", encoding="utf-8") as _f:
                    _f.write(_json_ws2.dumps({"timestamp": int(_time_ws2.time()*1000), "location": "bitbrowser_manager.py:_ensure_window_open:reopened", "message": "窗口重新打开成功", "data": {"window_id": info.window_id, "new_ws_url": ws}, "hypothesisId": "H4_ws_reopen", "runId": "p1p2-fix"}, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            return ws
        except Exception as e:
            logger.error("[BitBrowser] 打开窗口失败 - id=%s, error=%s", info.window_id, e)
            return None

    def _is_ws_alive(self, ws_url: str) -> bool:
        """快速检查 WebSocket 地址是否可达（TCP 探活，超时 2 秒）"""
        import socket
        try:
            from urllib.parse import urlparse
            parsed = urlparse(ws_url)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port
            if not port:
                return False
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception:
            return False

    def _open_window_api(self, window_id: str) -> str:
        """
        调用 BitBrowser API 打开窗口，并返回 CDP WebSocket 地址

        典型本地 API：
        - POST {API_URL}/browser/open  body: {\"id\": window_id}
        - 响应中常见字段：\"ws\", \"wsEndpoint\", \"debuggerAddress\" 等
        """
        url = self._api_url.rstrip("/") + "/browser/open"
        logger.debug("[BitBrowser] 打开窗口 API 调用: %s id=%s", url, window_id)

        resp = requests.post(
            url,
            json={"id": window_id},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}

        # 尝试从常见字段中提取 ws_url
        ws_url = None
        # 常见返回结构: {\"data\": {\"ws\": \"ws://...\"}} 或 {\"data\": {\"debuggerAddress\": \"ws://...\"}}
        candidate = data.get("data") if isinstance(data, dict) else None
        if isinstance(candidate, dict):
            ws_url = (
                candidate.get("ws")
                or candidate.get("wsEndpoint")
                or candidate.get("debuggerAddress")
            )
        # 备用：直接在顶层查找
        if not ws_url and isinstance(data, dict):
            ws_url = (
                data.get("ws")
                or data.get("wsEndpoint")
                or data.get("debuggerAddress")
            )

        if not ws_url:
            raise RuntimeError(f"BitBrowser 打开窗口返回中未找到 ws 地址: {data}")

        logger.info("[BitBrowser] 窗口已打开 - id=%s, ws=%s", window_id, ws_url)
        return ws_url

    def _close_window_api(self, window_id: str) -> None:
        """调用 BitBrowser API 关闭窗口"""
        url = self._api_url.rstrip("/") + "/browser/close"
        logger.debug("[BitBrowser] 关闭窗口 API 调用: %s id=%s", url, window_id)

        try:
            resp = requests.post(
                url,
                json={"id": window_id},
                timeout=10,
            )
            # 不严格校验返回结果，只记录日志
            if resp.status_code != 200:
                logger.warning(
                    "[BitBrowser] 关闭窗口返回非 200 状态码: %s, body=%s",
                    resp.status_code,
                    resp.text[:200],
                )
        except Exception as e:
            logger.warning("[BitBrowser] 调用关闭窗口接口失败 - id=%s, error=%s", window_id, e)


# 全局单例
bitbrowser_manager = BitBrowserManager()



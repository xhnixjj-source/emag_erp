"""Service for backfilling listed_at for FilterPool records.

该模块封装了通过 Istoric Preturi 接口为 FilterPool 记录回填上架日期的逻辑，
供一次性脚本和定时任务复用。
"""

import logging
import time
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from app.config import config
from app.models.product import FilterPool
from app.services.istoric_preturi_client import get_listed_at_via_browser
from app.utils.playwright_manager import get_playwright_pool
from app.utils.bitbrowser_manager import bitbrowser_manager

logger = logging.getLogger(__name__)


def _classify_error_type(exc: Exception) -> str:
    """根据异常类型粗略分类错误类型，便于排查。

    返回值示例：timeout / http_error / parse_failed / unknown
    """
    text = type(exc).__name__.lower()
    msg = str(exc).lower()

    if "timeout" in text or "timeout" in msg:
        return "timeout"
    if "connection" in text or "connection" in msg or "disconnected" in msg:
        return "host_disconnected"
    if "http" in text or "status_code" in msg or "bad gateway" in msg:
        return "http_error"
    if "json" in text or "parse" in text or "html" in msg:
        return "parse_failed"
    return "unknown"


def process_filterpool_item(db: Session, item: FilterPool, page=None) -> Tuple[bool, Optional[str]]:
    """为单条 FilterPool 记录尝试获取 listed_at，并更新状态字段（使用浏览器方式）。

    Args:
        db: 数据库会话
        item: FilterPool 记录
        page: Playwright 页面对象（可选，如果提供则复用，否则创建新的）

    Returns:
        (success, error_type)
        - success: 是否成功获取并写入 listed_at
        - error_type: 如果失败且为异常，则返回错误类型字符串；否则为 None
    """
    context = None
    created_page = None
    try:
        # 如果没有提供页面，创建一个新的
        if page is None:
            playwright_pool = get_playwright_pool()
            
            # 获取浏览器上下文（优先使用 BitBrowser，否则使用无代理模式）
            if config.BITBROWSER_ENABLED:
                window_info = bitbrowser_manager.acquire_window()
                if window_info:
                    context = playwright_pool.acquire_context(
                        cdp_url=window_info['ws'],
                        window_id=window_info['id'],
                    )
                    logger.debug(f"[ListedAt] 使用 BitBrowser 窗口: {window_info['id']}")
            else:
                # 不使用代理，直接创建浏览器上下文（模拟人工点击）
                context = playwright_pool.acquire_context(proxy=None)
                logger.debug(f"[ListedAt] 使用无代理浏览器上下文")
            
            if not context:
                raise RuntimeError("无法获取浏览器上下文")
            
            # 创建页面
            created_page = context.new_page()
            page = created_page
        
        # 使用浏览器方式获取上架日期（模拟人工点击）
        listed_at = get_listed_at_via_browser(page, item.product_url)
        
        if listed_at:
            item.listed_at = listed_at
            item.listed_at_status = "success"
            item.listed_at_error_type = None
            logger.info(
                "[ListedAt] 获取上架日期成功 id=%s, url=%s, listed_at=%s",
                item.id,
                item.product_url,
                listed_at.isoformat(),
            )
            return True, None

        # get_listed_at_via_browser 没抛异常但返回 None，视为"正常无上架日期"
        item.listed_at_status = "not_found"
        item.listed_at_error_type = None
        logger.info(
            "[ListedAt] 浏览器方式正常但未找到上架日期 id=%s, url=%s",
            item.id,
            item.product_url,
        )
        return False, None
    except Exception as e:  # noqa: BLE001
        error_type = _classify_error_type(e)
        item.listed_at_status = "error"
        item.listed_at_error_type = error_type
        logger.warning(
            "[ListedAt] 获取上架日期异常 id=%s, url=%s, error_type=%s, error=%s",
            item.id,
            item.product_url,
            error_type,
            e,
        )
        return False, error_type
    finally:
        # 清理资源（只清理我们创建的）
        if created_page:
            try:
                created_page.close()
            except Exception:
                pass
        if context:
            try:
                playwright_pool = get_playwright_pool()
                playwright_pool.release_context(context)
            except Exception:
                pass


def run_backfill_once(
    db: Session,
    batch_size: int,
    sleep_seconds: float = 0.0,
) -> Tuple[int, int, int]:
    """执行一次增量回填任务（使用浏览器方式，模拟人工点击）。

    逻辑：
    1. 扫描 filter_pool 表中 listed_at 为空的记录（按 id 升序），
       仅处理 listed_at_status in ('pending', 'error') 的记录；
    2. 按批次使用浏览器方式获取上架日期（复用同一个浏览器页面以提高效率）；
    3. 根据结果更新 listed_at / listed_at_status / listed_at_error_type 字段；
    4. 为避免对对方接口产生过大压力，批次内每条之间可加入轻量 sleep。

    Returns:
        (processed_count, success_count, error_count)
    """
    if config.DISABLE_LISTED_AT or not config.ISTORIC_PRETURI_ENABLED:
        logger.info(
            "[ListedAt] 全局上架日期开关关闭（DISABLE_LISTED_AT=%s, ISTORIC_PRETURI_ENABLED=%s），跳过本次回填",
            config.DISABLE_LISTED_AT,
            config.ISTORIC_PRETURI_ENABLED,
        )
        return 0, 0, 0

    q = (
        db.query(FilterPool)
        .filter(FilterPool.listed_at.is_(None))
        .filter(
            (FilterPool.listed_at_status.is_(None))
            | (FilterPool.listed_at_status.in_(["pending", "error"]))
        )
        .order_by(FilterPool.id)
        .limit(batch_size)
    )
    items = q.all()
    if not items:
        logger.info("[ListedAt] 当前没有需要回填上架日期的 FilterPool 记录")
        return 0, 0, 0

    processed = 0
    success = 0
    error_count = 0

    # 为整个批次创建一个浏览器页面，复用以提高效率
    context = None
    page = None
    try:
        playwright_pool = get_playwright_pool()
        
        # 获取浏览器上下文（优先使用 BitBrowser，否则使用无代理模式）
        if config.BITBROWSER_ENABLED:
            window_info = bitbrowser_manager.acquire_window()
            if window_info:
                context = playwright_pool.acquire_context(
                    cdp_url=window_info['ws'],
                    window_id=window_info['id'],
                )
                logger.debug(f"[ListedAt] 使用 BitBrowser 窗口: {window_info['id']}")
        else:
            # 不使用代理，直接创建浏览器上下文（模拟人工点击）
            context = playwright_pool.acquire_context(proxy=None)
            logger.debug(f"[ListedAt] 使用无代理浏览器上下文")
        
        if context:
            page = context.new_page()
            logger.info(f"[ListedAt] 创建浏览器页面，开始处理批次（共 {len(items)} 条记录）")
    except Exception as e:
        logger.warning(f"[ListedAt] 创建浏览器上下文失败，将逐条创建: {e}")

    try:
        for item in items:
            # 如果成功创建了共享页面，则复用；否则 process_filterpool_item 会创建新的
            ok, err_type = process_filterpool_item(db, item, page=page)
            processed += 1
            if ok:
                success += 1
            if err_type:
                error_count += 1

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        db.commit()

        logger.info(
            "[ListedAt] 本次回填完成 processed=%s, success=%s, error=%s",
            processed,
            success,
            error_count,
        )
    finally:
        # 清理浏览器资源
        if page:
            try:
                page.close()
            except Exception:
                pass
        if context:
            try:
                playwright_pool = get_playwright_pool()
                playwright_pool.release_context(context)
            except Exception:
                pass

    return processed, success, error_count



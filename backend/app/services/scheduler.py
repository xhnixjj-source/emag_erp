"""Scheduler service for scheduled tasks"""
import logging
import threading
from concurrent.futures import as_completed
from datetime import datetime
from typing import List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from app.config import config
from app.database import SessionLocal
from app.models.monitor_pool import MonitorPool, MonitorHistory, MonitorStatus
from app.services.crawler import crawl_monitor_product
from app.services.operation_log_service import create_operation_log
from app.services.listed_at_backfill_service import run_backfill_once
from app.utils.thread_pool import thread_pool_manager
from app.services.monitor_trigger_job import (
    finalize_job,
    get_job_internal,
    update_job,
)

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone=config.SCHEDULER_TIMEZONE)


def _within_seven_day_monitor_window(monitor: MonitorPool, seven_days_ago, tz) -> bool:
    """
    是否满足「7 天监控窗口」。
    - 自有店铺（is_own_shop=True）：不限制 7 天，只要 ACTIVE 即参与监控。
    - 非自有：仅当 last_monitored_at（或 created_at）落在最近 7 天内。
    """
    if getattr(monitor, "is_own_shop", False):
        return True
    check_date = monitor.last_monitored_at if monitor.last_monitored_at else monitor.created_at
    return bool(check_date and check_date.replace(tzinfo=tz.utc) > seven_days_ago)


def start_scheduler():
    """Start scheduler and register scheduled tasks"""
    # Register daily monitor task
    scheduler.add_job(
        func=run_daily_monitor,
        trigger="cron",
        hour=config.MONITOR_SCHEDULE_HOUR,
        minute=config.MONITOR_SCHEDULE_MINUTE,
        timezone=config.SCHEDULER_TIMEZONE,
        id="daily_monitor",
        replace_existing=True,
        max_instances=1,
    )

    # 上架日期 FilterPool 回填：已关闭 APScheduler 定时任务（不注册 interval / 启动 bootstrap）。
    # 需要时可调用 run_listed_at_backfill_job() 手动跑；恢复定时需在此重新 add_job。

    logger.info(
        "Scheduler started. Daily monitor task scheduled at %02d:%02d",
        config.MONITOR_SCHEDULE_HOUR,
        config.MONITOR_SCHEDULE_MINUTE,
    )
    
    scheduler.start()


def stop_scheduler():
    """Stop scheduler"""
    scheduler.shutdown()
    logger.info("Scheduler stopped")


def run_daily_monitor():
    """
    Run daily monitor task - crawl all active monitor pool products
    Uses thread pool for concurrent execution
    非自有店铺：只监控 7 天内（last_monitored_at 或 created_at）；自有店铺不受 7 天限制。
    """
    db = SessionLocal()
    try:
        from datetime import timedelta
        from datetime import timezone as tz
        
        # 计算7天前的时间
        seven_days_ago = datetime.now(tz.utc) - timedelta(days=7)
        
        # 全部 ACTIVE，再按 7 天窗口过滤（自有店铺不受限）
        monitors = db.query(MonitorPool).filter(
            MonitorPool.status == MonitorStatus.ACTIVE
        ).all()
        
        # 过滤：非自有店铺仅 7 天内；自有店铺始终参与
        valid_monitors = []
        skipped_count = 0
        for monitor in monitors:
            if _within_seven_day_monitor_window(monitor, seven_days_ago, tz):
                valid_monitors.append(monitor)
            else:
                skipped_count += 1
                check_date = monitor.last_monitored_at if monitor.last_monitored_at else monitor.created_at
                logger.debug(
                    f"Monitor {monitor.id} skipped: exceeded 7 days limit (check_date: {check_date})"
                )

        if skipped_count > 0:
            logger.info(
                f"Skipped {skipped_count} non-own-shop monitors outside 7-day window"
            )

        if not valid_monitors:
            logger.info("No active monitors to process (after 7-day filter for non-own-shop)")
            return

        logger.info(
            f"Starting daily monitor task for {len(valid_monitors)} products "
            f"(skipped {skipped_count} non-own-shop outside 7-day window)"
        )
        
        # Process monitors using thread pool
        futures = []
        for monitor in valid_monitors:
            future = thread_pool_manager.submit(
                "monitor",
                _crawl_single_monitor,
                monitor.id,
                monitor.product_url
            )
            futures.append((monitor.id, future))
        
        # Wait for all tasks to complete
        success_count = 0
        failed_count = 0
        
        for monitor_id, future in futures:
            try:
                result = future.result(timeout=300)  # 5 minute timeout per task
                if result:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Error processing monitor {monitor_id}: {e}")
                failed_count += 1
        
        logger.info(
            f"Daily monitor task completed: {success_count} succeeded, "
            f"{failed_count} failed out of {len(valid_monitors)} total"
        )
        
        # Log operation
        try:
            create_operation_log(
                db=db,
                user_id=1,  # System user
                operation_type="monitor_scheduled",
                target_type="monitor_pool",
                operation_detail={
                    "monitor_count": len(valid_monitors),
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "skipped_count": skipped_count
                }
            )
        except Exception as e:
            logger.error(f"Failed to log operation: {e}")
            
    except Exception as e:
        logger.error(f"Error in daily monitor task: {e}", exc_info=True)
    finally:
        db.close()


def run_listed_at_backfill_job() -> None:
    """Periodic job: backfill listed_at for FilterPool records.

    每次运行时，只处理一小批未获取到上架日期的记录；如果上一轮还在执行，
    由于 max_instances=1 的限制，新一轮不会并发启动。
    """
    db = SessionLocal()
    try:
        processed, success, error_count = run_backfill_once(
            db=db,
            batch_size=config.LISTED_AT_BATCH_SIZE,
            sleep_seconds=config.LISTED_AT_SLEEP_SECONDS,
        )
        logger.info(
            "[Scheduler][ListedAt] 本次任务结束 processed=%s, success=%s, error=%s",
            processed,
            success,
            error_count,
        )
    except Exception as e:  # noqa: BLE001
        logger.error("[Scheduler][ListedAt] 任务执行异常: %s", e, exc_info=True)
    finally:
        db.close()


def _crawl_single_monitor(monitor_id: int, product_url: str) -> bool:
    """
    Crawl a single monitor product and save to history

    短连接校验 → 长时间爬取（不占用 Session）→ 短连接写入，避免批量监控时占满连接池。
    """
    db = SessionLocal()
    try:
        monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
        if not monitor or monitor.status != MonitorStatus.ACTIVE:
            logger.warning(f"Monitor {monitor_id} not found or not active")
            return False
    finally:
        db.close()

    product_data = crawl_monitor_product(monitor_id, product_url)
    if not product_data:
        logger.warning(f"Failed to crawl product data for monitor {monitor_id}")
        return False

    db = SessionLocal()
    try:
        history = MonitorHistory(
            monitor_pool_id=monitor_id,
            price=product_data.get('price'),
            stock=product_data.get('stock'),
            review_count=product_data.get('review_count'),
            rating=product_data.get('reviews_score'),
            shop_rank=product_data.get('shop_rank'),
            category_rank=product_data.get('category_rank'),
            ad_rank=product_data.get('ad_rank'),
            monitored_at=datetime.utcnow(),
        )
        db.add(history)
        monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
        if not monitor:
            logger.warning(f"Monitor {monitor_id} disappeared before save")
            return False
        monitor.last_monitored_at = datetime.utcnow()
        db.commit()
        logger.info(f"Successfully crawled and saved monitor {monitor_id}")
        return True
    except Exception as e:
        logger.error(f"Error crawling monitor {monitor_id}: {e}", exc_info=True)
        db.rollback()
        return False
    finally:
        db.close()


def run_manual_monitor_work(
    monitor_ids: Optional[List[int]] = None,
    job_id: Optional[str] = None,
) -> dict:
    """
    执行手动监控批次。若传入 job_id，则更新异步任务进度（供轮询）。

    非自有店铺：仅处理 7 天窗口内项；自有店铺（is_own_shop）不受 7 天限制。

    Args:
        monitor_ids: 要处理的监控池 ID；None 表示所有 ACTIVE
        job_id: 可选，异步任务 ID
    """
    from datetime import timedelta
    from datetime import timezone as tz

    db = SessionLocal()
    try:
        seven_days_ago = datetime.now(tz.utc) - timedelta(days=7)

        if monitor_ids:
            monitors = db.query(MonitorPool).filter(
                MonitorPool.id.in_(monitor_ids),
                MonitorPool.status == MonitorStatus.ACTIVE,
            ).all()
        else:
            monitors = db.query(MonitorPool).filter(
                MonitorPool.status == MonitorStatus.ACTIVE
            ).all()

        if not monitors:
            msg = "No active monitors to process"
            if job_id:
                finalize_job(
                    job_id,
                    "completed",
                    msg,
                    processed=0,
                    success=0,
                    failed=0,
                    skipped=0,
                )
            return {
                "message": msg,
                "processed": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
            }

        valid_monitors = []
        skipped_count = 0
        for monitor in monitors:
            if _within_seven_day_monitor_window(monitor, seven_days_ago, tz):
                valid_monitors.append(monitor)
            else:
                skipped_count += 1
                check_date = monitor.last_monitored_at if monitor.last_monitored_at else monitor.created_at
                logger.debug(
                    f"Monitor {monitor.id} skipped: exceeded 7 days limit (check_date: {check_date})"
                )

        if not valid_monitors:
            msg = f"No monitors to process after filter (skipped {skipped_count})"
            if job_id:
                finalize_job(
                    job_id,
                    "completed",
                    msg,
                    processed=0,
                    success=0,
                    failed=0,
                    skipped=skipped_count,
                )
            return {
                "message": msg,
                "processed": 0,
                "success": 0,
                "failed": 0,
                "skipped": skipped_count,
            }

        total = len(valid_monitors)
        if job_id:
            update_job(
                job_id,
                total=total,
                skipped=skipped_count,
                message=f"共 {total} 项，正在爬取…",
            )

        futures = []
        for monitor in valid_monitors:
            fut = thread_pool_manager.submit(
                "monitor",
                _crawl_single_monitor,
                monitor.id,
                monitor.product_url,
            )
            futures.append((monitor.id, fut))

        fut_to_mid = {fut: mid for mid, fut in futures}
        success_count = 0
        failed_count = 0
        processed = 0

        for fut in as_completed(fut_to_mid.keys()):
            mid = fut_to_mid[fut]
            try:
                result = fut.result(timeout=300)
                if result:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Error processing monitor {mid}: {e}")
                failed_count += 1
            processed += 1
            if job_id:
                update_job(
                    job_id,
                    processed=processed,
                    success=success_count,
                    failed=failed_count,
                    message=f"进行中 {processed}/{total}",
                )

        msg = (
            f"Processed {total} monitors "
            f"(skipped {skipped_count} non-own-shop outside 7-day window)"
        )
        result_dict = {
            "message": msg,
            "processed": total,
            "success": success_count,
            "failed": failed_count,
            "skipped": skipped_count,
        }
        if job_id:
            finalize_job(
                job_id,
                "completed",
                msg,
                processed=total,
                success=success_count,
                failed=failed_count,
                skipped=skipped_count,
            )
        return result_dict

    except Exception as e:
        logger.error(f"Error in manual monitor trigger: {e}", exc_info=True)
        err_msg = f"Error: {str(e)}"
        if job_id:
            finalize_job(
                job_id,
                "failed",
                err_msg,
                processed=0,
                success=0,
                failed=0,
                skipped=0,
            )
        return {
            "message": err_msg,
            "processed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
        }
    finally:
        db.close()


def trigger_monitor_manual(monitor_ids: Optional[List[int]] = None) -> dict:
    """同步执行手动监控（内部仍用线程池并发爬取，但会阻塞到全部完成）。"""
    return run_manual_monitor_work(monitor_ids, job_id=None)


def start_monitor_trigger_job_async(
    job_id: str, monitor_ids: Optional[List[int]]
) -> None:
    """后台线程执行监控批次，用于 HTTP 立即返回 + 轮询进度。"""

    def _run() -> None:
        try:
            result = run_manual_monitor_work(monitor_ids, job_id=job_id)
            j = get_job_internal(job_id)
            uid = j.get("user_id") if j else None
            if uid is not None:
                log_db = SessionLocal()
                try:
                    create_operation_log(
                        db=log_db,
                        user_id=uid,
                        operation_type="monitor_trigger_batch",
                        target_type="monitor_pool",
                        operation_detail={
                            "monitor_ids": monitor_ids,
                            "processed": result.get("processed", 0),
                            "success": result.get("success", 0),
                            "failed": result.get("failed", 0),
                            "job_id": job_id,
                        },
                    )
                except Exception as log_err:
                    logger.error(
                        "monitor trigger job operation log failed: %s", log_err, exc_info=True
                    )
                finally:
                    log_db.close()
        except Exception as e:
            logger.error(f"monitor trigger async job {job_id}: {e}", exc_info=True)
            finalize_job(job_id, "failed", str(e))

    threading.Thread(target=_run, name=f"monitor-trigger-{job_id[:8]}", daemon=True).start()


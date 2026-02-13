"""Scheduler service for scheduled tasks"""
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from app.config import config
from app.database import SessionLocal
from app.models.monitor_pool import MonitorPool, MonitorHistory, MonitorStatus
from app.services.crawler import crawl_monitor_product
from app.services.operation_log_service import create_operation_log
from app.services.listed_at_backfill_service import run_backfill_once
from app.utils.thread_pool import thread_pool_manager

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone=config.SCHEDULER_TIMEZONE)


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

    # Register listed_at backfill task (interval, every N minutes)
    if getattr(config, "LISTED_AT_BACKFILL_ENABLED", True):
        scheduler.add_job(
            func=run_listed_at_backfill_job,
            trigger="interval",
            minutes=config.LISTED_AT_BACKFILL_INTERVAL_MINUTES,
            id="listed_at_backfill",
            replace_existing=True,
            max_instances=1,  # 保证上一次没完成不会并发
            coalesce=True,  # 合并错过的触发
        )
        logger.info(
            "Scheduler listed_at_backfill task scheduled every %s minutes",
            config.LISTED_AT_BACKFILL_INTERVAL_MINUTES,
        )
        # 在服务启动时立即先跑一轮 listed_at 回填
        scheduler.add_job(
            func=run_listed_at_backfill_job,
            trigger="date",
            run_date=datetime.now(),
            id="listed_at_backfill_bootstrap",
            replace_existing=True,
            max_instances=1,
        )

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
    只监控7天内的产品（从第一次监控成功的时间或创建时间开始计算）
    """
    db = SessionLocal()
    try:
        from datetime import timedelta
        from datetime import timezone as tz
        
        # 计算7天前的时间
        seven_days_ago = datetime.now(tz.utc) - timedelta(days=7)
        
        # Get all active monitors that are within 7 days
        # 如果 last_monitored_at 存在，使用它；否则使用 created_at
        monitors = db.query(MonitorPool).filter(
            MonitorPool.status == MonitorStatus.ACTIVE
        ).all()
        
        # 过滤出7天内的监控项
        valid_monitors = []
        skipped_count = 0
        for monitor in monitors:
            # 如果已经监控过，使用 last_monitored_at；否则使用 created_at
            check_date = monitor.last_monitored_at if monitor.last_monitored_at else monitor.created_at
            if check_date and check_date.replace(tzinfo=tz.utc) > seven_days_ago:
                valid_monitors.append(monitor)
            else:
                skipped_count += 1
                logger.debug(f"Monitor {monitor.id} skipped: exceeded 7 days limit (check_date: {check_date})")
        
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} monitors that exceeded 7 days limit")
        
        if not valid_monitors:
            logger.info("No active monitors within 7 days to process")
            return
        
        logger.info(f"Starting daily monitor task for {len(valid_monitors)} products (skipped {skipped_count} that exceeded 7 days)")
        
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
    
    Args:
        monitor_id: Monitor pool ID
        product_url: Product URL to crawl
        
    Returns:
        True if successful, False otherwise
    """
    db = SessionLocal()
    try:
        # Verify monitor still exists and is active
        monitor = db.query(MonitorPool).filter(MonitorPool.id == monitor_id).first()
        if not monitor or monitor.status != MonitorStatus.ACTIVE:
            logger.warning(f"Monitor {monitor_id} not found or not active")
            return False
        
        # Crawl product data
        product_data = crawl_monitor_product(monitor_id, product_url, db)
        
        if not product_data:
            logger.warning(f"Failed to crawl product data for monitor {monitor_id}")
            return False
        
        # Create monitor history record
        # 注意：ProductDataCrawler返回的字段名可能与MonitorHistory不同，需要映射
        # 只保存6个核心监控字段：价格、库存、评分、店铺排名、类目排名、广告排名
        history = MonitorHistory(
            monitor_pool_id=monitor_id,
            price=product_data.get('price'),
            stock=product_data.get('stock_count') or product_data.get('stock'),  # 新格式使用stock_count
            review_count=product_data.get('review_count'),  # 保留review_count用于兼容
            rating=product_data.get('reviews_score'),  # 评分字段
            shop_rank=product_data.get('store_rank') or product_data.get('shop_rank'),  # 新格式使用store_rank
            category_rank=product_data.get('category_rank'),
            ad_rank=product_data.get('ad_category_rank') or product_data.get('ad_rank'),  # 新格式使用ad_category_rank
            monitored_at=datetime.utcnow()
        )
        
        db.add(history)
        
        # Update monitor's last_monitored_at
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


def trigger_monitor_manual(monitor_ids: list[int] = None) -> dict:
    """
    Manually trigger monitoring for specific monitors or all active monitors
    只监控7天内的产品（从第一次监控成功的时间或创建时间开始计算）
    
    Args:
        monitor_ids: List of monitor IDs to process (None for all active)
        
    Returns:
        Dictionary with processing results
    """
    db = SessionLocal()
    try:
        from datetime import timedelta
        from datetime import timezone as tz
        
        # 计算7天前的时间
        seven_days_ago = datetime.now(tz.utc) - timedelta(days=7)
        
        if monitor_ids:
            monitors = db.query(MonitorPool).filter(
                MonitorPool.id.in_(monitor_ids),
                MonitorPool.status == MonitorStatus.ACTIVE
            ).all()
        else:
            monitors = db.query(MonitorPool).filter(
                MonitorPool.status == MonitorStatus.ACTIVE
            ).all()
        
        if not monitors:
            return {"message": "No active monitors to process", "processed": 0}
        
        # 过滤出7天内的监控项
        valid_monitors = []
        skipped_count = 0
        for monitor in monitors:
            # 如果已经监控过，使用 last_monitored_at；否则使用 created_at
            check_date = monitor.last_monitored_at if monitor.last_monitored_at else monitor.created_at
            if check_date and check_date.replace(tzinfo=tz.utc) > seven_days_ago:
                valid_monitors.append(monitor)
            else:
                skipped_count += 1
                logger.debug(f"Monitor {monitor.id} skipped: exceeded 7 days limit (check_date: {check_date})")
        
        if not valid_monitors:
            return {
                "message": f"No active monitors within 7 days to process (skipped {skipped_count})",
                "processed": 0,
                "skipped": skipped_count
            }
        
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
                result = future.result(timeout=300)
                if result:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Error processing monitor {monitor_id}: {e}")
                failed_count += 1
        
        return {
            "message": f"Processed {len(valid_monitors)} monitors (skipped {skipped_count} that exceeded 7 days)",
            "processed": len(valid_monitors),
            "success": success_count,
            "failed": failed_count,
            "skipped": skipped_count
        }
        
    except Exception as e:
        logger.error(f"Error in manual monitor trigger: {e}", exc_info=True)
        return {"message": f"Error: {str(e)}", "processed": 0}
    finally:
        db.close()


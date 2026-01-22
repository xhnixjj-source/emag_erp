"""Scheduler service for scheduled tasks"""
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from app.config import config
from app.database import SessionLocal
from app.models.monitor_pool import MonitorPool, MonitorHistory, MonitorStatus
from app.services.crawler import crawl_monitor_product
from app.services.operation_log_service import create_operation_log
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
        replace_existing=True
    )
    
    logger.info(
        f"Scheduler started. Daily monitor task scheduled at "
        f"{config.MONITOR_SCHEDULE_HOUR:02d}:{config.MONITOR_SCHEDULE_MINUTE:02d}"
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
    """
    db = SessionLocal()
    try:
        # Get all active monitors
        monitors = db.query(MonitorPool).filter(
            MonitorPool.status == MonitorStatus.ACTIVE
        ).all()
        
        if not monitors:
            logger.info("No active monitors to process")
            return
        
        logger.info(f"Starting daily monitor task for {len(monitors)} products")
        
        # Process monitors using thread pool
        futures = []
        for monitor in monitors:
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
            f"{failed_count} failed out of {len(monitors)} total"
        )
        
        # Log operation
        try:
            create_operation_log(
                db=db,
                user_id=1,  # System user
                operation_type="monitor_scheduled",
                target_type="monitor_pool",
                operation_detail={
                    "monitor_count": len(monitors),
                    "success_count": success_count,
                    "failed_count": failed_count
                }
            )
        except Exception as e:
            logger.error(f"Failed to log operation: {e}")
            
    except Exception as e:
        logger.error(f"Error in daily monitor task: {e}", exc_info=True)
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
        history = MonitorHistory(
            monitor_pool_id=monitor_id,
            price=product_data.get('price'),
            stock=product_data.get('stock_count') or product_data.get('stock'),  # 新格式使用stock_count
            review_count=product_data.get('review_count'),
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
    
    Args:
        monitor_ids: List of monitor IDs to process (None for all active)
        
    Returns:
        Dictionary with processing results
    """
    db = SessionLocal()
    try:
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
        
        # Process monitors using thread pool
        futures = []
        for monitor in monitors:
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
            "message": f"Processed {len(monitors)} monitors",
            "processed": len(monitors),
            "success": success_count,
            "failed": failed_count
        }
        
    except Exception as e:
        logger.error(f"Error in manual monitor trigger: {e}", exc_info=True)
        return {"message": f"Error: {str(e)}", "processed": 0}
    finally:
        db.close()


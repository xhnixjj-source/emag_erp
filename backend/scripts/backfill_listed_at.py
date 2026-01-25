"""一次性脚本：为历史 FilterPool 记录回填上架日期（listed_at）

使用说明（请在虚拟环境中执行，例如 backend 目录下）：

    python -m app.scripts.backfill_listed_at

脚本逻辑：
1. 扫描 filter_pool 表中 listed_at 为空的记录；
2. 按批次调用 Istoric Preturi 接口获取上架日期（通过 get_listed_at）；
3. 将成功获取到的日期写回数据库，失败的记录会跳过并打印日志；
4. 为避免对对方接口产生过大压力，批次之间会加入轻量级 sleep。
"""

import logging
import time
from typing import List

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.product import FilterPool
from app.services.istoric_preturi_client import get_listed_at

logger = logging.getLogger(__name__)


def _backfill_batch(db: Session, items: List[FilterPool]) -> int:
    """为一批 FilterPool 记录回填 listed_at，返回成功数量"""
    success = 0
    for item in items:
        try:
            listed_at = get_listed_at(item.product_url)
            if not listed_at:
                logger.info(
                    f"[回填] 未能获取上架日期，跳过 id={item.id}, url={item.product_url}"
                )
                continue
            item.listed_at = listed_at
            success += 1
            logger.info(
                f"[回填] 回填成功 id={item.id}, url={item.product_url}, listed_at={listed_at.isoformat()}"
            )
        except Exception as e:
            logger.warning(
                f"[回填] 回填上架日期失败 id={item.id}, url={item.product_url}, 错误: {e}"
            )
    db.commit()
    return success


def main(batch_size: int = 50, sleep_seconds: float = 1.0):
    db = SessionLocal()
    try:
        total_q = db.query(FilterPool).filter(FilterPool.listed_at.is_(None))
        total = total_q.count()
        logger.info(f"[回填] 共有 {total} 条记录 listed_at 为空，开始回填")

        offset = 0
        processed = 0
        total_success = 0

        while True:
            items = (
                total_q.order_by(FilterPool.id)
                .offset(offset)
                .limit(batch_size)
                .all()
            )
            if not items:
                break

            success = _backfill_batch(db, items)
            batch_count = len(items)
            processed += batch_count
            total_success += success
            logger.info(
                f"[回填] 本批处理 {batch_count} 条，成功 {success} 条，"
                f"累计处理 {processed}/{total} 条，累计成功 {total_success} 条"
            )

            offset += batch_count

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        logger.info(
            f"[回填完成] 共处理 {processed} 条记录，成功回填 {total_success} 条，"
            f"剩余 {total - processed} 条（可能在回填期间被其他进程修改）"
        )
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()



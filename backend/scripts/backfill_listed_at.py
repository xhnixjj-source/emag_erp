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
from typing import Tuple

from app.database import SessionLocal
from app.services.listed_at_backfill_service import run_backfill_once

logger = logging.getLogger(__name__)


def main(batch_size: int = 50, sleep_seconds: float = 1.0) -> Tuple[int, int, int]:
    db = SessionLocal()
    try:
        total_processed = 0
        total_success = 0
        total_error = 0

        while True:
            processed, success, error_count = run_backfill_once(
                db=db,
                batch_size=batch_size,
                sleep_seconds=sleep_seconds,
            )
            if processed == 0:
                break

            total_processed += processed
            total_success += success
            total_error += error_count

        logger.info(
            "[回填完成] 共处理 %s 条记录，成功回填 %s 条，异常 %s 条",
            total_processed,
            total_success,
            total_error,
        )

        return total_processed, total_success, total_error
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()



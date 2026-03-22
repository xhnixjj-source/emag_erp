"""
迁移：monitor_pool.is_own_shop、crawl_tasks.monitor_pool_id
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

from app.config import config
from app.database import Base

from app.models.product import FilterPool  # noqa: F401
from app.models.user import User  # noqa: F401
from app.models.crawl_task import CrawlTask, ErrorLog  # noqa: F401
from app.models.keyword import Keyword, KeywordLink  # noqa: F401
from app.models.monitor_pool import MonitorPool, MonitorHistory  # noqa: F401
from app.models.listing import ListingPool, ListingDetails, ProfitCalculation  # noqa: F401
from app.models.operation_log import OperationLog  # noqa: F401


def migrate_database():
    db_path = config.DATABASE_URL.replace("sqlite:///", "")
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_path)

    _dir = os.path.dirname(db_path)
    if _dir:
        os.makedirs(_dir, exist_ok=True)
    print(f"数据库文件路径: {db_path}")

    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as db:
        inspector = inspect(engine)

        if "monitor_pool" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("monitor_pool")]
            if "is_own_shop" not in cols:
                print("添加 monitor_pool.is_own_shop ...")
                db.execute(text("ALTER TABLE monitor_pool ADD COLUMN is_own_shop INTEGER DEFAULT 0 NOT NULL"))
                db.commit()
                print("[OK] is_own_shop")
            else:
                print("monitor_pool.is_own_shop 已存在，跳过")
        else:
            print("monitor_pool 表不存在，已由 create_all 处理")

        if "crawl_tasks" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("crawl_tasks")]
            if "monitor_pool_id" not in cols:
                print("添加 crawl_tasks.monitor_pool_id ...")
                db.execute(
                    text("ALTER TABLE crawl_tasks ADD COLUMN monitor_pool_id INTEGER REFERENCES monitor_pool(id)")
                )
                db.commit()
                print("[OK] monitor_pool_id")
            else:
                print("crawl_tasks.monitor_pool_id 已存在，跳过")

    print("迁移完成。")


if __name__ == "__main__":
    migrate_database()

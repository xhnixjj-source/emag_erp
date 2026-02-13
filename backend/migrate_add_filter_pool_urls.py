"""
数据库迁移脚本：为 filter_pool 表添加店铺介绍页链接、店铺链接和类目链接字段

新增字段：
- shop_intro_url: 店铺介绍页完整URL（/xxx/v?ref=see_vendor_page）
- shop_url: 店铺商品列表页完整URL（/vendors/vendor/xxx?...）
- category_url: 产品主类目页完整URL
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

from app.config import config
from app.database import Base

# 确保模型已加载
from app.models.product import FilterPool  # noqa: F401
from app.models.user import User  # noqa: F401
from app.models.crawl_task import CrawlTask, ErrorLog  # noqa: F401
from app.models.keyword import Keyword, KeywordLink  # noqa: F401
from app.models.monitor_pool import MonitorPool, MonitorHistory  # noqa: F401
from app.models.listing import ListingPool, ListingDetails, ProfitCalculation  # noqa: F401
from app.models.operation_log import OperationLog  # noqa: F401


def migrate_database():
    """
    为 filter_pool 表添加 shop_intro_url、shop_url 和 category_url 字段
    """
    # 获取数据库路径（当前项目主要使用 sqlite，保持与其他迁移脚本一致的写法）
    db_path = config.DATABASE_URL.replace("sqlite:///", "")
    if not os.path.isabs(db_path):
        # 假设是相对于 backend 目录
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_path)

    # 确保目录存在
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"数据库文件路径: {db_path}")

    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # 如果表不存在，创建所有表
    Base.metadata.create_all(bind=engine)
    print("数据库已初始化，如果不存在时会自动创建表结构")

    with SessionLocal() as db:
        inspector = inspect(engine)

        # 检查 filter_pool 表是否存在
        if "filter_pool" not in inspector.get_table_names():
            print("错误: filter_pool 表不存在")
            return

        columns = inspector.get_columns("filter_pool")
        column_names = [col["name"] for col in columns]

        # 添加 shop_intro_url 字段
        if "shop_intro_url" not in column_names:
            print("添加 shop_intro_url 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN shop_intro_url VARCHAR"))
                db.commit()
                print("[OK] shop_intro_url 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("shop_intro_url 字段已存在，跳过")

        # 添加 shop_url 字段
        if "shop_url" not in column_names:
            print("添加 shop_url 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN shop_url VARCHAR"))
                db.commit()
                print("[OK] shop_url 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("shop_url 字段已存在，跳过")

        # 添加 category_url 字段
        if "category_url" not in column_names:
            print("添加 category_url 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN category_url VARCHAR"))
                db.commit()
                print("[OK] category_url 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("category_url 字段已存在，跳过")

    print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()



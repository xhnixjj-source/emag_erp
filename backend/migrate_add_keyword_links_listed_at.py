"""
数据库迁移脚本：为 keyword_links 表添加上架日期相关字段
新增字段：
- listed_at: 上架日期（DateTime）
- listed_at_status: 上架日期获取状态（String）
- listed_at_error_type: 上架日期获取错误类型（String）
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from app.config import config
from app.database import Base

# 确保模型已加载，以便 Base.metadata 知道表结构
from app.models.product import FilterPool
from app.models.user import User
from app.models.crawl_task import CrawlTask, ErrorLog
from app.models.keyword import Keyword, KeywordLink
from app.models.monitor_pool import MonitorPool, MonitorHistory
from app.models.listing import ListingPool, ListingDetails, ProfitCalculation
from app.models.operation_log import OperationLog


def migrate_database():
    """
    为 keyword_links 表添加上架日期相关字段
    """
    # 获取数据库路径
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
        
        # 检查 keyword_links 表是否存在
        if 'keyword_links' not in inspector.get_table_names():
            print("错误: keyword_links 表不存在")
            return
        
        columns = inspector.get_columns('keyword_links')
        column_names = [col['name'] for col in columns]

        # 添加 listed_at 字段
        if 'listed_at' not in column_names:
            print("添加 listed_at 字段...")
            try:
                db.execute(text("ALTER TABLE keyword_links ADD COLUMN listed_at DATETIME"))
                db.commit()
                print("[OK] listed_at 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("listed_at 字段已存在，跳过")

        # 添加 listed_at_status 字段
        if 'listed_at_status' not in column_names:
            print("添加 listed_at_status 字段...")
            try:
                db.execute(text("ALTER TABLE keyword_links ADD COLUMN listed_at_status VARCHAR"))
                db.commit()
                print("[OK] listed_at_status 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("listed_at_status 字段已存在，跳过")

        # 添加 listed_at_error_type 字段
        if 'listed_at_error_type' not in column_names:
            print("添加 listed_at_error_type 字段...")
            try:
                db.execute(text("ALTER TABLE keyword_links ADD COLUMN listed_at_error_type VARCHAR"))
                db.commit()
                print("[OK] listed_at_error_type 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("listed_at_error_type 字段已存在，跳过")
    
    print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()


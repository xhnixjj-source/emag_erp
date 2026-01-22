"""
数据库迁移脚本：为 filter_pool 表添加新字段
新增字段：
- thumbnail_image: 产品缩略图URL
- brand: 品牌
- shop_name: 店铺名称
- is_fbe: 是否是FBE（Fulfilled by eMAG）
- competitor_count: 跟卖数
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
    为 filter_pool 表添加新字段
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
        
        # 检查 filter_pool 表是否存在
        if 'filter_pool' not in inspector.get_table_names():
            print("错误: filter_pool 表不存在")
            return
        
        columns = inspector.get_columns('filter_pool')
        column_names = [col['name'] for col in columns]

        # 添加 thumbnail_image 字段
        if 'thumbnail_image' not in column_names:
            print("添加 thumbnail_image 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN thumbnail_image VARCHAR"))
                db.commit()
                print("[OK] thumbnail_image 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("thumbnail_image 字段已存在，跳过")

        # 添加 brand 字段
        if 'brand' not in column_names:
            print("添加 brand 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN brand VARCHAR"))
                db.commit()
                print("[OK] brand 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("brand 字段已存在，跳过")

        # 添加 shop_name 字段
        if 'shop_name' not in column_names:
            print("添加 shop_name 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN shop_name VARCHAR"))
                db.commit()
                print("[OK] shop_name 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("shop_name 字段已存在，跳过")

        # 添加 is_fbe 字段
        if 'is_fbe' not in column_names:
            print("添加 is_fbe 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN is_fbe BOOLEAN DEFAULT 0"))
                db.commit()
                print("[OK] is_fbe 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("is_fbe 字段已存在，跳过")

        # 添加 competitor_count 字段
        if 'competitor_count' not in column_names:
            print("添加 competitor_count 字段...")
            try:
                db.execute(text("ALTER TABLE filter_pool ADD COLUMN competitor_count INTEGER DEFAULT 0"))
                db.commit()
                print("[OK] competitor_count 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("competitor_count 字段已存在，跳过")
    
    print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()


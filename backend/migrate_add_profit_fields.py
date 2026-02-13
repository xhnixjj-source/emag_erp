"""
数据库迁移脚本：为 profit_calculation 表添加新字段，为 listing_pool 表添加 REJECTED 状态支持

新增字段：
- chinese_name: 中文名
- model_number: 型号
- category_name: 类目名称
- length: 长 (cm)
- width: 宽 (cm)
- height: 高 (cm)
- weight: 重量 (kg)
- purchase_price: 采购价 (€)

注意：REJECTED 状态在 SQLite 中通过 Enum 存储，无需额外迁移，只需在代码中添加即可
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from app.config import config
from app.database import Base

# 确保模型已加载
from app.models.product import FilterPool
from app.models.user import User
from app.models.crawl_task import CrawlTask, ErrorLog
from app.models.keyword import Keyword, KeywordLink
from app.models.monitor_pool import MonitorPool, MonitorHistory
from app.models.listing import ListingPool, ListingDetails, ProfitCalculation
from app.models.operation_log import OperationLog


def migrate_database():
    """
    为 profit_calculation 表添加新字段
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
        
        # 检查 profit_calculation 表是否存在
        if 'profit_calculation' not in inspector.get_table_names():
            print("错误: profit_calculation 表不存在")
            return
        
        columns = inspector.get_columns('profit_calculation')
        column_names = [col['name'] for col in columns]

        # 添加 chinese_name 字段
        if 'chinese_name' not in column_names:
            print("添加 chinese_name 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN chinese_name VARCHAR"))
                db.commit()
                print("[OK] chinese_name 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("chinese_name 字段已存在，跳过")

        # 添加 model_number 字段
        if 'model_number' not in column_names:
            print("添加 model_number 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN model_number VARCHAR"))
                db.commit()
                print("[OK] model_number 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("model_number 字段已存在，跳过")

        # 添加 category_name 字段
        if 'category_name' not in column_names:
            print("添加 category_name 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN category_name VARCHAR"))
                db.commit()
                print("[OK] category_name 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("category_name 字段已存在，跳过")

        # 添加 length 字段
        if 'length' not in column_names:
            print("添加 length 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN length FLOAT"))
                db.commit()
                print("[OK] length 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("length 字段已存在，跳过")

        # 添加 width 字段
        if 'width' not in column_names:
            print("添加 width 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN width FLOAT"))
                db.commit()
                print("[OK] width 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("width 字段已存在，跳过")

        # 添加 height 字段
        if 'height' not in column_names:
            print("添加 height 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN height FLOAT"))
                db.commit()
                print("[OK] height 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("height 字段已存在，跳过")

        # 添加 weight 字段
        if 'weight' not in column_names:
            print("添加 weight 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN weight FLOAT"))
                db.commit()
                print("[OK] weight 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("weight 字段已存在，跳过")

        # 添加 purchase_price 字段
        if 'purchase_price' not in column_names:
            print("添加 purchase_price 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN purchase_price FLOAT"))
                db.commit()
                print("[OK] purchase_price 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("purchase_price 字段已存在，跳过")
    
    print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()


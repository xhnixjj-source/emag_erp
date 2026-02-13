"""
数据库迁移脚本：创建 profit_config 表用于存储利润测算的默认费用配置

新增表：
- profit_config: 存储头程物流费、订单处理费、仓储费、平台佣金、VAT等默认配置
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
from app.models.profit_config import ProfitConfig


def migrate_database():
    """
    创建 profit_config 表
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
        
        # 检查 profit_config 表是否存在
        if 'profit_config' not in inspector.get_table_names():
            print("创建 profit_config 表...")
            try:
                # 使用 SQLAlchemy 的 create_all 会自动创建表
                # 但为了确保字段正确，我们显式创建
                db.execute(text("""
                    CREATE TABLE profit_config (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        site VARCHAR NOT NULL DEFAULT 'emag_ro',
                        default_shipping_cost FLOAT NOT NULL DEFAULT 0.0,
                        default_order_fee FLOAT NOT NULL DEFAULT 0.0,
                        default_storage_fee FLOAT NOT NULL DEFAULT 0.0,
                        default_platform_commission FLOAT NOT NULL DEFAULT 0.0,
                        default_vat_rate FLOAT NOT NULL DEFAULT 0.0,
                        shipping_price_per_kg FLOAT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_by_user_id INTEGER,
                        FOREIGN KEY (updated_by_user_id) REFERENCES users(id)
                    )
                """))
                db.execute(text("CREATE INDEX ix_profit_config_site ON profit_config(site)"))
                db.commit()
                print("[OK] profit_config 表已创建")
            except Exception as e:
                print(f"创建表失败: {e}")
                db.rollback()
        else:
            print("profit_config 表已存在，跳过创建")
            
            # 检查字段是否存在，如果不存在则添加
            columns = inspector.get_columns('profit_config')
            column_names = [col['name'] for col in columns]
            
            # 检查并添加缺失的字段
            if 'shipping_price_per_kg' not in column_names:
                print("添加 shipping_price_per_kg 字段...")
                try:
                    db.execute(text("ALTER TABLE profit_config ADD COLUMN shipping_price_per_kg FLOAT"))
                    db.commit()
                    print("[OK] shipping_price_per_kg 字段已添加")
                except Exception as e:
                    print(f"添加字段失败: {e}")
                    db.rollback()
    
    print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()


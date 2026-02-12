"""
数据库迁移脚本：为 keyword_links 表添加 Chrome 插件扩展字段
新增字段：
- product_title: 产品标题
- brand: 品牌
- category: 类目
- commission_rate: 佣金比例(%)
- offer_count: 跟卖数
- purchase_price: 采购价
- last_offer_period: 最近offer周期
- tag: 标签（如 Super Hot）
- source: 来源 (keyword_search / chrome_extension)
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
    为 keyword_links 表添加 Chrome 插件扩展字段
    """
    db_path = config.DATABASE_URL.replace("sqlite:///", "")
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_path)
    
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"数据库文件路径: {db_path}")

    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    Base.metadata.create_all(bind=engine)
    print("数据库已初始化")

    new_columns = [
        ("product_title", "VARCHAR"),
        ("brand", "VARCHAR"),
        ("category", "VARCHAR"),
        ("commission_rate", "FLOAT"),
        ("offer_count", "INTEGER"),
        ("purchase_price", "FLOAT"),
        ("last_offer_period", "VARCHAR"),
        ("tag", "VARCHAR"),
        ("source", "VARCHAR DEFAULT 'keyword_search'"),
    ]

    with SessionLocal() as db:
        inspector = inspect(engine)
        
        if 'keyword_links' not in inspector.get_table_names():
            print("错误: keyword_links 表不存在")
            return
        
        columns = inspector.get_columns('keyword_links')
        column_names = [col['name'] for col in columns]

        for col_name, col_type in new_columns:
            if col_name not in column_names:
                print(f"添加 {col_name} 字段...")
                try:
                    db.execute(text(f"ALTER TABLE keyword_links ADD COLUMN {col_name} {col_type}"))
                    db.commit()
                    print(f"[OK] {col_name} 字段已创建")
                except Exception as e:
                    print(f"迁移失败: {e}")
                    db.rollback()
            else:
                print(f"{col_name} 字段已存在，跳过")

        # 将已有数据的 source 设置为 keyword_search
        try:
            db.execute(text("UPDATE keyword_links SET source = 'keyword_search' WHERE source IS NULL"))
            db.commit()
            print("[OK] 已有数据的 source 字段已更新为 'keyword_search'")
        except Exception as e:
            print(f"更新 source 字段失败: {e}")
            db.rollback()
    
    print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()


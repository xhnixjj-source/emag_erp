"""
数据库迁移脚本：为 emag_inbound_shipment 表添加 number_of_units 列
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

from app.config import config
from app.database import Base

# 确保模型已加载
from app.models.emag_sync import EmagInboundShipment


def migrate_database():
    """执行数据库迁移"""
    # 获取数据库路径
    db_path = config.DATABASE_URL.replace("sqlite:///", "")
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_path)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    print(f"数据库文件路径: {db_path}")

    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    with SessionLocal() as db:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()

        table_name = "emag_inbound_shipment"
        if table_name not in existing_tables:
            print(f"[ERROR] {table_name} 表不存在！请先运行初始化脚本。")
            return

        columns = inspector.get_columns(table_name)
        column_names = [col["name"] for col in columns]

        if "number_of_units" in column_names:
            print(f"[SKIP] {table_name}.number_of_units 列已存在，无需迁移。")
        else:
            print(f"添加 {table_name}.number_of_units 列...")
            try:
                db.execute(text(
                    f"ALTER TABLE {table_name} ADD COLUMN number_of_units INTEGER"
                ))
                db.commit()
                print(f"[OK] {table_name}.number_of_units 列已创建。")
            except Exception as e:
                print(f"[ERROR] {table_name}.number_of_units 迁移失败: {e}")
                db.rollback()

    print("\n[DONE] 迁移完成！")


if __name__ == "__main__":
    migrate_database()


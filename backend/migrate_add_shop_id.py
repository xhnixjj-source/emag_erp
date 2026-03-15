"""
数据库迁移脚本：多店铺支持

1. 创建 emag_shop 表（如果不存在）
2. 为以下表添加 shop_id 列：
   - emag_product
   - emag_order
   - emag_return
   - emag_inbound_shipment
   - ads_campaign
   - ads_adset
   - ads_product_performance
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

from app.config import config
from app.database import Base

# 确保所有模型已加载
from app.models.emag_sync import EmagShop, EmagAccount, EmagProduct, EmagOrder, EmagReturn, EmagInboundShipment, EmagInboundShipmentDetail  # noqa: F401
from app.models.emag_ads import AdsCampaign, AdsAdset, AdsProductPerformance  # noqa: F401


# 需要添加 shop_id 列的表
TABLES_NEEDING_SHOP_ID = [
    "emag_product",
    "emag_order",
    "emag_return",
    "emag_inbound_shipment",
    "ads_campaigns",
    "ads_adsets",
    "ads_product_performance",
]


def migrate_database():
    """执行多店铺支持的数据库迁移"""
    # 获取数据库路径
    db_path = config.DATABASE_URL.replace("sqlite:///", "")
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_path)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    print(f"数据库文件路径: {db_path}")

    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # 创建所有不存在的表（包括新的 emag_shop 表）
    Base.metadata.create_all(bind=engine)
    print("数据库已初始化（新表 emag_shop 如不存在已自动创建）")

    with SessionLocal() as db:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()

        # 确认 emag_shop 表已创建
        if "emag_shop" in existing_tables:
            print("[OK] emag_shop 表已存在")
        else:
            print("[ERROR] emag_shop 表创建失败！")
            return

        # 为每个表添加 shop_id 列
        for table_name in TABLES_NEEDING_SHOP_ID:
            if table_name not in existing_tables:
                print(f"[SKIP] {table_name} 表不存在，跳过")
                continue

            columns = inspector.get_columns(table_name)
            column_names = [col["name"] for col in columns]

            if "shop_id" in column_names:
                print(f"[SKIP] {table_name}.shop_id 已存在")
                continue

            print(f"添加 {table_name}.shop_id 列...")
            try:
                # SQLite ALTER TABLE 只支持添加列，不支持外键约束
                # shop_id 设为 nullable，默认 NULL（兼容已有数据）
                db.execute(text(
                    f"ALTER TABLE {table_name} ADD COLUMN shop_id INTEGER"
                ))
                db.commit()
                print(f"[OK] {table_name}.shop_id 列已创建")
            except Exception as e:
                print(f"[ERROR] {table_name}.shop_id 迁移失败: {e}")
                db.rollback()
                continue

            # 为 shop_id 创建索引
            index_name = f"idx_{table_name}_shop_id"
            try:
                db.execute(text(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (shop_id)"
                ))
                db.commit()
                print(f"[OK] 索引 {index_name} 已创建")
            except Exception as e:
                print(f"[WARN] 创建索引 {index_name} 失败 (可忽略): {e}")
                db.rollback()

        # ---------------------------------------------------------------
        # 处理唯一约束变更（SQLite 不支持 ALTER 修改约束，仅打印提示）
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("注意：以下唯一约束已在模型中更新为包含 shop_id，")
        print("但 SQLite 不支持修改已有约束。由于 shop_id 目前为 NULL，")
        print("现有数据不受影响。如果将来需要严格唯一约束，")
        print("需要重建表或导出数据后重新导入。")
        print("  - emag_product: UniqueConstraint('product_id', 'shop_id')")
        print("  - emag_return: UniqueConstraint('rma_id', 'shop_id')")
        print("  - emag_inbound_shipment: UniqueConstraint('reception_id', 'shop_id')")
        print("=" * 60)

    print("\n[DONE] 迁移完成！请重启后端服务。")


if __name__ == "__main__":
    migrate_database()


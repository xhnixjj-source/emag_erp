"""
数据库迁移脚本：创建 eMAG Marketplace API 同步相关表

新增表：
- emag_account: 存储 eMAG API 认证信息（系统级单账户）
- emag_product: 产品信息表（包含库存）
- emag_order: 订单信息表（包含订单产品明细）
- emag_return: 退货信息表（包含退货产品明细）
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from app.config import config
from app.database import Base

# 确保模型已加载
from app.models.emag_sync import EmagAccount, EmagProduct, EmagOrder, EmagReturn


def migrate_database():
    """
    创建 eMAG 同步相关表
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
        table_names = inspector.get_table_names()
        
        # 检查并创建 emag_account 表
        if 'emag_account' not in table_names:
            print("创建 emag_account 表...")
            try:
                db.execute(text("""
                    CREATE TABLE emag_account (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        platform VARCHAR(50) NOT NULL UNIQUE,
                        username VARCHAR(255) NOT NULL,
                        password VARCHAR(255) NOT NULL,
                        base_url VARCHAR(255) NOT NULL,
                        is_active INTEGER NOT NULL DEFAULT 1,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                db.execute(text("CREATE INDEX ix_emag_account_id ON emag_account(id)"))
                db.commit()
                print("[OK] emag_account 表已创建")
            except Exception as e:
                print(f"创建表失败: {e}")
                db.rollback()
        else:
            print("emag_account 表已存在，跳过创建")
        
        # 检查并创建 emag_product 表
        if 'emag_product' not in table_names:
            print("创建 emag_product 表...")
            try:
                db.execute(text("""
                    CREATE TABLE emag_product (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        product_id INTEGER NOT NULL UNIQUE,
                        pnk_code VARCHAR(255),
                        ean VARCHAR(255),
                        part_number VARCHAR(255),
                        name VARCHAR(255),
                        brand VARCHAR(255),
                        category_id INTEGER,
                        sale_price FLOAT,
                        vat_id INTEGER,
                        stock INTEGER,
                        status INTEGER,
                        warehouse_id INTEGER,
                        synced_at DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                db.execute(text("CREATE INDEX idx_emag_product_pnk ON emag_product(pnk_code)"))
                db.execute(text("CREATE INDEX idx_emag_product_ean ON emag_product(ean)"))
                db.execute(text("CREATE INDEX idx_emag_product_product_id ON emag_product(product_id)"))
                db.commit()
                print("[OK] emag_product 表已创建")
            except Exception as e:
                print(f"创建表失败: {e}")
                db.rollback()
        else:
            print("emag_product 表已存在，跳过创建")
        
        # 检查并创建 emag_order 表
        if 'emag_order' not in table_names:
            print("创建 emag_order 表...")
            try:
                db.execute(text("""
                    CREATE TABLE emag_order (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id INTEGER NOT NULL,
                        order_product_id INTEGER,
                        product_id INTEGER,
                        pnk_code VARCHAR(255),
                        ean VARCHAR(255),
                        order_status INTEGER,
                        payment_mode_id INTEGER,
                        customer_id INTEGER,
                        customer_name VARCHAR(255),
                        customer_email VARCHAR(255),
                        customer_phone VARCHAR(50),
                        billing_city VARCHAR(255),
                        shipping_city VARCHAR(255),
                        product_name VARCHAR(255),
                        quantity INTEGER,
                        sale_price FLOAT,
                        product_status INTEGER,
                        total_amount FLOAT,
                        order_date DATETIME,
                        order_updated_at DATETIME,
                        order_finalized_at DATETIME,
                        order_canceled_at DATETIME,
                        synced_at DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                db.execute(text("CREATE INDEX idx_emag_order_product_id ON emag_order(product_id)"))
                db.execute(text("CREATE INDEX idx_emag_order_pnk ON emag_order(pnk_code)"))
                db.execute(text("CREATE INDEX idx_emag_order_ean ON emag_order(ean)"))
                db.execute(text("CREATE INDEX idx_emag_order_order_id ON emag_order(order_id)"))
                db.execute(text("CREATE INDEX idx_emag_order_order_date ON emag_order(order_date)"))
                db.execute(text("CREATE INDEX idx_emag_order_status ON emag_order(order_status)"))
                db.commit()
                print("[OK] emag_order 表已创建")
            except Exception as e:
                print(f"创建表失败: {e}")
                db.rollback()
        else:
            print("emag_order 表已存在，跳过创建")
        
        # 检查并创建 emag_return 表
        if 'emag_return' not in table_names:
            print("创建 emag_return 表...")
            try:
                db.execute(text("""
                    CREATE TABLE emag_return (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        rma_id INTEGER NOT NULL UNIQUE,
                        order_id INTEGER,
                        order_product_id INTEGER,
                        product_id INTEGER,
                        pnk_code VARCHAR(255),
                        ean VARCHAR(255),
                        return_status INTEGER,
                        reason TEXT,
                        product_name VARCHAR(255),
                        quantity INTEGER,
                        sale_price FLOAT,
                        return_date DATETIME,
                        return_acknowledged_at DATETIME,
                        return_received_at DATETIME,
                        return_resolved_at DATETIME,
                        return_rejected_at DATETIME,
                        return_updated_at DATETIME,
                        synced_at DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                db.execute(text("CREATE INDEX idx_emag_return_product_id ON emag_return(product_id)"))
                db.execute(text("CREATE INDEX idx_emag_return_pnk ON emag_return(pnk_code)"))
                db.execute(text("CREATE INDEX idx_emag_return_ean ON emag_return(ean)"))
                db.execute(text("CREATE INDEX idx_emag_return_rma_id ON emag_return(rma_id)"))
                db.execute(text("CREATE INDEX idx_emag_return_order_id ON emag_return(order_id)"))
                db.execute(text("CREATE INDEX idx_emag_return_return_date ON emag_return(return_date)"))
                db.execute(text("CREATE INDEX idx_emag_return_status ON emag_return(return_status)"))
                db.commit()
                print("[OK] emag_return 表已创建")
            except Exception as e:
                print(f"创建表失败: {e}")
                db.rollback()
        else:
            print("emag_return 表已存在，跳过创建")
    
    print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()


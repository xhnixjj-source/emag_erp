"""
数据库迁移脚本：为 keyword_links 表添加 PNK_CODE、thumbnail_image 和 price 字段

使用方法：
    python migrate_add_link_fields.py
"""
import sqlite3
import os
from app.config import config

def migrate_database():
    """添加新字段到 keyword_links 表"""
    db_url = config.DATABASE_URL
    
    # 解析 SQLite 路径
    if db_url.startswith('sqlite:///'):
        db_path = db_url.replace('sqlite:///', '')
    elif db_url.startswith('sqlite://'):
        db_path = db_url.replace('sqlite://', '')
    else:
        db_path = db_url
    
    # 如果是相对路径，转换为绝对路径（相对于项目根目录）
    if not os.path.isabs(db_path):
        # 获取项目根目录（backend 的父目录）
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(backend_dir)
        # 处理 ./emag_erp.db 这种格式
        if db_path.startswith('./'):
            db_path = db_path[2:]
        db_path = os.path.join(project_root, db_path)
    
    # 如果项目根目录下的文件不存在，检查 backend 目录
    if not os.path.exists(db_path):
        backend_db = os.path.join(backend_dir, os.path.basename(db_path))
        if os.path.exists(backend_db):
            db_path = backend_db
    
    print(f"数据库文件路径: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"数据库文件不存在: {db_path}")
        print("首次运行，将在启动时自动创建表结构")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 检查字段是否已存在
        cursor.execute("PRAGMA table_info(keyword_links)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # 添加 PNK_CODE 字段（如果不存在）
        if 'PNK_CODE' not in columns and 'pnk_code' not in columns:
            print("添加 PNK_CODE 字段...")
            cursor.execute("ALTER TABLE keyword_links ADD COLUMN PNK_CODE TEXT")
            print("[OK] PNK_CODE 字段已添加")
        else:
            print("[OK] PNK_CODE 字段已存在")

        # 添加 thumbnail_image 字段（如果不存在）
        if 'thumbnail_image' not in columns:
            print("添加 thumbnail_image 字段...")
            cursor.execute("ALTER TABLE keyword_links ADD COLUMN thumbnail_image TEXT")
            print("[OK] thumbnail_image 字段已添加")
        else:
            print("[OK] thumbnail_image 字段已存在")
        
        # 添加 price 字段（如果不存在）
        if 'price' not in columns:
            print("添加 price 字段...")
            cursor.execute("ALTER TABLE keyword_links ADD COLUMN price REAL")
            print("[OK] price 字段已添加")
        else:
            print("[OK] price 字段已存在")
        
        conn.commit()
        print("\n迁移完成！")
        
    except Exception as e:
        conn.rollback()
        print(f"迁移失败: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate_database()


"""
数据迁移脚本：从 emag_tool 数据库迁移数据到 emag_erp 数据库

迁移内容：
1. screening_pool_products → filter_pool
2. screening_pool_daily_logs → monitor_history (需要先创建 monitor_pool 关联)

使用方法：
    python -m scripts.migrate_from_emag_tool [--dry-run] [--source-db PATH]

参数说明：
    --dry-run: 预览迁移，不实际执行
    --source-db: 源数据库路径，默认为 D:\\emag_tool\\emag.db
"""
import os
import sys
import argparse
import shutil
from datetime import datetime
from typing import Dict, Optional, List, Tuple

# 添加 backend 目录到 Python 路径
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, backend_dir)

import sqlite3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import config
from app.database import Base
from app.models.product import FilterPool
from app.models.monitor_pool import MonitorPool, MonitorHistory, MonitorStatus
from app.models.user import User


class MigrationResult:
    """迁移结果统计"""
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.total = 0
        self.inserted = 0
        self.skipped = 0
        self.failed = 0
        self.errors: List[str] = []
    
    def __str__(self):
        return (
            f"[{self.table_name}] "
            f"总计: {self.total}, 插入: {self.inserted}, "
            f"跳过: {self.skipped}, 失败: {self.failed}"
        )


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    解析日期字符串为 datetime 对象
    支持多种格式：YYYY-MM-DD, YYYY-MM-DD HH:MM:SS 等
    """
    if not date_str or date_str.strip() == '':
        return None
    
    date_formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%d/%m/%Y',
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    return None


def backup_database(db_path: str) -> str:
    """
    备份数据库文件
    返回备份文件路径
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"{db_path}.backup_{timestamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def get_existing_urls(session) -> set:
    """获取目标数据库中已存在的 product_url"""
    result = session.execute(text("SELECT product_url FROM filter_pool"))
    return {row[0] for row in result.fetchall()}


def get_pnk_to_url_mapping(source_conn: sqlite3.Connection) -> Dict[str, str]:
    """
    获取 PNK_CODE 到 URL 的映射关系
    """
    cursor = source_conn.cursor()
    cursor.execute("SELECT PNK_CODE, url FROM screening_pool_products")
    return {row[0]: row[1] for row in cursor.fetchall()}


def migrate_filter_pool(
    source_conn: sqlite3.Connection,
    session,
    dry_run: bool = False
) -> Tuple[MigrationResult, Dict[str, int]]:
    """
    迁移 screening_pool_products 到 filter_pool
    返回：(迁移结果, URL到filter_pool.id的映射)
    """
    result = MigrationResult("filter_pool")
    url_to_id: Dict[str, int] = {}
    
    # 获取已存在的 URL
    existing_urls = get_existing_urls(session)
    print(f"  目标数据库已有 {len(existing_urls)} 条记录")
    
    # 读取源数据
    cursor = source_conn.cursor()
    cursor.execute("""
        SELECT 
            url, title, image_url, brand, store_name, 
            listing_date, latest_review_date, has_resellers, 
            is_fbe, add_time
        FROM screening_pool_products
    """)
    
    rows = cursor.fetchall()
    result.total = len(rows)
    print(f"  源数据库有 {result.total} 条记录待迁移")
    
    for row in rows:
        url, title, image_url, brand, store_name, \
            listing_date, latest_review_date, has_resellers, \
            is_fbe, add_time = row
        
        # 检查是否已存在
        if url in existing_urls:
            result.skipped += 1
            # 获取已存在记录的 ID
            existing_record = session.execute(
                text("SELECT id FROM filter_pool WHERE product_url = :url"),
                {"url": url}
            ).fetchone()
            if existing_record:
                url_to_id[url] = existing_record[0]
            continue
        
        if dry_run:
            result.inserted += 1
            continue
        
        try:
            # 字段映射和转换
            new_record = FilterPool(
                product_url=url,
                product_name=title,
                thumbnail_image=image_url,
                brand=brand,
                shop_name=store_name,
                listed_at=parse_date(listing_date),
                latest_review_at=parse_date(latest_review_date),
                competitor_count=has_resellers if has_resellers else 0,
                is_fbe=bool(is_fbe) if is_fbe is not None else False,
                crawled_at=parse_date(add_time) or datetime.now(),
                # 以下字段源数据中没有，设为 NULL
                price=None,
                stock=None,
                review_count=None,
                earliest_review_at=None,
                shop_rank=None,
                category_rank=None,
                ad_rank=None,
            )
            session.add(new_record)
            session.flush()  # 获取自动生成的 ID
            url_to_id[url] = new_record.id
            result.inserted += 1
            
        except Exception as e:
            result.failed += 1
            result.errors.append(f"URL: {url}, 错误: {str(e)}")
            session.rollback()
    
    return result, url_to_id


def migrate_monitor_pool(
    source_conn: sqlite3.Connection,
    session,
    url_to_filter_id: Dict[str, int],
    user_id: int,
    dry_run: bool = False
) -> Tuple[MigrationResult, Dict[str, int]]:
    """
    为迁移的 filter_pool 记录创建 monitor_pool 关联
    返回：(迁移结果, URL到monitor_pool.id的映射)
    """
    result = MigrationResult("monitor_pool")
    url_to_monitor_id: Dict[str, int] = {}
    
    # 获取已存在的 monitor_pool URL
    existing_result = session.execute(text("SELECT product_url, id FROM monitor_pool"))
    existing_monitors = {row[0]: row[1] for row in existing_result.fetchall()}
    
    # 获取源数据中所有产品 URL（通过 PNK_CODE 映射）
    pnk_to_url = get_pnk_to_url_mapping(source_conn)
    
    # 获取有日志数据的 PNK_CODE 列表
    cursor = source_conn.cursor()
    cursor.execute("SELECT DISTINCT PNK_CODE FROM screening_pool_daily_logs")
    pnk_with_logs = {row[0] for row in cursor.fetchall()}
    
    result.total = len(pnk_with_logs)
    print(f"  需要创建 {result.total} 条 monitor_pool 记录")
    
    for pnk_code in pnk_with_logs:
        url = pnk_to_url.get(pnk_code)
        if not url:
            result.skipped += 1
            continue
        
        # 检查是否已存在
        if url in existing_monitors:
            url_to_monitor_id[url] = existing_monitors[url]
            result.skipped += 1
            continue
        
        filter_pool_id = url_to_filter_id.get(url)
        
        if dry_run:
            result.inserted += 1
            continue
        
        try:
            new_monitor = MonitorPool(
                filter_pool_id=filter_pool_id,
                product_url=url,
                created_by_user_id=user_id,
                status=MonitorStatus.ACTIVE,
            )
            session.add(new_monitor)
            session.flush()
            url_to_monitor_id[url] = new_monitor.id
            result.inserted += 1
            
        except Exception as e:
            result.failed += 1
            result.errors.append(f"URL: {url}, 错误: {str(e)}")
            session.rollback()
    
    return result, url_to_monitor_id


def migrate_monitor_history(
    source_conn: sqlite3.Connection,
    session,
    pnk_to_url: Dict[str, str],
    url_to_monitor_id: Dict[str, int],
    dry_run: bool = False
) -> MigrationResult:
    """
    迁移 screening_pool_daily_logs 到 monitor_history
    """
    result = MigrationResult("monitor_history")
    
    # 读取源数据
    cursor = source_conn.cursor()
    cursor.execute("""
        SELECT 
            PNK_CODE, price, stock_count, ad_category_rank,
            category_rank, store_rank, review_count, add_time
        FROM screening_pool_daily_logs
    """)
    
    rows = cursor.fetchall()
    result.total = len(rows)
    print(f"  源数据库有 {result.total} 条日志记录待迁移")
    
    batch_size = 500
    batch = []
    
    for row in rows:
        pnk_code, price, stock_count, ad_category_rank, \
            category_rank, store_rank, review_count, add_time = row
        
        # 通过 PNK_CODE 获取 URL
        url = pnk_to_url.get(pnk_code)
        if not url:
            result.skipped += 1
            continue
        
        # 通过 URL 获取 monitor_pool_id
        monitor_pool_id = url_to_monitor_id.get(url)
        if not monitor_pool_id:
            result.skipped += 1
            continue
        
        if dry_run:
            result.inserted += 1
            continue
        
        try:
            batch.append({
                'monitor_pool_id': monitor_pool_id,
                'price': price,
                'stock': stock_count,
                'ad_rank': ad_category_rank,
                'category_rank': category_rank,
                'shop_rank': store_rank,
                'review_count': review_count,
                'monitored_at': parse_date(add_time) or datetime.now(),
            })
            
            # 批量插入
            if len(batch) >= batch_size:
                session.execute(
                    text("""
                        INSERT INTO monitor_history 
                        (monitor_pool_id, price, stock, ad_rank, category_rank, shop_rank, review_count, monitored_at)
                        VALUES (:monitor_pool_id, :price, :stock, :ad_rank, :category_rank, :shop_rank, :review_count, :monitored_at)
                    """),
                    batch
                )
                result.inserted += len(batch)
                batch = []
            
        except Exception as e:
            result.failed += 1
            result.errors.append(f"PNK: {pnk_code}, 错误: {str(e)}")
    
    # 插入剩余的数据
    if batch and not dry_run:
        try:
            session.execute(
                text("""
                    INSERT INTO monitor_history 
                    (monitor_pool_id, price, stock, ad_rank, category_rank, shop_rank, review_count, monitored_at)
                    VALUES (:monitor_pool_id, :price, :stock, :ad_rank, :category_rank, :shop_rank, :review_count, :monitored_at)
                """),
                batch
            )
            result.inserted += len(batch)
        except Exception as e:
            result.failed += len(batch)
            result.errors.append(f"批量插入失败: {str(e)}")
    
    return result


def update_filter_pool_latest_data(
    session,
    source_conn: sqlite3.Connection,
    pnk_to_url: Dict[str, str],
    dry_run: bool = False
) -> MigrationResult:
    """
    用 screening_pool_daily_logs 最新一条记录的数据更新 filter_pool 的动态字段
    (price, stock, review_count, category_rank, shop_rank, ad_rank)
    """
    result = MigrationResult("filter_pool_update")
    
    cursor = source_conn.cursor()
    # 获取每个产品的最新日志记录
    cursor.execute("""
        SELECT PNK_CODE, price, stock_count, ad_category_rank, 
               category_rank, store_rank, review_count
        FROM screening_pool_daily_logs
        WHERE id IN (
            SELECT MAX(id) FROM screening_pool_daily_logs GROUP BY PNK_CODE
        )
    """)
    
    rows = cursor.fetchall()
    result.total = len(rows)
    print(f"  找到 {result.total} 条产品的最新日志数据")
    
    for row in rows:
        pnk_code, price, stock, ad_rank, category_rank, shop_rank, review_count = row
        url = pnk_to_url.get(pnk_code)
        
        if not url:
            result.skipped += 1
            continue
        
        if dry_run:
            result.inserted += 1
            continue
        
        try:
            update_result = session.execute(
                text("""
                    UPDATE filter_pool 
                    SET price = :price, stock = :stock, review_count = :review_count,
                        category_rank = :category_rank, shop_rank = :shop_rank, ad_rank = :ad_rank
                    WHERE product_url = :url
                """),
                {"price": price, "stock": stock, "review_count": review_count,
                 "category_rank": category_rank, "shop_rank": shop_rank, 
                 "ad_rank": ad_rank, "url": url}
            )
            if update_result.rowcount > 0:
                result.inserted += 1
            else:
                result.skipped += 1
                
        except Exception as e:
            result.failed += 1
            result.errors.append(f"PNK: {pnk_code}, 错误: {str(e)}")
    
    return result


def run_migration(source_db_path: str, dry_run: bool = False):
    """
    执行完整的数据迁移
    """
    print("=" * 60)
    print("数据迁移：emag_tool → emag_erp")
    print("=" * 60)
    
    if dry_run:
        print("\n[预览模式] 不会实际修改数据库\n")
    
    # 验证源数据库
    if not os.path.exists(source_db_path):
        print(f"错误：源数据库不存在: {source_db_path}")
        return False
    
    print(f"源数据库: {source_db_path}")
    
    # 获取目标数据库路径
    target_db_path = config.DATABASE_URL.replace("sqlite:///", "")
    if not os.path.isabs(target_db_path):
        target_db_path = os.path.join(backend_dir, target_db_path)
    
    print(f"目标数据库: {target_db_path}")
    
    # 备份目标数据库
    if not dry_run:
        backup_path = backup_database(target_db_path)
        print(f"已创建备份: {backup_path}")
    
    # 连接数据库
    source_conn = sqlite3.connect(source_db_path)
    engine = create_engine(f"sqlite:///{target_db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # 确保表结构存在
    Base.metadata.create_all(bind=engine)
    
    session = SessionLocal()
    
    try:
        # 验证用户存在（用于 created_by_user_id）
        user = session.query(User).filter(User.id == 1).first()
        if not user:
            print("错误：系统用户 (ID=1) 不存在，请先创建管理员用户")
            return False
        
        print(f"\n使用用户: {user.username} (ID={user.id}) 作为创建者\n")
        
        # Step 1: 迁移 filter_pool
        print("-" * 40)
        print("Step 1: 迁移产品数据到 filter_pool")
        print("-" * 40)
        filter_result, url_to_filter_id = migrate_filter_pool(source_conn, session, dry_run)
        print(f"  {filter_result}")
        
        # Step 2: 创建 monitor_pool 关联
        print("\n" + "-" * 40)
        print("Step 2: 创建 monitor_pool 关联记录")
        print("-" * 40)
        
        # 获取 PNK_CODE 到 URL 的映射
        pnk_to_url = get_pnk_to_url_mapping(source_conn)
        
        monitor_result, url_to_monitor_id = migrate_monitor_pool(
            source_conn, session, url_to_filter_id, user.id, dry_run
        )
        print(f"  {monitor_result}")
        
        # Step 3: 迁移 monitor_history
        print("\n" + "-" * 40)
        print("Step 3: 迁移日志数据到 monitor_history")
        print("-" * 40)
        history_result = migrate_monitor_history(
            source_conn, session, pnk_to_url, url_to_monitor_id, dry_run
        )
        print(f"  {history_result}")
        
        # Step 4: 用最新日志数据更新 filter_pool 的动态字段
        print("\n" + "-" * 40)
        print("Step 4: 更新 filter_pool 动态字段 (price, stock, review_count 等)")
        print("-" * 40)
        update_result = update_filter_pool_latest_data(
            session, source_conn, pnk_to_url, dry_run
        )
        print(f"  {update_result}")
        
        # 提交事务
        if not dry_run:
            session.commit()
            print("\n[OK] 事务已提交")
        
        # 打印汇总
        print("\n" + "=" * 60)
        print("迁移完成汇总")
        print("=" * 60)
        print(f"  filter_pool 插入:    {filter_result}")
        print(f"  filter_pool 更新:    {update_result}")
        print(f"  monitor_pool:        {monitor_result}")
        print(f"  monitor_history:     {history_result}")
        
        # 打印错误信息
        all_errors = filter_result.errors + monitor_result.errors + history_result.errors + update_result.errors
        if all_errors:
            print("\n错误详情:")
            for i, error in enumerate(all_errors[:10], 1):
                print(f"  {i}. {error}")
            if len(all_errors) > 10:
                print(f"  ... 还有 {len(all_errors) - 10} 条错误")
        
        return True
        
    except Exception as e:
        print(f"\n迁移失败: {str(e)}")
        session.rollback()
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        session.close()
        source_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="从 emag_tool 数据库迁移数据到 emag_erp 数据库"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式，不实际执行迁移"
    )
    parser.add_argument(
        "--source-db",
        default=r"D:\emag_tool\emag.db",
        help="源数据库路径 (默认: D:\\emag_tool\\emag.db)"
    )
    
    args = parser.parse_args()
    
    success = run_migration(args.source_db, args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()


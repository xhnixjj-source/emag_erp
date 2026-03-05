"""检查profit_calculation表结构"""
import sqlite3
from app.config import config
import re

# 获取数据库文件路径
db_path = config.DATABASE_URL.replace('sqlite:///', '')

print(f"数据库文件: {db_path}")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 获取表结构
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='profit_calculation'")
result = cursor.fetchone()

if result:
    sql = result[0]
    print("\nCREATE TABLE语句:")
    print(sql)
    
    # 检查是否包含commission_source
    if 'commission_source' in sql:
        print("\n✓ commission_source 列存在于CREATE TABLE语句中")
    else:
        print("\n✗ commission_source 列不存在于CREATE TABLE语句中")
        
    # 提取所有列名
    columns = re.findall(r'(\w+)\s+\w+', sql)
    print(f"\n所有列: {columns}")
else:
    print("表不存在")

conn.close()


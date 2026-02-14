# 数据库迁移指南

本文档说明服务器端需要执行的数据库迁移脚本及其执行顺序。

## 迁移脚本执行方法

所有迁移脚本都在 `backend` 目录下，执行方式：

```bash
cd backend
python migrate_xxx.py
```

## 本次更新需要执行的迁移脚本

根据最新的代码提交，**必须执行以下迁移脚本**：

### 1. 创建 profit_config 表（新增）
```bash
python migrate_add_profit_config.py
```
**说明**：创建利润测算配置表，用于存储默认费用配置（头程物流费、订单处理费、仓储费、平台佣金、VAT等）

---

## 其他迁移脚本（如果之前未执行过）

如果服务器数据库是从旧版本升级，可能需要按顺序执行以下脚本：

### 基础字段迁移（如果数据库是全新安装，这些通常会自动创建）

1. **migrate_add_link_fields.py** - 添加基础链接字段（PNK_CODE, thumbnail_image, price）
2. **migrate_add_review_fields.py** - 添加评论和评分字段（review_count, rating）
3. **migrate_add_chrome_extension_fields.py** - 添加 Chrome 插件相关字段
4. **migrate_add_keyword_links_listed_at.py** - 添加上架日期相关字段（listed_at, listed_at_status, listed_at_error_type）

### FilterPool 相关迁移

5. **migrate_add_filter_pool_fields.py** - 添加 FilterPool 基础字段（thumbnail_image, brand, shop_name, is_fbe, competitor_count）
6. **migrate_add_filter_pool_urls.py** - 添加 FilterPool URL 字段（shop_intro_url, shop_url, category_url）

### 利润计算相关迁移

7. **migrate_add_profit_fields.py** - 添加利润计算字段（chinese_name, model_number, category_name, length, width, height, weight, purchase_price）

### 监控相关迁移

8. **migrate_add_rating_to_monitor_history.py** - 为监控历史添加评分字段（rating）

---

## 迁移脚本特性

所有迁移脚本都具有以下特性：

1. **幂等性**：可以安全地多次执行，脚本会检查字段/表是否存在，已存在则跳过
2. **自动检测**：脚本会自动检测数据库路径和表结构
3. **错误处理**：如果迁移失败，会自动回滚，不会破坏数据库

## 验证迁移结果

执行迁移后，可以通过以下方式验证：

### 检查 profit_config 表
```sql
-- 连接到数据库
sqlite3 emag_erp.db

-- 检查表是否存在
.tables

-- 检查表结构
.schema profit_config

-- 检查数据
SELECT * FROM profit_config;
```

### 检查 keyword_links 表的上架日期字段
```sql
-- 检查字段是否存在
PRAGMA table_info(keyword_links);

-- 应该能看到以下字段：
-- listed_at (DATETIME)
-- listed_at_status (VARCHAR)
-- listed_at_error_type (VARCHAR)
```

## 注意事项

1. **备份数据库**：在执行迁移前，建议先备份数据库文件
2. **停止服务**：迁移时建议停止后端服务，避免并发访问问题
3. **检查日志**：执行迁移时注意查看输出日志，确认迁移成功

## 备份数据库

```bash
# 备份 SQLite 数据库
cp emag_erp.db emag_erp.db.backup_$(date +%Y%m%d_%H%M%S)
```

---

## 快速执行（仅本次更新）

如果只需要应用本次更新的数据库变更，执行：

```bash
cd backend
python migrate_add_profit_config.py
```

即可完成数据库迁移。


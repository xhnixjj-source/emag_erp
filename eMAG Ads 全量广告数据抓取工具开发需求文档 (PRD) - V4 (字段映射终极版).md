# eMAG Ads 全量广告数据抓取工具开发需求文档 (PRD) - V4 (字段映射终极版)

## 1. 项目背景
本版本针对 Cursor 提出的 `analytics` 对象内部字段名进行了精确核实。这些字段直接决定了数据库建表结构和解析逻辑。

## 2. 核心接口字段映射 (Analytics Object Mapping)

在 `Products` 接口 (`GET /api-ui/ads/campaign/{campaign_id}/products`) 返回的 `items` 数组中，每个产品包含一个 `analytics` 对象。以下是经过 F12 核实的精确字段名：

| 业务含义 | API 字段名 (Key) | 数据类型 | 备注 |
| :--- | :--- | :--- | :--- |
| **点击量** | `clicks` | Integer | 用户点击广告的次数 |
| **曝光量** | `impressions` | Integer | 广告展示的次数 |
| **点击率** | `ctr` | Float | 计算公式: `clicks / impressions * 100` |
| **实际点击成本** | `actual_cpc` | Float | 每次点击的平均成本 |
| **总成本** | `cost` | Float | 该时间段内的广告总支出 |
| **销售额** | `sales` | Float | 广告带来的订单总价值 |
| **售出数量** | `products_sold` | Integer | 广告带来的成交产品件数 |
| **广告成本占比** | `cps` | Float | 计算公式: `cost / sales * 100` |
| **成本占比** | `cost_percentage` | Float | 该产品在广告组总成本中的占比 |

## 3. 数据库建表建议 (Database Schema)

请 Cursor 在 `ads_product_performance` 表中使用以下字段名，以保持与 API 的一致性：

```sql
CREATE TABLE ads_product_performance (
    id INT AUTO_INCREMENT PRIMARY KEY,
    campaign_id INT NOT NULL,
    adset_id INT NOT NULL,
    product_id INT NOT NULL,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    clicks INT DEFAULT 0,
    impressions INT DEFAULT 0,
    ctr DECIMAL(10, 4) DEFAULT 0.0000,
    actual_cpc DECIMAL(10, 4) DEFAULT 0.0000,
    cost DECIMAL(10, 4) DEFAULT 0.0000,
    sales DECIMAL(10, 4) DEFAULT 0.0000,
    products_sold INT DEFAULT 0,
    cps DECIMAL(10, 4) DEFAULT 0.0000,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_perf (product_id, adset_id, date_start, date_end)
);
```

## 4. 针对 Cursor 的技术建议 (Implementation Notes)

1.  **字段解析**: 在解析 `analytics` 对象时，请务必使用 `product['analytics'].get('clicks', 0)` 这种带有默认值的获取方式，以防某些字段在特定情况下缺失。
2.  **数据类型转换**: API 返回的数值可能是字符串或浮点数，入库前请确保进行了正确的类型转换。
3.  **URL 编码**: 再次强调，`adset_name` 参数在请求 `Products` 接口时必须进行 URL 编码。

---
**开发者提示**: 经核实，`analytics` 对象中的字段名非常规范，直接使用上述 Key 即可完成解析。

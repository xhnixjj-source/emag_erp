# eMAG Ads 全量广告数据抓取工具开发需求文档 (PRD) - V3 (技术细节修正版)

## 1. 项目背景
在现有 eMAG 运单抓取系统的基础上，新增 **eMAG Ads 广告数据抓取** 功能。本版本针对 Cursor 提出的分页参数、数组参数格式及字段依赖进行了精确修正。

## 2. 核心接口技术细节 (API Technical Specs)

### 2.1 广告活动列表 (Campaigns)
- **URL**: `GET https://marketplace.emag.ro/api-ui/ads/campaign`
- **分页参数**: `page` (页码), `per_page` (每页数量，建议 25)
- **数组参数格式**: `inherited_status[]` (例如：`?inherited_status[]=active`)
- **排序参数**: `sort[]` (例如：`?sort[]={"field":"id","direction":"desc"}`)
- **返回结构**: 包含 `total` (总数), `page` (当前页), `per_page` (每页数), `items` (数据数组)。

### 2.2 广告组列表 (Adsets)
- **URL**: `GET https://marketplace.emag.ro/api-ui/ads/campaign/{campaign_id}/adsets`
- **分页参数**: `page`, `per_page`
- **数组参数格式**: `status[]` (例如：`?status[]=active`)
- **返回结构**: `items` 数组中包含 `id` 和 `name` 字段。**注意：** 第三层接口必须依赖此处的 `name` 字段。

### 2.3 广告产品详情 (Products)
- **URL**: `GET https://marketplace.emag.ro/api-ui/ads/campaign/{campaign_id}/products`
- **关键参数**:
  - `adset_id`: 必须 (从 2.2 获取)
  - `adset_name`: 必须 (从 2.2 获取，需进行 URL 编码)
  - `status[]`: `active`
- **分页参数**: `page`, `per_page`
- **返回结构**: `items` 数组中包含 `id` (产品 ID), `name` (产品名称), 以及 `analytics` 对象。

## 3. 针对 Cursor 的技术建议 (Implementation Notes)

1.  **URL 编码**: 由于 `adset_name` 可能包含特殊字符或空格，在拼接 `Products` 接口 URL 时，请务必使用 `encodeURIComponent` (JS) 或 `urllib.parse.quote` (Python) 进行编码。
2.  **数组参数拼接**: eMAG 后台期望的格式是 `parameter[]=value`。在 Python `requests` 中，可以通过 `params={'status[]': 'active'}` 自动处理，或者手动拼接字符串。
3.  **数据打平 (Flattening)**: 在入库 `ads_product_performance` 表时，请确保将 `campaign_id`, `campaign_name`, `adset_id`, `adset_name` 作为外键或关联字段存入，以便后续报表查询。

## 4. 前端交互与入库 (UI & DB)
- **前端**: 新增 `date_start` 和 `date_end` 时间控件，并将其值透传给上述所有接口。
- **入库**: 建议使用 `Upsert` 逻辑，以 `(product_id, adset_id, date_start, date_end)` 作为唯一标识。

---
**开发者提示**: 经核实，eMAG Ads 接口的分页参数名统一为 `page` 和 `per_page`，返回的总数字段名为 `total`。

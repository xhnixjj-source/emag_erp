# eMAG Ads 全量广告数据抓取工具开发需求文档 (PRD) - V2

## 1. 项目背景

在现有 eMAG 运单抓取系统的基础上，新增 **eMAG Ads 广告数据抓取** 功能。需要实现从广告活动到具体产品的三层数据递归抓取，并支持前端时间筛选及数据库入库。

## 2. 前端交互需求 (UI/UX)

需要在现有前端界面新增以下控件：

- **时间选择器 (Date Picker)**: 支持选择 `开始日期 (date_start)` 和 `结束日期 (date_end)`。

- **抓取按钮 (Sync Button)**: 点击后触发广告数据同步任务。

- **状态显示 (Status Bar)**: 显示当前抓取的进度（例如：正在抓取活动 A -> 广告组 B -> 产品 C）。

## 3. 核心接口定义 (API Endpoints)

### 3.1 广告活动列表 (Campaigns)

- **URL**: `GET https://marketplace.emag.ro/api-ui/ads/campaign`

- **关键参数**:
  - `date_start`: 由前端时间控件传入 (YYYY-MM-DD )
  - `date_end`: 由前端时间控件传入 (YYYY-MM-DD)
  - `inherited_status[]`: `active` (默认)

### 3.2 广告组列表 (Adsets)

- **URL**: `GET https://marketplace.emag.ro/api-ui/ads/campaign/{campaign_id}/adsets`

- **关键参数**:
  - `date_start`/`date_end`: 同上
  - `status[]`: `active`

### 3.3 广告产品详情 (Products )

- **URL**: `GET https://marketplace.emag.ro/api-ui/ads/campaign/{campaign_id}/products`

- **关键参数**:
  - `adset_id`: 必须提供上级广告组 ID
  - `adset_name`: 必须提供上级广告组名称
  - `date_start`/`date_end`: 同上

## 4. 数据库入库逻辑 (Database Integration )

请 Cursor 根据现有数据库结构，创建或更新以下表：

- **ads_campaigns 表**: 存储活动 ID、名称、预算、状态。

- **ads_adsets 表**: 存储广告组 ID、所属活动 ID、名称、当前竞价。

- **ads_product_performance 表**: 存储产品 ID、所属广告组 ID、日期区间、点击量、曝光量、CTR、成本、销售额、售出数量、CPS。
  - *`注意`*`: 建议使用 (product_id, adset_id, date_start, date_end) 作为复合唯一键，防止重复入库。`

## 5. 抓取逻辑流程 (Workflow)

1. **前端输入**: 用户选择日期区间并点击“同步”。

1. **递归抓取**:
  - 循环 1: 获取所有活动。
  - 循环 2: 获取每个活动下的所有广告组。
  - 循环 3: 获取每个广告组下的所有产品表现。

1. **入库**: 每一层抓取完成后，立即进行数据库 `Upsert` 操作。

## 6. 开发者提示 (For Cursor)

- **复用授权**: 请复用现有系统中的 `Session/Cookie` 管理模块。

- **频率控制**: eMAG API 对并发有一定限制，请在循环中加入适当的 `time.sleep()`。

- **分页处理**: 必须处理 `total` 和 `per_page` 逻辑，确保全量抓取。


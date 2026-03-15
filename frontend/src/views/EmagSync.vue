<template>
  <div class="emag-sync-container">
    <el-row :gutter="20">
      <!-- 左侧面板：同步控制 -->
      <el-col :span="8">
        <el-card class="control-panel">
          <template #header>
            <div class="card-header">
              <span>同步控制</span>
            </div>
          </template>

          <!-- API授权配置区域 -->
          <el-card shadow="never" class="section-card">
            <template #header>
              <span>API授权配置</span>
            </template>
            <el-form :model="accountForm" label-width="80px" size="small">
              <el-form-item label="平台">
                <el-select v-model="accountForm.platform" placeholder="选择平台" style="width: 100%">
                  <el-option label="eMAG Romania" value="ro" />
                  <el-option label="eMAG Bulgaria" value="bg" />
                  <el-option label="eMAG Hungary" value="hu" />
                  <el-option label="Fashion Days RO" value="fashiondays-ro" />
                  <el-option label="Fashion Days BG" value="fashiondays-bg" />
                </el-select>
              </el-form-item>
              <el-form-item label="用户名">
                <el-input v-model="accountForm.username" placeholder="API用户名" />
              </el-form-item>
              <el-form-item label="密码">
                <el-input v-model="accountForm.password" type="password" placeholder="API密码" show-password />
              </el-form-item>
              <el-form-item>
                <el-button type="primary" @click="testConnection" :loading="testingConnection" size="small">
                  测试连接
                </el-button>
                <el-button type="success" @click="saveAccount" :loading="savingAccount" size="small">
                  保存配置
                </el-button>
              </el-form-item>
              <el-form-item v-if="accountStatus">
                <el-tag :type="accountStatus.configured ? 'success' : 'info'" size="small">
                  {{ accountStatus.configured ? '已配置' : '未配置' }}
                </el-tag>
                <span v-if="accountStatus.platform" style="margin-left: 10px; font-size: 12px; color: #909399">
                  平台: {{ accountStatus.platform }}
                </span>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- eMAG 后台登录区域 -->
          <el-card shadow="never" class="section-card" style="margin-top: 20px">
            <template #header>
              <span>后台登录（卖家中心）</span>
            </template>
            <el-form :model="marketplaceLoginForm" label-width="80px" size="small">
              <el-alert
                v-if="marketplaceStatus === 'waiting_manual_login'"
                type="info"
                :closable="false"
                style="margin-bottom: 15px"
              >
                <template #title>
                  <div>
                    <strong>请在弹出的浏览器窗口中手动完成登录</strong>
                    <div style="margin-top: 5px; font-size: 12px">
                      包括输入用户名、密码和处理验证码（如需要）
                    </div>
                  </div>
                </template>
              </el-alert>
              <el-form-item label="状态">
                <el-tag :type="marketplaceStatusTagType" size="small">
                  {{ marketplaceStatusText }}
                </el-tag>
              </el-form-item>
              <el-form-item>
                <el-button
                  type="primary"
                  size="small"
                  :loading="marketplaceLoggingIn"
                  @click="handleMarketplaceLogin"
                >
                  登录后台
                </el-button>
                <el-button
                  size="small"
                  :disabled="marketplaceStatus !== 'logged_in'"
                  @click="handleMarketplaceLogout"
                  style="margin-left: 10px"
                >
                  退出后台
                </el-button>
                <el-button
                  type="success"
                  size="small"
                  :loading="syncingInboundShipments"
                  :disabled="marketplaceStatus !== 'logged_in'"
                  @click="handleSyncInboundShipments"
                  style="margin-left: 10px"
                >
                  运单同步
                </el-button>
              </el-form-item>

              <!-- 登录成功后的简短页面信息 -->
              <el-form-item v-if="marketplacePageInfo" label="页面信息">
                <div style="font-size: 12px; color: #606266;">
                  <div>标题：{{ marketplacePageInfo.title || '-' }}</div>
                  <div style="margin-top: 4px;">
                    URL：<span style="word-break: break-all;">{{ marketplacePageInfo.url || '-' }}</span>
                  </div>
                  <div v-if="marketplacePageInfo.seller_hint" style="margin-top: 4px;">
                    账号：{{ marketplacePageInfo.seller_hint }}
                  </div>
                </div>
              </el-form-item>

              <!-- 需要图形验证码时展示截图 -->
              <el-form-item v-if="marketplaceStatus === 'captcha_required'" label="验证码">
                <div style="font-size: 12px; color: #606266;">
                  <div>已检测到验证码，请在弹出的浏览器窗口中手动完成，然后点击下方按钮。</div>
                  <div v-if="marketplaceCaptcha" style="margin-top: 8px;">
                    <img
                      :src="`data:image/png;base64,${marketplaceCaptcha}`"
                      alt="Captcha Screenshot"
                      style="max-width: 100%; border: 1px solid #dcdfe6; border-radius: 4px;"
                    />
                  </div>
                  <el-button
                    type="primary"
                    size="small"
                    :loading="marketplaceCheckingCaptcha"
                    style="margin-top: 8px;"
                    @click="handleMarketplaceCaptchaDone"
                  >
                    我已完成验证码
                  </el-button>
                </div>
              </el-form-item>

              <!-- 需要手机验证码时展示输入框 -->
              <el-form-item v-if="marketplaceStatus === 'sms_verification_required'" label="手机验证码">
                <div style="font-size: 12px; color: #606266;">
                  <div style="margin-bottom: 8px;">已检测到需要手机验证码，请输入收到的验证码：</div>
                  <div v-if="marketplaceCaptcha" style="margin-bottom: 8px;">
                    <img
                      :src="`data:image/png;base64,${marketplaceCaptcha}`"
                      alt="SMS Verification Screenshot"
                      style="max-width: 100%; border: 1px solid #dcdfe6; border-radius: 4px;"
                    />
                  </div>
                  <el-input
                    v-model="marketplaceSmsCode"
                    placeholder="请输入手机验证码"
                    size="small"
                    style="margin-bottom: 8px;"
                    maxlength="10"
                    @keyup.enter="handleMarketplaceSubmitSmsCode"
                  />
                  <el-button
                    type="primary"
                    size="small"
                    :loading="marketplaceSubmittingSms"
                    :disabled="!marketplaceSmsCode"
                    style="width: 100%;"
                    @click="handleMarketplaceSubmitSmsCode"
                  >
                    提交验证码
                  </el-button>
                </div>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- 广告数据同步区域 -->
          <el-card shadow="never" class="section-card" style="margin-top: 20px">
            <template #header>
              <span>广告数据同步</span>
            </template>
            <el-form label-width="80px" size="small">
              <el-form-item label="站点">
                <el-select v-model="adsSyncMarketplace" style="width: 100%">
                  <el-option label="Romania (RO)" value="ro" />
                  <el-option label="Bulgaria (BG)" value="bg" />
                  <el-option label="Hungary (HU)" value="hu" />
                </el-select>
              </el-form-item>
              <el-form-item label="日期范围">
                <el-date-picker
                  v-model="adsDateRange"
                  type="daterange"
                  range-separator="至"
                  start-placeholder="开始日期"
                  end-placeholder="结束日期"
                  format="YYYY-MM-DD"
                  value-format="YYYY-MM-DD"
                  style="width: 100%"
                />
              </el-form-item>
              <el-form-item>
                <el-button
                  type="primary"
                  size="small"
                  :loading="syncingAds"
                  :disabled="marketplaceStatus !== 'logged_in' || !adsDateRange || adsDateRange.length !== 2"
                  @click="handleSyncAds"
                >
                  同步广告数据 ({{ adsSyncMarketplace.toUpperCase() }})
                </el-button>
              </el-form-item>
              <el-form-item v-if="adsSyncMessage">
                <el-alert :type="adsSyncMessageType" :closable="false" show-icon>
                  {{ adsSyncMessage }}
                </el-alert>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- 同步操作区域 -->
          <el-card shadow="never" class="section-card" style="margin-top: 20px">
            <template #header>
              <span>同步操作</span>
            </template>
            <div class="sync-buttons" style="display: flex; gap: 8px; flex-wrap: wrap">
              <el-button type="primary" size="small" @click="syncProducts" :loading="syncingProducts" :disabled="!accountStatus?.configured">
                同步产品
              </el-button>
              <el-button type="primary" size="small" @click="syncOrders" :loading="syncingOrders" :disabled="!accountStatus?.configured">
                同步订单
              </el-button>
              <el-button type="primary" size="small" @click="syncReturns" :loading="syncingReturns" :disabled="!accountStatus?.configured">
                同步退货
              </el-button>
              <el-button type="success" size="small" @click="syncAll" :loading="syncingAll" :disabled="!accountStatus?.configured">
                同步全部
              </el-button>
            </div>
            <div v-if="accountStatus" style="margin-top: 15px; font-size: 12px; color: #909399">
              <div>产品: {{ accountStatus.product_count || 0 }} 条</div>
              <div>订单: {{ accountStatus.order_count || 0 }} 条</div>
              <div>退货: {{ accountStatus.return_count || 0 }} 条</div>
              <div v-if="accountStatus.last_product_sync" style="margin-top: 5px">
                最后同步: {{ formatDateTime(accountStatus.last_product_sync) }}
              </div>
            </div>
          </el-card>

          <!-- 同步日志区域 -->
          <el-card shadow="never" class="section-card" style="margin-top: 20px">
            <template #header>
              <span>同步日志</span>
            </template>
            <div class="sync-logs">
              <div 
                v-for="(log, index) in syncLogs" 
                :key="index" 
                class="log-item"
                :class="log.type"
              >
                <div class="log-time">{{ formatDateTime(log.time) }}</div>
                <div class="log-content">
                  <el-tag :type="getLogType(log.type)" size="small">{{ log.type }}</el-tag>
                  <span style="margin-left: 10px">{{ log.message }}</span>
                </div>
              </div>
              <div v-if="syncLogs.length === 0" style="text-align: center; color: #909399; padding: 20px">
                暂无日志
              </div>
            </div>
          </el-card>
        </el-card>
      </el-col>

      <!-- 右侧面板：数据展示 -->
      <el-col :span="16">
        <el-card class="data-panel">
          <template #header>
            <div class="card-header">
              <span>数据展示</span>
            </div>
          </template>

          <el-tabs v-model="activeTab" @tab-change="handleTabChange">
            <!-- 产品库存标签 -->
            <el-tab-pane label="产品库存" name="products">
              <div style="margin-bottom: 15px">
                <el-input
                  v-model="productSearch"
                  placeholder="搜索PNK或EAN"
                  style="width: 300px"
                  clearable
                  @input="loadProducts"
                >
                  <template #prefix>
                    <el-icon><Search /></el-icon>
                  </template>
                </el-input>
              </div>
              <el-table
                :data="products"
                v-loading="loadingProducts"
                style="width: 100%"
                height="calc(100vh - 400px)"
              >
                <el-table-column prop="product_id" label="产品ID" width="100" />
                <el-table-column prop="pnk_code" label="PNK" width="150" show-overflow-tooltip />
                <el-table-column prop="ean" label="EAN" width="150" show-overflow-tooltip />
                <el-table-column prop="name" label="名称" min-width="200" show-overflow-tooltip />
                <el-table-column prop="brand" label="品牌" width="120" show-overflow-tooltip />
                <el-table-column prop="stock" label="库存" width="80" />
                <el-table-column prop="sale_price" label="价格" width="100">
                  <template #default="{ row }">
                    {{ row.sale_price ? `€${row.sale_price.toFixed(2)}` : '-' }}
                  </template>
                </el-table-column>
                <el-table-column prop="status" label="状态" width="80">
                  <template #default="{ row }">
                    <el-tag :type="row.status === 1 ? 'success' : 'info'" size="small">
                      {{ row.status === 1 ? '激活' : row.status === 0 ? '停用' : '结束' }}
                    </el-tag>
                  </template>
                </el-table-column>
              </el-table>
              <el-pagination
                v-model:current-page="productPage"
                v-model:page-size="productPageSize"
                :total="productTotal"
                :page-sizes="[50, 100, 200]"
                @current-change="loadProducts"
                @size-change="loadProducts"
                layout="total, sizes, prev, pager, next"
                style="margin-top: 15px"
              />
            </el-tab-pane>

            <!-- 订单列表标签 -->
            <el-tab-pane label="订单列表" name="orders">
              <div style="margin-bottom: 15px">
                <el-input
                  v-model="orderSearch"
                  placeholder="搜索PNK或EAN"
                  style="width: 300px; margin-right: 10px"
                  clearable
                  @input="loadOrders"
                >
                  <template #prefix>
                    <el-icon><Search /></el-icon>
                  </template>
                </el-input>
                <el-date-picker
                  v-model="orderDateRange"
                  type="datetimerange"
                  range-separator="至"
                  start-placeholder="开始日期"
                  end-placeholder="结束日期"
                  format="YYYY-MM-DD HH:mm:ss"
                  value-format="YYYY-MM-DDTHH:mm:ss"
                  style="width: 350px"
                  @change="loadOrders"
                />
              </div>
              <el-table
                :data="orders"
                v-loading="loadingOrders"
                style="width: 100%"
                height="calc(100vh - 400px)"
              >
                <el-table-column prop="order_id" label="订单ID" width="100" />
                <el-table-column prop="order_date" label="订单日期" width="180">
                  <template #default="{ row }">
                    {{ formatDateTime(row.order_date) }}
                  </template>
                </el-table-column>
                <el-table-column prop="pnk_code" label="PNK" width="120" show-overflow-tooltip />
                <el-table-column prop="ean" label="EAN" width="120" show-overflow-tooltip />
                <el-table-column prop="product_name" label="产品名称" min-width="200" show-overflow-tooltip />
                <el-table-column prop="quantity" label="数量" width="80" />
                <el-table-column prop="sale_price" label="单价" width="100">
                  <template #default="{ row }">
                    {{ row.sale_price ? `€${row.sale_price.toFixed(2)}` : '-' }}
                  </template>
                </el-table-column>
                <el-table-column prop="total_amount" label="总金额" width="100">
                  <template #default="{ row }">
                    {{ row.total_amount ? `€${row.total_amount.toFixed(2)}` : '-' }}
                  </template>
                </el-table-column>
                <el-table-column prop="order_status" label="状态" width="100">
                  <template #default="{ row }">
                    <el-tag :type="getOrderStatusType(row.order_status)" size="small">
                      {{ getOrderStatusText(row.order_status) }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="customer_name" label="客户" width="120" show-overflow-tooltip />
              </el-table>
              <el-pagination
                v-model:current-page="orderPage"
                v-model:page-size="orderPageSize"
                :total="orderTotal"
                :page-sizes="[50, 100, 200]"
                @current-change="loadOrders"
                @size-change="loadOrders"
                layout="total, sizes, prev, pager, next"
                style="margin-top: 15px"
              />
            </el-tab-pane>

            <!-- 退货列表标签 -->
            <el-tab-pane label="退货列表" name="returns">
              <div style="margin-bottom: 15px">
                <el-input
                  v-model="returnSearch"
                  placeholder="搜索PNK或EAN"
                  style="width: 300px; margin-right: 10px"
                  clearable
                  @input="loadReturns"
                >
                  <template #prefix>
                    <el-icon><Search /></el-icon>
                  </template>
                </el-input>
                <el-date-picker
                  v-model="returnDateRange"
                  type="datetimerange"
                  range-separator="至"
                  start-placeholder="开始日期"
                  end-placeholder="结束日期"
                  format="YYYY-MM-DD HH:mm:ss"
                  value-format="YYYY-MM-DDTHH:mm:ss"
                  style="width: 350px"
                  @change="loadReturns"
                />
              </div>
              <el-table
                :data="returns"
                v-loading="loadingReturns"
                style="width: 100%"
                height="calc(100vh - 400px)"
              >
                <el-table-column prop="rma_id" label="RMA ID" width="100" />
                <el-table-column prop="return_date" label="退货日期" width="180">
                  <template #default="{ row }">
                    {{ formatDateTime(row.return_date) }}
                  </template>
                </el-table-column>
                <el-table-column prop="order_id" label="订单ID" width="100" />
                <el-table-column prop="pnk_code" label="PNK" width="120" show-overflow-tooltip />
                <el-table-column prop="ean" label="EAN" width="120" show-overflow-tooltip />
                <el-table-column prop="product_name" label="产品名称" min-width="200" show-overflow-tooltip />
                <el-table-column prop="quantity" label="数量" width="80" />
                <el-table-column prop="return_status" label="状态" width="120">
                  <template #default="{ row }">
                    <el-tag :type="getReturnStatusType(row.return_status)" size="small">
                      {{ getReturnStatusText(row.return_status) }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="reason" label="原因" min-width="200" show-overflow-tooltip />
              </el-table>
              <el-pagination
                v-model:current-page="returnPage"
                v-model:page-size="returnPageSize"
                :total="returnTotal"
                :page-sizes="[50, 100, 200]"
                @current-change="loadReturns"
                @size-change="loadReturns"
                layout="total, sizes, prev, pager, next"
                style="margin-top: 15px"
              />
            </el-tab-pane>

            <!-- 发货单列表 -->
            <el-tab-pane label="发货单" name="shipments">
              <div style="margin-bottom: 15px">
                <el-input
                  v-model="shipmentSearch"
                  placeholder="搜索运单号"
                  style="width: 300px; margin-right: 10px"
                  clearable
                  @input="loadShipments"
                >
                  <template #prefix>
                    <el-icon><Search /></el-icon>
                  </template>
                </el-input>
              </div>
              <el-table
                :data="shipments"
                v-loading="loadingShipments"
                style="width: 100%"
                height="calc(100vh - 400px)"
                row-key="id"
              >
                <el-table-column type="expand">
                  <template #default="{ row }">
                    <div style="padding: 10px 20px">
                      <el-table :data="row.details" border size="small" style="width: 100%">
                        <el-table-column prop="vendor_product_id" label="产品 ID" width="180" />
                        <el-table-column prop="transferred_to_storage_quantity" label="入库数量" width="120" />
                        <el-table-column prop="expiration_date" label="过期日期" width="150">
                          <template #default="{ row: detail }">
                            {{ detail.expiration_date || '-' }}
                          </template>
                        </el-table-column>
                        <el-table-column prop="producer_lot" label="批次号" min-width="150">
                          <template #default="{ row: detail }">
                            {{ detail.producer_lot || '-' }}
                          </template>
                        </el-table-column>
                        <el-table-column prop="synced_at" label="同步时间" width="180">
                          <template #default="{ row: detail }">
                            {{ formatDateTime(detail.synced_at) }}
                          </template>
                        </el-table-column>
                      </el-table>
                    </div>
                  </template>
                </el-table-column>
                <el-table-column prop="reception_id" label="运单号" width="120" />
                <el-table-column prop="status" label="状态" width="120">
                  <template #default="{ row }">
                    <el-tag :type="row.status === 'finalized' ? 'success' : 'info'" size="small">
                      {{ row.status === 'finalized' ? '已完成' : row.status }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="detail_count" label="SKU 行数" width="100" />
                <el-table-column prop="total_quantity" label="总入库数" width="100" />
                <el-table-column prop="synced_at" label="同步时间" width="180">
                  <template #default="{ row }">
                    {{ formatDateTime(row.synced_at) }}
                  </template>
                </el-table-column>
                <el-table-column prop="created_at" label="创建时间" min-width="180">
                  <template #default="{ row }">
                    {{ formatDateTime(row.created_at) }}
                  </template>
                </el-table-column>
              </el-table>
              <el-pagination
                v-model:current-page="shipmentPage"
                v-model:page-size="shipmentPageSize"
                :total="shipmentTotal"
                :page-sizes="[50, 100, 200]"
                @current-change="loadShipments"
                @size-change="loadShipments"
                layout="total, sizes, prev, pager, next"
                style="margin-top: 15px"
              />
            </el-tab-pane>

            <!-- 广告数据 -->
            <el-tab-pane label="广告数据" name="ads">
              <div style="margin-bottom: 15px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap">
                <el-select
                  v-model="adsFilterMarketplace"
                  placeholder="全部站点"
                  style="width: 150px"
                  clearable
                  @change="loadAdsPerformance"
                >
                  <el-option label="RO" value="ro" />
                  <el-option label="BG" value="bg" />
                  <el-option label="HU" value="hu" />
                </el-select>
                <el-date-picker
                  v-model="adsFilterDateRange"
                  type="daterange"
                  range-separator="至"
                  start-placeholder="开始日期"
                  end-placeholder="结束日期"
                  format="YYYY-MM-DD"
                  value-format="YYYY-MM-DD"
                  style="width: 300px"
                  @change="loadAdsPerformance"
                />
                <el-input
                  v-model="adsSearchCampaignId"
                  placeholder="Campaign ID"
                  style="width: 150px"
                  clearable
                  @input="loadAdsPerformance"
                />
                <el-input
                  v-model="adsSearchAdsetId"
                  placeholder="Adset ID"
                  style="width: 150px"
                  clearable
                  @input="loadAdsPerformance"
                />
              </div>
              <el-table
                :data="adsPerformance"
                v-loading="loadingAds"
                style="width: 100%"
                height="calc(100vh - 400px)"
              >
                <el-table-column prop="marketplace" label="站点" width="60">
                  <template #default="{ row }">
                    {{ row.marketplace ? row.marketplace.toUpperCase() : '' }}
                  </template>
                </el-table-column>
                <el-table-column prop="campaign_id" label="活动ID" width="90" />
                <el-table-column prop="campaign_name" label="活动名称" width="160" show-overflow-tooltip />
                <el-table-column prop="adset_id" label="广告组ID" width="90" />
                <el-table-column prop="adset_name" label="广告组名称" width="160" show-overflow-tooltip />
                <el-table-column prop="product_id" label="产品ID" width="100" />
                <el-table-column prop="part_number" label="PNK" width="120" show-overflow-tooltip />
                <el-table-column prop="part_number_key" label="Prd_Code" width="130" show-overflow-tooltip />
                <el-table-column prop="product_name" label="产品名称" min-width="200" show-overflow-tooltip />
                <el-table-column prop="date_start" label="开始日期" width="110" />
                <el-table-column prop="date_end" label="结束日期" width="110" />
                <el-table-column prop="impressions" label="曝光" width="90" />
                <el-table-column prop="clicks" label="点击" width="80" />
                <el-table-column prop="ctr" label="CTR%" width="80">
                  <template #default="{ row }">
                    {{ row.ctr != null ? row.ctr.toFixed(2) : '-' }}
                  </template>
                </el-table-column>
                <el-table-column prop="cost" label="成本" width="90">
                  <template #default="{ row }">
                    {{ row.cost != null ? row.cost.toFixed(2) : '-' }}
                  </template>
                </el-table-column>
                <el-table-column prop="sales" label="销售额" width="90">
                  <template #default="{ row }">
                    {{ row.sales != null ? row.sales.toFixed(2) : '-' }}
                  </template>
                </el-table-column>
                <el-table-column prop="products_sold" label="售出" width="70" />
                <el-table-column prop="actual_cpc" label="CPC" width="80">
                  <template #default="{ row }">
                    {{ row.actual_cpc != null ? row.actual_cpc.toFixed(2) : '-' }}
                  </template>
                </el-table-column>
                <el-table-column prop="cps" label="CPS%" width="80">
                  <template #default="{ row }">
                    {{ row.cps != null ? row.cps.toFixed(2) : '-' }}
                  </template>
                </el-table-column>
              </el-table>
              <el-pagination
                v-model:current-page="adsPage"
                v-model:page-size="adsPageSize"
                :total="adsTotal"
                :page-sizes="[50, 100, 200]"
                @current-change="loadAdsPerformance"
                @size-change="loadAdsPerformance"
                layout="total, sizes, prev, pager, next"
                style="margin-top: 15px"
              />
            </el-tab-pane>
          </el-tabs>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onUnmounted, computed } from 'vue'
import { emagSyncApi } from '@/api/emagSync'
import { ElMessage } from 'element-plus'
import { Search } from '@element-plus/icons-vue'

// Account form
const accountForm = reactive({
  platform: 'ro',
  username: 'sea403464507@gmail.com',
  password: 'g6jYDh0'
})

const accountStatus = ref(null)
const testingConnection = ref(false)
const savingAccount = ref(false)

// Sync status
const syncingProducts = ref(false)
const syncingOrders = ref(false)
const syncingReturns = ref(false)
const syncingAll = ref(false)

// Sync logs
const syncLogs = ref([])

// Marketplace backend login state
const marketplaceLoginForm = reactive({
  username: '',
  password: ''
})
const marketplaceStatus = ref('not_logged_in') // not_logged_in | logging_in | captcha_required | sms_verification_required | logged_in | error
const marketplaceLoggingIn = ref(false)
const marketplaceCheckingCaptcha = ref(false)
const marketplaceSmsCode = ref('')
const marketplaceSubmittingSms = ref(false)
const marketplaceCaptcha = ref('')
const marketplacePageInfo = ref(null)
let marketplaceStatusTimer = null

// Inbound shipments sync
const syncingInboundShipments = ref(false)

// Ads sync
const adsSyncMarketplace = ref('ro')  // ro / bg / hu
const adsDateRange = ref(null)
const syncingAds = ref(false)
const adsSyncMessage = ref('')
const adsSyncMessageType = ref('info')

// Ads data display
const adsPerformance = ref([])
const loadingAds = ref(false)
const adsFilterMarketplace = ref('')  // filter: '' = all
const adsFilterDateRange = ref(null)
const adsSearchCampaignId = ref('')
const adsSearchAdsetId = ref('')
const adsPage = ref(1)
const adsPageSize = ref(50)
const adsTotal = ref(0)

const marketplaceStatusTagType = computed(() => {
  if (marketplaceStatus.value === 'logged_in') return 'success'
  if (marketplaceStatus.value === 'logging_in') return 'warning'
  if (marketplaceStatus.value === 'waiting_manual_login') return 'warning'
  if (marketplaceStatus.value === 'captcha_required') return 'warning'
  if (marketplaceStatus.value === 'sms_verification_required') return 'warning'
  if (marketplaceStatus.value === 'error') return 'danger'
  return 'info'
})

const marketplaceStatusText = computed(() => {
  const map = {
    not_logged_in: '未登录',
    logging_in: '登录中...',
    waiting_manual_login: '等待手动登录',
    captcha_required: '需要图形验证码',
    sms_verification_required: '需要手机验证码',
    logged_in: '已登录',
    error: '登录异常'
  }
  return map[marketplaceStatus.value] || '未知'
})

// Active tab
const activeTab = ref('products')

// Products data
const products = ref([])
const loadingProducts = ref(false)
const productSearch = ref('')
const productPage = ref(1)
const productPageSize = ref(100)
const productTotal = ref(0)

// Orders data
const orders = ref([])
const loadingOrders = ref(false)
const orderSearch = ref('')
const orderDateRange = ref(null)
const orderPage = ref(1)
const orderPageSize = ref(100)
const orderTotal = ref(0)

// Returns data
const returns = ref([])
const loadingReturns = ref(false)
const returnSearch = ref('')
const returnDateRange = ref(null)
const returnPage = ref(1)
const returnPageSize = ref(100)
const returnTotal = ref(0)

// Inbound shipments data
const shipments = ref([])
const loadingShipments = ref(false)
const shipmentSearch = ref('')
const shipmentPage = ref(1)
const shipmentPageSize = ref(50)
const shipmentTotal = ref(0)

// Methods
const addLog = (type, message) => {
  syncLogs.value.unshift({
    time: new Date(),
    type,
    message
  })
  if (syncLogs.value.length > 50) {
    syncLogs.value = syncLogs.value.slice(0, 50)
  }
}

const formatDateTime = (dateTime) => {
  if (!dateTime) return '-'
  const date = new Date(dateTime)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
}

const getLogType = (type) => {
  const typeMap = {
    'success': 'success',
    'error': 'danger',
    'info': 'info',
    'warning': 'warning'
  }
  return typeMap[type] || 'info'
}

const getOrderStatusType = (status) => {
  const statusMap = {
    0: 'info',
    1: 'warning',
    2: 'primary',
    3: 'primary',
    4: 'success',
    5: 'danger'
  }
  return statusMap[status] || 'info'
}

const getOrderStatusText = (status) => {
  const statusMap = {
    0: '取消',
    1: '新订单',
    2: '进行中',
    3: '已准备',
    4: '已完成',
    5: '已退货'
  }
  return statusMap[status] || '未知'
}

const getReturnStatusType = (status) => {
  const statusMap = {
    1: 'warning',
    2: 'primary',
    3: 'primary',
    4: 'success',
    5: 'danger'
  }
  return statusMap[status] || 'info'
}

const getReturnStatusText = (status) => {
  const statusMap = {
    1: '新',
    2: '已确认',
    3: '已收到',
    4: '已解决',
    5: '已拒绝'
  }
  return statusMap[status] || '未知'
}

// API methods
const loadAccountStatus = async () => {
  try {
    const response = await emagSyncApi.getAccount()
    // Fix: axios interceptor returns response.data, so access response directly
    if (response) {
      accountStatus.value = {
        configured: true,
        platform: response.platform
      }
    } else {
      accountStatus.value = { configured: false }
    }
    
    // Also load sync status
    const statusResponse = await emagSyncApi.getSyncStatus()
    // Fix: axios interceptor returns response.data, so access response directly
    if (statusResponse) {
      accountStatus.value = {
        ...accountStatus.value,
        ...statusResponse
      }
    }
  } catch (error) {
    accountStatus.value = { configured: false }
  }
}

const testConnection = async () => {
  if (!accountForm.platform || !accountForm.username || !accountForm.password) {
    ElMessage.warning('请填写完整的API配置信息')
    return
  }
  
  testingConnection.value = true
  try {
    const response = await emagSyncApi.testConnection(accountForm)
    // Fix: axios interceptor returns response.data, so access response.success directly
    if (response?.success) {
      ElMessage.success('连接测试成功')
      addLog('success', 'API连接测试成功')
    } else {
      ElMessage.error('连接测试失败: ' + (response?.message || '未知错误'))
      addLog('error', 'API连接测试失败')
    }
  } catch (error) {
    ElMessage.error('连接测试失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', 'API连接测试失败: ' + error.message)
  } finally {
    testingConnection.value = false
  }
}

const saveAccount = async () => {
  if (!accountForm.platform || !accountForm.username || !accountForm.password) {
    ElMessage.warning('请填写完整的API配置信息')
    return
  }
  
  savingAccount.value = true
  try {
    await emagSyncApi.saveAccount(accountForm)
    ElMessage.success('配置保存成功')
    addLog('success', 'API配置保存成功')
    await loadAccountStatus()
  } catch (error) {
    ElMessage.error('配置保存失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', 'API配置保存失败')
  } finally {
    savingAccount.value = false
  }
}

const updateMarketplaceStateFromResponse = (res, showMessage = true) => {
  if (!res) return
  marketplaceStatus.value = res.status || 'not_logged_in'
  marketplaceCaptcha.value = res.captcha_screenshot_b64 || ''
  marketplacePageInfo.value = res.page_info || null
  if (res.error && showMessage) {
    ElMessage.error(res.error)
    addLog('error', `后台登录失败: ${res.error}`)
  }
  if (marketplaceStatus.value === 'logged_in' && showMessage) {
    ElMessage.success('后台登录成功')
    addLog('success', '后台登录成功')
    marketplaceSmsCode.value = '' // 清空验证码
  } else if (marketplaceStatus.value === 'logging_in' && showMessage) {
    ElMessage.info('登录已启动，正在后台进行中...')
    addLog('info', '后台登录已在后台启动')
  } else if (marketplaceStatus.value === 'waiting_manual_login' && showMessage) {
    ElMessage.info('请在弹出的浏览器窗口中手动完成登录（包括输入用户名、密码和处理验证码）')
    addLog('info', '等待手动登录')
  } else if (marketplaceStatus.value === 'sms_verification_required' && showMessage) {
    ElMessage.warning('需要输入手机验证码')
    addLog('warning', '需要输入手机验证码')
  }
}

const pollMarketplaceStatusOnce = async () => {
  try {
    const res = await emagSyncApi.marketplaceLoginStatus()
    if (res && res.success) {
      updateMarketplaceStateFromResponse(res, false)
      // 如仍在登录中则保留轮询，由调用方决定是否再次调用
    }
  } catch (e) {
    // 忽略单次轮询错误
  }
}

const startMarketplaceStatusPolling = () => {
  if (marketplaceStatusTimer) return
  marketplaceStatusTimer = setInterval(async () => {
    if (marketplaceStatus.value === 'logging_in' || marketplaceStatus.value === 'waiting_manual_login') {
      await pollMarketplaceStatusOnce()
      if (marketplaceStatus.value !== 'logging_in' && marketplaceStatus.value !== 'waiting_manual_login') {
        // 登录已结束，自动停轮询
        stopMarketplaceStatusPolling()
      }
    } else {
      stopMarketplaceStatusPolling()
    }
  }, 2000)
}

const stopMarketplaceStatusPolling = () => {
  if (marketplaceStatusTimer) {
    clearInterval(marketplaceStatusTimer)
    marketplaceStatusTimer = null
  }
}

const handleMarketplaceLogin = async () => {
  // 手动登录方式，不需要用户名密码（但保留输入框以兼容）
  // 如果已经在登录中，防止重复点击
  if (marketplaceStatus.value === 'logging_in' || marketplaceStatus.value === 'waiting_manual_login' || marketplaceLoggingIn.value) {
    ElMessage.info('登录正在进行中，请稍候...')
    return
  }
  marketplaceLoggingIn.value = true
  marketplaceStatus.value = 'logging_in'
  try {
    // 手动登录方式，用户名密码可选
    const res = await emagSyncApi.marketplaceLogin({
      username: marketplaceLoginForm.username || '',
      password: marketplaceLoginForm.password || ''
    })
    updateMarketplaceStateFromResponse(res, true)
    // 如果返回的状态是 logging_in 或 waiting_manual_login，开启轮询
    if (marketplaceStatus.value === 'logging_in' || marketplaceStatus.value === 'waiting_manual_login') {
      startMarketplaceStatusPolling()
    }
  } catch (error) {
    ElMessage.error('后台登录请求失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '后台登录请求失败: ' + error.message)
    marketplaceStatus.value = 'error'
  } finally {
    marketplaceLoggingIn.value = false
  }
}

const handleMarketplaceLogout = async () => {
  try {
    await emagSyncApi.marketplaceLogout()
    marketplaceStatus.value = 'not_logged_in'
    marketplaceCaptcha.value = ''
    marketplacePageInfo.value = null
    stopMarketplaceStatusPolling()
    ElMessage.success('后台已退出')
    addLog('info', '已退出后台登录')
  } catch (error) {
    ElMessage.error('退出后台失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '退出后台失败: ' + error.message)
  }
}

const handleMarketplaceCaptchaDone = async () => {
  marketplaceCheckingCaptcha.value = true
  try {
    const res = await emagSyncApi.marketplaceCaptchaDone()
    updateMarketplaceStateFromResponse(res, true)
    if (marketplaceStatus.value === 'logging_in') {
      startMarketplaceStatusPolling()
    }
  } catch (error) {
    ElMessage.error('验证码确认失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '验证码确认失败: ' + error.message)
  } finally {
    marketplaceCheckingCaptcha.value = false
  }
}

const handleMarketplaceSubmitSmsCode = async () => {
  if (!marketplaceSmsCode.value) {
    ElMessage.warning('请输入手机验证码')
    return
  }
  
  marketplaceSubmittingSms.value = true
  try {
    const res = await emagSyncApi.marketplaceSubmitSmsCode(marketplaceSmsCode.value)
    updateMarketplaceStateFromResponse(res, true)
    if (marketplaceStatus.value === 'logged_in') {
      marketplaceSmsCode.value = '' // 清空验证码
    } else if (marketplaceStatus.value === 'logging_in') {
      startMarketplaceStatusPolling()
    }
  } catch (error) {
    ElMessage.error('提交验证码失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '提交验证码失败: ' + error.message)
  } finally {
    marketplaceSubmittingSms.value = false
  }
}

const handleSyncInboundShipments = async () => {
  if (marketplaceStatus.value !== 'logged_in') {
    ElMessage.warning('请先登录后台')
    return
  }
  
  syncingInboundShipments.value = true
  addLog('info', '开始同步运单详情到数据库...')
  try {
    const res = await emagSyncApi.syncInboundShipmentsDetails()
    if (res?.success) {
      ElMessage.success('运单同步已启动，正在后台进行...')
      addLog('info', '运单同步已在后台启动（获取列表→筛选finalized→获取详情→存入数据库）...')
      
      // 等待一段时间后提示完成
      setTimeout(() => {
        syncingInboundShipments.value = false
        ElMessage.info('运单同步可能需要较长时间，请查看日志了解进度')
      }, 5000)
    } else {
      ElMessage.error('运单同步启动失败: ' + (res?.error || '未知错误'))
      addLog('error', '运单同步启动失败')
      syncingInboundShipments.value = false
    }
  } catch (error) {
    ElMessage.error('运单同步失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '运单同步失败: ' + error.message)
    syncingInboundShipments.value = false
  }
}

const handleSyncAds = async () => {
  if (marketplaceStatus.value !== 'logged_in') {
    ElMessage.warning('请先登录后台')
    return
  }
  if (!adsDateRange.value || adsDateRange.value.length !== 2) {
    ElMessage.warning('请选择日期范围')
    return
  }

  syncingAds.value = true
  const mpLabel = adsSyncMarketplace.value.toUpperCase()
  adsSyncMessage.value = `正在同步 ${mpLabel} 广告数据（Campaign → 广告组 → 产品），请稍候...`
  adsSyncMessageType.value = 'info'
  addLog('info', `开始同步 ${mpLabel} 广告数据 ${adsDateRange.value[0]} ~ ${adsDateRange.value[1]}`)

  try {
    const res = await emagSyncApi.syncAds({
      date_start: adsDateRange.value[0],
      date_end: adsDateRange.value[1],
      marketplace: adsSyncMarketplace.value
    })
    if (res?.success) {
      ElMessage.success('广告数据同步已启动，正在后台进行...')
      adsSyncMessage.value = '同步任务已启动，数据量较大时可能需要几分钟，完成后切换到"广告数据"标签查看'
      adsSyncMessageType.value = 'success'
      addLog('info', '广告数据同步已在后台启动')
      setTimeout(() => { syncingAds.value = false }, 5000)
    } else {
      ElMessage.error('广告同步启动失败: ' + (res?.error || '未知错误'))
      adsSyncMessage.value = '同步启动失败'
      adsSyncMessageType.value = 'error'
      syncingAds.value = false
    }
  } catch (error) {
    ElMessage.error('广告同步失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '广告同步失败: ' + error.message)
    adsSyncMessage.value = '同步失败: ' + error.message
    adsSyncMessageType.value = 'error'
    syncingAds.value = false
  }
}

const loadAdsPerformance = async () => {
  loadingAds.value = true
  try {
    const params = {
      skip: (adsPage.value - 1) * adsPageSize.value,
      limit: adsPageSize.value
    }
    if (adsFilterMarketplace.value) {
      params.marketplace = adsFilterMarketplace.value
    }
    if (adsSearchCampaignId.value) {
      const parsed = parseInt(adsSearchCampaignId.value, 10)
      if (!isNaN(parsed)) params.campaign_id = parsed
    }
    if (adsSearchAdsetId.value) {
      const parsed = parseInt(adsSearchAdsetId.value, 10)
      if (!isNaN(parsed)) params.adset_id = parsed
    }
    if (adsFilterDateRange.value && adsFilterDateRange.value.length === 2) {
      params.date_start = adsFilterDateRange.value[0]
      params.date_end = adsFilterDateRange.value[1]
    }
    const response = await emagSyncApi.getAdsPerformance(params)
    if (response) {
      adsPerformance.value = response.items || []
      adsTotal.value = response.total || 0
    }
  } catch (error) {
    ElMessage.error('加载广告数据失败: ' + (error.response?.data?.detail || error.message))
    adsPerformance.value = []
    adsTotal.value = 0
  } finally {
    loadingAds.value = false
  }
}

const syncProducts = async () => {
  syncingProducts.value = true
  addLog('info', '开始同步产品...')
  try {
    const response = await emagSyncApi.syncProducts()
    // Fix: axios interceptor returns response.data, so access response.success directly
    // Handle background task response
    if (response?.success) {
      if (response?.message && response.message.includes('background')) {
        ElMessage.success('产品同步已启动，正在后台进行...')
        addLog('info', '产品同步已在后台启动')
      } else if (response.records_count !== undefined && response.records_count > 0) {
        ElMessage.success(`产品同步成功，共 ${response.records_count} 条记录`)
        addLog('success', `产品同步成功: ${response.records_count} 条`)
        await loadAccountStatus()
        if (activeTab.value === 'products') {
          await loadProducts()
        }
      } else {
        ElMessage.success('产品同步已启动，正在后台进行...')
        addLog('info', '产品同步已在后台启动')
      }
    } else {
      ElMessage.error('产品同步失败: ' + (response?.error || '未知错误'))
      addLog('error', '产品同步失败: ' + response?.error)
    }
  } catch (error) {
    ElMessage.error('产品同步失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '产品同步失败: ' + error.message)
  } finally {
    syncingProducts.value = false
  }
}

const syncOrders = async () => {
  syncingOrders.value = true
  addLog('info', '开始同步订单...')
  try {
    const response = await emagSyncApi.syncOrders()
    // Fix: axios interceptor returns response.data, so access response.success directly
    // Handle background task response
    if (response?.success) {
      if (response?.message && response.message.includes('background')) {
        // Background task started
        ElMessage.success('订单同步已启动，正在后台进行...')
        addLog('info', '订单同步已在后台启动')
        // Don't wait, just show message
      } else if (response.records_count !== undefined && response.records_count > 0) {
        // Immediate sync completed (shouldn't happen now, but handle it)
        ElMessage.success(`订单同步成功，共 ${response.records_count} 条记录`)
        addLog('success', `订单同步成功: ${response.records_count} 条`)
        await loadAccountStatus()
        if (activeTab.value === 'orders') {
          await loadOrders()
        }
      } else {
        // Background task started (no records_count yet)
        ElMessage.success('订单同步已启动，正在后台进行...')
        addLog('info', '订单同步已在后台启动')
      }
    } else {
      ElMessage.error('订单同步失败: ' + (response?.error || '未知错误'))
      addLog('error', '订单同步失败: ' + response?.error)
    }
  } catch (error) {
    ElMessage.error('订单同步失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '订单同步失败: ' + error.message)
  } finally {
    syncingOrders.value = false
  }
}

const syncReturns = async () => {
  syncingReturns.value = true
  addLog('info', '开始同步退货...')
  try {
    const response = await emagSyncApi.syncReturns()
    // Fix: axios interceptor returns response.data, so access response.success directly
    // Handle background task response
    if (response?.success) {
      if (response?.message && response.message.includes('background')) {
        ElMessage.success('退货同步已启动，正在后台进行...')
        addLog('info', '退货同步已在后台启动')
      } else if (response.records_count !== undefined && response.records_count > 0) {
        ElMessage.success(`退货同步成功，共 ${response.records_count} 条记录`)
        addLog('success', `退货同步成功: ${response.records_count} 条`)
        await loadAccountStatus()
        if (activeTab.value === 'returns') {
          await loadReturns()
        }
      } else {
        ElMessage.success('退货同步已启动，正在后台进行...')
        addLog('info', '退货同步已在后台启动')
      }
    } else {
      ElMessage.error('退货同步失败: ' + (response?.error || '未知错误'))
      addLog('error', '退货同步失败: ' + response?.error)
    }
  } catch (error) {
    ElMessage.error('退货同步失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '退货同步失败: ' + error.message)
  } finally {
    syncingReturns.value = false
  }
}

const syncAll = async () => {
  syncingAll.value = true
  addLog('info', '开始同步全部数据...')
  try {
    const response = await emagSyncApi.syncAll()
    // Fix: axios interceptor returns response.data, so access response.success directly
    // Handle background task response
    if (response?.success) {
      if (response?.message && response.message.includes('background')) {
        ElMessage.success('全部同步已启动，正在后台进行...')
        addLog('info', '全部同步已在后台启动')
      } else if (response.results) {
        const results = response.results
        const total = (results.products?.records_count || 0) + 
                      (results.orders?.records_count || 0) + 
                      (results.returns?.records_count || 0)
        if (total > 0) {
          ElMessage.success(`全部同步成功，共 ${total} 条记录`)
          addLog('success', `全部同步成功: 产品 ${results.products?.records_count || 0} 条, 订单 ${results.orders?.records_count || 0} 条, 退货 ${results.returns?.records_count || 0} 条`)
          await loadAccountStatus()
          await handleTabChange(activeTab.value)
        } else {
          ElMessage.success('全部同步已启动，正在后台进行...')
          addLog('info', '全部同步已在后台启动')
        }
      } else {
        ElMessage.success('全部同步已启动，正在后台进行...')
        addLog('info', '全部同步已在后台启动')
      }
    } else {
      ElMessage.error('全部同步失败')
      addLog('error', '全部同步失败')
    }
  } catch (error) {
    ElMessage.error('全部同步失败: ' + (error.response?.data?.detail || error.message))
    addLog('error', '全部同步失败: ' + error.message)
  } finally {
    syncingAll.value = false
  }
}

const loadProducts = async () => {
  loadingProducts.value = true
  try {
    const params = {
      skip: (productPage.value - 1) * productPageSize.value,
      limit: productPageSize.value
    }
    if (productSearch.value) {
      // Try PNK first, then EAN
      params.pnk_code = productSearch.value
    }
    const response = await emagSyncApi.getProducts(params)
    // Fix: axios interceptor returns response.data, so access response directly
    if (response) {
      products.value = response.items || []
      productTotal.value = response.total || 0
    }
  } catch (error) {
    ElMessage.error('加载产品失败: ' + (error.response?.data?.detail || error.message))
    products.value = []
    productTotal.value = 0
  } finally {
    loadingProducts.value = false
  }
}

const loadOrders = async () => {
  loadingOrders.value = true
  try {
    const params = {
      skip: (orderPage.value - 1) * orderPageSize.value,
      limit: orderPageSize.value
    }
    if (orderSearch.value) {
      params.pnk_code = orderSearch.value
    }
    if (orderDateRange && orderDateRange.value && orderDateRange.value.length === 2) {
      params.date_start = orderDateRange.value[0]
      params.date_end = orderDateRange.value[1]
    }
    const response = await emagSyncApi.getOrders(params)
    // Fix: axios interceptor returns response.data, so access response directly
    if (response) {
      orders.value = response.items || []
      orderTotal.value = response.total || 0
    }
  } catch (error) {
    ElMessage.error('加载订单失败: ' + (error.response?.data?.detail || error.message))
    orders.value = []
    orderTotal.value = 0
  } finally {
    loadingOrders.value = false
  }
}

const loadReturns = async () => {
  loadingReturns.value = true
  try {
    const params = {
      skip: (returnPage.value - 1) * returnPageSize.value,
      limit: returnPageSize.value
    }
    if (returnSearch.value) {
      params.pnk_code = returnSearch.value
    }
    if (returnDateRange && returnDateRange.value && returnDateRange.value.length === 2) {
      params.date_start = returnDateRange.value[0]
      params.date_end = returnDateRange.value[1]
    }
    const response = await emagSyncApi.getReturns(params)
    // Fix: axios interceptor returns response.data, so access response directly
    if (response) {
      returns.value = response.items || []
      returnTotal.value = response.total || 0
    }
  } catch (error) {
    ElMessage.error('加载退货失败: ' + (error.response?.data?.detail || error.message))
    returns.value = []
    returnTotal.value = 0
  } finally {
    loadingReturns.value = false
  }
}

const loadShipments = async () => {
  loadingShipments.value = true
  try {
    const params = {
      skip: (shipmentPage.value - 1) * shipmentPageSize.value,
      limit: shipmentPageSize.value
    }
    if (shipmentSearch.value) {
      const parsed = parseInt(shipmentSearch.value, 10)
      if (!isNaN(parsed)) {
        params.reception_id = parsed
      }
    }
    const response = await emagSyncApi.getInboundShipments(params)
    if (response) {
      shipments.value = response.items || []
      shipmentTotal.value = response.total || 0
    }
  } catch (error) {
    ElMessage.error('加载发货单失败: ' + (error.response?.data?.detail || error.message))
    shipments.value = []
    shipmentTotal.value = 0
  } finally {
    loadingShipments.value = false
  }
}

const handleTabChange = (tabName) => {
  if (tabName === 'products') {
    loadProducts()
  } else if (tabName === 'orders') {
    loadOrders()
  } else if (tabName === 'returns') {
    loadReturns()
  } else if (tabName === 'shipments') {
    loadShipments()
  } else if (tabName === 'ads') {
    loadAdsPerformance()
  }
}

onMounted(async () => {
  await loadAccountStatus()
  await loadProducts()
})

onUnmounted(() => {
  stopMarketplaceStatusPolling()
})
</script>

<style scoped>
.emag-sync-container {
  padding: 20px;
}

.control-panel {
  height: calc(100vh - 100px);
  overflow-y: auto;
}

.data-panel {
  height: calc(100vh - 100px);
}

.section-card {
  margin-bottom: 0;
}

.sync-buttons {
  margin-bottom: 15px;
}

.sync-logs {
  max-height: 300px;
  overflow-y: auto;
  font-size: 12px;
}

.log-item {
  padding: 8px;
  border-bottom: 1px solid #f0f0f0;
}

.log-time {
  color: #909399;
  font-size: 11px;
  margin-bottom: 4px;
}

.log-content {
  display: flex;
  align-items: center;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>


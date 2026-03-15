<template>
  <div class="reports-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>数据报表</span>
        </div>
      </template>

      <el-tabs v-model="activeTab" @tab-change="handleTabChange">
        <!-- ============================================================ -->
        <!-- Tab 1: 产品综合报表 -->
        <!-- ============================================================ -->
        <el-tab-pane label="产品综合报表" name="product-summary">
          <div style="margin-bottom: 15px; display: flex; gap: 10px; align-items: center">
            <el-input
              v-model="summarySearch"
              placeholder="搜索产品名称 / PNK / EAN"
              style="width: 320px"
              clearable
              @input="debouncedLoadSummary"
            >
              <template #prefix>
                <el-icon><Search /></el-icon>
              </template>
            </el-input>
            <el-button type="primary" @click="loadProductSummary" :loading="loadingSummary">
              查询
            </el-button>
          </div>

          <el-table
            :data="summaryData"
            v-loading="loadingSummary"
            style="width: 100%"
            height="calc(100vh - 320px)"
            border
            stripe
            :default-sort="{ prop: 'order_quantity', order: 'descending' }"
          >
            <el-table-column prop="product_id" label="产品ID" width="100" sortable />
            <el-table-column prop="pnk_code" label="PNK" width="140" show-overflow-tooltip />
            <el-table-column prop="ean" label="EAN" width="150" show-overflow-tooltip />
            <el-table-column prop="name" label="产品名称" min-width="240" show-overflow-tooltip />
            <el-table-column prop="brand" label="品牌" width="120" show-overflow-tooltip />
            <el-table-column prop="sale_price" label="售价" width="100" sortable>
              <template #default="{ row }">
                {{ row.sale_price != null ? `€${row.sale_price.toFixed(2)}` : '-' }}
              </template>
            </el-table-column>
            <el-table-column prop="shipment_quantity" label="发货数" width="100" sortable>
              <template #default="{ row }">
                <span :class="{ 'highlight-num': row.shipment_quantity > 0 }">{{ row.shipment_quantity }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="order_count" label="订单数" width="100" sortable>
              <template #default="{ row }">
                <span :class="{ 'highlight-num': row.order_count > 0 }">{{ row.order_count }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="order_quantity" label="订单件数" width="110" sortable>
              <template #default="{ row }">
                <span :class="{ 'highlight-num': row.order_quantity > 0 }">{{ row.order_quantity }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="return_quantity" label="退货数量" width="110" sortable>
              <template #default="{ row }">
                <span :class="{ 'highlight-warn': row.return_quantity > 0 }">{{ row.return_quantity }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="stock" label="库存数" width="100" sortable>
              <template #default="{ row }">
                <el-tag :type="row.stock > 0 ? 'success' : 'danger'" size="small">
                  {{ row.stock }}
                </el-tag>
              </template>
            </el-table-column>
          </el-table>

          <div style="margin-top: 10px; font-size: 12px; color: #909399">
            共 {{ summaryData.length }} 个产品
          </div>
        </el-tab-pane>

        <!-- ============================================================ -->
        <!-- Tab 2: 广告周报表 -->
        <!-- ============================================================ -->
        <el-tab-pane label="广告周报表" name="ads-weekly">
          <div style="margin-bottom: 15px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap">
            <el-select
              v-model="adsWeek"
              placeholder="选择周"
              clearable
              style="width: 180px"
              @change="loadAdsWeekly"
            >
              <el-option
                v-for="w in availableWeeks"
                :key="w"
                :label="w"
                :value="w"
              />
            </el-select>
            <el-input
              v-model="adsSearch"
              placeholder="搜索产品名称 / PNK"
              style="width: 280px"
              clearable
              @input="debouncedLoadAds"
            >
              <template #prefix>
                <el-icon><Search /></el-icon>
              </template>
            </el-input>
            <el-button type="primary" @click="loadAdsWeekly" :loading="loadingAds">
              查询
            </el-button>
          </div>

          <el-table
            :data="adsData"
            v-loading="loadingAds"
            style="width: 100%"
            height="calc(100vh - 320px)"
            border
            stripe
            :default-sort="{ prop: 'cost', order: 'descending' }"
          >
            <el-table-column prop="product_id" label="产品ID" width="100" sortable />
            <el-table-column prop="part_number" label="PNK" width="140" show-overflow-tooltip />
            <el-table-column prop="product_name" label="产品名称" min-width="240" show-overflow-tooltip />
            <el-table-column prop="week" label="周" width="120" sortable />
            <el-table-column prop="impressions" label="曝光" width="110" sortable>
              <template #default="{ row }">
                {{ formatNumber(row.impressions) }}
              </template>
            </el-table-column>
            <el-table-column prop="clicks" label="点击" width="100" sortable>
              <template #default="{ row }">
                {{ formatNumber(row.clicks) }}
              </template>
            </el-table-column>
            <el-table-column prop="ctr" label="CTR(%)" width="100" sortable>
              <template #default="{ row }">
                {{ row.ctr.toFixed(2) }}%
              </template>
            </el-table-column>
            <el-table-column prop="products_sold" label="成交" width="90" sortable>
              <template #default="{ row }">
                <span :class="{ 'highlight-num': row.products_sold > 0 }">{{ row.products_sold }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="cps" label="CPS" width="100" sortable>
              <template #default="{ row }">
                {{ row.cps > 0 ? `€${row.cps.toFixed(2)}` : '-' }}
              </template>
            </el-table-column>
            <el-table-column prop="cost" label="广告成本" width="120" sortable>
              <template #default="{ row }">
                <span :class="{ 'highlight-warn': row.cost > 0 }">€{{ row.cost.toFixed(2) }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="sales" label="销售额" width="120" sortable>
              <template #default="{ row }">
                {{ row.sales > 0 ? `€${row.sales.toFixed(2)}` : '-' }}
              </template>
            </el-table-column>
          </el-table>

          <div style="margin-top: 10px; font-size: 12px; color: #909399">
            共 {{ adsData.length }} 条记录
          </div>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { reportsApi } from '@/api/reports'
import { ElMessage } from 'element-plus'
import { Search } from '@element-plus/icons-vue'

// ----- Active tab -----
const activeTab = ref('product-summary')

// ----- Report 1 state -----
const summaryData = ref([])
const loadingSummary = ref(false)
const summarySearch = ref('')

// ----- Report 2 state -----
const adsData = ref([])
const loadingAds = ref(false)
const adsWeek = ref('')
const adsSearch = ref('')
const availableWeeks = ref([])

// ----- Utility -----
let summaryTimer = null
let adsTimer = null

const formatNumber = (n) => {
  if (n == null) return '0'
  return n.toLocaleString()
}

const debouncedLoadSummary = () => {
  clearTimeout(summaryTimer)
  summaryTimer = setTimeout(() => loadProductSummary(), 400)
}

const debouncedLoadAds = () => {
  clearTimeout(adsTimer)
  adsTimer = setTimeout(() => loadAdsWeekly(), 400)
}

// ----- Load data -----
const loadProductSummary = async () => {
  loadingSummary.value = true
  try {
    const params = {}
    if (summarySearch.value) params.search = summarySearch.value
    const res = await reportsApi.getProductSummary(params)
    summaryData.value = res?.items || []
  } catch (e) {
    ElMessage.error('加载产品报表失败: ' + (e.response?.data?.detail || e.message))
    summaryData.value = []
  } finally {
    loadingSummary.value = false
  }
}

const loadAdsWeekly = async () => {
  loadingAds.value = true
  try {
    const params = {}
    if (adsWeek.value) params.week = adsWeek.value
    if (adsSearch.value) params.search = adsSearch.value
    const res = await reportsApi.getAdsWeekly(params)
    adsData.value = res?.items || []
    if (res?.weeks) {
      availableWeeks.value = res.weeks
    }
  } catch (e) {
    ElMessage.error('加载广告周报表失败: ' + (e.response?.data?.detail || e.message))
    adsData.value = []
  } finally {
    loadingAds.value = false
  }
}

const handleTabChange = (tab) => {
  if (tab === 'product-summary') {
    loadProductSummary()
  } else if (tab === 'ads-weekly') {
    loadAdsWeekly()
  }
}

// ----- Init -----
onMounted(() => {
  loadProductSummary()
  // Pre-load weeks list
  reportsApi.getAdsWeekly({ search: '__none__' }).then(res => {
    if (res?.weeks) availableWeeks.value = res.weeks
  }).catch(() => {})
})
</script>

<style scoped>
.reports-container {
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 16px;
  font-weight: 600;
}

.highlight-num {
  color: #409eff;
  font-weight: 600;
}

.highlight-warn {
  color: #e6a23c;
  font-weight: 600;
}
</style>


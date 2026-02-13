<template>
  <div class="profit-calculation-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>利润测算</span>
          <div>
            <el-button @click="showFeeSettingsDialog = true">费用设置</el-button>
            <el-button @click="goBack">返回产品库</el-button>
          </div>
        </div>
      </template>

      <!-- 筛选器 -->
      <el-form :inline="true" class="filter-form">
        <el-form-item label="状态">
          <el-select v-model="filters.status" placeholder="全部" clearable style="width: 150px" @change="loadProfitList">
            <el-option label="待测算" value="pending_calc" />
            <el-option label="已通过" value="approved" />
            <el-option label="已上架" value="listed" />
            <el-option label="已采购" value="purchased" />
            <el-option label="已放弃" value="rejected" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="loadProfitList">刷新</el-button>
        </el-form-item>
      </el-form>

      <!-- 利润测算列表 -->
      <el-table
        :data="profitList"
        v-loading="loading"
        style="width: 100%"
        :row-class-name="getRowClassName"
        height="calc(100vh - 400px)"
      >
        <el-table-column prop="operator_name" label="操作人" width="120" />
        <el-table-column label="竞品图片" width="100">
          <template #default="{ row }">
            <el-image
              v-if="row.competitor_image"
              :src="row.competitor_image"
              :preview-src-list="[row.competitor_image]"
              fit="cover"
              style="width: 60px; height: 60px; cursor: pointer;"
              :lazy="true"
            >
              <template #error>
                <div style="width: 60px; height: 60px; display: flex; align-items: center; justify-content: center; background: #f5f5f5; color: #999;">无图</div>
              </template>
            </el-image>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="product_name_ro" label="产品名称(RO)" min-width="200" show-overflow-tooltip />
        <el-table-column prop="chinese_name" label="中文名" width="150" />
        <el-table-column prop="model_number" label="型号" width="150" />
        <el-table-column prop="sale_price" label="售价(€)" width="120">
          <template #default="{ row }">
            <span v-if="row.sale_price">{{ row.sale_price.toFixed(2) }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="purchase_price" label="采购价(€)" width="120" />
        <el-table-column label="利润额(€)" width="120">
          <template #default="{ row }">
            <span 
              v-if="row.profit_amount !== null && row.profit_amount !== undefined"
              :class="row.profit_amount >= 0 ? 'profit-positive' : 'profit-negative'"
            >
              {{ row.profit_amount.toFixed(2) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="利润率(%)" width="120">
          <template #default="{ row }">
            <span 
              v-if="row.profit_margin !== null && row.profit_margin !== undefined"
              :class="row.profit_margin >= 0 ? 'profit-positive' : 'profit-negative'"
            >
              {{ row.profit_margin.toFixed(2) }}%
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="ROI(%)" width="100">
          <template #default="{ row }">
            <span 
              v-if="row.roi !== null && row.roi !== undefined"
              :class="row.roi >= 0 ? 'profit-positive' : 'profit-negative'"
            >
              {{ row.roi.toFixed(2) }}%
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="VAT金额(€)" width="120">
          <template #default="{ row }">
            <span v-if="row.vat_amount !== null && row.vat_amount !== undefined">
              {{ row.vat_amount.toFixed(2) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="佣金金额(€)" width="120">
          <template #default="{ row }">
            <span v-if="row.platform_commission_amount !== null && row.platform_commission_amount !== undefined">
              {{ row.platform_commission_amount.toFixed(2) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="物流成本(€)" width="120">
          <template #default="{ row }">
            <span v-if="row.logistics_cost !== null && row.logistics_cost !== undefined">
              {{ row.logistics_cost.toFixed(2) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="getStatusType(row.status)">
              {{ getStatusText(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <el-button size="small" type="success" @click="handleList(row)" :disabled="row.status === 'listed'">
              上架
            </el-button>
            <el-button size="small" type="danger" @click="handleReject(row)" :disabled="row.status === 'rejected'">
              放弃
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="total"
        @current-change="loadProfitList"
        @size-change="loadProfitList"
        layout="total, sizes, prev, pager, next, jumper"
        style="margin-top: 20px; flex-shrink: 0;"
      />
    </el-card>

    <!-- 费用设置对话框 -->
    <el-dialog v-model="showFeeSettingsDialog" title="通用费用设置" width="600px">
      <el-form :model="feeSettings" label-width="150px">
        <el-form-item label="头程物流费 (€)">
          <el-input-number
            v-model="feeSettings.shipping_cost"
            :precision="2"
            :min="0"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="订单处理费 (€)">
          <el-input-number
            v-model="feeSettings.order_fee"
            :precision="2"
            :min="0"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="仓储费 (€)">
          <el-input-number
            v-model="feeSettings.storage_fee"
            :precision="2"
            :min="0"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="平台佣金 (%)">
          <el-input-number
            v-model="feeSettings.platform_commission"
            :precision="2"
            :min="0"
            :max="100"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="VAT (%)">
          <el-input-number
            v-model="feeSettings.vat"
            :precision="2"
            :min="0"
            :max="100"
            style="width: 100%"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showFeeSettingsDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSaveFeeSettings" :loading="savingFeeSettings">
          保存
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { profitApi } from '@/api/profit'
import { listingApi } from '@/api/listing'
import { useStore } from 'vuex'
import { ElMessage, ElMessageBox } from 'element-plus'

const router = useRouter()
const route = useRoute()
const store = useStore()
const isAdmin = computed(() => store.state.auth.user?.role === 'admin')

const loading = ref(false)
const profitList = ref([])
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const filters = reactive({
  status: null
})

const showFeeSettingsDialog = ref(false)
const feeSettings = reactive({
  shipping_cost: 0,
  order_fee: 0,
  storage_fee: 0,
  platform_commission: 0,
  vat: 0
})
const savingFeeSettings = ref(false)

const getRowClassName = ({ row }) => {
  if (row.profit_amount > 0) {
    return 'profit-positive-row'
  } else if (row.profit_amount < 0) {
    return 'profit-negative-row'
  }
  return ''
}

const getStatusType = (status) => {
  const statusMap = {
    'pending_calc': 'info',
    'approved': 'success',
    'listed': 'success',
    'purchased': 'warning',
    'rejected': 'danger'
  }
  return statusMap[status] || 'info'
}

const getStatusText = (status) => {
  const statusMap = {
    'pending_calc': '待测算',
    'approved': '已通过',
    'listed': '已上架',
    'purchased': '已采购',
    'rejected': '已放弃'
  }
  return statusMap[status] || status
}

const loadProfitList = async () => {
  loading.value = true
  try {
    const params = {
      page: page.value,
      page_size: pageSize.value
    }
    if (filters.status) {
      params.status = filters.status
    }
    // 如果URL中有listingId参数，筛选该产品
    if (route.query.listingId) {
      // 这里可以添加额外的筛选逻辑，或者直接加载所有数据后过滤
    }
    const response = await profitApi.getProfitList(params)
    let items = response.items || []
    
    // 如果URL中有listingId参数，只显示该产品
    if (route.query.listingId) {
      items = items.filter(item => item.listing_pool_id === parseInt(route.query.listingId))
    }
    
    profitList.value = items
    total.value = route.query.listingId ? items.length : response.total || 0
  } catch (error) {
    ElMessage.error('加载利润测算列表失败：' + (error.message || '未知错误'))
  } finally {
    loading.value = false
  }
}

const handleList = async (row) => {
  try {
    await ElMessageBox.confirm('确定要上架该产品吗？', '确认上架', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    })
    
    await listingApi.updateStatus(row.listing_pool_id, 'listed')
    ElMessage.success('上架成功')
    await loadProfitList()
  } catch (error) {
    if (error === 'cancel') {
      return
    }
    if (error.response && error.response.status === 400) {
      const errorMessage = error.response.data?.detail || '上架失败，请检查产品信息'
      ElMessage.error(errorMessage)
      if (errorMessage.includes('利润测算')) {
        ElMessage.warning('请先填写完整的产品信息（采购价、尺寸、重量等）并保存后再上架')
      }
    } else {
      ElMessage.error('上架失败：' + (error.message || '未知错误'))
    }
  }
}

const handleReject = async (row) => {
  try {
    await ElMessageBox.confirm('确定要放弃该产品吗？', '确认放弃', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    })
    
    await profitApi.rejectProfit(row.listing_pool_id)
    ElMessage.success('已放弃')
    await loadProfitList()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('操作失败：' + (error.message || '未知错误'))
    }
  }
}

const loadFeeSettings = async () => {
  try {
    const settings = await profitApi.getFeeSettings()
    Object.assign(feeSettings, settings)
  } catch (error) {
    console.error('Load fee settings error:', error)
  }
}

const handleSaveFeeSettings = async () => {
  savingFeeSettings.value = true
  try {
    await profitApi.updateFeeSettings(feeSettings)
    ElMessage.success('费用设置已保存')
    showFeeSettingsDialog.value = false
    await loadProfitList() // 重新加载以应用新设置
  } catch (error) {
    ElMessage.error('保存失败：' + (error.message || '未知错误'))
  } finally {
    savingFeeSettings.value = false
  }
}

const goBack = () => {
  router.push('/product-library')
}

onMounted(async () => {
  await loadProfitList()
  await loadFeeSettings()
})
</script>

<style scoped>
.profit-calculation-container {
  padding: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-form {
  margin-bottom: 20px;
}

.profit-positive {
  color: #67c23a;
  font-weight: bold;
}

.profit-negative {
  color: #f56c6c;
  font-weight: bold;
}

:deep(.profit-positive-row) {
  background-color: #f0f9ff;
}

:deep(.profit-negative-row) {
  background-color: #fef0f0;
}
</style>


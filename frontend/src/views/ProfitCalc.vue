<template>
  <div class="profit-calc-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>利润测算</span>
          <div>
            <el-button @click="showFeeSettingsDialog = true">费用设置</el-button>
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
        <el-table-column label="中文名" width="150">
          <template #default="{ row }">
            <el-input
              v-if="editingRow === row.id && editingField === 'chinese_name'"
              v-model="editingValue"
              size="small"
              @blur="handleSaveEdit(row, 'chinese_name')"
              @keyup.enter="handleSaveEdit(row, 'chinese_name')"
            />
            <span v-else @click="startEdit(row, 'chinese_name', row.chinese_name)" style="cursor: pointer;">
              {{ row.chinese_name || '-' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="型号" width="150">
          <template #default="{ row }">
            <el-input
              v-if="editingRow === row.id && editingField === 'model_number'"
              v-model="editingValue"
              size="small"
              @blur="handleSaveEdit(row, 'model_number')"
              @keyup.enter="handleSaveEdit(row, 'model_number')"
            />
            <span v-else @click="startEdit(row, 'model_number', row.model_number)" style="cursor: pointer;">
              {{ row.model_number || '-' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="profit_amount" label="利润" width="120">
          <template #default="{ row }">
            <span :class="{ 'profit-positive': row.profit_amount > 0, 'profit-negative': row.profit_amount < 0 }">
              {{ row.profit_amount !== null && row.profit_amount !== undefined ? `€${row.profit_amount.toFixed(2)}` : '-' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="profit_margin_without_vat" label="利润率（去除VAT）" width="150">
          <template #default="{ row }">
            {{ row.profit_margin_without_vat !== null && row.profit_margin_without_vat !== undefined ? `${row.profit_margin_without_vat.toFixed(2)}%` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="profit_margin" label="利润率（含VAT）" width="150">
          <template #default="{ row }">
            <span :class="{ 'profit-positive': row.profit_margin > 0, 'profit-negative': row.profit_margin < 0 }">
              {{ row.profit_margin !== null && row.profit_margin !== undefined ? `${row.profit_margin.toFixed(2)}%` : '-' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="类目名称" width="150">
          <template #default="{ row }">
            <el-input
              v-if="editingRow === row.id && editingField === 'category_name'"
              v-model="editingValue"
              size="small"
              @blur="handleSaveEdit(row, 'category_name')"
              @keyup.enter="handleSaveEdit(row, 'category_name')"
            />
            <span v-else @click="startEdit(row, 'category_name', row.category_name)" style="cursor: pointer;">
              {{ row.category_name || '-' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="platform_commission" label="佣金费率" width="120">
          <template #default="{ row }">
            {{ row.platform_commission !== null && row.platform_commission !== undefined ? `${row.platform_commission}%` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="domestic_logistics" label="国内物流" width="120">
          <template #default="{ row }">
            {{ row.domestic_logistics !== null && row.domestic_logistics !== undefined ? `€${row.domestic_logistics.toFixed(2)}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="platform_commission_amount" label="平台佣金" width="120">
          <template #default="{ row }">
            {{ row.platform_commission_amount !== null && row.platform_commission_amount !== undefined ? `€${row.platform_commission_amount.toFixed(2)}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="shipping_cost" label="物流费" width="120">
          <template #default="{ row }">
            {{ row.shipping_cost !== null && row.shipping_cost !== undefined ? `€${row.shipping_cost.toFixed(2)}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="250" fixed="right">
          <template #default="{ row }">
            <div style="display: flex; flex-wrap: wrap; gap: 5px;">
              <el-button size="small" @click="handleViewDetail(row)">查看详情</el-button>
              <el-button size="small" type="success" @click="handleList(row)" :disabled="row.status === 'listed'">
                上架
              </el-button>
              <el-button size="small" type="danger" @click="handleReject(row)" :disabled="row.status === 'rejected'">
                放弃
              </el-button>
            </div>
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

    <!-- 详情对话框 -->
    <el-dialog v-model="showDetailDialog" title="利润测算详情" width="80%">
      <div v-if="currentDetail">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="操作人">{{ currentDetail.operator_name }}</el-descriptions-item>
          <el-descriptions-item label="产品名称(RO)">{{ currentDetail.product_name_ro }}</el-descriptions-item>
          <el-descriptions-item label="中文名">{{ currentDetail.chinese_name || '-' }}</el-descriptions-item>
          <el-descriptions-item label="型号">{{ currentDetail.model_number || '-' }}</el-descriptions-item>
          <el-descriptions-item label="类目名称">{{ currentDetail.category_name || '-' }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(currentDetail.status)">{{ getStatusText(currentDetail.status) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="利润">
            <span :class="{ 'profit-positive': currentDetail.profit_amount > 0, 'profit-negative': currentDetail.profit_amount < 0 }">
              €{{ currentDetail.profit_amount?.toFixed(2) || '0.00' }}
            </span>
          </el-descriptions-item>
          <el-descriptions-item label="利润率（含VAT）">
            <span :class="{ 'profit-positive': currentDetail.profit_margin > 0, 'profit-negative': currentDetail.profit_margin < 0 }">
              {{ currentDetail.profit_margin?.toFixed(2) || '0.00' }}%
            </span>
          </el-descriptions-item>
          <el-descriptions-item label="利润率（去除VAT）">
            {{ currentDetail.profit_margin_without_vat?.toFixed(2) || '-' }}%
          </el-descriptions-item>
          <el-descriptions-item label="佣金费率">{{ currentDetail.platform_commission || '-' }}%</el-descriptions-item>
          <el-descriptions-item label="平台佣金">€{{ currentDetail.platform_commission_amount?.toFixed(2) || '0.00' }}</el-descriptions-item>
          <el-descriptions-item label="国内物流">€{{ currentDetail.domestic_logistics?.toFixed(2) || '0.00' }}</el-descriptions-item>
          <el-descriptions-item label="物流费">€{{ currentDetail.shipping_cost?.toFixed(2) || '0.00' }}</el-descriptions-item>
        </el-descriptions>
        <div v-if="currentDetail.competitor_image" style="margin-top: 20px;">
          <h4>竞品图片</h4>
          <el-image
            :src="currentDetail.competitor_image"
            :preview-src-list="[currentDetail.competitor_image]"
            fit="cover"
            style="width: 200px; height: 200px;"
          />
        </div>
      </div>
    </el-dialog>

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
import { profitApi } from '@/api/profit'
import { listingApi } from '@/api/listing'
import { useStore } from 'vuex'
import { ElMessage, ElMessageBox } from 'element-plus'

const store = useStore()
const isAdmin = computed(() => store.state.auth.user?.role === 'admin')
const currentUserId = computed(() => store.state.auth.user?.id)

const loading = ref(false)
const profitList = ref([])
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const filters = reactive({
  status: null
})

const editingRow = ref(null)
const editingField = ref(null)
const editingValue = ref('')

const showDetailDialog = ref(false)
const currentDetail = ref(null)

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
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:337','message':'Calling getProfitList API','data':{params},timestamp:Date.now(),sessionId:'debug-session',runId:'initial',hypothesisId:'E'})}).catch(()=>{});
    // #endregion
    const response = await profitApi.getProfitList(params)
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:339','message':'Received profit list response','data':{items_count:response.items?.length || 0,total:response.total,first_item:response.items?.[0] || null},timestamp:Date.now(),sessionId:'debug-session',runId:'initial',hypothesisId:'E'})}).catch(()=>{});
    // #endregion
    profitList.value = response.items || []
    total.value = response.total || 0
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:342','message':'Updated profitList value','data':{profitList_length:profitList.value.length,first_item:profitList.value[0] || null},timestamp:Date.now(),sessionId:'debug-session',runId:'initial',hypothesisId:'E'})}).catch(()=>{});
    // #endregion
  } catch (error) {
    ElMessage.error('加载利润测算列表失败')
    console.error(error)
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:345','message':'Error loading profit list','data':{error_message:error.message,error_stack:error.stack},timestamp:Date.now(),sessionId:'debug-session',runId:'initial',hypothesisId:'E'})}).catch(()=>{});
    // #endregion
  } finally {
    loading.value = false
  }
}

const startEdit = (row, field, value) => {
  editingRow.value = row.id
  editingField.value = field
  editingValue.value = value || ''
}

const handleSaveEdit = async (row, field) => {
  if (editingValue.value === (row[field] || '')) {
    editingRow.value = null
    editingField.value = null
    editingValue.value = ''
    return
  }

  try {
    const updateData = {
      [field]: editingValue.value || null
    }
    await profitApi.updateCalculation(row.listing_pool_id, updateData)
    row[field] = editingValue.value
    ElMessage.success('保存成功')
  } catch (error) {
    ElMessage.error('保存失败')
    console.error(error)
  } finally {
    editingRow.value = null
    editingField.value = null
    editingValue.value = ''
  }
}

const handleViewDetail = (row) => {
  currentDetail.value = row
  showDetailDialog.value = true
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
    if (error !== 'cancel') {
      ElMessage.error('上架失败')
      console.error(error)
    }
  }
}

const handleReject = async (row) => {
  try {
    await ElMessageBox.confirm('确定要放弃该利润测算吗？', '确认放弃', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    })
    
    await profitApi.rejectProfit(row.listing_pool_id)
    ElMessage.success('已放弃')
    await loadProfitList()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('操作失败')
      console.error(error)
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
  } catch (error) {
    ElMessage.error('保存失败')
  } finally {
    savingFeeSettings.value = false
  }
}

onMounted(async () => {
  await loadProfitList()
  await loadFeeSettings()
})
</script>

<style scoped>
.profit-calc-container {
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


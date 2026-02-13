<template>
  <div class="profit-calc-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>产品库</span>
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

      <!-- 产品库列表 -->
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
              v-model="row.chinese_name"
              size="small"
              placeholder="请输入中文名"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="型号" width="150">
          <template #default="{ row }">
            <el-input
              v-model="row.model_number"
              size="small"
              placeholder="请输入型号"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="长(cm)" width="120">
          <template #default="{ row }">
            <el-input-number
              v-model="row.length"
              :precision="2"
              :min="0"
              size="small"
              style="width: 100%"
              placeholder="长"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="宽(cm)" width="120">
          <template #default="{ row }">
            <el-input-number
              v-model="row.width"
              :precision="2"
              :min="0"
              size="small"
              style="width: 100%"
              placeholder="宽"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="高(cm)" width="120">
          <template #default="{ row }">
            <el-input-number
              v-model="row.height"
              :precision="2"
              :min="0"
              size="small"
              style="width: 100%"
              placeholder="高"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="重量(kg)" width="120">
          <template #default="{ row }">
            <el-input-number
              v-model="row.weight"
              :precision="2"
              :min="0"
              size="small"
              style="width: 100%"
              placeholder="重量"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="采购价(€)" width="120">
          <template #default="{ row }">
            <el-input-number
              v-model="row.purchase_price"
              :precision="2"
              :min="0"
              size="small"
              style="width: 100%"
              placeholder="采购价"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="售价(€)" width="120">
          <template #default="{ row }">
            <span v-if="row.sale_price">{{ row.sale_price.toFixed(2) }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
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
        <el-table-column label="类目名称" width="150">
          <template #default="{ row }">
            <el-input
              v-model="row.category_name"
              size="small"
              placeholder="请输入类目名称"
              :disabled="!editableRows[row.id]"
            />
          </template>
        </el-table-column>
        <el-table-column label="佣金费率(%)" width="130">
          <template #default="{ row }">
            <el-input-number
              v-model="row.platform_commission"
              :precision="2"
              :min="0"
              :max="100"
              size="small"
              style="width: 100%"
              placeholder="佣金费率"
              :disabled="!!row.auto_commission_rate || !editableRows[row.id]"
            />
            <span v-if="row.auto_commission_rate" style="font-size: 10px; color: #999; display: block;">
              自动: {{ row.auto_commission_rate }}%
            </span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <el-button 
              v-if="!editableRows[row.id]"
              size="small" 
              type="primary" 
              @click="handleEditProduct(row)"
            >
              修改
            </el-button>
            <template v-else>
              <el-button 
                size="small" 
                type="success" 
                @click="handleSaveProduct(row)"
                :loading="savingRows[row.id]"
              >
                保存
              </el-button>
              <el-button 
                size="small" 
                @click="handleCancelEdit(row)"
              >
                取消
              </el-button>
            </template>
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

const editableRows = ref({}) // 用于跟踪每行的编辑状态
const editingRows = ref({}) // 用于跟踪每行的修改状态
const savingRows = ref({}) // 用于跟踪每行的保存状态
const rowBackup = ref({}) // 用于保存编辑前的数据

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
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:350','message':'Loading profit list','data':{params},timestamp:Date.now(),runId:'initial',hypothesisId:'A'})}).catch(()=>{});
    // #endregion
    const response = await profitApi.getProfitList(params)
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:352','message':'Profit list loaded','data':{items_count:response.items?.length || 0,total:response.total},timestamp:Date.now(),runId:'initial',hypothesisId:'A'})}).catch(()=>{});
    // #endregion
    profitList.value = response.items || []
    total.value = response.total || 0
    // 重置编辑状态
    editableRows.value = {}
  } catch (error) {
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:356','message':'Error loading profit list','data':{error_message:error.message,error_stack:error.stack},timestamp:Date.now(),runId:'initial',hypothesisId:'A'})}).catch(()=>{});
    // #endregion
    throw error
  } finally {
    loading.value = false
  }
}

// 修改产品 - 启用编辑模式
const handleEditProduct = (row) => {
  // #region agent log
  fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:368','message':'Starting edit mode','data':{row_id:row.id,listing_pool_id:row.listing_pool_id},timestamp:Date.now(),runId:'initial',hypothesisId:'B'})}).catch(()=>{});
  // #endregion
  // 保存当前数据作为备份
  rowBackup.value[row.id] = {
    chinese_name: row.chinese_name,
    model_number: row.model_number,
    length: row.length,
    width: row.width,
    height: row.height,
    weight: row.weight,
    purchase_price: row.purchase_price,
    category_name: row.category_name,
    platform_commission: row.platform_commission
  }
  editableRows.value[row.id] = true
}

// 取消编辑
const handleCancelEdit = (row) => {
  // 恢复备份数据
  if (rowBackup.value[row.id]) {
    Object.assign(row, rowBackup.value[row.id])
    delete rowBackup.value[row.id]
  }
  editableRows.value[row.id] = false
}

// 保存产品 - 保存当前行的所有数据
const handleSaveProduct = async (row) => {
  // #region agent log
  fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:395','message':'Starting save product','data':{row_id:row.id,listing_pool_id:row.listing_pool_id,update_data:{chinese_name:row.chinese_name,model_number:row.model_number,length:row.length,width:row.width,height:row.height,weight:row.weight,purchase_price:row.purchase_price,category_name:row.category_name,platform_commission:row.platform_commission}},timestamp:Date.now(),runId:'initial',hypothesisId:'C'})}).catch(()=>{});
  // #endregion
  savingRows.value[row.id] = true
  try {
    const updateData = {
      chinese_name: row.chinese_name || null,
      model_number: row.model_number || null,
      length: row.length !== null && row.length !== undefined ? row.length : null,
      width: row.width !== null && row.width !== undefined ? row.width : null,
      height: row.height !== null && row.height !== undefined ? row.height : null,
      weight: row.weight !== null && row.weight !== undefined ? row.weight : null,
      purchase_price: row.purchase_price !== null && row.purchase_price !== undefined ? row.purchase_price : null,
      category_name: row.category_name || null,
      platform_commission: row.platform_commission !== null && row.platform_commission !== undefined ? row.platform_commission : null
    }
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:408','message':'Calling updateCalculation API','data':{listing_pool_id:row.listing_pool_id,updateData},timestamp:Date.now(),runId:'initial',hypothesisId:'C'})}).catch(()=>{});
    // #endregion
    await profitApi.updateCalculation(row.listing_pool_id, updateData)
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:410','message':'Update calculation API success','data':{listing_pool_id:row.listing_pool_id},timestamp:Date.now(),runId:'initial',hypothesisId:'C'})}).catch(()=>{});
    // #endregion
    ElMessage.success('保存成功')
    editableRows.value[row.id] = false
    delete rowBackup.value[row.id]
    await loadProfitList() // 重新加载列表以更新利润计算
  } catch (error) {
    // #region agent log
    fetch('http://127.0.0.1:7243/ingest/fa287b08-cc79-4533-9772-24c8be69156a',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'ProfitCalc.vue:416','message':'Error saving product','data':{error_message:error.message,error_response:error.response?.data,error_status:error.response?.status},timestamp:Date.now(),runId:'initial',hypothesisId:'C'})}).catch(()=>{});
    // #endregion
    throw error
  } finally {
    savingRows.value[row.id] = false
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
    // Handle API errors
    if (error.response && error.response.status === 400) {
      const errorMessage = error.response.data?.detail || '上架失败，请检查产品信息'
      ElMessage.error(errorMessage)
      // If error mentions profit calculation, suggest editing the product
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
      throw error
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
    throw error
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

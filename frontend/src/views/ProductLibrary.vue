<template>
  <div class="product-library-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>产品库</span>
        </div>
      </template>

      <!-- 筛选器 -->
      <el-form :inline="true" class="filter-form">
        <el-form-item label="状态">
          <el-select v-model="filters.status" placeholder="全部" clearable style="width: 150px" @change="loadProductList">
            <el-option label="待测算" value="pending_calc" />
            <el-option label="已通过" value="approved" />
            <el-option label="已上架" value="listed" />
            <el-option label="已采购" value="purchased" />
            <el-option label="已放弃" value="rejected" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="loadProductList">刷新</el-button>
        </el-form-item>
      </el-form>

      <!-- 产品库列表 -->
      <el-table
        :data="productList"
        v-loading="loading"
        style="width: 100%"
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
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="getStatusType(row.status)">
              {{ getStatusText(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="250" fixed="right">
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
            <el-button size="small" type="info" @click="goToProfitCalculation(row)">
              利润测算
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="total"
        @current-change="loadProductList"
        @size-change="loadProductList"
        layout="total, sizes, prev, pager, next, jumper"
        style="margin-top: 20px; flex-shrink: 0;"
      />
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { profitApi } from '@/api/profit'
import { useStore } from 'vuex'
import { ElMessage } from 'element-plus'

const router = useRouter()
const store = useStore()
const isAdmin = computed(() => store.state.auth.user?.role === 'admin')
const currentUserId = computed(() => store.state.auth.user?.id)

const loading = ref(false)
const productList = ref([])
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const filters = reactive({
  status: null
})

const editableRows = ref({})
const savingRows = ref({})
const rowBackup = ref({})

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

const loadProductList = async () => {
  loading.value = true
  try {
    const params = {
      page: page.value,
      page_size: pageSize.value
    }
    if (filters.status) {
      params.status = filters.status
    }
    const response = await profitApi.getProfitList(params)
    productList.value = response.items || []
    total.value = response.total || 0
    editableRows.value = {}
  } catch (error) {
    ElMessage.error('加载产品列表失败：' + (error.message || '未知错误'))
  } finally {
    loading.value = false
  }
}

const handleEditProduct = (row) => {
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

const handleCancelEdit = (row) => {
  if (rowBackup.value[row.id]) {
    Object.assign(row, rowBackup.value[row.id])
    delete rowBackup.value[row.id]
  }
  editableRows.value[row.id] = false
}

const handleSaveProduct = async (row) => {
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
    await profitApi.updateCalculation(row.listing_pool_id, updateData)
    ElMessage.success('保存成功')
    editableRows.value[row.id] = false
    delete rowBackup.value[row.id]
    await loadProductList()
  } catch (error) {
    ElMessage.error('保存失败：' + (error.message || '未知错误'))
  } finally {
    savingRows.value[row.id] = false
  }
}

const goToProfitCalculation = (row) => {
  router.push({
    path: '/profit-calculation',
    query: { listingId: row.listing_pool_id }
  })
}

onMounted(async () => {
  await loadProductList()
})
</script>

<style scoped>
.product-library-container {
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
</style>


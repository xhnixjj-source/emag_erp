<template>
  <div class="filter-pool-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>筛选池</span>
          <div>
            <el-button type="primary" @click="handleApplyFilter" :loading="filtering">
              应用筛选
            </el-button>
            <el-button @click="handleResetFilter">重置</el-button>
            <el-button @click="handleExport">导出</el-button>
          </div>
        </div>
      </template>

      <!-- 筛选器 -->
      <el-form :model="filters" label-width="100px" class="filter-form">
        <el-row :gutter="20">
          <el-col :span="6">
            <el-form-item label="价格区间">
              <el-input-number v-model="filters.price_min" :min="0" :precision="2" placeholder="最低价" style="width: 45%" />
              <span style="margin: 0 5px">-</span>
              <el-input-number v-model="filters.price_max" :min="0" :precision="2" placeholder="最高价" style="width: 45%" />
            </el-form-item>
          </el-col>
          <el-col :span="6">
            <el-form-item label="评论数">
              <el-input-number v-model="filters.review_count_min" :min="0" placeholder="最少评论" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="6">
            <el-form-item label="店铺排名">
              <el-input-number v-model="filters.shop_rank_max" :min="1" placeholder="最大排名" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="6">
            <el-form-item label="类目排名">
              <el-input-number v-model="filters.category_rank_max" :min="1" placeholder="最大排名" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="6">
            <el-form-item label="广告排名">
              <el-input-number v-model="filters.ad_rank_max" :min="1" placeholder="最大排名" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="6">
            <el-form-item label="库存状态">
              <el-select v-model="filters.stock" placeholder="全部" clearable style="width: 100%">
                <el-option label="有货" value="in_stock" />
                <el-option label="缺货" value="out_of_stock" />
              </el-select>
            </el-form-item>
          </el-col>
        </el-row>
      </el-form>

      <el-divider />

      <!-- 产品列表 -->
      <div class="table-header">
        <div>
          <el-checkbox v-model="selectAll" @change="handleSelectAll">全选</el-checkbox>
          <span style="margin-left: 20px">
            已选择 {{ selectedProducts.length }} 个产品
          </span>
        </div>
        <el-button
          type="primary"
          :disabled="selectedProducts.length === 0"
          @click="handleMoveToMonitor"
        >
          移入监控池
        </el-button>
      </div>

      <el-table
        :data="products"
        v-loading="loading"
        @selection-change="handleSelectionChange"
        style="width: 100%"
        height="calc(100vh - 450px)"
      >
        <el-table-column type="selection" width="55" />
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column label="缩略图" width="100">
          <template #default="{ row }">
            <el-image
              v-if="row.thumbnail_image"
              :src="row.thumbnail_image"
              :preview-src-list="[row.thumbnail_image]"
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
        <el-table-column prop="product_name" label="产品名称" show-overflow-tooltip min-width="200" />
        <el-table-column prop="brand" label="品牌" width="120" />
        <el-table-column prop="shop_name" label="店铺名称" width="150" show-overflow-tooltip />
        <el-table-column label="产品链接" min-width="250" show-overflow-tooltip>
          <template #default="{ row }">
            <a :href="row.product_url" target="_blank" style="color: #409eff; text-decoration: none;">
              {{ row.product_url }}
            </a>
          </template>
        </el-table-column>
        <el-table-column prop="listed_at" label="上架日期" width="150">
          <template #default="{ row }">
            <span v-if="row.listed_at">
              {{ formatDate(row.listed_at) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="price" label="价格" width="100">
          <template #default="{ row }">
            {{ row.price ? `€${row.price}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="stock" label="库存" width="100">
          <template #default="{ row }">
            <span :style="{ color: row.stock > 0 ? '#67c23a' : '#f56c6c' }">
              {{ row.stock !== null && row.stock !== undefined ? row.stock : '-' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="review_count" label="评论数" width="100" />
        <el-table-column label="FBE" width="80">
          <template #default="{ row }">
            <el-tag :type="row.is_fbe ? 'success' : 'info'" size="small">
              {{ row.is_fbe ? '是' : '否' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="competitor_count" label="跟卖数" width="100" />
        <el-table-column prop="shop_rank" label="店铺排名" width="120" />
        <el-table-column prop="category_rank" label="类目排名" width="120" />
        <el-table-column prop="ad_rank" label="广告排名" width="120" />
        <el-table-column prop="crawled_at" label="爬取时间" width="180" />
      </el-table>

      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="total"
        @current-change="loadProducts"
        @size-change="loadProducts"
        layout="total, sizes, prev, pager, next, jumper"
        style="margin-top: 20px; flex-shrink: 0;"
      />
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { filterPoolApi } from '@/api/filterPool'
import { ElMessage, ElMessageBox } from 'element-plus'

const loading = ref(false)
const filtering = ref(false)
const products = ref([])
const selectedProducts = ref([])
const selectAll = ref(false)
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const filters = reactive({
  price_min: null,
  price_max: null,
  review_count_min: null,
  shop_rank_max: null,
  category_rank_max: null,
  ad_rank_max: null,
  stock: null
})

// 简单日期格式化（假设后端返回 ISO 字符串）
const formatDate = (value) => {
  if (!value) return ''
  try {
    const d = new Date(value)
    if (Number.isNaN(d.getTime())) {
      return value
    }
    return d.toLocaleDateString()
  } catch (e) {
    return value
  }
}

const loadProducts = async () => {
  loading.value = true
  try {
    
    // Convert frontend params to backend expected format
    // Backend expects: skip, limit, min_price, max_price, min_review_count, max_shop_rank, max_category_rank, has_stock (bool)
    const skip = (page.value - 1) * pageSize.value
    const params = {
      skip: skip,
      limit: pageSize.value
    }
    
    // Map filter parameters to backend format
    if (filters.price_min !== null && filters.price_min !== '') {
      params.min_price = filters.price_min
    }
    if (filters.price_max !== null && filters.price_max !== '') {
      params.max_price = filters.price_max
    }
    if (filters.review_count_min !== null && filters.review_count_min !== '') {
      params.min_review_count = filters.review_count_min
    }
    if (filters.shop_rank_max !== null && filters.shop_rank_max !== '') {
      params.max_shop_rank = filters.shop_rank_max
    }
    if (filters.category_rank_max !== null && filters.category_rank_max !== '') {
      params.max_category_rank = filters.category_rank_max
    }
    // Convert stock string to boolean for has_stock
    if (filters.stock !== null && filters.stock !== '') {
      params.has_stock = filters.stock === 'in_stock'
    }
    
    
    const response = await filterPoolApi.getProducts(params)
    
    
    products.value = response.data || response.items || []
    total.value = response.total || 0
    
  } catch (error) {
    ElMessage.error('加载产品列表失败')
  } finally {
    loading.value = false
  }
}

const handleApplyFilter = async () => {
  filtering.value = true
  try {
    // 直接应用筛选条件到查询参数，不需要调用单独的筛选API
    page.value = 1
    await loadProducts()
    ElMessage.success('筛选条件已应用')
  } catch (error) {
    ElMessage.error('应用筛选失败')
  } finally {
    filtering.value = false
  }
}

const handleResetFilter = () => {
  Object.keys(filters).forEach(key => {
    filters[key] = null
  })
  loadProducts()
}

const handleSelectionChange = (selection) => {
  selectedProducts.value = selection.map(p => p.id)
  selectAll.value = selection.length === products.value.length && products.value.length > 0
}

const handleSelectAll = (checked) => {
  if (checked) {
    selectedProducts.value = products.value.map(p => p.id)
  } else {
    selectedProducts.value = []
  }
}

const handleMoveToMonitor = async () => {
  if (selectedProducts.value.length === 0) {
    ElMessage.warning('请选择要移入监控池的产品')
    return
  }
  
  try {
    
    await ElMessageBox.confirm(
      `确定要将 ${selectedProducts.value.length} 个产品移入监控池吗？`,
      '确认操作',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )
    
    
    await filterPoolApi.moveToMonitor(selectedProducts.value)
    
    
    ElMessage.success('产品已移入监控池')
    selectedProducts.value = []
    selectAll.value = false
    await loadProducts()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('移入监控池失败')
    }
  }
}

const handleExport = async () => {
  try {
    const params = { ...filters }
    Object.keys(params).forEach(key => {
      if (params[key] === null || params[key] === '') {
        delete params[key]
      }
    })
    
    const blob = await filterPoolApi.exportProducts(params)
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `filter_pool_${new Date().getTime()}.csv`
    a.click()
    window.URL.revokeObjectURL(url)
    ElMessage.success('导出成功')
  } catch (error) {
    ElMessage.error('导出失败')
  }
}

onMounted(() => {
  loadProducts()
})
</script>

<style scoped>
.filter-pool-container {
  padding: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-form {
  margin-top: 20px;
}

.table-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}
</style>


<template>
  <div class="listing-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>上架产品库</span>
          <el-button type="primary" @click="openAddDialog">从监控池添加</el-button>
        </div>
      </template>

      <!-- 筛选器 -->
      <el-form :inline="true" class="filter-form">
        <el-form-item label="状态">
          <el-select v-model="filters.status" placeholder="全部" clearable style="width: 150px">
            <el-option label="待测算" value="pending" />
            <el-option label="已通过" value="approved" />
            <el-option label="已上架" value="listed" />
            <el-option label="已采购" value="purchased" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="loadProducts">查询</el-button>
          <el-button @click="resetFilters">重置</el-button>
        </el-form-item>
      </el-form>

      <!-- 产品列表 -->
      <el-table :data="products" v-loading="loading" style="width: 100%" height="calc(100vh - 400px)">
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column prop="product_url" label="产品链接" show-overflow-tooltip min-width="300" />
        <el-table-column prop="status" label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="getStatusType(row.status)">
              {{ getStatusText(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="锁定状态" width="120">
          <template #default="{ row }">
            <el-tag v-if="row.is_locked" type="warning">
              <el-icon><Lock /></el-icon>
              已锁定
            </el-tag>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="locked_by_user_id" label="锁定人" width="120" v-if="isAdmin" />
        <el-table-column prop="created_at" label="创建时间" width="180" />
        <el-table-column label="操作" width="250" fixed="right">
          <template #default="{ row }">
            <el-button size="small" @click="viewProduct(row.id)">查看详情</el-button>
            <el-button 
              size="small" 
              type="primary" 
              @click="editProduct(row.id)"
              :disabled="isProductLocked(row)"
            >
              编辑
            </el-button>
            <el-button 
              v-if="isAdmin && row.is_locked" 
              size="small" 
              type="warning" 
              @click="unlockProduct(row.id)"
            >
              解锁
            </el-button>
          </template>
        </el-table-column>
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

    <!-- 添加产品对话框 -->
    <el-dialog v-model="showAddDialog" title="从监控池添加产品" width="600px">
      <el-table 
        :data="monitorProducts" 
        v-loading="monitorLoading"
        @selection-change="handleMonitorSelection"
        style="width: 100%"
      >
        <el-table-column type="selection" width="55" />
        <el-table-column prop="product_url" label="产品链接" show-overflow-tooltip />
        <el-table-column prop="last_monitored_at" label="最后监控时间" width="180" />
      </el-table>
      <template #footer>
        <el-button @click="showAddDialog = false">取消</el-button>
        <el-button type="primary" @click="handleAddProducts" :disabled="selectedMonitorIds.length === 0">
          确定添加
        </el-button>
      </template>
    </el-dialog>

    <!-- 产品详情对话框 -->
    <el-dialog 
      v-model="showDetailDialog" 
      :title="currentProduct ? `产品详情 - ${currentProduct.id}` : '产品详情'"
      width="80%"
      :close-on-click-modal="false"
    >
      <div v-if="currentProduct">
        <!-- 锁定状态提示 -->
        <el-alert
          v-if="currentProduct.is_locked && !canEditProduct(currentProduct)"
          :title="`该产品已被用户ID ${currentProduct.locked_by_user_id} 锁定，无法编辑`"
          type="warning"
          :closable="false"
          style="margin-bottom: 20px"
        />

        <el-tabs v-model="activeTab">
          <!-- 基本信息 -->
          <el-tab-pane label="基本信息" name="basic">
            <el-descriptions :column="2" border>
              <el-descriptions-item label="产品ID">{{ currentProduct.id }}</el-descriptions-item>
              <el-descriptions-item label="状态">
                <el-tag :type="getStatusType(currentProduct.status)">
                  {{ getStatusText(currentProduct.status) }}
                </el-tag>
              </el-descriptions-item>
              <el-descriptions-item label="产品链接" :span="2">
                <a :href="currentProduct.product_url" target="_blank">{{ currentProduct.product_url }}</a>
              </el-descriptions-item>
              <el-descriptions-item label="锁定状态">
                <el-tag v-if="currentProduct.is_locked" type="warning">已锁定</el-tag>
                <span v-else>未锁定</span>
              </el-descriptions-item>
              <el-descriptions-item label="锁定人" v-if="currentProduct.is_locked">
                用户ID: {{ currentProduct.locked_by_user_id }}
              </el-descriptions-item>
              <el-descriptions-item label="创建时间">{{ currentProduct.created_at }}</el-descriptions-item>
              <el-descriptions-item label="锁定时间" v-if="currentProduct.locked_at">
                {{ currentProduct.locked_at }}
              </el-descriptions-item>
            </el-descriptions>

            <!-- 状态流转 -->
            <div style="margin-top: 20px">
              <h3>状态流转</h3>
              <el-select 
                v-model="statusChange" 
                placeholder="选择新状态"
                :disabled="!canEditProduct(currentProduct)"
              >
                <el-option label="待测算" value="pending" />
                <el-option label="已通过" value="approved" />
                <el-option label="已上架" value="listed" />
                <el-option label="已采购" value="purchased" />
              </el-select>
              <el-button 
                type="primary" 
                @click="handleStatusChange"
                :disabled="!statusChange || !canEditProduct(currentProduct)"
                style="margin-left: 10px"
              >
                更新状态
              </el-button>
            </div>
          </el-tab-pane>

          <!-- 上架详情 -->
          <el-tab-pane label="上架详情" name="details">
            <el-form :model="productDetails" label-width="120px">
              <el-form-item label="图片链接">
                <el-input
                  v-model="productDetails.image_urls_text"
                  type="textarea"
                  :rows="5"
                  placeholder="每行一个图片链接"
                  :disabled="!canEditProduct(currentProduct)"
                />
              </el-form-item>
              <el-form-item label="竞品链接">
                <el-input
                  v-model="productDetails.competitor_urls_text"
                  type="textarea"
                  :rows="5"
                  placeholder="每行一个竞品链接"
                  :disabled="!canEditProduct(currentProduct)"
                />
              </el-form-item>
              <el-form-item label="Listing HTML">
                <el-input
                  v-model="productDetails.listing_html"
                  type="textarea"
                  :rows="10"
                  placeholder="输入Listing HTML代码"
                  :disabled="!canEditProduct(currentProduct)"
                />
              </el-form-item>
              <el-form-item>
                <el-button 
                  type="primary" 
                  @click="handleSaveDetails"
                  :disabled="!canEditProduct(currentProduct)"
                >
                  保存
                </el-button>
                <el-button @click="handlePreview">预览</el-button>
              </el-form-item>
            </el-form>
          </el-tab-pane>

          <!-- 操作历史 -->
          <el-tab-pane label="操作历史" name="history">
            <el-table :data="operationHistory" v-loading="historyLoading" style="width: 100%">
              <el-table-column prop="operation_type" label="操作类型" width="150" />
              <el-table-column prop="user_id" label="操作人" width="100" />
              <el-table-column prop="operation_detail" label="操作详情" show-overflow-tooltip />
              <el-table-column prop="ip_address" label="IP地址" width="150" />
              <el-table-column prop="created_at" label="操作时间" width="180" />
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </div>
    </el-dialog>

    <!-- 预览对话框 -->
    <el-dialog v-model="showPreviewDialog" title="HTML预览" width="80%">
      <div v-html="productDetails.listing_html" class="preview-content"></div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { listingApi } from '@/api/listing'
import { monitorPoolApi } from '@/api/monitorPool'
import { operationLogApi } from '@/api/operationLog'
import { useStore } from 'vuex'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Lock } from '@element-plus/icons-vue'

const store = useStore()
const isAdmin = computed(() => store.state.auth.user?.role === 'admin')
const currentUserId = computed(() => store.state.auth.user?.id)

const loading = ref(false)
const products = ref([])
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const filters = reactive({
  status: null
})

const showAddDialog = ref(false)
const monitorProducts = ref([])
const monitorLoading = ref(false)
const selectedMonitorIds = ref([])

const showDetailDialog = ref(false)
const currentProduct = ref(null)
const activeTab = ref('basic')
const statusChange = ref(null)
const productDetails = reactive({
  image_urls_text: '',
  competitor_urls_text: '',
  listing_html: ''
})
const operationHistory = ref([])
const historyLoading = ref(false)
const showPreviewDialog = ref(false)

const loadProducts = async () => {
  loading.value = true
  try {
    const params = {
      page: page.value,
      page_size: pageSize.value,
      ...filters
    }
    Object.keys(params).forEach(key => {
      if (params[key] === null || params[key] === '') {
        delete params[key]
      }
    })
    
    const response = await listingApi.getProducts(params)
    products.value = response.data || response.items || []
    total.value = response.total || 0
  } catch (error) {
    ElMessage.error('加载产品列表失败')
  } finally {
    loading.value = false
  }
}

const resetFilters = () => {
  filters.status = null
  loadProducts()
}

const isProductLocked = (product) => {
  if (!product.is_locked) return false
  if (isAdmin.value) return false
  return product.locked_by_user_id !== currentUserId.value
}

const canEditProduct = (product) => {
  if (!product.is_locked) return true
  if (isAdmin.value) return true
  return product.locked_by_user_id === currentUserId.value
}

const getStatusType = (status) => {
  const map = {
    'pending': 'info',
    'approved': 'success',
    'listed': 'warning',
    'purchased': 'success'
  }
  return map[status] || 'info'
}

const getStatusText = (status) => {
  const map = {
    'pending': '待测算',
    'approved': '已通过',
    'listed': '已上架',
    'purchased': '已采购'
  }
  return map[status] || status
}

const loadMonitorProducts = async () => {
  monitorLoading.value = true
  try {
    const response = await monitorPoolApi.getProducts({ page: 1, page_size: 100 })
    monitorProducts.value = response.data || response.items || []
  } catch (error) {
    ElMessage.error('加载监控池产品失败')
  } finally {
    monitorLoading.value = false
  }
}

const openAddDialog = async () => {
  showAddDialog.value = true
  await loadMonitorProducts()
}

const handleMonitorSelection = (selection) => {
  selectedMonitorIds.value = selection.map(p => p.id)
}

const handleAddProducts = async () => {
  try {
    for (const monitorId of selectedMonitorIds.value) {
      await listingApi.addProduct(monitorId)
    }
    ElMessage.success(`成功添加 ${selectedMonitorIds.value.length} 个产品`)
    showAddDialog.value = false
    selectedMonitorIds.value = []
    await loadProducts()
  } catch (error) {
    ElMessage.error('添加产品失败')
  }
}

const viewProduct = async (id) => {
  showDetailDialog.value = true
  activeTab.value = 'basic'
  try {
    const product = await listingApi.getProduct(id)
    currentProduct.value = product
    
    // 加载上架详情
    if (product.listing_details) {
      const details = product.listing_details
      productDetails.image_urls_text = Array.isArray(details.image_urls) 
        ? details.image_urls.join('\n') 
        : ''
      productDetails.competitor_urls_text = Array.isArray(details.competitor_urls)
        ? details.competitor_urls.join('\n')
        : ''
      productDetails.listing_html = details.listing_html || ''
    } else {
      productDetails.image_urls_text = ''
      productDetails.competitor_urls_text = ''
      productDetails.listing_html = ''
    }
    
    // 加载操作历史
    await loadOperationHistory(id)
  } catch (error) {
    ElMessage.error('加载产品详情失败')
  }
}

const editProduct = async (id) => {
  await viewProduct(id)
  activeTab.value = 'details'
}

const loadOperationHistory = async (productId) => {
  historyLoading.value = true
  try {
    const response = await operationLogApi.getLogs({
      target_type: 'listing_pool',
      target_id: productId,
      limit: 100
    })
    operationHistory.value = response.data || response.items || []
  } catch (error) {
    console.error('Load operation history error:', error)
  } finally {
    historyLoading.value = false
  }
}

const handleStatusChange = async () => {
  if (!currentProduct.value || !statusChange.value) return
  
  if (!canEditProduct(currentProduct.value)) {
    ElMessage.warning('该产品已被锁定，无法修改状态')
    return
  }
  
  try {
    await listingApi.updateStatus(currentProduct.value.id, statusChange.value)
    ElMessage.success('状态更新成功')
    
    // 如果状态变为"已上架"，产品会自动锁定
    if (statusChange.value === 'listed') {
      ElMessage.info('产品已自动锁定')
    }
    
    statusChange.value = null
    await viewProduct(currentProduct.value.id)
    await loadProducts()
  } catch (error) {
    ElMessage.error('状态更新失败')
  }
}

const handleSaveDetails = async () => {
  if (!currentProduct.value) return
  
  if (!canEditProduct(currentProduct.value)) {
    ElMessage.warning('该产品已被锁定，无法编辑')
    return
  }
  
  try {
    const imageUrls = productDetails.image_urls_text
      .split('\n')
      .map(url => url.trim())
      .filter(url => url)
    
    const competitorUrls = productDetails.competitor_urls_text
      .split('\n')
      .map(url => url.trim())
      .filter(url => url)
    
    await listingApi.updateProduct(currentProduct.value.id, {
      image_urls: imageUrls,
      competitor_urls: competitorUrls,
      listing_html: productDetails.listing_html
    })
    
    ElMessage.success('保存成功')
    await viewProduct(currentProduct.value.id)
  } catch (error) {
    ElMessage.error('保存失败')
  }
}

const handlePreview = () => {
  showPreviewDialog.value = true
}

const unlockProduct = async (id) => {
  try {
    await ElMessageBox.confirm('确定要解锁该产品吗？', '确认操作', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    })
    
    await listingApi.unlockProduct(id)
    ElMessage.success('产品已解锁')
    await viewProduct(id)
    await loadProducts()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('解锁失败')
    }
  }
}

onMounted(() => {
  loadProducts()
})
</script>

<style scoped>
.listing-container {
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

.preview-content {
  padding: 20px;
  border: 1px solid #ddd;
  border-radius: 4px;
  max-height: 600px;
  overflow-y: auto;
}
</style>


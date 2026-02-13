<template>
  <div class="monitor-pool-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>监控池</span>
          <div>
            <el-button type="success" @click="handleMoveToProfit" :loading="movingToProfit" :disabled="selectedProducts.length === 0">
              进入利润测算
            </el-button>
            <el-button type="primary" @click="handleTriggerMonitor" :loading="triggering">
              手动触发监控
            </el-button>
            <el-button @click="showScheduleDialog = true">定时任务配置</el-button>
          </div>
        </div>
      </template>

      <!-- 产品列表 -->
      <el-table 
        :data="products" 
        v-loading="loading" 
        @selection-change="handleSelectionChange"
        style="width: 100%"
        height="calc(100vh - 350px)"
      >
        <el-table-column type="selection" width="55" />
        <el-table-column label="缩略图" width="80">
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
        <el-table-column prop="rating" label="评分" width="100">
          <template #default="{ row }">
            {{ row.rating !== null && row.rating !== undefined ? row.rating.toFixed(2) : '-' }}
          </template>
        </el-table-column>
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
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 'active' ? 'success' : 'info'">
              {{ row.status === 'active' ? '监控中' : '已停止' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="listed_at" label="上架日期" width="180">
          <template #default="{ row }">
            {{ formatDate(row.listed_at) }}
          </template>
        </el-table-column>
        <el-table-column prop="last_monitored_at" label="最后监控时间" width="180" />
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <el-button size="small" @click="viewHistory(row.id)">查看历史</el-button>
            <el-button size="small" type="danger" @click="handleRemove(row.id)">移除</el-button>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="total"
        @current-change="loadProducts"
        layout="total, prev, pager, next"
        style="margin-top: 20px; flex-shrink: 0;"
      />
    </el-card>

    <!-- 历史数据对话框 -->
    <el-dialog v-model="showHistoryDialog" title="监控历史" width="80%">
      <el-table :data="historyData" v-loading="historyLoading" style="width: 100%">
        <el-table-column prop="monitored_at" label="监控时间" width="180">
          <template #default="{ row }">
            {{ formatDateTime(row.monitored_at) }}
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
        <el-table-column prop="rating" label="评分" width="100">
          <template #default="{ row }">
            {{ row.rating !== null && row.rating !== undefined ? row.rating.toFixed(2) : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="shop_rank" label="店铺排名" width="120" />
        <el-table-column prop="category_rank" label="类目排名" width="120" />
        <el-table-column prop="ad_rank" label="广告排名" width="120" />
      </el-table>
    </el-dialog>

    <!-- 定时任务配置对话框 -->
    <el-dialog v-model="showScheduleDialog" title="定时任务配置" width="500px">
      <el-form :model="scheduleConfig" label-width="150px">
        <el-form-item label="启用定时任务">
          <el-switch v-model="scheduleConfig.enabled" />
        </el-form-item>
        <el-form-item label="执行时间">
          <el-time-picker
            v-model="scheduleTime"
            format="HH:mm"
            value-format="HH:mm"
            placeholder="选择时间"
          />
        </el-form-item>
        <el-form-item label="时区">
          <el-input v-model="scheduleConfig.timezone" disabled />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showScheduleDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSaveSchedule" :loading="savingSchedule">
          保存
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { monitorPoolApi } from '@/api/monitorPool'
import { listingApi } from '@/api/listing'
import { ElMessage, ElMessageBox } from 'element-plus'

const loading = ref(false)
const triggering = ref(false)
const products = ref([])
const selectedProducts = ref([])
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)
const movingToProfit = ref(false)

const showHistoryDialog = ref(false)
const historyData = ref([])
const historyLoading = ref(false)
const currentProductId = ref(null)

const showScheduleDialog = ref(false)
const scheduleConfig = reactive({
  enabled: true,
  timezone: 'Asia/Shanghai'
})
const scheduleTime = ref('02:00')
const savingSchedule = ref(false)

const loadProducts = async () => {
  loading.value = true
  try {
    
    const response = await monitorPoolApi.getProducts({
      page: page.value,
      page_size: pageSize.value
    })
    
    
    products.value = response.data || response.items || []
    total.value = response.total || 0
    
  } catch (error) {
    ElMessage.error('加载产品列表失败')
  } finally {
    loading.value = false
  }
}

const viewHistory = (productId) => {
  currentProductId.value = productId
  showHistoryDialog.value = true
}

const loadHistory = async () => {
  if (!currentProductId.value) return
  
  historyLoading.value = true
  try {
    const response = await monitorPoolApi.getHistory(currentProductId.value, {
      limit: 100
    })
    historyData.value = (response.data || response || []).reverse()
  } catch (error) {
    ElMessage.error('加载历史数据失败')
  } finally {
    historyLoading.value = false
  }
}

// 格式化日期时间：yyyy-mm-dd hh:mm:ss
const formatDateTime = (value) => {
  if (!value) return '-'
  try {
    const d = new Date(value)
    if (Number.isNaN(d.getTime())) {
      return value
    }
    const year = d.getFullYear()
    const month = String(d.getMonth() + 1).padStart(2, '0')
    const day = String(d.getDate()).padStart(2, '0')
    const hours = String(d.getHours()).padStart(2, '0')
    const minutes = String(d.getMinutes()).padStart(2, '0')
    const seconds = String(d.getSeconds()).padStart(2, '0')
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
  } catch (e) {
    return value
  }
}

// 格式化日期：yyyy-mm-dd
const formatDate = (value) => {
  if (!value) return '-'
  try {
    const d = new Date(value)
    if (Number.isNaN(d.getTime())) {
      return value
    }
    const year = d.getFullYear()
    const month = String(d.getMonth() + 1).padStart(2, '0')
    const day = String(d.getDate()).padStart(2, '0')
    return `${year}-${month}-${day}`
  } catch (e) {
    return value
  }
}

const handleRemove = async (id) => {
  try {
    await ElMessageBox.confirm('确定要移除该产品吗？', '确认操作', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    })
    
    await monitorPoolApi.removeProduct(id)
    ElMessage.success('产品已移除')
    await loadProducts()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('移除失败')
    }
  }
}

const handleTriggerMonitor = async () => {
  triggering.value = true
  try {
    const productIds = products.value.map(p => p.id)
    await monitorPoolApi.triggerMonitor(productIds)
    ElMessage.success('监控任务已触发')
  } catch (error) {
    ElMessage.error('触发监控失败')
  } finally {
    triggering.value = false
  }
}

const loadScheduleConfig = async () => {
  try {
    const response = await monitorPoolApi.getScheduleConfig()
    Object.assign(scheduleConfig, response)
    if (response.schedule_time) {
      scheduleTime.value = response.schedule_time
    }
  } catch (error) {
    console.error('Load schedule config error:', error)
  }
}

const handleSaveSchedule = async () => {
  savingSchedule.value = true
  try {
    await monitorPoolApi.updateScheduleConfig({
      ...scheduleConfig,
      schedule_time: scheduleTime.value
    })
    ElMessage.success('定时任务配置已保存')
    showScheduleDialog.value = false
  } catch (error) {
    ElMessage.error('保存配置失败')
  } finally {
    savingSchedule.value = false
  }
}

const router = useRouter()

const handleSelectionChange = (selection) => {
  selectedProducts.value = selection.map(p => p.id)
}

const handleMoveToProfit = async () => {
  if (selectedProducts.value.length === 0) {
    ElMessage.warning('请选择要进入利润测算的产品')
    return
  }
  
  
  try {
    await ElMessageBox.confirm(
      `确定要将 ${selectedProducts.value.length} 个产品添加到利润测算吗？`,
      '确认操作',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )
    
    
    movingToProfit.value = true
    
    
    // 调用 listing API 将产品添加到 listing pool
    const response = await listingApi.addProducts(selectedProducts.value)
    
    
    // 即使 created_count 为 0（产品已存在），也继续流程
    if (response?.created_count === 0) {
      ElMessage.warning('所选产品已存在于利润测算中')
    } else {
      ElMessage.success(`成功添加 ${response?.created_count || 0} 个产品到利润测算`)
    }
    
    selectedProducts.value = []
    await loadProducts()
    
    // 跳转到利润测算页面
    router.push('/profit')
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('添加到利润测算失败')
    }
  } finally {
    movingToProfit.value = false
  }
}

watch(showHistoryDialog, (val) => {
  if (val) {
    loadHistory()
  }
})

onMounted(() => {
  loadProducts()
  loadScheduleConfig()
})
</script>

<style scoped>
.monitor-pool-container {
  padding: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

</style>


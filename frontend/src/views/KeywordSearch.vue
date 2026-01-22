<template>
  <div class="keyword-search-container">
    <el-card class="search-card">
      <template #header>
        <div class="card-header">
          <span>关键字搜索</span>
        </div>
      </template>
      
      <el-form :inline="true" class="search-form">
        <el-form-item label="关键字">
          <el-input
            v-model="keywordInput"
            placeholder="输入关键字"
            style="width: 300px"
            @keyup.enter="handleAddKeyword"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="handleAddKeyword" :loading="adding">
            添加关键字
          </el-button>
          <el-button @click="showBatchDialog = true">批量导入</el-button>
        </el-form-item>
      </el-form>

      <el-divider />

      <div class="keywords-list">
        <h3>关键字列表</h3>
        <el-table :data="keywords" style="width: 100%" v-loading="loading">
          <el-table-column prop="id" label="ID" width="80" />
          <el-table-column prop="keyword" label="关键字" />
          <el-table-column prop="status" label="状态" width="120">
            <template #default="{ row }">
              <el-tag :type="getStatusType(row.status)">
                {{ getStatusText(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="created_by_user_id" label="创建人ID" width="120" />
          <el-table-column prop="created_at" label="创建时间" width="180" />
        <el-table-column label="操作" width="250">
          <template #default="{ row }">
            <el-button size="small" @click="viewLinks(row.id)">查看链接</el-button>
            <el-button size="small" type="primary" @click="viewTasks(row.id)">查看任务</el-button>
            <el-button 
              v-if="row.status === 'failed'" 
              size="small" 
              type="warning" 
              @click="retryKeyword(row.id)"
            >
              重试
            </el-button>
          </template>
        </el-table-column>
        </el-table>
      </div>
    </el-card>

    <!-- 批量导入对话框 -->
    <el-dialog v-model="showBatchDialog" title="批量导入关键字" width="500px">
      <el-input
        v-model="batchKeywords"
        type="textarea"
        :rows="10"
        placeholder="每行一个关键字"
      />
      <template #footer>
        <el-button @click="showBatchDialog = false">取消</el-button>
        <el-button type="primary" @click="handleBatchAdd" :loading="batchAdding">
          确定
        </el-button>
      </template>
    </el-dialog>

    <!-- 链接库对话框 -->
    <el-dialog v-model="showLinksDialog" title="链接库" width="80%" :close-on-click-modal="false">
      <div class="links-header">
        <el-button type="primary" @click="exportLinks">导出链接</el-button>
      </div>
      <el-table :data="links" style="width: 100%" v-loading="linksLoading">
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column label="缩略图" width="120">
          <template #default="{ row }">
            <el-image
              v-if="row.thumbnail_image"
              :src="row.thumbnail_image"
              style="width: 80px; height: 80px"
              fit="cover"
              :preview-src-list="[row.thumbnail_image]"
            />
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="product_url" label="产品链接" show-overflow-tooltip />
        <el-table-column prop="price" label="售价" width="120">
          <template #default="{ row }">
            {{ row.price ? `€${row.price}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="crawled_at" label="爬取时间" width="180" />
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 'active' ? 'success' : 'info'">
              {{ row.status === 'active' ? '有效' : '无效' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
      <el-pagination
        v-model:current-page="linksPage"
        v-model:page-size="linksPageSize"
        :total="linksTotal"
        @current-change="loadLinks"
        layout="total, prev, pager, next"
        style="margin-top: 20px"
      />
    </el-dialog>

    <!-- 任务列表对话框 -->
    <el-dialog v-model="showTasksDialog" title="搜索任务" width="80%">
      <el-table :data="tasks" style="width: 100%" v-loading="tasksLoading">
        <el-table-column prop="id" label="任务ID" width="100" />
        <el-table-column prop="keyword_id" label="关键字ID" width="100" />
        <el-table-column prop="status" label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="getTaskStatusType(row.status)">
              {{ row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="priority" label="优先级" width="100" />
        <el-table-column prop="retry_count" label="重试次数" width="100" />
        <el-table-column prop="created_at" label="创建时间" width="180" />
        <el-table-column prop="completed_at" label="完成时间" width="180" />
        <el-table-column label="进度" width="150">
          <template #default="{ row }">
            <el-progress
              :percentage="getTaskProgress(row)"
              :status="row.status === 'failed' ? 'exception' : undefined"
            />
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { keywordsApi } from '@/api/keywords'
import { ElMessage } from 'element-plus'

const keywordInput = ref('')
const keywords = ref([])
const loading = ref(false)
const adding = ref(false)
const showBatchDialog = ref(false)
const batchKeywords = ref('')
const batchAdding = ref(false)

const showLinksDialog = ref(false)
const links = ref([])
const linksLoading = ref(false)
const linksPage = ref(1)
const linksPageSize = ref(20)
const linksTotal = ref(0)
const currentKeywordId = ref(null)

const showTasksDialog = ref(false)
const tasks = ref([])
const tasksLoading = ref(false)

const loadKeywords = async () => {
  loading.value = true
  try {
    const response = await keywordsApi.getKeywords()
    keywords.value = response.data || response
  } catch (error) {
    ElMessage.error('加载关键字列表失败')
  } finally {
    loading.value = false
  }
}

const handleAddKeyword = async () => {
  if (!keywordInput.value.trim()) {
    ElMessage.warning('请输入关键字')
    return
  }
  
  adding.value = true
  try {
    const response = await keywordsApi.addKeyword(keywordInput.value.trim())
    ElMessage.success('关键字添加成功，任务已提交')
    keywordInput.value = ''
    await loadKeywords()
    
    // 自动轮询任务状态
    if (response.task_id) {
      pollTaskStatus(response.task_id)
    }
  } catch (error) {
    ElMessage.error('添加关键字失败')
  } finally {
    adding.value = false
  }
}

const handleBatchAdd = async () => {
  const keywordList = batchKeywords.value
    .split('\n')
    .map(k => k.trim())
    .filter(k => k)
  
  if (keywordList.length === 0) {
    ElMessage.warning('请输入至少一个关键字')
    return
  }
  
  batchAdding.value = true
  try {
    await keywordsApi.batchAddKeywords(keywordList)
    ElMessage.success(`成功添加 ${keywordList.length} 个关键字`)
    showBatchDialog.value = false
    batchKeywords.value = ''
    await loadKeywords()
  } catch (error) {
    ElMessage.error('批量添加失败')
  } finally {
    batchAdding.value = false
  }
}

const viewLinks = async (keywordId) => {
  currentKeywordId.value = keywordId
  showLinksDialog.value = true
  await loadLinks()
}

const loadLinks = async () => {
  if (!currentKeywordId.value) return
  
  linksLoading.value = true
  try {
    const response = await keywordsApi.getKeywordLinks(currentKeywordId.value, {
      page: linksPage.value,
      page_size: linksPageSize.value
    })
    links.value = response.data || response.items || []
    linksTotal.value = response.total || 0
  } catch (error) {
    ElMessage.error('加载链接失败')
  } finally {
    linksLoading.value = false
  }
}

const exportLinks = async () => {
  if (!currentKeywordId.value) return
  
  try {
    const blob = await keywordsApi.exportLinks(currentKeywordId.value)
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `keyword_${currentKeywordId.value}_links.csv`
    a.click()
    window.URL.revokeObjectURL(url)
    ElMessage.success('导出成功')
  } catch (error) {
    ElMessage.error('导出失败')
  }
}

const viewTasks = async (keywordId) => {
  showTasksDialog.value = true
  tasksLoading.value = true
  try {
    const response = await keywordsApi.getTasks({ keyword_id: keywordId })
    tasks.value = response.data || response
  } catch (error) {
    ElMessage.error('加载任务列表失败')
  } finally {
    tasksLoading.value = false
  }
}

const pollTaskStatus = async (taskId) => {
  const maxAttempts = 60
  let attempts = 0
  
  const poll = async () => {
    if (attempts >= maxAttempts) {
      ElMessage.warning('任务状态查询超时，请手动刷新')
      return
    }
    
    try {
      const response = await keywordsApi.getTaskStatus(taskId)
      if (response.status === 'completed' || response.status === 'failed') {
        await loadKeywords()
        if (response.status === 'completed') {
          ElMessage.success('关键字搜索任务已完成')
        } else {
          ElMessage.warning('关键字搜索任务失败')
        }
        return
      }
      
      attempts++
      setTimeout(poll, 2000)
    } catch (error) {
      console.error('Poll task status error:', error)
      attempts++
      if (attempts < maxAttempts) {
        setTimeout(poll, 2000)
      }
    }
  }
  
  poll()
}

const getStatusType = (status) => {
  const map = {
    'pending': 'info',
    'processing': 'warning',
    'completed': 'success',
    'failed': 'danger'
  }
  return map[status] || 'info'
}

const getStatusText = (status) => {
  const map = {
    'pending': '待搜索',
    'processing': '搜索中',
    'completed': '已完成',
    'failed': '失败'
  }
  return map[status] || status
}

const getTaskStatusType = (status) => {
  return getStatusType(status)
}

const getTaskProgress = (task) => {
  if (task.status === 'completed') return 100
  if (task.status === 'failed') return 0
  if (task.status === 'processing') return 50
  return 0
}

const retryKeyword = async (keywordId) => {
  try {
    // 查找该关键字相关的失败任务
    const response = await keywordsApi.getTasks({ keyword_id: keywordId, status: 'failed' })
    const failedTasks = response.data || response
    
    if (failedTasks.length === 0) {
      ElMessage.warning('没有找到失败的任务')
      return
    }
    
    // 重试所有失败的任务
    for (const task of failedTasks) {
      await keywordsApi.retryTask(task.id)
    }
    
    ElMessage.success(`已重试 ${failedTasks.length} 个失败任务`)
    await loadKeywords()
  } catch (error) {
    ElMessage.error('重试失败')
  }
}

onMounted(async () => {
  // Wait a bit to ensure token is fully saved
  await new Promise(resolve => setTimeout(resolve, 100))
  await loadKeywords()
})
</script>

<style scoped>
.keyword-search-container {
  padding: 20px;
}

.search-card {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.search-form {
  margin-top: 20px;
}

.keywords-list {
  margin-top: 20px;
}

.links-header {
  margin-bottom: 20px;
}
</style>


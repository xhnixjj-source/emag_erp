<template>
  <div class="failed-tasks-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>失败任务列表</span>
          <el-button
            type="primary"
            :disabled="selectedIds.length === 0"
            :loading="retrying"
            @click="handleBatchRetry"
          >
            批量重试 ({{ selectedIds.length }})
          </el-button>
        </div>
      </template>

      <!-- 筛选器 -->
      <el-form :inline="true" class="filter-form">
        <el-form-item label="任务类型">
          <el-select
            v-model="filters.task_type"
            placeholder="全部"
            clearable
            style="width: 180px"
          >
            <el-option label="关键字搜索" value="keyword_search" />
            <el-option label="产品爬取" value="product_crawl" />
            <el-option label="监控爬取" value="monitor_crawl" />
          </el-select>
        </el-form-item>
        <el-form-item label="错误关键字">
          <el-input
            v-model="filters.error_keyword"
            placeholder="搜索错误信息"
            clearable
            style="width: 220px"
            @keyup.enter="loadTasks"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="loadTasks" :loading="loading">查询</el-button>
          <el-button @click="resetFilters">重置</el-button>
        </el-form-item>
      </el-form>

      <!-- 任务列表 -->
      <el-table
        ref="tableRef"
        :data="tasks"
        v-loading="loading"
        style="width: 100%"
        stripe
        height="calc(100vh - 420px)"
        @selection-change="handleSelectionChange"
      >
        <el-table-column type="selection" width="50" />
        <el-table-column prop="id" label="任务ID" width="80" />
        <el-table-column label="任务类型" width="120">
          <template #default="{ row }">
            <el-tag :type="getTaskTypeTagType(row.task_type)" size="small">
              {{ getTaskTypeLabel(row.task_type) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="目标" min-width="280" show-overflow-tooltip>
          <template #default="{ row }">
            <div v-if="row.product_url">
              <el-link
                type="primary"
                :href="row.product_url"
                target="_blank"
                :underline="false"
                style="font-size: 13px;"
              >
                {{ row.product_url }}
              </el-link>
            </div>
            <div v-else-if="row.keyword_name">
              <el-tag type="info" size="small">关键字</el-tag>
              <span style="margin-left: 6px;">{{ row.keyword_name }}</span>
            </div>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="重试次数" width="100" align="center">
          <template #default="{ row }">
            <span>{{ row.retry_count }} / {{ row.max_retries }}</span>
          </template>
        </el-table-column>
        <el-table-column label="错误信息" min-width="250" show-overflow-tooltip>
          <template #default="{ row }">
            <el-popover
              v-if="row.error_message"
              placement="top-start"
              :width="500"
              trigger="hover"
            >
              <template #reference>
                <span class="error-message">{{ row.error_message }}</span>
              </template>
              <pre class="error-detail">{{ row.error_message }}</pre>
            </el-popover>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="优先级" width="90" align="center">
          <template #default="{ row }">
            <el-tag
              :type="row.priority === 'high' ? 'danger' : row.priority === 'low' ? 'info' : ''"
              size="small"
            >
              {{ getPriorityLabel(row.priority) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="创建时间" width="170">
          <template #default="{ row }">
            {{ formatDateTime(row.created_at) }}
          </template>
        </el-table-column>
        <el-table-column label="更新时间" width="170">
          <template #default="{ row }">
            {{ formatDateTime(row.updated_at) }}
          </template>
        </el-table-column>
      </el-table>

      <!-- 分页 -->
      <div class="pagination-bar">
        <el-checkbox
          v-model="selectAll"
          :indeterminate="isIndeterminate"
          @change="handleSelectAll"
          style="margin-right: 16px;"
        >
          全选本页
        </el-checkbox>
        <el-pagination
          v-model:current-page="page"
          v-model:page-size="pageSize"
          :total="total"
          :page-sizes="[20, 50, 100, 200]"
          @current-change="handlePageChange"
          @size-change="handleSizeChange"
          layout="total, sizes, prev, pager, next, jumper"
        />
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, nextTick } from 'vue'
import { failedTasksApi } from '@/api/failedTasks'
import { ElMessage, ElMessageBox } from 'element-plus'

const loading = ref(false)
const retrying = ref(false)
const tasks = ref([])
const page = ref(1)
const pageSize = ref(50)
const total = ref(0)
const selectedIds = ref([])
const tableRef = ref(null)

const filters = reactive({
  task_type: null,
  error_keyword: null
})

// 任务类型映射
const taskTypeMap = {
  keyword_search: '关键字搜索',
  product_crawl: '产品爬取',
  monitor_crawl: '监控爬取'
}

const getTaskTypeLabel = (type) => taskTypeMap[type] || type

const getTaskTypeTagType = (type) => {
  const map = {
    keyword_search: 'primary',
    product_crawl: 'warning',
    monitor_crawl: 'success'
  }
  return map[type] || ''
}

const getPriorityLabel = (p) => {
  const map = { high: '高', normal: '普通', low: '低' }
  return map[p] || p
}

// 格式化日期
const formatDateTime = (dt) => {
  if (!dt) return '-'
  try {
    const date = new Date(dt)
    return date.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    })
  } catch {
    return dt
  }
}

// 全选逻辑
const selectAll = computed({
  get: () => tasks.value.length > 0 && selectedIds.value.length === tasks.value.length,
  set: () => {}
})
const isIndeterminate = computed(
  () => selectedIds.value.length > 0 && selectedIds.value.length < tasks.value.length
)

const handleSelectionChange = (selection) => {
  selectedIds.value = selection.map((row) => row.id)
}

const handleSelectAll = (val) => {
  if (tableRef.value) {
    if (val) {
      tableRef.value.toggleAllSelection()
    } else {
      tableRef.value.clearSelection()
    }
  }
}

// 加载失败任务
const loadTasks = async () => {
  loading.value = true
  try {
    const skip = (page.value - 1) * pageSize.value
    const params = {
      skip,
      limit: pageSize.value
    }
    if (filters.task_type) params.task_type = filters.task_type
    if (filters.error_keyword) params.error_keyword = filters.error_keyword

    const response = await failedTasksApi.getFailedTasks(params)
    tasks.value = response.items || []
    total.value = response.total || 0
  } catch (error) {
    console.error('加载失败任务列表失败:', error)
    ElMessage.error('加载失败任务列表失败')
    tasks.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

// 批量重试
const handleBatchRetry = async () => {
  if (selectedIds.value.length === 0) return

  try {
    await ElMessageBox.confirm(
      `确定要重试选中的 ${selectedIds.value.length} 个失败任务吗？`,
      '批量重试确认',
      { confirmButtonText: '确定', cancelButtonText: '取消', type: 'warning' }
    )
  } catch {
    return // 用户取消
  }

  retrying.value = true
  try {
    const response = await failedTasksApi.batchRetry(selectedIds.value)
    ElMessage.success(response.message || `成功重试 ${response.success_count} 个任务`)
    // 清除选中并刷新列表
    selectedIds.value = []
    if (tableRef.value) tableRef.value.clearSelection()
    await loadTasks()
  } catch (error) {
    console.error('批量重试失败:', error)
    ElMessage.error('批量重试失败')
  } finally {
    retrying.value = false
  }
}

// 重置筛选
const resetFilters = () => {
  filters.task_type = null
  filters.error_keyword = null
  page.value = 1
  loadTasks()
}

// 分页
const handlePageChange = (newPage) => {
  page.value = newPage
  loadTasks()
}

const handleSizeChange = (newSize) => {
  pageSize.value = newSize
  page.value = 1
  loadTasks()
}

onMounted(() => {
  loadTasks()
})
</script>

<style scoped>
.failed-tasks-container {
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-form {
  margin-bottom: 16px;
}

.error-message {
  color: #f56c6c;
  font-size: 13px;
  cursor: pointer;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  text-overflow: ellipsis;
}

.error-detail {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  font-size: 12px;
  line-height: 1.5;
  max-height: 300px;
  overflow-y: auto;
}

.pagination-bar {
  margin-top: 16px;
  display: flex;
  align-items: center;
  justify-content: flex-end;
}
</style>


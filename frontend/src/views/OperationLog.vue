<template>
  <div class="operation-log-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>操作日志</span>
          <el-button type="primary" @click="handleExport" :loading="exporting">
            导出日志
          </el-button>
        </div>
      </template>

      <!-- 筛选器 -->
      <el-form :inline="true" class="filter-form">
        <el-form-item label="操作类型">
          <el-select 
            v-model="filters.operation_type" 
            placeholder="全部" 
            clearable 
            style="width: 180px"
          >
            <el-option 
              v-for="(label, value) in operationTypeMap" 
              :key="value"
              :label="label" 
              :value="value" 
            />
          </el-select>
        </el-form-item>
        <el-form-item label="目标类型">
          <el-select 
            v-model="filters.target_type" 
            placeholder="全部" 
            clearable 
            style="width: 150px"
          >
            <el-option 
              v-for="(label, value) in targetTypeMap" 
              :key="value"
              :label="label" 
              :value="value" 
            />
          </el-select>
        </el-form-item>
        <el-form-item label="用户">
          <el-select 
            v-model="filters.user_id" 
            placeholder="全部" 
            clearable 
            filterable
            style="width: 150px"
          >
            <el-option 
              v-for="user in users" 
              :key="user.id"
              :label="user.username" 
              :value="user.id" 
            />
          </el-select>
        </el-form-item>
        <el-form-item label="开始时间">
          <el-date-picker
            v-model="filters.start_date"
            type="datetime"
            placeholder="选择开始时间"
            format="YYYY-MM-DD HH:mm:ss"
            value-format="YYYY-MM-DDTHH:mm:ss"
            style="width: 200px"
          />
        </el-form-item>
        <el-form-item label="结束时间">
          <el-date-picker
            v-model="filters.end_date"
            type="datetime"
            placeholder="选择结束时间"
            format="YYYY-MM-DD HH:mm:ss"
            value-format="YYYY-MM-DDTHH:mm:ss"
            style="width: 200px"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="loadLogs" :loading="loading">查询</el-button>
          <el-button @click="resetFilters">重置</el-button>
        </el-form-item>
      </el-form>

      <!-- 日志列表 -->
      <el-table :data="logs" v-loading="loading" style="width: 100%" stripe>
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column label="用户" width="150">
          <template #default="{ row }">
            <div>
              <div>{{ row.username || `用户${row.user_id}` }}</div>
              <div style="font-size: 12px; color: #909399;">ID: {{ row.user_id }}</div>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="操作类型" width="150">
          <template #default="{ row }">
            <el-tag :type="getOperationTypeTagType(row.operation_type)">
              {{ getOperationTypeLabel(row.operation_type) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="目标类型" width="120">
          <template #default="{ row }">
            <el-tag v-if="row.target_type" type="info" size="small">
              {{ getTargetTypeLabel(row.target_type) }}
            </el-tag>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="target_id" label="目标ID" width="100">
          <template #default="{ row }">
            <span v-if="row.target_id">{{ row.target_id }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="操作详情" show-overflow-tooltip min-width="250">
          <template #default="{ row }">
            <div v-if="row.operation_detail" class="operation-detail">
              <el-popover
                placement="top-start"
                :width="400"
                trigger="hover"
              >
                <template #reference>
                  <el-button text type="primary" size="small">查看详情</el-button>
                </template>
                <pre class="detail-content">{{ formatOperationDetail(row.operation_detail) }}</pre>
              </el-popover>
            </div>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="ip_address" label="IP地址" width="150">
          <template #default="{ row }">
            <span v-if="row.ip_address">{{ row.ip_address }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="操作时间" width="180">
          <template #default="{ row }">
            {{ formatDateTime(row.created_at) }}
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="total"
        :page-sizes="[10, 20, 50, 100]"
        @current-change="handlePageChange"
        @size-change="handleSizeChange"
        layout="total, sizes, prev, pager, next, jumper"
        style="margin-top: 20px; justify-content: flex-end;"
      />
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed } from 'vue'
import { operationLogApi } from '@/api/operationLog'
import { authApi } from '@/api/auth'
import { ElMessage } from 'element-plus'

const loading = ref(false)
const exporting = ref(false)
const logs = ref([])
const users = ref([])
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const filters = reactive({
  operation_type: null,
  target_type: null,
  user_id: null,
  start_date: null,
  end_date: null
})

// 操作类型映射
const operationTypeMap = {
  login: '登录',
  logout: '登出',
  keyword_add: '添加关键字',
  product_crawl: '爬取产品',
  filter_select: '筛选产品',
  monitor_add: '添加到监控池',
  listing_add: '添加到上架产品库',
  profit_calc: '利润测算',
  listing_edit: '编辑上架详情',
  status_change: '状态变更',
  product_lock: '产品锁定',
  product_unlock: '产品解锁',
  user_create: '创建用户',
  user_update: '更新用户',
  user_delete: '删除用户',
  monitor_scheduled: '定时监控',
  monitor_trigger: '触发监控',
  monitor_trigger_batch: '批量触发监控',
  task_retry: '任务重试'
}

// 目标类型映射
const targetTypeMap = {
  keyword: '关键字',
  product: '产品',
  filter_pool: '筛选池',
  monitor_pool: '监控池',
  listing_pool: '上架产品库',
  user: '用户',
  crawl_task: '爬取任务'
}

// 获取操作类型标签
const getOperationTypeLabel = (type) => {
  return operationTypeMap[type] || type
}

// 获取目标类型标签
const getTargetTypeLabel = (type) => {
  return targetTypeMap[type] || type
}

// 获取操作类型标签颜色
const getOperationTypeTagType = (type) => {
  const typeMap = {
    login: 'success',
    logout: 'info',
    keyword_add: 'primary',
    product_crawl: 'warning',
    filter_select: '',
    monitor_add: 'success',
    listing_add: 'success',
    profit_calc: 'warning',
    listing_edit: 'primary',
    status_change: 'info',
    product_lock: 'danger',
    product_unlock: 'success',
    user_create: 'success',
    user_update: 'primary',
    user_delete: 'danger'
  }
  return typeMap[type] || ''
}

// 格式化操作详情
const formatOperationDetail = (detail) => {
  if (!detail) return ''
  try {
    return JSON.stringify(detail, null, 2)
  } catch (e) {
    return String(detail)
  }
}

// 格式化日期时间
const formatDateTime = (dateTime) => {
  if (!dateTime) return '-'
  try {
    const date = new Date(dateTime)
    return date.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    })
  } catch (e) {
    return dateTime
  }
}

// 加载用户列表
const loadUsers = async () => {
  try {
    const response = await authApi.getUsers()
    users.value = Array.isArray(response) ? response : []
  } catch (error) {
    console.error('加载用户列表失败:', error)
    users.value = []
  }
}

// 加载日志
const loadLogs = async () => {
  loading.value = true
  try {
    const skip = (page.value - 1) * pageSize.value
    const params = {
      skip,
      limit: pageSize.value,
      ...filters
    }
    
    // 清理空值
    Object.keys(params).forEach(key => {
      if (params[key] === null || params[key] === '' || params[key] === undefined) {
        delete params[key]
      }
    })
    
    const response = await operationLogApi.getLogs(params)
    logs.value = response.logs || []
    total.value = response.total || 0
  } catch (error) {
    console.error('加载操作日志失败:', error)
    ElMessage.error('加载操作日志失败')
    logs.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

// 重置筛选
const resetFilters = () => {
  Object.keys(filters).forEach(key => {
    filters[key] = null
  })
  page.value = 1
  loadLogs()
}

// 分页变化
const handlePageChange = (newPage) => {
  page.value = newPage
  loadLogs()
}

// 每页数量变化
const handleSizeChange = (newSize) => {
  pageSize.value = newSize
  page.value = 1
  loadLogs()
}

// 导出日志
const handleExport = async () => {
  exporting.value = true
  try {
    const params = { ...filters }
    
    // 清理空值
    Object.keys(params).forEach(key => {
      if (params[key] === null || params[key] === '' || params[key] === undefined) {
        delete params[key]
      }
    })
    
    const blob = await operationLogApi.exportLogs(params)
    
    // 检查是否是错误响应（JSON格式）
    if (blob instanceof Blob && blob.type === 'application/json') {
      const text = await blob.text()
      try {
        const errorData = JSON.parse(text)
        ElMessage.error('导出失败: ' + (errorData.detail || '未知错误'))
        return
      } catch (e) {
        // 不是JSON，继续处理
      }
    }
    
    // 创建下载链接
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `operation_logs_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.csv`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    window.URL.revokeObjectURL(url)
    
    ElMessage.success('导出成功')
  } catch (error) {
    console.error('导出失败:', error)
    // 尝试解析错误响应
    if (error.response && error.response.data) {
      const errorMsg = error.response.data.detail || error.response.data.message || '未知错误'
      ElMessage.error('导出失败: ' + errorMsg)
    } else {
      ElMessage.error('导出失败: ' + (error.message || '未知错误'))
    }
  } finally {
    exporting.value = false
  }
}

onMounted(() => {
  loadUsers()
  loadLogs()
})
</script>

<style scoped>
.operation-log-container {
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

.operation-detail {
  max-width: 200px;
}

.detail-content {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  font-size: 12px;
  line-height: 1.5;
}
</style>

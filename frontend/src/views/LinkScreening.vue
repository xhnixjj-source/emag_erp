<template>
  <div class="link-screening-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>链接初筛</span>
          <div>
            <el-button 
              type="primary" 
              :disabled="selectedLinks.length === 0"
              @click="handleBatchCrawl"
              :loading="batchCrawling"
            >
              批量爬取 (已选择 {{ selectedLinks.length }} 个)
            </el-button>
          </div>
        </div>
      </template>

      <!-- 筛选器 -->
      <el-form :inline="true" class="filter-form">
        <el-form-item label="关键字">
          <el-select 
            v-model="selectedKeywordId" 
            placeholder="选择关键字" 
            clearable
            @change="loadLinks"
            style="width: 200px"
          >
            <el-option
              v-for="kw in keywords"
              :key="kw.id"
              :label="kw.keyword"
              :value="kw.id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="价格区间">
          <el-input-number v-model="filters.price_min" :min="0" :precision="2" placeholder="最低价" style="width: 120px" />
          <span style="margin: 0 5px">-</span>
          <el-input-number v-model="filters.price_max" :min="0" :precision="2" placeholder="最高价" style="width: 120px" />
        </el-form-item>
        <el-form-item>
          <el-button @click="loadLinks">搜索</el-button>
          <el-button @click="resetFilters">重置</el-button>
        </el-form-item>
      </el-form>

      <el-divider />

      <!-- 链接列表 -->
      <div class="table-header">
        <div>
          <el-checkbox v-model="selectAll" @change="handleSelectAll">全选</el-checkbox>
          <span style="margin-left: 20px">
            已选择 {{ selectedLinks.length }} 个链接
          </span>
        </div>
      </div>

      <el-table
        :data="links"
        v-loading="loading"
        @selection-change="handleSelectionChange"
        style="width: 100%"
        height="calc(100vh - 450px)"
      >
        <el-table-column type="selection" width="55" />
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
        <el-table-column prop="product_url" label="产品链接" show-overflow-tooltip min-width="300" />
        <el-table-column prop="price" label="售价" width="120">
          <template #default="{ row }">
            {{ row.price ? `€${row.price}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 'active' ? 'success' : 'info'">
              {{ row.status === 'active' ? '有效' : '无效' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="crawled_at" label="爬取时间" width="180" />
      </el-table>

      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="total"
        @current-change="loadLinks"
        @size-change="loadLinks"
        layout="total, sizes, prev, pager, next, jumper"
        style="margin-top: 20px; flex-shrink: 0;"
      />
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { keywordsApi } from '@/api/keywords'
import { ElMessage, ElMessageBox } from 'element-plus'

const loading = ref(false)
const batchCrawling = ref(false)
const links = ref([])
const keywords = ref([])
const selectedLinks = ref([])
const selectAll = ref(false)
const selectedKeywordId = ref(null)
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const filters = reactive({
  price_min: null,
  price_max: null
})

const loadKeywords = async () => {
  try {
    const response = await keywordsApi.getKeywords()
    keywords.value = response.data || response
  } catch (error) {
    ElMessage.error('加载关键字列表失败')
  }
}

const loadLinks = async () => {
  loading.value = true
  try {
    const params = {
      skip: (page.value - 1) * pageSize.value,
      limit: pageSize.value,
      price_min: filters.price_min,
      price_max: filters.price_max
    }
    
    // 如果选择了关键字，添加 keyword_id 参数
    if (selectedKeywordId.value) {
      params.keyword_id = selectedKeywordId.value
    }
    
    // 调用 API（不传 keywordId，而是通过 params 传递）
    const response = await keywordsApi.getKeywordLinks(null, params)
    
    // 后端返回格式：{ items: [], total: 100, skip: 0, limit: 20 }
    if (response.data) {
      links.value = response.data.items || response.data || []
      total.value = response.data.total || links.value.length
    } else if (response.items) {
      // 直接返回对象格式
      links.value = response.items || []
      total.value = response.total || links.value.length
    } else {
      // 兼容旧格式（数组）
      links.value = Array.isArray(response) ? response : []
      total.value = links.value.length
    }
  } catch (error) {
    console.error('加载链接失败:', error)
    ElMessage.error('加载链接失败: ' + (error.response?.data?.detail || error.message || '未知错误'))
    links.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const resetFilters = () => {
  filters.price_min = null
  filters.price_max = null
  loadLinks()
}

const handleSelectAll = (checked) => {
  if (checked) {
    selectedLinks.value = [...links.value]
  } else {
    selectedLinks.value = []
  }
}

const handleSelectionChange = (selection) => {
  selectedLinks.value = selection
  selectAll.value = selection.length === links.value.length && links.value.length > 0
}

const handleBatchCrawl = async () => {
  if (selectedLinks.value.length === 0) {
    ElMessage.warning('请先选择要爬取的链接')
    return
  }
  
  try {
    await ElMessageBox.confirm(
      `确定要批量爬取 ${selectedLinks.value.length} 个链接吗？`,
      '确认批量爬取',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )
    
    batchCrawling.value = true
    const linkIds = selectedLinks.value.map(link => link.id)
    
    // 调用批量爬取API
    const response = await keywordsApi.batchCrawlLinks(linkIds)
    
    ElMessage.success(`成功创建 ${response.data?.created_count || response.created_count || 0} 个爬取任务`)
    
    // 清空选择
    selectedLinks.value = []
    selectAll.value = false
    
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('批量爬取失败: ' + (error.response?.data?.detail || error.message))
    }
  } finally {
    batchCrawling.value = false
  }
}

onMounted(() => {
  loadKeywords()
  // 页面加载时自动加载所有链接
  loadLinks()
})
</script>

<style scoped>
.link-screening-container {
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

.table-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}
</style>


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
            filterable
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
        <el-form-item label="来源">
          <el-select
            v-model="filters.source"
            placeholder="全部来源"
            clearable
            @change="loadLinks"
            style="width: 150px"
          >
            <el-option label="关键字搜索" value="keyword_search" />
            <el-option label="Chrome 插件" value="chrome_extension" />
          </el-select>
        </el-form-item>
        <el-form-item label="标签">
          <el-select
            v-model="filters.tag"
            placeholder="全部标签"
            clearable
            @change="loadLinks"
            style="width: 150px"
          >
            <el-option label="Super Hot" value="Super Hot" />
            <el-option label="Hot" value="Hot" />
          </el-select>
        </el-form-item>
        <el-form-item label="价格区间">
          <el-input-number v-model="filters.price_min" :min="0" :precision="2" placeholder="最低价" style="width: 120px" />
          <span style="margin: 0 5px">-</span>
          <el-input-number v-model="filters.price_max" :min="0" :precision="2" placeholder="最高价" style="width: 120px" />
        </el-form-item>
        <el-form-item label="评论数">
          <el-input-number v-model="filters.review_count_min" :min="0" placeholder="最少评论" style="width: 120px" />
          <span style="margin: 0 5px">-</span>
          <el-input-number v-model="filters.review_count_max" :min="0" placeholder="最多评论" style="width: 120px" />
        </el-form-item>
        <el-form-item label="评分">
          <el-input-number v-model="filters.rating_min" :min="0" :max="5" :precision="2" placeholder="最低评分" style="width: 120px" />
          <span style="margin: 0 5px">-</span>
          <el-input-number v-model="filters.rating_max" :min="0" :max="5" :precision="2" placeholder="最高评分" style="width: 120px" />
        </el-form-item>
        <el-form-item label="爬取时间">
          <el-date-picker
            v-model="filters.crawled_at_range"
            type="datetimerange"
            range-separator="至"
            start-placeholder="开始时间"
            end-placeholder="结束时间"
            format="YYYY-MM-DD HH:mm:ss"
            value-format="YYYY-MM-DDTHH:mm:ss"
            style="width: 350px"
          />
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
        <el-table-column label="缩略图" width="100">
          <template #default="{ row }">
            <el-image
              v-if="row.thumbnail_image"
              :src="row.thumbnail_image"
              style="width: 60px; height: 60px"
              fit="cover"
              :preview-src-list="[row.thumbnail_image]"
            />
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="product_title" label="产品标题" show-overflow-tooltip min-width="200">
          <template #default="{ row }">
            {{ row.product_title || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="product_url" label="产品链接" show-overflow-tooltip min-width="200">
          <template #default="{ row }">
            <a :href="row.product_url" target="_blank" style="color: #409eff;">{{ row.product_url }}</a>
          </template>
        </el-table-column>
        <el-table-column label="关键字" width="150">
          <template #default="{ row }">
            {{ getKeywordName(row.keyword_id) }}
          </template>
        </el-table-column>
        <el-table-column prop="pnk_code" label="PNK" width="130" show-overflow-tooltip />
        <el-table-column prop="brand" label="品牌" width="120" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.brand || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="category" label="类目" width="150" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.category || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="price" label="售价" width="100">
          <template #default="{ row }">
            {{ row.price ? `€${row.price}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="purchase_price" label="采购价" width="100">
          <template #default="{ row }">
            {{ row.purchase_price ? `€${row.purchase_price}` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="commission_rate" label="佣金(%)" width="90">
          <template #default="{ row }">
            {{ row.commission_rate !== null && row.commission_rate !== undefined ? `${row.commission_rate}%` : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="offer_count" label="跟卖数" width="80">
          <template #default="{ row }">
            {{ row.offer_count !== null && row.offer_count !== undefined ? row.offer_count : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="review_count" label="评论数" width="80">
          <template #default="{ row }">
            {{ row.review_count !== null && row.review_count !== undefined ? row.review_count : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="rating" label="评分" width="80">
          <template #default="{ row }">
            {{ row.rating !== null && row.rating !== undefined ? row.rating.toFixed(2) : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="tag" label="标签" width="110">
          <template #default="{ row }">
            <el-tag v-if="row.tag" :type="row.tag === 'Super Hot' ? 'danger' : 'warning'" size="small">
              {{ row.tag }}
            </el-tag>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="source" label="来源" width="110">
          <template #default="{ row }">
            <el-tag :type="row.source === 'chrome_extension' ? 'warning' : 'primary'" size="small">
              {{ row.source === 'chrome_extension' ? 'Chrome插件' : '关键字搜索' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="last_offer_period" label="最近Offer" width="110" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.last_offer_period || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="80">
          <template #default="{ row }">
            <el-tag :type="row.status === 'active' ? 'success' : 'info'" size="small">
              {{ row.status === 'active' ? '有效' : '无效' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="crawled_at" label="抓取时间" width="170">
          <template #default="{ row }">
            {{ formatDateTime(row.crawled_at) }}
          </template>
        </el-table-column>
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
  price_max: null,
  review_count_min: null,
  review_count_max: null,
  rating_min: null,
  rating_max: null,
  crawled_at_range: null,
  source: null,
  tag: null
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
      price_max: filters.price_max,
      review_count_min: filters.review_count_min,
      review_count_max: filters.review_count_max,
      rating_min: filters.rating_min,
      rating_max: filters.rating_max
    }
    
    // 如果选择了关键字，添加 keyword_id 参数
    if (selectedKeywordId.value) {
      params.keyword_id = selectedKeywordId.value
    }
    
    // 处理来源和标签筛选
    if (filters.source) {
      params.source = filters.source
    }
    if (filters.tag) {
      params.tag = filters.tag
    }
    
    // 处理爬取时间范围
    if (filters.crawled_at_range && filters.crawled_at_range.length === 2) {
      params.crawled_at_start = filters.crawled_at_range[0]
      params.crawled_at_end = filters.crawled_at_range[1]
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
  filters.review_count_min = null
  filters.review_count_max = null
  filters.rating_min = null
  filters.rating_max = null
  filters.crawled_at_range = null
  filters.source = null
  filters.tag = null
  loadLinks()
}

const getKeywordName = (keywordId) => {
  const keyword = keywords.value.find(kw => kw.id === keywordId)
  return keyword ? keyword.keyword : '-'
}

const formatDateTime = (dateTime) => {
  if (!dateTime) return '-'
  const date = new Date(dateTime)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
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


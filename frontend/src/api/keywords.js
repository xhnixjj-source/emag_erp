import api from './index'

export const keywordsApi = {
  // Add keyword
  addKeyword: (keyword) => api.post('/keywords', { keyword }),
  
  // Batch add keywords
  batchAddKeywords: (keywords) => api.post('/keywords/batch', { keywords }),
  
  // Get keywords list
  getKeywords: (params) => api.get('/keywords', { params }),
  
  // Get keyword links (using /links endpoint with keyword_id param)
  getKeywordLinks: (keywordId, params = {}) => {
    const queryParams = { ...params }
    // 只有当 keywordId 不为 null/undefined 时才添加
    if (keywordId !== null && keywordId !== undefined) {
      queryParams.keyword_id = keywordId
    }
    return api.get('/keywords/links', { params: queryParams })
  },
  
  // Chrome 插件提交链接
  importChromeExtensionLinks: (items) => api.post('/keywords/links/chrome-extension', { items }),
  
  // Get tasks (supports filtering by keyword_id, status, etc.)
  getTasks: (params = {}) => api.get('/keywords/tasks', { params }),
  
  // Get task status
  getTaskStatus: (taskId) => api.get(`/keywords/tasks/${taskId}`),
  
  // Retry failed task
  retryTask: (taskId) => api.post(`/keywords/tasks/${taskId}/retry`),
  
  // Export links (client-side export if backend doesn't support)
  exportLinks: async (links) => {
    // If backend supports export endpoint, use it
    // Otherwise, do client-side export
    const csvContent = links.map(link => 
      `"${link.product_url}","${link.crawled_at || ''}","${link.status || 'active'}"`
    ).join('\n')
    const header = '"产品链接","爬取时间","状态"\n'
    return new Blob(['\ufeff' + header + csvContent], { type: 'text/csv;charset=utf-8;' })
  },
  
  // Get error logs
  getErrorLogs: (params = {}) => api.get('/keywords/error-logs', { params }),
  
  // Batch crawl links
  batchCrawlLinks: (linkIds) => api.post('/keywords/links/batch-crawl', { link_ids: linkIds }),
  
  // Batch get listed at (上架日期)
  batchGetListedAt: (linkIds) => api.post('/keywords/links/batch-get-listed-at', { link_ids: linkIds })
}


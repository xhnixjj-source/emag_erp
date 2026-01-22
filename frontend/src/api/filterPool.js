import api from './index'

export const filterPoolApi = {
  getProducts: (params) => api.get('/filter-pool', { params }),
  applyFilter: (filters) => api.post('/filter-pool/filter', filters),
  moveToMonitor: (productIds) => api.post('/filter-pool/move-to-monitor', { filter_pool_ids: productIds }),
  exportProducts: (params) => api.get('/filter-pool/export', { params, responseType: 'blob' })
}


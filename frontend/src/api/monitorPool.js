import api from './index'

export const monitorPoolApi = {
  getProducts: (params) => api.get('/monitor-pool', { params }),
  addProduct: (productUrl) => api.post('/monitor-pool', { product_url: productUrl }),
  removeProduct: (id) => api.delete(`/monitor-pool/${id}`),
  getHistory: (productId, params) => api.get(`/monitor-pool/${productId}/history`, { params }),
  getScheduleConfig: () => api.get('/monitor-pool/schedule'),
  updateScheduleConfig: (config) => api.put('/monitor-pool/schedule', config),
  triggerMonitor: (productIds) => api.post('/monitor-pool/trigger', { product_ids: productIds })
}


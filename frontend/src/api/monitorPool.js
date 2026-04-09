import api from './index'

export const monitorPoolApi = {
  getProducts: (params) => api.get('/monitor-pool', { params }),
  importFromTxt: (file, isOwnShop) => {
    const fd = new FormData()
    fd.append('file', file)
    fd.append('is_own_shop', isOwnShop ? 'true' : 'false')
    return api.post('/monitor-pool/import-from-txt', fd, {
      transformRequest: [(data, headers) => {
        delete headers['Content-Type']
        return data
      }]
    })
  },
  addProduct: (productUrl) => api.post('/monitor-pool', { product_url: productUrl }),
  removeProduct: (id) => api.delete(`/monitor-pool/${id}`),
  batchInactivate: (monitorIds) =>
    api.post('/monitor-pool/batch-inactivate', { monitor_ids: monitorIds }),
  getHistory: (productId, params) => api.get(`/monitor-pool/${productId}/history`, { params }),
  getScheduleConfig: () => api.get('/monitor-pool/schedule'),
  updateScheduleConfig: (config) => api.put('/monitor-pool/schedule', config),
  triggerMonitor: (productIds) => api.post('/monitor-pool/trigger', { product_ids: productIds }),
  getTriggerJobStatus: (jobId) => api.get(`/monitor-pool/trigger/jobs/${jobId}`)
}


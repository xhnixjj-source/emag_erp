import api from './index'

export const listingApi = {
  getProducts: (params) => api.get('/listing', { params }),
  addProduct: (monitorPoolId) => api.post('/listing', { monitor_pool_id: monitorPoolId }),
  addProducts: (monitorPoolIds) => api.post('/listing/add', { monitor_pool_ids: monitorPoolIds }),
  updateStatus: (id, status) => api.put(`/listing/${id}/status`, { status }),
  getProduct: (id) => api.get(`/listing/${id}`),
  updateProduct: (id, data) => api.put(`/listing/${id}`, data),
  unlockProduct: (id) => api.post(`/listing/${id}/unlock`)
}


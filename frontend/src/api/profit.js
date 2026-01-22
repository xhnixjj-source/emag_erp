import api from './index'

export const profitApi = {
  getCalculation: (listingId) => api.get(`/profit/${listingId}`),
  createCalculation: (listingId, data) => api.post(`/profit/${listingId}`, data),
  updateCalculation: (listingId, data) => api.put(`/profit/${listingId}`, data),
  getFeeSettings: () => api.get('/profit/fee-settings'),
  updateFeeSettings: (settings) => api.put('/profit/fee-settings', settings)
}


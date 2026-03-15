import api from './index'

export const emagSyncApi = {
  // ── 店铺管理 ──
  getShops: () => api.get('/emag-shops'),
  getShop: (id) => api.get(`/emag-shops/${id}`),
  createShop: (data) => api.post('/emag-shops', data),
  updateShop: (id, data) => api.put(`/emag-shops/${id}`, data),
  deleteShop: (id) => api.delete(`/emag-shops/${id}`),
  getShopCredentials: (id) => api.get(`/emag-shops/${id}/credentials`),

  // API Auth (legacy, kept for backward compat)
  saveAccount: (accountData) => api.post('/emag-sync/auth', accountData),
  getAccount: () => api.get('/emag-sync/auth'),
  testConnection: (accountData) => api.post('/emag-sync/auth/test', accountData),
  
  // Sync operations (支持 shop_id 参数)
  syncProducts: (shopId) => api.post(`/emag-sync/products${shopId ? '?shop_id=' + shopId : ''}`),
  syncOrders: (shopId) => api.post(`/emag-sync/orders${shopId ? '?shop_id=' + shopId : ''}`),
  syncReturns: (shopId) => api.post(`/emag-sync/returns${shopId ? '?shop_id=' + shopId : ''}`),
  syncAll: (shopId) => api.post(`/emag-sync/all${shopId ? '?shop_id=' + shopId : ''}`),
  
  // Data queries (支持 shop_id 筛选)
  getProducts: (params) => api.get('/emag-sync/products', { params }),
  getOrders: (params) => api.get('/emag-sync/orders', { params }),
  getReturns: (params) => api.get('/emag-sync/returns', { params }),
  getSyncStatus: () => api.get('/emag-sync/sync-status'),

  // Marketplace backend login (支持 shop_id)
  marketplaceLogin: (data) => api.post('/emag-marketplace/login', data),
  marketplaceLoginStatus: () => api.get('/emag-marketplace/login-status'),
  marketplaceCaptchaDone: () => api.post('/emag-marketplace/captcha-done'),
  marketplaceSubmitSmsCode: (code) => api.post('/emag-marketplace/sms-code', { code }),
  marketplaceLogout: () => api.post('/emag-marketplace/logout'),
  
  // Marketplace inbound shipments (支持 shop_id 筛选)
  syncInboundShipments: () => api.post('/emag-marketplace/inbound-shipments/sync'),
  getInboundShipmentsSyncStatus: () => api.get('/emag-marketplace/inbound-shipments/sync-status'),
  syncInboundShipmentsDetails: () => api.post('/emag-marketplace/inbound-shipments/sync-details'),
  getInboundShipments: (params) => api.get('/emag-marketplace/inbound-shipments', { params }),

  // Ads (支持 shop_id 筛选)
  syncAds: (data) => api.post('/emag-marketplace/ads/sync', data),
  getAdsSyncProgress: () => api.get('/emag-marketplace/ads/sync-progress'),
  getAdsPerformance: (params) => api.get('/emag-marketplace/ads/performance', { params })
}

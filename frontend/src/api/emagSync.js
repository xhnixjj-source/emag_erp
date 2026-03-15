import api from './index'

export const emagSyncApi = {
  // API Auth
  saveAccount: (accountData) => api.post('/emag-sync/auth', accountData),
  getAccount: () => api.get('/emag-sync/auth'),
  testConnection: (accountData) => api.post('/emag-sync/auth/test', accountData),
  
  // Sync operations
  syncProducts: () => api.post('/emag-sync/products'),
  syncOrders: () => api.post('/emag-sync/orders'),
  syncReturns: () => api.post('/emag-sync/returns'),
  syncAll: () => api.post('/emag-sync/all'),
  
  // Data queries
  getProducts: (params) => api.get('/emag-sync/products', { params }),
  getOrders: (params) => api.get('/emag-sync/orders', { params }),
  getReturns: (params) => api.get('/emag-sync/returns', { params }),
  getSyncStatus: () => api.get('/emag-sync/sync-status'),

  // Marketplace backend login
  marketplaceLogin: (data) => api.post('/emag-marketplace/login', data),
  marketplaceLoginStatus: () => api.get('/emag-marketplace/login-status'),
  marketplaceCaptchaDone: () => api.post('/emag-marketplace/captcha-done'),
  marketplaceSubmitSmsCode: (code) => api.post('/emag-marketplace/sms-code', { code }),
  marketplaceLogout: () => api.post('/emag-marketplace/logout'),
  
  // Marketplace inbound shipments
  syncInboundShipments: () => api.post('/emag-marketplace/inbound-shipments/sync'),
  getInboundShipmentsSyncStatus: () => api.get('/emag-marketplace/inbound-shipments/sync-status'),
  syncInboundShipmentsDetails: () => api.post('/emag-marketplace/inbound-shipments/sync-details'),
  getInboundShipments: (params) => api.get('/emag-marketplace/inbound-shipments', { params }),

  // Ads
  syncAds: (data) => api.post('/emag-marketplace/ads/sync', data),
  getAdsPerformance: (params) => api.get('/emag-marketplace/ads/performance', { params })
}


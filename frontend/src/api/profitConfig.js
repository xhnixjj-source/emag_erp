import api from './index'

export const profitConfigApi = {
  // 物流单价
  getLogisticsPrices: () => api.get('/profit-config/logistics'),
  createLogisticsPrice: (data) => api.post('/profit-config/logistics', data),
  
  // VAT配置
  getVatConfigs: (site) => api.get('/profit-config/vat', { params: { site } }),
  createVatConfig: (data) => api.post('/profit-config/vat', data),
  
  // 汇率配置
  getExchangeRates: (params) => api.get('/profit-config/exchange-rate', { params }),
  createExchangeRate: (data) => api.post('/profit-config/exchange-rate', data),
  
  // Genius规则
  getGeniusRules: () => api.get('/profit-config/genius-rule'),
  createGeniusRule: (data) => api.post('/profit-config/genius-rule', data),
  
  // 佣金配置
  getCommissionConfigs: (site) => api.get('/profit-config/commission', { params: { site } }),
  createCommissionConfig: (data) => api.post('/profit-config/commission', data),
  
  // 包材配置
  getPackagingTemplates: () => api.get('/profit-config/packaging'),
  createPackagingTemplate: (data) => api.post('/profit-config/packaging', data),
  
  // 费用模板
  getFeeTemplates: (feeType) => api.get('/profit-config/fee-template', { params: { fee_type: feeType } }),
  createFeeTemplate: (data) => api.post('/profit-config/fee-template', data),
}


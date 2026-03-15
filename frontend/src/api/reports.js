import api from './index'

export const reportsApi = {
  // Report 1: Product Summary (发货数/订单数/退货数量/库存数)
  getProductSummary: (params) => api.get('/reports/product-summary', { params }),

  // Report 2: Ads Weekly Performance (按周广告表现)
  getAdsWeekly: (params) => api.get('/reports/ads-weekly', { params }),
}


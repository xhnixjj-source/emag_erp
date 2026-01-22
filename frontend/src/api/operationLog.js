import api from './index'
import axios from 'axios'

// 创建独立的axios实例用于导出，避免响应拦截器影响blob数据
const exportApi = axios.create({
  baseURL: '/api',
  timeout: 60000,
  responseType: 'blob'
})

// 添加token到导出请求
exportApi.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token')
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// 处理导出响应错误
exportApi.interceptors.response.use(
  (response) => {
    return response.data
  },
  (error) => {
    // 处理blob错误响应（后端可能返回JSON格式的错误）
    if (error.response && error.response.data instanceof Blob) {
      return error.response.data.text().then(text => {
        try {
          const errorData = JSON.parse(text)
          error.response.data = errorData
        } catch (e) {
          // 不是JSON，保持原样
        }
        return Promise.reject(error)
      })
    }
    return Promise.reject(error)
  }
)

export const operationLogApi = {
  getLogs: (params) => api.get('/operation-logs', { params }),
  exportLogs: (params) => exportApi.get('/operation-logs/export', { 
    params,
    headers: {
      'Accept': 'text/csv'
    }
  })
}


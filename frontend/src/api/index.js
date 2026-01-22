import axios from 'axios'
import { ElMessage } from 'element-plus'

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// Request interceptor - Add token
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token')
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    } else {
    }
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// Response interceptor - Handle errors
api.interceptors.response.use(
  (response) => {
    // 对于blob响应，直接返回response对象
    if (response.config.responseType === 'blob') {
      return response.data
    }
    return response.data
  },
  (error) => {
    if (error.response) {
      const { status, data } = error.response
      if (status === 401) {
        const token = localStorage.getItem('token')
        const requestAuthHeader = error.config?.headers?.Authorization
        // Only clear token if we actually sent one (token was invalid)
        // If no token was sent, it might be a timing issue
        if (requestAuthHeader && token) {
          localStorage.removeItem('token')
          // Dispatch to store to clear auth state
          // We'll handle redirect in router guard
        } else {
        }
      } else {
        ElMessage.error(data.detail || '请求失败')
      }
    } else {
      ElMessage.error('网络错误，请稍后重试')
    }
    return Promise.reject(error)
  }
)

export default api


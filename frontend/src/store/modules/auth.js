import { authApi } from '@/api/auth'

// Initialize state from localStorage if available
const getInitialState = () => {
  try {
    const token = localStorage.getItem('token')
    // 尝试从 localStorage 恢复用户信息（如果有保存）
    let user = null
    try {
      const savedUser = localStorage.getItem('user')
      if (savedUser) {
        user = JSON.parse(savedUser)
      }
    } catch (e) {
      // 忽略解析错误
    }
    
    return {
      isAuthenticated: !!token,  // If token exists, assume authenticated (will be verified by checkAuth)
      user: user,  // 从 localStorage 恢复用户信息（如果有）
      token: token
    }
  } catch (error) {
    return {
      isAuthenticated: false,
      user: null,
      token: null
    }
  }
}

const state = getInitialState()

const mutations = {
  SET_AUTH(state, { user, token }) {
    state.isAuthenticated = true
    state.user = user
    state.token = token
    if (token) {
      localStorage.setItem('token', token)
    }
    // 保存用户信息到 localStorage（用于刷新后恢复）
    if (user) {
      try {
        localStorage.setItem('user', JSON.stringify(user))
      } catch (e) {
        console.warn('Failed to save user to localStorage:', e)
      }
    }
  },
  CLEAR_AUTH(state) {
    state.isAuthenticated = false
    state.user = null
    state.token = null
    localStorage.removeItem('token')
    localStorage.removeItem('user')
  }
}

const actions = {
  async login({ commit }, { username, password }) {
    try {
      const response = await authApi.login(username, password)
      const token = response.access_token || response.token
      commit('SET_AUTH', {
        user: response.user,
        token: token
      })
      return { success: true }
    } catch (error) {
      return { success: false, error: error.response?.data?.detail || error.message || '登录失败' }
    }
  },
  async logout({ commit }) {
    try {
      await authApi.logout()
    } catch (error) {
      console.error('Logout error:', error)
    } finally {
      commit('CLEAR_AUTH')
    }
  },
  async checkAuth({ commit, state }) {
    // Re-read token from localStorage in case state was reset
    const localStorageToken = localStorage.getItem('token')
    const token = localStorageToken || state.token
    if (!token) {
      commit('CLEAR_AUTH')
      return
    }
    try {
      const user = await authApi.getCurrentUser()
      commit('SET_AUTH', { user, token: token })
    } catch (error) {
      
      // 只清除认证状态如果是401（未授权）错误
      // 网络错误、超时等临时错误不应该清除认证状态，保留token和用户信息
      if (error.response?.status === 401) {
        // Token 无效或过期，清除认证
        commit('CLEAR_AUTH')
      } else {
        // 网络错误、超时等临时错误，保留认证状态
        // 如果state中已经有用户信息，保持不变
        if (state.user && state.token) {
          // 保持当前认证状态，不清除
          console.warn('checkAuth failed (network/timeout error) but keeping auth state:', error.message)
        } else {
          // 如果state中没有用户信息，但token存在，尝试从localStorage恢复用户信息
          try {
            const savedUser = localStorage.getItem('user')
            if (savedUser) {
              const user = JSON.parse(savedUser)
              // 恢复用户信息，但标记为需要验证
              commit('SET_AUTH', { user, token: token })
              console.warn('checkAuth failed but restored user from localStorage:', error.message)
            } else {
              // 如果localStorage中也没有用户信息，但有token，设置一个临时的认证状态
              // 这样至少可以让用户看到界面（侧边栏会显示）
              // 后续API调用会重新验证
              if (token) {
                // 设置临时的认证状态（有token但没有用户信息）
                state.isAuthenticated = true
                state.token = token
                console.warn('checkAuth failed but keeping token for retry:', error.message)
              }
            }
          } catch (e) {
            console.warn('checkAuth failed and could not restore user:', error.message)
            // 如果localStorage解析失败，至少保留token
            if (token) {
              state.isAuthenticated = true
              state.token = token
            }
          }
        }
      }
    }
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions
}


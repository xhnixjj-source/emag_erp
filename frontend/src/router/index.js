import { createRouter, createWebHistory } from 'vue-router'
import store from '@/store'

const routes = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/Login.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/',
    redirect: '/keywords'
  },
  {
    path: '/keywords',
    name: 'Keywords',
    component: () => import('@/views/KeywordSearch.vue'),
    meta: { requiresAuth: true }
  },
  {
    path: '/filter-pool',
    name: 'FilterPool',
    component: () => import('@/views/FilterPool.vue'),
    meta: { requiresAuth: true }
  },
  {
    path: '/monitor-pool',
    name: 'MonitorPool',
    component: () => import('@/views/MonitorPool.vue'),
    meta: { requiresAuth: true }
  },
  {
    path: '/listing',
    name: 'Listing',
    component: () => import('@/views/Listing.vue'),
    meta: { requiresAuth: true }
  },
  {
    path: '/product-library',
    name: 'ProductLibrary',
    component: () => import('@/views/ProductLibrary.vue'),
    meta: { requiresAuth: true }
  },
  {
    path: '/profit-calculation',
    name: 'ProfitCalculation',
    component: () => import('@/views/ProfitCalculation.vue'),
    meta: { requiresAuth: true }
  },
  {
    path: '/operation-log',
    name: 'OperationLog',
    component: () => import('@/views/OperationLog.vue'),
    meta: { requiresAuth: true, requiresAdmin: true }
  },
  {
    path: '/link-screening',
    name: 'LinkScreening',
    component: () => import('@/views/LinkScreening.vue'),
    meta: { requiresAuth: true }
  },
  {
    path: '/failed-tasks',
    name: 'FailedTasks',
    component: () => import('@/views/FailedTasks.vue'),
    meta: { requiresAuth: true }
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

// Route guard
router.beforeEach(async (to, from, next) => {
  // Check if we have a token in localStorage but not in state (state might be reset)
  const token = localStorage.getItem('token')
  const isAuthenticated = store.state.auth.isAuthenticated
  const user = store.state.auth.user
  

  // 如果访问登录页面
  if (to.path === '/login') {
    // 如果已经登录，重定向到首页
    if (token && user && isAuthenticated) {
      next('/')
      return
    }
    // 如果只有 token 但没有用户信息，尝试恢复
    if (token && !user && !isAuthenticated) {
      try {
        const savedUser = localStorage.getItem('user')
        if (savedUser) {
          const parsedUser = JSON.parse(savedUser)
          store.commit('auth/SET_AUTH', { user: parsedUser, token: token })
          // 恢复成功后重定向到首页
          next('/')
          return
        }
      } catch (e) {
        console.warn('Failed to restore user from localStorage:', e)
      }
    }
    // 如果没有认证信息，允许访问登录页面
    next()
    return
  }

  // 如果已经有token和用户信息，且已认证，直接允许访问（优化性能，避免不必要的API调用）
  if (token && user && isAuthenticated) {
    // 已经有完整的认证信息，直接检查权限
    if (to.meta.requiresAuth && !isAuthenticated) {
      next('/login')
      return
    } else if (to.meta.requiresAdmin && user?.role !== 'admin') {
      next('/')
      return
    } else {
      next()
      return
    }
  }

  // 如果有token但没有用户信息，尝试从localStorage恢复用户信息
  if (token && !user && !isAuthenticated && to.path !== '/login') {
    try {
      const savedUser = localStorage.getItem('user')
      if (savedUser) {
        const parsedUser = JSON.parse(savedUser)
        // 临时恢复用户信息（用于显示界面）
        store.commit('auth/SET_AUTH', { user: parsedUser, token: token })
        // 继续执行路由守卫逻辑，不需要调用checkAuth
        const finalIsAuthenticated = store.state.auth.isAuthenticated
        const finalUser = store.state.auth.user
        
        if (to.meta.requiresAuth && !finalIsAuthenticated) {
          next('/login')
          return
        } else if (to.meta.requiresAdmin && finalUser?.role !== 'admin') {
          next('/')
          return
        } else {
          next()
          return
        }
      }
    } catch (e) {
      console.warn('Failed to restore user from localStorage:', e)
    }
  }

  // 如果没有认证信息但有token，尝试恢复认证状态
  if (token && !isAuthenticated && to.path !== '/login') {
    try {
      await store.dispatch('auth/checkAuth')
    } catch (error) {
      // checkAuth失败，但不一定是认证失败
      // 网络错误不应该阻止用户访问已认证的页面
      console.warn('checkAuth error in router guard:', error)
    }
  }

  const finalIsAuthenticated = store.state.auth.isAuthenticated
  const finalUser = store.state.auth.user

  if (to.meta.requiresAuth && !finalIsAuthenticated) {
    next('/login')
  } else if (to.meta.requiresAdmin && finalUser?.role !== 'admin') {
    next('/')
  } else {
    next()
  }
})

export default router


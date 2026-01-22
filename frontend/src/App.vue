<template>
  <el-container v-if="isAuthenticated && currentUser" class="app-container">
    <el-header class="app-header">
      <div class="header-content">
        <h1 class="app-title">eMAG ERP 选品上架管理系统</h1>
        <div class="header-actions">
          <span class="user-info">欢迎，{{ currentUser?.username }}</span>
          <el-button type="danger" size="small" @click="handleLogout">退出登录</el-button>
        </div>
      </div>
    </el-header>
    <el-container>
      <el-aside width="200px" class="app-sidebar">
        <el-menu
          :default-active="activeMenu"
          router
          class="sidebar-menu"
        >
          <el-menu-item index="/keywords">
            <el-icon><Search /></el-icon>
            <span>关键字管理</span>
          </el-menu-item>
          <el-menu-item index="/link-screening">
            <el-icon><Connection /></el-icon>
            <span>链接初筛</span>
          </el-menu-item>
          <el-menu-item index="/filter-pool">
            <el-icon><Filter /></el-icon>
            <span>筛选池</span>
          </el-menu-item>
          <el-menu-item index="/monitor-pool">
            <el-icon><Monitor /></el-icon>
            <span>监控池</span>
          </el-menu-item>
          <el-menu-item index="/profit">
            <el-icon><Money /></el-icon>
            <span>利润测算</span>
          </el-menu-item>
          <el-menu-item index="/listing">
            <el-icon><Document /></el-icon>
            <span>上架管理</span>
          </el-menu-item>
          <el-menu-item index="/operation-log" v-if="isAdmin">
            <el-icon><Document /></el-icon>
            <span>操作日志</span>
          </el-menu-item>
        </el-menu>
      </el-aside>
      <el-main class="app-main">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
  <router-view v-else />
</template>

<script setup>
import { computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useStore } from 'vuex'
import { Search, Filter, Monitor, Document, Money, Connection } from '@element-plus/icons-vue'

const router = useRouter()
const route = useRoute()
const store = useStore()

const isAuthenticated = computed(() => store.state.auth.isAuthenticated)
const currentUser = computed(() => store.state.auth.user)
const isAdmin = computed(() => currentUser.value?.role === 'admin')
const activeMenu = computed(() => route.path)

const handleLogout = async () => {
  await store.dispatch('auth/logout')
  router.push('/login')
}

onMounted(async () => {
  // Only check auth if not on login page
  if (route.path !== '/login') {
    await store.dispatch('auth/checkAuth')
  }
})
</script>

<style scoped>
.app-container {
  height: 100vh;
}

.app-header {
  background-color: #409eff;
  color: white;
  padding: 0;
  line-height: 60px;
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 20px;
}

.app-title {
  margin: 0;
  font-size: 20px;
  font-weight: 500;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 15px;
}

.user-info {
  font-size: 14px;
}

.app-sidebar {
  background-color: #304156;
}

.sidebar-menu {
  border-right: none;
  background-color: #304156;
}

/* 菜单项文字颜色为白色 */
.sidebar-menu .el-menu-item {
  color: white !important;
}

.sidebar-menu .el-menu-item:hover {
  background-color: #263445 !important;
  color: white !important;
}

.sidebar-menu .el-menu-item.is-active {
  background-color: #409eff !important;
  color: white !important;
}

/* 菜单图标颜色为白色 */
.sidebar-menu .el-menu-item .el-icon {
  color: white !important;
}

.sidebar-menu .el-menu-item span {
  color: white !important;
}

.app-main {
  background-color: #f0f2f5;
  padding: 20px;
}
</style>

<style>
body {
  margin: 0;
  padding: 0;
}

#app {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', sans-serif;
}
</style>


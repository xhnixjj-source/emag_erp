import api from './index'

export const failedTasksApi = {
  // Get failed tasks list
  getFailedTasks: (params = {}) => api.get('/failed-tasks', { params }),

  // Batch retry failed tasks
  batchRetry: (taskIds) => api.post('/failed-tasks/batch-retry', { task_ids: taskIds })
}


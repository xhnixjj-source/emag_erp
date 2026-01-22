<template>
  <div class="profit-calc-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>利润测算</span>
          <div>
            <el-button @click="showFeeSettingsDialog = true">费用设置</el-button>
          </div>
        </div>
      </template>

      <!-- 产品选择 -->
      <el-form :inline="true" class="filter-form">
        <el-form-item label="选择产品">
          <el-select 
            v-model="selectedListingId" 
            placeholder="请选择产品"
            filterable
            style="width: 400px"
            @change="loadCalculation"
          >
            <el-option
              v-for="product in listingProducts"
              :key="product.id"
              :label="`${product.id} - ${product.product_url?.substring(0, 50)}...`"
              :value="product.id"
            />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="loadListingProducts">刷新产品列表</el-button>
        </el-form-item>
      </el-form>

      <!-- 锁定状态提示 -->
      <el-alert
        v-if="currentProduct && currentProduct.is_locked && !canEdit"
        :title="`该产品已被用户ID ${currentProduct.locked_by_user_id} 锁定，无法编辑`"
        type="warning"
        :closable="false"
        style="margin-bottom: 20px"
      />

      <!-- 利润测算表单 -->
      <el-card v-if="selectedListingId" shadow="never" style="margin-top: 20px">
        <template #header>
          <div class="calc-header">
            <span>利润测算表单</span>
            <el-button 
              type="primary" 
              @click="handleSave"
              :disabled="!canEdit"
              :loading="saving"
            >
              保存
            </el-button>
          </div>
        </template>

        <el-form :model="calculation" label-width="150px" :disabled="!canEdit">
          <el-row :gutter="20">
            <el-col :span="12">
              <el-form-item label="采购价 (€)">
                <el-input-number
                  v-model="calculation.purchase_price"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="售价 (€)">
                <el-input-number
                  v-model="calculation.selling_price"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
              </el-form-item>
            </el-col>
          </el-row>

          <el-divider>尺寸和重量</el-divider>

          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="长度 (cm)">
                <el-input-number
                  v-model="calculation.length"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="宽度 (cm)">
                <el-input-number
                  v-model="calculation.width"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="高度 (cm)">
                <el-input-number
                  v-model="calculation.height"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="12">
              <el-form-item label="重量 (kg)">
                <el-input-number
                  v-model="calculation.weight"
                  :precision="3"
                  :min="0"
                  style="width: 100%"
                />
              </el-form-item>
            </el-col>
          </el-row>

          <el-divider>费用设置</el-divider>

          <el-row :gutter="20">
            <el-col :span="12">
              <el-form-item label="头程物流费 (€)">
                <el-input-number
                  v-model="calculation.shipping_cost"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
                <span class="form-tip">或使用通用设置</span>
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="订单处理费 (€)">
                <el-input-number
                  v-model="calculation.order_fee"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
                <span class="form-tip">或使用通用设置</span>
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="12">
              <el-form-item label="仓储费 (€)">
                <el-input-number
                  v-model="calculation.storage_fee"
                  :precision="2"
                  :min="0"
                  style="width: 100%"
                />
                <span class="form-tip">或使用通用设置</span>
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="平台佣金 (%)">
                <el-input-number
                  v-model="calculation.platform_commission"
                  :precision="2"
                  :min="0"
                  :max="100"
                  style="width: 100%"
                />
                <span class="form-tip">或使用通用设置</span>
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="12">
              <el-form-item label="VAT (%)">
                <el-input-number
                  v-model="calculation.vat"
                  :precision="2"
                  :min="0"
                  :max="100"
                  style="width: 100%"
                />
                <span class="form-tip">或使用通用设置</span>
              </el-form-item>
            </el-col>
          </el-row>

          <el-divider>计算结果</el-divider>

          <el-row :gutter="20">
            <el-col :span="12">
              <el-form-item label="总成本 (€)">
                <el-input-number
                  :model-value="totalCost"
                  :precision="2"
                  disabled
                  style="width: 100%"
                />
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="利润额 (€)">
                <el-input-number
                  :model-value="profitAmount"
                  :precision="2"
                  disabled
                  style="width: 100%"
                  :class="{ 'profit-positive': profitAmount > 0, 'profit-negative': profitAmount < 0 }"
                />
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="12">
              <el-form-item label="利润率 (%)">
                <el-input-number
                  :model-value="profitMargin"
                  :precision="2"
                  disabled
                  style="width: 100%"
                  :class="{ 'profit-positive': profitMargin > 0, 'profit-negative': profitMargin < 0 }"
                />
              </el-form-item>
            </el-col>
          </el-row>
        </el-form>
      </el-card>

      <el-empty v-else description="请先选择产品" />
    </el-card>

    <!-- 费用设置对话框 -->
    <el-dialog v-model="showFeeSettingsDialog" title="通用费用设置" width="600px">
      <el-form :model="feeSettings" label-width="150px">
        <el-form-item label="头程物流费 (€)">
          <el-input-number
            v-model="feeSettings.shipping_cost"
            :precision="2"
            :min="0"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="订单处理费 (€)">
          <el-input-number
            v-model="feeSettings.order_fee"
            :precision="2"
            :min="0"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="仓储费 (€)">
          <el-input-number
            v-model="feeSettings.storage_fee"
            :precision="2"
            :min="0"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="平台佣金 (%)">
          <el-input-number
            v-model="feeSettings.platform_commission"
            :precision="2"
            :min="0"
            :max="100"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="VAT (%)">
          <el-input-number
            v-model="feeSettings.vat"
            :precision="2"
            :min="0"
            :max="100"
            style="width: 100%"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showFeeSettingsDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSaveFeeSettings" :loading="savingFeeSettings">
          保存
        </el-button>
        <el-button @click="handleApplyFeeSettings">应用到当前测算</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { profitApi } from '@/api/profit'
import { listingApi } from '@/api/listing'
import { useStore } from 'vuex'
import { ElMessage } from 'element-plus'

const store = useStore()
const isAdmin = computed(() => store.state.auth.user?.role === 'admin')
const currentUserId = computed(() => store.state.auth.user?.id)

const selectedListingId = ref(null)
const listingProducts = ref([])
const currentProduct = ref(null)
const calculation = reactive({
  purchase_price: 0,
  selling_price: 0,
  length: 0,
  width: 0,
  height: 0,
  weight: 0,
  shipping_cost: 0,
  order_fee: 0,
  storage_fee: 0,
  platform_commission: 0,
  vat: 0
})

const saving = ref(false)
const showFeeSettingsDialog = ref(false)
const feeSettings = reactive({
  shipping_cost: 0,
  order_fee: 0,
  storage_fee: 0,
  platform_commission: 0,
  vat: 0
})
const savingFeeSettings = ref(false)

const canEdit = computed(() => {
  if (!currentProduct.value) return false
  if (!currentProduct.value.is_locked) return true
  if (isAdmin.value) return true
  return currentProduct.value.locked_by_user_id === currentUserId.value
})

const totalCost = computed(() => {
  const purchase = calculation.purchase_price || 0
  const shipping = calculation.shipping_cost || 0
  const order = calculation.order_fee || 0
  const storage = calculation.storage_fee || 0
  const commission = (calculation.selling_price || 0) * (calculation.platform_commission || 0) / 100
  const vat = (calculation.selling_price || 0) * (calculation.vat || 0) / 100
  return purchase + shipping + order + storage + commission + vat
})

const profitAmount = computed(() => {
  return (calculation.selling_price || 0) - totalCost.value
})

const profitMargin = computed(() => {
  if (!calculation.selling_price || calculation.selling_price === 0) return 0
  return (profitAmount.value / calculation.selling_price) * 100
})

const loadListingProducts = async () => {
  try {
    const response = await listingApi.getProducts({ page: 1, page_size: 1000 })
    listingProducts.value = response.data || response.items || []
  } catch (error) {
    ElMessage.error('加载产品列表失败')
  }
}

const loadCalculation = async () => {
  if (!selectedListingId.value) return
  
  try {
    // 加载产品信息以检查锁定状态
    const product = await listingApi.getProduct(selectedListingId.value)
    currentProduct.value = product
    
    // 加载利润测算数据
    try {
      const calc = await profitApi.getCalculation(selectedListingId.value)
      Object.assign(calculation, {
        purchase_price: calc.purchase_price || 0,
        selling_price: calc.selling_price || 0,
        length: calc.length || 0,
        width: calc.width || 0,
        height: calc.height || 0,
        weight: calc.weight || 0,
        shipping_cost: calc.shipping_cost || 0,
        order_fee: calc.order_fee || 0,
        storage_fee: calc.storage_fee || 0,
        platform_commission: calc.platform_commission || 0,
        vat: calc.vat || 0
      })
    } catch (error) {
      // 如果没有测算数据，使用通用费用设置
      await loadFeeSettings()
      handleApplyFeeSettings()
    }
  } catch (error) {
    ElMessage.error('加载产品信息失败')
  }
}

const loadFeeSettings = async () => {
  try {
    const settings = await profitApi.getFeeSettings()
    Object.assign(feeSettings, settings)
  } catch (error) {
    console.error('Load fee settings error:', error)
  }
}

const handleSave = async () => {
  if (!selectedListingId.value) return
  
  if (!canEdit.value) {
    ElMessage.warning('该产品已被锁定，无法编辑')
    return
  }
  
  saving.value = true
  try {
    const calcData = { ...calculation }
    try {
      await profitApi.updateCalculation(selectedListingId.value, calcData)
      ElMessage.success('保存成功')
    } catch (error) {
      // 如果更新失败，尝试创建
      await profitApi.createCalculation(selectedListingId.value, calcData)
      ElMessage.success('保存成功')
    }
  } catch (error) {
    ElMessage.error('保存失败')
  } finally {
    saving.value = false
  }
}

const handleSaveFeeSettings = async () => {
  savingFeeSettings.value = true
  try {
    await profitApi.updateFeeSettings(feeSettings)
    ElMessage.success('费用设置已保存')
    showFeeSettingsDialog.value = false
  } catch (error) {
    ElMessage.error('保存失败')
  } finally {
    savingFeeSettings.value = false
  }
}

const handleApplyFeeSettings = () => {
  calculation.shipping_cost = feeSettings.shipping_cost
  calculation.order_fee = feeSettings.order_fee
  calculation.storage_fee = feeSettings.storage_fee
  calculation.platform_commission = feeSettings.platform_commission
  calculation.vat = feeSettings.vat
  ElMessage.success('费用设置已应用到当前测算')
}

onMounted(async () => {
  await loadListingProducts()
  await loadFeeSettings()
})
</script>

<style scoped>
.profit-calc-container {
  padding: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-form {
  margin-bottom: 20px;
}

.calc-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.form-tip {
  font-size: 12px;
  color: #909399;
  margin-left: 10px;
}

.profit-positive {
  color: #67c23a;
}

.profit-negative {
  color: #f56c6c;
}
</style>


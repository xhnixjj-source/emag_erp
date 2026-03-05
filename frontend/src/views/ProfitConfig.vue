<template>
  <div class="profit-config-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>利润测算配置管理</span>
          <el-button type="primary" @click="loadConfigs">刷新</el-button>
        </div>
      </template>

      <el-tabs v-model="activeTab" type="border-card">
        <!-- 物流单价配置 -->
        <el-tab-pane label="物流单价" name="logistics">
          <el-button type="primary" @click="handleAddLogistics" style="margin-bottom: 20px">
            新增物流单价
          </el-button>
          <el-table :data="logisticsPrices" style="width: 100%">
            <el-table-column prop="transport_mode" label="运输方式" width="120">
              <template #default="{ row }">
                {{ row.transport_mode === 'air' ? '空运' : '陆运' }}
              </template>
            </el-table-column>
            <el-table-column prop="price_per_kg_rmb" label="单价(RMB/kg)" width="150" />
            <el-table-column prop="effective_from" label="生效时间" width="180">
              <template #default="{ row }">
                {{ formatDateTime(row.effective_from) }}
              </template>
            </el-table-column>
            <el-table-column prop="effective_to" label="失效时间" width="180">
              <template #default="{ row }">
                {{ row.effective_to ? formatDateTime(row.effective_to) : '-' }}
              </template>
            </el-table-column>
            <el-table-column prop="remark" label="备注" />
          </el-table>
        </el-tab-pane>

        <!-- VAT配置 -->
        <el-tab-pane label="VAT配置" name="vat">
          <el-button type="primary" @click="handleAddVat" style="margin-bottom: 20px">
            新增VAT配置
          </el-button>
          <el-table :data="vatConfigs" style="width: 100%">
            <el-table-column prop="site" label="站点" width="120" />
            <el-table-column prop="vat_rate" label="VAT率" width="120">
              <template #default="{ row }">
                {{ (row.vat_rate * 100).toFixed(2) }}%
              </template>
            </el-table-column>
            <el-table-column prop="effective_from" label="生效时间" width="180">
              <template #default="{ row }">
                {{ formatDateTime(row.effective_from) }}
              </template>
            </el-table-column>
            <el-table-column prop="effective_to" label="失效时间" width="180">
              <template #default="{ row }">
                {{ row.effective_to ? formatDateTime(row.effective_to) : '-' }}
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- 汇率配置 -->
        <el-tab-pane label="汇率配置" name="exchange">
          <el-button type="primary" @click="handleAddExchange" style="margin-bottom: 20px">
            新增汇率配置
          </el-button>
          <el-table :data="exchangeRates" style="width: 100%">
            <el-table-column prop="from_currency" label="源货币" width="100" />
            <el-table-column prop="to_currency" label="目标货币" width="100" />
            <el-table-column prop="rate" label="汇率" width="120">
              <template #default="{ row }">
                1 {{ row.from_currency }} = {{ row.rate }} {{ row.to_currency }}
              </template>
            </el-table-column>
            <el-table-column prop="source" label="来源" width="120" />
            <el-table-column prop="effective_from" label="生效时间" width="180">
              <template #default="{ row }">
                {{ formatDateTime(row.effective_from) }}
              </template>
            </el-table-column>
            <el-table-column prop="effective_to" label="失效时间" width="180">
              <template #default="{ row }">
                {{ row.effective_to ? formatDateTime(row.effective_to) : '-' }}
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- Genius规则 -->
        <el-tab-pane label="Genius规则" name="genius">
          <el-button type="primary" @click="handleAddGenius" style="margin-bottom: 20px">
            新增Genius规则
          </el-button>
          <el-table :data="geniusRules" style="width: 100%">
            <el-table-column prop="rule_name" label="规则名称" width="200" />
            <el-table-column prop="currency" label="货币" width="100" />
            <el-table-column prop="is_active" label="是否激活" width="100">
              <template #default="{ row }">
                <el-tag :type="row.is_active ? 'success' : 'info'">
                  {{ row.is_active ? '是' : '否' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="阶梯明细" min-width="300">
              <template #default="{ row }">
                <div v-for="step in row.steps" :key="step.id" style="margin-bottom: 5px;">
                  {{ step.min_sales_amount }} - {{ step.max_sales_amount || '∞' }}: {{ step.fee_amount }} {{ row.currency }}
                </div>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- 佣金配置 -->
        <el-tab-pane label="佣金配置" name="commission">
          <el-button type="primary" @click="handleAddCommission" style="margin-bottom: 20px">
            新增佣金配置
          </el-button>
          <el-table :data="commissionConfigs" style="width: 100%">
            <el-table-column prop="site" label="站点" width="120" />
            <el-table-column prop="category_or_group" label="类目/佣金组" width="200" />
            <el-table-column prop="commission_rate" label="佣金率" width="120">
              <template #default="{ row }">
                {{ (row.commission_rate * 100).toFixed(2) }}%
              </template>
            </el-table-column>
            <el-table-column prop="effective_from" label="生效时间" width="180">
              <template #default="{ row }">
                {{ formatDateTime(row.effective_from) }}
              </template>
            </el-table-column>
            <el-table-column prop="effective_to" label="失效时间" width="180">
              <template #default="{ row }">
                {{ row.effective_to ? formatDateTime(row.effective_to) : '-' }}
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- 包材配置 -->
        <el-tab-pane label="包材配置" name="packaging">
          <el-button type="primary" @click="handleAddPackaging" style="margin-bottom: 20px">
            新增包材配置
          </el-button>
          <el-table :data="packagingTemplates" style="width: 100%">
            <el-table-column prop="name" label="名称" width="200" />
            <el-table-column prop="cost_rmb" label="成本(RMB)" width="150" />
            <el-table-column prop="apply_scope" label="适用范围" width="200" />
            <el-table-column prop="is_default" label="是否默认" width="100">
              <template #default="{ row }">
                <el-tag :type="row.is_default ? 'success' : 'info'">
                  {{ row.is_default ? '是' : '否' }}
                </el-tag>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- 费用模板 -->
        <el-tab-pane label="费用模板" name="fee">
          <el-button type="primary" @click="handleAddFeeTemplate" style="margin-bottom: 20px">
            新增费用模板
          </el-button>
          <el-table :data="feeTemplates" style="width: 100%">
            <el-table-column prop="template_name" label="模板名称" width="200" />
            <el-table-column prop="fee_type" label="费用类型" width="150">
              <template #default="{ row }">
                {{ row.fee_type === 'order_handling' ? '订单处理费' : '仓储费' }}
              </template>
            </el-table-column>
            <el-table-column prop="currency" label="货币" width="100" />
            <el-table-column prop="calculation_method" label="计算方式" width="150" />
            <el-table-column prop="base_amount" label="基础金额" width="120" />
            <el-table-column prop="rate" label="费率" width="120" />
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- 物流单价对话框 -->
    <el-dialog v-model="showLogisticsDialog" title="物流单价配置" width="600px">
      <el-form :model="logisticsForm" label-width="150px">
        <el-form-item label="运输方式" required>
          <el-select v-model="logisticsForm.transport_mode" style="width: 100%">
            <el-option label="空运" value="air" />
            <el-option label="陆运" value="land" />
          </el-select>
        </el-form-item>
        <el-form-item label="单价(RMB/kg)" required>
          <el-input-number v-model="logisticsForm.price_per_kg_rmb" :precision="2" :min="0" style="width: 100%" />
        </el-form-item>
        <el-form-item label="生效时间">
          <el-date-picker
            v-model="logisticsForm.effective_from"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="失效时间">
          <el-date-picker
            v-model="logisticsForm.effective_to"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="备注">
          <el-input v-model="logisticsForm.remark" type="textarea" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showLogisticsDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSaveLogistics" :loading="saving">保存</el-button>
      </template>
    </el-dialog>

    <!-- VAT配置对话框 -->
    <el-dialog v-model="showVatDialog" title="VAT配置" width="600px">
      <el-form :model="vatForm" label-width="150px">
        <el-form-item label="站点" required>
          <el-input v-model="vatForm.site" />
        </el-form-item>
        <el-form-item label="VAT率(%)" required>
          <el-input-number v-model="vatForm.vat_rate" :precision="4" :min="0" :max="1" style="width: 100%" />
          <div style="color: #999; font-size: 12px; margin-top: 5px;">例如：0.21 表示 21%</div>
        </el-form-item>
        <el-form-item label="生效时间">
          <el-date-picker
            v-model="vatForm.effective_from"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="失效时间">
          <el-date-picker
            v-model="vatForm.effective_to"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showVatDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSaveVat" :loading="saving">保存</el-button>
      </template>
    </el-dialog>

    <!-- 汇率配置对话框 -->
    <el-dialog v-model="showExchangeDialog" title="汇率配置" width="600px">
      <el-form :model="exchangeForm" label-width="150px">
        <el-form-item label="源货币" required>
          <el-input v-model="exchangeForm.from_currency" />
        </el-form-item>
        <el-form-item label="目标货币" required>
          <el-input v-model="exchangeForm.to_currency" />
        </el-form-item>
        <el-form-item label="汇率" required>
          <el-input-number v-model="exchangeForm.rate" :precision="4" :min="0" style="width: 100%" />
          <div style="color: #999; font-size: 12px; margin-top: 5px;">例如：1.6 表示 1 RON = 1.6 CNY</div>
        </el-form-item>
        <el-form-item label="来源">
          <el-input v-model="exchangeForm.source" />
        </el-form-item>
        <el-form-item label="生效时间">
          <el-date-picker
            v-model="exchangeForm.effective_from"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="失效时间">
          <el-date-picker
            v-model="exchangeForm.effective_to"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showExchangeDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSaveExchange" :loading="saving">保存</el-button>
      </template>
    </el-dialog>

    <!-- 包材配置对话框 -->
    <el-dialog v-model="showPackagingDialog" title="包材配置" width="600px">
      <el-form :model="packagingForm" label-width="150px">
        <el-form-item label="名称" required>
          <el-input v-model="packagingForm.name" />
        </el-form-item>
        <el-form-item label="成本(RMB)" required>
          <el-input-number v-model="packagingForm.cost_rmb" :precision="2" :min="0" style="width: 100%" />
        </el-form-item>
        <el-form-item label="适用范围">
          <el-input v-model="packagingForm.apply_scope" />
        </el-form-item>
        <el-form-item label="是否默认">
          <el-switch v-model="packagingForm.is_default" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showPackagingDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSavePackaging" :loading="saving">保存</el-button>
      </template>
    </el-dialog>

    <!-- 佣金配置对话框 -->
    <el-dialog v-model="showCommissionDialog" title="佣金配置" width="600px">
      <el-form :model="commissionForm" label-width="150px">
        <el-form-item label="站点" required>
          <el-input v-model="commissionForm.site" />
        </el-form-item>
        <el-form-item label="类目/佣金组" required>
          <el-input v-model="commissionForm.category_or_group" />
        </el-form-item>
        <el-form-item label="佣金率(%)" required>
          <el-input-number v-model="commissionForm.commission_rate" :precision="4" :min="0" :max="1" style="width: 100%" />
          <div style="color: #999; font-size: 12px; margin-top: 5px;">例如：0.15 表示 15%</div>
        </el-form-item>
        <el-form-item label="生效时间">
          <el-date-picker
            v-model="commissionForm.effective_from"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="失效时间">
          <el-date-picker
            v-model="commissionForm.effective_to"
            type="datetime"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showCommissionDialog = false">取消</el-button>
        <el-button type="primary" @click="handleSaveCommission" :loading="saving">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { profitConfigApi } from '@/api/profitConfig'
import { ElMessage } from 'element-plus'

const activeTab = ref('logistics')
const logisticsPrices = ref([])
const vatConfigs = ref([])
const exchangeRates = ref([])
const geniusRules = ref([])
const commissionConfigs = ref([])
const packagingTemplates = ref([])
const feeTemplates = ref([])

const showLogisticsDialog = ref(false)
const showVatDialog = ref(false)
const showExchangeDialog = ref(false)
const showGeniusDialog = ref(false)
const showCommissionDialog = ref(false)
const showPackagingDialog = ref(false)
const showFeeTemplateDialog = ref(false)
const saving = ref(false)

const logisticsForm = reactive({
  transport_mode: 'air',
  price_per_kg_rmb: 0,
  effective_from: null,
  effective_to: null,
  remark: ''
})

const vatForm = reactive({
  site: 'emag_ro',
  vat_rate: 0.21,
  effective_from: null,
  effective_to: null
})

const exchangeForm = reactive({
  from_currency: 'RON',
  to_currency: 'CNY',
  rate: 1.6,
  source: 'manual',
  effective_from: null,
  effective_to: null
})

const packagingForm = reactive({
  name: '',
  cost_rmb: 0.2,
  apply_scope: '',
  is_default: false
})

const commissionForm = reactive({
  site: 'emag_ro',
  category_or_group: '',
  commission_rate: 0.15,
  effective_from: null,
  effective_to: null
})

const formatDateTime = (dateStr) => {
  if (!dateStr) return '-'
  const date = new Date(dateStr)
  return date.toLocaleString('zh-CN')
}

const loadConfigs = async () => {
  try {
    logisticsPrices.value = await profitConfigApi.getLogisticsPrices()
    vatConfigs.value = await profitConfigApi.getVatConfigs()
    exchangeRates.value = await profitConfigApi.getExchangeRates()
    geniusRules.value = await profitConfigApi.getGeniusRules()
    commissionConfigs.value = await profitConfigApi.getCommissionConfigs()
    packagingTemplates.value = await profitConfigApi.getPackagingTemplates()
    feeTemplates.value = await profitConfigApi.getFeeTemplates()
    ElMessage.success('配置加载成功')
  } catch (error) {
    console.error('加载配置失败:', error)
    ElMessage.error('加载配置失败: ' + (error.response?.data?.detail || error.message))
  }
}

const handleAddLogistics = () => {
  Object.assign(logisticsForm, {
    transport_mode: 'air',
    price_per_kg_rmb: 0,
    effective_from: null,
    effective_to: null,
    remark: ''
  })
  showLogisticsDialog.value = true
}

const handleSaveLogistics = async () => {
  if (!logisticsForm.transport_mode || !logisticsForm.price_per_kg_rmb) {
    ElMessage.warning('请填写完整信息')
    return
  }
  saving.value = true
  try {
    await profitConfigApi.createLogisticsPrice(logisticsForm)
    ElMessage.success('保存成功')
    showLogisticsDialog.value = false
    loadConfigs()
  } catch (error) {
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    saving.value = false
  }
}

const handleAddVat = () => {
  Object.assign(vatForm, {
    site: 'emag_ro',
    vat_rate: 0.21,
    effective_from: null,
    effective_to: null
  })
  showVatDialog.value = true
}

const handleSaveVat = async () => {
  if (!vatForm.site || vatForm.vat_rate === null) {
    ElMessage.warning('请填写完整信息')
    return
  }
  saving.value = true
  try {
    await profitConfigApi.createVatConfig(vatForm)
    ElMessage.success('保存成功')
    showVatDialog.value = false
    loadConfigs()
  } catch (error) {
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    saving.value = false
  }
}

const handleAddExchange = () => {
  Object.assign(exchangeForm, {
    from_currency: 'RON',
    to_currency: 'CNY',
    rate: 1.6,
    source: 'manual',
    effective_from: null,
    effective_to: null
  })
  showExchangeDialog.value = true
}

const handleSaveExchange = async () => {
  if (!exchangeForm.from_currency || !exchangeForm.to_currency || !exchangeForm.rate) {
    ElMessage.warning('请填写完整信息')
    return
  }
  saving.value = true
  try {
    await profitConfigApi.createExchangeRate(exchangeForm)
    ElMessage.success('保存成功')
    showExchangeDialog.value = false
    loadConfigs()
  } catch (error) {
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    saving.value = false
  }
}

const handleAddPackaging = () => {
  Object.assign(packagingForm, {
    name: '',
    cost_rmb: 0.2,
    apply_scope: '',
    is_default: false
  })
  showPackagingDialog.value = true
}

const handleSavePackaging = async () => {
  if (!packagingForm.name || packagingForm.cost_rmb === null) {
    ElMessage.warning('请填写完整信息')
    return
  }
  saving.value = true
  try {
    await profitConfigApi.createPackagingTemplate(packagingForm)
    ElMessage.success('保存成功')
    showPackagingDialog.value = false
    loadConfigs()
  } catch (error) {
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    saving.value = false
  }
}

const handleAddCommission = () => {
  Object.assign(commissionForm, {
    site: 'emag_ro',
    category_or_group: '',
    commission_rate: 0.15,
    effective_from: null,
    effective_to: null
  })
  showCommissionDialog.value = true
}

const handleSaveCommission = async () => {
  if (!commissionForm.site || !commissionForm.category_or_group || commissionForm.commission_rate === null) {
    ElMessage.warning('请填写完整信息')
    return
  }
  saving.value = true
  try {
    await profitConfigApi.createCommissionConfig(commissionForm)
    ElMessage.success('保存成功')
    showCommissionDialog.value = false
    loadConfigs()
  } catch (error) {
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    saving.value = false
  }
}

const handleAddGenius = () => {
  ElMessage.info('Genius规则配置功能开发中，请稍后...')
}

const handleAddFeeTemplate = () => {
  ElMessage.info('费用模板配置功能开发中，请稍后...')
}

onMounted(() => {
  loadConfigs()
})
</script>

<style scoped>
.profit-config-container {
  padding: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>

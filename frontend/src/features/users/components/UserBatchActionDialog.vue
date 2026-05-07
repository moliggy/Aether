<template>
  <Dialog
    :model-value="open"
    title="用户批量操作"
    description="按当前选择批量启用、禁用或更新访问控制"
    size="2xl"
    persistent
    @update:model-value="handleDialogUpdate"
  >
    <div class="space-y-4">
      <div class="rounded-lg border bg-muted/30 px-3 py-3 text-sm">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div>
            <div class="font-medium text-foreground">
              影响用户：{{ impactCount }} 个
            </div>
            <div class="mt-1 text-xs text-muted-foreground">
              {{ selectAllFiltered ? '目标为当前筛选条件匹配的全部用户，执行前后端会重新解析。' : '目标为当前已勾选的用户，重复 ID 会自动去重。' }}
            </div>
          </div>
          <Badge variant="secondary">
            {{ selectAllFiltered ? '全选筛选结果' : '手动选择' }}
          </Badge>
        </div>
        <div
          v-if="previewLoading"
          class="mt-3 text-xs text-muted-foreground"
        >
          正在解析影响范围...
        </div>
        <div
          v-else-if="previewItems.length > 0"
          class="mt-3 flex flex-wrap gap-1.5"
        >
          <Badge
            v-for="item in previewItems"
            :key="item.user_id"
            variant="outline"
            class="text-[11px]"
          >
            {{ item.username }}
          </Badge>
          <span
            v-if="impactCount > previewItems.length"
            class="text-xs text-muted-foreground"
          >
            等 {{ impactCount }} 个用户
          </span>
        </div>
      </div>

      <div class="space-y-2">
        <Label class="text-sm font-medium">批量动作</Label>
        <Select v-model="selectedAction">
          <SelectTrigger class="h-10 w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="enable">
              批量启用用户
            </SelectItem>
            <SelectItem value="disable">
              批量禁用用户
            </SelectItem>
            <SelectItem value="update_access_control">
              批量设置访问控制
            </SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div
        v-if="selectedAction === 'update_access_control'"
        class="space-y-3 rounded-lg border bg-background px-3 py-3"
      >
        <div class="text-xs text-muted-foreground">
          每个字段都可独立选择“不修改 / 不限制 / 指定列表”。指定列表为空表示全部禁用。
        </div>

        <div class="space-y-2">
          <Label class="text-sm font-medium">允许的提供商</Label>
          <div class="grid gap-2 md:grid-cols-[10rem_minmax(0,1fr)]">
            <Select v-model="providerMode">
              <SelectTrigger class="h-9">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="skip">不修改</SelectItem>
                <SelectItem value="unrestricted">不限制</SelectItem>
                <SelectItem value="specific">指定列表</SelectItem>
              </SelectContent>
            </Select>
            <MultiSelect
              v-model="allowedProviders"
              :options="providerOptions"
              :disabled="providerMode !== 'specific'"
              :search-threshold="0"
              placeholder="未选择时表示全部禁用"
              empty-text="暂无可用提供商"
            />
          </div>
        </div>

        <div class="space-y-2">
          <Label class="text-sm font-medium">允许的端点</Label>
          <div class="grid gap-2 md:grid-cols-[10rem_minmax(0,1fr)]">
            <Select v-model="apiFormatMode">
              <SelectTrigger class="h-9">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="skip">不修改</SelectItem>
                <SelectItem value="unrestricted">不限制</SelectItem>
                <SelectItem value="specific">指定列表</SelectItem>
              </SelectContent>
            </Select>
            <MultiSelect
              v-model="allowedApiFormats"
              :options="apiFormatOptions"
              :disabled="apiFormatMode !== 'specific'"
              :search-threshold="0"
              placeholder="未选择时表示全部禁用"
              empty-text="暂无可用端点"
            />
          </div>
        </div>

        <div class="space-y-2">
          <Label class="text-sm font-medium">允许的模型</Label>
          <div class="grid gap-2 md:grid-cols-[10rem_minmax(0,1fr)]">
            <Select v-model="modelMode">
              <SelectTrigger class="h-9">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="skip">不修改</SelectItem>
                <SelectItem value="unrestricted">不限制</SelectItem>
                <SelectItem value="specific">指定列表</SelectItem>
              </SelectContent>
            </Select>
            <MultiSelect
              v-model="allowedModels"
              :options="modelOptions"
              :disabled="modelMode !== 'specific'"
              :search-threshold="0"
              placeholder="未选择时表示全部禁用"
              empty-text="暂无可用模型"
            />
          </div>
        </div>

        <div class="space-y-2">
          <Label class="text-sm font-medium">速率限制</Label>
          <div class="grid gap-2 md:grid-cols-[10rem_minmax(0,1fr)]">
            <Select v-model="rateLimitMode">
              <SelectTrigger class="h-9">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="skip">不修改</SelectItem>
                <SelectItem value="inherit">跟随默认</SelectItem>
                <SelectItem value="custom">指定数值</SelectItem>
              </SelectContent>
            </Select>
            <Input
              :model-value="rateLimit ?? ''"
              type="number"
              min="0"
              max="10000"
              class="h-9"
              :disabled="rateLimitMode !== 'custom'"
              placeholder="0 = 不限速"
              @update:model-value="(value) => rateLimit = parseNumberInput(value, { min: 0, max: 10000 })"
            />
          </div>
        </div>
      </div>

      <div
        v-if="lastResult"
        class="rounded-lg border bg-muted/20 px-3 py-2 text-xs text-muted-foreground"
      >
        成功 {{ lastResult.success }} 个，失败 {{ lastResult.failed }} 个
        <span v-if="lastResult.failures.length > 0">
          ：{{ lastResult.failures.slice(0, 3).map((item) => `${item.user_id} ${item.reason}`).join('；') }}
        </span>
      </div>
    </div>

    <template #footer>
      <Button
        variant="outline"
        :disabled="executing"
        @click="emit('close')"
      >
        关闭
      </Button>
      <Button
        :disabled="!canExecute"
        @click="executeBatchAction"
      >
        {{ executing ? '执行中...' : `确认执行（${impactCount}）` }}
      </Button>
    </template>
  </Dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import {
  Dialog,
  Button,
  Badge,
  Input,
  Label,
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from '@/components/ui'
import { MultiSelect } from '@/components/common'
import { useUsersStore } from '@/stores/users'
import { useToast } from '@/composables/useToast'
import { parseApiError } from '@/utils/errorParser'
import { parseNumberInput } from '@/utils/form'
import { useUserAccessControlOptions } from '@/features/users/composables/useUserAccessControlOptions'
import type {
  UserBatchAccessControlPayload,
  UserBatchAction,
  UserBatchActionResponse,
  UserBatchSelection,
  UserBatchSelectionFilters,
  UserBatchSelectionItem,
} from '@/api/users'

type AccessFieldMode = 'skip' | 'unrestricted' | 'specific'
type RateLimitMode = 'skip' | 'inherit' | 'custom'

const props = defineProps<{
  open: boolean
  selectedIds: string[]
  selectAllFiltered: boolean
  selectedCount: number
  filters: UserBatchSelectionFilters
}>()

const emit = defineEmits<{
  close: []
  completed: [result: UserBatchActionResponse]
}>()

const usersStore = useUsersStore()
const { success, warning, error } = useToast()
const {
  providerOptions,
  apiFormatOptions,
  modelOptions,
  loadAccessControlOptions,
} = useUserAccessControlOptions()

const selectedAction = ref<UserBatchAction>('enable')
const providerMode = ref<AccessFieldMode>('skip')
const apiFormatMode = ref<AccessFieldMode>('skip')
const modelMode = ref<AccessFieldMode>('skip')
const rateLimitMode = ref<RateLimitMode>('skip')
const allowedProviders = ref<string[]>([])
const allowedApiFormats = ref<string[]>([])
const allowedModels = ref<string[]>([])
const rateLimit = ref<number | undefined>(undefined)
const previewLoading = ref(false)
const previewItems = ref<UserBatchSelectionItem[]>([])
const resolvedTotal = ref<number | null>(null)
const executing = ref(false)
const lastResult = ref<UserBatchActionResponse | null>(null)

const impactCount = computed(() => resolvedTotal.value ?? props.selectedCount)
const canExecute = computed(() => props.selectedCount > 0 && !previewLoading.value && !executing.value)

watch(
  () => props.open,
  (open) => {
    if (!open) return
    resetLocalState()
    void loadAccessControlOptions().catch((err) => {
      error(parseApiError(err, '加载访问控制选项失败'))
    })
    void resolvePreview()
  },
)

watch(
  () => [props.selectedIds, props.selectAllFiltered, props.selectedCount, props.filters] as const,
  () => {
    if (props.open) void resolvePreview()
  },
)

function handleDialogUpdate(value: boolean): void {
  if (!value) emit('close')
}

function resetLocalState(): void {
  selectedAction.value = 'enable'
  providerMode.value = 'skip'
  apiFormatMode.value = 'skip'
  modelMode.value = 'skip'
  rateLimitMode.value = 'skip'
  allowedProviders.value = []
  allowedApiFormats.value = []
  allowedModels.value = []
  rateLimit.value = undefined
  lastResult.value = null
}

function buildSelection(): UserBatchSelection {
  if (props.selectAllFiltered) {
    return { filters: props.filters }
  }
  return { user_ids: [...props.selectedIds] }
}

async function resolvePreview(): Promise<void> {
  if (props.selectedCount === 0) {
    resolvedTotal.value = 0
    previewItems.value = []
    return
  }
  previewLoading.value = true
  try {
    const result = await usersStore.resolveBatchSelection(buildSelection())
    resolvedTotal.value = result.total
    previewItems.value = result.items.slice(0, 6)
  } catch (err) {
    resolvedTotal.value = props.selectedCount
    previewItems.value = []
    error(parseApiError(err, '解析用户选择失败'))
  } finally {
    previewLoading.value = false
  }
}

function buildAccessControlPayload(): UserBatchAccessControlPayload | null {
  const payload: UserBatchAccessControlPayload = {}
  if (providerMode.value === 'unrestricted') payload.allowed_providers = null
  if (providerMode.value === 'specific') payload.allowed_providers = [...allowedProviders.value]
  if (apiFormatMode.value === 'unrestricted') payload.allowed_api_formats = null
  if (apiFormatMode.value === 'specific') payload.allowed_api_formats = [...allowedApiFormats.value]
  if (modelMode.value === 'unrestricted') payload.allowed_models = null
  if (modelMode.value === 'specific') payload.allowed_models = [...allowedModels.value]
  if (rateLimitMode.value === 'inherit') payload.rate_limit = null
  if (rateLimitMode.value === 'custom' && rateLimit.value != null) payload.rate_limit = rateLimit.value
  return Object.keys(payload).length > 0 ? payload : null
}

async function executeBatchAction(): Promise<void> {
  if (!canExecute.value) return
  if (selectedAction.value === 'update_access_control' && rateLimitMode.value === 'custom' && rateLimit.value == null) {
    warning('请输入速率限制数值，0 表示不限速')
    return
  }
  const payload = selectedAction.value === 'update_access_control'
    ? buildAccessControlPayload()
    : null
  if (selectedAction.value === 'update_access_control' && payload === null) {
    warning('请至少选择一个要修改的访问控制字段')
    return
  }

  executing.value = true
  try {
    const result = await usersStore.batchAction({
      selection: buildSelection(),
      action: selectedAction.value,
      payload,
    })
    lastResult.value = result
    success(`批量操作完成：成功 ${result.success} 个，失败 ${result.failed} 个`)
    emit('completed', result)
  } catch (err) {
    error(parseApiError(err, '批量操作失败'))
  } finally {
    executing.value = false
  }
}
</script>

<template>
  <Popover v-model:open="isOpen">
    <PopoverTrigger as-child>
      <button
        type="button"
        class="flex h-9 w-9 items-center justify-center rounded-lg transition"
        :class="buttonClass"
        :title="buttonTitle"
        aria-label="版本信息"
      >
        <Info
          class="h-4 w-4"
          :class="loading ? 'animate-pulse' : ''"
        />
      </button>
    </PopoverTrigger>

    <PopoverContent
      align="end"
      side="bottom"
      :side-offset="8"
      class="w-[22rem] max-w-[calc(100vw-1rem)] overflow-hidden rounded-xl border-border/60 bg-card/95 p-0 text-card-foreground shadow-xl shadow-black/5 backdrop-blur supports-[backdrop-filter]:bg-card/90"
    >
      <div class="text-left">
        <div class="flex items-center justify-between gap-3 border-b border-border/60 bg-muted/30 px-3 py-2.5">
          <div>
            <div class="text-xs font-semibold text-foreground">
              版本信息
            </div>
            <div class="mt-0.5 text-[10px] uppercase tracking-[0.3em] text-muted-foreground">
              System version
            </div>
          </div>
          <span
            class="rounded-full border px-2 py-0.5 text-[10px] font-semibold"
            :class="statusPillClass"
          >
            {{ statusLabel }}
          </span>
        </div>

        <div class="space-y-3 px-3 py-3">
          <div class="rounded-lg border border-border/60 bg-muted/20 px-3 py-2.5">
            <div>
              <p class="text-xs text-muted-foreground">
                当前版本
              </p>
              <p class="mt-1 break-all font-mono text-sm text-foreground">
                {{ currentVersionLabel }}
              </p>
            </div>
            <div
              v-if="latestVersionLabel"
              class="mt-2"
            >
              <p class="text-xs text-muted-foreground">
                最新版本
              </p>
              <p class="mt-1 break-all font-mono text-sm text-foreground">
                {{ latestVersionLabel }}
              </p>
            </div>
          </div>

          <p
            v-if="status?.error"
            class="text-xs text-muted-foreground"
          >
            检查更新失败：{{ status.error }}
          </p>

          <div class="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              class="flex-1"
              :disabled="loading"
              @click="handleRefresh"
            >
              <RefreshCw
                class="mr-2 h-3.5 w-3.5"
                :class="loading ? 'animate-spin' : ''"
              />
              重新检查
            </Button>
            <Button
              v-if="status?.has_update && status.release_url"
              size="sm"
              class="flex-1"
              @click="handleOpenRelease"
            >
              <ExternalLink class="mr-2 h-3.5 w-3.5" />
              查看更新
            </Button>
          </div>
        </div>
      </div>
    </PopoverContent>
  </Popover>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import type { CheckUpdateResponse } from '@/api/admin'
import { Button, Popover, PopoverContent, PopoverTrigger } from '@/components/ui'
import { formatDisplayVersion } from '@/utils/version'
import { describeUpdateStatus } from '@/utils/updateStatus'
import { ExternalLink, Info, RefreshCw } from 'lucide-vue-next'

const props = defineProps<{
  status: CheckUpdateResponse | null
  loading?: boolean
}>()

const emit = defineEmits<{
  refresh: []
  openRelease: []
}>()

const isOpen = ref(false)

const loading = computed(() => props.loading ?? false)
const buttonClass = computed(() => {
  const classes = []

  if (isOpen.value) {
    classes.push('bg-muted/50')
  } else {
    classes.push('hover:bg-muted/50')
  }

  if (props.status?.has_update) {
    classes.push('text-primary')
  } else if (isOpen.value) {
    classes.push('text-foreground')
  } else {
    classes.push('text-muted-foreground hover:text-foreground')
  }

  return classes
})
const statusLabel = computed(() => describeUpdateStatus(props.status))
const currentVersionLabel = computed(() => {
  return props.status?.current_version
    ? formatDisplayVersion(props.status.current_version)
    : '加载中...'
})
const latestVersionLabel = computed(() => {
  return props.status?.latest_version
    ? formatDisplayVersion(props.status.latest_version)
    : ''
})
const statusPillClass = computed(() => {
  if (!props.status) return 'border-border/60 bg-background/70 text-muted-foreground'
  if (props.status.has_update) return 'border-primary/20 bg-primary/10 text-primary'
  if (props.status.error) return 'border-destructive/20 bg-destructive/10 text-destructive'
  return 'border-border/60 bg-background/70 text-muted-foreground'
})
const buttonTitle = computed(() => {
  if (!props.status) return '版本信息'
  return `版本信息：${statusLabel.value}`
})

function handleRefresh() {
  emit('refresh')
}

function handleOpenRelease() {
  isOpen.value = false
  emit('openRelease')
}
</script>

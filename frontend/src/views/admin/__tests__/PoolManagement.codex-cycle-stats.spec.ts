import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick, type App } from 'vue'

import PoolManagement from '@/views/admin/PoolManagement.vue'
import type { PoolKeyDetail, PoolOverviewItem, PoolKeysPageResponse } from '@/api/endpoints/pool'
import { POOL_MANAGEMENT_VIEW_STORAGE_KEY } from '@/features/pool/utils/poolManagementState'

const endpointMocks = vi.hoisted(() => ({
  getPoolOverview: vi.fn(),
  getPoolSchedulingPresets: vi.fn(),
  listPoolKeys: vi.fn(),
  clearPoolCooldown: vi.fn(),
  getProvider: vi.fn(),
  updateProvider: vi.fn(),
  revealEndpointKey: vi.fn(),
  exportKey: vi.fn(),
  deleteEndpointKey: vi.fn(),
  updateProviderKey: vi.fn(),
  refreshProviderQuota: vi.fn(),
  refreshProviderOAuth: vi.fn(),
}))

const routeMocks = vi.hoisted(() => ({
  query: {} as Record<string, string>,
  patchQuery: vi.fn((patch: Record<string, string | undefined | null>) => {
    for (const [key, value] of Object.entries(patch)) {
      if (value == null || String(value).trim() === '') {
        delete routeMocks.query[key]
      } else {
        routeMocks.query[key] = String(value)
      }
    }
  }),
}))

const proxyStoreMocks = vi.hoisted(() => ({
  ensureLoaded: vi.fn(),
}))

vi.mock('@/api/endpoints/pool', () => ({
  getPoolOverview: endpointMocks.getPoolOverview,
  getPoolSchedulingPresets: endpointMocks.getPoolSchedulingPresets,
  listPoolKeys: endpointMocks.listPoolKeys,
  clearPoolCooldown: endpointMocks.clearPoolCooldown,
}))

vi.mock('@/api/endpoints/keys', () => ({
  revealEndpointKey: endpointMocks.revealEndpointKey,
  exportKey: endpointMocks.exportKey,
  deleteEndpointKey: endpointMocks.deleteEndpointKey,
  updateProviderKey: endpointMocks.updateProviderKey,
  refreshProviderQuota: endpointMocks.refreshProviderQuota,
}))

vi.mock('@/api/endpoints/provider_oauth', () => ({
  refreshProviderOAuth: endpointMocks.refreshProviderOAuth,
}))

vi.mock('@/api/endpoints', () => ({
  getProvider: endpointMocks.getProvider,
  updateProvider: endpointMocks.updateProvider,
}))

vi.mock('@/composables/useRouteQuery', () => ({
  useRouteQuery: () => ({
    getQueryValue: (key: string) => routeMocks.query[key],
    patchQuery: routeMocks.patchQuery,
  }),
}))

vi.mock('@/stores/proxy-nodes', () => ({
  useProxyNodesStore: () => ({
    nodes: [],
    ensureLoaded: proxyStoreMocks.ensureLoaded,
  }),
}))

vi.mock('@/composables/useToast', () => ({
  useToast: () => ({
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
  }),
}))

vi.mock('@/composables/useConfirm', () => ({
  useConfirm: () => ({
    confirm: vi.fn().mockResolvedValue(true),
  }),
}))

vi.mock('@/composables/useClipboard', () => ({
  useClipboard: () => ({
    copyToClipboard: vi.fn().mockResolvedValue(undefined),
  }),
}))

vi.mock('@/composables/useCountdownTimer', async () => {
  const { ref } = await import('vue')
  return {
    useCountdownTimer: () => ({
      tick: ref(0),
      start: vi.fn(),
    }),
    getCodexResetCountdown: () => ({
      isExpired: false,
      text: '1h',
    }),
  }
})

vi.mock('lucide-vue-next', async () => {
  const { defineComponent, h } = await import('vue')
  const Icon = defineComponent({
    name: 'IconStub',
    setup() {
      return () => h('span')
    },
  })

  return {
    Search: Icon,
    Upload: Icon,
    ChevronDown: Icon,
    RefreshCw: Icon,
    Power: Icon,
    Database: Icon,
    KeyRound: Icon,
    Download: Icon,
    Copy: Icon,
    Shield: Icon,
    Globe: Icon,
    SquarePen: Icon,
    Trash2: Icon,
    Users: Icon,
    Settings2: Icon,
    SlidersHorizontal: Icon,
  }
})

vi.mock('@/components/ui', async () => {
  const { defineComponent, h } = await import('vue')
  const passthrough = (name: string, tag = 'div') => defineComponent({
    name,
    inheritAttrs: false,
    setup(_, { attrs, slots }) {
      return () => h(tag, attrs, slots.default?.())
    },
  })

  const Button = defineComponent({
    name: 'ButtonStub',
    inheritAttrs: false,
    props: {
      disabled: Boolean,
    },
    setup(props, { attrs, slots }) {
      return () => h('button', { ...attrs, disabled: props.disabled, type: attrs.type ?? 'button' }, slots.default?.())
    },
  })

  const Input = defineComponent({
    name: 'InputStub',
    inheritAttrs: false,
    props: {
      modelValue: { type: [String, Number], default: '' },
    },
    emits: ['update:modelValue'],
    setup(props, { attrs, emit }) {
      return () => h('input', {
        ...attrs,
        value: props.modelValue ?? '',
        onInput: (event: Event) => emit('update:modelValue', (event.target as HTMLInputElement).value),
      })
    },
  })

  const Switch = defineComponent({
    name: 'SwitchStub',
    inheritAttrs: false,
    props: {
      modelValue: Boolean,
    },
    emits: ['update:modelValue'],
    setup(props, { attrs, emit }) {
      return () => h('input', {
        ...attrs,
        type: 'checkbox',
        role: 'switch',
        checked: props.modelValue,
        onChange: (event: Event) => emit('update:modelValue', (event.target as HTMLInputElement).checked),
      })
    },
  })

  const Pagination = defineComponent({
    name: 'PaginationStub',
    setup() {
      return () => h('nav')
    },
  })

  return {
    Card: passthrough('CardStub'),
    Badge: passthrough('BadgeStub', 'span'),
    Button,
    Input,
    Select: passthrough('SelectStub'),
    SelectTrigger: passthrough('SelectTriggerStub', 'button'),
    SelectValue: passthrough('SelectValueStub', 'span'),
    SelectContent: passthrough('SelectContentStub'),
    SelectItem: passthrough('SelectItemStub'),
    Table: passthrough('TableStub', 'table'),
    TableHeader: passthrough('TableHeaderStub', 'thead'),
    TableBody: passthrough('TableBodyStub', 'tbody'),
    TableRow: passthrough('TableRowStub', 'tr'),
    TableHead: passthrough('TableHeadStub', 'th'),
    SortableTableHead: passthrough('SortableTableHeadStub', 'th'),
    TableFilterMenu: passthrough('TableFilterMenuStub'),
    TableCell: passthrough('TableCellStub', 'td'),
    Switch,
    Pagination,
    Popover: passthrough('PopoverStub'),
    PopoverTrigger: passthrough('PopoverTriggerStub'),
    PopoverContent: passthrough('PopoverContentStub'),
  }
})

vi.mock('@/components/ui/refresh-button.vue', async () => {
  const { defineComponent, h } = await import('vue')
  return {
    default: defineComponent({
      name: 'RefreshButtonStub',
      setup(_, { attrs }) {
        return () => h('button', attrs, '刷新')
      },
    }),
  }
})

vi.mock('@/features/pool/components/PoolSchedulingDialog.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'PoolSchedulingDialogStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/pool/components/PoolAdvancedDialog.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'PoolAdvancedDialogStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/pool/components/PoolAccountBatchDialog.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'PoolAccountBatchDialogStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/pool/components/ProviderProxyPopover.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'ProviderProxyPopoverStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/providers/components/KeyAllowedModelsEditDialog.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'KeyAllowedModelsEditDialogStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/providers/components/KeyFormDialog.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'KeyFormDialogStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/providers/components/OAuthKeyEditDialog.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'OAuthKeyEditDialogStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/providers/components/OAuthAccountDialog.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'OAuthAccountDialogStub',
      setup() {
        return () => null
      },
    }),
  }
})
vi.mock('@/features/providers/components/ProxyNodeSelect.vue', async () => {
  const { defineComponent } = await import('vue')
  return {
    default: defineComponent({
      name: 'ProxyNodeSelectStub',
      setup() {
        return () => null
      },
    }),
  }
})

const mountedApps: Array<{ app: App, root: HTMLElement }> = []

function createOverview(providerType: string): PoolOverviewItem {
  return {
    provider_id: `${providerType}-provider`,
    provider_name: `${providerType} Provider`,
    provider_type: providerType,
    total_keys: 1,
    active_keys: 1,
    cooldown_count: 0,
    pool_enabled: true,
  }
}

function createProvider(providerType: string) {
  return {
    id: `${providerType}-provider`,
    name: `${providerType} Provider`,
    provider_type: providerType,
    is_active: true,
    api_formats: ['openai:chat'],
    proxy: null,
    pool_advanced: null,
    claude_code_advanced: null,
  }
}

function createPoolKey(providerType = 'codex', overrides: Partial<PoolKeyDetail> = {}): PoolKeyDetail {
  return {
    key_id: `${providerType}-key-1`,
    key_name: `${providerType} key`,
    is_active: true,
    auth_type: 'api_key',
    api_formats: ['openai:chat'],
    internal_priority: 50,
    account_quota: null,
    cooldown_reason: null,
    cooldown_ttl_seconds: null,
    cost_window_usage: 0,
    cost_limit: null,
    request_count: 9876,
    total_tokens: 4321000,
    total_cost_usd: '8.7654',
    sticky_sessions: 0,
    lru_score: null,
    created_at: '2026-05-05T00:00:00Z',
    imported_at: '2026-05-05T00:00:00Z',
    last_used_at: '2026-05-05T01:00:00Z',
    status_snapshot: {
      oauth: { code: 'none' },
      account: { code: 'ok', blocked: false },
      quota: {
        code: 'ok',
        exhausted: false,
        provider_type: providerType,
        windows: providerType === 'codex'
          ? [
              {
                code: '5h',
                remaining_ratio: 0.8,
                usage: { request_count: 7, total_tokens: 2500, total_cost_usd: '0.0045' },
              },
              {
                code: 'weekly',
                remaining_ratio: 0.5,
                usage: { request_count: 0, total_tokens: 0, total_cost_usd: '0.00000000' },
              },
            ]
          : [],
      },
    },
    ...overrides,
  }
}

function createKeyPage(key: PoolKeyDetail): PoolKeysPageResponse {
  return {
    total: 1,
    page: 1,
    page_size: 50,
    keys: [key],
  }
}

function resetQuery() {
  for (const key of Object.keys(routeMocks.query)) {
    delete routeMocks.query[key]
  }
}

function mountPoolManagement() {
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp(PoolManagement)
  app.mount(root)
  mountedApps.push({ app, root })
  return root
}

async function settle() {
  for (let index = 0; index < 8; index += 1) {
    await Promise.resolve()
    await nextTick()
  }
}

function seedStoredStatsMode(statsMode: 'current_cycle' | 'account_total') {
  window.sessionStorage.setItem(
    POOL_MANAGEMENT_VIEW_STORAGE_KEY,
    JSON.stringify({ statsMode }),
  )
}

beforeEach(() => {
  resetQuery()
  window.sessionStorage.clear()
  routeMocks.patchQuery.mockClear()
  proxyStoreMocks.ensureLoaded.mockClear()

  endpointMocks.getPoolOverview.mockReset()
  endpointMocks.getPoolSchedulingPresets.mockReset()
  endpointMocks.listPoolKeys.mockReset()
  endpointMocks.clearPoolCooldown.mockReset()
  endpointMocks.getProvider.mockReset()
  endpointMocks.updateProvider.mockReset()
  endpointMocks.revealEndpointKey.mockReset()
  endpointMocks.exportKey.mockReset()
  endpointMocks.deleteEndpointKey.mockReset()
  endpointMocks.updateProviderKey.mockReset()
  endpointMocks.refreshProviderQuota.mockReset()
  endpointMocks.refreshProviderOAuth.mockReset()

  endpointMocks.getPoolSchedulingPresets.mockResolvedValue([])
  endpointMocks.clearPoolCooldown.mockResolvedValue({ message: 'ok' })
  endpointMocks.refreshProviderQuota.mockResolvedValue({ success: 0, failed: 0 })
})

afterEach(() => {
  for (const { app, root } of mountedApps.splice(0)) {
    app.unmount()
    root.remove()
  }
})

describe('PoolManagement Codex cycle stats mode', () => {
  it('defaults Codex providers to current-cycle groups and toggles back to account totals', async () => {
    const codexKey = createPoolKey('codex')
    endpointMocks.getPoolOverview.mockResolvedValue({ items: [createOverview('codex')] })
    endpointMocks.listPoolKeys.mockResolvedValue(createKeyPage(codexKey))
    endpointMocks.getProvider.mockResolvedValue(createProvider('codex'))

    const root = mountPoolManagement()
    await settle()

    const modeSwitch = root.querySelector<HTMLInputElement>('[data-testid="pool-stats-mode-switch"]')
    expect(modeSwitch).not.toBeNull()
    expect(modeSwitch?.checked).toBe(true)
    expect(root.querySelectorAll('[data-testid="pool-stats-cycle-group-5h"]').length).toBeGreaterThan(0)
    expect(root.querySelectorAll('[data-testid="pool-stats-cycle-group-weekly"]').length).toBeGreaterThan(0)
    expect(root.querySelector('[data-testid="pool-stats-5h-request_count"]')?.textContent?.trim()).toBe('7')
    expect(root.querySelector('[data-testid="pool-stats-weekly-total_tokens"]')?.textContent?.trim()).toBe('0')

    if (!modeSwitch) throw new Error('expected stats switch')
    modeSwitch.checked = false
    modeSwitch.dispatchEvent(new Event('change', { bubbles: true }))
    await settle()

    expect(routeMocks.query.statsMode).toBe('account_total')
    expect(window.sessionStorage.getItem(POOL_MANAGEMENT_VIEW_STORAGE_KEY)).toContain('"statsMode":"account_total"')
    expect(root.querySelector('[data-testid="pool-stats-account-total"]')).not.toBeNull()
    expect(root.textContent).toContain('9,876')
    expect(root.textContent).toContain('4.3M')
    expect(root.textContent).toContain('$8.77')
  })

  it('renders the Codex stats switch in header actions instead of a standalone mode bar', async () => {
    const codexKey = createPoolKey('codex')
    endpointMocks.getPoolOverview.mockResolvedValue({ items: [createOverview('codex')] })
    endpointMocks.listPoolKeys.mockResolvedValue(createKeyPage(codexKey))
    endpointMocks.getProvider.mockResolvedValue(createProvider('codex'))

    const root = mountPoolManagement()
    await settle()

    const desktopHeaderActions = root.querySelector('[data-testid="pool-header-actions"]')
    const mobileHeaderActions = root.querySelector('[data-testid="pool-mobile-header-actions"]')
    const modeControls = Array.from(root.querySelectorAll('[data-testid="pool-stats-mode-control"]'))

    expect(desktopHeaderActions?.querySelector('[data-testid="pool-stats-mode-control"]')).not.toBeNull()
    expect(mobileHeaderActions?.querySelector('[data-testid="pool-stats-mode-control"]')).not.toBeNull()
    expect(modeControls).toHaveLength(2)
    expect(modeControls.every(control => control.closest('[data-testid="pool-header-actions"], [data-testid="pool-mobile-header-actions"]'))).toBe(true)
    expect(desktopHeaderActions?.textContent).toContain('累计')
    expect(desktopHeaderActions?.textContent).toContain('周期')
    expect(root.textContent).not.toContain('Codex 统计模式')
    expect(root.textContent).not.toContain('当前周期显示 5H 与周窗口')
  })

  it('restores stored Codex account-total mode when the query omits statsMode', async () => {
    seedStoredStatsMode('account_total')
    const codexKey = createPoolKey('codex')
    endpointMocks.getPoolOverview.mockResolvedValue({ items: [createOverview('codex')] })
    endpointMocks.listPoolKeys.mockResolvedValue(createKeyPage(codexKey))
    endpointMocks.getProvider.mockResolvedValue(createProvider('codex'))

    const root = mountPoolManagement()
    await settle()

    const modeSwitch = root.querySelector<HTMLInputElement>('[data-testid="pool-stats-mode-switch"]')
    expect(modeSwitch).not.toBeNull()
    expect(modeSwitch?.checked).toBe(false)
    expect(root.querySelector('[data-testid="pool-stats-account-total"]')).not.toBeNull()
    expect(root.querySelector('[data-testid="pool-stats-cycle-group-5h"]')).toBeNull()
    expect(routeMocks.query.statsMode).toBe('account_total')
    expect(window.sessionStorage.getItem(POOL_MANAGEMENT_VIEW_STORAGE_KEY)).toContain('"statsMode":"account_total"')
  })

  it('lets a current-cycle statsMode query override stored Codex account-total mode', async () => {
    seedStoredStatsMode('account_total')
    routeMocks.query.statsMode = 'current_cycle'
    const codexKey = createPoolKey('codex')
    endpointMocks.getPoolOverview.mockResolvedValue({ items: [createOverview('codex')] })
    endpointMocks.listPoolKeys.mockResolvedValue(createKeyPage(codexKey))
    endpointMocks.getProvider.mockResolvedValue(createProvider('codex'))

    const root = mountPoolManagement()
    await settle()

    const modeSwitch = root.querySelector<HTMLInputElement>('[data-testid="pool-stats-mode-switch"]')
    expect(modeSwitch).not.toBeNull()
    expect(modeSwitch?.checked).toBe(true)
    expect(root.querySelector('[data-testid="pool-stats-cycle-group-5h"]')).not.toBeNull()
    expect(root.querySelector('[data-testid="pool-stats-account-total"]')).toBeNull()
    expect(routeMocks.query.statsMode).toBeUndefined()
    expect(window.sessionStorage.getItem(POOL_MANAGEMENT_VIEW_STORAGE_KEY)).toContain('"statsMode":"current_cycle"')
  })

  it('hides the stats mode switch for non-Codex providers and keeps account totals', async () => {
    const openaiKey = createPoolKey('openai', {
      request_count: 12,
      total_tokens: 3456,
      total_cost_usd: '1.25',
    })
    endpointMocks.getPoolOverview.mockResolvedValue({ items: [createOverview('openai')] })
    endpointMocks.listPoolKeys.mockResolvedValue(createKeyPage(openaiKey))
    endpointMocks.getProvider.mockResolvedValue(createProvider('openai'))

    const root = mountPoolManagement()
    await settle()

    expect(root.querySelector('[data-testid="pool-stats-mode-switch"]')).toBeNull()
    expect(root.querySelector('[data-testid="pool-stats-mode-control"]')).toBeNull()
    expect(root.querySelector('[data-testid="pool-stats-cycle-group-5h"]')).toBeNull()
    expect(root.querySelector('[data-testid="pool-stats-account-total"]')).not.toBeNull()
    expect(root.textContent).toContain('12')
    expect(root.textContent).toContain('3.5K')
    expect(root.textContent).toContain('$1.25')
  })
})

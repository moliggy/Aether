import { computed, ref } from 'vue'
import { getProvidersSummary } from '@/api/endpoints/providers'
import { getGlobalModels } from '@/api/global-models'
import { adminApi } from '@/api/admin'
import type { ProviderWithEndpointsSummary } from '@/api/endpoints/types'
import type { GlobalModelResponse } from '@/api/global-models'

export function useUserAccessControlOptions() {
  const providers = ref<ProviderWithEndpointsSummary[]>([])
  const globalModels = ref<GlobalModelResponse[]>([])
  const apiFormats = ref<Array<{ value: string; label: string }>>([])

  const providerOptions = computed(() =>
    providers.value.map((provider) => ({
      value: provider.id,
      label: provider.name,
    })),
  )
  const apiFormatOptions = computed(() =>
    apiFormats.value.map((format) => ({
      value: format.value,
      label: format.label,
    })),
  )
  const modelOptions = computed(() =>
    globalModels.value.map((model) => ({
      value: model.name,
      label: model.name,
    })),
  )

  async function loadAccessControlOptions(): Promise<void> {
    const [providersResponse, modelsData, formatsData] = await Promise.all([
      getProvidersSummary({ page_size: 9999 }),
      getGlobalModels({ limit: 1000, is_active: true }),
      adminApi.getApiFormats(),
    ])
    providers.value = providersResponse.items
    globalModels.value = modelsData.models || []
    apiFormats.value = formatsData.formats || []
  }

  return {
    providers,
    globalModels,
    apiFormats,
    providerOptions,
    apiFormatOptions,
    modelOptions,
    loadAccessControlOptions,
  }
}

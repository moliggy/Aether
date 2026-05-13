/// <reference types="vite/client" />

declare const __APP_VERSION__: string

interface Window {
  __aetherShowUpdateDialog?: () => void
  __aetherMockVersionStatus?: (hasUpdate?: boolean) => void
}

export function formatDisplayVersion(version: string): string {
  const normalized = version.trim()
  if (!normalized) return ''
  return normalized.startsWith('v') || normalized.startsWith('V') ? normalized : `v${normalized}`
}

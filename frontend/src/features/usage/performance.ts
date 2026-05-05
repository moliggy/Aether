export interface UsagePerformanceTiming {
  output_tokens?: number | null
  response_time_ms?: number | null
  first_byte_time_ms?: number | null
  is_stream?: boolean | null
  upstream_is_stream?: boolean | null
}

export function getGenerationTimeMs(timing: UsagePerformanceTiming): number | null {
  const responseTimeMs = timing.response_time_ms
  const firstByteTimeMs = timing.first_byte_time_ms

  if (responseTimeMs == null || firstByteTimeMs == null) return null
  if (!Number.isFinite(responseTimeMs) || !Number.isFinite(firstByteTimeMs)) return null
  if (responseTimeMs <= 0 || firstByteTimeMs < 0 || firstByteTimeMs >= responseTimeMs) return null

  return responseTimeMs - firstByteTimeMs
}

export function isOutputRateUsingGenerationTime(timing: UsagePerformanceTiming): boolean {
  return timing.upstream_is_stream ?? timing.is_stream ?? false
}

export function getOutputRateDurationMs(timing: UsagePerformanceTiming): number | null {
  if (isOutputRateUsingGenerationTime(timing)) {
    return getGenerationTimeMs(timing)
  }

  const responseTimeMs = timing.response_time_ms
  if (responseTimeMs == null || !Number.isFinite(responseTimeMs) || responseTimeMs <= 0) return null
  return responseTimeMs
}

export function calculateOutputRate(timing: UsagePerformanceTiming): number | null {
  const outputTokens = timing.output_tokens ?? 0
  const outputRateDurationMs = getOutputRateDurationMs(timing)

  if (!Number.isFinite(outputTokens) || outputTokens <= 0 || outputRateDurationMs == null) return null

  const outputRateSeconds = outputRateDurationMs / 1000
  if (outputRateSeconds <= 0) return null

  return outputTokens / outputRateSeconds
}

export function shouldHideOutputRate(
  outputRate: number | null,
  timing: UsagePerformanceTiming,
): boolean {
  if (!isOutputRateUsingGenerationTime(timing)) return false

  const responseTimeMs = timing.response_time_ms
  const generationTimeMs = getGenerationTimeMs(timing)

  if (outputRate == null || responseTimeMs == null || generationTimeMs == null) return false
  if (!Number.isFinite(outputRate) || !Number.isFinite(responseTimeMs) || responseTimeMs <= 0) return true

  return generationTimeMs / responseTimeMs < 0.1 && outputRate > 5000
}

export function getDisplayOutputRate(timing: UsagePerformanceTiming): number | null {
  const outputRate = calculateOutputRate(timing)
  if (shouldHideOutputRate(outputRate, timing)) return null
  return outputRate
}

export function formatDurationMs(ms: number | null | undefined, fractionDigits = 2): string {
  if (ms == null || !Number.isFinite(ms)) return '-'
  if (ms >= 1000) return `${(ms / 1000).toFixed(fractionDigits)}s`
  return `${Math.round(ms)}ms`
}

export function formatOutputRateValue(outputRate: number | null | undefined): string {
  if (outputRate == null || !Number.isFinite(outputRate)) return '-'
  if (outputRate >= 1000) return Math.round(outputRate).toLocaleString()
  if (outputRate >= 100) return `${Math.round(outputRate)}`
  if (outputRate >= 10) return outputRate.toFixed(1)
  return outputRate.toFixed(2)
}

export function formatOutputRate(outputRate: number | null | undefined): string {
  const value = formatOutputRateValue(outputRate)
  if (value === '-') return value
  return `${value} tps`
}

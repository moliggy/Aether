import type { RequestStatus, UsageRecord } from '../types'

export type TimelineFinalStatus = 'success' | 'failed' | 'streaming' | 'pending' | 'cancelled'

type RequestStatusLike = RequestStatus | string | null | undefined

function hasLegacyFailureSignal(
  record: Pick<UsageRecord, 'status_code' | 'error_message'>
): boolean {
  return (typeof record.status_code === 'number' && record.status_code >= 400) ||
    (typeof record.error_message === 'string' && record.error_message.trim().length > 0)
}

export function hasUsageFallback(
  record: Pick<UsageRecord, 'has_fallback'>
): boolean {
  return record.has_fallback === true
}

export function resolveUsageStreamModes(
  record: Pick<
    UsageRecord,
    'is_stream' | 'upstream_is_stream' | 'client_requested_stream' | 'client_is_stream'
  >
): { clientRequestedStream: boolean, upstreamStream: boolean } {
  const upstreamStream = typeof record.upstream_is_stream === 'boolean'
    ? record.upstream_is_stream
    : record.is_stream

  return {
    clientRequestedStream: typeof record.client_is_stream === 'boolean'
      ? record.client_is_stream
      : typeof record.client_requested_stream === 'boolean'
        ? record.client_requested_stream
        : upstreamStream,
    upstreamStream
  }
}

export function isUsageUpstreamStream(
  record: Pick<
    UsageRecord,
    'is_stream' | 'upstream_is_stream' | 'client_requested_stream' | 'client_is_stream'
  >
): boolean {
  return resolveUsageStreamModes(record).upstreamStream
}

export function formatUsageStreamLabel(
  record: Pick<
    UsageRecord,
    'is_stream' | 'upstream_is_stream' | 'client_requested_stream' | 'client_is_stream'
  >
): string {
  const { clientRequestedStream, upstreamStream } = resolveUsageStreamModes(record)
  const clientLabel = clientRequestedStream ? '流式' : '标准'
  const upstreamLabel = upstreamStream ? '流式' : '标准'

  if (clientRequestedStream === upstreamStream) {
    return clientLabel
  }

  return `${clientLabel} -> ${upstreamLabel}`
}

function hasTerminalSuccessStatusCode(
  record: Pick<UsageRecord, 'status_code'>
): boolean {
  return typeof record.status_code === 'number' &&
    record.status_code >= 200 &&
    record.status_code < 400
}

export function isUsageRecordFailed(
  record: Pick<UsageRecord, 'status' | 'status_code' | 'error_message'>
): boolean {
  const status = typeof record.status === 'string' ? record.status.trim().toLowerCase() : ''
  if (status) {
    if (status === 'pending' || status === 'streaming' || status === 'cancelled') {
      return false
    }
    if (status === 'completed') {
      return false
    }
    if (status === 'failed') {
      return !hasTerminalSuccessStatusCode(record)
    }
  }
  if (hasTerminalSuccessStatusCode(record)) {
    return false
  }
  if (status) {
    return status === 'failed'
  }
  return hasLegacyFailureSignal(record)
}

export function isUsageRecordSuccessful(
  record: Pick<UsageRecord, 'status' | 'status_code' | 'error_message'>
): boolean {
  const status = typeof record.status === 'string' ? record.status.trim().toLowerCase() : ''
  if (status) {
    if (status === 'completed') {
      return true
    }
    if (status === 'failed') {
      return hasTerminalSuccessStatusCode(record)
    }
    return false
  }
  if (hasTerminalSuccessStatusCode(record)) {
    return true
  }
  return !hasLegacyFailureSignal(record)
}

export function normalizeRequestStatus(status: RequestStatusLike): RequestStatus | undefined {
  const normalized = typeof status === 'string' ? status.trim().toLowerCase() : ''
  switch (normalized) {
    case 'pending':
    case 'streaming':
    case 'completed':
    case 'failed':
    case 'cancelled':
      return normalized
    default:
      return undefined
  }
}

export function resolveDisplayRequestStatus(
  record: Pick<UsageRecord, 'status' | 'first_byte_time_ms'>
): RequestStatus | undefined {
  const status = normalizeRequestStatus(record.status)
  if (status === 'streaming' && record.first_byte_time_ms == null) {
    return 'pending'
  }
  return status
}

export function mapRequestStatusToTimelineStatus(
  status: RequestStatusLike
): TimelineFinalStatus | undefined {
  switch (normalizeRequestStatus(status)) {
    case 'completed':
      return 'success'
    case 'failed':
      return 'failed'
    case 'streaming':
      return 'streaming'
    case 'pending':
      return 'pending'
    case 'cancelled':
      return 'cancelled'
    default:
      return undefined
  }
}

function normalizeTimelineFinalStatus(status: string | null | undefined): TimelineFinalStatus | undefined {
  const normalized = typeof status === 'string' ? status.trim().toLowerCase() : ''
  switch (normalized) {
    case 'success':
    case 'failed':
    case 'streaming':
    case 'pending':
    case 'cancelled':
      return normalized
    default:
      return undefined
  }
}

export function resolveTimelineFinalStatus(params: {
  hasPendingCandidates?: boolean
  traceFinalStatus?: string | null
  requestStatus?: RequestStatusLike
  statusCode?: number
}): TimelineFinalStatus {
  if (params.hasPendingCandidates) {
    return 'pending'
  }

  if (typeof params.statusCode === 'number') {
    return params.statusCode >= 200 && params.statusCode < 400 ? 'success' : 'failed'
  }

  const traceStatus = normalizeTimelineFinalStatus(params.traceFinalStatus)
  if (traceStatus) {
    return traceStatus
  }

  const requestStatus = mapRequestStatusToTimelineStatus(params.requestStatus)
  if (requestStatus) {
    return requestStatus
  }

  return 'pending'
}

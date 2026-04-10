export interface ActiveRequestDiscoverySnapshot {
  activeRequestIds: Iterable<string>
  knownRecordIds: Iterable<string>
  discoveredActiveRequestIds: Iterable<string>
}

export interface ActiveRequestDiscoveryResult {
  retainedDiscoveredActiveRequestIds: string[]
  unseenActiveRequestIds: string[]
}

export function reconcileActiveRequestDiscovery(
  snapshot: ActiveRequestDiscoverySnapshot
): ActiveRequestDiscoveryResult {
  const knownRecordIds = new Set(snapshot.knownRecordIds)
  const activeRequestIds: string[] = []
  const activeRequestIdSet = new Set<string>()

  for (const id of snapshot.activeRequestIds) {
    if (!id || activeRequestIdSet.has(id)) continue
    activeRequestIdSet.add(id)
    activeRequestIds.push(id)
  }

  const retainedDiscoveredActiveRequestIds: string[] = []
  const retainedDiscoveredSet = new Set<string>()

  for (const id of snapshot.discoveredActiveRequestIds) {
    if (!id || retainedDiscoveredSet.has(id)) continue
    if (knownRecordIds.has(id) || !activeRequestIdSet.has(id)) continue
    retainedDiscoveredSet.add(id)
    retainedDiscoveredActiveRequestIds.push(id)
  }

  const unseenActiveRequestIds = activeRequestIds.filter(
    id => !knownRecordIds.has(id) && !retainedDiscoveredSet.has(id)
  )

  return {
    retainedDiscoveredActiveRequestIds,
    unseenActiveRequestIds
  }
}

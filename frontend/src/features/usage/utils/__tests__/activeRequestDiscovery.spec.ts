import { describe, expect, it } from 'vitest'

import { reconcileActiveRequestDiscovery } from '../activeRequestDiscovery'

describe('reconcileActiveRequestDiscovery', () => {
  it('returns unseen active request ids while retaining still-pending discoveries', () => {
    const result = reconcileActiveRequestDiscovery({
      activeRequestIds: ['req-new', 'req-retained', 'req-new'],
      knownRecordIds: ['req-known'],
      discoveredActiveRequestIds: ['req-retained', 'req-stale']
    })

    expect(result).toEqual({
      retainedDiscoveredActiveRequestIds: ['req-retained'],
      unseenActiveRequestIds: ['req-new']
    })
  })

  it('drops discovered ids once they are known in the table', () => {
    const result = reconcileActiveRequestDiscovery({
      activeRequestIds: ['req-known', 'req-fresh'],
      knownRecordIds: ['req-known'],
      discoveredActiveRequestIds: ['req-known']
    })

    expect(result).toEqual({
      retainedDiscoveredActiveRequestIds: [],
      unseenActiveRequestIds: ['req-fresh']
    })
  })

  it('returns no unseen ids when every active request is already known or retained', () => {
    const result = reconcileActiveRequestDiscovery({
      activeRequestIds: ['req-known', 'req-retained'],
      knownRecordIds: ['req-known'],
      discoveredActiveRequestIds: ['req-retained']
    })

    expect(result).toEqual({
      retainedDiscoveredActiveRequestIds: ['req-retained'],
      unseenActiveRequestIds: []
    })
  })
})

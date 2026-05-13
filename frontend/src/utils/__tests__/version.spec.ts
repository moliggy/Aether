import { describe, expect, it } from 'vitest'
import { formatDisplayVersion } from '../version'

describe('formatDisplayVersion', () => {
  it('adds one v prefix to unprefixed versions', () => {
    expect(formatDisplayVersion('0.7.0-rc28')).toBe('v0.7.0-rc28')
  })

  it('keeps existing v prefixes intact', () => {
    expect(formatDisplayVersion('v0.7.0-rc28')).toBe('v0.7.0-rc28')
  })

  it('preserves git describe development suffixes', () => {
    expect(formatDisplayVersion('0.7.0-rc28-11-g63149fe2-dirty')).toBe(
      'v0.7.0-rc28-11-g63149fe2-dirty'
    )
  })
})

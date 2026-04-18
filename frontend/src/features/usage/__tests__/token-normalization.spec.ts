import { describe, expect, it } from 'vitest'

import { getCacheCreationTokens, getEffectiveInputTokens } from '../token-normalization'

describe('usage token normalization', () => {
  it('prefers explicit cache creation totals when present', () => {
    expect(getCacheCreationTokens({
      cache_creation_input_tokens: 18,
      cache_creation_ephemeral_5m_input_tokens: 5,
      cache_creation_ephemeral_1h_input_tokens: 7,
    })).toBe(18)
  })

  it('rehydrates cache creation totals from classified fields when legacy total is zero', () => {
    expect(getCacheCreationTokens({
      cache_creation_input_tokens: 0,
      cache_creation_ephemeral_5m_input_tokens: 12,
      cache_creation_ephemeral_1h_input_tokens: 8,
    })).toBe(20)
  })

  it('keeps effective input token normalization unchanged', () => {
    expect(getEffectiveInputTokens({
      input_tokens: 100,
      cache_read_input_tokens: 20,
      api_format: 'openai:chat',
    })).toBe(80)
  })
})

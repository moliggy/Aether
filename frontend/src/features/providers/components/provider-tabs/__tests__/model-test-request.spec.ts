import { describe, expect, it } from 'vitest'

import { buildDefaultModelTestRequestBody } from '../model-test-request'

describe('buildDefaultModelTestRequestBody', () => {
  it.each([
    'openai:embedding',
    'gemini:embedding',
    'jina:embedding',
    'doubao:embedding',
    '  OPENAI:EMBEDDING  ',
  ])('uses embedding input payloads for %s api formats', (apiFormat) => {
    const body = JSON.parse(buildDefaultModelTestRequestBody('text-embedding-3-small', apiFormat))

    expect(body).toEqual({
      model: 'text-embedding-3-small',
      input: 'This is a test embedding input.',
    })
    expect(body.messages).toBeUndefined()
    expect(body.stream).toBeUndefined()
  })

  it.each([
    'openai:rerank',
    'jina:rerank',
    '  JINA:RERANK  ',
  ])('uses rerank query/documents payloads for %s api formats', (apiFormat) => {
    const body = JSON.parse(buildDefaultModelTestRequestBody('bge-reranker-base', apiFormat))

    expect(body.model).toBe('bge-reranker-base')
    expect(body.query).toBe('This is a test rerank query.')
    expect(body.documents).toHaveLength(2)
    expect(body.top_n).toBe(1)
    expect(body.return_documents).toBe(true)
    expect(body.messages).toBeUndefined()
    expect(body.stream).toBeUndefined()
  })

  it('keeps chat payloads for chat api formats', () => {
    const body = JSON.parse(buildDefaultModelTestRequestBody('gpt-5.1', 'openai:chat'))

    expect(body.messages).toEqual([{ role: 'user', content: 'Hello! This is a test message.' }])
    expect(body.stream).toBe(true)
    expect(body.input).toBeUndefined()
  })
})

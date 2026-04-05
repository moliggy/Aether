/**
 * 解析器注册表和统一入口
 */

import type { ApiFormat, ApiFormatParser, ParsedConversation, StreamResponseBody } from './types'
import { createEmptyConversation, isStreamResponse } from './types'
import type { RenderResult } from './render'
import { createEmptyRenderResult } from './render'
import { claudeParser } from './claude'
import { openaiParser } from './openai'
import { geminiParser } from './gemini'

function parseRawSseResponse(responseBody: unknown): StreamResponseBody | null {
  if (typeof responseBody !== 'string') {
    return null
  }

  const text = responseBody.trim()
  if (!text.includes('data:')) {
    return null
  }

  const chunks: unknown[] = []
  const eventBlocks = text.split(/\r?\n\r?\n/)

  for (const block of eventBlocks) {
    if (!block.trim()) {
      continue
    }

    const dataLines = block
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(line => line.startsWith('data:'))
      .map(line => line.slice(5).trim())
      .filter(Boolean)

    if (dataLines.length === 0) {
      continue
    }

    const payload = dataLines.join('\n')
    if (payload === '[DONE]') {
      continue
    }

    try {
      chunks.push(JSON.parse(payload))
    } catch {
      // 保持原始数据不变，交给现有解析器兜底
    }
  }

  if (chunks.length === 0) {
    return null
  }

  return {
    metadata: {
      stream: true,
    },
    chunks,
  }
}

function normalizeResponseBody(responseBody: unknown): unknown {
  if (isStreamResponse(responseBody)) {
    return responseBody
  }

  return parseRawSseResponse(responseBody) ?? responseBody
}

/**
 * 解析器注册表
 */
class ParserRegistry {
  private parsers: ApiFormatParser[] = []

  /**
   * 注册解析器
   */
  register(parser: ApiFormatParser): void {
    this.parsers.push(parser)
  }

  /**
   * 获取所有解析器
   */
  getAll(): ApiFormatParser[] {
    return [...this.parsers]
  }

  /**
   * 根据格式获取解析器
   */
  getByFormat(format: ApiFormat): ApiFormatParser | undefined {
    return this.parsers.find(p => p.format === format)
  }

  /**
   * 检测 API 格式并返回最佳匹配的解析器
   */
  detectParser(requestBody: unknown, responseBody: unknown, hint?: string): ApiFormatParser | undefined {
    let bestParser: ApiFormatParser | undefined
    let bestScore = 0

    for (const parser of this.parsers) {
      const score = parser.detect(requestBody, responseBody, hint)
      if (score > bestScore) {
        bestScore = score
        bestParser = parser
      }
    }

    return bestScore > 0 ? bestParser : undefined
  }

  /**
   * 检测 API 格式
   */
  detectFormat(requestBody: unknown, responseBody: unknown, hint?: string): ApiFormat {
    const parser = this.detectParser(requestBody, responseBody, hint)
    return parser?.format ?? 'unknown'
  }
}

/** 全局解析器注册表 */
export const parserRegistry = new ParserRegistry()

// 注册默认解析器
parserRegistry.register(claudeParser)
parserRegistry.register(openaiParser)
parserRegistry.register(geminiParser)

/**
 * 解析请求体
 */
export function parseRequest(
  requestBody: unknown,
  responseBody?: unknown,
  formatHint?: string
): ParsedConversation {
  if (!requestBody) {
    return createEmptyConversation('unknown', '无请求体')
  }

  const parser = parserRegistry.detectParser(
    requestBody,
    normalizeResponseBody(responseBody),
    formatHint
  )
  if (!parser) {
    return createEmptyConversation('unknown', '无法识别的 API 格式')
  }

  return parser.parseRequest(requestBody)
}

/**
 * 解析响应体
 */
export function parseResponse(
  responseBody: unknown,
  requestBody?: unknown,
  formatHint?: string
): ParsedConversation {
  if (!responseBody) {
    return createEmptyConversation('unknown', '无响应体')
  }

  const normalizedResponseBody = normalizeResponseBody(responseBody)
  const parser = parserRegistry.detectParser(requestBody, normalizedResponseBody, formatHint)
  if (!parser) {
    return createEmptyConversation('unknown', '无法识别的 API 格式')
  }

  // 判断是否为流式响应
  if (isStreamResponse(normalizedResponseBody)) {
    return parser.parseStreamResponse(normalizedResponseBody.chunks || [])
  }

  return parser.parseResponse(normalizedResponseBody)
}

/**
 * 检测 API 格式
 */
export function detectApiFormat(
  requestBody: unknown,
  responseBody: unknown,
  hint?: string
): ApiFormat {
  return parserRegistry.detectFormat(requestBody, normalizeResponseBody(responseBody), hint)
}

/**
 * 渲染请求体
 */
export function renderRequest(
  requestBody: unknown,
  responseBody?: unknown,
  formatHint?: string
): RenderResult {
  if (!requestBody) {
    return createEmptyRenderResult('无请求体')
  }

  const parser = parserRegistry.detectParser(
    requestBody,
    normalizeResponseBody(responseBody),
    formatHint
  )
  if (!parser) {
    return createEmptyRenderResult('无法识别的 API 格式')
  }

  return parser.renderRequest(requestBody)
}

/**
 * 渲染响应体
 */
export function renderResponse(
  responseBody: unknown,
  requestBody?: unknown,
  formatHint?: string
): RenderResult {
  if (!responseBody) {
    return createEmptyRenderResult('无响应体')
  }

  const normalizedResponseBody = normalizeResponseBody(responseBody)
  const parser = parserRegistry.detectParser(requestBody, normalizedResponseBody, formatHint)
  if (!parser) {
    return createEmptyRenderResult('无法识别的 API 格式')
  }

  return parser.renderResponse(normalizedResponseBody)
}

// 账号级别封禁/异常的关键词匹配（用于判断 oauth_invalid_reason 是否属于账号封禁）
const ACCOUNT_BLOCK_REASON_KEYWORDS = [
  'account_block',
  'account blocked',
  'account has been disabled',
  'account disabled',
  'organization has been disabled',
  'organization_disabled',
  'validation_required',
  'verify your account',
  'suspended',
  // Kiro quota refresher 写入的确切文本
  '账户已封禁',
  // Antigravity quota refresher 写入的确切文本
  '账户访问被禁止',
  '封禁',
  '封号',
  '被封',
  '访问被禁止',
  '账号异常',
]

export function isAccountLevelBlockReason(reason: string | null | undefined): boolean {
  if (!reason) return false
  const text = reason.trim()
  if (!text) return false
  if (text.startsWith('[ACCOUNT_BLOCK]')) return true
  const lowered = text.toLowerCase()
  return ACCOUNT_BLOCK_REASON_KEYWORDS.some(keyword => lowered.includes(keyword))
}

export function cleanAccountBlockReason(reason: string): string {
  return reason.replace(/^\[ACCOUNT_BLOCK\]\s*/i, '').trim()
}

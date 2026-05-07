/**
 * Management Token API
 */

import apiClient from './client'

// ============== 类型定义 ==============

export interface ManagementToken {
  id: string
  user_id: string
  name: string
  description?: string
  token_display: string
  allowed_ips?: string[] | null
  permissions?: string[] | null
  permission_mode?: 'legacy_full' | 'full' | 'read_only' | 'custom'
  permission_summary?: string
  expires_at?: string | null
  last_used_at?: string | null
  last_used_ip?: string | null
  usage_count: number
  is_active: boolean
  created_at: string
  updated_at: string
  user?: {
    id: string
    email: string
    username: string
    role: string
  }
}

export interface CreateManagementTokenRequest {
  name: string
  description?: string
  allowed_ips?: string[]
  permissions?: string[] | null
  expires_at?: string | null
}

export interface CreateManagementTokenResponse {
  message: string
  token: string
  data: ManagementToken
}

export interface UpdateManagementTokenRequest {
  name?: string
  description?: string | null
  allowed_ips?: string[] | null
  permissions?: string[] | null
  expires_at?: string | null
  is_active?: boolean
}

export interface ManagementTokenPermissionCatalogItem {
  key: string
  scope: string
  scope_label: string
  access: 'read' | 'write' | 'admin'
  access_label: string
}

export interface ManagementTokenPermissionCatalogResponse {
  items: ManagementTokenPermissionCatalogItem[]
  all_permissions: string[]
  read_only_permissions: string[]
}

export interface ManagementTokenListResponse {
  items: ManagementToken[]
  total: number
  skip: number
  limit: number
  quota?: {
    used: number
    max: number
  }
}

// ============== 用户自助管理 API ==============

export const managementTokenApi = {
  /**
   * 列出当前用户的 Management Tokens
   */
  async listTokens(params?: {
    is_active?: boolean
    skip?: number
    limit?: number
  }): Promise<ManagementTokenListResponse> {
    const response = await apiClient.get<ManagementTokenListResponse>(
      '/api/me/management-tokens',
      { params }
    )
    return response.data
  },

  /**
   * 创建 Management Token
   */
  async createToken(
    data: CreateManagementTokenRequest
  ): Promise<CreateManagementTokenResponse> {
    const response = await apiClient.post<CreateManagementTokenResponse>(
      '/api/me/management-tokens',
      data
    )
    return response.data
  },

  /**
   * 获取 Token 详情
   */
  async getToken(tokenId: string): Promise<ManagementToken> {
    const response = await apiClient.get<ManagementToken>(
      `/api/me/management-tokens/${tokenId}`
    )
    return response.data
  },

  /**
   * 更新 Token
   */
  async updateToken(
    tokenId: string,
    data: UpdateManagementTokenRequest
  ): Promise<{ message: string; data: ManagementToken }> {
    const response = await apiClient.put<{ message: string; data: ManagementToken }>(
      `/api/me/management-tokens/${tokenId}`,
      data
    )
    return response.data
  },

  /**
   * 删除 Token
   */
  async deleteToken(tokenId: string): Promise<{ message: string }> {
    const response = await apiClient.delete<{ message: string }>(
      `/api/me/management-tokens/${tokenId}`
    )
    return response.data
  },

  /**
   * 切换 Token 状态
   */
  async toggleToken(
    tokenId: string
  ): Promise<{ message: string; data: ManagementToken }> {
    const response = await apiClient.patch<{ message: string; data: ManagementToken }>(
      `/api/me/management-tokens/${tokenId}/status`
    )
    return response.data
  },

  /**
   * 重新生成 Token
   */
  async regenerateToken(
    tokenId: string
  ): Promise<{ token: string; data: ManagementToken }> {
    const response = await apiClient.post<{ token: string; data: ManagementToken }>(
      `/api/me/management-tokens/${tokenId}/regenerate`
    )
    return response.data
  }
}

// ============== 管理员 API ==============

export const adminManagementTokenApi = {
  /**
   * 列出所有 Management Tokens（管理员）
   */
  async listAllTokens(params?: {
    user_id?: string
    is_active?: boolean
    skip?: number
    limit?: number
  }): Promise<ManagementTokenListResponse> {
    const response = await apiClient.get<ManagementTokenListResponse>(
      '/api/admin/management-tokens',
      { params }
    )
    return response.data
  },

  /**
   * 获取 Token 详情（管理员）
   */
  async getToken(tokenId: string): Promise<ManagementToken> {
    const response = await apiClient.get<ManagementToken>(
      `/api/admin/management-tokens/${tokenId}`
    )
    return response.data
  },

  /**
   * 获取可分配权限目录（管理员）
   */
  async getPermissionCatalog(): Promise<ManagementTokenPermissionCatalogResponse> {
    const response = await apiClient.get<ManagementTokenPermissionCatalogResponse>(
      '/api/admin/management-tokens/permissions/catalog'
    )
    return response.data
  },

  /**
   * 创建 Management Token（管理员）
   */
  async createToken(
    data: CreateManagementTokenRequest
  ): Promise<CreateManagementTokenResponse> {
    const response = await apiClient.post<CreateManagementTokenResponse>(
      '/api/admin/management-tokens',
      data
    )
    return response.data
  },

  /**
   * 更新 Management Token（管理员）
   */
  async updateToken(
    tokenId: string,
    data: UpdateManagementTokenRequest
  ): Promise<{ message: string; data: ManagementToken }> {
    const response = await apiClient.put<{ message: string; data: ManagementToken }>(
      `/api/admin/management-tokens/${tokenId}`,
      data
    )
    return response.data
  },

  /**
   * 删除任意 Token（管理员）
   */
  async deleteToken(tokenId: string): Promise<{ message: string }> {
    const response = await apiClient.delete<{ message: string }>(
      `/api/admin/management-tokens/${tokenId}`
    )
    return response.data
  },

  /**
   * 切换任意 Token 状态（管理员）
   */
  async toggleToken(
    tokenId: string
  ): Promise<{ message: string; data: ManagementToken }> {
    const response = await apiClient.patch<{ message: string; data: ManagementToken }>(
      `/api/admin/management-tokens/${tokenId}/status`
    )
    return response.data
  },

  /**
   * 重新生成任意 Token（管理员）
   */
  async regenerateToken(
    tokenId: string
  ): Promise<{ token: string; data: ManagementToken }> {
    const response = await apiClient.post<{ token: string; data: ManagementToken }>(
      `/api/admin/management-tokens/${tokenId}/regenerate`
    )
    return response.data
  }
}

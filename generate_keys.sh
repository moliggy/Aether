#!/usr/bin/env bash
# 生成安全密钥

set -euo pipefail

urlsafe_rand() { openssl rand -base64 "$1" | tr '+/' '-_' | tr -d '='; }

jwt_key=$(urlsafe_rand 32)
encryption_key=$(urlsafe_rand 32)

cat <<EOF

将以下内容添加到 .env 文件:

JWT_SECRET_KEY=${jwt_key}
ENCRYPTION_KEY=${encryption_key}

注意:
  - JWT_SECRET_KEY 用于用户登录 token 签名
  - ENCRYPTION_KEY 用于敏感数据加密 (如 Provider API Keys)
EOF

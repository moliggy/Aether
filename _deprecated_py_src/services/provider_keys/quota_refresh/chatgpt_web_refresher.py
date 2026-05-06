"""ChatGPT Web 生图配额刷新策略。"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy.orm import Session

from src.core.crypto import crypto_service
from src.models.database import Provider, ProviderAPIKey, ProviderEndpoint
from src.services.provider.auth import get_provider_auth
from src.services.provider.pool.account_state import (
    OAUTH_ACCOUNT_BLOCK_PREFIX,
    OAUTH_EXPIRED_PREFIX,
)
from src.services.provider_keys.quota_refresh._helpers import build_success_state_update

CHATGPT_WEB_DEFAULT_BASE_URL = "https://chatgpt.com"
CHATGPT_WEB_CONVERSATION_INIT_PATH = "/backend-api/conversation/init"
CHATGPT_WEB_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
)
CHATGPT_WEB_CLIENT_VERSION = "prod-be885abbfcfe7b1f511e88b3003d9ee44757fbad"
CHATGPT_WEB_BUILD_NUMBER = "5955942"
CHATGPT_WEB_SEC_CH_UA = '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"'


def _coerce_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _is_image_feature(value: str) -> bool:
    return value.strip().lower() in {"image_gen", "image_generation", "image_edit", "img_gen"}


def _feature_name(item: dict[str, Any]) -> str | None:
    return _text(
        item.get("feature_name")
        or item.get("featureName")
        or item.get("feature")
        or item.get("name")
    )


def _feature_number(item: dict[str, Any], *fields: str) -> float | None:
    for field in fields:
        parsed = _coerce_float(item.get(field))
        if parsed is not None:
            return parsed
    return None


def _parse_reset_at(raw: Any, observed_at: int) -> int | None:
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
        except Exception:
            parsed = _coerce_float(text)
    else:
        parsed = _coerce_float(raw)
    if parsed is None or parsed <= 0:
        return None
    if parsed > 1_000_000_000_000:
        return int(parsed / 1000)
    if parsed > 1_000_000_000:
        return int(parsed)
    return observed_at + int(parsed)


def _auth_config_from_key(key: ProviderAPIKey) -> dict[str, Any]:
    if not getattr(key, "auth_config", None):
        return {}
    try:
        decrypted = crypto_service.decrypt(key.auth_config)
        value = json.loads(decrypted)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def parse_chatgpt_web_conversation_init_response(
    data: Any,
    *,
    auth_config: dict[str, Any] | None = None,
    observed_at: int | None = None,
) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    observed_at = observed_at or int(time.time())
    limits_progress = data.get("limits_progress") or data.get("limitsProgress") or []
    if not isinstance(limits_progress, list):
        limits_progress = []
    image_item = next(
        (
            item
            for item in limits_progress
            if isinstance(item, dict)
            and _feature_name(item) is not None
            and _is_image_feature(str(_feature_name(item)))
        ),
        None,
    )
    blocked_features = [
        value.strip()
        for value in (data.get("blocked_features") or data.get("blockedFeatures") or [])
        if isinstance(value, str) and value.strip()
    ]
    image_blocked = any(_is_image_feature(value) for value in blocked_features)

    if image_item is None and not image_blocked:
        return None

    metadata: dict[str, Any] = {
        "updated_at": observed_at,
        "blocked_features": blocked_features,
        "limits_progress": limits_progress,
    }
    default_model_slug = _text(data.get("default_model_slug") or data.get("defaultModelSlug"))
    if default_model_slug:
        metadata["default_model_slug"] = default_model_slug

    auth_config = auth_config or {}
    for field in ("plan_type", "email", "account_id", "account_user_id", "user_id"):
        value = _text(data.get(field)) or _text(auth_config.get(field))
        if value:
            metadata[field] = value.lower() if field == "plan_type" else value

    if image_blocked:
        metadata["image_quota_blocked"] = True

    if isinstance(image_item, dict):
        feature_name = _feature_name(image_item)
        if feature_name:
            metadata["image_quota_feature_name"] = feature_name
        remaining = _feature_number(
            image_item,
            "remaining",
            "remaining_value",
            "remainingValue",
            "remaining_count",
            "remainingCount",
        )
        total = _feature_number(
            image_item,
            "max_value",
            "maxValue",
            "cap",
            "total",
            "limit",
            "quota",
            "usage_limit",
            "usageLimit",
        )
        used = _feature_number(
            image_item,
            "used",
            "used_value",
            "usedValue",
            "consumed",
            "current_usage",
            "currentUsage",
        )
        if used is None and total is not None and remaining is not None:
            used = max(0.0, total - remaining)
        reset_at = _parse_reset_at(
            image_item.get("reset_at")
            or image_item.get("resetAt")
            or image_item.get("next_reset_at")
            or image_item.get("nextResetAt")
            or image_item.get("reset_after")
            or image_item.get("resetAfter"),
            observed_at,
        )
        if remaining is not None:
            metadata["image_quota_remaining"] = remaining
        elif image_blocked:
            metadata["image_quota_remaining"] = 0.0
        if total is not None:
            metadata["image_quota_total"] = total
        if used is not None:
            metadata["image_quota_used"] = used
        if reset_at is not None:
            metadata["image_quota_reset_at"] = reset_at
        reset_after = _text(image_item.get("reset_after") or image_item.get("resetAfter"))
        if reset_after:
            metadata["image_quota_reset_after"] = reset_after
    elif image_blocked:
        metadata["image_quota_remaining"] = 0.0

    return metadata


def _build_headers(auth_header: str, auth_value: str, base_url: str) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        auth_header: auth_value,
        "User-Agent": CHATGPT_WEB_USER_AGENT,
        "Origin": base_url,
        "Referer": f"{base_url}/",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-CH-UA": CHATGPT_WEB_SEC_CH_UA,
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "OAI-Language": "zh-CN",
        "OAI-Client-Version": CHATGPT_WEB_CLIENT_VERSION,
        "OAI-Client-Build-Number": CHATGPT_WEB_BUILD_NUMBER,
        "X-OpenAI-Target-Path": CHATGPT_WEB_CONVERSATION_INIT_PATH,
        "X-OpenAI-Target-Route": CHATGPT_WEB_CONVERSATION_INIT_PATH,
    }


def _extract_error_message_from_response(response: httpx.Response) -> str:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            err = payload.get("error")
            if isinstance(err, dict):
                message = str(err.get("message") or "").strip()
                if message:
                    return message
            if isinstance(err, str) and err.strip():
                return err.strip()
            message = str(payload.get("message") or "").strip()
            if message:
                return message
    except Exception:
        pass
    text = str(getattr(response, "text", "") or "").strip()
    return text[:300] if text else ""


async def refresh_chatgpt_web_key_quota(
    *,
    db: Session,
    provider: Provider,
    key: ProviderAPIKey,
    endpoint: ProviderEndpoint | None,
    codex_wham_usage_url: str,
    metadata_updates: dict[str, dict],
    state_updates: dict[str, dict],
) -> dict:
    """刷新单个 ChatGPT Web Key 的生图限额信息。"""
    _ = db
    _ = codex_wham_usage_url
    if endpoint is None:
        return {
            "key_id": key.id,
            "key_name": key.name,
            "status": "error",
            "message": "找不到有效的 openai:image 端点",
        }

    auth_info = await get_provider_auth(endpoint, key)
    if auth_info:
        auth_header = auth_info.auth_header
        auth_value = auth_info.auth_value
    else:
        decrypted_key = crypto_service.decrypt(key.api_key)
        auth_header = "Authorization"
        auth_value = f"Bearer {decrypted_key}"

    base_url = str(getattr(endpoint, "base_url", "") or CHATGPT_WEB_DEFAULT_BASE_URL).strip()
    base_url = base_url.rstrip("/") or CHATGPT_WEB_DEFAULT_BASE_URL
    url = f"{base_url}{CHATGPT_WEB_CONVERSATION_INIT_PATH}"
    headers = _build_headers(auth_header, auth_value, base_url)
    body = {
        "gizmo_id": None,
        "requested_default_model": None,
        "conversation_id": None,
        "timezone_offset_min": -480,
        "system_hints": ["picture_v2"],
    }

    response: httpx.Response
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.post(url, headers=headers, json=body)

    if response.status_code != 200:
        status_code = int(response.status_code)
        err_msg = _extract_error_message_from_response(response)
        if status_code in (401, 403):
            prefix = OAUTH_EXPIRED_PREFIX if status_code == 401 else OAUTH_ACCOUNT_BLOCK_PREFIX
            detail = err_msg or (
                "ChatGPT Web Token 无效或已过期"
                if status_code == 401
                else "ChatGPT Web 账户访问受限"
            )
            state_updates[key.id] = {
                "oauth_invalid_at": datetime.now(timezone.utc),
                "oauth_invalid_reason": f"{prefix}{detail}",
            }
            return {
                "key_id": key.id,
                "key_name": key.name,
                "status": "auth_invalid" if status_code == 401 else "forbidden",
                "message": f"conversation/init 返回状态码 {status_code}{f': {err_msg}' if err_msg else ''}",
                "status_code": status_code,
            }
        return {
            "key_id": key.id,
            "key_name": key.name,
            "status": "error",
            "message": f"conversation/init 返回状态码 {status_code}{f': {err_msg}' if err_msg else ''}",
            "status_code": status_code,
        }

    try:
        data = response.json()
    except Exception:
        return {
            "key_id": key.id,
            "key_name": key.name,
            "status": "error",
            "message": "无法解析 conversation/init API 响应",
        }

    metadata = parse_chatgpt_web_conversation_init_response(
        data,
        auth_config=_auth_config_from_key(key),
        observed_at=int(time.time()),
    )
    if metadata:
        metadata_updates[key.id] = {"chatgpt_web": metadata}
        state_updates[key.id] = build_success_state_update(key)
        return {
            "key_id": key.id,
            "key_name": key.name,
            "status": "success",
            "metadata": metadata,
        }

    return {
        "key_id": key.id,
        "key_name": key.name,
        "status": "no_metadata",
        "message": "响应中未包含 ChatGPT Web 生图限额信息",
        "status_code": response.status_code,
    }

"""Pool account state helpers.

Provides a shared way to classify account-level hard-block states
from upstream metadata and OAuth invalid reasons.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

OAUTH_ACCOUNT_BLOCK_PREFIX = "[ACCOUNT_BLOCK] "

ACCOUNT_BLOCK_REASON_KEYWORDS: tuple[str, ...] = (
    "account_block",
    "account blocked",
    "account has been disabled",
    "account disabled",
    "organization has been disabled",
    "organization_disabled",
    "validation_required",
    "verify your account",
    "suspended",
    # Kiro quota refresher 写入的确切文本
    "账户已封禁",
    # Antigravity quota refresher 写入的确切文本
    "账户访问被禁止",
    "封禁",
    "封号",
    "被封",
    "访问被禁止",
    "账号异常",
)


@dataclass(frozen=True, slots=True)
class PoolAccountState:
    """Resolved account-level state for one key."""

    blocked: bool
    code: str | None = None  # account_banned / account_forbidden / account_blocked
    label: str | None = None
    reason: str | None = None


def _is_truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "y"}
    return False


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _extract_reason(source: dict[str, Any] | None, *fields: str) -> str | None:
    if not isinstance(source, dict):
        return None
    for field in fields:
        text = _clean_text(source.get(field))
        if text:
            return text
    return None


def _resolve_from_metadata(
    provider_type: str | None,
    upstream_metadata: Any,
) -> PoolAccountState | None:
    if not isinstance(upstream_metadata, dict):
        return None

    normalized_provider = str(provider_type or "").strip().lower()
    provider_bucket: dict[str, Any] | None = None
    if normalized_provider:
        maybe_bucket = upstream_metadata.get(normalized_provider)
        if isinstance(maybe_bucket, dict):
            provider_bucket = maybe_bucket

    if (
        normalized_provider == "kiro"
        and provider_bucket
        and _is_truthy_flag(provider_bucket.get("is_banned"))
    ):
        reason = _extract_reason(provider_bucket, "ban_reason", "reason", "message")
        return PoolAccountState(
            blocked=True,
            code="account_banned",
            label="账号封禁",
            reason=reason or "Kiro 账号已封禁",
        )

    if (
        normalized_provider == "antigravity"
        and provider_bucket
        and _is_truthy_flag(provider_bucket.get("is_forbidden"))
    ):
        reason = _extract_reason(provider_bucket, "forbidden_reason", "reason", "message")
        return PoolAccountState(
            blocked=True,
            code="account_forbidden",
            label="访问受限",
            reason=reason or "Antigravity 账户访问受限",
        )

    for source in (provider_bucket, upstream_metadata):
        if not isinstance(source, dict):
            continue
        if _is_truthy_flag(source.get("is_banned")):
            reason = _extract_reason(source, "ban_reason", "forbidden_reason", "reason", "message")
            return PoolAccountState(
                blocked=True,
                code="account_banned",
                label="账号封禁",
                reason=reason or "账号已封禁",
            )
        if _is_truthy_flag(source.get("is_forbidden")) or _is_truthy_flag(
            source.get("account_disabled")
        ):
            reason = _extract_reason(source, "forbidden_reason", "ban_reason", "reason", "message")
            return PoolAccountState(
                blocked=True,
                code="account_forbidden",
                label="访问受限",
                reason=reason or "账号访问受限",
            )

    return None


def _resolve_from_oauth_invalid_reason(reason: str | None) -> PoolAccountState | None:
    text = _clean_text(reason)
    if not text:
        return None

    if text.startswith(OAUTH_ACCOUNT_BLOCK_PREFIX):
        cleaned = text[len(OAUTH_ACCOUNT_BLOCK_PREFIX) :].strip()
        return PoolAccountState(
            blocked=True,
            code="account_blocked",
            label="账号异常",
            reason=cleaned or "账号异常",
        )

    lowered = text.lower()
    if any(keyword in lowered for keyword in ACCOUNT_BLOCK_REASON_KEYWORDS):
        return PoolAccountState(
            blocked=True,
            code="account_blocked",
            label="账号异常",
            reason=text,
        )

    return None


def resolve_pool_account_state(
    *,
    provider_type: str | None,
    upstream_metadata: Any,
    oauth_invalid_reason: str | None,
) -> PoolAccountState:
    """Resolve account-level hard-block state for pool scheduling."""

    from_metadata = _resolve_from_metadata(provider_type, upstream_metadata)
    if from_metadata is not None:
        return from_metadata

    from_oauth = _resolve_from_oauth_invalid_reason(oauth_invalid_reason)
    if from_oauth is not None:
        return from_oauth

    return PoolAccountState(blocked=False)


__all__ = [
    "ACCOUNT_BLOCK_REASON_KEYWORDS",
    "OAUTH_ACCOUNT_BLOCK_PREFIX",
    "PoolAccountState",
    "resolve_pool_account_state",
]

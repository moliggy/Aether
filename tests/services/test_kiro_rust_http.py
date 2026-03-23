from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

import src.services.provider.adapters.kiro.token_manager as token_manager_mod
import src.services.provider.adapters.kiro.usage as usage_mod
import src.services.provider.adapters.kiro.rust_http as kiro_rust_http_mod
import src.services.request.rust_executor_client as rust_client_mod
from src.services.provider.adapters.kiro.token_manager import (
    refresh_idc_token,
    refresh_social_token,
)
from src.services.provider.adapters.kiro.usage import fetch_kiro_usage_limits
from src.services.request.rust_executor_client import (
    RustExecutorClientError,
    RustExecutorSyncResult,
)


def _long_refresh_token() -> str:
    return "r" * 120


@pytest.mark.asyncio
async def test_fetch_kiro_usage_limits_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kiro_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={
                    "subscriptionInfo": {"subscriptionTitle": "KIRO PRO+"},
                    "usageBreakdownList": [],
                    "desktopUserInfo": {"email": "kiro@example.com"},
                },
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )

    result = await fetch_kiro_usage_limits(
        {
            "refresh_token": _long_refresh_token(),
            "access_token": "cached-access-token",
            "expires_at": int(time.time()) + 3600,
            "machine_id": "a" * 64,
            "api_region": "us-east-1",
            "kiro_version": "0.9.1",
        }
    )

    assert result["usage_data"]["desktopUserInfo"]["email"] == "kiro@example.com"

    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.method == "GET"
    assert "getUsageLimits" in plan.url
    assert plan.provider_api_format == "kiro:usage"
    assert plan.headers["Authorization"] == "Bearer cached-access-token"


@pytest.mark.asyncio
async def test_fetch_kiro_usage_limits_falls_back_to_python_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kiro_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(side_effect=RustExecutorClientError("executor down")),
    )
    fake_client = SimpleNamespace(
        get=AsyncMock(
            return_value=httpx.Response(
                status_code=200,
                request=httpx.Request("GET", "https://q.us-east-1.amazonaws.com/getUsageLimits"),
                json={
                    "subscriptionInfo": {"subscriptionTitle": "KIRO PRO"},
                    "usageBreakdownList": [],
                },
            )
        )
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(return_value=fake_client),
    )

    result = await fetch_kiro_usage_limits(
        {
            "refresh_token": _long_refresh_token(),
            "access_token": "cached-access-token",
            "expires_at": int(time.time()) + 3600,
            "machine_id": "a" * 64,
            "api_region": "us-east-1",
            "kiro_version": "0.9.1",
        }
    )

    assert result["usage_data"]["subscriptionInfo"]["subscriptionTitle"] == "KIRO PRO"
    fake_client.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_refresh_social_token_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kiro_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(time, "time", lambda: 1_000)
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={
                    "accessToken": "new-social-access-token",
                    "refreshToken": "rotated-refresh-token",
                    "expiresIn": 1800,
                },
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )

    access_token, new_cfg = await refresh_social_token(
        token_manager_mod.KiroAuthConfig.from_dict(
            {
                "auth_method": "social",
                "refresh_token": _long_refresh_token(),
                "machine_id": "b" * 64,
                "auth_region": "us-east-1",
                "kiro_version": "0.9.1",
            }
        ),
        proxy_config=None,
    )

    assert access_token == "new-social-access-token"
    assert new_cfg.refresh_token == "rotated-refresh-token"
    assert new_cfg.access_token == "new-social-access-token"
    assert new_cfg.expires_at == 2800

    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.method == "POST"
    assert plan.url == "https://prod.us-east-1.auth.desktop.kiro.dev/refreshToken"
    assert plan.provider_api_format == "kiro:social_refresh"
    assert plan.body.json_body == {"refreshToken": _long_refresh_token()}


@pytest.mark.asyncio
async def test_refresh_idc_token_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kiro_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(time, "time", lambda: 2_000)
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={
                    "accessToken": "new-idc-access-token",
                    "refreshToken": "rotated-idc-refresh-token",
                    "expiresIn": 3600,
                },
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )

    access_token, new_cfg = await refresh_idc_token(
        token_manager_mod.KiroAuthConfig.from_dict(
            {
                "auth_method": "idc",
                "refresh_token": _long_refresh_token(),
                "client_id": "client-id",
                "client_secret": "client-secret",
                "machine_id": "c" * 64,
                "auth_region": "eu-west-1",
            }
        ),
        proxy_config=None,
    )

    assert access_token == "new-idc-access-token"
    assert new_cfg.refresh_token == "rotated-idc-refresh-token"
    assert new_cfg.access_token == "new-idc-access-token"
    assert new_cfg.expires_at == 5600

    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.method == "POST"
    assert plan.url == "https://oidc.eu-west-1.amazonaws.com/token"
    assert plan.provider_api_format == "kiro:idc_refresh"
    assert plan.body.json_body == {
        "clientId": "client-id",
        "clientSecret": "client-secret",
        "refreshToken": _long_refresh_token(),
        "grantType": "refresh_token",
    }

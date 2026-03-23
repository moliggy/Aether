from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import src.services.provider.adapters.antigravity.rust_http as antigravity_rust_http_mod
import src.services.request.rust_executor_client as rust_client_mod
from src.services.provider.adapters.antigravity.client import (
    fetch_available_models,
    load_code_assist,
    onboard_user,
    parse_retry_delay,
)
from src.services.provider.adapters.antigravity.constants import (
    DAILY_BASE_URL,
    PROD_BASE_URL,
    SANDBOX_BASE_URL,
)
from src.services.request.rust_executor_client import RustExecutorSyncResult

# ---------------------------------------------------------------------------
# load_code_assist
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_code_assist_falls_back_on_500() -> None:
    """500 时 fallback 到下一个 URL（Sandbox → Daily → Prod 顺序）。"""
    resp_fail = httpx.Response(500, json={"error": {"message": "boom"}})
    resp_ok = httpx.Response(200, json={"cloudaicompanionProject": "project-1"})

    client = SimpleNamespace(post=AsyncMock(side_effect=[resp_fail, resp_ok]))

    with (
        patch(
            "src.clients.http_client.HTTPClientPool.get_proxy_client",
            AsyncMock(return_value=client),
        ),
        patch(
            "src.services.provider.adapters.antigravity.client.url_availability.get_ordered_urls",
            return_value=[SANDBOX_BASE_URL, DAILY_BASE_URL, PROD_BASE_URL],
        ),
    ):
        data = await load_code_assist("tok", proxy_config=None, timeout_seconds=1.0)

    assert data["cloudaicompanionProject"] == "project-1"
    assert client.post.await_count == 2
    assert client.post.call_args_list[0].args[0] == f"{SANDBOX_BASE_URL}/v1internal:loadCodeAssist"
    assert client.post.call_args_list[1].args[0] == f"{DAILY_BASE_URL}/v1internal:loadCodeAssist"


@pytest.mark.asyncio
async def test_load_code_assist_4xx_does_not_fallback() -> None:
    """401/403 等 4xx 客户端错误不应 fallback，直接抛出。"""
    resp_401 = httpx.Response(401, json={"error": "unauthorized"}, text="unauthorized")

    client = SimpleNamespace(post=AsyncMock(return_value=resp_401))

    with (
        patch(
            "src.clients.http_client.HTTPClientPool.get_proxy_client",
            AsyncMock(return_value=client),
        ),
        patch(
            "src.services.provider.adapters.antigravity.client.url_availability.get_ordered_urls",
            return_value=[SANDBOX_BASE_URL, DAILY_BASE_URL],
        ),
        pytest.raises(RuntimeError, match="status=401"),
    ):
        await load_code_assist("tok", proxy_config=None, timeout_seconds=1.0)

    # 只调用了一次（没有 fallback 到第二个 URL）
    assert client.post.await_count == 1


@pytest.mark.asyncio
async def test_load_code_assist_requires_token() -> None:
    with pytest.raises(ValueError):
        await load_code_assist("", proxy_config=None)


@pytest.mark.asyncio
async def test_load_code_assist_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(antigravity_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={"cloudaicompanionProject": "project-rust"},
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    monkeypatch.setattr(
        "src.services.provider.adapters.antigravity.client.url_availability.get_ordered_urls",
        lambda prefer_daily=True: [SANDBOX_BASE_URL],
    )

    data = await load_code_assist("tok", proxy_config=None, timeout_seconds=1.0)

    assert data["cloudaicompanionProject"] == "project-rust"
    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.url == f"{SANDBOX_BASE_URL}/v1internal:loadCodeAssist"
    assert plan.provider_api_format == "antigravity:load_code_assist"


# ---------------------------------------------------------------------------
# fetch_available_models
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_available_models_falls_back_on_500() -> None:
    resp_fail = httpx.Response(500, json={"error": {"message": "boom"}})
    resp_ok = httpx.Response(
        200,
        json={
            "models": {
                "claude-sonnet-4": {
                    "displayName": "Claude Sonnet 4",
                    "quotaInfo": {"remainingFraction": 0.75, "resetTime": "2024-01-15T12:00:00Z"},
                }
            }
        },
    )

    client = SimpleNamespace(post=AsyncMock(side_effect=[resp_fail, resp_ok]))

    with (
        patch(
            "src.clients.http_client.HTTPClientPool.get_proxy_client",
            AsyncMock(return_value=client),
        ),
        patch(
            "src.services.provider.adapters.antigravity.client.url_availability.get_ordered_urls",
            return_value=[DAILY_BASE_URL, PROD_BASE_URL],
        ),
    ):
        data = await fetch_available_models(
            "tok",
            project_id="project-1",
            proxy_config=None,
            timeout_seconds=1.0,
        )

    assert "models" in data
    assert client.post.await_count == 2
    assert (
        client.post.call_args_list[0].args[0] == f"{DAILY_BASE_URL}/v1internal:fetchAvailableModels"
    )
    assert (
        client.post.call_args_list[1].args[0] == f"{PROD_BASE_URL}/v1internal:fetchAvailableModels"
    )


@pytest.mark.asyncio
async def test_fetch_available_models_requires_project_id() -> None:
    with pytest.raises(ValueError):
        await fetch_available_models("tok", project_id="", proxy_config=None)


@pytest.mark.asyncio
async def test_fetch_available_models_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(antigravity_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={"models": {"claude-sonnet-4": {"displayName": "Claude Sonnet 4"}}},
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    monkeypatch.setattr(
        "src.services.provider.adapters.antigravity.client.url_availability.get_ordered_urls",
        lambda prefer_daily=True: [DAILY_BASE_URL],
    )

    data = await fetch_available_models(
        "tok",
        project_id="project-1",
        proxy_config=None,
        timeout_seconds=1.0,
    )

    assert "models" in data
    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.url == f"{DAILY_BASE_URL}/v1internal:fetchAvailableModels"
    assert plan.provider_api_format == "antigravity:fetch_available_models"
    assert plan.body.json_body == {"project": "project-1"}


@pytest.mark.asyncio
async def test_onboard_user_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(antigravity_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={
                    "done": True,
                    "response": {"cloudaicompanionProject": {"id": "project-onboard"}},
                },
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    monkeypatch.setattr(
        "src.services.provider.adapters.antigravity.client.url_availability.get_ordered_urls",
        lambda prefer_daily=True: [PROD_BASE_URL],
    )

    project_id = await onboard_user(
        "tok",
        tier_id="LEGACY",
        proxy_config=None,
        timeout_seconds=1.0,
        max_attempts=1,
    )

    assert project_id == "project-onboard"
    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.url == f"{PROD_BASE_URL}/v1internal:onboardUser"
    assert plan.provider_api_format == "antigravity:onboard_user"


# ---------------------------------------------------------------------------
# parse_retry_delay
# ---------------------------------------------------------------------------


def test_parse_retry_delay_from_retry_info() -> None:
    error_json = '{"error": {"details": [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "1.5s"}]}}'
    delay = parse_retry_delay(error_json)
    assert delay is not None
    # 1500ms + 200ms buffer = 1700ms = 1.7s
    assert 1.5 < delay < 2.0


def test_parse_retry_delay_from_quota_reset() -> None:
    error_json = '{"error": {"details": [{"metadata": {"quotaResetDelay": "200ms"}}]}}'
    delay = parse_retry_delay(error_json)
    assert delay is not None
    assert 0.3 < delay < 0.5


def test_parse_retry_delay_invalid() -> None:
    assert parse_retry_delay("not json") is None
    assert parse_retry_delay("{}") is None
    assert parse_retry_delay('{"error": {}}') is None

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

import src.services.provider.adapters.gemini_cli.rust_http as gemini_cli_rust_http_mod
import src.services.request.rust_executor_client as rust_client_mod
from src.services.provider.adapters.gemini_cli.client import (
    load_code_assist,
    onboard_user,
)
from src.services.request.rust_executor_client import (
    RustExecutorClientError,
    RustExecutorSyncResult,
)


@pytest.mark.asyncio
async def test_load_code_assist_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gemini_cli_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={"cloudaicompanionProject": "project-1"},
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )

    result = await load_code_assist("access-token", proxy_config=None, timeout_seconds=12.0)

    assert result["cloudaicompanionProject"] == "project-1"
    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.method == "POST"
    assert plan.url.endswith("/v1internal:loadCodeAssist")
    assert plan.provider_api_format == "gemini_cli:load_code_assist"
    assert plan.body.json_body == {
        "metadata": {
            "ideType": "ANTIGRAVITY",
            "platform": "PLATFORM_UNSPECIFIED",
            "pluginType": "GEMINI",
        }
    }


@pytest.mark.asyncio
async def test_load_code_assist_falls_back_to_python_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gemini_cli_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(side_effect=RustExecutorClientError("executor down")),
    )
    fake_client = SimpleNamespace(
        post=AsyncMock(
            return_value=httpx.Response(
                status_code=200,
                request=httpx.Request("POST", "https://example.invalid/v1internal:loadCodeAssist"),
                json={"cloudaicompanionProject": "project-fallback"},
            )
        )
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(return_value=fake_client),
    )

    result = await load_code_assist("access-token", proxy_config=None, timeout_seconds=12.0)

    assert result["cloudaicompanionProject"] == "project-fallback"
    fake_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_onboard_user_uses_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gemini_cli_rust_http_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=200,
                headers={"content-type": "application/json"},
                response_json={
                    "done": True,
                    "response": {"cloudaicompanionProject": {"id": "project-onboarded"}},
                },
            )
        ),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_proxy_client",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )

    project_id = await onboard_user(
        "access-token",
        tier_id="legacy",
        proxy_config=None,
        timeout_seconds=15.0,
        max_attempts=1,
    )

    assert project_id == "project-onboarded"
    plan = rust_client_mod.RustExecutorClient.execute_sync_json.await_args.args[0]
    assert plan.method == "POST"
    assert plan.url.endswith("/v1internal:onboardUser")
    assert plan.provider_api_format == "gemini_cli:onboard_user"
    assert plan.body.json_body["tierId"] == "legacy"

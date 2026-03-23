from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import src.core.api_format.capabilities as capabilities_mod
import src.services.model.upstream_fetcher as fetcher_mod
import src.services.request.rust_executor_client as rust_client_mod
from src.services.model.upstream_fetcher import fetch_models_from_endpoints
from src.services.request.rust_executor_client import (
    RustExecutorClientError,
    RustExecutorSyncResult,
)


@pytest.mark.asyncio
async def test_fetch_models_from_endpoints_uses_rust_for_openai(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fetcher_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        fetcher_mod,
        "_build_models_proxy_snapshot",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_client_kwargs",
        lambda proxy_config, timeout=30.0: {"timeout": timeout},
    )
    monkeypatch.setattr(
        capabilities_mod,
        "fetch_models_for_api_format",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    execute_sync = AsyncMock(
        return_value=RustExecutorSyncResult(
            status_code=200,
            response_json={"data": [{"id": "gpt-5", "owned_by": "openai"}]},
            headers={"content-type": "application/json"},
        )
    )
    monkeypatch.setattr(rust_client_mod.RustExecutorClient, "execute_sync_json", execute_sync)

    models, errors, ok = await fetch_models_from_endpoints(
        [
            {
                "api_key": "test-key",
                "base_url": "https://api.openai.com",
                "api_format": "openai:chat",
                "extra_headers": None,
            }
        ],
        timeout=1.0,
    )

    assert ok is True
    assert errors == []
    assert [m.get("id") for m in models] == ["gpt-5"]
    plan = execute_sync.await_args.args[0]
    assert plan.method == "GET"
    assert plan.url == "https://api.openai.com/v1/models"
    assert plan.provider_api_format == "openai:chat"


@pytest.mark.asyncio
async def test_fetch_models_from_endpoints_uses_rust_for_claude_paginated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fetcher_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        fetcher_mod,
        "_build_models_proxy_snapshot",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_client_kwargs",
        lambda proxy_config, timeout=30.0: {"timeout": timeout},
    )
    monkeypatch.setattr(
        capabilities_mod,
        "fetch_models_for_api_format",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    execute_sync = AsyncMock(
        side_effect=[
            RustExecutorSyncResult(
                status_code=200,
                response_json={
                    "data": [{"id": "claude-sonnet-4"}],
                    "has_more": True,
                    "last_id": "claude-sonnet-4",
                },
                headers={"content-type": "application/json"},
            ),
            RustExecutorSyncResult(
                status_code=200,
                response_json={
                    "data": [{"id": "claude-opus-4"}],
                    "has_more": False,
                },
                headers={"content-type": "application/json"},
            ),
        ]
    )
    monkeypatch.setattr(rust_client_mod.RustExecutorClient, "execute_sync_json", execute_sync)

    models, errors, ok = await fetch_models_from_endpoints(
        [
            {
                "api_key": "test-key",
                "base_url": "https://api.anthropic.com",
                "api_format": "claude:chat",
                "extra_headers": None,
            }
        ],
        timeout=1.0,
    )

    assert ok is True
    assert errors == []
    assert [m.get("id") for m in models] == ["claude-sonnet-4", "claude-opus-4"]
    assert execute_sync.await_count == 2
    first_plan = execute_sync.await_args_list[0].args[0]
    second_plan = execute_sync.await_args_list[1].args[0]
    assert first_plan.url == "https://api.anthropic.com/v1/models?limit=100"
    assert (
        second_plan.url == "https://api.anthropic.com/v1/models?limit=100&after_id=claude-sonnet-4"
    )


@pytest.mark.asyncio
async def test_fetch_models_from_endpoints_uses_rust_for_gemini(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fetcher_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        fetcher_mod,
        "_build_models_proxy_snapshot",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_client_kwargs",
        lambda proxy_config, timeout=30.0: {"timeout": timeout},
    )
    monkeypatch.setattr(
        capabilities_mod,
        "fetch_models_for_api_format",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    execute_sync = AsyncMock(
        return_value=RustExecutorSyncResult(
            status_code=200,
            response_json={
                "models": [
                    {
                        "name": "models/gemini-2.5-pro",
                        "displayName": "Gemini 2.5 Pro",
                    }
                ]
            },
            headers={"content-type": "application/json"},
        )
    )
    monkeypatch.setattr(rust_client_mod.RustExecutorClient, "execute_sync_json", execute_sync)

    models, errors, ok = await fetch_models_from_endpoints(
        [
            {
                "api_key": "test-key",
                "base_url": "https://generativelanguage.googleapis.com",
                "api_format": "gemini:chat",
                "extra_headers": None,
            }
        ],
        timeout=1.0,
    )

    assert ok is True
    assert errors == []
    assert models == [
        {
            "id": "gemini-2.5-pro",
            "owned_by": "google",
            "display_name": "Gemini 2.5 Pro",
            "api_format": "gemini:chat",
        }
    ]
    plan = execute_sync.await_args.args[0]
    assert plan.url == "https://generativelanguage.googleapis.com/v1beta/models?key=test-key"


@pytest.mark.asyncio
async def test_fetch_models_from_endpoints_falls_back_to_python_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fetcher_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        fetcher_mod,
        "_build_models_proxy_snapshot",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_client_kwargs",
        lambda proxy_config, timeout=30.0: {"timeout": timeout},
    )
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(side_effect=RustExecutorClientError("executor down")),
    )
    python_fetch = AsyncMock(return_value=([{"id": "fallback-model"}], None))
    monkeypatch.setattr(capabilities_mod, "fetch_models_for_api_format", python_fetch)

    models, errors, ok = await fetch_models_from_endpoints(
        [
            {
                "api_key": "test-key",
                "base_url": "https://api.openai.com",
                "api_format": "openai:chat",
                "extra_headers": None,
            }
        ],
        timeout=1.0,
    )

    assert ok is True
    assert errors == []
    assert models == [{"id": "fallback-model", "api_format": "openai:chat"}]
    python_fetch.assert_awaited_once()

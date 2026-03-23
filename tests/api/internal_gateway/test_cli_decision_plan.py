"""Gateway internal decision/plan builder tests."""

import asyncio
import base64
import json
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.api.internal.gateway import (
    CONTROL_ACTION_HEADER,
    CONTROL_ACTION_PROXY_PUBLIC,
    CONTROL_EXECUTED_HEADER,
    GatewayAuthContext,
    GatewayExecutionDecisionResponse,
    GatewayExecuteRequest,
    GatewayExecutionPlanResponse,
    GatewayResolveRequest,
    GatewayStreamReportRequest,
    GatewaySyncReportRequest,
    _dispatch_gateway_sync_telemetry,
    _build_gateway_sync_error_payload,
    _build_gateway_sync_telemetry_writer,
    _run_gateway_stream_report_background,
    _run_gateway_sync_report_background,
    _stream_executor_requires_python_rewrite,
    _build_claude_chat_sync_decision,
    _build_claude_chat_stream_decision,
    _build_claude_cli_sync_decision,
    _build_claude_cli_stream_decision,
    _build_gemini_files_download_stream_decision,
    _build_gemini_files_proxy_sync_decision,
    _build_gemini_chat_sync_decision,
    _build_gemini_chat_stream_decision,
    _build_gemini_cli_sync_decision,
    _build_gemini_cli_stream_decision,
    _build_openai_chat_sync_decision,
    _build_openai_chat_stream_decision,
    _build_openai_cli_stream_decision,
    _build_openai_video_content_stream_decision,
    _extract_gateway_sync_error_message,
    _record_gateway_direct_candidate_graph,
    _resolve_gateway_sync_error_status_code,
    _build_openai_chat_sync_plan,
    _build_openai_cli_sync_decision,
    _build_openai_cli_stream_plan,
    _build_openai_cli_sync_plan,
    _is_streaming_sync_payload,
    _resolve_auth_context,
    _resolve_gateway_sync_adapter,
    classify_gateway_route,
    router,
)
from src.database import get_db
from src.models.database import Base, RequestCandidate
from src.services.orchestration.candidate_resolver import CandidateResolver
from src.services.scheduling.schemas import PoolCandidate, ProviderCandidate
from src.services.request.executor_plan import ExecutionPlan, ExecutionPlanBody, PreparedExecutionPlan


def _wait_until(predicate: Any, *, timeout: float = 1.0, interval: float = 0.01) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()


async def test_build_openai_cli_sync_decision_allows_upstream_stream_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-cli-upstream-stream-decision-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello"}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-cli-upstream-stream-decision-123",
            name="openai",
            provider_type="openai",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(id="endpoint-cli-upstream-stream-decision-123", api_format="openai:cli"),
        key=SimpleNamespace(id="key-cli-upstream-stream-decision-123", proxy=None),
        mapping_matched_model="gpt-5",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-cli-upstream-stream-decision-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.openai.example/v1/responses",
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "accept": "text/event-stream",
        },
        payload={
            "model": "gpt-5",
            "input": "hello",
            "stream": True,
            "prompt_cache_key": "cache-key-123",
        },
        upstream_is_stream=True,
        envelope=None,
        tls_profile=None,
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(self, body: dict[str, object], path_params: dict[str, str]) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gpt-5"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-cli-upstream-stream-decision-123"),
            SimpleNamespace(id="api-key-cli-upstream-stream-decision-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route("POST", "/v1/responses")
    auth_context = GatewayAuthContext(
        user_id="user-cli-upstream-stream-decision-123",
        api_key_id="api-key-cli-upstream-stream-decision-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello"},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_sync_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.upstream_is_stream is True
    assert result.report_kind == "openai_cli_sync_finalize"
    assert result.mapped_model == "gpt-5"
    assert result.prompt_cache_key == "cache-key-123"
    assert result.upstream_url == "https://api.openai.example/v1/responses"
    assert result.extra_headers == {}
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "accept": "text/event-stream",
    }
    assert result.provider_request_body == {
        "model": "gpt-5",
        "input": "hello",
        "stream": True,
        "prompt_cache_key": "cache-key-123",
    }


@pytest.mark.asyncio


async def test_build_openai_cli_sync_decision_uses_finalize_for_cross_format_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-cli-decision-finalize-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello"}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-cli-decision-finalize-123",
            name="gemini",
            provider_type="gemini",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-cli-decision-finalize-123",
            api_format="gemini:cli",
            base_url="https://api.gemini.example",
        ),
        key=SimpleNamespace(id="key-cli-decision-finalize-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="gemini-2.5-pro",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-cli-decision-finalize-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent",
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "x-provider-extra": "1",
        },
        payload={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
        upstream_is_stream=False,
        envelope=object(),
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gemini-2.5-pro"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-cli-decision-finalize-123"),
            SimpleNamespace(id="api-key-cli-decision-finalize-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route("POST", "/v1/responses")
    auth_context = GatewayAuthContext(
        user_id="user-cli-decision-finalize-123",
        api_key_id="api-key-cli-decision-finalize-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello"},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_sync_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_cli_sync_finalize"
    assert result.provider_api_format == "gemini:cli"
    assert result.client_api_format == "openai:cli"
    assert result.mapped_model == "gemini-2.5-pro"
    assert result.upstream_url == "https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent"
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}]
    }


@pytest.mark.asyncio


async def test_build_claude_cli_sync_decision_uses_finalize_for_cross_format_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.claude_cli import ClaudeCliAdapter

    adapter = ClaudeCliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-claude-cli-decision-finalize-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "claude-code", "messages": []}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-claude-cli-decision-finalize-123",
            name="openai",
            provider_type="openai",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-claude-cli-decision-finalize-123",
            api_format="openai:cli",
            base_url="https://api.openai.example",
        ),
        key=SimpleNamespace(id="key-claude-cli-decision-finalize-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="gpt-5.1",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-claude-cli-decision-finalize-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.openai.example/v1/responses",
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "x-provider-extra": "1",
        },
        payload={
            "model": "gpt-5.1",
            "input": "hello",
            "prompt_cache_key": "cache-key-123",
            "metadata": {"decision": "exact"},
        },
        upstream_is_stream=False,
        envelope=object(),
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "claude:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "claude-code"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gpt-5.1"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-claude-cli-decision-finalize-123"),
            SimpleNamespace(id="api-key-claude-cli-decision-finalize-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1/messages",
        {"content-type": "application/json", "authorization": "Bearer test-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-claude-cli-decision-finalize-123",
        api_key_id="api-key-claude-cli-decision-finalize-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/messages",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "claude-code", "messages": []},
        auth_context=auth_context,
    )

    result = await _build_claude_cli_sync_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "claude_cli_sync_finalize"
    assert result.provider_api_format == "openai:cli"
    assert result.client_api_format == "claude:cli"
    assert result.mapped_model == "gpt-5.1"
    assert result.upstream_url == "https://api.openai.example/v1/responses"
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "model": "gpt-5.1",
        "input": "hello",
        "prompt_cache_key": "cache-key-123",
        "metadata": {"decision": "exact"},
    }


@pytest.mark.asyncio


async def test_build_gemini_cli_sync_decision_uses_finalize_for_cross_format_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.gemini_cli import GeminiCliAdapter

    adapter = GeminiCliAdapter()
    fake_context = SimpleNamespace(
        path_params={"model": "gemini-cli"},
        request_id="req-gemini-cli-decision-finalize-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "x-goog-api-key": "client-key",
        },
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-gemini-cli-decision-finalize-123",
            name="openai",
            provider_type="openai",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-gemini-cli-decision-finalize-123",
            api_format="openai:cli",
            base_url="https://api.openai.example",
        ),
        key=SimpleNamespace(id="key-gemini-cli-decision-finalize-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="gpt-5.1",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-gemini-cli-decision-finalize-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.openai.example/v1/responses",
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "x-provider-extra": "1",
        },
        payload={
            "model": "gpt-5.1",
            "input": "hello",
            "prompt_cache_key": "cache-key-123",
            "metadata": {"decision": "exact"},
        },
        upstream_is_stream=False,
        envelope=object(),
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "gemini:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gemini-cli"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gpt-5.1"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: {**body, "model": path_params["model"]},
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {"model": "gemini-cli"}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-gemini-cli-decision-finalize-123"),
            SimpleNamespace(id="api-key-gemini-cli-decision-finalize-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1beta/models/gemini-cli:generateContent",
        {
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "x-goog-api-key": "client-key",
        },
    )
    auth_context = GatewayAuthContext(
        user_id="user-gemini-cli-decision-finalize-123",
        api_key_id="api-key-gemini-cli-decision-finalize-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1beta/models/gemini-cli:generateContent",
        headers={
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "x-goog-api-key": "client-key",
        },
        body_json={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
        auth_context=auth_context,
    )

    result = await _build_gemini_cli_sync_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "gemini_cli_sync_finalize"
    assert result.provider_api_format == "openai:cli"
    assert result.client_api_format == "gemini:cli"
    assert result.mapped_model == "gpt-5.1"
    assert result.upstream_url == "https://api.openai.example/v1/responses"
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "model": "gpt-5.1",
        "input": "hello",
        "prompt_cache_key": "cache-key-123",
        "metadata": {"decision": "exact"},
    }


@pytest.mark.asyncio


async def test_build_claude_cli_stream_decision_preserves_exact_provider_request_for_same_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.claude_cli import ClaudeCliAdapter

    adapter = ClaudeCliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-claude-cli-stream-exact-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "claude-code", "messages": [], "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-claude-cli-stream-exact-123",
            name="claude",
            provider_type="claude",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-claude-cli-stream-exact-123",
            api_format="claude:cli",
            base_url="https://api.anthropic.example",
        ),
        key=SimpleNamespace(id="key-claude-cli-stream-exact-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="claude-sonnet-4-upstream",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-claude-cli-stream-exact-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.anthropic.example/v1/messages",
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "x-provider-extra": "1",
        },
        payload={
            "model": "claude-sonnet-4-upstream",
            "messages": [],
            "stream": True,
            "metadata": {"decision": "exact"},
        },
        upstream_is_stream=True,
        envelope=None,
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "claude:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "claude-code"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "claude-sonnet-4-upstream"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-claude-cli-stream-exact-123"),
            SimpleNamespace(id="api-key-claude-cli-stream-exact-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1/messages",
        {"content-type": "application/json", "authorization": "Bearer test-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-claude-cli-stream-exact-123",
        api_key_id="api-key-claude-cli-stream-exact-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/messages",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "claude-code", "messages": [], "stream": True},
        auth_context=auth_context,
    )

    result = await _build_claude_cli_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "claude_cli_stream_success"
    assert result.provider_api_format == "claude:cli"
    assert result.client_api_format == "claude:cli"
    assert result.mapped_model == "claude-sonnet-4-upstream"
    assert result.upstream_url == "https://api.anthropic.example/v1/messages"
    assert result.tls_profile == "custom-profile"
    assert result.extra_headers == {}
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "model": "claude-sonnet-4-upstream",
        "messages": [],
        "stream": True,
        "metadata": {"decision": "exact"},
    }


@pytest.mark.asyncio


async def test_build_gemini_cli_stream_decision_preserves_exact_provider_request_for_same_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.gemini_cli import GeminiCliAdapter

    adapter = GeminiCliAdapter()
    fake_context = SimpleNamespace(
        path_params={"model": "gemini-cli", "stream": True},
        request_id="req-gemini-cli-stream-exact-123",
        client_ip="127.0.0.1",
        user_agent="GeminiCLI/1.0",
        start_time=0.0,
        original_headers={
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "x-goog-api-key": "client-key",
        },
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "stream": True,
        }
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-gemini-cli-stream-exact-123",
            name="gemini",
            provider_type="gemini",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-gemini-cli-stream-exact-123",
            api_format="gemini:cli",
            base_url="https://generativelanguage.googleapis.com",
        ),
        key=SimpleNamespace(id="key-gemini-cli-stream-exact-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="gemini-cli-upstream",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-gemini-cli-stream-exact-123",
    )
    fake_upstream_request = SimpleNamespace(
        url=(
            "https://generativelanguage.googleapis.com/"
            "v1beta/models/gemini-cli-upstream:streamGenerateContent"
        ),
        headers={
            "content-type": "application/json",
            "x-goog-api-key": "upstream-key",
            "x-provider-extra": "1",
        },
        payload={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "generationConfig": {"temperature": 0.2},
            "prompt_cache_key": "cache-key-123",
        },
        upstream_is_stream=True,
        envelope=None,
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "gemini:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gemini-cli"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gemini-cli-upstream"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: {**body, "model": path_params["model"]},
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {"model": "gemini-cli", "stream": True}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-gemini-cli-stream-exact-123"),
            SimpleNamespace(id="api-key-gemini-cli-stream-exact-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1beta/models/gemini-cli:streamGenerateContent",
        {
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "x-goog-api-key": "client-key",
        },
    )
    auth_context = GatewayAuthContext(
        user_id="user-gemini-cli-stream-exact-123",
        api_key_id="api-key-gemini-cli-stream-exact-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1beta/models/gemini-cli:streamGenerateContent",
        headers={
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "x-goog-api-key": "client-key",
        },
        body_json={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "stream": True,
        },
        auth_context=auth_context,
    )

    result = await _build_gemini_cli_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "gemini_cli_stream_success"
    assert result.provider_api_format == "gemini:cli"
    assert result.client_api_format == "gemini:cli"
    assert result.mapped_model == "gemini-cli-upstream"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/gemini-cli-upstream:streamGenerateContent"
    )
    assert result.tls_profile == "custom-profile"
    assert result.extra_headers == {}
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-goog-api-key": "upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.2},
        "prompt_cache_key": "cache-key-123",
    }


@pytest.mark.asyncio


async def test_build_openai_cli_stream_decision_allows_claude_cross_format_exact_provider_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-openai-cli-stream-claude-xfmt-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello", "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-openai-cli-stream-claude-xfmt-123",
            name="claude",
            provider_type="claude",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-openai-cli-stream-claude-xfmt-123",
            api_format="claude:cli",
            base_url="https://api.anthropic.example",
        ),
        key=SimpleNamespace(
            id="key-openai-cli-stream-claude-xfmt-123",
            proxy=None,
            api_key="enc-key",
        ),
        mapping_matched_model="claude-sonnet-4-upstream",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-openai-cli-stream-claude-xfmt-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.anthropic.example/v1/messages",
        headers={
            "content-type": "application/json",
            "x-api-key": "upstream-key",
            "accept": "text/event-stream",
            "x-provider-extra": "1",
        },
        payload={
            "model": "claude-sonnet-4-upstream",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
            "metadata": {"decision": "exact"},
        },
        upstream_is_stream=True,
        envelope=None,
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "claude-sonnet-4-upstream"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-openai-cli-stream-claude-xfmt-123"),
            SimpleNamespace(id="api-key-openai-cli-stream-claude-xfmt-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1/responses",
        {"content-type": "application/json", "authorization": "Bearer test-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-openai-cli-stream-claude-xfmt-123",
        api_key_id="api-key-openai-cli-stream-claude-xfmt-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello", "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_cli_stream_success"
    assert result.provider_api_format == "claude:cli"
    assert result.client_api_format == "openai:cli"
    assert result.mapped_model == "claude-sonnet-4-upstream"
    assert result.upstream_url == "https://api.anthropic.example/v1/messages"
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-api-key": "upstream-key",
        "accept": "text/event-stream",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "model": "claude-sonnet-4-upstream",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": True,
        "metadata": {"decision": "exact"},
    }


@pytest.mark.asyncio


async def test_build_openai_cli_stream_decision_allows_gemini_cross_format_exact_provider_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-openai-cli-stream-gemini-xfmt-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello", "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-openai-cli-stream-gemini-xfmt-123",
            name="gemini",
            provider_type="gemini",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-openai-cli-stream-gemini-xfmt-123",
            api_format="gemini:cli",
            base_url="https://generativelanguage.googleapis.com",
        ),
        key=SimpleNamespace(
            id="key-openai-cli-stream-gemini-xfmt-123",
            proxy=None,
            api_key="enc-key",
        ),
        mapping_matched_model="gemini-2.5-pro-upstream",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-openai-cli-stream-gemini-xfmt-123",
    )
    fake_upstream_request = SimpleNamespace(
        url=(
            "https://generativelanguage.googleapis.com/"
            "v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse"
        ),
        headers={
            "content-type": "application/json",
            "x-goog-api-key": "upstream-key",
            "accept": "text/event-stream",
            "x-provider-extra": "1",
        },
        payload={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "generationConfig": {"temperature": 0.2},
            "prompt_cache_key": "cache-key-123",
        },
        upstream_is_stream=True,
        envelope=None,
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gemini-2.5-pro-upstream"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-openai-cli-stream-gemini-xfmt-123"),
            SimpleNamespace(id="api-key-openai-cli-stream-gemini-xfmt-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1/responses",
        {"content-type": "application/json", "authorization": "Bearer test-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-openai-cli-stream-gemini-xfmt-123",
        api_key_id="api-key-openai-cli-stream-gemini-xfmt-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello", "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_cli_stream_success"
    assert result.provider_api_format == "gemini:cli"
    assert result.client_api_format == "openai:cli"
    assert result.mapped_model == "gemini-2.5-pro-upstream"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse"
    )
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-goog-api-key": "upstream-key",
        "accept": "text/event-stream",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.2},
        "prompt_cache_key": "cache-key-123",
    }


@pytest.mark.asyncio


async def test_build_openai_compact_stream_decision_allows_gemini_cross_format_exact_provider_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-openai-compact-stream-gemini-xfmt-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello", "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-openai-compact-stream-gemini-xfmt-123",
            name="gemini",
            provider_type="gemini",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-openai-compact-stream-gemini-xfmt-123",
            api_format="gemini:cli",
            base_url="https://generativelanguage.googleapis.com",
        ),
        key=SimpleNamespace(
            id="key-openai-compact-stream-gemini-xfmt-123",
            proxy=None,
            api_key="enc-key",
        ),
        mapping_matched_model="gemini-2.5-pro-upstream",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-openai-compact-stream-gemini-xfmt-123",
    )
    fake_upstream_request = SimpleNamespace(
        url=(
            "https://generativelanguage.googleapis.com/"
            "v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse"
        ),
        headers={
            "content-type": "application/json",
            "x-goog-api-key": "upstream-key",
            "accept": "text/event-stream",
            "x-provider-extra": "1",
        },
        payload={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "generationConfig": {"temperature": 0.2},
            "prompt_cache_key": "cache-key-123",
        },
        upstream_is_stream=True,
        envelope=None,
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "openai:compact"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gemini-2.5-pro-upstream"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-openai-compact-stream-gemini-xfmt-123"),
            SimpleNamespace(id="api-key-openai-compact-stream-gemini-xfmt-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1/responses/compact",
        {"content-type": "application/json", "authorization": "Bearer test-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-openai-compact-stream-gemini-xfmt-123",
        api_key_id="api-key-openai-compact-stream-gemini-xfmt-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses/compact",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello", "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.decision_kind == "openai_compact_stream"
    assert result.report_kind == "openai_cli_stream_success"
    assert result.provider_api_format == "gemini:cli"
    assert result.client_api_format == "openai:compact"
    assert result.mapped_model == "gemini-2.5-pro-upstream"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse"
    )
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-goog-api-key": "upstream-key",
        "accept": "text/event-stream",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.2},
        "prompt_cache_key": "cache-key-123",
    }


@pytest.mark.asyncio


async def test_build_openai_cli_stream_decision_allows_antigravity_gemini_cross_format_exact_provider_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    class AntigravityEnvelope:
        name = "antigravity:v1internal"

        def force_stream_rewrite(self) -> bool:
            return True

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-openai-cli-stream-antigravity-gemini-xfmt-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello", "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-openai-cli-stream-antigravity-gemini-xfmt-123",
            name="antigravity",
            provider_type="antigravity",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-openai-cli-stream-antigravity-gemini-xfmt-123",
            api_format="gemini:cli",
            base_url="https://generativelanguage.googleapis.com",
        ),
        key=SimpleNamespace(
            id="key-openai-cli-stream-antigravity-gemini-xfmt-123",
            proxy=None,
            api_key="enc-key",
        ),
        mapping_matched_model="claude-sonnet-4-5",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-openai-cli-stream-antigravity-gemini-xfmt-123",
    )
    fake_upstream_request = SimpleNamespace(
        url=(
            "https://generativelanguage.googleapis.com/"
            "v1beta/models/claude-sonnet-4-5:streamGenerateContent?alt=sse"
        ),
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "accept": "text/event-stream",
            "x-provider-extra": "1",
        },
        payload={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "generationConfig": {"temperature": 0.2},
        },
        upstream_is_stream=True,
        envelope=AntigravityEnvelope(),
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "claude-sonnet-4-5"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-openai-cli-stream-antigravity-gemini-xfmt-123"),
            SimpleNamespace(id="api-key-openai-cli-stream-antigravity-gemini-xfmt-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1/responses",
        {"content-type": "application/json", "authorization": "Bearer test-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-openai-cli-stream-antigravity-gemini-xfmt-123",
        api_key_id="api-key-openai-cli-stream-antigravity-gemini-xfmt-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello", "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_cli_stream_success"
    assert result.provider_api_format == "gemini:cli"
    assert result.client_api_format == "openai:cli"
    assert result.mapped_model == "claude-sonnet-4-5"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/claude-sonnet-4-5:streamGenerateContent?alt=sse"
    )
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "accept": "text/event-stream",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.2},
    }
    assert result.report_context["has_envelope"] is True
    assert result.report_context["envelope_name"] == "antigravity:v1internal"
    assert result.report_context["needs_conversion"] is True


async def test_build_gemini_cli_stream_decision_allows_antigravity_force_rewrite_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.gemini_cli import GeminiCliAdapter

    class AntigravityEnvelope:
        name = "antigravity:v1internal"

        def force_stream_rewrite(self) -> bool:
            return True

    adapter = GeminiCliAdapter()
    fake_context = SimpleNamespace(
        path_params={"model": "gemini-cli", "stream": True},
        request_id="req-antigravity-gemini-cli-stream-exact-123",
        client_ip="127.0.0.1",
        user_agent="GeminiCLI/1.0",
        start_time=0.0,
        original_headers={
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "authorization": "Bearer client-key",
        },
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "stream": True,
        }
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-antigravity-gemini-cli-stream-exact-123",
            name="antigravity",
            provider_type="antigravity",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-antigravity-gemini-cli-stream-exact-123",
            api_format="gemini:cli",
            base_url="https://generativelanguage.googleapis.com",
        ),
        key=SimpleNamespace(
            id="key-antigravity-gemini-cli-stream-exact-123",
            proxy=None,
            api_key="enc-key",
        ),
        mapping_matched_model="claude-sonnet-4-5",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-antigravity-gemini-cli-stream-exact-123",
    )
    fake_upstream_request = SimpleNamespace(
        url=(
            "https://generativelanguage.googleapis.com/"
            "v1beta/models/claude-sonnet-4-5:streamGenerateContent"
        ),
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "x-provider-extra": "1",
        },
        payload={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "generationConfig": {"temperature": 0.2},
        },
        upstream_is_stream=True,
        envelope=AntigravityEnvelope(),
        tls_profile="custom-profile",
    )

    class FakeCliHandler:
        primary_api_format = "gemini:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gemini-cli"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "claude-sonnet-4-5"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: {**body, "model": path_params["model"]},
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {"model": "gemini-cli", "stream": True}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-antigravity-gemini-cli-stream-exact-123"),
            SimpleNamespace(id="api-key-antigravity-gemini-cli-stream-exact-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route(
        "POST",
        "/v1beta/models/gemini-cli:streamGenerateContent",
        {
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "authorization": "Bearer client-key",
        },
    )
    auth_context = GatewayAuthContext(
        user_id="user-antigravity-gemini-cli-stream-exact-123",
        api_key_id="api-key-antigravity-gemini-cli-stream-exact-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1beta/models/gemini-cli:streamGenerateContent",
        headers={
            "content-type": "application/json",
            "user-agent": "GeminiCLI/1.0",
            "authorization": "Bearer client-key",
        },
        body_json={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "stream": True,
        },
        auth_context=auth_context,
    )

    result = await _build_gemini_cli_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "gemini_cli_stream_success"
    assert result.provider_api_format == "gemini:cli"
    assert result.client_api_format == "gemini:cli"
    assert result.mapped_model == "claude-sonnet-4-5"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/claude-sonnet-4-5:streamGenerateContent"
    )
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.2},
    }


@pytest.mark.asyncio


async def test_build_openai_cli_sync_plan_uses_finalize_for_cross_format_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-cli-finalize-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello"}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-cli-finalize-123",
            name="gemini",
            provider_type="gemini",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(id="endpoint-cli-finalize-123", api_format="gemini:cli"),
        key=SimpleNamespace(id="key-cli-finalize-123", proxy=None),
        mapping_matched_model="gemini-2.5-pro",
        needs_conversion=True,
        output_limit=None,
        request_candidate_id="cand-cli-finalize-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent",
        headers={"content-type": "application/json"},
        payload={"contents": []},
        upstream_is_stream=False,
        envelope=object(),
        tls_profile=None,
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(self, body: dict[str, object], path_params: dict[str, str]) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gemini-2.5-pro"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-cli-finalize-123"),
            SimpleNamespace(id="api-key-cli-finalize-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route("POST", "/v1/responses")
    auth_context = GatewayAuthContext(
        user_id="user-cli-finalize-123",
        api_key_id="api-key-cli-finalize-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello"},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_sync_plan(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_cli_sync_finalize"
    assert result.report_context["provider_api_format"] == "gemini:cli"
    assert result.report_context["client_api_format"] == "openai:cli"
    assert result.report_context["mapped_model"] == "gemini-2.5-pro"


@pytest.mark.asyncio


async def test_build_openai_cli_sync_plan_allows_upstream_stream_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-cli-upstream-stream-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello"}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-cli-upstream-stream-123",
            name="openai",
            provider_type="openai",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(id="endpoint-cli-upstream-stream-123", api_format="openai:cli"),
        key=SimpleNamespace(id="key-cli-upstream-stream-123", proxy=None),
        mapping_matched_model="gpt-5",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-cli-upstream-stream-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.openai.example/v1/responses",
        headers={"content-type": "application/json"},
        payload={"model": "gpt-5", "input": "hello"},
        upstream_is_stream=True,
        envelope=None,
        tls_profile=None,
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gpt-5"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-cli-upstream-stream-123"),
            SimpleNamespace(id="api-key-cli-upstream-stream-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route("POST", "/v1/responses")
    auth_context = GatewayAuthContext(
        user_id="user-cli-upstream-stream-123",
        api_key_id="api-key-cli-upstream-stream-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello"},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_sync_plan(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_cli_sync_finalize"
    assert result.plan["stream"] is True


@pytest.mark.asyncio


async def test_build_openai_cli_stream_plan_allows_non_rewrite_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    class NonRewriteEnvelope:
        def force_stream_rewrite(self) -> bool:
            return False

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-cli-stream-envelope-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={"perf": None},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello", "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-cli-stream-envelope-123",
            name="claude_code",
            proxy=None,
        ),
        endpoint=SimpleNamespace(id="endpoint-cli-stream-envelope-123", api_format="openai:cli"),
        key=SimpleNamespace(id="key-cli-stream-envelope-123", proxy=None),
        mapping_matched_model="gpt-5",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-cli-stream-envelope-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.openai.example/v1/responses",
        headers={"content-type": "application/json", "accept": "text/event-stream"},
        payload={"model": "gpt-5", "input": "hello", "stream": True},
        upstream_is_stream=True,
        envelope=NonRewriteEnvelope(),
        tls_profile=None,
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gpt-5"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-cli-stream-envelope-123"),
            SimpleNamespace(id="api-key-cli-stream-envelope-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route("POST", "/v1/responses")
    auth_context = GatewayAuthContext(
        user_id="user-cli-stream-envelope-123",
        api_key_id="api-key-cli-stream-envelope-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello", "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_stream_plan(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.plan_kind == "openai_cli_stream"
    assert result.report_kind == "openai_cli_stream_success"


@pytest.mark.asyncio


async def test_build_openai_compact_stream_plan_allows_non_rewrite_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    class NonRewriteEnvelope:
        def force_stream_rewrite(self) -> bool:
            return False

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-compact-stream-envelope-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={"perf": None},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello", "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-compact-stream-envelope-123",
            name="openai",
            proxy=None,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-compact-stream-envelope-123",
            api_format="openai:compact",
        ),
        key=SimpleNamespace(id="key-compact-stream-envelope-123", proxy=None),
        mapping_matched_model="gpt-5",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-compact-stream-envelope-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.openai.example/v1/responses/compact",
        headers={"content-type": "application/json", "accept": "text/event-stream"},
        payload={"model": "gpt-5", "input": "hello", "stream": True},
        upstream_is_stream=True,
        envelope=NonRewriteEnvelope(),
        tls_profile=None,
    )

    class FakeCliHandler:
        primary_api_format = "openai:compact"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gpt-5"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-compact-stream-envelope-123"),
            SimpleNamespace(id="api-key-compact-stream-envelope-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route("POST", "/v1/responses/compact")
    auth_context = GatewayAuthContext(
        user_id="user-compact-stream-envelope-123",
        api_key_id="api-key-compact-stream-envelope-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses/compact",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello", "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_stream_plan(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.plan_kind == "openai_compact_stream"
    assert result.report_kind == "openai_cli_stream_success"


@pytest.mark.asyncio


async def test_build_openai_cli_stream_plan_rejects_force_rewrite_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai_cli import OpenAICliAdapter

    class RewriteEnvelope:
        def force_stream_rewrite(self) -> bool:
            return True

    adapter = OpenAICliAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-cli-stream-rewrite-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={"perf": None},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "input": "hello", "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-cli-stream-rewrite-123",
            name="kiro",
            proxy=None,
        ),
        endpoint=SimpleNamespace(id="endpoint-cli-stream-rewrite-123", api_format="openai:cli"),
        key=SimpleNamespace(id="key-cli-stream-rewrite-123", proxy=None),
        mapping_matched_model="gpt-5",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-cli-stream-rewrite-123",
    )
    fake_upstream_request = SimpleNamespace(
        url="https://api.openai.example/v1/responses",
        headers={"content-type": "application/json", "accept": "text/event-stream"},
        payload={"model": "gpt-5", "input": "hello", "stream": True},
        upstream_is_stream=True,
        envelope=RewriteEnvelope(),
        tls_profile=None,
    )

    class FakeCliHandler:
        primary_api_format = "openai:cli"

        def __init__(self, **kwargs: object) -> None:
            pass

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _get_mapped_model(self, **kwargs: object) -> str:
            return "gpt-5"

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_upstream_request

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        type(adapter),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-cli-stream-rewrite-123"),
            SimpleNamespace(id="api-key-cli-stream-rewrite-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_request_context",
        lambda **kwargs: fake_context,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._select_gateway_direct_candidate",
        AsyncMock(return_value=fake_candidate),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_proxy_info_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.build_proxy_url_async",
        AsyncMock(return_value=None),
    )

    decision = classify_gateway_route("POST", "/v1/responses")
    auth_context = GatewayAuthContext(
        user_id="user-cli-stream-rewrite-123",
        api_key_id="api-key-cli-stream-rewrite-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/responses",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "input": "hello", "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_cli_stream_plan(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is None

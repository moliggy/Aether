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


async def test_build_openai_chat_sync_plan_uses_finalize_for_cross_format_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai import OpenAIChatAdapter

    adapter = OpenAIChatAdapter()
    fake_handler = SimpleNamespace(
        allowed_api_formats=["openai:chat"],
        _convert_request=AsyncMock(return_value={"model": "gpt-5", "messages": []}),
        extract_model_from_request=lambda body, path_params: "gpt-5",
        _resolve_capability_requirements=lambda **kwargs: None,
        _resolve_preferred_key_ids=AsyncMock(return_value=None),
    )
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-chat-finalize-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "messages": []}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(id="provider-chat-finalize-123", name="gemini"),
        endpoint=SimpleNamespace(id="endpoint-chat-finalize-123", api_format="gemini:chat"),
        key=SimpleNamespace(id="key-chat-finalize-123"),
        mapping_matched_model="gemini-2.5-pro",
        request_candidate_id="cand-chat-finalize-123",
    )
    prepared_plan = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-chat-finalize-123",
            candidate_id="cand-chat-finalize-123",
            provider_name="gemini",
            provider_id="provider-chat-finalize-123",
            endpoint_id="endpoint-chat-finalize-123",
            key_id="key-chat-finalize-123",
            method="POST",
            url="https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"contents": []}),
            stream=False,
            provider_api_format="gemini:chat",
            client_api_format="openai:chat",
            model_name="gpt-5",
        ),
        payload={"contents": []},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=True,
        provider_type="gemini",
        request_timeout=30.0,
        envelope=object(),
        proxy_info={"mode": "direct"},
    )

    class FakeChatSyncExecutor:
        def __init__(self, handler: object) -> None:
            self._ctx = SimpleNamespace(mapped_model_result="gemini-2.5-pro")

        async def _build_sync_execution_plan(self, *args: object, **kwargs: object) -> PreparedExecutionPlan:
            return prepared_plan

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        adapter,
        "_create_handler",
        lambda **kwargs: fake_handler,
        raising=False,
    )
    monkeypatch.setattr(
        adapter,
        "_validate_request_body",
        lambda body, path_params: body,
        raising=False,
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
            SimpleNamespace(id="user-chat-finalize-123"),
            SimpleNamespace(id="api-key-chat-finalize-123"),
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
        "src.api.handlers.base.chat_sync_executor.ChatSyncExecutor",
        FakeChatSyncExecutor,
    )

    decision = classify_gateway_route("POST", "/v1/chat/completions")
    auth_context = GatewayAuthContext(
        user_id="user-chat-finalize-123",
        api_key_id="api-key-chat-finalize-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "messages": []},
        auth_context=auth_context,
    )

    result = await _build_openai_chat_sync_plan(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_chat_sync_finalize"
    assert result.report_context["provider_api_format"] == "gemini:chat"
    assert result.report_context["client_api_format"] == "openai:chat"
    assert result.report_context["mapped_model"] == "gemini-2.5-pro"


async def test_build_openai_chat_sync_decision_allows_upstream_stream_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai import OpenAIChatAdapter

    adapter = OpenAIChatAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-chat-upstream-stream-decision-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "messages": []}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-chat-upstream-stream-decision-123",
            name="openai",
            provider_type="openai",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(id="endpoint-chat-upstream-stream-decision-123", api_format="openai:chat"),
        key=SimpleNamespace(id="key-chat-upstream-stream-decision-123", proxy=None),
        mapping_matched_model="gpt-5",
        request_candidate_id="cand-chat-upstream-stream-decision-123",
    )

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        adapter,
        "_create_handler",
        lambda **kwargs: SimpleNamespace(
            allowed_api_formats=["openai:chat"],
            extract_model_from_request=lambda body, path_params: "gpt-5",
            apply_mapped_model=lambda body, mapped_model: {**body, "model": mapped_model},
            _resolve_capability_requirements=lambda **kwargs: None,
            _resolve_preferred_key_ids=AsyncMock(return_value=None),
            _get_mapped_model=AsyncMock(return_value="gpt-5"),
        ),
        raising=False,
    )
    monkeypatch.setattr(
        adapter,
        "_validate_request_body",
        lambda body, path_params: body,
        raising=False,
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
            SimpleNamespace(id="user-chat-upstream-stream-decision-123"),
            SimpleNamespace(id="api-key-chat-upstream-stream-decision-123"),
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
        "src.services.provider.behavior.get_provider_behavior",
        lambda **kwargs: SimpleNamespace(
            envelope=None,
            same_format_variant=None,
            cross_format_variant=None,
        ),
    )
    monkeypatch.setattr(
        "src.services.provider.stream_policy.resolve_upstream_is_stream",
        lambda **kwargs: True,
    )
    monkeypatch.setattr(
        "src.services.provider.prompt_cache.maybe_patch_request_with_prompt_cache_key",
        lambda body, **kwargs: {**body, "prompt_cache_key": "cache-key-123"},
    )
    monkeypatch.setattr(
        "src.services.provider.auth.get_provider_auth",
        AsyncMock(
            return_value=SimpleNamespace(
                as_tuple=lambda: ("authorization", "Bearer upstream-key"),
                decrypted_auth_config=None,
            )
        ),
    )
    monkeypatch.setattr(
        "src.services.provider.upstream_headers.build_upstream_extra_headers",
        lambda **kwargs: {"x-extra-upstream": "1"},
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

    decision = classify_gateway_route("POST", "/v1/chat/completions")
    auth_context = GatewayAuthContext(
        user_id="user-chat-upstream-stream-decision-123",
        api_key_id="api-key-chat-upstream-stream-decision-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "messages": []},
        auth_context=auth_context,
    )

    result = await _build_openai_chat_sync_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.upstream_is_stream is True
    assert result.report_kind == "openai_chat_sync_finalize"
    assert result.mapped_model == "gpt-5"
    assert result.prompt_cache_key == "cache-key-123"
    assert result.extra_headers == {"x-extra-upstream": "1"}
    assert result.provider_request_headers is None
    assert result.provider_request_body is None


@pytest.mark.asyncio


async def test_build_claude_chat_sync_decision_uses_finalize_for_cross_format_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.claude import ClaudeChatAdapter

    adapter = ClaudeChatAdapter()
    fake_handler = SimpleNamespace(
        allowed_api_formats=["claude:chat"],
        _convert_request=AsyncMock(return_value={"model": "claude-sonnet-4-5", "messages": []}),
        extract_model_from_request=lambda body, path_params: "claude-sonnet-4-5",
        _resolve_capability_requirements=lambda **kwargs: None,
        _resolve_preferred_key_ids=AsyncMock(return_value=None),
    )
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-claude-chat-decision-finalize-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "x-api-key": "client-key"},
        query_params={},
        client_content_encoding=None,
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "claude-sonnet-4-5", "messages": []}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(id="provider-claude-chat-decision-finalize-123", name="gemini"),
        endpoint=SimpleNamespace(
            id="endpoint-claude-chat-decision-finalize-123",
            base_url="https://api.gemini.example",
        ),
        key=SimpleNamespace(id="key-claude-chat-decision-finalize-123", api_key="enc-key"),
        request_candidate_id="cand-claude-chat-decision-finalize-123",
    )
    prepared_plan = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-claude-chat-decision-finalize-123",
            candidate_id="cand-claude-chat-decision-finalize-123",
            provider_name="gemini",
            provider_id="provider-claude-chat-decision-finalize-123",
            endpoint_id="endpoint-claude-chat-decision-finalize-123",
            key_id="key-claude-chat-decision-finalize-123",
            method="POST",
            url="https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent",
            headers={
                "content-type": "application/json",
                "x-goog-api-key": "upstream-key",
                "x-provider-extra": "1",
            },
            body=ExecutionPlanBody(
                json_body={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}
            ),
            stream=False,
            provider_api_format="gemini:chat",
            client_api_format="claude:chat",
            model_name="claude-sonnet-4-5",
            tls_profile="custom-profile",
        ),
        payload={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
        headers={
            "content-type": "application/json",
            "x-goog-api-key": "upstream-key",
            "x-provider-extra": "1",
        },
        upstream_is_stream=False,
        needs_conversion=True,
        provider_type="gemini",
        request_timeout=30.0,
        envelope=object(),
        proxy_info={"mode": "direct"},
    )

    class FakeChatSyncExecutor:
        def __init__(self, handler: object) -> None:
            self._ctx = SimpleNamespace(mapped_model_result="gemini-2.5-pro")

        async def _build_sync_execution_plan(
            self, *args: object, **kwargs: object
        ) -> PreparedExecutionPlan:
            return prepared_plan

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: fake_handler, raising=False)
    monkeypatch.setattr(
        adapter, "_validate_request_body", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        adapter, "_merge_path_params", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-claude-chat-decision-finalize-123"),
            SimpleNamespace(id="api-key-claude-chat-decision-finalize-123"),
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
        "src.api.handlers.base.chat_sync_executor.ChatSyncExecutor",
        FakeChatSyncExecutor,
    )

    decision = classify_gateway_route("POST", "/v1/messages", {"x-api-key": "client-key"})
    auth_context = GatewayAuthContext(
        user_id="user-claude-chat-decision-finalize-123",
        api_key_id="api-key-claude-chat-decision-finalize-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/messages",
        headers={"content-type": "application/json", "x-api-key": "client-key"},
        body_json={"model": "claude-sonnet-4-5", "messages": []},
        auth_context=auth_context,
    )

    result = await _build_claude_chat_sync_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "claude_chat_sync_finalize"
    assert result.provider_api_format == "gemini:chat"
    assert result.client_api_format == "claude:chat"
    assert result.mapped_model == "gemini-2.5-pro"
    assert (
        result.upstream_url
        == "https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent"
    )
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-goog-api-key": "upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}]
    }


@pytest.mark.asyncio


async def test_build_gemini_chat_sync_decision_uses_finalize_for_cross_format_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.gemini import GeminiChatAdapter

    adapter = GeminiChatAdapter()
    fake_handler = SimpleNamespace(
        allowed_api_formats=["gemini:chat"],
        _convert_request=AsyncMock(return_value={"contents": []}),
        extract_model_from_request=lambda body, path_params: "gemini-2.5-pro",
        _resolve_capability_requirements=lambda **kwargs: None,
        _resolve_preferred_key_ids=AsyncMock(return_value=None),
    )
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-gemini-chat-decision-finalize-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "x-goog-api-key": "client-key"},
        query_params={},
        client_content_encoding=None,
    )
    fake_context.ensure_json_body_async = AsyncMock(return_value={"contents": []})
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(id="provider-gemini-chat-decision-finalize-123", name="openai"),
        endpoint=SimpleNamespace(
            id="endpoint-gemini-chat-decision-finalize-123",
            base_url="https://api.openai.example",
        ),
        key=SimpleNamespace(id="key-gemini-chat-decision-finalize-123", api_key="enc-key"),
        request_candidate_id="cand-gemini-chat-decision-finalize-123",
    )
    prepared_plan = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-gemini-chat-decision-finalize-123",
            candidate_id="cand-gemini-chat-decision-finalize-123",
            provider_name="openai",
            provider_id="provider-gemini-chat-decision-finalize-123",
            endpoint_id="endpoint-gemini-chat-decision-finalize-123",
            key_id="key-gemini-chat-decision-finalize-123",
            method="POST",
            url="https://api.openai.example/v1/chat/completions",
            headers={
                "content-type": "application/json",
                "authorization": "Bearer upstream-key",
                "x-provider-extra": "1",
            },
            body=ExecutionPlanBody(
                json_body={"model": "gpt-5.1", "messages": [{"role": "user", "content": "hello"}]}
            ),
            stream=False,
            provider_api_format="openai:chat",
            client_api_format="gemini:chat",
            model_name="gemini-2.5-pro",
            tls_profile="custom-profile",
        ),
        payload={"model": "gpt-5.1", "messages": [{"role": "user", "content": "hello"}]},
        headers={
            "content-type": "application/json",
            "authorization": "Bearer upstream-key",
            "x-provider-extra": "1",
        },
        upstream_is_stream=False,
        needs_conversion=True,
        provider_type="openai",
        request_timeout=30.0,
        envelope=object(),
        proxy_info={"mode": "direct"},
    )

    class FakeChatSyncExecutor:
        def __init__(self, handler: object) -> None:
            self._ctx = SimpleNamespace(mapped_model_result="gpt-5.1")

        async def _build_sync_execution_plan(
            self, *args: object, **kwargs: object
        ) -> PreparedExecutionPlan:
            return prepared_plan

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: fake_handler, raising=False)
    monkeypatch.setattr(
        adapter, "_validate_request_body", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        adapter, "_merge_path_params", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-gemini-chat-decision-finalize-123"),
            SimpleNamespace(id="api-key-gemini-chat-decision-finalize-123"),
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
        "src.api.handlers.base.chat_sync_executor.ChatSyncExecutor",
        FakeChatSyncExecutor,
    )

    decision = classify_gateway_route(
        "POST",
        "/v1beta/models/gemini-2.5-pro:generateContent",
        {"x-goog-api-key": "client-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-gemini-chat-decision-finalize-123",
        api_key_id="api-key-gemini-chat-decision-finalize-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1beta/models/gemini-2.5-pro:generateContent",
        headers={"content-type": "application/json", "x-goog-api-key": "client-key"},
        body_json={"contents": []},
        auth_context=auth_context,
    )

    result = await _build_gemini_chat_sync_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "gemini_chat_sync_finalize"
    assert result.provider_api_format == "openai:chat"
    assert result.client_api_format == "gemini:chat"
    assert result.mapped_model == "gpt-5.1"
    assert result.upstream_url == "https://api.openai.example/v1/chat/completions"
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
    }
    assert result.provider_request_body == {
        "model": "gpt-5.1",
        "messages": [{"role": "user", "content": "hello"}],
    }


def test_stream_executor_requires_python_rewrite_allows_claude_to_openai_chat_conversion() -> None:
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=None,
            needs_conversion=True,
            provider_api_format="claude:chat",
            client_api_format="openai:chat",
        )
        is False
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=None,
            needs_conversion=True,
            provider_api_format="gemini:chat",
            client_api_format="openai:chat",
        )
        is False
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=None,
            needs_conversion=True,
            provider_api_format="claude:cli",
            client_api_format="openai:cli",
        )
        is False
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=None,
            needs_conversion=True,
            provider_api_format="gemini:cli",
            client_api_format="openai:cli",
        )
        is False
    )


@pytest.mark.asyncio


async def test_build_openai_chat_stream_decision_preserves_exact_provider_request_for_same_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai import OpenAIChatAdapter

    adapter = OpenAIChatAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-openai-chat-stream-exact-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "messages": [], "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-openai-chat-stream-exact-123",
            name="openai",
            provider_type="openai",
            proxy=None,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-openai-chat-stream-exact-123",
            api_format="openai:chat",
            base_url="https://api.openai.example",
            custom_path=None,
        ),
        key=SimpleNamespace(id="key-openai-chat-stream-exact-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="gpt-5-upstream",
        request_candidate_id="cand-openai-chat-stream-exact-123",
    )
    fake_prep = SimpleNamespace(
        request_body={"model": "gpt-5-upstream", "messages": [], "stream": True},
        url_model="gpt-5-upstream",
        mapped_model="gpt-5-upstream",
        envelope=None,
        extra_headers={"x-provider-extra": "1"},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_api_format="openai:chat",
        client_api_format="openai:chat",
        auth_info=SimpleNamespace(
            as_tuple=lambda: ("authorization", "Bearer upstream-key"),
            decrypted_auth_config=None,
        ),
        tls_profile="custom-profile",
    )

    class FakeChatHandler:
        allowed_api_formats = ["openai:chat"]

        def __init__(self, **kwargs: object) -> None:
            self._request_builder = SimpleNamespace(
                build=lambda *args, **kwargs: (
                    {
                        "model": "gpt-5-upstream",
                        "messages": [],
                        "stream": True,
                        "metadata": {"decision": "exact"},
                    },
                    {
                        "content-type": "application/json",
                        "authorization": "Bearer upstream-key",
                        "x-provider-extra": "1",
                    },
                )
            )

        async def _convert_request(self, request_obj: object) -> object:
            return SimpleNamespace(model="gpt-5")

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _prepare_provider_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_prep

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: FakeChatHandler(), raising=False)
    monkeypatch.setattr(
        adapter, "_validate_request_body", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        adapter, "_merge_path_params", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-openai-chat-stream-exact-123"),
            SimpleNamespace(id="api-key-openai-chat-stream-exact-123"),
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

    decision = classify_gateway_route("POST", "/v1/chat/completions")
    auth_context = GatewayAuthContext(
        user_id="user-openai-chat-stream-exact-123",
        api_key_id="api-key-openai-chat-stream-exact-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "messages": [], "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_chat_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_chat_stream_success"
    assert result.provider_api_format == "openai:chat"
    assert result.client_api_format == "openai:chat"
    assert result.mapped_model == "gpt-5-upstream"
    assert result.upstream_url == "https://api.openai.example/v1/chat/completions"
    assert result.tls_profile == "custom-profile"
    assert result.extra_headers == {}
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
        "accept": "text/event-stream",
    }
    assert result.provider_request_body == {
        "model": "gpt-5-upstream",
        "messages": [],
        "stream": True,
        "metadata": {"decision": "exact"},
    }


@pytest.mark.asyncio


async def test_build_openai_chat_stream_decision_allows_claude_cross_format_exact_provider_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai import OpenAIChatAdapter

    adapter = OpenAIChatAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-openai-chat-stream-claude-xfmt-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "messages": [], "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-openai-chat-stream-claude-xfmt-123",
            name="claude",
            provider_type="anthropic",
            proxy=None,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-openai-chat-stream-claude-xfmt-123",
            api_format="claude:chat",
            base_url="https://api.claude.example",
            custom_path=None,
        ),
        key=SimpleNamespace(id="key-openai-chat-stream-claude-xfmt-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="claude-sonnet-4-5",
        request_candidate_id="cand-openai-chat-stream-claude-xfmt-123",
    )
    fake_prep = SimpleNamespace(
        request_body={"model": "claude-sonnet-4-5", "messages": [], "stream": True},
        url_model="claude-sonnet-4-5",
        mapped_model="claude-sonnet-4-5",
        envelope=None,
        extra_headers={"x-provider-extra": "1"},
        upstream_is_stream=True,
        needs_conversion=True,
        provider_api_format="claude:chat",
        client_api_format="openai:chat",
        auth_info=SimpleNamespace(
            as_tuple=lambda: ("authorization", "Bearer upstream-key"),
            decrypted_auth_config=None,
        ),
        tls_profile="custom-profile",
    )

    class FakeChatHandler:
        allowed_api_formats = ["openai:chat"]

        def __init__(self, **kwargs: object) -> None:
            self._request_builder = SimpleNamespace(
                build=lambda *args, **kwargs: (
                    {
                        "model": "claude-sonnet-4-5",
                        "messages": [],
                        "stream": True,
                    },
                    {
                        "content-type": "application/json",
                        "authorization": "Bearer upstream-key",
                        "x-provider-extra": "1",
                    },
                )
            )

        async def _convert_request(self, request_obj: object) -> object:
            return SimpleNamespace(model="gpt-5")

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _prepare_provider_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_prep

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: FakeChatHandler(), raising=False)
    monkeypatch.setattr(
        adapter, "_validate_request_body", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        adapter, "_merge_path_params", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-openai-chat-stream-claude-xfmt-123"),
            SimpleNamespace(id="api-key-openai-chat-stream-claude-xfmt-123"),
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

    decision = classify_gateway_route("POST", "/v1/chat/completions")
    auth_context = GatewayAuthContext(
        user_id="user-openai-chat-stream-claude-xfmt-123",
        api_key_id="api-key-openai-chat-stream-claude-xfmt-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "messages": [], "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_chat_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_chat_stream_success"
    assert result.provider_api_format == "claude:chat"
    assert result.client_api_format == "openai:chat"
    assert result.mapped_model == "claude-sonnet-4-5"
    assert result.upstream_url == "https://api.claude.example/v1/messages"
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-key",
        "x-provider-extra": "1",
        "accept": "text/event-stream",
    }
    assert result.provider_request_body == {
        "model": "claude-sonnet-4-5",
        "messages": [],
        "stream": True,
    }


@pytest.mark.asyncio


async def test_build_openai_chat_stream_decision_allows_gemini_cross_format_exact_provider_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai import OpenAIChatAdapter

    adapter = OpenAIChatAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-openai-chat-stream-gemini-xfmt-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "messages": [], "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-openai-chat-stream-gemini-xfmt-123",
            name="gemini",
            provider_type="google",
            proxy=None,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-openai-chat-stream-gemini-xfmt-123",
            api_format="gemini:chat",
            base_url="https://generativelanguage.googleapis.com",
            custom_path=None,
        ),
        key=SimpleNamespace(id="key-openai-chat-stream-gemini-xfmt-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="gemini-2.5-pro-upstream",
        request_candidate_id="cand-openai-chat-stream-gemini-xfmt-123",
    )
    fake_prep = SimpleNamespace(
        request_body={"contents": [], "stream": True},
        url_model="gemini-2.5-pro-upstream",
        mapped_model="gemini-2.5-pro-upstream",
        envelope=None,
        extra_headers={"x-provider-extra": "1"},
        upstream_is_stream=True,
        needs_conversion=True,
        provider_api_format="gemini:chat",
        client_api_format="openai:chat",
        auth_info=SimpleNamespace(
            as_tuple=lambda: ("x-goog-api-key", "upstream-key"),
            decrypted_auth_config=None,
        ),
        tls_profile="custom-profile",
    )

    class FakeRequestBuilder:
        def build(self, *args: object, **kwargs: object) -> tuple[dict[str, object], dict[str, str]]:
            return (
                {
                    "contents": [],
                    "generationConfig": {"temperature": 0.2},
                },
                {
                    "content-type": "application/json",
                    "x-goog-api-key": "upstream-key",
                    "x-provider-extra": "1",
                },
            )

    class FakeChatHandler:
        allowed_api_formats = ["openai:chat"]

        def __init__(self, **kwargs: object) -> None:
            self._request_builder = FakeRequestBuilder()

        async def _convert_request(self, request_obj: object) -> object:
            return SimpleNamespace(model="gpt-5")

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gpt-5"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _prepare_provider_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_prep

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: FakeChatHandler(), raising=False)
    monkeypatch.setattr(
        adapter, "_validate_request_body", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        adapter, "_merge_path_params", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-openai-chat-stream-gemini-xfmt-123"),
            SimpleNamespace(id="api-key-openai-chat-stream-gemini-xfmt-123"),
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

    decision = classify_gateway_route("POST", "/v1/chat/completions")
    auth_context = GatewayAuthContext(
        user_id="user-openai-chat-stream-gemini-xfmt-123",
        api_key_id="api-key-openai-chat-stream-gemini-xfmt-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "messages": [], "stream": True},
        auth_context=auth_context,
    )

    result = await _build_openai_chat_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_chat_stream_success"
    assert result.provider_api_format == "gemini:chat"
    assert result.client_api_format == "openai:chat"
    assert result.mapped_model == "gemini-2.5-pro-upstream"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse"
    )
    assert result.tls_profile == "custom-profile"
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-goog-api-key": "upstream-key",
        "x-provider-extra": "1",
        "accept": "text/event-stream",
    }
    assert result.provider_request_body == {
        "contents": [],
        "generationConfig": {"temperature": 0.2},
    }


@pytest.mark.asyncio


async def test_build_claude_chat_stream_decision_preserves_exact_provider_request_for_same_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.claude import ClaudeChatAdapter

    adapter = ClaudeChatAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-claude-chat-stream-exact-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "x-api-key": "client-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "claude-sonnet-4", "messages": [], "stream": True}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-claude-chat-stream-exact-123",
            name="claude",
            provider_type="claude",
            proxy=None,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-claude-chat-stream-exact-123",
            api_format="claude:chat",
            base_url="https://api.anthropic.example",
            custom_path=None,
        ),
        key=SimpleNamespace(id="key-claude-chat-stream-exact-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="claude-sonnet-4-upstream",
        request_candidate_id="cand-claude-chat-stream-exact-123",
    )
    fake_prep = SimpleNamespace(
        request_body={"model": "claude-sonnet-4-upstream", "messages": [], "stream": True},
        url_model="claude-sonnet-4-upstream",
        mapped_model="claude-sonnet-4-upstream",
        envelope=None,
        extra_headers={"x-provider-extra": "1"},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_api_format="claude:chat",
        client_api_format="claude:chat",
        auth_info=SimpleNamespace(
            as_tuple=lambda: ("x-api-key", "upstream-secret"),
            decrypted_auth_config=None,
        ),
        tls_profile="custom-profile",
    )

    class FakeChatHandler:
        allowed_api_formats = ["claude:chat"]

        def __init__(self, **kwargs: object) -> None:
            self._request_builder = SimpleNamespace(
                build=lambda *args, **kwargs: (
                    {
                        "model": "claude-sonnet-4-upstream",
                        "messages": [],
                        "stream": True,
                        "metadata": {"decision": "exact"},
                    },
                    {
                        "content-type": "application/json",
                        "x-api-key": "upstream-secret",
                        "x-provider-extra": "1",
                    },
                )
            )

        async def _convert_request(self, request_obj: object) -> object:
            return SimpleNamespace(model="claude-sonnet-4")

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "claude-sonnet-4"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _prepare_provider_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_prep

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: FakeChatHandler(), raising=False)
    monkeypatch.setattr(
        adapter, "_validate_request_body", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        adapter, "_merge_path_params", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-claude-chat-stream-exact-123"),
            SimpleNamespace(id="api-key-claude-chat-stream-exact-123"),
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
        {"content-type": "application/json", "x-api-key": "client-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-claude-chat-stream-exact-123",
        api_key_id="api-key-claude-chat-stream-exact-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/messages",
        headers={"content-type": "application/json", "x-api-key": "client-key"},
        body_json={"model": "claude-sonnet-4", "messages": [], "stream": True},
        auth_context=auth_context,
    )

    result = await _build_claude_chat_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "claude_chat_stream_success"
    assert result.provider_api_format == "claude:chat"
    assert result.client_api_format == "claude:chat"
    assert result.mapped_model == "claude-sonnet-4-upstream"
    assert result.upstream_url == "https://api.anthropic.example/v1/messages"
    assert result.tls_profile == "custom-profile"
    assert result.extra_headers == {}
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-api-key": "upstream-secret",
        "x-provider-extra": "1",
        "accept": "text/event-stream",
    }
    assert result.provider_request_body == {
        "model": "claude-sonnet-4-upstream",
        "messages": [],
        "stream": True,
        "metadata": {"decision": "exact"},
    }


@pytest.mark.asyncio


async def test_build_gemini_chat_stream_decision_preserves_exact_provider_request_for_same_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.gemini import GeminiChatAdapter

    adapter = GeminiChatAdapter()
    fake_context = SimpleNamespace(
        path_params={"model": "gemini-2.5-pro", "stream": True},
        request_id="req-gemini-chat-stream-exact-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "x-goog-api-key": "client-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-gemini-chat-stream-exact-123",
            name="gemini",
            provider_type="gemini",
            proxy=None,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-gemini-chat-stream-exact-123",
            api_format="gemini:chat",
            base_url="https://generativelanguage.googleapis.com",
            custom_path=None,
        ),
        key=SimpleNamespace(id="key-gemini-chat-stream-exact-123", proxy=None, api_key="enc-key"),
        mapping_matched_model="gemini-2.5-pro-upstream",
        request_candidate_id="cand-gemini-chat-stream-exact-123",
    )
    fake_prep = SimpleNamespace(
        request_body={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
        url_model="gemini-2.5-pro-upstream",
        mapped_model="gemini-2.5-pro-upstream",
        envelope=None,
        extra_headers={"x-provider-extra": "1"},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_api_format="gemini:chat",
        client_api_format="gemini:chat",
        auth_info=SimpleNamespace(
            as_tuple=lambda: ("x-goog-api-key", "upstream-key"),
            decrypted_auth_config=None,
        ),
        tls_profile="custom-profile",
    )

    class FakeChatHandler:
        allowed_api_formats = ["gemini:chat"]

        def __init__(self, **kwargs: object) -> None:
            self._request_builder = SimpleNamespace(
                build=lambda *args, **kwargs: (
                    {
                        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
                        "generationConfig": {"temperature": 0.2},
                        "prompt_cache_key": "cache-key-123",
                    },
                    {
                        "content-type": "application/json",
                        "x-goog-api-key": "upstream-key",
                        "x-provider-extra": "1",
                    },
                )
            )

        async def _convert_request(self, request_obj: object) -> object:
            return SimpleNamespace(model="gemini-2.5-pro")

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gemini-2.5-pro"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _prepare_provider_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_prep

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: FakeChatHandler(), raising=False)
    monkeypatch.setattr(
        adapter,
        "_validate_request_body",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        adapter,
        "_merge_path_params",
        lambda body, path_params: body,
        raising=False,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {"model": "gemini-2.5-pro", "stream": True}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-gemini-chat-stream-exact-123"),
            SimpleNamespace(id="api-key-gemini-chat-stream-exact-123"),
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
        "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        {"content-type": "application/json", "x-goog-api-key": "client-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-gemini-chat-stream-exact-123",
        api_key_id="api-key-gemini-chat-stream-exact-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        headers={"content-type": "application/json", "x-goog-api-key": "client-key"},
        body_json={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
        auth_context=auth_context,
    )

    result = await _build_gemini_chat_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "gemini_chat_stream_success"
    assert result.provider_api_format == "gemini:chat"
    assert result.client_api_format == "gemini:chat"
    assert result.mapped_model == "gemini-2.5-pro-upstream"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse"
    )
    assert result.tls_profile == "custom-profile"
    assert result.extra_headers == {}
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "x-goog-api-key": "upstream-key",
        "x-provider-extra": "1",
        "accept": "text/event-stream",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.2},
        "prompt_cache_key": "cache-key-123",
    }


@pytest.mark.asyncio


async def test_build_gemini_chat_stream_decision_allows_antigravity_force_rewrite_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.gemini import GeminiChatAdapter

    class AntigravityEnvelope:
        name = "antigravity:v1internal"

        def force_stream_rewrite(self) -> bool:
            return True

    adapter = GeminiChatAdapter()
    fake_context = SimpleNamespace(
        path_params={"model": "gemini-2.5-pro"},
        request_id="req-antigravity-gemini-chat-stream-exact-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={
            "content-type": "application/json",
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
            id="provider-antigravity-gemini-chat-stream-exact-123",
            name="antigravity",
            provider_type="antigravity",
            proxy=None,
        ),
        endpoint=SimpleNamespace(
            id="endpoint-antigravity-gemini-chat-stream-exact-123",
            api_format="gemini:chat",
            base_url="https://generativelanguage.googleapis.com",
            custom_path=None,
        ),
        key=SimpleNamespace(
            id="key-antigravity-gemini-chat-stream-exact-123",
            proxy=None,
            api_key="enc-key",
        ),
        mapping_matched_model="claude-sonnet-4-5",
        request_candidate_id="cand-antigravity-gemini-chat-stream-exact-123",
    )
    fake_prep = SimpleNamespace(
        request_body={"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
        url_model="claude-sonnet-4-5",
        mapped_model="claude-sonnet-4-5",
        envelope=AntigravityEnvelope(),
        extra_headers={"x-provider-extra": "1"},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_api_format="gemini:chat",
        client_api_format="gemini:chat",
        auth_info=SimpleNamespace(
            as_tuple=lambda: ("authorization", "Bearer upstream-secret"),
            decrypted_auth_config=None,
        ),
        tls_profile="custom-profile",
    )

    class FakeChatHandler:
        allowed_api_formats = ["gemini:chat"]

        def __init__(self, **kwargs: object) -> None:
            self._request_builder = SimpleNamespace(
                build=lambda *args, **kwargs: (
                    {
                        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
                        "generationConfig": {"temperature": 0.2},
                    },
                    {
                        "content-type": "application/json",
                        "authorization": "Bearer upstream-secret",
                        "x-provider-extra": "1",
                    },
                )
            )

        async def _convert_request(self, request_obj: object) -> object:
            return SimpleNamespace(model="gemini-2.5-pro")

        def extract_model_from_request(
            self, body: dict[str, object], path_params: dict[str, str]
        ) -> str:
            return "gemini-2.5-pro"

        def _resolve_capability_requirements(self, **kwargs: object) -> None:
            return None

        async def _resolve_preferred_key_ids(self, **kwargs: object) -> None:
            return None

        async def _prepare_provider_request(self, **kwargs: object) -> SimpleNamespace:
            return fake_prep

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(adapter, "_create_handler", lambda **kwargs: FakeChatHandler(), raising=False)
    monkeypatch.setattr(
        adapter, "_validate_request_body", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        adapter, "_merge_path_params", lambda body, path_params: body, raising=False
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (adapter, {"model": "gemini-2.5-pro"}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-antigravity-gemini-chat-stream-exact-123"),
            SimpleNamespace(id="api-key-antigravity-gemini-chat-stream-exact-123"),
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
        "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        {"content-type": "application/json", "authorization": "Bearer client-key"},
    )
    auth_context = GatewayAuthContext(
        user_id="user-antigravity-gemini-chat-stream-exact-123",
        api_key_id="api-key-antigravity-gemini-chat-stream-exact-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        headers={"content-type": "application/json", "authorization": "Bearer client-key"},
        body_json={
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "stream": True,
        },
        auth_context=auth_context,
    )

    result = await _build_gemini_chat_stream_decision(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "gemini_chat_stream_success"
    assert result.provider_api_format == "gemini:chat"
    assert result.client_api_format == "gemini:chat"
    assert result.mapped_model == "claude-sonnet-4-5"
    assert result.upstream_url == (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/claude-sonnet-4-5:streamGenerateContent?alt=sse"
    )
    assert result.provider_request_headers == {
        "content-type": "application/json",
        "authorization": "Bearer upstream-secret",
        "x-provider-extra": "1",
        "accept": "text/event-stream",
    }
    assert result.provider_request_body == {
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.2},
    }


@pytest.mark.asyncio


async def test_build_openai_chat_sync_plan_allows_upstream_stream_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.openai import OpenAIChatAdapter

    adapter = OpenAIChatAdapter()
    fake_context = SimpleNamespace(
        path_params={},
        request_id="req-chat-upstream-stream-123",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
        original_headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        query_params={},
        client_content_encoding=None,
        extra={},
    )
    fake_context.ensure_json_body_async = AsyncMock(
        return_value={"model": "gpt-5", "messages": []}
    )
    fake_candidate = SimpleNamespace(
        provider=SimpleNamespace(
            id="provider-chat-upstream-stream-123",
            name="openai",
            provider_type="openai",
            proxy=None,
            request_timeout=30.0,
        ),
        endpoint=SimpleNamespace(id="endpoint-chat-upstream-stream-123", api_format="openai:chat"),
        key=SimpleNamespace(id="key-chat-upstream-stream-123", proxy=None),
        mapping_matched_model="gpt-5",
        needs_conversion=False,
        output_limit=None,
        request_candidate_id="cand-chat-upstream-stream-123",
    )
    prepared_plan = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="plan-chat-upstream-stream-123",
            candidate_id="cand-chat-upstream-stream-123",
            provider_name="openai",
            provider_id="provider-chat-upstream-stream-123",
            endpoint_id="endpoint-chat-upstream-stream-123",
            key_id="key-chat-upstream-stream-123",
            method="POST",
            url="https://api.openai.example/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-5", "messages": []}),
            stream=True,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            model_name="gpt-5",
        ),
        payload={"model": "gpt-5", "messages": []},
        headers={"content-type": "application/json"},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=30.0,
        envelope=None,
        proxy_info={"mode": "direct"},
    )

    class FakeChatSyncExecutor:
        def __init__(self, handler: object) -> None:
            self._ctx = SimpleNamespace(mapped_model_result="gpt-5")

        async def _build_sync_execution_plan(self, *args: object, **kwargs: object) -> PreparedExecutionPlan:
            return prepared_plan

    monkeypatch.setattr(adapter, "authorize", lambda context: None)
    monkeypatch.setattr(
        adapter,
        "_create_handler",
        lambda **kwargs: SimpleNamespace(
            allowed_api_formats=["openai:chat"],
            _convert_request=AsyncMock(return_value=SimpleNamespace(model="gpt-5")),
            extract_model_from_request=lambda body, path_params: "gpt-5",
            _resolve_capability_requirements=lambda **kwargs: None,
            _resolve_preferred_key_ids=AsyncMock(return_value=None),
        ),
        raising=False,
    )
    monkeypatch.setattr(
        adapter,
        "_validate_request_body",
        lambda body, path_params: body,
        raising=False,
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
            SimpleNamespace(id="user-chat-upstream-stream-123"),
            SimpleNamespace(id="api-key-chat-upstream-stream-123"),
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
        "src.api.handlers.base.chat_sync_executor.ChatSyncExecutor",
        FakeChatSyncExecutor,
    )

    decision = classify_gateway_route("POST", "/v1/chat/completions")
    auth_context = GatewayAuthContext(
        user_id="user-chat-upstream-stream-123",
        api_key_id="api-key-chat-upstream-stream-123",
        access_allowed=True,
    )
    payload = GatewayExecuteRequest(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json", "authorization": "Bearer test-key"},
        body_json={"model": "gpt-5", "messages": []},
        auth_context=auth_context,
    )

    result = await _build_openai_chat_sync_plan(
        request=SimpleNamespace(),
        payload=payload,
        db=object(),
        auth_context=auth_context,
        decision=decision,
    )

    assert result is not None
    assert result.report_kind == "openai_chat_sync_finalize"
    assert result.plan["stream"] is True

"""Gateway internal report/finalize/trace route tests."""

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


def test_report_sync_route_records_openai_chat_sync_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_openai_chat_sync_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-openai-chat-report",
            "report_kind": "openai_chat_sync_success",
            "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "choices": [],
            },
            "telemetry": {"elapsed_ms": 123},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_sync_route_records_openai_cli_sync_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_cli_sync_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-openai-cli-report",
            "report_kind": "openai_cli_sync_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "openai:cli",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {
                "id": "resp_123",
                "object": "response",
                "output": [],
            },
            "telemetry": {"elapsed_ms": 91},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_sync_route_records_claude_cli_sync_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_cli_sync_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-claude-cli-report",
            "report_kind": "claude_cli_sync_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "claude:cli",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {
                "id": "msg_123",
                "type": "message",
                "content": [],
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_sync_route_records_gemini_cli_sync_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_cli_sync_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-gemini-cli-report",
            "report_kind": "gemini_cli_sync_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "gemini:cli",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {
                "candidates": [],
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


@pytest.mark.parametrize(
    ("report_kind", "recorder_attr"),
    [
        ("openai_video_create_sync_success", "_record_gateway_openai_video_create_sync_success"),
        ("openai_video_remix_sync_success", "_record_gateway_openai_video_remix_sync_success"),
        ("openai_video_delete_sync_success", "_record_gateway_openai_video_delete_sync_success"),
        ("openai_video_cancel_sync_success", "_record_gateway_openai_video_cancel_sync_success"),
        ("gemini_video_create_sync_success", "_record_gateway_gemini_video_create_sync_success"),
        ("gemini_video_cancel_sync_success", "_record_gateway_gemini_video_cancel_sync_success"),
    ],
)
def test_report_sync_route_records_video_sync_success_variants(
    monkeypatch: pytest.MonkeyPatch,
    report_kind: str,
    recorder_attr: str,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(f"src.api.internal.gateway.{recorder_attr}", record_mock)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": f"trace-{report_kind}",
            "report_kind": report_kind,
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "task_id": "task-123",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {},
            "telemetry": {"elapsed_ms": 33},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("recorder_attr", "finalize_attr"),
    [
        (
            "_record_gateway_openai_video_create_sync_success",
            "_finalize_gateway_openai_video_create_sync",
        ),
        (
            "_record_gateway_openai_video_remix_sync_success",
            "_finalize_gateway_openai_video_remix_sync",
        ),
        (
            "_record_gateway_gemini_video_create_sync_success",
            "_finalize_gateway_gemini_video_create_sync",
        ),
    ],
)
async def test_video_sync_success_recorders_delegate_to_finalize(
    monkeypatch: pytest.MonkeyPatch,
    recorder_attr: str,
    finalize_attr: str,
) -> None:
    import src.api.internal.gateway as gateway_module

    finalize_mock = AsyncMock(return_value=JSONResponse({"ok": True}))
    monkeypatch.setattr(f"src.api.internal.gateway.{finalize_attr}", finalize_mock)

    db = object()
    payload = GatewaySyncReportRequest(
        trace_id="trace-video-sync-success-123",
        report_kind="video-sync-success",
        report_context={"user_id": "user-123", "api_key_id": "key-123"},
        status_code=200,
        headers={"content-type": "application/json"},
        body_json={"id": "ext-123"},
    )

    recorder = getattr(gateway_module, recorder_attr)
    await recorder(payload, db=db)

    finalize_mock.assert_awaited_once_with(payload, db=db)


def test_report_sync_route_records_claude_chat_sync_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_chat_sync_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-claude-chat-report",
            "report_kind": "claude_chat_sync_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "claude:chat",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {
                "id": "msg_123",
                "type": "message",
                "content": [],
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_sync_route_records_gemini_chat_sync_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_chat_sync_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-gemini-chat-report",
            "report_kind": "gemini_chat_sync_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "gemini:chat",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {
                "candidates": [],
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


@pytest.mark.asyncio


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_record_gateway_openai_chat_sync_success_uses_provider_and_client_bodies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module

    record_success = AsyncMock(return_value=None)
    fake_handler = SimpleNamespace(
        _normalize_response=lambda response: response,
        _extract_usage=lambda response: {
            "input_tokens": response.get("usage", {}).get("prompt_tokens", 0),
            "output_tokens": response.get("usage", {}).get("completion_tokens", 0),
        },
        telemetry=SimpleNamespace(record_success=record_success),
    )

    class FakeOpenAIChatAdapter:
        API_FAMILY = SimpleNamespace(value="openai")
        ENDPOINT_KIND = SimpleNamespace(value="chat")

        def _create_handler(self, **kwargs: Any) -> Any:
            return fake_handler

    fake_user = object()
    fake_api_key = object()

    class FakeQuery:
        def __init__(self, value: Any) -> None:
            self.value = value

        def filter(self, *args: Any, **kwargs: Any) -> "FakeQuery":
            return self

        def first(self) -> Any:
            return self.value

    class FakeDb:
        def __init__(self) -> None:
            self._values = iter((fake_user, fake_api_key))

        def query(self, model: Any) -> FakeQuery:
            return FakeQuery(next(self._values))

    monkeypatch.setattr("src.api.handlers.openai.OpenAIChatAdapter", FakeOpenAIChatAdapter)

    provider_body = {
        "responseId": "resp-provider-123",
        "candidates": [
            {
                "content": {"parts": [{"text": "provider"}], "role": "model"},
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 2,
            "candidatesTokenCount": 3,
            "totalTokenCount": 5,
        },
    }
    client_body = {
        "id": "chatcmpl-client-123",
        "object": "chat.completion",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "client"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 2,
            "completion_tokens": 3,
            "total_tokens": 5,
        },
    }
    payload = GatewaySyncReportRequest(
        trace_id="trace-openai-chat-conversion-123",
        report_kind="openai_chat_sync_success",
        report_context={
            "user_id": "user-123",
            "api_key_id": "key-123",
            "provider_name": "gemini",
            "provider_api_format": "gemini:chat",
            "client_api_format": "openai:chat",
            "model": "gpt-5",
            "mapped_model": "gpt-5",
            "request_id": "req-123",
        },
        status_code=200,
        headers={"content-type": "application/json"},
        body_json=provider_body,
        client_body_json=client_body,
        telemetry={"elapsed_ms": 31},
    )

    await gateway_module._record_gateway_openai_chat_sync_success(payload, db=FakeDb())

    kwargs = record_success.await_args.kwargs
    assert kwargs["response_body"] == provider_body
    assert kwargs["client_response_body"] == client_body
    assert kwargs["has_format_conversion"] is True


@pytest.mark.asyncio


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_record_gateway_passthrough_cli_sync_success_uses_provider_and_client_bodies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module

    record_success = AsyncMock(return_value=None)

    class FakeCliHandler:
        def __init__(self, **kwargs: Any) -> None:
            self.parser = SimpleNamespace(
                extract_usage_from_response=lambda response: {
                    "input_tokens": response.get("usage", {}).get("input_tokens", 0),
                    "output_tokens": response.get("usage", {}).get("output_tokens", 0),
                    "cache_creation_tokens": 0,
                    "cache_read_tokens": 0,
                }
            )
            self.telemetry = SimpleNamespace(record_success=record_success)

        def _extract_response_metadata(self, response: dict[str, Any]) -> dict[str, Any]:
            return {"response_id": response.get("id")}

    class FakeOpenAICliAdapter:
        API_FAMILY = SimpleNamespace(value="openai")
        ENDPOINT_KIND = SimpleNamespace(value="cli")
        HANDLER_CLASS = FakeCliHandler
        allowed_api_formats = ["openai:cli"]

        @staticmethod
        def detect_capability_requirements(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return {}

    fake_user = object()
    fake_api_key = object()

    class FakeQuery:
        def __init__(self, value: Any) -> None:
            self.value = value

        def filter(self, *args: Any, **kwargs: Any) -> "FakeQuery":
            return self

        def first(self) -> Any:
            return self.value

    class FakeDb:
        def __init__(self) -> None:
            self._values = iter((fake_user, fake_api_key))

        def query(self, model: Any) -> FakeQuery:
            return FakeQuery(next(self._values))

    monkeypatch.setattr("src.api.handlers.openai_cli.OpenAICliAdapter", FakeOpenAICliAdapter)

    provider_body = {
        "responseId": "resp-provider-cli-123",
        "candidates": [
            {
                "content": {"parts": [{"text": "provider"}], "role": "model"},
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 2,
            "candidatesTokenCount": 3,
            "totalTokenCount": 5,
        },
    }
    client_body = {
        "id": "resp-client-cli-123",
        "object": "response",
        "output": [],
        "usage": {
            "input_tokens": 2,
            "output_tokens": 3,
            "total_tokens": 5,
        },
    }
    payload = GatewaySyncReportRequest(
        trace_id="trace-openai-cli-conversion-123",
        report_kind="openai_cli_sync_success",
        report_context={
            "user_id": "user-123",
            "api_key_id": "key-123",
            "provider_name": "gemini",
            "provider_api_format": "gemini:cli",
            "client_api_format": "openai:cli",
            "model": "gpt-5",
            "mapped_model": "gpt-5",
            "request_id": "req-cli-123",
        },
        status_code=200,
        headers={"content-type": "application/json"},
        body_json=provider_body,
        client_body_json=client_body,
        telemetry={"elapsed_ms": 29},
    )

    await gateway_module._record_gateway_passthrough_cli_sync_success(
        payload, db=FakeDb()
    )

    kwargs = record_success.await_args.kwargs
    assert kwargs["response_body"] == provider_body
    assert kwargs["client_response_body"] == client_body
    assert kwargs["has_format_conversion"] is True


@pytest.mark.asyncio


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_record_gateway_passthrough_cli_sync_success_postprocesses_antigravity_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module

    record_success = AsyncMock(return_value=None)
    seen_cache: list[tuple[str, dict[str, Any]]] = []

    class FakeCliHandler:
        def __init__(self, **kwargs: Any) -> None:
            self.parser = SimpleNamespace(
                extract_usage_from_response=lambda response: {
                    "input_tokens": response.get("usageMetadata", {}).get("promptTokenCount", 0),
                    "output_tokens": response.get("usageMetadata", {}).get(
                        "candidatesTokenCount", 0
                    ),
                    "cache_creation_tokens": 0,
                    "cache_read_tokens": 0,
                }
            )
            self.telemetry = SimpleNamespace(record_success=record_success)

        def _extract_response_metadata(self, response: dict[str, Any]) -> dict[str, Any]:
            return {"response_id": response.get("_v1internal_response_id")}

    class FakeGeminiCliAdapter:
        API_FAMILY = SimpleNamespace(value="gemini")
        ENDPOINT_KIND = SimpleNamespace(value="cli")
        HANDLER_CLASS = FakeCliHandler
        allowed_api_formats = ["gemini:cli"]

        @staticmethod
        def detect_capability_requirements(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return {}

    fake_user = object()
    fake_api_key = object()

    class FakeQuery:
        def __init__(self, value: Any) -> None:
            self.value = value

        def filter(self, *args: Any, **kwargs: Any) -> "FakeQuery":
            return self

        def first(self) -> Any:
            return self.value

    class FakeDb:
        def __init__(self) -> None:
            self._values = iter((fake_user, fake_api_key))

        def query(self, model: Any) -> FakeQuery:
            return FakeQuery(next(self._values))

    def _record_cache(model: str, response: dict[str, Any]) -> None:
        seen_cache.append((model, response))

    monkeypatch.setattr("src.api.handlers.gemini_cli.GeminiCliAdapter", FakeGeminiCliAdapter)
    monkeypatch.setattr(
        "src.services.provider.adapters.antigravity.envelope.cache_thought_signatures",
        _record_cache,
    )

    provider_body = {
        "_v1internal_response_id": "resp-antigravity-123",
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": "thinking",
                            "thoughtSignature": "a" * 60,
                        }
                    ],
                    "role": "model",
                },
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 2,
            "candidatesTokenCount": 3,
            "totalTokenCount": 5,
        },
    }
    payload = GatewaySyncReportRequest(
        trace_id="trace-antigravity-cli-report-123",
        report_kind="gemini_cli_sync_success",
        report_context={
            "user_id": "user-123",
            "api_key_id": "key-123",
            "provider_name": "antigravity",
            "provider_api_format": "gemini:cli",
            "client_api_format": "gemini:cli",
            "model": "claude-sonnet-4-5",
            "mapped_model": "claude-sonnet-4-5",
            "request_id": "req-antigravity-cli-123",
            "envelope_name": "antigravity:v1internal",
        },
        status_code=200,
        headers={"content-type": "application/json"},
        body_json=provider_body,
        telemetry={"elapsed_ms": 17},
    )

    await gateway_module._record_gateway_passthrough_cli_sync_success(payload, db=FakeDb())

    assert len(seen_cache) == 1
    assert seen_cache[0][0] == "claude-sonnet-4-5"
    assert seen_cache[0][1]["_v1internal_response_id"] == "resp-antigravity-123"

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
from src.services.request.executor_plan import (
    ExecutionPlan,
    ExecutionPlanBody,
    PreparedExecutionPlan,
)


def _wait_until(predicate: Any, *, timeout: float = 1.0, interval: float = 0.01) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()


def test_build_gateway_sync_telemetry_writer_uses_queue_writer_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config.settings import config
    from src.services.usage.telemetry_writer import QueueTelemetryWriter

    monkeypatch.setattr(config, "usage_queue_enabled", True)
    monkeypatch.setattr(
        "src.services.system.config.SystemConfigService.get_request_record_level",
        lambda db: SimpleNamespace(value="headers"),
    )
    monkeypatch.setattr(
        "src.services.system.config.SystemConfigService.get_sensitive_headers",
        lambda db: ["authorization"],
    )
    monkeypatch.setattr(
        "src.services.system.config.SystemConfigService.get_config",
        lambda db, key, default=None: default,
    )

    writer = _build_gateway_sync_telemetry_writer(
        db=object(),
        request_id="req-queue-writer-123",
        user_id="user-queue-writer-123",
        api_key_id="api-key-queue-writer-123",
        fallback_telemetry=object(),
    )

    assert isinstance(writer, QueueTelemetryWriter)
    assert writer.request_id == "req-queue-writer-123"
    assert writer.include_headers is True


@pytest.mark.asyncio
async def test_dispatch_gateway_sync_telemetry_backgrounds_queue_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module

    started = asyncio.Event()
    release = asyncio.Event()
    scheduled: list[asyncio.Task[Any]] = []

    class FakeWriter:
        request_id = "req-background-dispatch-123"

        def supports_background_submission(self) -> bool:
            return True

        async def record_success(self, **kwargs: Any) -> None:
            started.set()
            await release.wait()

    def fake_safe_create_task(coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        scheduled.append(task)
        return task

    monkeypatch.setattr(gateway_module, "safe_create_task", fake_safe_create_task)

    await _dispatch_gateway_sync_telemetry(
        telemetry_writer=FakeWriter(),
        operation="record_success",
        provider="openai",
    )

    await asyncio.wait_for(started.wait(), timeout=0.2)
    assert scheduled
    assert scheduled[0].done() is False

    release.set()
    await scheduled[0]


def test_classify_openai_chat_route_as_ai_public() -> None:
    decision = classify_gateway_route("POST", "/v1/chat/completions")

    assert decision.route_class == "ai_public"
    assert decision.route_family == "openai"
    assert decision.route_kind == "chat"
    assert decision.auth_endpoint_signature == "openai:chat"
    assert decision.executor_candidate is True
    assert decision.action == "proxy_public"


def test_classify_gemini_files_download_route_as_ai_public() -> None:
    decision = classify_gateway_route("GET", "/v1beta/files/file-123:download")

    assert decision.route_class == "ai_public"
    assert decision.route_family == "gemini"
    assert decision.route_kind == "files"
    assert decision.auth_endpoint_signature == "gemini:chat"
    assert decision.executor_candidate is True


def test_classify_gemini_files_nested_metadata_route_as_ai_public() -> None:
    decision = classify_gateway_route("GET", "/v1beta/files/files/abc-123")

    assert decision.route_class == "ai_public"
    assert decision.route_family == "gemini"
    assert decision.route_kind == "files"
    assert decision.executor_candidate is True


def test_classify_gemini_video_operation_route_as_ai_public() -> None:
    decision = classify_gateway_route("GET", "/v1beta/models/veo-3/operations/op-123")

    assert decision.route_class == "ai_public"
    assert decision.route_family == "gemini"
    assert decision.route_kind == "video"
    assert decision.auth_endpoint_signature == "gemini:video"
    assert decision.executor_candidate is True


def test_classify_non_ai_route_as_passthrough() -> None:
    decision = classify_gateway_route("GET", "/api/admin/system/info")

    assert decision.route_class == "passthrough"
    assert decision.route_family is None
    assert decision.route_kind is None
    assert decision.executor_candidate is False


def test_classify_claude_cli_route_from_bearer_header() -> None:
    decision = classify_gateway_route(
        "POST",
        "/v1/messages",
        {"authorization": "Bearer sk-cli"},
    )

    assert decision.route_class == "ai_public"
    assert decision.route_family == "claude"
    assert decision.route_kind == "cli"
    assert decision.auth_endpoint_signature == "claude:cli"


def test_classify_gemini_cli_route_from_user_agent() -> None:
    decision = classify_gateway_route(
        "POST",
        "/v1beta/models/gemini-2.5-pro:generateContent",
        {"user-agent": "GeminiCLI/1.2.3"},
    )

    assert decision.route_class == "ai_public"
    assert decision.route_family == "gemini"
    assert decision.route_kind == "cli"
    assert decision.auth_endpoint_signature == "gemini:cli"


def test_resolve_sync_adapter_for_openai_chat_route() -> None:
    decision = classify_gateway_route("POST", "/v1/chat/completions")

    adapter, path_params = _resolve_gateway_sync_adapter(decision, "/v1/chat/completions")

    assert adapter is not None
    assert adapter.name == "openai.chat"
    assert path_params == {}


def test_resolve_sync_adapter_for_gemini_route_extracts_model_path_params() -> None:
    decision = classify_gateway_route("POST", "/v1beta/models/gemini-2.5-pro:generateContent")

    adapter, path_params = _resolve_gateway_sync_adapter(
        decision,
        "/v1beta/models/gemini-2.5-pro:generateContent",
    )

    assert adapter is not None
    assert adapter.name == "gemini.chat"
    assert path_params == {"model": "gemini-2.5-pro", "stream": False}


def test_resolve_sync_adapter_rejects_non_sync_files_route() -> None:
    decision = classify_gateway_route("GET", "/v1beta/files/file-123:download")

    adapter, path_params = _resolve_gateway_sync_adapter(
        decision, "/v1beta/files/file-123:download"
    )

    assert adapter is None
    assert path_params == {}


def test_is_streaming_sync_payload_detects_body_and_path_stream_flags() -> None:
    assert _is_streaming_sync_payload({"stream": True}, {}) is True
    assert _is_streaming_sync_payload({}, {"stream": True}) is True
    assert _is_streaming_sync_payload({"stream": False}, {"stream": False}) is False


@pytest.mark.asyncio
async def test_resolve_auth_context_from_openai_bearer_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = GatewayResolveRequest(
        method="POST",
        path="/v1/chat/completions",
        headers={"Authorization": "Bearer sk-test"},
    )
    decision = classify_gateway_route(payload.method, payload.path, payload.headers)
    monkeypatch.setattr(
        "src.api.internal.gateway.AuthService.authenticate_api_key_threadsafe",
        AsyncMock(
            return_value=SimpleNamespace(
                user=SimpleNamespace(id="user-123"),
                api_key=SimpleNamespace(id="key-123"),
                balance_remaining=42.5,
                access_allowed=True,
            )
        ),
    )

    auth_context = await _resolve_auth_context(payload, decision)

    assert auth_context == {
        "user_id": "user-123",
        "api_key_id": "key-123",
        "balance_remaining": 42.5,
        "access_allowed": True,
    }


def test_auth_context_route_returns_openai_bearer_auth_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway.AuthService.authenticate_api_key_threadsafe",
        AsyncMock(
            return_value=SimpleNamespace(
                user=SimpleNamespace(id="user-auth-route-123"),
                api_key=SimpleNamespace(id="api-key-auth-route-123"),
                balance_remaining=3.5,
                access_allowed=True,
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/auth-context",
        json={
            "trace_id": "trace-auth-route-123",
            "query_string": "",
            "headers": {"authorization": "Bearer sk-test"},
            "auth_endpoint_signature": "openai:chat",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "auth_context": {
            "user_id": "user-auth-route-123",
            "api_key_id": "api-key-auth-route-123",
            "balance_remaining": 3.5,
            "access_allowed": True,
        }
    }


def test_execute_sync_route_returns_controlled_response(monkeypatch: pytest.MonkeyPatch) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    class FakeAdapter:
        mode = SimpleNamespace(value="standard")
        allowed_api_formats = ["openai:chat"]

        def authorize(self, context: object) -> None:
            self.authorized_context = context

        async def handle(self, context: object) -> JSONResponse:
            assert getattr(context, "path_params", {}) == {}
            return JSONResponse(
                status_code=201, content={"ok": True, "request_id": context.request_id}
            )

    fake_adapter = FakeAdapter()
    fake_pipeline = SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None))

    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (fake_adapter, {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr("src.api.internal.gateway.get_pipeline", lambda: fake_pipeline)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-sync",
        json={
            "trace_id": "trace-sync-123",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {"user-agent": "pytest"},
            "body_json": {"model": "gpt-5", "messages": []},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "balance_remaining": 12.5,
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 201
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json()["ok"] is True
    assert response.json()["request_id"] == "trace-sync-123"


def test_execute_sync_route_resolves_auth_context_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    class FakeAdapter:
        mode = SimpleNamespace(value="standard")
        allowed_api_formats = ["openai:chat"]

        def authorize(self, context: object) -> None:
            self.authorized_context = context

        async def handle(self, context: object) -> JSONResponse:
            return JSONResponse(status_code=200, content={"request_id": context.request_id})

    fake_pipeline = SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None))
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (FakeAdapter(), {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_auth_context_signature",
        AsyncMock(
            return_value={
                "user_id": "user-123",
                "api_key_id": "key-123",
                "balance_remaining": 9.5,
                "access_allowed": True,
            }
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id=auth_context.user_id),
            SimpleNamespace(id=auth_context.api_key_id),
        ),
    )
    monkeypatch.setattr("src.api.internal.gateway.get_pipeline", lambda: fake_pipeline)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-sync",
        json={
            "trace_id": "trace-sync-derive-auth",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {
                "user-agent": "pytest",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "messages": []},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {"request_id": "trace-sync-derive-auth"}


def test_execute_sync_route_falls_back_for_stream_payload() -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-sync",
        json={
            "trace_id": "trace-stream-123",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {"user-agent": "pytest"},
            "body_json": {"model": "gpt-5", "messages": [], "stream": True},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 409
    assert response.headers[CONTROL_ACTION_HEADER] == CONTROL_ACTION_PROXY_PUBLIC
    assert response.json() == {"action": CONTROL_ACTION_PROXY_PUBLIC}
    monkeypatch.undo()


def test_execute_stream_route_returns_controlled_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    class FakeAdapter:
        mode = SimpleNamespace(value="standard")
        allowed_api_formats = ["openai:chat"]

        def authorize(self, context: object) -> None:
            self.authorized_context = context

        async def handle(self, context: object) -> StreamingResponse:
            async def _iter() -> object:
                yield b"data: one\n\n"
                yield b"data: [DONE]\n\n"

            return StreamingResponse(_iter(), media_type="text/event-stream")

    fake_pipeline = SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None))
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (FakeAdapter(), {}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr("src.api.internal.gateway.get_pipeline", lambda: fake_pipeline)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-stream",
        json={
            "trace_id": "trace-stream-123",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {"user-agent": "pytest"},
            "body_json": {"model": "gpt-5", "messages": [], "stream": True},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "balance_remaining": 12.5,
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.text == "data: one\n\ndata: [DONE]\n\n"

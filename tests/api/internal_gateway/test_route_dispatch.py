"""Gateway internal route/control/dispatch tests."""

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

def test_plan_sync_route_executes_control_when_direct_plan_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_sync_plan_response",
        AsyncMock(return_value=None),
    )
    execute_mock = AsyncMock(
        return_value=JSONResponse(
            status_code=202,
            content={"ok": True, "via": "plan-sync"},
            headers={CONTROL_EXECUTED_HEADER: "true"},
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._execute_gateway_control_request",
        execute_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-plan-sync-control-123",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {"content-type": "application/json"},
            "body_json": {"model": "gpt-5", "messages": []},
        },
    )

    assert response.status_code == 202
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {"ok": True, "via": "plan-sync"}
    execute_mock.assert_awaited_once()


def test_plan_stream_route_executes_control_when_direct_plan_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gateway_stream_plan_response",
        AsyncMock(return_value=None),
    )
    execute_mock = AsyncMock(
        return_value=StreamingResponse(
            iter([b"data: one\n\n", b"data: [DONE]\n\n"]),
            media_type="text/event-stream",
            headers={CONTROL_EXECUTED_HEADER: "true"},
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._execute_gateway_control_request",
        execute_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-plan-stream-control-123",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {"content-type": "application/json"},
            "body_json": {"model": "gpt-5", "messages": [], "stream": True},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.text == "data: one\n\ndata: [DONE]\n\n"
    execute_mock.assert_awaited_once()


def test_execute_stream_route_falls_back_for_sync_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-stream",
        json={
            "trace_id": "trace-sync-456",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {"user-agent": "pytest"},
            "body_json": {"model": "gpt-5", "messages": []},
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


def test_execute_sync_route_handles_gemini_files_list(monkeypatch: pytest.MonkeyPatch) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )

    async def _fake_list_files(
        request: object,
        pageSize: int | None = None,
        pageToken: str | None = None,
    ) -> JSONResponse:
        del request
        assert pageSize == 25
        assert pageToken == "page-2"
        return JSONResponse(status_code=200, content={"files": [{"name": "files/abc"}]})

    monkeypatch.setattr("src.api.public.gemini_files.list_files", _fake_list_files)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-sync",
        json={
            "trace_id": "trace-files-list",
            "method": "GET",
            "path": "/v1beta/files",
            "query_string": "pageSize=25&pageToken=page-2",
            "headers": {"x-goog-api-key": "client-key"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {"files": [{"name": "files/abc"}]}


def test_execute_sync_route_handles_gemini_files_upload_raw_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )

    async def _fake_upload_file(request: object) -> JSONResponse:
        body = await request.body()
        assert body == b"upload-bytes"
        assert request.headers["content-type"] == "application/octet-stream"
        return JSONResponse(status_code=201, content={"uploaded": True})

    monkeypatch.setattr("src.api.public.gemini_files.upload_file", _fake_upload_file)

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-sync",
        json={
            "trace_id": "trace-files-upload",
            "method": "POST",
            "path": "/upload/v1beta/files",
            "query_string": "uploadType=resumable",
            "headers": {
                "x-goog-api-key": "client-key",
                "content-type": "application/octet-stream",
            },
            "body_base64": base64.b64encode(b"upload-bytes").decode("ascii"),
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 201
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {"uploaded": True}


def test_execute_sync_route_handles_openai_video_remix_with_original_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    class FakeVideoAdapter:
        mode = SimpleNamespace(value="standard")
        allowed_api_formats = ["openai:video"]

        def authorize(self, context: object) -> None:
            self.authorized_context = context

        async def handle(self, context: object) -> JSONResponse:
            assert context.request.method == "POST"
            assert context.request.url.path == "/v1/videos/task-123/remix"
            assert context.path_params == {"task_id": "task-123"}
            assert await context.ensure_json_body_async() == {"prompt": "remix this"}
            return JSONResponse(status_code=200, content={"video": True})

    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_sync_adapter",
        lambda decision, path: (FakeVideoAdapter(), {"task_id": "task-123"}),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/execute-sync",
        json={
            "trace_id": "trace-video-remix",
            "method": "POST",
            "path": "/v1/videos/task-123/remix",
            "headers": {
                "content-type": "application/json",
                "user-agent": "pytest",
            },
            "body_json": {"prompt": "remix this"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {"video": True}


def test_plan_stream_route_returns_executor_plan_for_gemini_files_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-123",
        "provider_id": "provider-123",
        "endpoint_id": "endpoint-123",
        "key_id": "key-123",
        "method": "GET",
        "url": "https://example.com/v1beta/files/file-123:download",
        "headers": {"x-goog-api-key": "upstream-key"},
        "body": {},
        "stream": True,
        "provider_api_format": "gemini:files",
        "client_api_format": "gemini:files",
        "model_name": "gemini-files",
    }

    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_files_download_stream_plan",
        AsyncMock(return_value=fake_plan),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-files-plan",
            "method": "GET",
            "path": "/v1beta/files/file-123:download",
            "query_string": "alt=media",
            "headers": {"x-goog-api-key": "client-key"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "gemini_files_download",
        "plan": fake_plan,
    }


def test_plan_stream_route_returns_executor_plan_for_openai_video_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-video-123",
        "provider_id": "provider-video-123",
        "endpoint_id": "endpoint-video-123",
        "key_id": "key-video-123",
        "method": "GET",
        "url": "https://api.openai.com/v1/videos/ext-123/content",
        "headers": {"authorization": "Bearer upstream-key"},
        "body": {},
        "stream": True,
        "provider_api_format": "openai:video",
        "client_api_format": "openai:video",
        "model_name": "sora-2",
    }

    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_video_content_stream_plan",
        AsyncMock(return_value=fake_plan),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-video-plan",
            "method": "GET",
            "path": "/v1/videos/task-123/content",
            "query_string": "variant=video",
            "headers": {"authorization": "Bearer client-key"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "openai_video_content",
        "plan": fake_plan,
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_files_get(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-files-get-123",
        "provider_id": "provider-files-123",
        "endpoint_id": "endpoint-files-123",
        "key_id": "file-key-123",
        "method": "GET",
        "url": "https://example.com/v1beta/files/files/abc-123",
        "headers": {"x-goog-api-key": "upstream-key"},
        "body": {},
        "stream": False,
        "provider_api_format": "gemini:files",
        "client_api_format": "gemini:files",
        "model_name": "gemini-files",
    }

    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_files_get_sync_plan",
        AsyncMock(return_value=(fake_plan, {"file_key_id": "file-key-123", "user_id": "user-123"})),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-files-get-plan",
            "method": "GET",
            "path": "/v1beta/files/files/abc-123",
            "query_string": "view=FULL",
            "headers": {"x-goog-api-key": "client-key"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_files_get",
        "plan": fake_plan,
        "report_kind": "gemini_files_store_mapping",
        "report_context": {"file_key_id": "file-key-123", "user_id": "user-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_openai_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-chat-123",
        "provider_id": "provider-openai-chat-123",
        "endpoint_id": "endpoint-openai-chat-123",
        "key_id": "key-openai-chat-123",
        "provider_name": "openai",
        "method": "POST",
        "url": "https://api.openai.example/v1/chat/completions",
        "headers": {"authorization": "Bearer upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"model": "gpt-5", "messages": []}},
        "stream": False,
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:chat",
        "model_name": "gpt-5",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_chat_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="openai_chat_sync",
                plan=fake_plan,
                report_kind="openai_chat_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-chat-plan",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "messages": []},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "openai_chat_sync",
        "plan": fake_plan,
        "report_kind": "openai_chat_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_decision_sync_route_returns_executor_decision_for_openai_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_chat_sync_decision",
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_sync_decision",
                decision_kind="openai_chat_sync",
                request_id="req-openai-chat-decision-123",
                candidate_id="cand-openai-chat-decision-123",
                provider_name="openai",
                provider_id="provider-openai-chat-decision-123",
                endpoint_id="endpoint-openai-chat-decision-123",
                key_id="key-openai-chat-decision-123",
                upstream_base_url="https://api.openai.example",
                auth_header="authorization",
                auth_value="Bearer upstream-key",
                provider_api_format="openai:chat",
                client_api_format="openai:chat",
                model_name="gpt-5",
                mapped_model="gpt-5-upstream",
                prompt_cache_key="cache-key-123",
                extra_headers={"x-extra-upstream": "1"},
                content_type="application/json",
                timeouts={"connect_ms": 1000, "total_ms": 5000},
                report_kind="openai_chat_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-sync",
        json={
            "trace_id": "trace-openai-chat-decision",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "messages": []},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync_decision",
        "decision_kind": "openai_chat_sync",
        "request_id": "req-openai-chat-decision-123",
        "candidate_id": "cand-openai-chat-decision-123",
        "provider_name": "openai",
        "provider_id": "provider-openai-chat-decision-123",
        "endpoint_id": "endpoint-openai-chat-decision-123",
        "key_id": "key-openai-chat-decision-123",
        "upstream_base_url": "https://api.openai.example",
        "auth_header": "authorization",
        "auth_value": "Bearer upstream-key",
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:chat",
        "model_name": "gpt-5",
        "mapped_model": "gpt-5-upstream",
        "prompt_cache_key": "cache-key-123",
        "extra_headers": {"x-extra-upstream": "1"},
        "content_type": "application/json",
        "timeouts": {"connect_ms": 1000, "total_ms": 5000},
        "report_kind": "openai_chat_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_decision_stream_route_returns_executor_decision_for_openai_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_chat_stream_decision",
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_stream_decision",
                decision_kind="openai_chat_stream",
                request_id="req-openai-chat-stream-decision-123",
                candidate_id="cand-openai-chat-stream-decision-123",
                provider_name="openai",
                provider_id="provider-openai-chat-stream-decision-123",
                endpoint_id="endpoint-openai-chat-stream-decision-123",
                key_id="key-openai-chat-stream-decision-123",
                upstream_base_url="https://api.openai.example",
                auth_header="authorization",
                auth_value="Bearer upstream-key",
                provider_api_format="openai:chat",
                client_api_format="openai:chat",
                model_name="gpt-5",
                mapped_model="gpt-5-upstream",
                prompt_cache_key="cache-key-123",
                extra_headers={"x-extra-upstream": "1"},
                content_type="application/json",
                timeouts={"connect_ms": 1000, "read_ms": 2000},
                report_kind="openai_chat_stream_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-stream",
        json={
            "trace_id": "trace-openai-chat-stream-decision",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "messages": [], "stream": True},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream_decision",
        "decision_kind": "openai_chat_stream",
        "request_id": "req-openai-chat-stream-decision-123",
        "candidate_id": "cand-openai-chat-stream-decision-123",
        "provider_name": "openai",
        "provider_id": "provider-openai-chat-stream-decision-123",
        "endpoint_id": "endpoint-openai-chat-stream-decision-123",
        "key_id": "key-openai-chat-stream-decision-123",
        "upstream_base_url": "https://api.openai.example",
        "auth_header": "authorization",
        "auth_value": "Bearer upstream-key",
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:chat",
        "model_name": "gpt-5",
        "mapped_model": "gpt-5-upstream",
        "prompt_cache_key": "cache-key-123",
        "extra_headers": {"x-extra-upstream": "1"},
        "content_type": "application/json",
        "timeouts": {"connect_ms": 1000, "read_ms": 2000},
        "report_kind": "openai_chat_stream_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


@pytest.mark.parametrize(
    ("path", "headers", "decision_kind", "client_api_format", "builder_path", "model_name", "mapped_model", "upstream_url"),
    [
        (
            "/v1/messages",
            {
                "content-type": "application/json",
                "x-api-key": "client-key",
            },
            "claude_chat_stream",
            "claude:chat",
            "src.api.internal.gateway._build_claude_chat_stream_decision",
            "claude-sonnet-4-5",
            "claude-sonnet-4-5-upstream",
            "https://api.anthropic.example/v1/messages",
        ),
        (
            "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
            {
                "content-type": "application/json",
                "x-goog-api-key": "client-key",
            },
            "gemini_chat_stream",
            "gemini:chat",
            "src.api.internal.gateway._build_gemini_chat_stream_decision",
            "gemini-2.5-pro",
            "gemini-2.5-pro-upstream",
            (
                "https://generativelanguage.googleapis.com/v1beta/models/"
                "gemini-2.5-pro-upstream:streamGenerateContent"
            ),
        ),
    ],
)
def test_decision_stream_route_returns_executor_decision_for_claude_and_gemini_chat(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    headers: dict[str, str],
    decision_kind: str,
    client_api_format: str,
    builder_path: str,
    model_name: str,
    mapped_model: str,
    upstream_url: str,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    monkeypatch.setattr(
        builder_path,
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_stream_decision",
                decision_kind=decision_kind,
                request_id="req-stream-decision-123",
                candidate_id="cand-stream-decision-123",
                provider_name="provider-name",
                provider_id="provider-stream-decision-123",
                endpoint_id="endpoint-stream-decision-123",
                key_id="key-stream-decision-123",
                upstream_base_url=upstream_url.split("/v1", 1)[0],
                upstream_url=upstream_url,
                auth_header=headers.get("x-goog-api-key") and "x-goog-api-key" or "x-api-key",
                auth_value="upstream-secret",
                provider_api_format=client_api_format,
                client_api_format=client_api_format,
                model_name=model_name,
                mapped_model=mapped_model,
                prompt_cache_key="cache-key-123",
                extra_headers={"x-extra-upstream": "1"},
                content_type="application/json",
                timeouts={"connect_ms": 1000, "read_ms": 2000},
                report_kind=f"{decision_kind}_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    body_json = (
        {"model": model_name, "messages": [], "stream": True}
        if decision_kind == "claude_chat_stream"
        else {"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-stream",
        json={
            "trace_id": "trace-stream-decision",
            "method": "POST",
            "path": path,
            "headers": headers,
            "body_json": body_json,
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream_decision",
        "decision_kind": decision_kind,
        "request_id": "req-stream-decision-123",
        "candidate_id": "cand-stream-decision-123",
        "provider_name": "provider-name",
        "provider_id": "provider-stream-decision-123",
        "endpoint_id": "endpoint-stream-decision-123",
        "key_id": "key-stream-decision-123",
        "upstream_base_url": upstream_url.split("/v1", 1)[0],
        "upstream_url": upstream_url,
        "auth_header": "x-goog-api-key" if "x-goog-api-key" in headers else "x-api-key",
        "auth_value": "upstream-secret",
        "provider_api_format": client_api_format,
        "client_api_format": client_api_format,
        "model_name": model_name,
        "mapped_model": mapped_model,
        "prompt_cache_key": "cache-key-123",
        "extra_headers": {"x-extra-upstream": "1"},
        "content_type": "application/json",
        "timeouts": {"connect_ms": 1000, "read_ms": 2000},
        "report_kind": f"{decision_kind}_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


@pytest.mark.parametrize(
    (
        "path",
        "headers",
        "decision_kind",
        "client_api_format",
        "builder_path",
        "model_name",
        "mapped_model",
        "upstream_url",
        "auth_header",
        "body_json",
    ),
    [
        (
            "/v1/responses",
            {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "openai_cli_stream",
            "openai:cli",
            "src.api.internal.gateway._build_openai_cli_stream_decision",
            "gpt-5",
            "gpt-5-upstream",
            None,
            "authorization",
            {"model": "gpt-5", "input": "hello", "stream": True},
        ),
        (
            "/v1/messages",
            {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "claude_cli_stream",
            "claude:cli",
            "src.api.internal.gateway._build_claude_cli_stream_decision",
            "claude-code",
            "claude-code-upstream",
            "https://api.anthropic.example/v1/messages",
            "authorization",
            {"model": "claude-code", "messages": [], "stream": True},
        ),
        (
            "/v1beta/models/gemini-cli:streamGenerateContent",
            {
                "content-type": "application/json",
                "user-agent": "GeminiCLI/1.0",
                "x-goog-api-key": "client-key",
            },
            "gemini_cli_stream",
            "gemini:cli",
            "src.api.internal.gateway._build_gemini_cli_stream_decision",
            "gemini-cli",
            "gemini-cli-upstream",
            (
                "https://generativelanguage.googleapis.com/v1beta/models/"
                "gemini-cli-upstream:streamGenerateContent"
            ),
            "x-goog-api-key",
            {"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
        ),
    ],
)
def test_decision_stream_route_returns_executor_decision_for_cli_variants(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    headers: dict[str, str],
    decision_kind: str,
    client_api_format: str,
    builder_path: str,
    model_name: str,
    mapped_model: str,
    upstream_url: str | None,
    auth_header: str,
    body_json: dict[str, Any],
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    monkeypatch.setattr(
        builder_path,
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_stream_decision",
                decision_kind=decision_kind,
                request_id="req-cli-stream-decision-123",
                candidate_id="cand-cli-stream-decision-123",
                provider_name="provider-name",
                provider_id="provider-cli-stream-decision-123",
                endpoint_id="endpoint-cli-stream-decision-123",
                key_id="key-cli-stream-decision-123",
                upstream_base_url=(
                    "https://api.openai.example"
                    if upstream_url is None
                    else upstream_url.split("/v1", 1)[0]
                ),
                upstream_url=upstream_url,
                auth_header=auth_header,
                auth_value="upstream-secret",
                provider_api_format=client_api_format,
                client_api_format=client_api_format,
                model_name=model_name,
                mapped_model=mapped_model,
                prompt_cache_key="cache-key-123",
                extra_headers={"x-extra-upstream": "1"},
                content_type="application/json",
                timeouts={"connect_ms": 1000, "read_ms": 2000},
                report_kind=f"{decision_kind}_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-stream",
        json={
            "trace_id": "trace-cli-stream-decision",
            "method": "POST",
            "path": path,
            "headers": headers,
            "body_json": body_json,
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    expected = {
        "action": "executor_stream_decision",
        "decision_kind": decision_kind,
        "request_id": "req-cli-stream-decision-123",
        "candidate_id": "cand-cli-stream-decision-123",
        "provider_name": "provider-name",
        "provider_id": "provider-cli-stream-decision-123",
        "endpoint_id": "endpoint-cli-stream-decision-123",
        "key_id": "key-cli-stream-decision-123",
        "upstream_base_url": (
            "https://api.openai.example"
            if upstream_url is None
            else upstream_url.split("/v1", 1)[0]
        ),
        "auth_header": auth_header,
        "auth_value": "upstream-secret",
        "provider_api_format": client_api_format,
        "client_api_format": client_api_format,
        "model_name": model_name,
        "mapped_model": mapped_model,
        "prompt_cache_key": "cache-key-123",
        "extra_headers": {"x-extra-upstream": "1"},
        "content_type": "application/json",
        "timeouts": {"connect_ms": 1000, "read_ms": 2000},
        "report_kind": f"{decision_kind}_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }
    if upstream_url is not None:
        expected["upstream_url"] = upstream_url
    assert response.json() == expected


@pytest.mark.parametrize(
    ("path", "decision_kind", "client_api_format"),
    [
        ("/v1/responses", "openai_cli_sync", "openai:cli"),
        ("/v1/responses/compact", "openai_compact_sync", "openai:compact"),
    ],
)
def test_decision_sync_route_returns_executor_decision_for_openai_cli_variants(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    decision_kind: str,
    client_api_format: str,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_cli_sync_decision",
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_sync_decision",
                decision_kind=decision_kind,
                request_id="req-openai-cli-decision-123",
                candidate_id="cand-openai-cli-decision-123",
                provider_name="openai",
                provider_id="provider-openai-cli-decision-123",
                endpoint_id="endpoint-openai-cli-decision-123",
                key_id="key-openai-cli-decision-123",
                upstream_base_url="https://api.openai.example",
                auth_header="authorization",
                auth_value="Bearer upstream-key",
                provider_api_format=client_api_format,
                client_api_format=client_api_format,
                model_name="gpt-5",
                mapped_model="gpt-5-upstream",
                prompt_cache_key="cache-key-123",
                extra_headers={"x-extra-upstream": "1"},
                content_type="application/json",
                timeouts={"connect_ms": 1000, "total_ms": 5000},
                report_kind="openai_cli_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-sync",
        json={
            "trace_id": "trace-openai-cli-decision",
            "method": "POST",
            "path": path,
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "input": "hello"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync_decision",
        "decision_kind": decision_kind,
        "request_id": "req-openai-cli-decision-123",
        "candidate_id": "cand-openai-cli-decision-123",
        "provider_name": "openai",
        "provider_id": "provider-openai-cli-decision-123",
        "endpoint_id": "endpoint-openai-cli-decision-123",
        "key_id": "key-openai-cli-decision-123",
        "upstream_base_url": "https://api.openai.example",
        "auth_header": "authorization",
        "auth_value": "Bearer upstream-key",
        "provider_api_format": client_api_format,
        "client_api_format": client_api_format,
        "model_name": "gpt-5",
        "mapped_model": "gpt-5-upstream",
        "prompt_cache_key": "cache-key-123",
        "extra_headers": {"x-extra-upstream": "1"},
        "content_type": "application/json",
        "timeouts": {"connect_ms": 1000, "total_ms": 5000},
        "report_kind": "openai_cli_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


@pytest.mark.parametrize(
    ("headers", "decision_kind", "client_api_format"),
    [
        (
            {
                "content-type": "application/json",
                "x-api-key": "client-key",
            },
            "claude_chat_sync",
            "claude:chat",
        ),
        (
            {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "claude_cli_sync",
            "claude:cli",
        ),
    ],
)
def test_decision_sync_route_returns_executor_decision_for_claude_variants(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    decision_kind: str,
    client_api_format: str,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    target = (
        "src.api.internal.gateway._build_claude_cli_sync_decision"
        if decision_kind == "claude_cli_sync"
        else "src.api.internal.gateway._build_claude_chat_sync_decision"
    )
    monkeypatch.setattr(
        target,
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_sync_decision",
                decision_kind=decision_kind,
                request_id="req-claude-decision-123",
                candidate_id="cand-claude-decision-123",
                provider_name="claude",
                provider_id="provider-claude-decision-123",
                endpoint_id="endpoint-claude-decision-123",
                key_id="key-claude-decision-123",
                upstream_base_url="https://api.anthropic.example",
                upstream_url="https://api.anthropic.example/v1/messages",
                auth_header="x-api-key" if decision_kind == "claude_chat_sync" else "authorization",
                auth_value="upstream-secret",
                provider_api_format=client_api_format,
                client_api_format=client_api_format,
                model_name="claude-sonnet-4-5",
                mapped_model="claude-sonnet-4-5-upstream",
                prompt_cache_key="cache-key-123",
                extra_headers={"x-extra-upstream": "1"},
                content_type="application/json",
                timeouts={"connect_ms": 1000, "total_ms": 5000},
                report_kind=(
                    "claude_cli_sync_success"
                    if decision_kind == "claude_cli_sync"
                    else "claude_chat_sync_success"
                ),
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-sync",
        json={
            "trace_id": "trace-claude-decision",
            "method": "POST",
            "path": "/v1/messages",
            "headers": headers,
            "body_json": {"model": "claude-sonnet-4-5", "messages": []},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync_decision",
        "decision_kind": decision_kind,
        "request_id": "req-claude-decision-123",
        "candidate_id": "cand-claude-decision-123",
        "provider_name": "claude",
        "provider_id": "provider-claude-decision-123",
        "endpoint_id": "endpoint-claude-decision-123",
        "key_id": "key-claude-decision-123",
        "upstream_base_url": "https://api.anthropic.example",
        "upstream_url": "https://api.anthropic.example/v1/messages",
        "auth_header": "x-api-key" if decision_kind == "claude_chat_sync" else "authorization",
        "auth_value": "upstream-secret",
        "provider_api_format": client_api_format,
        "client_api_format": client_api_format,
        "model_name": "claude-sonnet-4-5",
        "mapped_model": "claude-sonnet-4-5-upstream",
        "prompt_cache_key": "cache-key-123",
        "extra_headers": {"x-extra-upstream": "1"},
        "content_type": "application/json",
        "timeouts": {"connect_ms": 1000, "total_ms": 5000},
        "report_kind": (
            "claude_cli_sync_success"
            if decision_kind == "claude_cli_sync"
            else "claude_chat_sync_success"
        ),
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


@pytest.mark.parametrize(
    ("path", "headers", "decision_kind", "client_api_format", "model_name", "mapped_model"),
    [
        (
            "/v1beta/models/gemini-2.5-pro:generateContent",
            {
                "content-type": "application/json",
                "x-goog-api-key": "client-key",
            },
            "gemini_chat_sync",
            "gemini:chat",
            "gemini-2.5-pro",
            "gemini-2.5-pro-upstream",
        ),
        (
            "/v1beta/models/gemini-cli:generateContent",
            {
                "content-type": "application/json",
                "user-agent": "GeminiCLI/1.0",
                "x-goog-api-key": "client-key",
            },
            "gemini_cli_sync",
            "gemini:cli",
            "gemini-cli",
            "gemini-cli-upstream",
        ),
    ],
)
def test_decision_sync_route_returns_executor_decision_for_gemini_variants(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    headers: dict[str, str],
    decision_kind: str,
    client_api_format: str,
    model_name: str,
    mapped_model: str,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    monkeypatch.setattr(
        (
            "src.api.internal.gateway._build_gemini_cli_sync_decision"
            if decision_kind == "gemini_cli_sync"
            else "src.api.internal.gateway._build_gemini_chat_sync_decision"
        ),
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_sync_decision",
                decision_kind=decision_kind,
                request_id="req-gemini-decision-123",
                candidate_id="cand-gemini-decision-123",
                provider_name="gemini",
                provider_id="provider-gemini-decision-123",
                endpoint_id="endpoint-gemini-decision-123",
                key_id="key-gemini-decision-123",
                upstream_base_url="https://generativelanguage.googleapis.com",
                upstream_url=(
                    "https://generativelanguage.googleapis.com/v1beta/models/"
                    f"{mapped_model}:generateContent"
                ),
                auth_header="x-goog-api-key",
                auth_value="upstream-key",
                provider_api_format=client_api_format,
                client_api_format=client_api_format,
                model_name=model_name,
                mapped_model=mapped_model,
                prompt_cache_key="cache-key-123",
                extra_headers={"x-extra-upstream": "1"},
                content_type="application/json",
                timeouts={"connect_ms": 1000, "total_ms": 5000},
                report_kind=(
                    "gemini_cli_sync_success"
                    if decision_kind == "gemini_cli_sync"
                    else "gemini_chat_sync_success"
                ),
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-sync",
        json={
            "trace_id": "trace-gemini-decision",
            "method": "POST",
            "path": path,
            "headers": headers,
            "body_json": {"contents": [{"role": "user", "parts": [{"text": "hello"}]}]},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync_decision",
        "decision_kind": decision_kind,
        "request_id": "req-gemini-decision-123",
        "candidate_id": "cand-gemini-decision-123",
        "provider_name": "gemini",
        "provider_id": "provider-gemini-decision-123",
        "endpoint_id": "endpoint-gemini-decision-123",
        "key_id": "key-gemini-decision-123",
        "upstream_base_url": "https://generativelanguage.googleapis.com",
        "upstream_url": (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{mapped_model}:generateContent"
        ),
        "auth_header": "x-goog-api-key",
        "auth_value": "upstream-key",
        "provider_api_format": client_api_format,
        "client_api_format": client_api_format,
        "model_name": model_name,
        "mapped_model": mapped_model,
        "prompt_cache_key": "cache-key-123",
        "extra_headers": {"x-extra-upstream": "1"},
        "content_type": "application/json",
        "timeouts": {"connect_ms": 1000, "total_ms": 5000},
        "report_kind": (
            "gemini_cli_sync_success"
            if decision_kind == "gemini_cli_sync"
            else "gemini_chat_sync_success"
        ),
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_decision_sync_route_returns_executor_decision_for_gemini_files_get(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id=auth_context.user_id),
            SimpleNamespace(id=auth_context.api_key_id),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_execute_auth_context",
        AsyncMock(
            return_value=GatewayAuthContext(
                user_id="user-files-decision-123",
                api_key_id="key-files-decision-123",
                access_allowed=True,
            )
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_files_proxy_sync_decision",
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_sync_decision",
                decision_kind="gemini_files_get",
                request_id="req-files-get-decision-123",
                provider_name="gemini",
                provider_id="provider-files-decision-123",
                endpoint_id="endpoint-files-decision-123",
                key_id="key-files-decision-123",
                upstream_base_url="https://generativelanguage.googleapis.com",
                upstream_url="https://generativelanguage.googleapis.com/v1beta/files/files/abc-123",
                auth_header="",
                auth_value="",
                provider_api_format="gemini:files",
                client_api_format="gemini:files",
                model_name="gemini-files",
                provider_request_headers={"x-goog-api-key": "provider-key"},
                report_kind="gemini_files_store_mapping",
                report_context={
                    "file_key_id": "key-files-decision-123",
                    "user_id": "user-files-decision-123",
                },
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-sync",
        json={
            "trace_id": "trace-files-get-decision-123",
            "method": "GET",
            "path": "/v1beta/files/files/abc-123",
            "headers": {"x-goog-api-key": "client-key"},
            "body_json": {},
        },
    )

    assert response.status_code == 200
    assert response.json()["decision_kind"] == "gemini_files_get"
    assert response.json()["provider_api_format"] == "gemini:files"


@pytest.mark.parametrize(
    (
        "method",
        "path",
        "body_json",
        "builder_target",
        "decision_kind",
        "client_api_format",
        "model_name",
        "provider_request_method",
        "provider_request_body",
        "upstream_url",
    ),
    [
        (
            "POST",
            "/v1/videos",
            {"model": "sora-2", "prompt": "hello"},
            "src.api.internal.gateway._build_openai_video_create_sync_decision",
            "openai_video_create_sync",
            "openai:video",
            "sora-2",
            "POST",
            {"model": "sora-2", "prompt": "hello"},
            "https://api.openai.example/v1/videos",
        ),
        (
            "POST",
            "/v1/videos/task-123/remix",
            {"model": "sora-2", "prompt": "remix me"},
            "src.api.internal.gateway._build_openai_video_remix_sync_decision",
            "openai_video_remix_sync",
            "openai:video",
            "sora-2",
            "POST",
            {"model": "sora-2", "prompt": "remix me"},
            "https://api.openai.example/v1/videos/ext-123/remix",
        ),
        (
            "POST",
            "/v1/videos/task-123/cancel",
            {},
            "src.api.internal.gateway._build_openai_video_cancel_sync_decision",
            "openai_video_cancel_sync",
            "openai:video",
            "sora-2",
            "DELETE",
            None,
            "https://api.openai.example/v1/videos/ext-123",
        ),
        (
            "DELETE",
            "/v1/videos/task-123",
            {},
            "src.api.internal.gateway._build_openai_video_delete_sync_decision",
            "openai_video_delete_sync",
            "openai:video",
            "sora-2",
            "DELETE",
            None,
            "https://api.openai.example/v1/videos/ext-123",
        ),
        (
            "POST",
            "/v1beta/models/veo-3:predictLongRunning",
            {"instances": [{"prompt": "hello"}]},
            "src.api.internal.gateway._build_gemini_video_create_sync_decision",
            "gemini_video_create_sync",
            "gemini:video",
            "veo-3",
            "POST",
            {"instances": [{"prompt": "hello"}]},
            "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
        ),
        (
            "POST",
            "/v1beta/models/veo-3/operations/ext-123:cancel",
            {},
            "src.api.internal.gateway._build_gemini_video_cancel_sync_decision",
            "gemini_video_cancel_sync",
            "gemini:video",
            "veo-3",
            "POST",
            {},
            "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-123:cancel",
        ),
    ],
)
def test_decision_sync_route_returns_executor_decision_for_video_variants(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    body_json: dict[str, Any],
    builder_target: str,
    decision_kind: str,
    client_api_format: str,
    model_name: str,
    provider_request_method: str,
    provider_request_body: dict[str, Any] | None,
    upstream_url: str,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        builder_target,
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_sync_decision",
                decision_kind=decision_kind,
                request_id="req-video-sync-decision-123",
                provider_name="provider-name",
                provider_id="provider-video-sync-decision-123",
                endpoint_id="endpoint-video-sync-decision-123",
                key_id="key-video-sync-decision-123",
                upstream_base_url=upstream_url.split("/v1", 1)[0],
                upstream_url=upstream_url,
                provider_request_method=provider_request_method,
                auth_header="",
                auth_value="",
                provider_api_format=client_api_format,
                client_api_format=client_api_format,
                model_name=model_name,
                provider_request_headers={"authorization": "Bearer upstream-key"},
                provider_request_body=provider_request_body,
                content_type="application/json" if provider_request_body is not None else None,
                timeouts={"connect_ms": 1000, "total_ms": 5000},
                report_kind=f"{decision_kind}_finalize",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-sync",
        json={
            "trace_id": "trace-video-sync-decision-123",
            "method": method,
            "path": path,
            "headers": {"authorization": "Bearer client-key"},
            "body_json": body_json,
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    expected = {
        "action": "executor_sync_decision",
        "decision_kind": decision_kind,
        "request_id": "req-video-sync-decision-123",
        "provider_name": "provider-name",
        "provider_id": "provider-video-sync-decision-123",
        "endpoint_id": "endpoint-video-sync-decision-123",
        "key_id": "key-video-sync-decision-123",
        "upstream_base_url": upstream_url.split("/v1", 1)[0],
        "upstream_url": upstream_url,
        "provider_request_method": provider_request_method,
        "auth_header": "",
        "auth_value": "",
        "provider_api_format": client_api_format,
        "client_api_format": client_api_format,
        "model_name": model_name,
        "extra_headers": {},
        "provider_request_headers": {"authorization": "Bearer upstream-key"},
        "timeouts": {"connect_ms": 1000, "total_ms": 5000},
        "report_kind": f"{decision_kind}_finalize",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }
    if provider_request_body is not None:
        expected["provider_request_body"] = provider_request_body
        expected["content_type"] = "application/json"

    assert response.json() == expected


def test_decision_stream_route_returns_executor_decision_for_gemini_files_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id=auth_context.user_id),
            SimpleNamespace(id=auth_context.api_key_id),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_execute_auth_context",
        AsyncMock(
            return_value=GatewayAuthContext(
                user_id="user-files-download-decision-123",
                api_key_id="key-files-download-decision-123",
                access_allowed=True,
            )
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_files_download_stream_decision",
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_stream_decision",
                decision_kind="gemini_files_download",
                request_id="req-files-download-decision-123",
                provider_name="gemini",
                provider_id="provider-files-download-decision-123",
                endpoint_id="endpoint-files-download-decision-123",
                key_id="key-files-download-decision-123",
                upstream_base_url="",
                upstream_url=(
                    "https://generativelanguage.googleapis.com/"
                    "v1beta/files/files/abc-123:download?alt=media"
                ),
                auth_header="",
                auth_value="",
                provider_api_format="gemini:files",
                client_api_format="gemini:files",
                model_name="gemini-files",
                provider_request_headers={"x-goog-api-key": "provider-key"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-stream",
        json={
            "trace_id": "trace-files-download-decision-123",
            "method": "GET",
            "path": "/v1beta/files/file-123:download",
            "query_string": "alt=media",
            "headers": {"x-goog-api-key": "client-key"},
            "body_json": {},
        },
    )

    assert response.status_code == 200
    assert response.json()["decision_kind"] == "gemini_files_download"


def test_decision_stream_route_returns_executor_decision_for_openai_video_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id=auth_context.user_id),
            SimpleNamespace(id=auth_context.api_key_id),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_execute_auth_context",
        AsyncMock(
            return_value=GatewayAuthContext(
                user_id="user-video-content-decision-123",
                api_key_id="key-video-content-decision-123",
                access_allowed=True,
            )
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_video_content_stream_decision",
        AsyncMock(
            return_value=GatewayExecutionDecisionResponse(
                action="executor_stream_decision",
                decision_kind="openai_video_content",
                request_id="req-video-content-decision-123",
                provider_name="openai",
                provider_id="provider-video-content-decision-123",
                endpoint_id="endpoint-video-content-decision-123",
                key_id="key-video-content-decision-123",
                upstream_base_url="",
                upstream_url="https://cdn.example.com/video.mp4",
                auth_header="",
                auth_value="",
                provider_api_format="openai:video",
                client_api_format="openai:video",
                model_name="sora-2",
                provider_request_headers={},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/decision-stream",
        json={
            "trace_id": "trace-video-content-decision-123",
            "method": "GET",
            "path": "/v1/videos/task-123/content",
            "headers": {"authorization": "Bearer client-key"},
            "body_json": {},
        },
    )

    assert response.status_code == 200
    assert response.json()["decision_kind"] == "openai_video_content"


def test_plan_sync_route_resolves_auth_context_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    build_plan = AsyncMock(
        return_value=GatewayExecutionPlanResponse(
            action="executor_sync",
            plan_kind="openai_chat_sync",
            plan={
                "request_id": "plan-openai-chat-derived-auth-123",
                "provider_name": "openai",
                "method": "POST",
                "url": "https://api.openai.example/v1/chat/completions",
                "headers": {"content-type": "application/json"},
                "body": {"json_body": {"model": "gpt-5", "messages": []}},
                "stream": False,
                "provider_api_format": "openai:chat",
                "client_api_format": "openai:chat",
                "model_name": "gpt-5",
            },
            report_kind="openai_chat_sync_success",
            report_context={"user_id": "user-123", "api_key_id": "key-123"},
        )
    )
    monkeypatch.setattr("src.api.internal.gateway._build_openai_chat_sync_plan", build_plan)
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_auth_context_signature",
        AsyncMock(
            return_value={
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            }
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-chat-plan-derived-auth",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "messages": []},
        },
    )

    assert response.status_code == 200
    assert response.json()["action"] == "executor_sync"
    assert response.json()["auth_context"] == {
        "user_id": "user-123",
        "api_key_id": "key-123",
        "access_allowed": True,
    }
    assert build_plan.await_args.kwargs["auth_context"].user_id == "user-123"
    assert build_plan.await_args.kwargs["auth_context"].api_key_id == "key-123"


def test_plan_sync_route_returns_executor_plan_for_openai_video_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-video-create-123",
        "provider_id": "provider-openai-video-create-123",
        "endpoint_id": "endpoint-openai-video-create-123",
        "key_id": "key-openai-video-create-123",
        "provider_name": "openai",
        "method": "POST",
        "url": "https://api.openai.example/v1/videos",
        "headers": {"authorization": "Bearer upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"model": "sora-2", "prompt": "hello"}},
        "stream": False,
        "provider_api_format": "openai:video",
        "client_api_format": "openai:video",
        "model_name": "sora-2",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_video_create_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="openai_video_create_sync",
                plan=fake_plan,
                report_kind="openai_video_create_sync_finalize",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-video-create-plan",
            "method": "POST",
            "path": "/v1/videos",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "sora-2", "prompt": "hello"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "openai_video_create_sync",
        "plan": fake_plan,
        "report_kind": "openai_video_create_sync_finalize",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_openai_video_remix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-video-remix-123",
        "provider_id": "provider-openai-video-remix-123",
        "endpoint_id": "endpoint-openai-video-remix-123",
        "key_id": "key-openai-video-remix-123",
        "provider_name": "openai",
        "method": "POST",
        "url": "https://api.openai.example/v1/videos/ext-123/remix",
        "headers": {"authorization": "Bearer upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"prompt": "remix this"}},
        "stream": False,
        "provider_api_format": "openai:video",
        "client_api_format": "openai:video",
        "model_name": "sora-2",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_video_remix_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="openai_video_remix_sync",
                plan=fake_plan,
                report_kind="openai_video_remix_sync_finalize",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-video-remix-plan",
            "method": "POST",
            "path": "/v1/videos/task-123/remix",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"prompt": "remix this"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "openai_video_remix_sync",
        "plan": fake_plan,
        "report_kind": "openai_video_remix_sync_finalize",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_video_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-gemini-video-create-123",
        "provider_id": "provider-gemini-video-create-123",
        "endpoint_id": "endpoint-gemini-video-create-123",
        "key_id": "key-gemini-video-create-123",
        "provider_name": "gemini",
        "method": "POST",
        "url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
        "headers": {"x-goog-api-key": "upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"prompt": "make a video"}},
        "stream": False,
        "provider_api_format": "gemini:video",
        "client_api_format": "gemini:video",
        "model_name": "veo-3",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_video_create_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_video_create_sync",
                plan=fake_plan,
                report_kind="gemini_video_create_sync_finalize",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-gemini-video-create-plan",
            "method": "POST",
            "path": "/v1beta/models/veo-3:predictLongRunning",
            "headers": {
                "content-type": "application/json",
                "x-goog-api-key": "client-key",
            },
            "body_json": {"prompt": "make a video"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_video_create_sync",
        "plan": fake_plan,
        "report_kind": "gemini_video_create_sync_finalize",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_openai_video_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-video-cancel-123",
        "provider_id": "provider-openai-video-cancel-123",
        "endpoint_id": "endpoint-openai-video-cancel-123",
        "key_id": "key-openai-video-cancel-123",
        "provider_name": "openai",
        "method": "DELETE",
        "url": "https://api.openai.example/v1/videos/ext-123",
        "headers": {"authorization": "Bearer upstream-key"},
        "body": {},
        "stream": False,
        "provider_api_format": "openai:video",
        "client_api_format": "openai:video",
        "model_name": "sora-2",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_video_cancel_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="openai_video_cancel_sync",
                plan=fake_plan,
                report_kind="openai_video_cancel_sync_finalize",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-video-cancel-plan",
            "method": "POST",
            "path": "/v1/videos/task-123/cancel",
            "headers": {
                "authorization": "Bearer client-key",
            },
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "openai_video_cancel_sync",
        "plan": fake_plan,
        "report_kind": "openai_video_cancel_sync_finalize",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_openai_video_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-video-delete-123",
        "provider_id": "provider-openai-video-delete-123",
        "endpoint_id": "endpoint-openai-video-delete-123",
        "key_id": "key-openai-video-delete-123",
        "provider_name": "openai",
        "method": "DELETE",
        "url": "https://api.openai.example/v1/videos/ext-123",
        "headers": {"authorization": "Bearer upstream-key"},
        "body": {},
        "stream": False,
        "provider_api_format": "openai:video",
        "client_api_format": "openai:video",
        "model_name": "sora-2",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_video_delete_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="openai_video_delete_sync",
                plan=fake_plan,
                report_kind="openai_video_delete_sync_finalize",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-video-delete-plan",
            "method": "DELETE",
            "path": "/v1/videos/task-123",
            "headers": {
                "authorization": "Bearer client-key",
            },
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "openai_video_delete_sync",
        "plan": fake_plan,
        "report_kind": "openai_video_delete_sync_finalize",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_video_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-gemini-video-cancel-123",
        "provider_id": "provider-gemini-video-cancel-123",
        "endpoint_id": "endpoint-gemini-video-cancel-123",
        "key_id": "key-gemini-video-cancel-123",
        "provider_name": "gemini",
        "method": "POST",
        "url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-123:cancel",
        "headers": {"authorization": "Bearer upstream-key", "content-type": "application/json"},
        "body": {"json_body": {}},
        "stream": False,
        "provider_api_format": "gemini:video",
        "client_api_format": "gemini:video",
        "model_name": "veo-3",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_video_cancel_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_video_cancel_sync",
                plan=fake_plan,
                report_kind="gemini_video_cancel_sync_finalize",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-gemini-video-cancel-plan",
            "method": "POST",
            "path": "/v1beta/models/veo-3/operations/ext-123:cancel",
            "headers": {
                "x-goog-api-key": "client-key",
            },
            "body_json": {},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_video_cancel_sync",
        "plan": fake_plan,
        "report_kind": "gemini_video_cancel_sync_finalize",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_stream_route_returns_executor_plan_for_openai_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-chat-stream-123",
        "provider_id": "provider-openai-chat-stream-123",
        "endpoint_id": "endpoint-openai-chat-stream-123",
        "key_id": "key-openai-chat-stream-123",
        "provider_name": "openai",
        "method": "POST",
        "url": "https://api.openai.example/v1/chat/completions",
        "headers": {
            "authorization": "Bearer upstream-key",
            "content-type": "application/json",
            "accept": "text/event-stream",
        },
        "body": {
            "json_body": {
                "model": "gpt-5",
                "messages": [],
                "stream": True,
            }
        },
        "stream": True,
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:chat",
        "model_name": "gpt-5",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_chat_stream_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_stream",
                plan_kind="openai_chat_stream",
                plan=fake_plan,
                report_kind="openai_chat_stream_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-openai-chat-stream-plan",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "messages": [], "stream": True},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "openai_chat_stream",
        "plan": fake_plan,
        "report_kind": "openai_chat_stream_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_stream_route_resolves_auth_context_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    build_plan = AsyncMock(
        return_value=GatewayExecutionPlanResponse(
            action="executor_stream",
            plan_kind="openai_chat_stream",
            plan={
                "request_id": "plan-openai-chat-stream-derived-auth-123",
                "provider_name": "openai",
                "method": "POST",
                "url": "https://api.openai.example/v1/chat/completions",
                "headers": {
                    "content-type": "application/json",
                    "accept": "text/event-stream",
                },
                "body": {
                    "json_body": {"model": "gpt-5", "messages": [], "stream": True}
                },
                "stream": True,
                "provider_api_format": "openai:chat",
                "client_api_format": "openai:chat",
                "model_name": "gpt-5",
            },
            report_kind="openai_chat_stream_success",
            report_context={"user_id": "user-123", "api_key_id": "key-123"},
        )
    )
    monkeypatch.setattr("src.api.internal.gateway._build_openai_chat_stream_plan", build_plan)
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_auth_context_signature",
        AsyncMock(
            return_value={
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            }
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-openai-chat-stream-plan-derived-auth",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "messages": [], "stream": True},
        },
    )

    assert response.status_code == 200
    assert response.json()["action"] == "executor_stream"
    assert build_plan.await_args.kwargs["auth_context"].user_id == "user-123"
    assert build_plan.await_args.kwargs["auth_context"].api_key_id == "key-123"


def test_plan_stream_route_returns_executor_plan_for_claude_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-claude-chat-stream-123",
        "provider_id": "provider-claude-chat-stream-123",
        "endpoint_id": "endpoint-claude-chat-stream-123",
        "key_id": "key-claude-chat-stream-123",
        "provider_name": "claude",
        "method": "POST",
        "url": "https://api.anthropic.example/v1/messages",
        "headers": {
            "x-api-key": "upstream-key",
            "content-type": "application/json",
            "accept": "text/event-stream",
        },
        "body": {
            "json_body": {
                "model": "claude-sonnet-4",
                "messages": [],
                "stream": True,
            }
        },
        "stream": True,
        "provider_api_format": "claude:chat",
        "client_api_format": "claude:chat",
        "model_name": "claude-sonnet-4",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_claude_chat_stream_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_stream",
                plan_kind="claude_chat_stream",
                plan=fake_plan,
                report_kind="claude_chat_stream_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-claude-chat-stream-plan",
            "method": "POST",
            "path": "/v1/messages",
            "headers": {
                "content-type": "application/json",
                "x-api-key": "client-key",
            },
            "body_json": {"model": "claude-sonnet-4", "messages": [], "stream": True},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "claude_chat_stream",
        "plan": fake_plan,
        "report_kind": "claude_chat_stream_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_stream_route_returns_executor_plan_for_gemini_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-gemini-chat-stream-123",
        "provider_id": "provider-gemini-chat-stream-123",
        "endpoint_id": "endpoint-gemini-chat-stream-123",
        "key_id": "key-gemini-chat-stream-123",
        "provider_name": "gemini",
        "method": "POST",
        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        "headers": {
            "x-goog-api-key": "upstream-key",
            "content-type": "application/json",
            "accept": "text/event-stream",
        },
        "body": {"json_body": {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}},
        "stream": True,
        "provider_api_format": "gemini:chat",
        "client_api_format": "gemini:chat",
        "model_name": "gemini-2.5-pro",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_chat_stream_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_stream",
                plan_kind="gemini_chat_stream",
                plan=fake_plan,
                report_kind="gemini_chat_stream_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-gemini-chat-stream-plan",
            "method": "POST",
            "path": "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
            "headers": {
                "content-type": "application/json",
                "x-goog-api-key": "client-key",
            },
            "body_json": {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "gemini_chat_stream",
        "plan": fake_plan,
        "report_kind": "gemini_chat_stream_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_stream_route_returns_executor_plan_for_openai_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-cli-stream-123",
        "provider_id": "provider-openai-cli-stream-123",
        "endpoint_id": "endpoint-openai-cli-stream-123",
        "key_id": "key-openai-cli-stream-123",
        "provider_name": "openai",
        "method": "POST",
        "url": "https://api.openai.example/v1/responses",
        "headers": {
            "authorization": "Bearer upstream-key",
            "content-type": "application/json",
            "accept": "text/event-stream",
        },
        "body": {"json_body": {"model": "gpt-5", "input": "hello", "stream": True}},
        "stream": True,
        "provider_api_format": "openai:cli",
        "client_api_format": "openai:cli",
        "model_name": "gpt-5",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_cli_stream_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_stream",
                plan_kind="openai_cli_stream",
                plan=fake_plan,
                report_kind="openai_cli_stream_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-openai-cli-stream-plan",
            "method": "POST",
            "path": "/v1/responses",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "input": "hello", "stream": True},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "openai_cli_stream",
        "plan": fake_plan,
        "report_kind": "openai_cli_stream_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_stream_route_returns_executor_plan_for_claude_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-claude-cli-stream-123",
        "provider_id": "provider-claude-cli-stream-123",
        "endpoint_id": "endpoint-claude-cli-stream-123",
        "key_id": "key-claude-cli-stream-123",
        "provider_name": "claude",
        "method": "POST",
        "url": "https://api.anthropic.example/v1/messages",
        "headers": {
            "x-api-key": "upstream-key",
            "content-type": "application/json",
            "accept": "text/event-stream",
        },
        "body": {
            "json_body": {
                "model": "claude-sonnet-4",
                "messages": [],
                "stream": True,
            }
        },
        "stream": True,
        "provider_api_format": "claude:cli",
        "client_api_format": "claude:cli",
        "model_name": "claude-sonnet-4",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_claude_cli_stream_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_stream",
                plan_kind="claude_cli_stream",
                plan=fake_plan,
                report_kind="claude_cli_stream_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-claude-cli-stream-plan",
            "method": "POST",
            "path": "/v1/messages",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
                "anthropic-beta": "output-128k-2025-02-19",
            },
            "body_json": {
                "model": "claude-sonnet-4",
                "messages": [],
                "stream": True,
            },
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "claude_cli_stream",
        "plan": fake_plan,
        "report_kind": "claude_cli_stream_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_stream_route_returns_executor_plan_for_gemini_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-gemini-cli-stream-123",
        "provider_id": "provider-gemini-cli-stream-123",
        "endpoint_id": "endpoint-gemini-cli-stream-123",
        "key_id": "key-gemini-cli-stream-123",
        "provider_name": "gemini",
        "method": "POST",
        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli:streamGenerateContent",
        "headers": {
            "x-goog-api-key": "upstream-key",
            "content-type": "application/json",
            "accept": "text/event-stream",
        },
        "body": {"json_body": {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}},
        "stream": True,
        "provider_api_format": "gemini:cli",
        "client_api_format": "gemini:cli",
        "model_name": "gemini-cli",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_cli_stream_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_stream",
                plan_kind="gemini_cli_stream",
                plan=fake_plan,
                report_kind="gemini_cli_stream_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-stream",
        json={
            "trace_id": "trace-gemini-cli-stream-plan",
            "method": "POST",
            "path": "/v1beta/models/gemini-cli:streamGenerateContent",
            "headers": {
                "content-type": "application/json",
                "user-agent": "GeminiCLI/1.0",
                "x-goog-api-key": "client-key",
            },
            "body_json": {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_stream",
        "plan_kind": "gemini_cli_stream",
        "plan": fake_plan,
        "report_kind": "gemini_cli_stream_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_openai_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-cli-123",
        "provider_id": "provider-openai-cli-123",
        "endpoint_id": "endpoint-openai-cli-123",
        "key_id": "key-openai-cli-123",
        "provider_name": "openai",
        "method": "POST",
        "url": "https://api.openai.example/v1/responses",
        "headers": {"authorization": "Bearer upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"model": "gpt-5", "input": "hello"}},
        "stream": False,
        "provider_api_format": "openai:cli",
        "client_api_format": "openai:cli",
        "model_name": "gpt-5",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_cli_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="openai_cli_sync",
                plan=fake_plan,
                report_kind="openai_cli_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-cli-plan",
            "method": "POST",
            "path": "/v1/responses",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "input": "hello"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "openai_cli_sync",
        "plan": fake_plan,
        "report_kind": "openai_cli_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_openai_compact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-openai-compact-123",
        "provider_id": "provider-openai-compact-123",
        "endpoint_id": "endpoint-openai-compact-123",
        "key_id": "key-openai-compact-123",
        "provider_name": "openai",
        "method": "POST",
        "url": "https://api.openai.example/v1/responses/compact",
        "headers": {"authorization": "Bearer upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"model": "gpt-5", "input": "hello"}},
        "stream": False,
        "provider_api_format": "openai:compact",
        "client_api_format": "openai:compact",
        "model_name": "gpt-5",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_openai_cli_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="openai_compact_sync",
                plan=fake_plan,
                report_kind="openai_cli_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-openai-compact-plan",
            "method": "POST",
            "path": "/v1/responses/compact",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
            },
            "body_json": {"model": "gpt-5", "input": "hello"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "openai_compact_sync",
        "plan": fake_plan,
        "report_kind": "openai_cli_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_claude_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-claude-chat-123",
        "provider_id": "provider-claude-chat-123",
        "endpoint_id": "endpoint-claude-chat-123",
        "key_id": "key-claude-chat-123",
        "provider_name": "claude",
        "method": "POST",
        "url": "https://api.anthropic.example/v1/messages",
        "headers": {"x-api-key": "upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"model": "claude-sonnet-4", "messages": []}},
        "stream": False,
        "provider_api_format": "claude:chat",
        "client_api_format": "claude:chat",
        "model_name": "claude-sonnet-4",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_claude_chat_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="claude_chat_sync",
                plan=fake_plan,
                report_kind="claude_chat_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-claude-chat-plan",
            "method": "POST",
            "path": "/v1/messages",
            "headers": {
                "content-type": "application/json",
                "x-api-key": "client-key",
            },
            "body_json": {"model": "claude-sonnet-4", "messages": []},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "claude_chat_sync",
        "plan": fake_plan,
        "report_kind": "claude_chat_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-gemini-chat-123",
        "provider_id": "provider-gemini-chat-123",
        "endpoint_id": "endpoint-gemini-chat-123",
        "key_id": "key-gemini-chat-123",
        "provider_name": "gemini",
        "method": "POST",
        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent",
        "headers": {"x-goog-api-key": "upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}},
        "stream": False,
        "provider_api_format": "gemini:chat",
        "client_api_format": "gemini:chat",
        "model_name": "gemini-2.5-pro",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_chat_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_chat_sync",
                plan=fake_plan,
                report_kind="gemini_chat_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-gemini-chat-plan",
            "method": "POST",
            "path": "/v1beta/models/gemini-2.5-pro:generateContent",
            "headers": {
                "content-type": "application/json",
                "x-goog-api-key": "client-key",
            },
            "body_json": {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_chat_sync",
        "plan": fake_plan,
        "report_kind": "gemini_chat_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_claude_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-claude-cli-123",
        "provider_id": "provider-claude-cli-123",
        "endpoint_id": "endpoint-claude-cli-123",
        "key_id": "key-claude-cli-123",
        "provider_name": "claude",
        "method": "POST",
        "url": "https://api.anthropic.example/v1/messages",
        "headers": {"x-api-key": "upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"model": "claude-code", "messages": []}},
        "stream": False,
        "provider_api_format": "claude:cli",
        "client_api_format": "claude:cli",
        "model_name": "claude-code",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_claude_cli_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="claude_cli_sync",
                plan=fake_plan,
                report_kind="claude_cli_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-claude-cli-plan",
            "method": "POST",
            "path": "/v1/messages",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer cli-key",
            },
            "body_json": {"model": "claude-code", "messages": []},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "claude_cli_sync",
        "plan": fake_plan,
        "report_kind": "claude_cli_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-gemini-cli-123",
        "provider_id": "provider-gemini-cli-123",
        "endpoint_id": "endpoint-gemini-cli-123",
        "key_id": "key-gemini-cli-123",
        "provider_name": "gemini",
        "method": "POST",
        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli:generateContent",
        "headers": {"x-goog-api-key": "upstream-key", "content-type": "application/json"},
        "body": {"json_body": {"contents": []}},
        "stream": False,
        "provider_api_format": "gemini:cli",
        "client_api_format": "gemini:cli",
        "model_name": "gemini-cli",
    }
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_cli_sync_plan",
        AsyncMock(
            return_value=GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_cli_sync",
                plan=fake_plan,
                report_kind="gemini_cli_sync_success",
                report_context={"user_id": "user-123", "api_key_id": "key-123"},
            )
        ),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-gemini-cli-plan",
            "method": "POST",
            "path": "/v1beta/models/gemini-cli:generateContent",
            "headers": {
                "content-type": "application/json",
                "user-agent": "GeminiCLI/1.0",
                "x-goog-api-key": "client-key",
            },
            "body_json": {"contents": []},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_cli_sync",
        "plan": fake_plan,
        "report_kind": "gemini_cli_sync_success",
        "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_files_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-files-list-123",
        "provider_id": "provider-files-123",
        "endpoint_id": "endpoint-files-123",
        "key_id": "file-key-123",
        "method": "GET",
        "url": "https://example.com/v1beta/files?pageSize=10",
        "headers": {"x-goog-api-key": "upstream-key"},
        "body": {},
        "stream": False,
        "provider_api_format": "gemini:files",
        "client_api_format": "gemini:files",
        "model_name": "gemini-files",
    }

    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_files_proxy_sync_plan",
        AsyncMock(return_value=(fake_plan, {"file_key_id": "file-key-123", "user_id": "user-123"})),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-files-list-plan",
            "method": "GET",
            "path": "/v1beta/files",
            "query_string": "pageSize=10",
            "headers": {"x-goog-api-key": "client-key"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_files_list",
        "plan": fake_plan,
        "report_kind": "gemini_files_store_mapping",
        "report_context": {"file_key_id": "file-key-123", "user_id": "user-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_files_upload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-files-upload-123",
        "provider_id": "provider-files-123",
        "endpoint_id": "endpoint-files-123",
        "key_id": "file-key-123",
        "method": "POST",
        "url": "https://example.com/upload/v1beta/files?uploadType=resumable",
        "headers": {"x-goog-api-key": "upstream-key", "content-type": "application/octet-stream"},
        "body": {"body_bytes_b64": base64.b64encode(b"upload-body").decode("ascii")},
        "stream": False,
        "provider_api_format": "gemini:files",
        "client_api_format": "gemini:files",
        "model_name": "gemini-files",
    }

    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_files_proxy_sync_plan",
        AsyncMock(return_value=(fake_plan, {"file_key_id": "file-key-123", "user_id": "user-123"})),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-files-upload-plan",
            "method": "POST",
            "path": "/upload/v1beta/files",
            "query_string": "uploadType=resumable",
            "headers": {
                "x-goog-api-key": "client-key",
                "content-type": "application/octet-stream",
            },
            "body_base64": base64.b64encode(b"upload-body").decode("ascii"),
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_files_upload",
        "plan": fake_plan,
        "report_kind": "gemini_files_store_mapping",
        "report_context": {"file_key_id": "file-key-123", "user_id": "user-123"},
    }


def test_plan_sync_route_returns_executor_plan_for_gemini_files_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    fake_plan = {
        "request_id": "plan-files-delete-123",
        "provider_id": "provider-files-123",
        "endpoint_id": "endpoint-files-123",
        "key_id": "file-key-123",
        "method": "DELETE",
        "url": "https://example.com/v1beta/files/files/abc-123",
        "headers": {"x-goog-api-key": "upstream-key"},
        "body": {},
        "stream": False,
        "provider_api_format": "gemini:files",
        "client_api_format": "gemini:files",
        "model_name": "gemini-files",
    }

    monkeypatch.setattr(
        "src.api.internal.gateway._load_gateway_auth_models",
        lambda db, auth_context: (
            SimpleNamespace(id="user-123"),
            SimpleNamespace(id="key-123"),
        ),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.get_pipeline",
        lambda: SimpleNamespace(_check_user_rate_limit=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._build_gemini_files_proxy_sync_plan",
        AsyncMock(return_value=(fake_plan, {})),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/plan-sync",
        json={
            "trace_id": "trace-files-delete-plan",
            "method": "DELETE",
            "path": "/v1beta/files/files/abc-123",
            "headers": {"x-goog-api-key": "client-key"},
            "auth_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "access_allowed": True,
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "action": "executor_sync",
        "plan_kind": "gemini_files_delete",
        "plan": fake_plan,
        "report_kind": "gemini_files_delete_mapping",
        "report_context": {"file_name": "files/abc-123"},
    }

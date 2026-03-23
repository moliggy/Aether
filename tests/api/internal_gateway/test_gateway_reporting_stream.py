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


def test_report_stream_route_uses_lazy_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    background_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._run_gateway_stream_report_background_with_session",
        background_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.create_session",
        lambda: (_ for _ in ()).throw(AssertionError("create_session should not be called")),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-stream",
        json={
            "trace_id": "trace-report-stream-lazy-123",
            "report_kind": "openai_chat_stream_success",
            "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
            "status_code": 200,
            "headers": {"content-type": "text/event-stream"},
            "body_base64": base64.b64encode(b"data: ok\n\n").decode("ascii"),
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    background_mock.assert_awaited_once()


@pytest.mark.asyncio


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_run_gateway_stream_report_background_ensures_request_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = object()
    ensure_mock = MagicMock(return_value=SimpleNamespace(id="cand-stream-123"))
    apply_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._ensure_gateway_request_candidate",
        ensure_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._apply_gateway_stream_report",
        apply_mock,
    )

    payload = GatewayStreamReportRequest(
        trace_id="trace-stream-candidate-123",
        report_kind="openai_chat_stream_success",
        report_context={
            "request_id": "req-stream-candidate-123",
            "candidate_id": "cand-stream-123",
            "provider_id": "provider-123",
            "endpoint_id": "endpoint-123",
            "key_id": "key-123",
        },
        status_code=200,
        headers={"content-type": "text/event-stream"},
        body_base64=base64.b64encode(b"data: ok\n\n").decode("ascii"),
    )

    await _run_gateway_stream_report_background(payload, db)

    ensure_mock.assert_called_once_with(
        db=db,
        report_context=dict(payload.report_context or {}),
        trace_id="trace-stream-candidate-123",
        initial_status="streaming",
    )
    apply_mock.assert_awaited_once_with(payload, db=db)


def test_report_stream_route_records_openai_chat_stream_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_openai_chat_stream_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-stream",
        json={
            "trace_id": "trace-openai-chat-stream-report",
            "report_kind": "openai_chat_stream_success",
            "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
            "status_code": 200,
            "headers": {"content-type": "text/event-stream"},
            "body_base64": base64.b64encode(b"data: hello\\n\\ndata: [DONE]\\n\\n").decode("ascii"),
            "telemetry": {"elapsed_ms": 123, "ttfb_ms": 22},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_stream_route_records_claude_chat_stream_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_chat_stream_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-stream",
        json={
            "trace_id": "trace-claude-chat-stream-report",
            "report_kind": "claude_chat_stream_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "claude:chat",
            },
            "status_code": 200,
            "headers": {"content-type": "text/event-stream"},
            "body_base64": base64.b64encode(b"event: content_block_delta\n\n").decode("ascii"),
            "telemetry": {"elapsed_ms": 66},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_stream_route_records_gemini_chat_stream_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_chat_stream_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-stream",
        json={
            "trace_id": "trace-gemini-chat-stream-report",
            "report_kind": "gemini_chat_stream_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "gemini:chat",
            },
            "status_code": 200,
            "headers": {"content-type": "text/event-stream"},
            "body_base64": base64.b64encode(b'data: {"candidates":[]}\n\n').decode("ascii"),
            "telemetry": {"elapsed_ms": 44},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_stream_route_records_openai_cli_stream_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_cli_stream_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-stream",
        json={
            "trace_id": "trace-openai-cli-stream-report",
            "report_kind": "openai_cli_stream_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "openai:cli",
            },
            "status_code": 200,
            "headers": {"content-type": "text/event-stream"},
            "body_base64": base64.b64encode(
                b'event: response.completed\ndata: {"type":"response.completed"}\n\n'
            ).decode("ascii"),
            "telemetry": {"elapsed_ms": 66},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_stream_route_records_claude_cli_stream_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_cli_stream_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-stream",
        json={
            "trace_id": "trace-claude-cli-stream-report",
            "report_kind": "claude_cli_stream_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "claude:cli",
            },
            "status_code": 200,
            "headers": {"content-type": "text/event-stream"},
            "body_base64": base64.b64encode(
                b'event: message_start\ndata: {"type":"message_start"}\n\n'
            ).decode("ascii"),
            "telemetry": {"elapsed_ms": 44},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()


def test_report_stream_route_records_gemini_cli_stream_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    record_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_passthrough_cli_stream_success",
        record_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-stream",
        json={
            "trace_id": "trace-gemini-cli-stream-report",
            "report_kind": "gemini_cli_stream_success",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "gemini:cli",
            },
            "status_code": 200,
            "headers": {"content-type": "text/event-stream"},
            "body_base64": base64.b64encode(b'data: {"candidates":[]}\n\n').decode("ascii"),
            "telemetry": {"elapsed_ms": 44},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: record_mock.await_count == 1)
    record_mock.assert_awaited_once()

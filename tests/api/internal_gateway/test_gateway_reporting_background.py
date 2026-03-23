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


def test_report_sync_route_applies_gemini_files_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    seen: dict[str, object] = {}

    async def _fake_store_mapping(**kwargs: object) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(
        "src.api.public.gemini_files._maybe_store_file_mapping_from_payload",
        _fake_store_mapping,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-report-files-123",
            "report_kind": "gemini_files_store_mapping",
            "report_context": {
                "file_key_id": "file-key-123",
                "user_id": "user-123",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"name": "files/abc-123", "displayName": "Report File"},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: "status_code" in seen)
    assert seen["status_code"] == 200
    assert seen["headers"] == {"content-type": "application/json"}
    assert seen["file_key_id"] == "file-key-123"
    assert seen["user_id"] == "user-123"
    assert json.loads(seen["content_bytes"]) == {
        "name": "files/abc-123",
        "displayName": "Report File",
    }


def test_report_sync_route_applies_gemini_files_delete_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    seen: list[str] = []

    async def _fake_delete_mapping(file_name: str) -> None:
        seen.append(file_name)

    monkeypatch.setattr(
        "src.services.gemini_files_mapping.delete_file_key_mapping",
        _fake_delete_mapping,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-report-delete-files-123",
            "report_kind": "gemini_files_delete_mapping",
            "report_context": {"file_name": "files/abc-123"},
            "status_code": 200,
            "headers": {},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    _wait_until(lambda: bool(seen))
    assert seen == ["files/abc-123"]


def test_report_sync_route_uses_lazy_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    background_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._run_gateway_sync_report_background_with_session",
        background_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.create_session",
        lambda: (_ for _ in ()).throw(AssertionError("create_session should not be called")),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-report-sync-lazy-123",
            "report_kind": "openai_chat_sync_success",
            "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "chatcmpl-123", "object": "chat.completion", "choices": []},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    background_mock.assert_awaited_once()


def test_report_sync_route_runs_video_create_success_inline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)

    background_with_session_mock = AsyncMock(return_value=None)
    run_background_mock = AsyncMock(return_value=None)
    cleanup_mock = MagicMock()
    monkeypatch.setattr(
        "src.api.internal.gateway._run_gateway_sync_report_background_with_session",
        background_with_session_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._run_gateway_sync_report_background",
        run_background_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._resolve_gateway_background_db",
        lambda app_obj: ("db-inline-123", cleanup_mock),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/report-sync",
        json={
            "trace_id": "trace-openai-video-create-inline-123",
            "report_kind": "openai_video_create_sync_success",
            "report_context": {"user_id": "user-123", "api_key_id": "key-123"},
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "ext-video-123", "status": "submitted"},
            "client_body_json": {
                "id": "local-video-123",
                "object": "video",
                "status": "queued",
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    background_with_session_mock.assert_not_awaited()
    run_background_mock.assert_awaited_once()
    cleanup_mock.assert_called_once()


@pytest.mark.asyncio


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_run_gateway_sync_report_background_ensures_request_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = object()
    ensure_mock = MagicMock(return_value=SimpleNamespace(id="cand-sync-123"))
    finalize_mock = MagicMock()
    apply_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._ensure_gateway_request_candidate",
        ensure_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._mark_gateway_sync_candidate_terminal_state",
        finalize_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._apply_gateway_sync_report",
        apply_mock,
    )

    payload = GatewaySyncReportRequest(
        trace_id="trace-sync-candidate-123",
        report_kind="openai_chat_sync_success",
        report_context={
            "request_id": "req-sync-candidate-123",
            "candidate_id": "cand-sync-123",
            "provider_id": "provider-123",
            "endpoint_id": "endpoint-123",
            "key_id": "key-123",
        },
        status_code=200,
        headers={"content-type": "application/json"},
        body_json={"id": "chatcmpl-123", "object": "chat.completion", "choices": []},
    )

    await _run_gateway_sync_report_background(payload, db)

    ensure_mock.assert_called_once_with(
        db=db,
        report_context=dict(payload.report_context or {}),
        trace_id="trace-sync-candidate-123",
        initial_status="pending",
    )
    apply_mock.assert_awaited_once_with(payload, db=db)
    finalize_mock.assert_called_once_with(
        db=db,
        candidate=ensure_mock.return_value,
        payload=payload,
    )


@pytest.mark.asyncio
async def test_run_gateway_sync_report_background_updates_direct_candidate_terminal_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[RequestCandidate.__table__])
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as db:
        resolver = CandidateResolver(db=db, cache_scheduler=SimpleNamespace())
        user_api_key = SimpleNamespace(
            id="api-key-direct-terminal-123",
            name="client-key",
            user_id="user-direct-terminal-123",
            user=SimpleNamespace(id="user-direct-terminal-123", username="direct-terminal-user"),
        )
        candidates = [
            ProviderCandidate(
                provider=SimpleNamespace(id="provider-selected-terminal-123", name="openai", max_retries=1),
                endpoint=SimpleNamespace(id="endpoint-selected-terminal-123"),
                key=SimpleNamespace(id="key-selected-terminal-123"),
                provider_api_format="openai:chat",
            ),
            ProviderCandidate(
                provider=SimpleNamespace(id="provider-unused-terminal-123", name="openai", max_retries=1),
                endpoint=SimpleNamespace(id="endpoint-unused-terminal-123"),
                key=SimpleNamespace(id="key-unused-terminal-123"),
                provider_api_format="openai:chat",
            ),
        ]

        _record_gateway_direct_candidate_graph(
            db=db,
            candidate_resolver=resolver,
            candidates=candidates,
            request_id="req-direct-terminal-123",
            user_api_key=user_api_key,
            required_capabilities=None,
            selected_candidate_index=0,
        )
        monkeypatch.setattr(
            "src.api.internal.gateway._apply_gateway_sync_report",
            AsyncMock(return_value=None),
        )

        payload = GatewaySyncReportRequest(
            trace_id="trace-direct-terminal-123",
            report_kind="openai_chat_sync_success",
            report_context={
                "request_id": "req-direct-terminal-123",
                "candidate_id": getattr(candidates[0], "request_candidate_id"),
                "provider_id": "provider-selected-terminal-123",
                "endpoint_id": "endpoint-selected-terminal-123",
                "key_id": "key-selected-terminal-123",
                "client_api_format": "openai:chat",
            },
            status_code=200,
            headers={"content-type": "application/json"},
            body_json={"id": "chatcmpl-123", "object": "chat.completion", "choices": []},
        )

        await _run_gateway_sync_report_background(payload, db)

        rows = (
            db.query(RequestCandidate)
            .filter(RequestCandidate.request_id == "req-direct-terminal-123")
            .order_by(RequestCandidate.candidate_index, RequestCandidate.retry_index)
            .all()
        )

        assert len(rows) == 2
        assert rows[0].status == "success"
        assert rows[1].status == "unused"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("report_kind", "recorder_attr"),
    [
        ("openai_chat_sync_error", "_record_gateway_chat_sync_failure"),
        ("claude_chat_sync_error", "_record_gateway_chat_sync_failure"),
        ("gemini_chat_sync_error", "_record_gateway_chat_sync_failure"),
    ],
)


async def test_apply_gateway_sync_report_routes_chat_error_to_failure_recorder(
    monkeypatch: pytest.MonkeyPatch,
    report_kind: str,
    recorder_attr: str,
) -> None:
    db = object()
    recorder = AsyncMock(return_value=None)
    monkeypatch.setattr(
        f"src.api.internal.gateway.{recorder_attr}",
        recorder,
    )

    payload = GatewaySyncReportRequest(
        trace_id="trace-chat-error-report-123",
        report_kind=report_kind,
        report_context={"user_id": "user-123", "api_key_id": "key-123"},
        status_code=429,
        headers={"content-type": "application/json"},
        body_json={"error": {"message": "rate limited"}},
    )

    from src.api.internal.gateway import _apply_gateway_sync_report

    await _apply_gateway_sync_report(payload, db=db)

    recorder.assert_awaited_once_with(payload, db=db)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("report_kind", "recorder_attr"),
    [
        ("openai_cli_sync_error", "_record_gateway_cli_sync_failure"),
        ("openai_compact_sync_error", "_record_gateway_cli_sync_failure"),
        ("claude_cli_sync_error", "_record_gateway_cli_sync_failure"),
        ("gemini_cli_sync_error", "_record_gateway_cli_sync_failure"),
    ],
)


async def test_apply_gateway_sync_report_routes_cli_error_to_failure_recorder(
    monkeypatch: pytest.MonkeyPatch,
    report_kind: str,
    recorder_attr: str,
) -> None:
    db = object()
    recorder = AsyncMock(return_value=None)
    monkeypatch.setattr(
        f"src.api.internal.gateway.{recorder_attr}",
        recorder,
    )

    payload = GatewaySyncReportRequest(
        trace_id="trace-cli-error-report-123",
        report_kind=report_kind,
        report_context={"user_id": "user-123", "api_key_id": "key-123"},
        status_code=429,
        headers={"content-type": "application/json"},
        body_json={"error": {"message": "rate limited"}},
    )

    from src.api.internal.gateway import _apply_gateway_sync_report

    await _apply_gateway_sync_report(payload, db=db)

    recorder.assert_awaited_once_with(payload, db=db)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "report_kind",
    [
        "openai_video_create_sync_error",
        "openai_video_remix_sync_error",
        "gemini_video_create_sync_error",
        "openai_video_delete_sync_error",
        "openai_video_cancel_sync_error",
        "gemini_video_cancel_sync_error",
    ],
)
async def test_apply_gateway_sync_report_routes_video_error_to_failure_recorder(
    monkeypatch: pytest.MonkeyPatch,
    report_kind: str,
) -> None:
    db = object()
    recorder = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._record_gateway_video_sync_failure",
        recorder,
    )

    payload = GatewaySyncReportRequest(
        trace_id="trace-video-error-report-123",
        report_kind=report_kind,
        report_context={"user_id": "user-123", "api_key_id": "key-123"},
        status_code=429,
        headers={"content-type": "application/json"},
        body_json={"error": {"message": "rate limited"}},
    )

    from src.api.internal.gateway import _apply_gateway_sync_report

    await _apply_gateway_sync_report(payload, db=db)

    recorder.assert_awaited_once_with(payload, db=db)

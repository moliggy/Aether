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


def test_extract_gateway_sync_error_message_prefers_nested_error_message() -> None:
    payload = GatewaySyncReportRequest(
        trace_id="trace-error-message-123",
        report_kind="openai_chat_sync_finalize",
        status_code=429,
        headers={"content-type": "application/json"},
        body_json={"error": {"message": "rate limited"}},
    )

    assert _extract_gateway_sync_error_message(payload) == "rate limited"


def test_extract_gateway_sync_error_message_falls_back_to_body_base64() -> None:
    payload = GatewaySyncReportRequest(
        trace_id="trace-error-body-123",
        report_kind="openai_chat_sync_finalize",
        status_code=502,
        headers={"content-type": "text/plain"},
        body_base64=base64.b64encode(b"upstream exploded").decode("ascii"),
    )

    assert _extract_gateway_sync_error_message(payload) == "upstream exploded"


def test_build_gateway_sync_error_payload_returns_provider_error_when_no_conversion() -> None:
    payload = GatewaySyncReportRequest(
        trace_id="trace-error-payload-123",
        report_kind="openai_chat_sync_finalize",
        status_code=400,
        headers={"content-type": "application/json"},
        body_json={"error": {"message": "bad request"}},
    )

    assert _build_gateway_sync_error_payload(
        payload,
        client_api_format="openai:chat",
        provider_api_format="openai:chat",
        needs_conversion=False,
    ) == {"error": {"message": "bad request"}}


def test_build_gateway_sync_error_payload_builds_generic_client_error_for_non_json() -> None:
    payload = GatewaySyncReportRequest(
        trace_id="trace-error-generic-123",
        report_kind="openai_cli_sync_finalize",
        status_code=504,
        headers={"content-type": "text/plain"},
        body_base64=base64.b64encode(b"gateway timeout").decode("ascii"),
    )

    result = _build_gateway_sync_error_payload(
        payload,
        client_api_format="openai:cli",
        provider_api_format="openai:cli",
        needs_conversion=False,
    )

    assert result["error"]["message"] == "gateway timeout"


def test_resolve_gateway_sync_error_status_code_prefers_embedded_code() -> None:
    class _FakeParser:
        def parse_response(self, response: dict[str, object], status_code: int) -> SimpleNamespace:
            del response, status_code
            return SimpleNamespace(embedded_status_code=429)

    payload = GatewaySyncReportRequest(
        trace_id="trace-error-status-123",
        report_kind="openai_chat_sync_finalize",
        status_code=200,
        headers={"content-type": "application/json"},
        body_json={"error": {"message": "rate limited", "code": 429}},
    )

    assert _resolve_gateway_sync_error_status_code(payload, provider_parser=_FakeParser()) == 429


def test_resolve_gateway_sync_error_status_code_defaults_embedded_200_to_400() -> None:
    payload = GatewaySyncReportRequest(
        trace_id="trace-error-status-default-123",
        report_kind="openai_chat_sync_finalize",
        status_code=200,
        headers={"content-type": "application/json"},
        body_json={"error": {"message": "bad request"}},
    )

    assert _resolve_gateway_sync_error_status_code(payload) == 400


def test_record_gateway_direct_candidate_graph_creates_full_candidate_graph() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[RequestCandidate.__table__])
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as db:
        resolver = CandidateResolver(db=db, cache_scheduler=SimpleNamespace())
        user_api_key = SimpleNamespace(
            id="api-key-direct-123",
            name="client-key",
            user_id="user-direct-123",
            user=SimpleNamespace(id="user-direct-123", username="direct-user"),
        )
        candidates = [
            ProviderCandidate(
                provider=SimpleNamespace(id="provider-selected-123", name="openai", max_retries=1),
                endpoint=SimpleNamespace(id="endpoint-selected-123"),
                key=SimpleNamespace(id="key-selected-123"),
                provider_api_format="openai:chat",
            ),
            ProviderCandidate(
                provider=SimpleNamespace(id="provider-unused-123", name="openai", max_retries=1),
                endpoint=SimpleNamespace(id="endpoint-unused-123"),
                key=SimpleNamespace(id="key-unused-123"),
                provider_api_format="openai:chat",
            ),
            ProviderCandidate(
                provider=SimpleNamespace(id="provider-skipped-123", name="openai", max_retries=1),
                endpoint=SimpleNamespace(id="endpoint-skipped-123"),
                key=SimpleNamespace(id="key-skipped-123"),
                is_skipped=True,
                skip_reason="capability_miss",
                provider_api_format="openai:chat",
            ),
        ]

        _record_gateway_direct_candidate_graph(
            db=db,
            candidate_resolver=resolver,
            candidates=candidates,
            request_id="req-direct-candidate-graph-123",
            user_api_key=user_api_key,
            required_capabilities={"tools": True},
            selected_candidate_index=0,
        )

        rows = (
            db.query(RequestCandidate)
            .filter(RequestCandidate.request_id == "req-direct-candidate-graph-123")
            .order_by(RequestCandidate.candidate_index, RequestCandidate.retry_index)
            .all()
        )

        assert len(rows) == 3
        assert rows[0].status == "pending"
        assert rows[1].status == "unused"
        assert rows[2].status == "skipped"
        assert getattr(candidates[0], "request_candidate_id") == rows[0].id
        assert rows[0].started_at is not None
        assert rows[1].finished_at is not None


def test_record_gateway_direct_candidate_graph_uses_selected_pool_key_index() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[RequestCandidate.__table__])
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as db:
        resolver = CandidateResolver(db=db, cache_scheduler=SimpleNamespace())
        user_api_key = SimpleNamespace(
            id="api-key-pool-123",
            name="client-key",
            user_id="user-pool-123",
            user=SimpleNamespace(id="user-pool-123", username="pool-user"),
        )
        pool_keys = [
            SimpleNamespace(id="pool-key-0"),
            SimpleNamespace(id="pool-key-1"),
        ]
        candidate = PoolCandidate(
            provider=SimpleNamespace(id="provider-pool-123", name="claude", max_retries=1),
            endpoint=SimpleNamespace(id="endpoint-pool-123"),
            key=pool_keys[1],
            pool_keys=pool_keys,
            provider_api_format="claude:cli",
        )
        candidate._pool_key_index = 1

        _record_gateway_direct_candidate_graph(
            db=db,
            candidate_resolver=resolver,
            candidates=[candidate],
            request_id="req-direct-pool-graph-123",
            user_api_key=user_api_key,
            required_capabilities=None,
            selected_candidate_index=0,
        )

        rows = (
            db.query(RequestCandidate)
            .filter(RequestCandidate.request_id == "req-direct-pool-graph-123")
            .order_by(RequestCandidate.candidate_index, RequestCandidate.retry_index)
            .all()
        )

        assert len(rows) == 2
        assert rows[0].status == "unused"
        assert rows[1].status == "pending"
        assert getattr(candidate, "request_candidate_id") == rows[1].id

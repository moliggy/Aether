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


def test_stream_executor_requires_python_rewrite_allows_antigravity_same_format() -> None:
    class AntigravityEnvelope:
        name = "antigravity:v1internal"

        def force_stream_rewrite(self) -> bool:
            return True

    envelope = AntigravityEnvelope()
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=envelope,
            needs_conversion=False,
            provider_api_format="gemini:cli",
            client_api_format="gemini:cli",
        )
        is False
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=envelope,
            needs_conversion=False,
            provider_api_format="gemini:chat",
            client_api_format="gemini:chat",
        )
        is False
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=envelope,
            needs_conversion=False,
            provider_api_format="gemini:cli",
            client_api_format="openai:cli",
        )
        is True
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=envelope,
            needs_conversion=True,
            provider_api_format="gemini:chat",
            client_api_format="openai:chat",
        )
        is False
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=envelope,
            needs_conversion=True,
            provider_api_format="gemini:cli",
            client_api_format="openai:cli",
        )
        is False
    )


def test_stream_executor_requires_python_rewrite_allows_kiro_same_format() -> None:
    class KiroEnvelope:
        name = "kiro:generateAssistantResponse"

        def force_stream_rewrite(self) -> bool:
            return True

    envelope = KiroEnvelope()
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=envelope,
            needs_conversion=False,
            provider_api_format="claude:cli",
            client_api_format="claude:cli",
        )
        is False
    )
    assert (
        _stream_executor_requires_python_rewrite(
            envelope=envelope,
            needs_conversion=False,
            provider_api_format="claude:chat",
            client_api_format="claude:chat",
        )
        is True
    )

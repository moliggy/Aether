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


def test_finalize_sync_route_finalizes_openai_video_create_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(
        return_value=JSONResponse(
            content={
                "id": "vid_local_123",
                "object": "video",
                "status": "submitted",
            }
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_openai_video_create_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-openai-video-finalize",
            "report_kind": "openai_video_create_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "ext-video-task-123", "status": "submitted"},
            "telemetry": {"elapsed_ms": 42},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {
        "id": "vid_local_123",
        "object": "video",
        "status": "submitted",
    }
    finalize_mock.assert_awaited_once()


def test_finalize_sync_route_finalizes_openai_chat_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(
        return_value=JSONResponse(
            content={
                "id": "chatcmpl-local-123",
                "object": "chat.completion",
                "choices": [],
            }
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_chat_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-openai-chat-finalize",
            "report_kind": "openai_chat_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "openai:chat",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "upstream-123"},
            "telemetry": {"elapsed_ms": 12},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {
        "id": "chatcmpl-local-123",
        "object": "chat.completion",
        "choices": [],
    }
    finalize_mock.assert_awaited_once()


def test_finalize_sync_route_finalizes_openai_cli_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(
        return_value=JSONResponse(
            content={
                "id": "resp_local_123",
                "object": "response",
                "output": [],
            }
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_cli_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-openai-cli-finalize",
            "report_kind": "openai_cli_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "client_api_format": "openai:cli",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "upstream-resp-123"},
            "telemetry": {"elapsed_ms": 23},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {
        "id": "resp_local_123",
        "object": "response",
        "output": [],
    }
    finalize_mock.assert_awaited_once()


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_finalize_gateway_chat_sync_uses_dispatch_helper_for_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module

    class FakeQuery:
        def __init__(self, obj: object) -> None:
            self._obj = obj

        def filter(self, *args: object, **kwargs: object) -> "FakeQuery":
            return self

        def first(self) -> object:
            return self._obj

    class FakeDB:
        def __init__(self, mapping: dict[str, object]) -> None:
            self._mapping = mapping

        def query(self, model: object) -> FakeQuery:
            return FakeQuery(self._mapping[getattr(model, "__name__", "")])

    fake_handler = SimpleNamespace(
        _prepare_provider_request=AsyncMock(
            return_value=SimpleNamespace(
                needs_conversion=False,
                envelope=None,
                upstream_is_stream=False,
            )
        ),
        _normalize_response=lambda body: body,
        _extract_usage=lambda body: {"input_tokens": 1, "output_tokens": 2},
        _extract_response_metadata=lambda body: {"finish_reason": "stop"},
        telemetry=SimpleNamespace(
            record_success=AsyncMock(return_value=None),
            record_failure=AsyncMock(return_value=None),
        ),
    )
    dispatch_mock = AsyncMock(return_value=None)
    fake_writer = SimpleNamespace(
        supports_background_submission=lambda: True,
        record_success=AsyncMock(return_value=None),
    )

    monkeypatch.setattr(
        "src.api.handlers.openai.OpenAIChatAdapter._create_handler",
        lambda self, **kwargs: fake_handler,
    )
    monkeypatch.setattr(gateway_module, "_dispatch_gateway_sync_telemetry", dispatch_mock)
    monkeypatch.setattr(
        gateway_module,
        "_build_gateway_sync_telemetry_writer",
        lambda **kwargs: fake_writer,
    )

    db = FakeDB(
        {
            "User": SimpleNamespace(id="user-chat-finalize-dispatch-123"),
            "ApiKey": SimpleNamespace(id="api-key-chat-finalize-dispatch-123"),
            "Provider": SimpleNamespace(
                id="provider-chat-finalize-dispatch-123",
                name="openai",
                provider_type="openai",
            ),
            "ProviderEndpoint": SimpleNamespace(id="endpoint-chat-finalize-dispatch-123"),
            "ProviderAPIKey": SimpleNamespace(id="key-chat-finalize-dispatch-123"),
        }
    )

    payload = GatewaySyncReportRequest(
        trace_id="trace-chat-finalize-dispatch-123",
        report_kind="openai_chat_sync_finalize",
        report_context={
            "user_id": "user-chat-finalize-dispatch-123",
            "api_key_id": "api-key-chat-finalize-dispatch-123",
            "provider_id": "provider-chat-finalize-dispatch-123",
            "endpoint_id": "endpoint-chat-finalize-dispatch-123",
            "key_id": "key-chat-finalize-dispatch-123",
            "request_id": "req-chat-finalize-dispatch-123",
            "model": "gpt-5",
            "provider_name": "openai",
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "mapped_model": "gpt-5",
            "original_headers": {"content-type": "application/json"},
            "original_request_body": {"model": "gpt-5", "messages": []},
            "provider_request_headers": {"content-type": "application/json"},
            "provider_request_body": {"model": "gpt-5", "messages": []},
        },
        status_code=200,
        headers={"content-type": "application/json"},
        body_json={
            "id": "chatcmpl-finalize-dispatch-123",
            "object": "chat.completion",
            "choices": [],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3,
            },
        },
        telemetry={"elapsed_ms": 11},
    )

    response = await gateway_module._finalize_gateway_chat_sync(payload, db=db)

    assert response.status_code == 200
    dispatch_mock.assert_awaited_once()
    assert dispatch_mock.await_args.kwargs["telemetry_writer"] is fake_writer
    assert dispatch_mock.await_args.kwargs["operation"] == "record_success"
    fake_writer.record_success.assert_not_called()


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_finalize_gateway_chat_sync_aggregates_upstream_stream_body_base64(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module

    class FakeQuery:
        def __init__(self, obj: object) -> None:
            self._obj = obj

        def filter(self, *args: object, **kwargs: object) -> "FakeQuery":
            return self

        def first(self) -> object:
            return self._obj

    class FakeDB:
        def __init__(self, mapping: dict[str, object]) -> None:
            self._mapping = mapping

        def query(self, model: object) -> FakeQuery:
            return FakeQuery(self._mapping[getattr(model, "__name__", "")])

    fake_handler = SimpleNamespace(
        _prepare_provider_request=AsyncMock(
            return_value=SimpleNamespace(
                needs_conversion=False,
                envelope=None,
                upstream_is_stream=True,
                provider_api_format="openai:chat",
                mapped_model="gpt-5",
            )
        ),
        _normalize_response=lambda body: body,
        _extract_usage=lambda body: {"input_tokens": 1, "output_tokens": 2},
        telemetry=SimpleNamespace(
            record_success=AsyncMock(return_value=None),
            record_failure=AsyncMock(return_value=None),
        ),
    )
    seen_body: dict[str, bytes] = {}

    class FakeChatSyncExecutor:
        def __init__(self, handler: object) -> None:
            self._ctx = SimpleNamespace(provider_response_json=None)

        async def _finalize_rust_stream_sync_result(
            self,
            *,
            prepared_plan: object,
            provider: object,
            model: str,
            response_body_bytes: bytes,
        ) -> dict[str, object]:
            seen_body["body"] = response_body_bytes
            return {
                "id": "chatcmpl-direct-stream-123",
                "object": "chat.completion",
                "choices": [],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 2,
                    "total_tokens": 3,
                },
            }

    monkeypatch.setattr(
        "src.api.handlers.base.chat_sync_executor.ChatSyncExecutor",
        FakeChatSyncExecutor,
    )
    monkeypatch.setattr(
        "src.api.handlers.openai.OpenAIChatAdapter._create_handler",
        lambda self, **kwargs: fake_handler,
    )

    db = FakeDB(
        {
            "User": SimpleNamespace(id="user-chat-stream-finalize-123"),
            "ApiKey": SimpleNamespace(id="api-key-chat-stream-finalize-123"),
            "Provider": SimpleNamespace(
                id="provider-chat-stream-finalize-123",
                name="openai",
                provider_type="openai",
            ),
            "ProviderEndpoint": SimpleNamespace(id="endpoint-chat-stream-finalize-123"),
            "ProviderAPIKey": SimpleNamespace(id="key-chat-stream-finalize-123"),
        }
    )

    payload = GatewaySyncReportRequest(
        trace_id="trace-chat-stream-sync-finalize-123",
        report_kind="openai_chat_sync_finalize",
        report_context={
            "user_id": "user-chat-stream-finalize-123",
            "api_key_id": "api-key-chat-stream-finalize-123",
            "provider_id": "provider-chat-stream-finalize-123",
            "endpoint_id": "endpoint-chat-stream-finalize-123",
            "key_id": "key-chat-stream-finalize-123",
            "request_id": "req-chat-stream-sync-finalize-123",
            "model": "gpt-5",
            "provider_name": "openai",
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "mapped_model": "gpt-5",
            "original_headers": {"content-type": "application/json"},
            "original_request_body": {"model": "gpt-5", "messages": []},
            "provider_request_headers": {"content-type": "application/json"},
            "provider_request_body": {"model": "gpt-5", "messages": []},
        },
        status_code=200,
        headers={"content-type": "text/event-stream"},
        body_base64=base64.b64encode(b"data: stub\n\n").decode("ascii"),
        telemetry={"elapsed_ms": 12},
    )

    response = await gateway_module._finalize_gateway_chat_sync(payload, db=db)

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "id": "chatcmpl-direct-stream-123",
        "object": "chat.completion",
        "choices": [],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 2,
            "total_tokens": 3,
        },
    }
    assert seen_body["body"] == b"data: stub\n\n"
    fake_handler.telemetry.record_success.assert_awaited_once()


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_finalize_gateway_cli_sync_aggregates_upstream_stream_body_base64(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module
    from src.api.handlers.openai_cli import OpenAICliAdapter

    class FakeQuery:
        def __init__(self, obj: object) -> None:
            self._obj = obj

        def filter(self, *args: object, **kwargs: object) -> "FakeQuery":
            return self

        def first(self) -> object:
            return self._obj

    class FakeDB:
        def __init__(self, mapping: dict[str, object]) -> None:
            self._mapping = mapping

        def query(self, model: object) -> FakeQuery:
            return FakeQuery(self._mapping[getattr(model, "__name__", "")])

    aggregate_mock = AsyncMock(
        return_value={
            "id": "resp-direct-stream-123",
            "object": "response",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 2},
        }
    )

    class FakeCliHandler:
        def __init__(self, **kwargs: object) -> None:
            self.parser = SimpleNamespace(
                extract_usage_from_response=lambda body: {
                    "input_tokens": 1,
                    "output_tokens": 2,
                }
            )
            self.telemetry = SimpleNamespace(
                record_success=AsyncMock(return_value=None),
                record_failure=AsyncMock(return_value=None),
            )

        async def _build_upstream_request(self, **kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(
                upstream_is_stream=True,
                envelope=None,
            )

        async def _aggregate_upstream_stream_sync_response(self, **kwargs: object) -> dict[str, object]:
            return await aggregate_mock(**kwargs)

        def _extract_response_metadata(self, body: dict[str, object]) -> dict[str, object]:
            return {}

    monkeypatch.setattr(
        type(OpenAICliAdapter()),
        "HANDLER_CLASS",
        property(lambda self: FakeCliHandler),
    )

    db = FakeDB(
        {
            "User": SimpleNamespace(id="user-cli-stream-finalize-123"),
            "ApiKey": SimpleNamespace(id="api-key-cli-stream-finalize-123"),
            "Provider": SimpleNamespace(
                id="provider-cli-stream-finalize-123",
                name="openai",
                provider_type="openai",
            ),
            "ProviderEndpoint": SimpleNamespace(id="endpoint-cli-stream-finalize-123"),
            "ProviderAPIKey": SimpleNamespace(id="key-cli-stream-finalize-123"),
        }
    )

    payload = GatewaySyncReportRequest(
        trace_id="trace-cli-stream-sync-finalize-123",
        report_kind="openai_cli_sync_finalize",
        report_context={
            "user_id": "user-cli-stream-finalize-123",
            "api_key_id": "api-key-cli-stream-finalize-123",
            "provider_id": "provider-cli-stream-finalize-123",
            "endpoint_id": "endpoint-cli-stream-finalize-123",
            "key_id": "key-cli-stream-finalize-123",
            "request_id": "req-cli-stream-sync-finalize-123",
            "model": "gpt-5",
            "provider_name": "openai",
            "provider_api_format": "openai:cli",
            "client_api_format": "openai:cli",
            "mapped_model": "gpt-5",
            "original_headers": {"content-type": "application/json"},
            "original_request_body": {"model": "gpt-5", "input": "hello"},
            "provider_request_headers": {"content-type": "application/json"},
            "provider_request_body": {"model": "gpt-5", "input": "hello"},
        },
        status_code=200,
        headers={"content-type": "text/event-stream"},
        body_base64=base64.b64encode(b"data: cli\n\n").decode("ascii"),
        telemetry={"elapsed_ms": 9},
    )

    response = await gateway_module._finalize_gateway_cli_sync(payload, db=db)

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "id": "resp-direct-stream-123",
        "object": "response",
        "output": [],
        "usage": {"input_tokens": 1, "output_tokens": 2},
    }
    aggregate_mock.assert_awaited_once()


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_finalize_gateway_chat_sync_fast_path_returns_before_db_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.internal import gateway as gateway_module

    aggregate_mock = AsyncMock(
        return_value={
            "id": "chatcmpl-fast-finalize-123",
            "object": "chat.completion",
            "choices": [],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
        }
    )
    monkeypatch.setattr(
        gateway_module,
        "_aggregate_gateway_sync_response_bytes",
        aggregate_mock,
    )

    class FailingDB:
        def query(self, model: object) -> object:
            raise AssertionError(f"db query should not happen for {getattr(model, '__name__', model)}")

    payload = GatewaySyncReportRequest(
        trace_id="trace-chat-fast-finalize-123",
        report_kind="openai_chat_sync_finalize",
        report_context={
            "user_id": "user-chat-fast-finalize-123",
            "api_key_id": "api-key-chat-fast-finalize-123",
            "provider_id": "provider-chat-fast-finalize-123",
            "endpoint_id": "endpoint-chat-fast-finalize-123",
            "key_id": "key-chat-fast-finalize-123",
            "request_id": "req-chat-fast-finalize-123",
            "model": "gpt-5",
            "provider_name": "openai",
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "mapped_model": "gpt-5",
            "original_headers": {"content-type": "application/json"},
            "original_request_body": {"model": "gpt-5", "messages": []},
            "provider_request_headers": {"content-type": "application/json"},
            "provider_request_body": {"model": "gpt-5", "messages": []},
            "has_envelope": False,
            "needs_conversion": False,
        },
        status_code=200,
        headers={"content-type": "text/event-stream"},
        body_base64=base64.b64encode(b"data: stub\n\n").decode("ascii"),
        telemetry={"elapsed_ms": 7},
    )

    background_tasks = BackgroundTasks()
    response = await gateway_module._finalize_gateway_chat_sync(
        payload,
        db=FailingDB(),
        background_tasks=background_tasks,
    )

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "id": "chatcmpl-fast-finalize-123",
        "object": "chat.completion",
        "choices": [],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
    }
    aggregate_mock.assert_awaited_once()
    assert len(background_tasks.tasks) == 1
    assert background_tasks.tasks[0].func is gateway_module._run_gateway_chat_sync_finalize_background


@pytest.mark.asyncio


@pytest.mark.asyncio
async def test_finalize_gateway_cli_sync_fast_path_returns_before_db_lookup() -> None:
    from src.api.internal import gateway as gateway_module

    class FailingDB:
        def query(self, model: object) -> object:
            raise AssertionError(f"db query should not happen for {getattr(model, '__name__', model)}")

    payload = GatewaySyncReportRequest(
        trace_id="trace-cli-fast-finalize-123",
        report_kind="openai_cli_sync_finalize",
        report_context={
            "user_id": "user-cli-fast-finalize-123",
            "api_key_id": "api-key-cli-fast-finalize-123",
            "provider_id": "provider-cli-fast-finalize-123",
            "endpoint_id": "endpoint-cli-fast-finalize-123",
            "key_id": "key-cli-fast-finalize-123",
            "request_id": "req-cli-fast-finalize-123",
            "model": "gpt-5",
            "provider_name": "openai",
            "provider_api_format": "openai:cli",
            "client_api_format": "openai:cli",
            "mapped_model": "gpt-5",
            "original_headers": {"content-type": "application/json"},
            "original_request_body": {"model": "gpt-5", "input": "hello"},
            "provider_request_headers": {"content-type": "application/json"},
            "provider_request_body": {"model": "gpt-5", "input": "hello"},
            "has_envelope": False,
            "needs_conversion": False,
        },
        status_code=200,
        headers={"content-type": "application/json"},
        body_json={
            "id": "resp-fast-finalize-123",
            "object": "response",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 2},
        },
        telemetry={"elapsed_ms": 5},
    )

    background_tasks = BackgroundTasks()
    response = await gateway_module._finalize_gateway_cli_sync(
        payload,
        db=FailingDB(),
        background_tasks=background_tasks,
    )

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "id": "resp-fast-finalize-123",
        "object": "response",
        "output": [],
        "usage": {"input_tokens": 1, "output_tokens": 2},
    }
    assert len(background_tasks.tasks) == 1
    assert background_tasks.tasks[0].func is gateway_module._run_gateway_cli_sync_finalize_background


def test_finalize_sync_route_uses_chat_fast_path_without_db_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    fast_mock = AsyncMock(
        return_value=JSONResponse(
            content={"id": "chat-fast-123", "object": "chat.completion", "choices": []}
        )
    )
    background_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._maybe_build_gateway_core_sync_fast_success_response",
        fast_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._run_gateway_chat_sync_finalize_background_with_session",
        background_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.create_session",
        lambda: (_ for _ in ()).throw(AssertionError("create_session should not be called")),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-chat-fast-route-123",
            "report_kind": "openai_chat_sync_finalize",
            "report_context": {
                "provider_api_format": "openai:chat",
                "client_api_format": "openai:chat",
                "has_envelope": False,
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "chat-fast-123", "object": "chat.completion", "choices": []},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json()["id"] == "chat-fast-123"
    fast_mock.assert_awaited_once()
    background_mock.assert_awaited_once()


def test_finalize_sync_route_uses_cli_fast_path_without_db_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    fast_mock = AsyncMock(
        return_value=JSONResponse(
            content={"id": "resp-fast-123", "object": "response", "output": []}
        )
    )
    background_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.api.internal.gateway._maybe_build_gateway_core_sync_fast_success_response",
        fast_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._run_gateway_cli_sync_finalize_background_with_session",
        background_mock,
    )
    monkeypatch.setattr(
        "src.api.internal.gateway.create_session",
        lambda: (_ for _ in ()).throw(AssertionError("create_session should not be called")),
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-cli-fast-route-123",
            "report_kind": "openai_cli_sync_finalize",
            "report_context": {
                "provider_api_format": "openai:cli",
                "client_api_format": "openai:cli",
                "has_envelope": False,
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "resp-fast-123", "object": "response", "output": []},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json()["id"] == "resp-fast-123"
    fast_mock.assert_awaited_once()
    background_mock.assert_awaited_once()


def test_finalize_sync_route_finalizes_openai_video_remix_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(
        return_value=JSONResponse(
            content={
                "id": "vid_local_remix_123",
                "object": "video",
                "status": "submitted",
            }
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_openai_video_remix_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-openai-video-remix-finalize",
            "report_kind": "openai_video_remix_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "task_id": "task-123",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"id": "ext-remix-task-123", "status": "submitted"},
            "telemetry": {"elapsed_ms": 55},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {
        "id": "vid_local_remix_123",
        "object": "video",
        "status": "submitted",
    }
    finalize_mock.assert_awaited_once()


def test_finalize_sync_route_finalizes_gemini_video_create_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(
        return_value=JSONResponse(
            content={
                "name": "models/veo-3/operations/aev_123",
                "done": False,
                "metadata": {},
            }
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_gemini_video_create_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-gemini-video-create-finalize",
            "report_kind": "gemini_video_create_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "model": "veo-3",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {"name": "operations/ext-video-123"},
            "telemetry": {"elapsed_ms": 48},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {
        "name": "models/veo-3/operations/aev_123",
        "done": False,
        "metadata": {},
    }
    finalize_mock.assert_awaited_once()


def test_finalize_sync_route_finalizes_openai_video_delete_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(
        return_value=JSONResponse(
            content={
                "id": "task-123",
                "object": "video",
                "deleted": True,
            }
        )
    )
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_openai_video_delete_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-openai-video-delete-finalize",
            "report_kind": "openai_video_delete_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "task_id": "task-123",
            },
            "status_code": 404,
            "headers": {"content-type": "application/json"},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {
        "id": "task-123",
        "object": "video",
        "deleted": True,
    }
    finalize_mock.assert_awaited_once()


def test_finalize_sync_route_finalizes_openai_video_cancel_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(return_value=JSONResponse(content={}))
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_openai_video_cancel_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-openai-video-cancel-finalize",
            "report_kind": "openai_video_cancel_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "task_id": "task-123",
            },
            "status_code": 204,
            "headers": {},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {}
    finalize_mock.assert_awaited_once()


def test_finalize_sync_route_finalizes_gemini_video_cancel_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: object()
    monkeypatch.setattr("src.api.internal.gateway.ensure_loopback", lambda request: None)
    finalize_mock = AsyncMock(return_value=JSONResponse(content={}))
    monkeypatch.setattr(
        "src.api.internal.gateway._finalize_gateway_gemini_video_cancel_sync",
        finalize_mock,
    )

    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.post(
        "/api/internal/gateway/finalize-sync",
        json={
            "trace_id": "trace-gemini-video-cancel-finalize",
            "report_kind": "gemini_video_cancel_sync_finalize",
            "report_context": {
                "user_id": "user-123",
                "api_key_id": "key-123",
                "task_id": "models/veo-3/operations/ext-123",
            },
            "status_code": 200,
            "headers": {"content-type": "application/json"},
            "body_json": {},
        },
    )

    assert response.status_code == 200
    assert response.headers[CONTROL_EXECUTED_HEADER] == "true"
    assert response.json() == {}
    finalize_mock.assert_awaited_once()

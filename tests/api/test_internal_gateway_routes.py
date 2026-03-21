import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient

from src.api.internal.gateway import (
    CONTROL_ACTION_HEADER,
    CONTROL_ACTION_PROXY_PUBLIC,
    CONTROL_EXECUTED_HEADER,
    GatewayExecutionPlanResponse,
    GatewayResolveRequest,
    _is_streaming_sync_payload,
    _resolve_auth_context,
    _resolve_gateway_sync_adapter,
    classify_gateway_route,
    router,
)
from src.database import get_db


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
    assert seen == ["files/abc-123"]


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
    record_mock.assert_awaited_once()


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
    record_mock.assert_awaited_once()

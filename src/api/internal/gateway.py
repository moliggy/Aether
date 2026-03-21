from __future__ import annotations

import base64
import re
import time
import uuid
from typing import Any
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.api.base.context import ApiRequestContext
from src.api.base.pipeline import get_pipeline
from src.core.api_format.headers import extract_client_api_key_for_endpoint_with_query
from src.core.crypto import crypto_service
from src.core.http_compression import normalize_content_encoding
from src.database import get_db
from src.models.database import ApiKey, User
from src.services.auth.service import AuthService

from .common import ensure_loopback

router = APIRouter(
    prefix="/api/internal/gateway",
    tags=["Internal - Gateway"],
    include_in_schema=False,
)

_GEMINI_MODEL_ROUTE_RE = re.compile(
    r"^/(v1|v1beta)/models/[^/]+:(generateContent|streamGenerateContent|predictLongRunning)$"
)
_GEMINI_MODEL_OPERATION_CANCEL_RE = re.compile(r"^/v1beta/models/[^/]+/operations/[^/]+:cancel$")
_GEMINI_OPERATION_CANCEL_RE = re.compile(r"^/v1beta/operations/[^/]+:cancel$")
_GEMINI_MODEL_OPERATION_ROUTE_RE = re.compile(r"^/v1beta/models/[^/]+/operations/[^/]+$")
_GEMINI_FILES_ROUTE_RE = re.compile(r"^/v1beta/files(?:/.+)?$")
_GEMINI_FILES_DOWNLOAD_ROUTE_RE = re.compile(r"^/v1beta/files/(?P<file_id>[^/]+):download$")
_GEMINI_FILES_RESOURCE_ROUTE_RE = re.compile(r"^/v1beta/files/(?P<file_name>.+)$")
_GEMINI_SYNC_ROUTE_RE = re.compile(
    r"^/(?P<version>v1|v1beta)/models/(?P<model>[^/]+):(?P<action>generateContent|streamGenerateContent)$"
)
_OPENAI_VIDEO_CANCEL_ROUTE_RE = re.compile(r"^/v1/videos/(?P<task_id>[^/]+)/cancel$")
_OPENAI_VIDEO_REMIX_ROUTE_RE = re.compile(r"^/v1/videos/(?P<task_id>[^/]+)/remix$")
_OPENAI_VIDEO_CONTENT_ROUTE_RE = re.compile(r"^/v1/videos/(?P<task_id>[^/]+)/content$")
_OPENAI_VIDEO_TASK_ROUTE_RE = re.compile(r"^/v1/videos/(?P<task_id>[^/]+)$")
_GEMINI_VIDEO_CREATE_ROUTE_RE = re.compile(r"^/v1beta/models/(?P<model>[^/]+):predictLongRunning$")
_GEMINI_VIDEO_MODEL_OPERATION_ANY_RE = re.compile(
    r"^/v1beta/models/(?P<model>[^/]+)/operations/(?P<operation_id>[^/]+)(?P<cancel>:cancel)?$"
)

CONTROL_EXECUTED_HEADER = "x-aether-control-executed"
CONTROL_ACTION_HEADER = "x-aether-control-action"
CONTROL_ACTION_PROXY_PUBLIC = "proxy_public"


class GatewayResolveRequest(BaseModel):
    trace_id: str | None = Field(None, max_length=128)
    method: str = Field(..., min_length=1, max_length=16)
    path: str = Field(..., min_length=1, max_length=2048)
    query_string: str | None = Field(None, max_length=8192)
    headers: dict[str, str] = Field(default_factory=dict)
    has_body: bool = False
    content_type: str | None = Field(None, max_length=512)
    content_length: int | None = Field(None, ge=0)


class GatewayRouteDecision(BaseModel):
    action: str = "proxy_public"
    route_class: str
    public_path: str
    public_query_string: str | None = None
    route_family: str | None = None
    route_kind: str | None = None
    auth_endpoint_signature: str | None = None
    executor_candidate: bool = False
    auth_context: dict[str, Any] | None = None


class GatewayAuthContext(BaseModel):
    user_id: str
    api_key_id: str
    balance_remaining: float | None = None
    access_allowed: bool = True


class GatewayExecuteRequest(BaseModel):
    trace_id: str | None = Field(None, max_length=128)
    method: str = Field(..., min_length=1, max_length=16)
    path: str = Field(..., min_length=1, max_length=2048)
    query_string: str | None = Field(None, max_length=8192)
    headers: dict[str, str] = Field(default_factory=dict)
    body_json: dict[str, Any] = Field(default_factory=dict)
    body_base64: str | None = None
    auth_context: GatewayAuthContext | None = None


class GatewayExecutionPlanResponse(BaseModel):
    action: str
    plan_kind: str
    plan: dict[str, Any]
    report_kind: str | None = None
    report_context: dict[str, Any] | None = None


class GatewaySyncReportRequest(BaseModel):
    trace_id: str | None = Field(None, max_length=128)
    report_kind: str = Field(..., min_length=1, max_length=128)
    report_context: dict[str, Any] = Field(default_factory=dict)
    status_code: int = Field(..., ge=100, le=599)
    headers: dict[str, str] = Field(default_factory=dict)
    body_json: Any = None
    body_base64: str | None = None
    telemetry: dict[str, Any] | None = None


def classify_gateway_route(
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
) -> GatewayRouteDecision:
    normalized_method = str(method or "").strip().upper() or "GET"
    normalized_path = str(path or "").strip() or "/"
    normalized_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"

    if normalized_method == "POST" and normalized_path == "/v1/chat/completions":
        return _ai_route(
            normalized_path,
            family="openai",
            kind="chat",
            auth_endpoint_signature="openai:chat",
        )

    if normalized_method == "POST" and normalized_path in {
        "/v1/responses",
        "/v1/responses/compact",
    }:
        route_kind = "compact" if normalized_path.endswith("/compact") else "cli"
        auth_endpoint_signature = "openai:compact" if route_kind == "compact" else "openai:cli"
        return _ai_route(
            normalized_path,
            family="openai",
            kind=route_kind,
            auth_endpoint_signature=auth_endpoint_signature,
        )

    if normalized_method == "POST" and normalized_path == "/v1/messages":
        is_claude_cli = _is_claude_cli_request(normalized_headers)
        return _ai_route(
            normalized_path,
            family="claude",
            kind="cli" if is_claude_cli else "chat",
            auth_endpoint_signature="claude:cli" if is_claude_cli else "claude:chat",
        )

    if normalized_path.startswith("/v1/videos"):
        return _ai_route(
            normalized_path,
            family="openai",
            kind="video",
            auth_endpoint_signature="openai:video",
        )

    if _GEMINI_MODEL_ROUTE_RE.match(normalized_path):
        if normalized_path.endswith(":predictLongRunning"):
            return _ai_route(
                normalized_path,
                family="gemini",
                kind="video",
                auth_endpoint_signature="gemini:video",
            )
        is_gemini_cli = _is_gemini_cli_request(normalized_headers)
        return _ai_route(
            normalized_path,
            family="gemini",
            kind="cli" if is_gemini_cli else "chat",
            auth_endpoint_signature="gemini:cli" if is_gemini_cli else "gemini:chat",
        )

    if (
        _GEMINI_MODEL_OPERATION_CANCEL_RE.match(normalized_path)
        or _GEMINI_OPERATION_CANCEL_RE.match(normalized_path)
        or _GEMINI_MODEL_OPERATION_ROUTE_RE.match(normalized_path)
        or normalized_path == "/v1beta/operations"
        or normalized_path.startswith("/v1beta/operations/")
    ):
        return _ai_route(
            normalized_path,
            family="gemini",
            kind="video",
            auth_endpoint_signature="gemini:video",
        )

    if normalized_method == "POST" and normalized_path == "/upload/v1beta/files":
        return _ai_route(
            normalized_path,
            family="gemini",
            kind="files",
            auth_endpoint_signature="gemini:chat",
        )

    if _GEMINI_FILES_ROUTE_RE.match(normalized_path):
        return _ai_route(
            normalized_path,
            family="gemini",
            kind="files",
            auth_endpoint_signature="gemini:chat",
        )

    return GatewayRouteDecision(
        route_class="passthrough",
        public_path=normalized_path,
        executor_candidate=False,
    )


def _ai_route(
    path: str,
    *,
    family: str,
    kind: str,
    auth_endpoint_signature: str,
) -> GatewayRouteDecision:
    return GatewayRouteDecision(
        route_class="ai_public",
        public_path=path,
        route_family=family,
        route_kind=kind,
        auth_endpoint_signature=auth_endpoint_signature,
        executor_candidate=True,
    )


@router.post("/resolve")
async def resolve_gateway_route(request: Request, payload: GatewayResolveRequest) -> dict[str, Any]:
    ensure_loopback(request)
    decision = classify_gateway_route(payload.method, payload.path, payload.headers)
    decision.auth_context = await _resolve_auth_context(payload, decision)
    return decision.model_dump(exclude_none=True)


@router.post("/execute-sync")
async def execute_gateway_sync(
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session = Depends(get_db),
) -> Response:
    return await _execute_gateway_control_request(
        request=request,
        payload=payload,
        db=db,
        require_stream=False,
    )


@router.post("/execute-stream")
async def execute_gateway_stream(
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session = Depends(get_db),
) -> Response:
    return await _execute_gateway_control_request(
        request=request,
        payload=payload,
        db=db,
        require_stream=True,
    )


@router.post("/plan-stream")
async def plan_gateway_stream(
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session = Depends(get_db),
) -> Response:
    ensure_loopback(request)
    try:
        planned = await _build_gateway_stream_plan_response(
            request=request,
            payload=payload,
            db=db,
        )
    except HTTPException as exc:
        headers = dict(exc.headers or {})
        headers[CONTROL_EXECUTED_HEADER] = "true"
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )

    if planned is None:
        return _build_proxy_public_fallback_response()

    return JSONResponse(status_code=200, content=planned.model_dump(exclude_none=True))


@router.post("/plan-sync")
async def plan_gateway_sync(
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session = Depends(get_db),
) -> Response:
    ensure_loopback(request)
    try:
        planned = await _build_gateway_sync_plan_response(
            request=request,
            payload=payload,
            db=db,
        )
    except HTTPException as exc:
        headers = dict(exc.headers or {})
        headers[CONTROL_EXECUTED_HEADER] = "true"
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )

    if planned is None:
        return _build_proxy_public_fallback_response()

    return JSONResponse(status_code=200, content=planned.model_dump(exclude_none=True))


@router.post("/report-sync")
async def report_gateway_sync(
    request: Request,
    payload: GatewaySyncReportRequest,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ensure_loopback(request)
    await _apply_gateway_sync_report(payload, db=db)
    return {"ok": True}


async def _execute_gateway_control_request(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    require_stream: bool,
) -> Response:
    ensure_loopback(request)
    decision = classify_gateway_route(payload.method, payload.path, payload.headers)
    if _is_gemini_files_route(decision):
        return await _execute_gateway_files_control_request(
            request=request,
            payload=payload,
            db=db,
            require_stream=require_stream,
        )

    adapter, path_params = _resolve_gateway_sync_adapter(decision, payload.path)
    if adapter is None:
        return _build_proxy_public_fallback_response()

    is_stream_request = _is_stream_request_payload(payload.body_json, path_params)
    if is_stream_request != require_stream:
        return _build_proxy_public_fallback_response()

    auth_context = payload.auth_context
    if auth_context is None or not auth_context.access_allowed:
        return _build_proxy_public_fallback_response()

    try:
        effective_request = (
            _build_gateway_forward_request(request=request, payload=payload)
            if _is_video_route(decision)
            else request
        )
        user, api_key = _load_gateway_auth_models(db, auth_context)
        pipeline = get_pipeline()
        await pipeline._check_user_rate_limit(effective_request, db, user, api_key)
        context = _build_gateway_request_context(
            request=effective_request,
            payload=payload,
            db=db,
            user=user,
            api_key=api_key,
            adapter=adapter,
            path_params=path_params,
            balance_remaining=auth_context.balance_remaining,
        )
        authorize_result = adapter.authorize(context)
        if hasattr(authorize_result, "__await__"):
            await authorize_result
        response = await adapter.handle(context)
    except HTTPException as exc:
        headers = dict(exc.headers or {})
        headers[CONTROL_EXECUTED_HEADER] = "true"
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )

    response.headers[CONTROL_EXECUTED_HEADER] = "true"
    return response


async def _execute_gateway_files_control_request(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    require_stream: bool,
) -> Response:
    if require_stream:
        return _build_proxy_public_fallback_response()

    auth_context = payload.auth_context
    if auth_context is None or not auth_context.access_allowed:
        return _build_proxy_public_fallback_response()

    try:
        user, api_key = _load_gateway_auth_models(db, auth_context)
        gateway_request = _build_gateway_forward_request(request=request, payload=payload)
        pipeline = get_pipeline()
        await pipeline._check_user_rate_limit(gateway_request, db, user, api_key)
        response = await _dispatch_gateway_files_handler(gateway_request, payload)
    except HTTPException as exc:
        headers = dict(exc.headers or {})
        headers[CONTROL_EXECUTED_HEADER] = "true"
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )

    response.headers[CONTROL_EXECUTED_HEADER] = "true"
    return response


def _is_claude_cli_request(headers: dict[str, str]) -> bool:
    auth_header = str(headers.get("authorization") or "").strip().lower()
    has_bearer = auth_header.startswith("bearer ")
    has_api_key = bool(str(headers.get("x-api-key") or "").strip())
    return has_bearer and not has_api_key


def _is_gemini_cli_request(headers: dict[str, str]) -> bool:
    x_app = str(headers.get("x-app") or "").lower()
    if "cli" in x_app:
        return True

    user_agent = str(headers.get("user-agent") or "").lower()
    return "geminicli" in user_agent or "gemini-cli" in user_agent


def _parse_query_string(query_string: str | None) -> dict[str, str]:
    return {
        str(key): str(value) for key, value in parse_qsl(query_string or "", keep_blank_values=True)
    }


async def _resolve_auth_context(
    payload: GatewayResolveRequest,
    decision: GatewayRouteDecision,
) -> dict[str, Any] | None:
    auth_endpoint_signature = str(decision.auth_endpoint_signature or "").strip().lower()
    if not auth_endpoint_signature:
        return None

    client_api_key = extract_client_api_key_for_endpoint_with_query(
        payload.headers,
        _parse_query_string(payload.query_string),
        auth_endpoint_signature,
    )
    if not client_api_key:
        return None

    auth_result = await AuthService.authenticate_api_key_threadsafe(client_api_key)
    if not auth_result or not auth_result.user or not auth_result.api_key:
        return None

    return GatewayAuthContext(
        user_id=str(auth_result.user.id),
        api_key_id=str(auth_result.api_key.id),
        balance_remaining=auth_result.balance_remaining,
        access_allowed=bool(auth_result.access_allowed),
    ).model_dump(exclude_none=True)


def _resolve_gateway_sync_adapter(
    decision: GatewayRouteDecision,
    path: str,
) -> tuple[Any | None, dict[str, Any]]:
    if decision.route_class != "ai_public":
        return None, {}

    family = str(decision.route_family or "").strip().lower()
    kind = str(decision.route_kind or "").strip().lower()
    if family == "openai" and kind == "chat":
        from src.api.handlers.openai import OpenAIChatAdapter

        return OpenAIChatAdapter(), {}
    if family == "openai" and kind == "cli":
        from src.api.handlers.openai_cli import OpenAICliAdapter

        return OpenAICliAdapter(), {}
    if family == "openai" and kind == "compact":
        from src.api.handlers.openai_cli import OpenAICompactAdapter

        return OpenAICompactAdapter(), {}
    if family == "claude" and kind == "chat":
        from src.api.handlers.claude.adapter import ClaudeChatAdapter

        return ClaudeChatAdapter(), {}
    if family == "claude" and kind == "cli":
        from src.api.handlers.claude_cli import ClaudeCliAdapter

        return ClaudeCliAdapter(), {}
    if family == "gemini" and kind == "chat":
        from src.api.handlers.gemini.adapter import GeminiChatAdapter

        return GeminiChatAdapter(), _extract_gemini_path_params(path)
    if family == "gemini" and kind == "cli":
        from src.api.handlers.gemini_cli import GeminiCliAdapter

        return GeminiCliAdapter(), _extract_gemini_path_params(path)
    if family == "openai" and kind == "video":
        from src.api.handlers.openai.video_adapter import OpenAIVideoAdapter

        return OpenAIVideoAdapter(), _extract_openai_video_path_params(path)
    if family == "gemini" and kind == "video":
        from src.api.handlers.gemini.video_adapter import GeminiVeoAdapter

        return GeminiVeoAdapter(), _extract_gemini_video_path_params(path)
    return None, {}


def _is_gemini_files_route(decision: GatewayRouteDecision) -> bool:
    return (
        decision.route_class == "ai_public"
        and decision.route_family == "gemini"
        and decision.route_kind == "files"
    )


def _is_video_route(decision: GatewayRouteDecision) -> bool:
    return decision.route_class == "ai_public" and decision.route_kind == "video"


async def _dispatch_gateway_files_handler(
    request: Request,
    payload: GatewayExecuteRequest,
) -> Response:
    from src.api.public.gemini_files import (
        delete_file,
        download_file,
        get_file,
        list_files,
        upload_file,
    )

    method = str(payload.method or "").strip().upper()
    path = str(payload.path or "").strip()

    if method == "POST" and path == "/upload/v1beta/files":
        return await upload_file(request)

    if method == "GET" and path == "/v1beta/files":
        query_params = _parse_query_string(payload.query_string)
        page_size: int | None = None
        if query_params.get("pageSize") not in {None, ""}:
            try:
                page_size = int(str(query_params["pageSize"]))
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid pageSize") from exc
        return await list_files(
            request,
            pageSize=page_size,
            pageToken=query_params.get("pageToken"),
        )

    download_match = _GEMINI_FILES_DOWNLOAD_ROUTE_RE.match(path)
    if method == "GET" and download_match:
        return await download_file(download_match.group("file_id"), request)

    resource_match = _GEMINI_FILES_RESOURCE_ROUTE_RE.match(path)
    if resource_match:
        file_name = resource_match.group("file_name")
        if method == "GET":
            return await get_file(file_name, request)
        if method == "DELETE":
            return await delete_file(file_name, request)

    return _build_proxy_public_fallback_response()


def _extract_gemini_path_params(path: str) -> dict[str, Any]:
    match = _GEMINI_SYNC_ROUTE_RE.match(str(path or "").strip())
    if not match:
        return {}
    action = str(match.group("action") or "").strip()
    return {
        "model": str(match.group("model") or "").strip(),
        "stream": action == "streamGenerateContent",
    }


def _extract_openai_video_path_params(path: str) -> dict[str, Any]:
    normalized_path = str(path or "").strip()
    for route_re, extra in (
        (_OPENAI_VIDEO_CANCEL_ROUTE_RE, {"action": "cancel"}),
        (_OPENAI_VIDEO_REMIX_ROUTE_RE, {}),
        (_OPENAI_VIDEO_CONTENT_ROUTE_RE, {}),
        (_OPENAI_VIDEO_TASK_ROUTE_RE, {}),
    ):
        match = route_re.match(normalized_path)
        if match:
            params = {"task_id": str(match.group("task_id") or "").strip()}
            params.update(extra)
            return params
    return {}


def _extract_gemini_video_path_params(path: str) -> dict[str, Any]:
    normalized_path = str(path or "").strip()
    create_match = _GEMINI_VIDEO_CREATE_ROUTE_RE.match(normalized_path)
    if create_match:
        return {"model": str(create_match.group("model") or "").strip()}

    model_operation_match = _GEMINI_VIDEO_MODEL_OPERATION_ANY_RE.match(normalized_path)
    if model_operation_match:
        operation_name = (
            f"models/{str(model_operation_match.group('model') or '').strip()}/operations/"
            f"{str(model_operation_match.group('operation_id') or '').strip()}"
        )
        params = {"task_id": operation_name}
        if model_operation_match.group("cancel"):
            params["action"] = "cancel"
        return params

    if normalized_path == "/v1beta/operations":
        return {}

    if normalized_path.startswith("/v1beta/operations/"):
        operation_id = normalized_path[len("/v1beta/operations/") :].strip()
        if operation_id.endswith(":cancel"):
            return {"task_id": operation_id[: -len(":cancel")], "action": "cancel"}
        return {"task_id": operation_id}

    return {}


def _is_stream_request_payload(
    body_json: dict[str, Any],
    path_params: dict[str, Any] | None = None,
) -> bool:
    if bool((path_params or {}).get("stream")):
        return True
    return bool(body_json.get("stream"))


def _is_streaming_sync_payload(
    body_json: dict[str, Any],
    path_params: dict[str, Any] | None = None,
) -> bool:
    return _is_stream_request_payload(body_json, path_params)


def _load_gateway_auth_models(
    db: Session,
    auth_context: GatewayAuthContext,
) -> tuple[User, ApiKey]:
    user = db.query(User).filter(User.id == auth_context.user_id).first()
    api_key = db.query(ApiKey).filter(ApiKey.id == auth_context.api_key_id).first()
    if not user or not api_key:
        raise HTTPException(status_code=401, detail="无效的API密钥")
    if not user.is_active or user.is_deleted:
        raise HTTPException(status_code=401, detail="无效的API密钥")
    if not api_key.is_active:
        raise HTTPException(status_code=401, detail="无效的API密钥")
    if api_key.is_locked and not api_key.is_standalone:
        raise HTTPException(status_code=403, detail="该密钥已被管理员锁定，请联系管理员")
    if str(api_key.user_id) != str(user.id):
        raise HTTPException(status_code=401, detail="无效的API密钥")
    return user, api_key


def _build_gateway_request_context(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    user: User,
    api_key: ApiKey,
    adapter: Any,
    path_params: dict[str, Any],
    balance_remaining: float | None,
) -> ApiRequestContext:
    request_id = str(
        payload.trace_id or getattr(request.state, "request_id", "") or uuid.uuid4().hex[:8]
    )
    request.state.request_id = request_id
    request.state.user_id = user.id
    request.state.api_key_id = api_key.id
    request.state.prefetched_balance_remaining = balance_remaining

    original_headers = {str(k): str(v) for k, v in (payload.headers or {}).items()}
    client_accept_encoding = str(original_headers.get("accept-encoding") or "").strip() or None
    raw_body = _extract_gateway_raw_body(payload)
    return ApiRequestContext(
        request=request,
        db=db,
        user=user,
        api_key=api_key,
        request_id=request_id,
        start_time=time.time(),
        client_ip=request.client.host if request.client else "127.0.0.1",
        user_agent=str(original_headers.get("user-agent") or "unknown"),
        original_headers=original_headers,
        query_params=_parse_query_string(payload.query_string),
        raw_body=raw_body,
        json_body=(dict(payload.body_json) if payload.body_json else None),
        balance_remaining=balance_remaining,
        mode=getattr(getattr(adapter, "mode", None), "value", "standard"),
        api_format_hint=(
            adapter.allowed_api_formats[0]
            if getattr(adapter, "allowed_api_formats", None)
            else None
        ),
        path_params=dict(path_params or {}),
        client_content_encoding=normalize_content_encoding(
            original_headers.get("content-encoding")
        ),
        client_accept_encoding=client_accept_encoding,
    )


def _build_gateway_forward_request(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
) -> Request:
    body = _decode_gateway_body(payload)
    header_items = [
        (str(key).encode("latin-1"), str(value).encode("latin-1"))
        for key, value in (payload.headers or {}).items()
    ]
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": str(payload.method or "GET").upper(),
        "scheme": "http",
        "path": str(payload.path or "/"),
        "raw_path": str(payload.path or "/").encode("utf-8"),
        "query_string": str(payload.query_string or "").encode("utf-8"),
        "headers": header_items,
        "client": ("127.0.0.1", 0),
        "server": ("127.0.0.1", 80),
        "app": request.app,
        "state": {"request_id": str(payload.trace_id or uuid.uuid4().hex[:8])},
    }

    received = False

    async def receive() -> dict[str, object]:
        nonlocal received
        if received:
            return {"type": "http.request", "body": b"", "more_body": False}
        received = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


def _decode_gateway_body(payload: GatewayExecuteRequest) -> bytes:
    if not payload.body_base64:
        return b""
    try:
        return base64.b64decode(payload.body_base64, validate=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid gateway body payload") from exc


def _extract_gateway_raw_body(payload: GatewayExecuteRequest) -> bytes:
    if payload.body_base64:
        return _decode_gateway_body(payload)
    if payload.body_json:
        return JSONResponse(content=payload.body_json).body
    return b""


def _build_proxy_public_fallback_response() -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"action": CONTROL_ACTION_PROXY_PUBLIC},
        headers={CONTROL_ACTION_HEADER: CONTROL_ACTION_PROXY_PUBLIC},
    )


async def _build_gateway_stream_plan_response(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
) -> GatewayExecutionPlanResponse | None:
    decision = classify_gateway_route(payload.method, payload.path, payload.headers)
    auth_context = payload.auth_context
    if auth_context is None or not auth_context.access_allowed:
        return None

    if decision.route_family == "gemini" and decision.route_kind == "files":
        download_match = _GEMINI_FILES_DOWNLOAD_ROUTE_RE.match(str(payload.path or "").strip())
        if not download_match or str(payload.method or "").strip().upper() != "GET":
            return None

        gateway_request = _build_gateway_forward_request(request=request, payload=payload)
        user, api_key = _load_gateway_auth_models(db, auth_context)
        pipeline = get_pipeline()
        await pipeline._check_user_rate_limit(gateway_request, db, user, api_key)
        plan = await _build_gemini_files_download_stream_plan(
            request=gateway_request,
            db=db,
            user=user,
            user_api_key=api_key,
            file_id=download_match.group("file_id"),
        )
        return GatewayExecutionPlanResponse(
            action="executor_stream",
            plan_kind="gemini_files_download",
            plan=plan,
        )

    if decision.route_family == "openai" and decision.route_kind == "video":
        content_match = _OPENAI_VIDEO_CONTENT_ROUTE_RE.match(str(payload.path or "").strip())
        if not content_match or str(payload.method or "").strip().upper() != "GET":
            return None

        gateway_request = _build_gateway_forward_request(request=request, payload=payload)
        user, api_key = _load_gateway_auth_models(db, auth_context)
        pipeline = get_pipeline()
        await pipeline._check_user_rate_limit(gateway_request, db, user, api_key)
        plan = await _build_openai_video_content_stream_plan(
            request=gateway_request,
            db=db,
            user=user,
            user_api_key=api_key,
            task_id=content_match.group("task_id"),
        )
        return GatewayExecutionPlanResponse(
            action="executor_stream",
            plan_kind="openai_video_content",
            plan=plan,
        )

    return None


async def _build_gateway_sync_plan_response(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
) -> GatewayExecutionPlanResponse | None:
    decision = classify_gateway_route(payload.method, payload.path, payload.headers)
    auth_context = payload.auth_context
    if auth_context is None or not auth_context.access_allowed:
        return None

    if decision.route_family == "openai" and decision.route_kind == "chat":
        planned = await _build_openai_chat_sync_plan(
            request=request,
            payload=payload,
            db=db,
            auth_context=auth_context,
            decision=decision,
        )
        if planned is not None:
            return planned

    if decision.route_family == "claude" and decision.route_kind == "chat":
        planned = await _build_claude_chat_sync_plan(
            request=request,
            payload=payload,
            db=db,
            auth_context=auth_context,
            decision=decision,
        )
        if planned is not None:
            return planned

    if decision.route_family == "gemini" and decision.route_kind == "chat":
        planned = await _build_gemini_chat_sync_plan(
            request=request,
            payload=payload,
            db=db,
            auth_context=auth_context,
            decision=decision,
        )
        if planned is not None:
            return planned

    if decision.route_family == "openai" and decision.route_kind in {"cli", "compact"}:
        planned = await _build_openai_cli_sync_plan(
            request=request,
            payload=payload,
            db=db,
            auth_context=auth_context,
            decision=decision,
        )
        if planned is not None:
            return planned

    if decision.route_family == "claude" and decision.route_kind == "cli":
        planned = await _build_claude_cli_sync_plan(
            request=request,
            payload=payload,
            db=db,
            auth_context=auth_context,
            decision=decision,
        )
        if planned is not None:
            return planned

    if decision.route_family == "gemini" and decision.route_kind == "cli":
        planned = await _build_gemini_cli_sync_plan(
            request=request,
            payload=payload,
            db=db,
            auth_context=auth_context,
            decision=decision,
        )
        if planned is not None:
            return planned

    if decision.route_family == "gemini" and decision.route_kind == "files":
        resource_match = _GEMINI_FILES_RESOURCE_ROUTE_RE.match(str(payload.path or "").strip())
        download_match = _GEMINI_FILES_DOWNLOAD_ROUTE_RE.match(str(payload.path or "").strip())
        method = str(payload.method or "").strip().upper()

        if method == "POST" and str(payload.path or "").strip() == "/upload/v1beta/files":
            gateway_request = _build_gateway_forward_request(request=request, payload=payload)
            user, api_key = _load_gateway_auth_models(db, auth_context)
            pipeline = get_pipeline()
            await pipeline._check_user_rate_limit(gateway_request, db, user, api_key)
            plan, report_context = await _build_gemini_files_proxy_sync_plan(
                request=gateway_request,
                db=db,
                method="POST",
                upstream_path="/v1beta/files",
                is_upload=True,
            )
            return GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_files_upload",
                plan=plan,
                report_kind="gemini_files_store_mapping",
                report_context=report_context,
            )

        if method == "GET" and str(payload.path or "").strip() == "/v1beta/files":
            gateway_request = _build_gateway_forward_request(request=request, payload=payload)
            user, api_key = _load_gateway_auth_models(db, auth_context)
            pipeline = get_pipeline()
            await pipeline._check_user_rate_limit(gateway_request, db, user, api_key)
            plan, report_context = await _build_gemini_files_proxy_sync_plan(
                request=gateway_request,
                db=db,
                method="GET",
                upstream_path="/v1beta/files",
            )
            return GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_files_list",
                plan=plan,
                report_kind="gemini_files_store_mapping",
                report_context=report_context,
            )

        if (
            not resource_match
            or download_match
            or str(payload.path or "").strip() == "/v1beta/files"
        ):
            return None

        gateway_request = _build_gateway_forward_request(request=request, payload=payload)
        user, api_key = _load_gateway_auth_models(db, auth_context)
        pipeline = get_pipeline()
        await pipeline._check_user_rate_limit(gateway_request, db, user, api_key)
        file_name = resource_match.group("file_name")
        if method == "GET":
            plan, report_context = await _build_gemini_files_get_sync_plan(
                request=gateway_request,
                db=db,
                file_name=file_name,
            )
            return GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_files_get",
                plan=plan,
                report_kind="gemini_files_store_mapping",
                report_context=report_context,
            )
        if method == "DELETE":
            normalized_file_name = (
                file_name if str(file_name or "").startswith("files/") else f"files/{file_name}"
            )
            plan, _report_context = await _build_gemini_files_proxy_sync_plan(
                request=gateway_request,
                db=db,
                method="DELETE",
                upstream_path=f"/v1beta/{normalized_file_name}",
            )
            return GatewayExecutionPlanResponse(
                action="executor_sync",
                plan_kind="gemini_files_delete",
                plan=plan,
                report_kind="gemini_files_delete_mapping",
                report_context={"file_name": normalized_file_name},
            )

    return None


async def _build_chat_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
    expected_api_format: str,
    plan_kind: str,
    report_kind: str,
) -> GatewayExecutionPlanResponse | None:
    from src.api.handlers.base.chat_adapter_base import ChatAdapterBase
    from src.api.handlers.base.chat_sync_executor import ChatSyncExecutor
    from src.services.task.request_state import MutableRequestBodyState

    if str(payload.method or "").strip().upper() != "POST":
        return None

    adapter, path_params = _resolve_gateway_sync_adapter(decision, payload.path)
    if not isinstance(adapter, ChatAdapterBase):
        return None
    if _is_stream_request_payload(payload.body_json, path_params):
        return None

    user, api_key = _load_gateway_auth_models(db, auth_context)
    pipeline = get_pipeline()
    await pipeline._check_user_rate_limit(request, db, user, api_key)

    context = _build_gateway_request_context(
        request=request,
        payload=payload,
        db=db,
        user=user,
        api_key=api_key,
        adapter=adapter,
        path_params=path_params,
        balance_remaining=auth_context.balance_remaining,
    )
    authorize_result = adapter.authorize(context)
    if hasattr(authorize_result, "__await__"):
        await authorize_result

    original_request_body = await context.ensure_json_body_async()
    if context.path_params:
        original_request_body = adapter._merge_path_params(
            original_request_body,
            context.path_params,
        )
    request_obj = adapter._validate_request_body(original_request_body, context.path_params)
    if isinstance(request_obj, JSONResponse):
        return None

    handler = adapter._create_handler(
        db=db,
        user=user,
        api_key=api_key,
        request_id=context.request_id,
        client_ip=context.client_ip,
        user_agent=context.user_agent,
        start_time=context.start_time,
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
    )

    await handler._convert_request(request_obj)
    model = handler.extract_model_from_request(
        original_request_body,
        context.path_params,
    )
    api_format = handler.allowed_api_formats[0]
    capability_requirements = handler._resolve_capability_requirements(
        model_name=model,
        request_headers=context.original_headers,
        request_body=original_request_body,
    )
    preferred_key_ids = await handler._resolve_preferred_key_ids(
        model_name=model,
        request_body=original_request_body,
    )

    candidate = await _select_gateway_direct_candidate(
        db=db,
        redis_client=getattr(handler, "redis", None),
        api_format=str(api_format),
        model_name=str(model or "unknown"),
        user_api_key=api_key,
        request_id=context.request_id,
        is_stream=False,
        capability_requirements=capability_requirements or None,
        preferred_key_ids=preferred_key_ids or None,
        request_body=original_request_body,
    )
    if candidate is None:
        return None

    sync_executor = ChatSyncExecutor(handler)
    prepared_plan = await sync_executor._build_sync_execution_plan(
        candidate.provider,
        candidate.endpoint,
        candidate.key,
        candidate,
        model=str(model or "unknown"),
        api_format=api_format,
        original_headers=context.original_headers,
        request_state=MutableRequestBodyState(original_request_body),
        query_params=context.query_params,
        client_content_encoding=context.client_content_encoding,
    )

    if (
        prepared_plan.upstream_is_stream
        or prepared_plan.needs_conversion
        or prepared_plan.envelope is not None
        or not prepared_plan.remote_eligible
        or str(prepared_plan.contract.provider_api_format or "").strip().lower()
        != expected_api_format
        or str(prepared_plan.contract.client_api_format or "").strip().lower()
        != expected_api_format
    ):
        return None

    return GatewayExecutionPlanResponse(
        action="executor_sync",
        plan_kind=plan_kind,
        plan=prepared_plan.contract.to_payload(),
        report_kind=report_kind,
        report_context={
            "user_id": str(user.id),
            "api_key_id": str(api_key.id),
            "request_id": str(context.request_id),
            "model": str(model or "unknown"),
            "provider_name": str(prepared_plan.contract.provider_name or "unknown"),
            "provider_id": str(prepared_plan.contract.provider_id or ""),
            "endpoint_id": str(prepared_plan.contract.endpoint_id or ""),
            "key_id": str(prepared_plan.contract.key_id or ""),
            "provider_api_format": str(prepared_plan.contract.provider_api_format or ""),
            "client_api_format": str(prepared_plan.contract.client_api_format or ""),
            "mapped_model": sync_executor._ctx.mapped_model_result,
            "original_headers": dict(context.original_headers),
            "original_request_body": original_request_body,
            "provider_request_headers": dict(prepared_plan.headers),
            "provider_request_body": prepared_plan.payload,
            "proxy_info": prepared_plan.proxy_info,
        },
    )


async def _build_openai_chat_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
) -> GatewayExecutionPlanResponse | None:
    return await _build_chat_sync_plan(
        request=request,
        payload=payload,
        db=db,
        auth_context=auth_context,
        decision=decision,
        expected_api_format="openai:chat",
        plan_kind="openai_chat_sync",
        report_kind="openai_chat_sync_success",
    )


async def _build_claude_chat_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
) -> GatewayExecutionPlanResponse | None:
    return await _build_chat_sync_plan(
        request=request,
        payload=payload,
        db=db,
        auth_context=auth_context,
        decision=decision,
        expected_api_format="claude:chat",
        plan_kind="claude_chat_sync",
        report_kind="claude_chat_sync_success",
    )


async def _build_gemini_chat_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
) -> GatewayExecutionPlanResponse | None:
    return await _build_chat_sync_plan(
        request=request,
        payload=payload,
        db=db,
        auth_context=auth_context,
        decision=decision,
        expected_api_format="gemini:chat",
        plan_kind="gemini_chat_sync",
        report_kind="gemini_chat_sync_success",
    )


async def _build_cli_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
    expected_provider_formats: set[str],
    plan_kind: str,
    report_kind: str,
    log_label: str,
) -> GatewayExecutionPlanResponse | None:
    from loguru import logger

    from src.api.handlers.base.cli_adapter_base import CliAdapterBase
    from src.config.settings import config
    from src.services.provider.transport import redact_url_for_log
    from src.services.proxy_node.resolver import (
        build_proxy_url_async,
        resolve_delegate_config_async,
        resolve_effective_proxy,
        resolve_proxy_info_async,
    )
    from src.services.request.executor_plan import (
        ExecutionPlan,
        ExecutionPlanTimeouts,
        ExecutionProxySnapshot,
        build_execution_plan_body,
        is_remote_contract_eligible,
    )

    if str(payload.method or "").strip().upper() != "POST":
        return None

    adapter, path_params = _resolve_gateway_sync_adapter(decision, payload.path)
    if not isinstance(adapter, CliAdapterBase):
        return None
    if _is_stream_request_payload(payload.body_json, path_params):
        return None

    user, api_key = _load_gateway_auth_models(db, auth_context)
    pipeline = get_pipeline()
    await pipeline._check_user_rate_limit(request, db, user, api_key)

    context = _build_gateway_request_context(
        request=request,
        payload=payload,
        db=db,
        user=user,
        api_key=api_key,
        adapter=adapter,
        path_params=path_params,
        balance_remaining=auth_context.balance_remaining,
    )
    authorize_result = adapter.authorize(context)
    if hasattr(authorize_result, "__await__"):
        await authorize_result

    original_request_body = await context.ensure_json_body_async()
    if context.path_params:
        original_request_body = adapter._merge_path_params(
            original_request_body,
            context.path_params,
        )
    if decision.route_kind == "compact":
        original_request_body.pop("stream", None)

    handler = adapter.HANDLER_CLASS(
        db=db,
        user=user,
        api_key=api_key,
        request_id=context.request_id,
        client_ip=context.client_ip,
        user_agent=context.user_agent,
        start_time=context.start_time,
        allowed_api_formats=adapter.allowed_api_formats,
        adapter_detector=adapter.detect_capability_requirements,
        perf_metrics=context.extra.get("perf"),
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
    )

    model = handler.extract_model_from_request(original_request_body, context.path_params)
    client_api_format = handler.primary_api_format
    capability_requirements = handler._resolve_capability_requirements(
        model_name=model,
        request_headers=context.original_headers,
        request_body=original_request_body,
    )
    preferred_key_ids = await handler._resolve_preferred_key_ids(
        model_name=model,
        request_body=original_request_body,
    )

    candidate = await _select_gateway_direct_candidate(
        db=db,
        redis_client=getattr(handler, "redis", None),
        api_format=str(client_api_format),
        model_name=str(model or "unknown"),
        user_api_key=api_key,
        request_id=context.request_id,
        is_stream=False,
        capability_requirements=capability_requirements or None,
        preferred_key_ids=preferred_key_ids or None,
        request_body=original_request_body,
    )
    if candidate is None:
        return None

    provider = candidate.provider
    endpoint = candidate.endpoint
    key = candidate.key
    provider_api_format = str(endpoint.api_format or "")
    mapped_model = candidate.mapping_matched_model if candidate else None
    if not mapped_model:
        mapped_model = await handler._get_mapped_model(
            source_model=str(model or "unknown"),
            provider_id=str(provider.id),
        )
    needs_conversion = bool(getattr(candidate, "needs_conversion", False))

    upstream_request = await handler._build_upstream_request(
        provider=provider,
        endpoint=endpoint,
        key=key,
        request_body=dict(original_request_body),
        original_headers=context.original_headers,
        query_params=context.query_params,
        client_api_format=str(client_api_format),
        provider_api_format=provider_api_format,
        fallback_model=str(model or "unknown"),
        mapped_model=mapped_model,
        client_is_stream=False,
        needs_conversion=needs_conversion,
        output_limit=candidate.output_limit if candidate else None,
    )

    _effective_proxy = resolve_effective_proxy(provider.proxy, getattr(key, "proxy", None))
    sync_proxy_info = await resolve_proxy_info_async(_effective_proxy)
    delegate_cfg = await resolve_delegate_config_async(_effective_proxy)
    is_tunnel_delegate = bool(delegate_cfg and delegate_cfg.get("tunnel"))
    proxy_url: str | None = None
    if _effective_proxy and not is_tunnel_delegate:
        proxy_url = await build_proxy_url_async(_effective_proxy)

    contract = ExecutionPlan(
        request_id=str(context.request_id or ""),
        candidate_id=str(
            getattr(candidate, "request_candidate_id", "") or getattr(candidate, "id", "") or ""
        )
        or None,
        provider_name=str(provider.name),
        provider_id=str(provider.id),
        endpoint_id=str(endpoint.id),
        key_id=str(key.id),
        method="POST",
        url=upstream_request.url,
        headers=dict(upstream_request.headers),
        body=build_execution_plan_body(
            upstream_request.payload,
            content_type=str(upstream_request.headers.get("content-type") or "").strip() or None,
        ),
        stream=upstream_request.upstream_is_stream,
        provider_api_format=provider_api_format,
        client_api_format=str(client_api_format),
        model_name=str(model or ""),
        content_type=str(upstream_request.headers.get("content-type") or "").strip() or None,
        content_encoding=context.client_content_encoding,
        proxy=ExecutionProxySnapshot.from_proxy_info(
            sync_proxy_info,
            proxy_url=proxy_url,
            mode_override="tunnel" if is_tunnel_delegate else None,
            node_id_override=(
                str(delegate_cfg.get("node_id") or "").strip() or None
                if is_tunnel_delegate
                else None
            ),
        ),
        tls_profile=upstream_request.tls_profile,
        timeouts=ExecutionPlanTimeouts(
            connect_ms=int(config.http_connect_timeout * 1000),
            read_ms=int(config.http_read_timeout * 1000),
            write_ms=int(config.http_write_timeout * 1000),
            pool_ms=int(config.http_pool_timeout * 1000),
            total_ms=int((provider.request_timeout or config.http_request_timeout) * 1000),
        ),
    )

    if (
        upstream_request.upstream_is_stream
        or needs_conversion
        or upstream_request.envelope is not None
        or not is_remote_contract_eligible(contract)
        or provider_api_format != str(client_api_format)
        or provider_api_format not in expected_provider_formats
    ):
        return None

    logger.debug(
        "[gateway] {} sync direct executor candidate accepted: path={} provider={} url={}",
        log_label,
        payload.path,
        provider.name,
        redact_url_for_log(upstream_request.url),
    )

    return GatewayExecutionPlanResponse(
        action="executor_sync",
        plan_kind=plan_kind,
        plan=contract.to_payload(),
        report_kind=report_kind,
        report_context={
            "user_id": str(user.id),
            "api_key_id": str(api_key.id),
            "request_id": str(context.request_id),
            "model": str(model or "unknown"),
            "provider_name": str(contract.provider_name or "unknown"),
            "provider_id": str(contract.provider_id or ""),
            "endpoint_id": str(contract.endpoint_id or ""),
            "key_id": str(contract.key_id or ""),
            "provider_api_format": str(contract.provider_api_format or ""),
            "client_api_format": str(contract.client_api_format or ""),
            "mapped_model": mapped_model,
            "original_headers": dict(context.original_headers),
            "original_request_body": original_request_body,
            "provider_request_headers": dict(upstream_request.headers),
            "provider_request_body": upstream_request.payload,
            "proxy_info": sync_proxy_info,
        },
    )


async def _build_openai_cli_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
) -> GatewayExecutionPlanResponse | None:
    return await _build_cli_sync_plan(
        request=request,
        payload=payload,
        db=db,
        auth_context=auth_context,
        decision=decision,
        expected_provider_formats={"openai:cli", "openai:compact"},
        plan_kind="openai_compact_sync" if decision.route_kind == "compact" else "openai_cli_sync",
        report_kind="openai_cli_sync_success",
        log_label="openai cli",
    )


async def _build_claude_cli_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
) -> GatewayExecutionPlanResponse | None:
    return await _build_cli_sync_plan(
        request=request,
        payload=payload,
        db=db,
        auth_context=auth_context,
        decision=decision,
        expected_provider_formats={"claude:cli"},
        plan_kind="claude_cli_sync",
        report_kind="claude_cli_sync_success",
        log_label="claude cli",
    )


async def _build_gemini_cli_sync_plan(
    *,
    request: Request,
    payload: GatewayExecuteRequest,
    db: Session,
    auth_context: GatewayAuthContext,
    decision: GatewayRouteDecision,
) -> GatewayExecutionPlanResponse | None:
    return await _build_cli_sync_plan(
        request=request,
        payload=payload,
        db=db,
        auth_context=auth_context,
        decision=decision,
        expected_provider_formats={"gemini:cli"},
        plan_kind="gemini_cli_sync",
        report_kind="gemini_cli_sync_success",
        log_label="gemini cli",
    )


async def _build_gemini_files_download_stream_plan(
    *,
    request: Request,
    db: Session,
    user: User,
    user_api_key: ApiKey,
    file_id: str,
) -> dict[str, Any]:
    from src.api.public.gemini_files import (
        GEMINI_FILES_BASE_URL,
        UpstreamContext,
        _build_upstream_headers,
        _build_upstream_url,
        _enrich_upstream_context_proxy,
        _find_video_task_by_id,
        _resolve_files_model_name,
        _select_provider_candidate,
        resolve_provider_proxy,
    )
    from src.services.request.executor_plan import (
        ExecutionPlan,
        ExecutionPlanBody,
        ExecutionPlanTimeouts,
        ExecutionProxySnapshot,
    )

    proxy_snapshot = None
    provider_id = ""
    endpoint_id = ""
    file_key_id = ""

    if file_id.startswith("aev_"):
        short_id = file_id[4:]
        upstream_key, video_url = await _find_video_task_by_id(db, short_id, user.id)
        if not upstream_key or not video_url:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": 404,
                        "message": f"Video not found or not ready: {file_id}",
                        "status": "NOT_FOUND",
                    }
                },
            )
        upstream_url = video_url

        try:
            from src.services.proxy_node.resolver import (
                build_proxy_url_async,
                get_system_proxy_config_async,
                resolve_delegate_config_async,
                resolve_proxy_info_async,
            )

            system_proxy = await get_system_proxy_config_async()
            delegate_cfg = await resolve_delegate_config_async(system_proxy)
            proxy_url: str | None = None
            if system_proxy and not (delegate_cfg and delegate_cfg.get("tunnel")):
                proxy_url = await build_proxy_url_async(system_proxy)
            proxy_info = await resolve_proxy_info_async(system_proxy)
            proxy_snapshot = ExecutionProxySnapshot.from_proxy_info(
                proxy_info,
                proxy_url=proxy_url,
                mode_override="tunnel" if delegate_cfg and delegate_cfg.get("tunnel") else None,
                node_id_override=(
                    str(delegate_cfg.get("node_id") or "").strip() or None
                    if delegate_cfg and delegate_cfg.get("tunnel")
                    else None
                ),
            )
        except Exception:
            proxy_snapshot = None
    else:
        model_name = _resolve_files_model_name(db, user_api_key, user)
        if not model_name:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": {
                        "code": 503,
                        "message": "No available model for Gemini Files API routing",
                        "status": "UNAVAILABLE",
                    }
                },
            )

        candidate = await _select_provider_candidate(
            db,
            user_api_key,
            model_name,
            require_files_capability=True,
        )
        if not candidate:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": {
                        "code": 503,
                        "message": "No available Gemini key with 'gemini_files' capability enabled",
                        "status": "UNAVAILABLE",
                    }
                },
            )

        try:
            upstream_key = crypto_service.decrypt(candidate.key.api_key)
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": {
                        "code": 500,
                        "message": "Failed to decrypt provider key",
                        "status": "INTERNAL",
                    }
                },
            ) from exc

        regular_ctx = UpstreamContext(
            upstream_key=upstream_key,
            base_url=candidate.endpoint.base_url or GEMINI_FILES_BASE_URL,
            file_key_id=str(candidate.key.id),
            user_id=str(user.id),
            provider_id=str(candidate.provider.id),
            endpoint_id=str(candidate.endpoint.id),
            provider_proxy=resolve_provider_proxy(endpoint=candidate.endpoint, key=candidate.key),
            key_proxy=(
                candidate.key.proxy
                if isinstance(getattr(candidate.key, "proxy", None), dict)
                else None
            ),
        )
        ctx = await _enrich_upstream_context_proxy(regular_ctx)
        file_key_id = ctx.file_key_id
        provider_id = ctx.provider_id
        endpoint_id = ctx.endpoint_id
        proxy_snapshot = ctx.proxy_snapshot
        file_name = f"files/{file_id}" if not file_id.startswith("files/") else file_id
        upstream_url = _build_upstream_url(
            ctx.base_url,
            f"/v1beta/{file_name}:download",
            dict(request.query_params),
        )

    headers = _build_upstream_headers(dict(request.headers), upstream_key)
    plan = ExecutionPlan(
        request_id=str(getattr(request.state, "request_id", "") or uuid.uuid4().hex),
        candidate_id=None,
        provider_name="gemini",
        provider_id=provider_id,
        endpoint_id=endpoint_id,
        key_id=str(file_key_id or ""),
        method="GET",
        url=upstream_url,
        headers=headers,
        body=ExecutionPlanBody(),
        stream=True,
        provider_api_format="gemini:files",
        client_api_format="gemini:files",
        model_name="gemini-files",
        proxy=proxy_snapshot,
        timeouts=ExecutionPlanTimeouts(
            connect_ms=30_000,
            read_ms=300_000,
            write_ms=300_000,
            pool_ms=30_000,
            total_ms=None,
        ),
    )
    return plan.to_payload()


async def _build_openai_video_content_stream_plan(
    *,
    request: Request,
    db: Session,
    user: User,
    user_api_key: ApiKey,
    task_id: str,
) -> dict[str, Any]:
    from src.api.handlers.openai.video_handler import OpenAIVideoHandler
    from src.core.api_format.conversion.internal_video import VideoStatus
    from src.services.request.executor_plan import (
        ExecutionPlan,
        ExecutionPlanBody,
        ExecutionPlanTimeouts,
    )

    request_id = str(getattr(request.state, "request_id", "") or uuid.uuid4().hex)
    handler = OpenAIVideoHandler(
        db=db,
        user=user,
        api_key=user_api_key,
        request_id=request_id,
        client_ip=request.client.host if request.client else "127.0.0.1",
        user_agent=str(request.headers.get("user-agent") or "aether-gateway"),
        start_time=time.time(),
    )

    task = handler._get_task(task_id)

    if task.status in (
        VideoStatus.PENDING.value,
        VideoStatus.SUBMITTED.value,
        VideoStatus.QUEUED.value,
        VideoStatus.PROCESSING.value,
    ):
        raise HTTPException(
            status_code=202,
            detail=f"Video is still processing (status: {task.status})",
        )
    if task.status == VideoStatus.FAILED.value:
        raise HTTPException(
            status_code=422,
            detail=f"Video generation failed: {task.error_message or 'Unknown error'}",
        )
    if task.status == VideoStatus.CANCELLED.value:
        raise HTTPException(status_code=404, detail="Video task was cancelled")

    query_params = dict(request.query_params)
    variant = str(query_params.get("variant") or "video")
    if variant not in {"video", "thumbnail", "spritesheet"}:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid variant '{variant}'. Must be one of: video, thumbnail, spritesheet",
        )

    provider_id = ""
    endpoint_id = ""
    key_id = ""

    if variant == "video" and task.video_url and str(task.video_url).startswith("http"):
        upstream_url = str(task.video_url)
        headers: dict[str, str] = {}
    else:
        if not task.external_task_id:
            raise HTTPException(status_code=500, detail="Task missing external_task_id")
        endpoint, key = handler._get_endpoint_and_key(task)
        if not key.api_key:
            raise HTTPException(status_code=500, detail="Provider key not configured")
        try:
            upstream_key = crypto_service.decrypt(key.api_key)
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Failed to decrypt provider key") from exc

        content_path = f"{task.external_task_id}/content"
        if variant != "video":
            content_path = f"{content_path}?variant={variant}"
        upstream_url = handler._build_upstream_url(endpoint.base_url, content_path)
        headers = handler._build_upstream_headers(dict(request.headers), upstream_key, endpoint)
        provider_id = str(getattr(endpoint, "provider_id", "") or "")
        endpoint_id = str(getattr(endpoint, "id", "") or "")
        key_id = str(getattr(key, "id", "") or "")

    plan = ExecutionPlan(
        request_id=request_id,
        candidate_id=None,
        provider_name="openai",
        provider_id=provider_id,
        endpoint_id=endpoint_id,
        key_id=key_id,
        method="GET",
        url=upstream_url,
        headers=headers,
        body=ExecutionPlanBody(),
        stream=True,
        provider_api_format="openai:video",
        client_api_format="openai:video",
        model_name=str(getattr(task, "model", "") or ""),
        timeouts=ExecutionPlanTimeouts(
            connect_ms=30_000,
            read_ms=300_000,
            write_ms=300_000,
            pool_ms=30_000,
            total_ms=None,
        ),
    )
    return plan.to_payload()


async def _build_gemini_files_get_sync_plan(
    *,
    request: Request,
    db: Session,
    file_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_file_name = (
        file_name if str(file_name or "").startswith("files/") else f"files/{file_name}"
    )
    return await _build_gemini_files_proxy_sync_plan(
        request=request,
        db=db,
        method="GET",
        upstream_path=f"/v1beta/{normalized_file_name}",
    )


async def _build_gemini_files_proxy_sync_plan(
    *,
    request: Request,
    db: Session,
    method: str,
    upstream_path: str,
    is_upload: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from src.api.public.gemini_files import (
        _build_upstream_headers,
        _build_upstream_url,
        _enrich_upstream_context_proxy,
        _resolve_upstream_context,
    )
    from src.services.request.executor_plan import (
        ExecutionPlan,
        ExecutionPlanTimeouts,
        build_execution_plan_body,
    )

    ctx = await _resolve_upstream_context(request, db)
    ctx = await _enrich_upstream_context_proxy(ctx)

    upstream_url = _build_upstream_url(
        ctx.base_url,
        upstream_path,
        dict(request.query_params),
        is_upload=is_upload,
    )
    headers = _build_upstream_headers(dict(request.headers), ctx.upstream_key)
    body_bytes = await request.body()
    content_type = str(headers.get("content-type") or "").strip() or None
    plan = ExecutionPlan(
        request_id=str(getattr(request.state, "request_id", "") or uuid.uuid4().hex),
        candidate_id=None,
        provider_name="gemini",
        provider_id=str(ctx.provider_id or ""),
        endpoint_id=str(ctx.endpoint_id or ""),
        key_id=str(ctx.file_key_id or ""),
        method=method.upper(),
        url=upstream_url,
        headers=headers,
        body=(
            build_execution_plan_body(body_bytes, content_type=content_type)
            if body_bytes
            else build_execution_plan_body(None)
        ),
        stream=False,
        provider_api_format="gemini:files",
        client_api_format="gemini:files",
        model_name="gemini-files",
        proxy=ctx.proxy_snapshot,
        timeouts=ExecutionPlanTimeouts(
            connect_ms=30_000,
            read_ms=300_000,
            write_ms=300_000,
            pool_ms=30_000,
            total_ms=300_000,
        ),
    )
    return plan.to_payload(), {
        "file_key_id": str(ctx.file_key_id or ""),
        "user_id": str(ctx.user_id or ""),
    }


async def _select_gateway_direct_candidate(
    *,
    db: Session,
    redis_client: Any | None,
    api_format: str,
    model_name: str,
    user_api_key: ApiKey,
    request_id: str,
    is_stream: bool,
    capability_requirements: dict[str, bool] | None,
    preferred_key_ids: list[str] | None,
    request_body: dict[str, Any] | None,
) -> Any | None:
    from src.services.candidate.resolver import CandidateResolver
    from src.services.scheduling.aware_scheduler import (
        CacheAwareScheduler,
        get_cache_aware_scheduler,
    )
    from src.services.system.config import SystemConfigService
    from src.services.task.execute.pool import TaskPoolOperationsService

    priority_mode = SystemConfigService.get_config(
        db,
        "provider_priority_mode",
        CacheAwareScheduler.PRIORITY_MODE_PROVIDER,
    )
    scheduling_mode = SystemConfigService.get_config(
        db,
        "scheduling_mode",
        CacheAwareScheduler.SCHEDULING_MODE_CACHE_AFFINITY,
    )
    cache_scheduler = await get_cache_aware_scheduler(
        redis_client,
        priority_mode=priority_mode,
        scheduling_mode=scheduling_mode,
    )
    await cache_scheduler._ensure_initialized()

    candidate_resolver = CandidateResolver(
        db=db,
        cache_scheduler=cache_scheduler,
    )
    candidates, _global_model_id = await candidate_resolver.fetch_candidates(
        api_format=api_format,
        model_name=model_name,
        affinity_key=str(user_api_key.id),
        user_api_key=user_api_key,
        request_id=request_id,
        is_stream=is_stream,
        capability_requirements=capability_requirements,
        preferred_key_ids=preferred_key_ids,
        request_body=request_body,
    )
    candidates, _pool_traces = await TaskPoolOperationsService().apply_pool_reorder(
        candidates,
        request_body=request_body,
    )
    return candidates[0] if candidates else None


async def _apply_gateway_sync_report(
    payload: GatewaySyncReportRequest,
    *,
    db: Session,
) -> None:
    if payload.report_kind == "gemini_files_store_mapping":
        from src.api.public.gemini_files import _maybe_store_file_mapping_from_payload

        await _maybe_store_file_mapping_from_payload(
            status_code=payload.status_code,
            headers=dict(payload.headers or {}),
            content_bytes=_extract_gateway_report_body_bytes(payload),
            file_key_id=str(payload.report_context.get("file_key_id") or "") or None,
            user_id=str(payload.report_context.get("user_id") or "") or None,
        )
    elif payload.report_kind == "gemini_files_delete_mapping" and payload.status_code < 300:
        from src.services.gemini_files_mapping import delete_file_key_mapping

        file_name = str(payload.report_context.get("file_name") or "").strip()
        if file_name:
            await delete_file_key_mapping(file_name)
    elif payload.report_kind == "openai_chat_sync_success":
        await _record_gateway_openai_chat_sync_success(payload, db=db)
    elif payload.report_kind == "claude_chat_sync_success":
        await _record_gateway_passthrough_chat_sync_success(payload, db=db)
    elif payload.report_kind == "gemini_chat_sync_success":
        await _record_gateway_passthrough_chat_sync_success(payload, db=db)
    elif payload.report_kind == "openai_cli_sync_success":
        await _record_gateway_passthrough_cli_sync_success(payload, db=db)
    elif payload.report_kind == "claude_cli_sync_success":
        await _record_gateway_passthrough_cli_sync_success(payload, db=db)
    elif payload.report_kind == "gemini_cli_sync_success":
        await _record_gateway_passthrough_cli_sync_success(payload, db=db)


async def _record_gateway_openai_chat_sync_success(
    payload: GatewaySyncReportRequest,
    *,
    db: Session,
) -> None:
    from src.api.handlers.openai import OpenAIChatAdapter

    if payload.status_code >= 400:
        return
    if not isinstance(payload.body_json, dict) or payload.body_json.get("error") is not None:
        return

    context = dict(payload.report_context or {})
    user_id = str(context.get("user_id") or "").strip()
    api_key_id = str(context.get("api_key_id") or "").strip()
    if not user_id or not api_key_id:
        return

    user = db.query(User).filter(User.id == user_id).first()
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not user or not api_key:
        return

    adapter = OpenAIChatAdapter()
    handler = adapter._create_handler(
        db=db,
        user=user,
        api_key=api_key,
        request_id=str(context.get("request_id") or payload.trace_id or uuid.uuid4().hex[:8]),
        client_ip="127.0.0.1",
        user_agent=str(
            (context.get("original_headers") or {}).get("user-agent") or "aether-gateway"
        ),
        start_time=time.time(),
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
    )

    response_json = handler._normalize_response(dict(payload.body_json))
    usage_info = handler._extract_usage(response_json)
    request_metadata: dict[str, Any] = {
        "gateway_direct_executor": True,
        "phase": "3c_trial",
    }
    proxy_info = context.get("proxy_info")
    if isinstance(proxy_info, dict):
        request_metadata["proxy"] = proxy_info

    response_time_ms = 0
    if isinstance(payload.telemetry, dict):
        raw_elapsed_ms = payload.telemetry.get("elapsed_ms")
        try:
            response_time_ms = max(int(raw_elapsed_ms or 0), 0)
        except (TypeError, ValueError):
            response_time_ms = 0

    await handler.telemetry.record_success(
        provider=str(context.get("provider_name") or "openai"),
        model=str(context.get("model") or "unknown"),
        input_tokens=int(usage_info.get("input_tokens", 0) or 0),
        output_tokens=int(usage_info.get("output_tokens", 0) or 0),
        response_time_ms=response_time_ms,
        status_code=payload.status_code,
        request_headers=dict(context.get("original_headers") or {}),
        request_body=dict(context.get("original_request_body") or {}),
        response_headers=dict(payload.headers or {}),
        client_response_headers=dict(payload.headers or {}),
        response_body=response_json,
        provider_request_headers=dict(context.get("provider_request_headers") or {}),
        provider_request_body=context.get("provider_request_body"),
        is_stream=False,
        provider_id=str(context.get("provider_id") or "") or None,
        provider_endpoint_id=str(context.get("endpoint_id") or "") or None,
        provider_api_key_id=str(context.get("key_id") or "") or None,
        api_format=str(context.get("client_api_format") or "openai:chat"),
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
        endpoint_api_format=str(context.get("provider_api_format") or "") or None,
        has_format_conversion=False,
        target_model=str(context.get("mapped_model") or "") or None,
        request_metadata=request_metadata,
    )


async def _record_gateway_passthrough_chat_sync_success(
    payload: GatewaySyncReportRequest,
    *,
    db: Session,
) -> None:
    from src.api.handlers.claude import ClaudeChatAdapter
    from src.api.handlers.gemini import GeminiChatAdapter
    from src.api.handlers.openai import OpenAIChatAdapter

    if payload.status_code >= 400:
        return
    if not isinstance(payload.body_json, dict) or payload.body_json.get("error") is not None:
        return

    context = dict(payload.report_context or {})
    user_id = str(context.get("user_id") or "").strip()
    api_key_id = str(context.get("api_key_id") or "").strip()
    client_api_format = str(context.get("client_api_format") or "").strip().lower()
    if not user_id or not api_key_id or not client_api_format:
        return

    user = db.query(User).filter(User.id == user_id).first()
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not user or not api_key:
        return

    if client_api_format == "claude:chat":
        adapter = ClaudeChatAdapter()
    elif client_api_format == "gemini:chat":
        adapter = GeminiChatAdapter()
    elif client_api_format == "openai:chat":
        adapter = OpenAIChatAdapter()
    else:
        return

    handler = adapter._create_handler(
        db=db,
        user=user,
        api_key=api_key,
        request_id=str(context.get("request_id") or payload.trace_id or uuid.uuid4().hex[:8]),
        client_ip="127.0.0.1",
        user_agent=str(
            (context.get("original_headers") or {}).get("user-agent") or "aether-gateway"
        ),
        start_time=time.time(),
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
    )

    response_json = handler._normalize_response(dict(payload.body_json))
    usage_info = handler._extract_usage(response_json)
    request_metadata: dict[str, Any] = {
        "gateway_direct_executor": True,
        "phase": "3c_trial",
    }
    proxy_info = context.get("proxy_info")
    if isinstance(proxy_info, dict):
        request_metadata["proxy"] = proxy_info

    response_time_ms = 0
    if isinstance(payload.telemetry, dict):
        raw_elapsed_ms = payload.telemetry.get("elapsed_ms")
        try:
            response_time_ms = max(int(raw_elapsed_ms or 0), 0)
        except (TypeError, ValueError):
            response_time_ms = 0

    await handler.telemetry.record_success(
        provider=str(context.get("provider_name") or "unknown"),
        model=str(context.get("model") or "unknown"),
        input_tokens=int(usage_info.get("input_tokens", 0) or 0),
        output_tokens=int(usage_info.get("output_tokens", 0) or 0),
        response_time_ms=response_time_ms,
        status_code=payload.status_code,
        request_headers=dict(context.get("original_headers") or {}),
        request_body=dict(context.get("original_request_body") or {}),
        response_headers=dict(payload.headers or {}),
        client_response_headers=dict(payload.headers or {}),
        response_body=response_json,
        provider_request_headers=dict(context.get("provider_request_headers") or {}),
        provider_request_body=context.get("provider_request_body"),
        cache_creation_tokens=int(
            usage_info.get("cache_creation_input_tokens", 0)
            or usage_info.get("cache_creation_tokens", 0)
            or 0
        ),
        cache_read_tokens=int(
            usage_info.get("cache_read_input_tokens", 0)
            or usage_info.get("cache_read_tokens", 0)
            or 0
        ),
        is_stream=False,
        provider_id=str(context.get("provider_id") or "") or None,
        provider_endpoint_id=str(context.get("endpoint_id") or "") or None,
        provider_api_key_id=str(context.get("key_id") or "") or None,
        api_format=client_api_format,
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
        endpoint_api_format=str(context.get("provider_api_format") or "") or None,
        has_format_conversion=False,
        target_model=str(context.get("mapped_model") or "") or None,
        request_metadata=request_metadata,
    )


async def _record_gateway_passthrough_cli_sync_success(
    payload: GatewaySyncReportRequest,
    *,
    db: Session,
) -> None:
    from src.api.handlers.claude_cli import ClaudeCliAdapter
    from src.api.handlers.gemini_cli import GeminiCliAdapter
    from src.api.handlers.openai_cli import OpenAICliAdapter, OpenAICompactAdapter

    if payload.status_code >= 400:
        return
    if not isinstance(payload.body_json, dict) or payload.body_json.get("error") is not None:
        return

    context = dict(payload.report_context or {})
    user_id = str(context.get("user_id") or "").strip()
    api_key_id = str(context.get("api_key_id") or "").strip()
    client_api_format = str(context.get("client_api_format") or "openai:cli").strip().lower()
    if not user_id or not api_key_id:
        return

    user = db.query(User).filter(User.id == user_id).first()
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not user or not api_key:
        return

    if client_api_format == "openai:compact":
        adapter = OpenAICompactAdapter()
    elif client_api_format == "claude:cli":
        adapter = ClaudeCliAdapter()
    elif client_api_format == "gemini:cli":
        adapter = GeminiCliAdapter()
    else:
        adapter = OpenAICliAdapter()

    handler = adapter.HANDLER_CLASS(
        db=db,
        user=user,
        api_key=api_key,
        request_id=str(context.get("request_id") or payload.trace_id or uuid.uuid4().hex[:8]),
        client_ip="127.0.0.1",
        user_agent=str(
            (context.get("original_headers") or {}).get("user-agent") or "aether-gateway"
        ),
        start_time=time.time(),
        allowed_api_formats=adapter.allowed_api_formats,
        adapter_detector=adapter.detect_capability_requirements,
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
    )

    response_json = dict(payload.body_json)
    usage_info = handler.parser.extract_usage_from_response(response_json)
    response_metadata = handler._extract_response_metadata(response_json)
    request_metadata: dict[str, Any] = {
        "gateway_direct_executor": True,
        "phase": "3c_trial",
    }
    proxy_info = context.get("proxy_info")
    if isinstance(proxy_info, dict):
        request_metadata["proxy"] = proxy_info

    response_time_ms = 0
    if isinstance(payload.telemetry, dict):
        raw_elapsed_ms = payload.telemetry.get("elapsed_ms")
        try:
            response_time_ms = max(int(raw_elapsed_ms or 0), 0)
        except (TypeError, ValueError):
            response_time_ms = 0

    await handler.telemetry.record_success(
        provider=str(context.get("provider_name") or "openai"),
        model=str(context.get("model") or "unknown"),
        input_tokens=int(usage_info.get("input_tokens", 0) or 0),
        output_tokens=int(usage_info.get("output_tokens", 0) or 0),
        response_time_ms=response_time_ms,
        status_code=payload.status_code,
        request_headers=dict(context.get("original_headers") or {}),
        request_body=dict(context.get("original_request_body") or {}),
        response_headers=dict(payload.headers or {}),
        client_response_headers=dict(payload.headers or {}),
        response_body=response_json,
        provider_request_headers=dict(context.get("provider_request_headers") or {}),
        provider_request_body=context.get("provider_request_body"),
        cache_creation_tokens=int(usage_info.get("cache_creation_tokens", 0) or 0),
        cache_read_tokens=int(usage_info.get("cache_read_tokens", 0) or 0),
        is_stream=False,
        provider_id=str(context.get("provider_id") or "") or None,
        provider_endpoint_id=str(context.get("endpoint_id") or "") or None,
        provider_api_key_id=str(context.get("key_id") or "") or None,
        api_format=client_api_format,
        api_family=adapter.API_FAMILY.value if adapter.API_FAMILY else None,
        endpoint_kind=adapter.ENDPOINT_KIND.value if adapter.ENDPOINT_KIND else None,
        endpoint_api_format=str(context.get("provider_api_format") or "") or None,
        has_format_conversion=False,
        target_model=str(context.get("mapped_model") or "") or None,
        response_metadata=response_metadata if response_metadata else None,
        request_metadata=request_metadata,
    )


def _extract_gateway_report_body_bytes(payload: GatewaySyncReportRequest) -> bytes:
    if payload.body_base64:
        try:
            return base64.b64decode(payload.body_base64, validate=True)
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail="Invalid gateway report body payload"
            ) from exc
    if payload.body_json is not None:
        return JSONResponse(content=payload.body_json).body
    return b""

from __future__ import annotations

import json

import httpx
import pytest

from src.core.vertex_auth import VertexAuthError, VertexAuthService


class _TimeoutAsyncClient:
    def __init__(self, **_: object) -> None:
        pass

    async def __aenter__(self) -> "_TimeoutAsyncClient":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    async def post(self, *args: object, **kwargs: object) -> object:
        raise httpx.ReadTimeout("")


@pytest.mark.asyncio
async def test_vertex_auth_timeout_error_includes_readable_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = VertexAuthService(
        json.dumps(
            {
                "client_email": "svc@example.iam.gserviceaccount.com",
                "private_key": "not-used-in-test",
                "project_id": "demo-project",
            }
        )
    )

    monkeypatch.setattr(service, "_create_jwt", lambda: "signed-jwt")
    monkeypatch.setattr("src.core.vertex_auth.httpx.AsyncClient", _TimeoutAsyncClient)

    with pytest.raises(VertexAuthError, match=r"request timed out after 30s"):
        await service.get_access_token(httpx_client_kwargs={"timeout": 30})

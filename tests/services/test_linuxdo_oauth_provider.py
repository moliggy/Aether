from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from src.services.auth.oauth.providers.linuxdo import LinuxDoOAuthProvider


def _make_config() -> SimpleNamespace:
    return SimpleNamespace(
        client_id="client-id",
        redirect_uri="https://api.example.com/api/oauth/linuxdo/callback",
        authorization_url_override=None,
        token_url_override=None,
        userinfo_url_override=None,
        scopes=None,
        get_client_secret=lambda: "client-secret",
    )


def test_linuxdo_authorization_url_omits_empty_scope() -> None:
    provider = LinuxDoOAuthProvider()
    url = provider.get_authorization_url(_make_config(), "state-1")

    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    assert parsed.netloc == "connect.linux.do"
    assert "scope" not in params
    assert params["state"] == ["state-1"]


@pytest.mark.asyncio
async def test_linuxdo_exchange_code_uses_basic_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = LinuxDoOAuthProvider()
    config = _make_config()
    captured: dict[str, object] = {}

    async def _fake_post_form(
        url: str,
        data: dict[str, str],
        *,
        timeout_seconds: float = 5.0,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        captured["url"] = url
        captured["data"] = data
        captured["headers"] = headers or {}
        captured["timeout_seconds"] = timeout_seconds
        return httpx.Response(
            200,
            json={"access_token": "access-1", "token_type": "bearer"},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(provider, "_http_post_form", _fake_post_form)

    token = await provider.exchange_code(config, "code-1")

    assert token.access_token == "access-1"
    assert captured["url"] == provider.token_url
    assert captured["data"] == {
        "grant_type": "authorization_code",
        "code": "code-1",
        "redirect_uri": config.redirect_uri,
    }
    assert captured["headers"] == {
        "Authorization": provider._build_basic_auth_header("client-id", "client-secret"),
        "Accept": "application/json",
    }


@pytest.mark.asyncio
async def test_linuxdo_exchange_code_falls_back_to_backup_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = LinuxDoOAuthProvider()
    config = _make_config()
    called_urls: list[str] = []

    async def _fake_post_form(
        url: str,
        data: dict[str, str],
        *,
        timeout_seconds: float = 5.0,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        called_urls.append(url)
        if len(called_urls) == 1:
            raise httpx.ConnectError("network down", request=httpx.Request("POST", url))
        return httpx.Response(
            200,
            json={"access_token": "access-2", "token_type": "bearer"},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(provider, "_http_post_form", _fake_post_form)

    token = await provider.exchange_code(config, "code-2")

    assert token.access_token == "access-2"
    assert called_urls == [provider.token_url, provider.backup_token_url]


@pytest.mark.asyncio
async def test_linuxdo_userinfo_falls_back_to_backup_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = LinuxDoOAuthProvider()
    config = _make_config()
    called_urls: list[str] = []

    async def _fake_get(
        url: str,
        *,
        timeout_seconds: float = 5.0,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        called_urls.append(url)
        if len(called_urls) == 1:
            raise httpx.ConnectError("network down", request=httpx.Request("GET", url))
        return httpx.Response(
            200,
            json={"id": 42, "username": "neo", "email": "Neo@Linux.Do"},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(provider, "_http_get", _fake_get)

    user = await provider.get_user_info(config, "access-token")

    assert user.id == "42"
    assert user.username == "neo"
    assert user.email == "neo@linux.do"
    assert called_urls == [provider.userinfo_url, provider.backup_userinfo_url]

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs, urlparse

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.core.enums import AuthSource, UserRole
from src.models.database import Base, OAuthProvider, User, UserOAuthLink
from src.services.auth.oauth.service import OAuthService
from src.services.auth.oauth.state import OAuthStateData


@pytest.mark.asyncio
async def test_build_bind_authorize_url_includes_client_device_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = MagicMock()
    user = SimpleNamespace(id="user-1", auth_source=AuthSource.LOCAL)
    provider = SimpleNamespace(
        get_authorization_url=MagicMock(return_value="https://provider.example/authorize")
    )
    config = SimpleNamespace()
    create_state = AsyncMock(return_value="state-1")

    monkeypatch.setattr(
        "src.services.auth.oauth.service.OAuthService._require_module_active",
        lambda _db: None,
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.OAuthService._get_provider_impl",
        lambda _provider_type: provider,
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.OAuthService._get_enabled_provider_config",
        lambda _db, _provider_type: config,
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.get_redis_client",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.create_oauth_state",
        create_state,
    )

    url = await OAuthService.build_bind_authorize_url(
        db,
        user,
        "github",
        client_device_id="device-1",
    )

    assert url == "https://provider.example/authorize"
    assert create_state.await_args.kwargs["client_device_id"] == "device-1"


@pytest.mark.asyncio
async def test_handle_callback_allows_bind_state_without_device_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = MagicMock()
    provider = SimpleNamespace(
        exchange_code=AsyncMock(return_value=SimpleNamespace(access_token="provider-access")),
        get_user_info=AsyncMock(
            return_value=SimpleNamespace(
                id="oauth-user",
                username="tester",
                email="user@example.com",
                email_verified=True,
                raw={},
            )
        ),
    )
    config = SimpleNamespace(
        frontend_callback_url="https://app.example.com/auth/callback",
        display_name="GitHub",
        is_enabled=True,
    )

    monkeypatch.setattr(
        "src.services.auth.oauth.service.OAuthService._require_module_active",
        lambda _db: None,
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.OAuthService._get_provider_impl",
        lambda _provider_type: provider,
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.OAuthService._get_provider_config",
        lambda _db, _provider_type: config,
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.get_redis_client",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.consume_oauth_state",
        AsyncMock(
            return_value=OAuthStateData(
                nonce="state-1",
                provider_type="github",
                action="bind",
                user_id="user-1",
                client_device_id=None,
                created_at=123,
            )
        ),
    )
    monkeypatch.setattr(
        "src.services.auth.oauth.service.OAuthService._handle_bind",
        AsyncMock(return_value=SimpleNamespace()),
    )

    result = await OAuthService.handle_callback(
        db=db,
        provider_type="github",
        state="state-1",
        code="code-1",
        error=None,
        error_description=None,
        client_ip=None,
        user_agent="pytest-agent",
        headers={},
    )

    parsed = urlparse(result.redirect_url)
    assert result.refresh_token is None
    assert parse_qs(parsed.query)["oauth_bound"] == ["GitHub"]


def test_handle_login_sync_returns_user_snapshot_outside_db_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[User.__table__, OAuthProvider.__table__, UserOAuthLink.__table__],
    )
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as db:
        provider = OAuthProvider(
            provider_type="linuxdo",
            display_name="Linux Do",
            client_id="client-id",
            redirect_uri="https://api.example.com/api/oauth/linuxdo/callback",
            frontend_callback_url="https://app.example.com/auth/callback",
            is_enabled=True,
        )
        user = User(
            email="user@example.com",
            email_verified=True,
            username="tester",
            auth_source=AuthSource.OAUTH,
            role=UserRole.USER,
            is_active=True,
            is_deleted=False,
        )
        db.add(provider)
        db.add(user)
        db.flush()
        user_id = str(user.id)
        db.add(
            UserOAuthLink(
                user_id=user_id,
                provider_type="linuxdo",
                provider_user_id="linuxdo-user-1",
                provider_username="tester",
                provider_email="user@example.com",
                extra_data={},
            )
        )
        db.commit()

    @contextmanager
    def _fake_get_db_context():
        db = SessionLocal()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    monkeypatch.setattr("src.services.auth.oauth.service.get_db_context", _fake_get_db_context)

    snapshot = OAuthService._handle_login_sync(
        "linuxdo",
        SimpleNamespace(
            id="linuxdo-user-1",
            username="tester",
            email="user@example.com",
            email_verified=True,
            raw={},
        ),
    )

    assert snapshot.user_id == user_id
    assert snapshot.email == "user@example.com"
    assert snapshot.role == UserRole.USER

    engine.dispose()

use super::super::{
    admin_default_user_initial_gift, build_admin_users_read_only_response,
    legacy_admin_list_policy_mode, legacy_admin_rate_limit_policy_mode,
    normalize_admin_list_policy_mode, normalize_admin_optional_user_email,
    normalize_admin_rate_limit_policy_mode, normalize_admin_user_api_formats,
    normalize_admin_user_group_ids, normalize_admin_user_role, normalize_admin_user_string_list,
    normalize_admin_username, validate_admin_user_password, AdminCreateUserRequest,
};
use super::support::{admin_user_password_policy, build_admin_user_payload_with_groups};
use crate::handlers::admin::request::{AdminAppState, AdminRequestContext};
use crate::handlers::admin::shared::attach_admin_audit_response;
use crate::GatewayError;
use axum::{
    body::Body,
    http,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub(in super::super) async fn build_admin_create_user_response(
    state: &AdminAppState<'_>,
    _request_context: &AdminRequestContext<'_>,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_user_write_capability() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法创建用户",
        ));
    }
    if !state.has_auth_wallet_write_capability() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法初始化用户钱包",
        ));
    }
    let Some(request_body) = request_body else {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "请求数据验证失败" })),
        )
            .into_response());
    };
    let payload = match serde_json::from_slice::<AdminCreateUserRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "请求数据验证失败" })),
            )
                .into_response())
        }
    };

    let email = match normalize_admin_optional_user_email(payload.email.as_deref()) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let username = match normalize_admin_username(&payload.username) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let role = match normalize_admin_user_role(payload.role.as_deref()) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let password_policy = admin_user_password_policy(state).await?;
    if let Err(detail) = validate_admin_user_password(&payload.password, &password_policy) {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": detail })),
        )
            .into_response());
    }
    if payload.rate_limit.is_some_and(|value| value < 0) {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "rate_limit 必须大于等于 0" })),
        )
            .into_response());
    }
    if payload
        .initial_gift_usd
        .is_some_and(|value| !value.is_finite() || !(0.0..=10000.0).contains(&value))
    {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "初始赠款必须在 0-10000 范围内" })),
        )
            .into_response());
    }
    let allowed_providers =
        match normalize_admin_user_string_list(payload.allowed_providers, "allowed_providers") {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        };
    let allowed_api_formats = match normalize_admin_user_api_formats(payload.allowed_api_formats) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let allowed_models =
        match normalize_admin_user_string_list(payload.allowed_models, "allowed_models") {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        };
    let allowed_providers_mode = match payload.allowed_providers_mode.as_deref() {
        Some(value) => match normalize_admin_list_policy_mode(value) {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        },
        None => legacy_admin_list_policy_mode(&allowed_providers),
    };
    let allowed_api_formats_mode = match payload.allowed_api_formats_mode.as_deref() {
        Some(value) => match normalize_admin_list_policy_mode(value) {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        },
        None => legacy_admin_list_policy_mode(&allowed_api_formats),
    };
    let allowed_models_mode = match payload.allowed_models_mode.as_deref() {
        Some(value) => match normalize_admin_list_policy_mode(value) {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        },
        None => legacy_admin_list_policy_mode(&allowed_models),
    };
    let rate_limit_mode = match payload.rate_limit_mode.as_deref() {
        Some(value) => match normalize_admin_rate_limit_policy_mode(value) {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        },
        None => legacy_admin_rate_limit_policy_mode(payload.rate_limit),
    };
    let requested_group_ids = normalize_admin_user_group_ids(payload.group_ids);
    let group_ids = state
        .include_default_user_group_ids(&requested_group_ids)
        .await?;
    let groups = if group_ids.is_empty() {
        Vec::new()
    } else {
        let groups = state.list_user_groups_by_ids(&group_ids).await?;
        if groups.len() != group_ids.len() {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "用户分组不存在" })),
            )
                .into_response());
        }
        groups
    };

    if let Some(email) = email.as_deref() {
        if state.find_user_auth_by_identifier(email).await?.is_some() {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": format!("邮箱已存在: {email}") })),
            )
                .into_response());
        }
    }
    if state
        .find_user_auth_by_identifier(&username)
        .await?
        .is_some()
    {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": format!("用户名已存在: {username}") })),
        )
            .into_response());
    }

    let password_hash = match bcrypt::hash(&payload.password, bcrypt::DEFAULT_COST) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "密码长度不能超过72字节" })),
            )
                .into_response())
        }
    };
    let initial_gift_usd = if payload.unlimited {
        0.0
    } else if let Some(value) = payload.initial_gift_usd {
        value
    } else {
        admin_default_user_initial_gift(
            state
                .read_system_config_json_value("default_user_initial_gift_usd")
                .await?
                .as_ref(),
        )
    };

    let Some(user) = state
        .create_local_auth_user_with_settings(
            email,
            false,
            username,
            password_hash,
            role,
            allowed_providers,
            allowed_api_formats,
            allowed_models,
            payload.rate_limit,
        )
        .await?
    else {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法创建用户",
        ));
    };

    if state
        .initialize_auth_user_wallet(&user.id, initial_gift_usd, payload.unlimited)
        .await?
        .is_none()
    {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法初始化用户钱包",
        ));
    }
    let Some(user) = state
        .update_local_auth_user_policy_modes(
            &user.id,
            Some(allowed_providers_mode.clone()),
            Some(allowed_api_formats_mode.clone()),
            Some(allowed_models_mode.clone()),
            Some(rate_limit_mode.clone()),
        )
        .await?
    else {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法创建用户",
        ));
    };
    if !group_ids.is_empty() {
        state
            .replace_user_groups_for_user(&user.id, &group_ids)
            .await?;
    }

    Ok(attach_admin_audit_response(
        Json(build_admin_user_payload_with_groups(
            &user,
            payload.rate_limit,
            Some(rate_limit_mode.as_str()),
            payload.unlimited,
            &groups,
        ))
        .into_response(),
        "admin_user_created",
        "create_user",
        "user",
        &user.id,
    ))
}

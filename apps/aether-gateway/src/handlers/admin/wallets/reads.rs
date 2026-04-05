use super::admin_wallets_shared::{
    admin_wallet_id_from_detail_path, admin_wallet_id_from_suffix_path,
    build_admin_wallet_not_found_response, build_admin_wallet_refund_payload,
    build_admin_wallet_summary_payload, build_admin_wallets_bad_request_response,
    parse_admin_wallets_limit, parse_admin_wallets_offset, parse_admin_wallets_owner_type_filter,
    resolve_admin_wallet_owner_summary, wallet_owner_summary_from_fields,
    ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
};
use crate::control::GatewayPublicRequestContext;
use crate::handlers::{query_param_value, unix_secs_to_rfc3339};
use crate::{AppState, GatewayError};
use axum::{
    body::Body,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub(super) async fn build_admin_wallet_detail_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) = admin_wallet_id_from_detail_path(&request_context.request_path) else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };

    let Some(wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };

    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    let mut payload = build_admin_wallet_summary_payload(&wallet, &owner);
    if let Some(object) = payload.as_object_mut() {
        object.insert("pending_refund_count".to_string(), serde_json::Value::Null);
    }
    Ok(Json(payload).into_response())
}

pub(super) async fn build_admin_wallet_list_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_wallets_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let status = query_param_value(query, "status");
    let owner_type = parse_admin_wallets_owner_type_filter(query);

    let (wallets, total) = state
        .list_admin_wallets(status.as_deref(), owner_type.as_deref(), limit, offset)
        .await?;
    let items = wallets
        .into_iter()
        .map(|wallet| {
            let owner = wallet_owner_summary_from_fields(
                wallet.user_id.as_deref(),
                wallet.user_name.clone(),
                wallet.api_key_id.as_deref(),
                wallet.api_key_name.clone(),
            );
            json!({
                "id": wallet.id,
                "user_id": wallet.user_id,
                "api_key_id": wallet.api_key_id,
                "owner_type": owner.owner_type,
                "owner_name": owner.owner_name,
                "balance": wallet.balance + wallet.gift_balance,
                "recharge_balance": wallet.balance,
                "gift_balance": wallet.gift_balance,
                "refundable_balance": wallet.balance,
                "currency": wallet.currency,
                "status": wallet.status,
                "limit_mode": wallet.limit_mode.clone(),
                "unlimited": wallet.limit_mode.eq_ignore_ascii_case("unlimited"),
                "total_recharged": wallet.total_recharged,
                "total_consumed": wallet.total_consumed,
                "total_refunded": wallet.total_refunded,
                "total_adjusted": wallet.total_adjusted,
                "created_at": wallet.created_at_unix_secs.and_then(unix_secs_to_rfc3339),
                "updated_at": wallet.updated_at_unix_secs.and_then(unix_secs_to_rfc3339),
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

pub(super) async fn build_admin_wallet_ledger_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_wallets_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let category = query_param_value(query, "category");
    let reason_code = query_param_value(query, "reason_code");
    let owner_type = parse_admin_wallets_owner_type_filter(query);

    let (ledger, total) = state
        .list_admin_wallet_ledger(
            category.as_deref(),
            reason_code.as_deref(),
            owner_type.as_deref(),
            limit,
            offset,
        )
        .await?;
    let items = ledger
        .into_iter()
        .map(|entry| {
            let owner = wallet_owner_summary_from_fields(
                entry.wallet_user_id.as_deref(),
                entry.wallet_user_name.clone(),
                entry.wallet_api_key_id.as_deref(),
                entry.api_key_name.clone(),
            );
            json!({
                "id": entry.id,
                "wallet_id": entry.wallet_id,
                "owner_type": owner.owner_type,
                "owner_name": owner.owner_name,
                "wallet_status": entry.wallet_status,
                "category": entry.category,
                "reason_code": entry.reason_code,
                "amount": entry.amount,
                "balance_before": entry.balance_before,
                "balance_after": entry.balance_after,
                "recharge_balance_before": entry.recharge_balance_before,
                "recharge_balance_after": entry.recharge_balance_after,
                "gift_balance_before": entry.gift_balance_before,
                "gift_balance_after": entry.gift_balance_after,
                "link_type": entry.link_type,
                "link_id": entry.link_id,
                "operator_id": entry.operator_id,
                "operator_name": entry.operator_name,
                "operator_email": entry.operator_email,
                "description": entry.description,
                "created_at": entry.created_at_unix_secs.and_then(unix_secs_to_rfc3339),
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

pub(super) async fn build_admin_wallet_refund_requests_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_wallets_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let status = query_param_value(query, "status");
    let owner_type = parse_admin_wallets_owner_type_filter(query);
    if owner_type.as_deref() == Some("api_key") {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
        ));
    }

    let (refunds, total) = state
        .list_admin_wallet_refund_requests(status.as_deref(), limit, offset)
        .await?;
    let mut items = Vec::with_capacity(refunds.len());
    for refund in refunds {
        let mut owner = wallet_owner_summary_from_fields(
            refund.wallet_user_id.as_deref(),
            refund.wallet_user_name.clone(),
            refund.wallet_api_key_id.as_deref(),
            refund.api_key_name.clone(),
        );
        if owner.owner_name.is_none() {
            if let Some(wallet) = state
                .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
                    &refund.wallet_id,
                ))
                .await?
            {
                owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
            }
        }
        items.push(json!({
            "id": refund.id,
            "refund_no": refund.refund_no,
            "wallet_id": refund.wallet_id,
            "owner_type": owner.owner_type,
            "owner_name": owner.owner_name,
            "wallet_status": refund.wallet_status,
            "user_id": refund.user_id,
            "payment_order_id": refund.payment_order_id,
            "source_type": refund.source_type,
            "source_id": refund.source_id,
            "refund_mode": refund.refund_mode,
            "amount_usd": refund.amount_usd,
            "status": refund.status,
            "reason": refund.reason,
            "failure_reason": refund.failure_reason,
            "gateway_refund_id": refund.gateway_refund_id,
            "payout_method": refund.payout_method,
            "payout_reference": refund.payout_reference,
            "payout_proof": refund.payout_proof,
            "requested_by": refund.requested_by,
            "approved_by": refund.approved_by,
            "processed_by": refund.processed_by,
            "created_at": refund.created_at_unix_secs.and_then(unix_secs_to_rfc3339),
            "updated_at": refund.updated_at_unix_secs.and_then(unix_secs_to_rfc3339),
            "processed_at": refund.processed_at_unix_secs.and_then(unix_secs_to_rfc3339),
            "completed_at": refund.completed_at_unix_secs.and_then(unix_secs_to_rfc3339),
        }));
    }

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

pub(super) async fn build_admin_wallet_transactions_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) =
        admin_wallet_id_from_suffix_path(&request_context.request_path, "/transactions")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };
    let limit = match parse_admin_wallets_limit(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };

    let Some(wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    let wallet_payload = build_admin_wallet_summary_payload(&wallet, &owner);

    let (transactions, total) = state
        .list_admin_wallet_transactions(&wallet.id, limit, offset)
        .await?;
    let mut items = Vec::with_capacity(transactions.len());
    for transaction in transactions {
        let (operator_name, operator_email) =
            if transaction.operator_name.is_some() || transaction.operator_email.is_some() {
                (transaction.operator_name, transaction.operator_email)
            } else {
                match transaction.operator_id.as_deref() {
                    Some(operator_id) => state
                        .find_user_auth_by_id(operator_id)
                        .await?
                        .map(|user| (Some(user.username), user.email))
                        .unwrap_or((None, None)),
                    None => (None, None),
                }
            };
        items.push(json!({
            "id": transaction.id,
            "wallet_id": transaction.wallet_id,
            "owner_type": owner.owner_type,
            "owner_name": owner.owner_name.clone(),
            "wallet_status": wallet.status.clone(),
            "category": transaction.category,
            "reason_code": transaction.reason_code,
            "amount": transaction.amount,
            "balance_before": transaction.balance_before,
            "balance_after": transaction.balance_after,
            "recharge_balance_before": transaction.recharge_balance_before,
            "recharge_balance_after": transaction.recharge_balance_after,
            "gift_balance_before": transaction.gift_balance_before,
            "gift_balance_after": transaction.gift_balance_after,
            "link_type": transaction.link_type,
            "link_id": transaction.link_id,
            "operator_id": transaction.operator_id,
            "operator_name": operator_name,
            "operator_email": operator_email,
            "description": transaction.description,
            "created_at": transaction.created_at_unix_secs.and_then(unix_secs_to_rfc3339),
        }));
    }

    Ok(Json(json!({
        "wallet": wallet_payload,
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

pub(super) async fn build_admin_wallet_refunds_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) =
        admin_wallet_id_from_suffix_path(&request_context.request_path, "/refunds")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };
    let limit = match parse_admin_wallets_limit(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };

    let Some(wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    if wallet.api_key_id.is_some() {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
        ));
    }

    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    let wallet_payload = build_admin_wallet_summary_payload(&wallet, &owner);
    let (refunds, total) = state
        .list_admin_wallet_refunds(&wallet.id, limit, offset)
        .await?;
    let items = refunds
        .into_iter()
        .map(|refund| build_admin_wallet_refund_payload(&wallet, &owner, &refund))
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "wallet": wallet_payload,
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

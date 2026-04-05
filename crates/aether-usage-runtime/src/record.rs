use aether_data::repository::usage::UpsertUsageRecord;
use aether_data::DataLayerError;

use crate::{UsageEvent, UsageEventType};

pub fn build_upsert_usage_record_from_event(
    event: &UsageEvent,
) -> Result<UpsertUsageRecord, DataLayerError> {
    let (status, billing_status) = lifecycle_status_and_billing(event.event_type);
    let data = event.data.clone();
    let now_unix_secs = event.timestamp_ms / 1_000;

    Ok(UpsertUsageRecord {
        request_id: event.request_id.clone(),
        user_id: data.user_id,
        api_key_id: data.api_key_id,
        username: data.username,
        api_key_name: data.api_key_name,
        provider_name: data.provider_name,
        model: data.model,
        target_model: data.target_model,
        provider_id: empty_to_none(data.provider_id),
        provider_endpoint_id: empty_to_none(data.provider_endpoint_id),
        provider_api_key_id: empty_to_none(data.provider_api_key_id),
        request_type: data.request_type,
        api_format: data.api_format,
        api_family: data.api_family,
        endpoint_kind: data.endpoint_kind,
        endpoint_api_format: data.endpoint_api_format,
        provider_api_family: data.provider_api_family,
        provider_endpoint_kind: data.provider_endpoint_kind,
        has_format_conversion: data.has_format_conversion,
        is_stream: data.is_stream,
        input_tokens: data.input_tokens,
        output_tokens: data.output_tokens,
        total_tokens: data.total_tokens,
        cache_creation_input_tokens: data.cache_creation_input_tokens,
        cache_read_input_tokens: data.cache_read_input_tokens,
        cache_creation_cost_usd: data.cache_creation_cost_usd,
        cache_read_cost_usd: data.cache_read_cost_usd,
        output_price_per_1m: data.output_price_per_1m,
        total_cost_usd: data.total_cost_usd,
        actual_total_cost_usd: data.actual_total_cost_usd,
        status_code: data.status_code,
        error_message: data.error_message,
        error_category: data.error_category,
        response_time_ms: data.response_time_ms,
        first_byte_time_ms: data.first_byte_time_ms,
        status: status.to_string(),
        billing_status: billing_status.to_string(),
        request_headers: data.request_headers,
        request_body: data.request_body,
        provider_request_headers: data.provider_request_headers,
        provider_request_body: data.provider_request_body,
        response_headers: data.response_headers,
        response_body: data.response_body,
        client_response_headers: data.client_response_headers,
        client_response_body: data.client_response_body,
        request_metadata: data.request_metadata,
        finalized_at_unix_secs: Some(now_unix_secs),
        created_at_unix_secs: Some(now_unix_secs),
        updated_at_unix_secs: now_unix_secs,
    })
}

fn lifecycle_status_and_billing(event_type: UsageEventType) -> (&'static str, &'static str) {
    match event_type {
        UsageEventType::Pending => ("pending", "pending"),
        UsageEventType::Streaming => ("streaming", "pending"),
        UsageEventType::Completed => ("completed", "pending"),
        UsageEventType::Failed => ("failed", "void"),
        UsageEventType::Cancelled => ("cancelled", "void"),
    }
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use crate::{UsageEvent, UsageEventData, UsageEventType};

    use super::build_upsert_usage_record_from_event;

    #[test]
    fn builds_upsert_record_from_terminal_event() {
        let record = build_upsert_usage_record_from_event(&UsageEvent {
            event_type: UsageEventType::Completed,
            request_id: "req-1".to_string(),
            timestamp_ms: 1_700_000_000_000,
            data: UsageEventData {
                user_id: Some("user-1".to_string()),
                api_key_id: Some("key-1".to_string()),
                provider_name: "OpenAI".to_string(),
                model: "gpt-5".to_string(),
                api_format: Some("openai:chat".to_string()),
                endpoint_api_format: Some("openai:chat".to_string()),
                input_tokens: Some(10),
                output_tokens: Some(20),
                total_tokens: Some(30),
                status_code: Some(200),
                ..UsageEventData::default()
            },
        })
        .expect("record should build");

        assert_eq!(record.request_id, "req-1");
        assert_eq!(record.status, "completed");
        assert_eq!(record.billing_status, "pending");
        assert_eq!(record.total_tokens, Some(30));
        assert_eq!(record.finalized_at_unix_secs, Some(1_700_000_000));
    }
}

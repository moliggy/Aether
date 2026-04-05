use aether_data::repository::billing::StoredBillingModelContext;
use aether_data::DataLayerError;
use aether_usage_runtime::{UsageEvent, UsageEventType};
use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::{BillingModelPricingSnapshot, BillingService, BillingUsageInput};

#[async_trait]
pub trait BillingModelContextLookup: Send + Sync {
    async fn find_billing_model_context(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        global_model_name: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError>;
}

pub async fn enrich_usage_event_with_billing(
    data: &dyn BillingModelContextLookup,
    event: &mut UsageEvent,
) -> Result<(), DataLayerError> {
    if !matches!(event.event_type, UsageEventType::Completed) {
        event.data.total_cost_usd = Some(0.0);
        event.data.actual_total_cost_usd = Some(0.0);
        return Ok(());
    }

    let Some(provider_id) = event
        .data
        .provider_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let model_name = event.data.model.trim();
    if model_name.is_empty() {
        return Ok(());
    }

    let Some(context) = data
        .find_billing_model_context(
            provider_id,
            event.data.provider_api_key_id.as_deref(),
            model_name,
        )
        .await?
    else {
        return Ok(());
    };

    let pricing = map_pricing_context(context);
    let input = BillingUsageInput {
        task_type: event
            .data
            .request_type
            .clone()
            .unwrap_or_else(|| "chat".to_string()),
        api_format: event
            .data
            .endpoint_api_format
            .clone()
            .or_else(|| event.data.api_format.clone()),
        request_count: if event.data.status_code.unwrap_or_default() >= 400
            || event.data.error_message.is_some()
        {
            0
        } else {
            1
        },
        input_tokens: event.data.input_tokens.unwrap_or_default() as i64,
        output_tokens: event.data.output_tokens.unwrap_or_default() as i64,
        cache_creation_tokens: event.data.cache_creation_input_tokens.unwrap_or_default() as i64,
        cache_read_tokens: event.data.cache_read_input_tokens.unwrap_or_default() as i64,
        cache_ttl_minutes: pricing.provider_api_key_cache_ttl_minutes,
    };

    let computation = BillingService::new()
        .calculate(&pricing, &input)
        .map_err(|err| {
            DataLayerError::UnexpectedValue(format!("billing calculation failed: {err}"))
        })?;
    event.data.total_cost_usd = Some(computation.cost_result.cost);
    event.data.actual_total_cost_usd = Some(computation.actual_total_cost);
    merge_billing_snapshot_metadata(
        &mut event.data.request_metadata,
        &computation.cost_result.snapshot,
        computation.rate_multiplier,
        computation.is_free_tier,
    )?;
    Ok(())
}

fn map_pricing_context(context: StoredBillingModelContext) -> BillingModelPricingSnapshot {
    BillingModelPricingSnapshot {
        provider_id: context.provider_id,
        provider_billing_type: context.provider_billing_type,
        provider_api_key_id: context.provider_api_key_id,
        provider_api_key_rate_multipliers: context.provider_api_key_rate_multipliers,
        provider_api_key_cache_ttl_minutes: context.provider_api_key_cache_ttl_minutes,
        global_model_id: context.global_model_id,
        global_model_name: context.global_model_name,
        global_model_config: context.global_model_config,
        default_price_per_request: context.default_price_per_request,
        default_tiered_pricing: context.default_tiered_pricing,
        model_id: context.model_id,
        model_provider_model_name: context.model_provider_model_name,
        model_config: context.model_config,
        model_price_per_request: context.model_price_per_request,
        model_tiered_pricing: context.model_tiered_pricing,
    }
}

fn merge_billing_snapshot_metadata(
    request_metadata: &mut Option<Value>,
    snapshot: &crate::BillingSnapshot,
    rate_multiplier: f64,
    is_free_tier: bool,
) -> Result<(), DataLayerError> {
    let snapshot = serde_json::to_value(snapshot).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("failed to serialize billing snapshot: {err}"))
    })?;

    let mut metadata = match request_metadata.take() {
        Some(Value::Object(object)) => object,
        _ => Map::new(),
    };
    metadata.insert("billing_snapshot".to_string(), snapshot);
    metadata.insert("rate_multiplier".to_string(), Value::from(rate_multiplier));
    metadata.insert("is_free_tier".to_string(), Value::from(is_free_tier));
    *request_metadata = Some(Value::Object(metadata));
    Ok(())
}

#[cfg(test)]
mod tests {
    use aether_data::repository::billing::StoredBillingModelContext;
    use aether_usage_runtime::{UsageEvent, UsageEventData, UsageEventType};
    use async_trait::async_trait;
    use serde_json::json;
    use serde_json::Value;

    use super::{enrich_usage_event_with_billing, BillingModelContextLookup};

    struct TestLookup {
        context: Option<StoredBillingModelContext>,
    }

    #[async_trait]
    impl BillingModelContextLookup for TestLookup {
        async fn find_billing_model_context(
            &self,
            _provider_id: &str,
            _provider_api_key_id: Option<&str>,
            _global_model_name: &str,
        ) -> Result<Option<StoredBillingModelContext>, aether_data::DataLayerError> {
            Ok(self.context.clone())
        }
    }

    #[tokio::test]
    async fn enriches_completed_usage_event_with_billing_snapshot() {
        let lookup = TestLookup {
            context: Some(
                StoredBillingModelContext::new(
                    "provider-1".to_string(),
                    Some("pay_as_you_go".to_string()),
                    Some("key-1".to_string()),
                    Some(json!({"openai:chat": 0.5})),
                    Some(60),
                    "global-model-1".to_string(),
                    "gpt-5".to_string(),
                    None,
                    Some(0.02),
                    Some(json!({"tiers":[{"up_to":null,"input_price_per_1m":3.0,"output_price_per_1m":15.0,"cache_creation_price_per_1m":3.75,"cache_read_price_per_1m":0.30}]})),
                    Some("model-1".to_string()),
                    Some("gpt-5-upstream".to_string()),
                    None,
                    None,
                    None,
                )
                .expect("billing context should build"),
            ),
        };
        let mut event = UsageEvent::new(
            UsageEventType::Completed,
            "req-billing-1",
            UsageEventData {
                provider_name: "OpenAI".to_string(),
                model: "gpt-5".to_string(),
                provider_id: Some("provider-1".to_string()),
                provider_api_key_id: Some("key-1".to_string()),
                request_type: Some("chat".to_string()),
                api_format: Some("openai:chat".to_string()),
                endpoint_api_format: Some("openai:chat".to_string()),
                input_tokens: Some(1_000),
                output_tokens: Some(500),
                cache_read_input_tokens: Some(100),
                status_code: Some(200),
                ..UsageEventData::default()
            },
        );

        enrich_usage_event_with_billing(&lookup, &mut event)
            .await
            .expect("billing should succeed");

        assert!(event.data.total_cost_usd.unwrap_or_default() > 0.0);
        assert!(event.data.actual_total_cost_usd.unwrap_or_default() > 0.0);
        assert_eq!(
            event
                .data
                .request_metadata
                .as_ref()
                .and_then(|value| value.get("billing_snapshot"))
                .and_then(|value| value.get("status"))
                .and_then(Value::as_str),
            Some("complete")
        );
    }
}

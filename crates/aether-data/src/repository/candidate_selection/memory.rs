use std::sync::RwLock;

use async_trait::async_trait;

use super::{
    MinimalCandidateSelectionReadRepository, StoredMinimalCandidateSelectionRow,
    StoredPoolKeyCandidateRowsQuery, StoredRequestedModelCandidateRowsQuery,
};
use crate::DataLayerError;

#[derive(Debug, Default)]
pub struct InMemoryMinimalCandidateSelectionReadRepository {
    rows: RwLock<Vec<StoredMinimalCandidateSelectionRow>>,
}

impl InMemoryMinimalCandidateSelectionReadRepository {
    pub fn seed<I>(rows: I) -> Self
    where
        I: IntoIterator<Item = StoredMinimalCandidateSelectionRow>,
    {
        Self {
            rows: RwLock::new(rows.into_iter().collect()),
        }
    }
}

#[async_trait]
impl MinimalCandidateSelectionReadRepository for InMemoryMinimalCandidateSelectionReadRepository {
    async fn list_for_exact_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let api_format = api_format.trim();
        let mut rows = self
            .rows
            .read()
            .expect("candidate selection repository lock")
            .iter()
            .filter(|row| {
                row.provider_is_active
                    && row.endpoint_is_active
                    && row.key_is_active
                    && row.model_is_active
                    && row.model_is_available
                    && api_format_matches(&row.endpoint_api_format, api_format)
                    && row.key_supports_api_format(api_format)
                    && key_auth_channel_matches(row, api_format)
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.provider_priority
                .cmp(&right.provider_priority)
                .then(left.key_internal_priority.cmp(&right.key_internal_priority))
                .then(left.provider_id.cmp(&right.provider_id))
                .then(left.endpoint_id.cmp(&right.endpoint_id))
                .then(left.key_id.cmp(&right.key_id))
                .then(left.model_id.cmp(&right.model_id))
        });
        Ok(rows)
    }

    async fn list_for_exact_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let rows = self.list_for_exact_api_format(api_format).await?;
        Ok(rows
            .into_iter()
            .filter(|row| row.global_model_name == global_model_name)
            .collect())
    }

    async fn list_for_exact_api_format_and_requested_model(
        &self,
        api_format: &str,
        requested_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        self.list_for_exact_api_format_and_requested_model_page(
            &StoredRequestedModelCandidateRowsQuery {
                api_format: api_format.to_string(),
                requested_model_name: requested_model_name.to_string(),
                offset: 0,
                limit: u32::MAX,
            },
        )
        .await
    }

    async fn list_for_exact_api_format_and_requested_model_page(
        &self,
        query: &StoredRequestedModelCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let rows = self.list_for_exact_api_format(&query.api_format).await?;
        let mut rows = rows
            .into_iter()
            .filter(|row| {
                row_matches_requested_model(row, &query.requested_model_name, &query.api_format)
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.global_model_name
                .cmp(&right.global_model_name)
                .then(left.provider_priority.cmp(&right.provider_priority))
                .then(left.key_internal_priority.cmp(&right.key_internal_priority))
                .then(left.provider_id.cmp(&right.provider_id))
                .then(left.endpoint_id.cmp(&right.endpoint_id))
                .then(left.key_id.cmp(&right.key_id))
                .then(left.model_id.cmp(&right.model_id))
        });
        Ok(rows
            .into_iter()
            .skip(query.offset as usize)
            .take(query.limit as usize)
            .collect())
    }

    async fn list_pool_key_rows_for_group(
        &self,
        query: &StoredPoolKeyCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let mut rows = self
            .list_for_exact_api_format(&query.api_format)
            .await?
            .into_iter()
            .filter(|row| {
                row.provider_id == query.provider_id
                    && row.endpoint_id == query.endpoint_id
                    && row.model_id == query.model_id
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.key_internal_priority
                .cmp(&right.key_internal_priority)
                .then(left.key_id.cmp(&right.key_id))
        });
        Ok(rows
            .into_iter()
            .skip(query.offset as usize)
            .take(query.limit as usize)
            .collect())
    }
}

fn normalize_api_format(value: &str) -> String {
    aether_ai_formats::normalize_api_format_alias(value)
}

fn api_format_matches(left: &str, right: &str) -> bool {
    aether_ai_formats::api_format_alias_matches(left, right)
}

fn row_matches_requested_model(
    row: &StoredMinimalCandidateSelectionRow,
    requested_model_name: &str,
    api_format: &str,
) -> bool {
    row.global_model_name == requested_model_name
        || row.model_provider_model_name == requested_model_name
        || row
            .model_provider_model_mappings
            .as_ref()
            .is_some_and(|mappings| {
                mappings.iter().any(|mapping| {
                    mapping.api_formats.as_ref().is_none_or(|formats| {
                        formats
                            .iter()
                            .any(|value| api_format_matches(value, api_format))
                    }) && mapping.name == requested_model_name
                })
            })
}

fn key_auth_channel_matches(row: &StoredMinimalCandidateSelectionRow, api_format: &str) -> bool {
    let provider_type = row.provider_type.trim().to_ascii_lowercase();
    let auth_type = row.key_auth_type.trim().to_ascii_lowercase();
    let api_format = normalize_api_format(api_format);
    match provider_type.as_str() {
        "codex" => {
            auth_type == "oauth"
                && matches!(
                    api_format.as_str(),
                    "openai:responses" | "openai:responses:compact" | "openai:image"
                )
        }
        "claude_code" => auth_type == "oauth" && api_format == "claude:messages",
        "kiro" => {
            matches!(auth_type.as_str(), "oauth" | "bearer") && api_format == "claude:messages"
        }
        "gemini_cli" | "antigravity" => {
            auth_type == "oauth" && api_format == "gemini:generate_content"
        }
        "vertex_ai" => {
            (auth_type == "api_key" && api_format == "gemini:generate_content")
                || (matches!(auth_type.as_str(), "service_account" | "vertex_ai")
                    && matches!(
                        api_format.as_str(),
                        "claude:messages" | "gemini:generate_content"
                    ))
        }
        _ => auth_type != "oauth",
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryMinimalCandidateSelectionReadRepository;
    use crate::repository::candidate_selection::{
        MinimalCandidateSelectionReadRepository, StoredMinimalCandidateSelectionRow,
        StoredPoolKeyCandidateRowsQuery, StoredRequestedModelCandidateRowsQuery,
    };

    fn sample_row(
        provider_id: &str,
        api_format: &str,
        global_model_name: &str,
        provider_priority: i32,
    ) -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: provider_id.to_string(),
            provider_name: provider_id.to_string(),
            provider_type: "custom".to_string(),
            provider_priority,
            provider_is_active: true,
            endpoint_id: format!("endpoint-{provider_id}"),
            endpoint_api_format: api_format.to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: format!("key-{provider_id}"),
            key_name: "prod".to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec![api_format.to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 50,
            key_global_priority_by_format: None,
            model_id: format!("model-{provider_id}"),
            global_model_id: "global-model-1".to_string(),
            global_model_name: global_model_name.to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: global_model_name.to_string(),
            model_provider_model_mappings: None,
            model_supports_streaming: None,
            model_is_active: true,
            model_is_available: true,
        }
    }

    #[tokio::test]
    async fn filters_by_exact_api_format_and_global_model() {
        let repository = InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_row("provider-2", "openai:chat", "gpt-4.1", 20),
            sample_row("provider-1", "openai:chat", "gpt-4.1", 10),
            sample_row("provider-3", "openai:responses", "gpt-4.1", 5),
            sample_row("provider-4", "openai:chat", "gpt-4.1-mini", 1),
        ]);

        let rows = repository
            .list_for_exact_api_format_and_global_model("openai:chat", "gpt-4.1")
            .await
            .expect("list should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].provider_id, "provider-1");
        assert_eq!(rows[1].provider_id, "provider-2");
    }

    #[tokio::test]
    async fn filters_by_exact_api_format_and_requested_model_aliases() {
        let mut mapped = sample_row("provider-1", "openai:chat", "gpt-4.1", 10);
        mapped.model_provider_model_name = "provider-gpt-4.1".to_string();
        mapped.model_provider_model_mappings = Some(vec![
            crate::repository::candidate_selection::StoredProviderModelMapping {
                name: "alias-gpt-4.1".to_string(),
                priority: 0,
                api_formats: Some(vec!["openai:chat".to_string()]),
            },
        ]);
        let repository = InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            mapped,
            sample_row("provider-2", "openai:chat", "gpt-4.1-mini", 20),
        ]);

        let rows = repository
            .list_for_exact_api_format_and_requested_model("openai:chat", "alias-gpt-4.1")
            .await
            .expect("list should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].provider_id, "provider-1");
    }

    #[tokio::test]
    async fn requested_model_page_returns_requested_slice_only() {
        let mut rows = Vec::new();
        for index in 0..5 {
            let mut row = sample_row(&format!("provider-{index}"), "openai:chat", "gpt-5", index);
            row.key_internal_priority = index;
            rows.push(row);
        }
        let repository = InMemoryMinimalCandidateSelectionReadRepository::seed(rows);

        let page = repository
            .list_for_exact_api_format_and_requested_model_page(
                &StoredRequestedModelCandidateRowsQuery {
                    api_format: "openai:chat".to_string(),
                    requested_model_name: "gpt-5".to_string(),
                    offset: 2,
                    limit: 2,
                },
            )
            .await
            .expect("page should load");

        assert_eq!(
            page.iter()
                .map(|row| row.provider_id.as_str())
                .collect::<Vec<_>>(),
            vec!["provider-2", "provider-3"]
        );
    }

    #[tokio::test]
    async fn filters_by_exact_api_format_only() {
        let repository = InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_row("provider-2", "openai:chat", "gpt-4.1", 20),
            sample_row("provider-1", "openai:chat", "gpt-4.1-mini", 10),
            sample_row("provider-3", "openai:responses", "gpt-4.1", 5),
        ]);

        let rows = repository
            .list_for_exact_api_format("openai:chat")
            .await
            .expect("list should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].provider_id, "provider-1");
        assert_eq!(rows[1].provider_id, "provider-2");
    }

    #[tokio::test]
    async fn list_pool_key_rows_for_group_returns_requested_page_only() {
        let mut rows = Vec::new();
        for index in 0..5 {
            let mut row = sample_row("provider-pool", "openai:chat", "gpt-5", 10);
            row.endpoint_id = "endpoint-pool".to_string();
            row.model_id = "model-pool".to_string();
            row.key_id = format!("key-{index}");
            row.key_internal_priority = index;
            rows.push(row);
        }
        let repository = InMemoryMinimalCandidateSelectionReadRepository::seed(rows);

        let page = repository
            .list_pool_key_rows_for_group(&StoredPoolKeyCandidateRowsQuery {
                api_format: "openai:chat".to_string(),
                provider_id: "provider-pool".to_string(),
                endpoint_id: "endpoint-pool".to_string(),
                model_id: "model-pool".to_string(),
                selected_provider_model_name: "gpt-5".to_string(),
                offset: 2,
                limit: 2,
            })
            .await
            .expect("pool key page should load");

        assert_eq!(
            page.iter()
                .map(|row| row.key_id.as_str())
                .collect::<Vec<_>>(),
            vec!["key-2", "key-3"]
        );
    }
}

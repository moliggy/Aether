use crate::provider_transport::url::build_passthrough_path_url;

use super::credentials::DEFAULT_REGION;

pub(crate) const GENERATE_ASSISTANT_RESPONSE_PATH: &str = "/generateAssistantResponse";
pub(crate) const KIRO_ENVELOPE_NAME: &str = "kiro:generateAssistantResponse";

pub(crate) fn resolve_kiro_base_url(upstream_base_url: &str, api_region: Option<&str>) -> String {
    let region = api_region
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_REGION);
    upstream_base_url
        .trim()
        .replace("{region}", region)
        .trim_end_matches('/')
        .to_string()
}

pub(crate) fn build_kiro_generate_assistant_response_url(
    upstream_base_url: &str,
    query: Option<&str>,
    api_region: Option<&str>,
) -> Option<String> {
    let upstream_base_url = resolve_kiro_base_url(upstream_base_url, api_region);
    build_passthrough_path_url(
        upstream_base_url.as_str(),
        GENERATE_ASSISTANT_RESPONSE_PATH,
        query,
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::{
        build_kiro_generate_assistant_response_url, resolve_kiro_base_url,
        GENERATE_ASSISTANT_RESPONSE_PATH, KIRO_ENVELOPE_NAME,
    };

    #[test]
    fn exposes_kiro_request_constants() {
        assert_eq!(
            GENERATE_ASSISTANT_RESPONSE_PATH,
            "/generateAssistantResponse"
        );
        assert_eq!(KIRO_ENVELOPE_NAME, "kiro:generateAssistantResponse");
    }

    #[test]
    fn builds_generate_assistant_response_url() {
        assert_eq!(
            build_kiro_generate_assistant_response_url(
                "https://kiro.{region}.example?tenant=demo",
                Some("stream=true"),
                Some("us-west-2")
            )
            .as_deref(),
            Some(
                "https://kiro.us-west-2.example/generateAssistantResponse?stream=true&tenant=demo"
            )
        );
    }

    #[test]
    fn resolves_region_placeholder_in_base_url() {
        assert_eq!(
            resolve_kiro_base_url("https://kiro.{region}.example/", Some("us-west-2")),
            "https://kiro.us-west-2.example"
        );
    }
}

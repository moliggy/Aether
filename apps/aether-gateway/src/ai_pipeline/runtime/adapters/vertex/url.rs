use std::collections::BTreeMap;

use url::form_urlencoded;

use crate::provider_transport::url::build_passthrough_path_url;

pub(crate) const VERTEX_API_KEY_BASE_URL: &str = "https://aiplatform.googleapis.com";

pub(crate) fn build_vertex_api_key_gemini_content_url(
    model: &str,
    stream: bool,
    api_key: &str,
    request_query: Option<&str>,
) -> Option<String> {
    build_vertex_api_key_google_model_url(model, stream, api_key, request_query)
}

pub(crate) fn build_vertex_api_key_imagen_content_url(
    model: &str,
    stream: bool,
    api_key: &str,
    request_query: Option<&str>,
) -> Option<String> {
    build_vertex_api_key_google_model_url(model, stream, api_key, request_query)
}

fn build_vertex_api_key_google_model_url(
    model: &str,
    stream: bool,
    api_key: &str,
    request_query: Option<&str>,
) -> Option<String> {
    let trimmed_model = model.trim();
    let trimmed_api_key = api_key.trim();
    if trimmed_model.is_empty() || trimmed_api_key.is_empty() {
        return None;
    }

    let action = if stream {
        "streamGenerateContent"
    } else {
        "generateContent"
    };
    let path = format!("/v1/publishers/google/models/{trimmed_model}:{action}");
    let merged_query = build_vertex_api_key_query(trimmed_api_key, request_query, stream);
    build_passthrough_path_url(VERTEX_API_KEY_BASE_URL, &path, merged_query.as_deref(), &[])
}

fn build_vertex_api_key_query(
    api_key: &str,
    request_query: Option<&str>,
    stream: bool,
) -> Option<String> {
    let mut merged = BTreeMap::new();
    merge_query_string(&mut merged, request_query);
    merged.remove("beta");
    merged.insert("key".to_string(), api_key.to_string());
    if stream {
        merged
            .entry("alt".to_string())
            .or_insert_with(|| "sse".to_string());
    }

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in merged {
        serializer.append_pair(&key, &value);
    }
    let query = serializer.finish();
    if query.is_empty() {
        None
    } else {
        Some(query)
    }
}

fn merge_query_string(out: &mut BTreeMap<String, String>, query: Option<&str>) {
    let Some(query) = query.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };

    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        out.insert(key.into_owned(), value.into_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::{build_vertex_api_key_gemini_content_url, build_vertex_api_key_imagen_content_url};

    #[test]
    fn builds_vertex_gemini_api_key_stream_url() {
        assert_eq!(
            build_vertex_api_key_gemini_content_url(
                "gemini-2.5-pro",
                true,
                "vertex-secret",
                Some("foo=bar&beta=v1")
            )
            .as_deref(),
            Some(
                "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-pro:streamGenerateContent?alt=sse&foo=bar&key=vertex-secret"
            )
        );
    }

    #[test]
    fn builds_vertex_imagen_api_key_sync_url() {
        assert_eq!(
            build_vertex_api_key_imagen_content_url(
                "imagen-3.0-generate-001",
                false,
                "vertex-secret",
                Some("view=full")
            )
            .as_deref(),
            Some(
                "https://aiplatform.googleapis.com/v1/publishers/google/models/imagen-3.0-generate-001:generateContent?key=vertex-secret&view=full"
            )
        );
    }
}

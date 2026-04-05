#[derive(Debug, Clone, Copy)]
pub struct ProviderOAuthTemplate {
    pub provider_type: &'static str,
    pub display_name: &'static str,
    pub authorize_url: &'static str,
    pub token_url: &'static str,
    pub client_id: &'static str,
    pub client_secret: &'static str,
    pub scopes: &'static [&'static str],
    pub redirect_uri: &'static str,
    pub use_pkce: bool,
}

pub fn provider_type_is_fixed(provider_type: &str) -> bool {
    matches!(
        provider_type.trim().to_ascii_lowercase().as_str(),
        "claude_code" | "kiro" | "codex" | "gemini_cli" | "antigravity" | "vertex_ai"
    )
}

pub fn provider_type_enables_format_conversion_by_default(provider_type: &str) -> bool {
    matches!(
        provider_type.trim().to_ascii_lowercase().as_str(),
        "claude_code" | "kiro" | "codex" | "antigravity" | "vertex_ai"
    )
}

pub fn fixed_provider_template(
    provider_type: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    match provider_type.trim().to_ascii_lowercase().as_str() {
        "claude_code" => Some(("https://api.anthropic.com", &["claude:cli"])),
        "codex" => Some((
            "https://chatgpt.com/backend-api/codex",
            &["openai:cli", "openai:compact"],
        )),
        "kiro" => Some(("https://q.{region}.amazonaws.com", &["claude:cli"])),
        "gemini_cli" => Some(("https://cloudcode-pa.googleapis.com", &["gemini:cli"])),
        "vertex_ai" => Some((
            "https://aiplatform.googleapis.com",
            &["gemini:chat", "claude:chat"],
        )),
        "antigravity" => Some(("https://cloudcode-pa.googleapis.com", &["gemini:chat"])),
        _ => None,
    }
}

pub fn provider_type_supports_model_fetch(provider_type: &str) -> bool {
    !matches!(
        provider_type.trim().to_ascii_lowercase().as_str(),
        "vertex_ai" | "antigravity" | "codex" | "kiro" | "claude_code"
    )
}

pub fn provider_type_supports_local_openai_chat_transport(provider_type: &str) -> bool {
    !matches!(
        provider_type.trim().to_ascii_lowercase().as_str(),
        "antigravity" | "claude_code" | "codex" | "gemini_cli" | "kiro" | "vertex_ai"
    )
}

pub fn provider_type_supports_local_same_format_transport(provider_type: &str) -> bool {
    !matches!(
        provider_type.trim().to_ascii_lowercase().as_str(),
        "antigravity" | "claude_code" | "kiro" | "vertex_ai"
    )
}

pub fn is_codex_cli_backend_url(url: &str) -> bool {
    let url = url.trim().to_ascii_lowercase();
    url.contains("/codex") && (url.contains("/backend-api/") || url.contains("/backendapi/"))
}

pub fn provider_type_is_fixed_for_admin_oauth(provider_type: &str) -> bool {
    provider_type_is_fixed(provider_type)
}

pub fn provider_type_admin_oauth_template(provider_type: &str) -> Option<ProviderOAuthTemplate> {
    match provider_type.trim().to_ascii_lowercase().as_str() {
        "claude_code" => Some(ProviderOAuthTemplate {
            provider_type: "claude_code",
            display_name: "ClaudeCode",
            authorize_url: "https://claude.ai/oauth/authorize",
            token_url: "https://console.anthropic.com/v1/oauth/token",
            client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
            client_secret: "",
            scopes: &["org:create_api_key", "user:profile", "user:inference"],
            redirect_uri: "http://localhost:54545/callback",
            use_pkce: true,
        }),
        "codex" => Some(ProviderOAuthTemplate {
            provider_type: "codex",
            display_name: "Codex",
            authorize_url: "https://auth.openai.com/oauth/authorize",
            token_url: "https://auth.openai.com/oauth/token",
            client_id: "app_EMoamEEZ73f0CkXaXp7hrann",
            client_secret: "",
            scopes: &["openid", "email", "profile", "offline_access"],
            redirect_uri: "http://localhost:1455/auth/callback",
            use_pkce: true,
        }),
        "gemini_cli" => Some(ProviderOAuthTemplate {
            provider_type: "gemini_cli",
            display_name: "GeminiCli",
            authorize_url: "https://accounts.google.com/o/oauth2/v2/auth",
            token_url: "https://oauth2.googleapis.com/token",
            client_id: "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com",
            client_secret: "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",
            scopes: &[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/userinfo.profile",
            ],
            redirect_uri: "http://localhost:8085/oauth2callback",
            use_pkce: false,
        }),
        "antigravity" => Some(ProviderOAuthTemplate {
            provider_type: "antigravity",
            display_name: "Antigravity",
            authorize_url: "https://accounts.google.com/o/oauth2/v2/auth",
            token_url: "https://oauth2.googleapis.com/token",
            client_id: "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com",
            client_secret: "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",
            scopes: &[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/userinfo.profile",
                "https://www.googleapis.com/auth/cclog",
                "https://www.googleapis.com/auth/experimentsandconfigs",
            ],
            redirect_uri: "http://localhost:51121/oauth2callback",
            use_pkce: true,
        }),
        _ => None,
    }
}

pub const ADMIN_PROVIDER_OAUTH_TEMPLATE_TYPES: &[&str] =
    &["claude_code", "codex", "gemini_cli", "antigravity"];

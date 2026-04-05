#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontdoorCorsConfig {
    allowed_origins: Vec<String>,
    allow_credentials: bool,
}

impl FrontdoorCorsConfig {
    pub fn new(allowed_origins: Vec<String>, allow_credentials: bool) -> Option<Self> {
        let allowed_origins = allowed_origins
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        if allowed_origins.is_empty() {
            return None;
        }
        let allow_any_origin = allowed_origins.iter().any(|value| value == "*");
        Some(Self {
            allowed_origins,
            allow_credentials: allow_credentials && !allow_any_origin,
        })
    }

    pub fn from_environment(
        environment: &str,
        cors_origins: Option<&str>,
        allow_credentials: bool,
    ) -> Option<Self> {
        let configured = cors_origins
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if !configured.is_empty() {
            return Self::new(configured, allow_credentials);
        }
        if environment.eq_ignore_ascii_case("development") {
            return Self::new(
                vec![
                    "http://localhost:3000".to_string(),
                    "http://localhost:5173".to_string(),
                    "http://127.0.0.1:3000".to_string(),
                    "http://127.0.0.1:5173".to_string(),
                ],
                allow_credentials,
            );
        }
        None
    }

    pub(crate) fn allows_origin(&self, origin: &str) -> bool {
        self.allowed_origins
            .iter()
            .any(|value| value == "*" || value == origin)
    }

    pub(crate) fn allow_any_origin(&self) -> bool {
        self.allowed_origins.iter().any(|value| value == "*")
    }

    pub(crate) fn allow_credentials(&self) -> bool {
        self.allow_credentials
    }

    pub(crate) fn allowed_origins(&self) -> &[String] {
        &self.allowed_origins
    }
}

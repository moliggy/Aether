#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulerAuthConstraints {
    pub allowed_providers: Option<Vec<String>>,
    pub allowed_api_formats: Option<Vec<String>>,
    pub allowed_models: Option<Vec<String>>,
}

pub fn auth_constraints_allow_provider(
    constraints: Option<&SchedulerAuthConstraints>,
    provider_id: &str,
    provider_name: &str,
) -> bool {
    let Some(allowed) =
        constraints.and_then(|constraints| constraints.allowed_providers.as_deref())
    else {
        return true;
    };

    allowed.iter().any(|value| {
        value.trim().eq_ignore_ascii_case(provider_id.trim())
            || value.trim().eq_ignore_ascii_case(provider_name.trim())
    })
}

pub fn auth_constraints_allow_api_format(
    constraints: Option<&SchedulerAuthConstraints>,
    api_format: &str,
) -> bool {
    let Some(allowed) =
        constraints.and_then(|constraints| constraints.allowed_api_formats.as_deref())
    else {
        return true;
    };

    allowed
        .iter()
        .any(|value| crate::normalize_api_format(value) == api_format)
}

pub fn auth_constraints_allow_model(
    constraints: Option<&SchedulerAuthConstraints>,
    requested_model_name: &str,
    resolved_global_model_name: &str,
) -> bool {
    let Some(allowed) = constraints.and_then(|constraints| constraints.allowed_models.as_deref())
    else {
        return true;
    };

    allowed
        .iter()
        .any(|value| value == requested_model_name || value == resolved_global_model_name)
}

#[cfg(test)]
mod tests {
    use super::{
        auth_constraints_allow_api_format, auth_constraints_allow_model,
        auth_constraints_allow_provider, SchedulerAuthConstraints,
    };

    fn sample_constraints() -> SchedulerAuthConstraints {
        SchedulerAuthConstraints {
            allowed_providers: Some(vec!["provider-1".to_string(), "OpenAI".to_string()]),
            allowed_api_formats: Some(vec!["OPENAI:CHAT".to_string()]),
            allowed_models: Some(vec!["gpt-5".to_string()]),
        }
    }

    #[test]
    fn constraints_allow_matching_provider_identifier_or_name() {
        let constraints = sample_constraints();
        assert!(auth_constraints_allow_provider(
            Some(&constraints),
            "provider-1",
            "other"
        ));
        assert!(auth_constraints_allow_provider(
            Some(&constraints),
            "other",
            "openai"
        ));
        assert!(!auth_constraints_allow_provider(
            Some(&constraints),
            "other",
            "other"
        ));
    }

    #[test]
    fn constraints_normalize_api_formats_and_models() {
        let constraints = sample_constraints();
        assert!(auth_constraints_allow_api_format(
            Some(&constraints),
            "openai:chat"
        ));
        assert!(auth_constraints_allow_model(
            Some(&constraints),
            "gpt-5",
            "gpt-5"
        ));
        assert!(!auth_constraints_allow_model(
            Some(&constraints),
            "gpt-4.1",
            "gpt-4.1"
        ));
    }
}

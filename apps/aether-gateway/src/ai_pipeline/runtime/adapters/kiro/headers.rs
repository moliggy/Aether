use std::collections::BTreeMap;

use uuid::Uuid;

use super::credentials::KiroAuthConfig;

pub(crate) const AWS_EVENTSTREAM_CONTENT_TYPE: &str = "application/vnd.amazon.eventstream";
const AWS_SDK_JS_MAIN_VERSION: &str = "1.0.27";
const CODEWHISPERER_OPTOUT: &str = "true";
const KIRO_AGENT_MODE: &str = "vibe";

fn build_kiro_ide_tag(kiro_version: &str, machine_id: &str) -> String {
    if machine_id.trim().is_empty() {
        format!("KiroIDE-{kiro_version}")
    } else {
        format!("KiroIDE-{kiro_version}-{machine_id}")
    }
}

fn build_x_amz_user_agent_main(kiro_version: &str, machine_id: &str) -> String {
    format!(
        "aws-sdk-js/{AWS_SDK_JS_MAIN_VERSION} {}",
        build_kiro_ide_tag(kiro_version, machine_id)
    )
}

fn build_user_agent_main(
    system_version: &str,
    node_version: &str,
    kiro_version: &str,
    machine_id: &str,
) -> String {
    format!(
        "aws-sdk-js/{AWS_SDK_JS_MAIN_VERSION} ua/2.1 os/{system_version} lang/js md/nodejs#{node_version} api/codewhispererstreaming#{AWS_SDK_JS_MAIN_VERSION} m/E {}",
        build_kiro_ide_tag(kiro_version, machine_id)
    )
}

pub(crate) fn build_generate_assistant_headers(
    auth_config: &KiroAuthConfig,
    machine_id: &str,
) -> BTreeMap<String, String> {
    let kiro_version = auth_config.effective_kiro_version();
    let system_version = auth_config.effective_system_version();
    let node_version = auth_config.effective_node_version();
    let region = auth_config.effective_api_region();
    let host = format!("q.{region}.amazonaws.com");

    BTreeMap::from([
        (
            "accept".to_string(),
            AWS_EVENTSTREAM_CONTENT_TYPE.to_string(),
        ),
        (
            "amz-sdk-invocation-id".to_string(),
            Uuid::new_v4().to_string(),
        ),
        (
            "amz-sdk-request".to_string(),
            "attempt=1; max=3".to_string(),
        ),
        ("connection".to_string(), "close".to_string()),
        ("content-type".to_string(), "application/json".to_string()),
        ("host".to_string(), host),
        (
            "user-agent".to_string(),
            build_user_agent_main(system_version, node_version, kiro_version, machine_id),
        ),
        (
            "x-amz-user-agent".to_string(),
            build_x_amz_user_agent_main(kiro_version, machine_id),
        ),
        (
            "x-amzn-codewhisperer-optout".to_string(),
            CODEWHISPERER_OPTOUT.to_string(),
        ),
        (
            "x-amzn-kiro-agent-mode".to_string(),
            KIRO_AGENT_MODE.to_string(),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::super::credentials::KiroAuthConfig;
    use super::{build_generate_assistant_headers, AWS_EVENTSTREAM_CONTENT_TYPE};

    #[test]
    fn builds_generate_assistant_headers_for_region() {
        let auth_config = KiroAuthConfig {
            auth_method: None,
            refresh_token: None,
            expires_at: None,
            profile_arn: None,
            region: None,
            auth_region: None,
            api_region: Some("us-west-2".to_string()),
            client_id: None,
            client_secret: None,
            machine_id: None,
            kiro_version: Some("1.2.3".to_string()),
            system_version: Some("darwin#24.6.0".to_string()),
            node_version: Some("22.21.1".to_string()),
            access_token: None,
        };

        let headers = build_generate_assistant_headers(&auth_config, "machine-123");
        assert_eq!(
            headers.get("accept").map(String::as_str),
            Some(AWS_EVENTSTREAM_CONTENT_TYPE)
        );
        assert_eq!(
            headers.get("host").map(String::as_str),
            Some("q.us-west-2.amazonaws.com")
        );
        assert_eq!(
            headers.get("x-amzn-kiro-agent-mode").map(String::as_str),
            Some("vibe")
        );
    }
}

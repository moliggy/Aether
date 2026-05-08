use super::{
    AuthApiKeyLookupKey, CreateManagementTokenRecord, DataLayerError, GatewayAuthApiKeySnapshot,
    GatewayDataState, ManagementTokenListQuery, ProxyNodeHeartbeatMutation,
    ProxyNodeManualCreateMutation, ProxyNodeManualUpdateMutation, ProxyNodeRegistrationMutation,
    ProxyNodeRemoteConfigMutation, ProxyNodeTrafficMutation, ProxyNodeTunnelStatusMutation,
    RegenerateManagementTokenSecret, StoredAuthApiKeyExportRecord, StoredAuthApiKeySnapshot,
    StoredLdapModuleConfig, StoredManagementToken, StoredManagementTokenListPage,
    StoredManagementTokenWithUser, StoredOAuthProviderConfig, StoredOAuthProviderModuleConfig,
    StoredProxyFleetMetricsBucket, StoredProxyNode, StoredProxyNodeEvent,
    StoredProxyNodeMetricsBucket, StoredUserAuthRecord, StoredUserOAuthLinkSummary,
    StoredUserPreferenceRecord, StoredUserSessionRecord, StoredWalletSnapshot,
    UpdateManagementTokenRecord, UpsertOAuthProviderConfigRecord,
};
use crate::LocalMutationOutcome;
use aether_data::repository::auth::{
    read_resolved_auth_api_key_snapshot_by_key_hash,
    read_resolved_auth_api_key_snapshot_by_user_api_key_ids,
};

impl GatewayDataState {
    pub(crate) async fn is_other_user_auth_email_taken(
        &self,
        email: &str,
        user_id: &str,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        Ok(repository
            .find_user_auth_by_email(email)
            .await?
            .is_some_and(|user| user.id != user_id))
    }

    pub(crate) async fn is_other_user_auth_username_taken(
        &self,
        username: &str,
        user_id: &str,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        Ok(repository
            .find_user_auth_by_username(username)
            .await?
            .is_some_and(|user| user.id != user_id))
    }

    pub(crate) async fn find_active_user_auth_by_email_ci(
        &self,
        email: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository.find_active_user_auth_by_email_ci(email).await
    }

    pub(crate) async fn find_user_auth_by_username(
        &self,
        username: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository.find_user_auth_by_username(username).await
    }

    pub(crate) async fn find_user_auth_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        match &self.user_reader {
            Some(repository) => repository.find_user_auth_by_id(user_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn find_user_auth_by_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        match &self.user_reader {
            Some(repository) => repository.find_user_auth_by_identifier(identifier).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn list_user_oauth_links(
        &self,
        user_id: &str,
    ) -> Result<Vec<StoredUserOAuthLinkSummary>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(Vec::new());
        };
        repository.list_user_oauth_links(user_id).await
    }

    pub(crate) async fn find_oauth_linked_user(
        &self,
        provider_type: &str,
        provider_user_id: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .find_oauth_linked_user(provider_type, provider_user_id)
            .await
    }

    pub(crate) async fn touch_oauth_link(
        &self,
        provider_type: &str,
        provider_user_id: &str,
        provider_username: Option<&str>,
        provider_email: Option<&str>,
        extra_data: Option<serde_json::Value>,
        touched_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .touch_oauth_link(
                provider_type,
                provider_user_id,
                provider_username,
                provider_email,
                extra_data,
                touched_at,
            )
            .await
    }

    pub(crate) async fn create_oauth_auth_user(
        &self,
        email: Option<String>,
        username: String,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .create_oauth_auth_user(email, username, created_at)
            .await
    }

    pub(crate) async fn find_oauth_link_owner(
        &self,
        provider_type: &str,
        provider_user_id: &str,
    ) -> Result<Option<String>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .find_oauth_link_owner(provider_type, provider_user_id)
            .await
    }

    pub(crate) async fn has_user_oauth_provider_link(
        &self,
        user_id: &str,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .has_user_oauth_provider_link(user_id, provider_type)
            .await
    }

    pub(crate) async fn count_user_oauth_links(
        &self,
        user_id: &str,
    ) -> Result<u64, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(0);
        };
        repository.count_user_oauth_links(user_id).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn upsert_user_oauth_link(
        &self,
        user_id: &str,
        provider_type: &str,
        provider_user_id: &str,
        provider_username: Option<&str>,
        provider_email: Option<&str>,
        extra_data: Option<serde_json::Value>,
        linked_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(());
        };
        repository
            .upsert_user_oauth_link(
                user_id,
                provider_type,
                provider_user_id,
                provider_username,
                provider_email,
                extra_data,
                linked_at,
            )
            .await
    }

    pub(crate) async fn delete_user_oauth_link(
        &self,
        user_id: &str,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .delete_user_oauth_link(user_id, provider_type)
            .await
    }

    pub(crate) async fn read_user_preferences(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserPreferenceRecord>, DataLayerError> {
        if let Some(store) = &self.user_preferences {
            return Ok(store
                .read()
                .expect("user preference store should lock")
                .get(user_id)
                .cloned());
        }

        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository.read_user_preferences(user_id).await
    }

    pub(crate) async fn write_user_preferences(
        &self,
        preferences: &StoredUserPreferenceRecord,
    ) -> Result<Option<StoredUserPreferenceRecord>, DataLayerError> {
        if let Some(store) = &self.user_preferences {
            store
                .write()
                .expect("user preference store should lock")
                .insert(preferences.user_id.clone(), preferences.clone());
            return Ok(Some(preferences.clone()));
        }

        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository.write_user_preferences(preferences).await
    }

    pub(crate) async fn find_active_provider_name(
        &self,
        provider_id: &str,
    ) -> Result<Option<String>, DataLayerError> {
        let providers = self.list_provider_catalog_providers(true).await?;
        Ok(providers
            .into_iter()
            .find(|provider| provider.id == provider_id)
            .map(|provider| provider.name))
    }

    pub(crate) async fn find_user_session(
        &self,
        user_id: &str,
        session_id: &str,
    ) -> Result<Option<StoredUserSessionRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository.find_user_session(user_id, session_id).await
    }

    pub(crate) async fn list_user_sessions(
        &self,
        user_id: &str,
    ) -> Result<Vec<StoredUserSessionRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(Vec::new());
        };
        repository.list_user_sessions(user_id).await
    }

    pub(crate) async fn create_user_session(
        &self,
        session: &StoredUserSessionRecord,
    ) -> Result<Option<StoredUserSessionRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository.create_user_session(session).await
    }

    pub(crate) async fn update_user_model_capability_settings(
        &self,
        user_id: &str,
        settings: Option<serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_user_model_capability_settings(user_id, settings)
            .await
    }

    pub(crate) async fn update_local_auth_user_profile(
        &self,
        user_id: &str,
        email: Option<String>,
        username: Option<String>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_local_auth_user_profile(user_id, email, username)
            .await
    }

    pub(crate) async fn update_local_auth_user_password_hash(
        &self,
        user_id: &str,
        password_hash: String,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_local_auth_user_password_hash(user_id, password_hash, updated_at)
            .await
    }

    #[allow(dead_code)]
    pub(crate) async fn create_local_auth_user(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .create_local_auth_user(email, email_verified, username, password_hash)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_local_auth_user_with_settings(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
        role: String,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models: Option<Vec<String>>,
        rate_limit: Option<i32>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .create_local_auth_user_with_settings(
                email,
                email_verified,
                username,
                password_hash,
                role,
                allowed_providers,
                allowed_api_formats,
                allowed_models,
                rate_limit,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_local_auth_user_admin_fields(
        &self,
        user_id: &str,
        role: Option<String>,
        allowed_providers_present: bool,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats_present: bool,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models_present: bool,
        allowed_models: Option<Vec<String>>,
        rate_limit_present: bool,
        rate_limit: Option<i32>,
        is_active: Option<bool>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_local_auth_user_admin_fields(
                user_id,
                role,
                allowed_providers_present,
                allowed_providers,
                allowed_api_formats_present,
                allowed_api_formats,
                allowed_models_present,
                allowed_models,
                rate_limit_present,
                rate_limit,
                is_active,
            )
            .await
    }

    pub(crate) async fn touch_auth_user_last_login(
        &self,
        user_id: &str,
        logged_in_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .touch_auth_user_last_login(user_id, logged_in_at)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn get_or_create_ldap_auth_user(
        &self,
        email: String,
        username: String,
        ldap_dn: Option<String>,
        ldap_username: Option<String>,
        logged_in_at: chrono::DateTime<chrono::Utc>,
        initial_gift_usd: f64,
        unlimited: bool,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(None);
        };
        let Some(outcome) = repository
            .get_or_create_ldap_auth_user(email, username, ldap_dn, ldap_username, logged_in_at)
            .await?
        else {
            return Ok(None);
        };
        if outcome.created {
            match self
                .initialize_auth_user_wallet(&outcome.user.id, initial_gift_usd, unlimited)
                .await
            {
                Ok(Some(_wallet)) => {}
                Ok(None) => {
                    let _ = self.delete_local_auth_user(&outcome.user.id).await;
                    return Ok(None);
                }
                Err(err) => {
                    let _ = self.delete_local_auth_user(&outcome.user.id).await;
                    return Err(err);
                }
            }
        }
        Ok(Some(outcome.user))
    }

    #[allow(dead_code)]
    pub(crate) async fn initialize_auth_user_wallet(
        &self,
        user_id: &str,
        initial_gift_usd: f64,
        unlimited: bool,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .initialize_auth_user_wallet(user_id, initial_gift_usd, unlimited)
            .await
    }

    pub(crate) async fn initialize_auth_api_key_wallet(
        &self,
        api_key_id: &str,
        initial_gift_usd: f64,
        unlimited: bool,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .initialize_auth_api_key_wallet(api_key_id, initial_gift_usd, unlimited)
            .await
    }

    pub(crate) async fn update_auth_user_wallet_limit_mode(
        &self,
        user_id: &str,
        limit_mode: &str,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_auth_user_wallet_limit_mode(user_id, limit_mode)
            .await
    }

    pub(crate) async fn update_auth_api_key_wallet_limit_mode(
        &self,
        api_key_id: &str,
        limit_mode: &str,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_auth_api_key_wallet_limit_mode(api_key_id, limit_mode)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_auth_user_wallet_snapshot(
        &self,
        user_id: &str,
        balance: f64,
        gift_balance: f64,
        limit_mode: &str,
        currency: &str,
        status: &str,
        total_recharged: f64,
        total_consumed: f64,
        total_refunded: f64,
        total_adjusted: f64,
        updated_at_unix_secs: Option<u64>,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_auth_user_wallet_snapshot(
                user_id,
                balance,
                gift_balance,
                limit_mode,
                currency,
                status,
                total_recharged,
                total_consumed,
                total_refunded,
                total_adjusted,
                updated_at_unix_secs,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_auth_api_key_wallet_snapshot(
        &self,
        api_key_id: &str,
        balance: f64,
        gift_balance: f64,
        limit_mode: &str,
        currency: &str,
        status: &str,
        total_recharged: f64,
        total_consumed: f64,
        total_refunded: f64,
        total_adjusted: f64,
        updated_at_unix_secs: Option<u64>,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(None);
        };
        repository
            .update_auth_api_key_wallet_snapshot(
                api_key_id,
                balance,
                gift_balance,
                limit_mode,
                currency,
                status,
                total_recharged,
                total_consumed,
                total_refunded,
                total_adjusted,
                updated_at_unix_secs,
            )
            .await
    }

    pub(crate) async fn count_active_admin_users(&self) -> Result<u64, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(0);
        };
        repository.count_active_admin_users().await
    }

    pub(crate) async fn count_active_local_admin_users_with_valid_password(
        &self,
    ) -> Result<u64, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(0);
        };
        repository
            .count_active_local_admin_users_with_valid_password()
            .await
    }

    pub(crate) async fn count_user_pending_refunds(
        &self,
        user_id: &str,
    ) -> Result<u64, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(0);
        };
        repository.count_pending_refunds_by_user_id(user_id).await
    }

    pub(crate) async fn count_user_pending_payment_orders(
        &self,
        user_id: &str,
    ) -> Result<u64, DataLayerError> {
        let Some(repository) = self.wallet_reader.as_ref() else {
            return Ok(0);
        };
        repository
            .count_pending_payment_orders_by_user_id(user_id)
            .await
    }

    pub(crate) async fn delete_local_auth_user(
        &self,
        user_id: &str,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository.delete_local_auth_user(user_id).await
    }

    pub(crate) async fn register_local_auth_user(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
        initial_gift_usd: f64,
        unlimited: bool,
    ) -> Result<Option<(StoredUserAuthRecord, StoredWalletSnapshot)>, DataLayerError> {
        let Some(user) = self
            .create_local_auth_user(email, email_verified, username, password_hash)
            .await?
        else {
            return Ok(None);
        };

        match self
            .initialize_auth_user_wallet(&user.id, initial_gift_usd, unlimited)
            .await
        {
            Ok(Some(wallet)) => Ok(Some((user, wallet))),
            Ok(None) => {
                let _ = self.delete_local_auth_user(&user.id).await;
                Ok(None)
            }
            Err(err) => {
                let _ = self.delete_local_auth_user(&user.id).await;
                Err(err)
            }
        }
    }

    pub(crate) async fn touch_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        touched_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .touch_user_session(user_id, session_id, touched_at, ip_address, user_agent)
            .await
    }

    pub(crate) async fn update_user_session_device_label(
        &self,
        user_id: &str,
        session_id: &str,
        device_label: &str,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .update_user_session_device_label(user_id, session_id, device_label, updated_at)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn rotate_user_session_refresh_token(
        &self,
        user_id: &str,
        session_id: &str,
        previous_refresh_token_hash: &str,
        next_refresh_token_hash: &str,
        rotated_at: chrono::DateTime<chrono::Utc>,
        expires_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .rotate_user_session_refresh_token(
                user_id,
                session_id,
                previous_refresh_token_hash,
                next_refresh_token_hash,
                rotated_at,
                expires_at,
                ip_address,
                user_agent,
            )
            .await
    }

    pub(crate) async fn revoke_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<bool, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(false);
        };
        repository
            .revoke_user_session(user_id, session_id, revoked_at, reason)
            .await
    }

    pub(crate) async fn revoke_all_user_sessions(
        &self,
        user_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<u64, DataLayerError> {
        let Some(repository) = self.user_reader.as_ref() else {
            return Ok(0);
        };
        repository
            .revoke_all_user_sessions(user_id, revoked_at, reason)
            .await
    }

    pub(crate) async fn list_enabled_oauth_module_providers(
        &self,
    ) -> Result<Vec<StoredOAuthProviderModuleConfig>, DataLayerError> {
        match &self.auth_module_reader {
            Some(repository) => repository.list_enabled_oauth_providers().await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn get_ldap_module_config(
        &self,
    ) -> Result<Option<StoredLdapModuleConfig>, DataLayerError> {
        match &self.auth_module_reader {
            Some(repository) => repository.get_ldap_config().await,
            None => Ok(None),
        }
    }

    pub(crate) async fn upsert_ldap_module_config(
        &self,
        config: &StoredLdapModuleConfig,
    ) -> Result<Option<StoredLdapModuleConfig>, DataLayerError> {
        match &self.auth_module_writer {
            Some(repository) => repository.upsert_ldap_config(config).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn list_oauth_provider_configs(
        &self,
    ) -> Result<Vec<StoredOAuthProviderConfig>, DataLayerError> {
        match &self.oauth_provider_reader {
            Some(repository) => repository.list_oauth_provider_configs().await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn get_oauth_provider_config(
        &self,
        provider_type: &str,
    ) -> Result<Option<StoredOAuthProviderConfig>, DataLayerError> {
        match &self.oauth_provider_reader {
            Some(repository) => repository.get_oauth_provider_config(provider_type).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn count_locked_users_if_oauth_provider_disabled(
        &self,
        provider_type: &str,
        ldap_exclusive: bool,
    ) -> Result<usize, DataLayerError> {
        match &self.oauth_provider_reader {
            Some(repository) => {
                repository
                    .count_locked_users_if_provider_disabled(provider_type, ldap_exclusive)
                    .await
            }
            None => Ok(0),
        }
    }

    pub(crate) async fn upsert_oauth_provider_config(
        &self,
        record: &UpsertOAuthProviderConfigRecord,
    ) -> Result<Option<StoredOAuthProviderConfig>, DataLayerError> {
        match &self.oauth_provider_writer {
            Some(repository) => repository
                .upsert_oauth_provider_config(record)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn delete_oauth_provider_config(
        &self,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        match &self.oauth_provider_writer {
            Some(repository) => repository.delete_oauth_provider_config(provider_type).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn list_management_tokens(
        &self,
        query: &ManagementTokenListQuery,
    ) -> Result<StoredManagementTokenListPage, DataLayerError> {
        match &self.management_token_reader {
            Some(repository) => repository.list_management_tokens(query).await,
            None => Ok(StoredManagementTokenListPage {
                items: Vec::new(),
                total: 0,
            }),
        }
    }

    pub(crate) async fn get_management_token_with_user(
        &self,
        token_id: &str,
    ) -> Result<Option<StoredManagementTokenWithUser>, DataLayerError> {
        match &self.management_token_reader {
            Some(repository) => repository.get_management_token_with_user(token_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn get_management_token_with_user_by_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<StoredManagementTokenWithUser>, DataLayerError> {
        match &self.management_token_reader {
            Some(repository) => {
                repository
                    .get_management_token_with_user_by_hash(token_hash)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn create_management_token(
        &self,
        record: &CreateManagementTokenRecord,
    ) -> Result<LocalMutationOutcome<StoredManagementToken>, DataLayerError> {
        match &self.management_token_writer {
            Some(repository) => match repository.create_management_token(record).await {
                Ok(token) => Ok(LocalMutationOutcome::Applied(token)),
                Err(DataLayerError::InvalidInput(detail)) => {
                    Ok(LocalMutationOutcome::Invalid(detail))
                }
                Err(err) => Err(err),
            },
            None => Ok(LocalMutationOutcome::Unavailable),
        }
    }

    pub(crate) async fn update_management_token(
        &self,
        record: &UpdateManagementTokenRecord,
    ) -> Result<LocalMutationOutcome<StoredManagementToken>, DataLayerError> {
        match &self.management_token_writer {
            Some(repository) => match repository.update_management_token(record).await {
                Ok(Some(token)) => Ok(LocalMutationOutcome::Applied(token)),
                Ok(None) => Ok(LocalMutationOutcome::NotFound),
                Err(DataLayerError::InvalidInput(detail)) => {
                    Ok(LocalMutationOutcome::Invalid(detail))
                }
                Err(err) => Err(err),
            },
            None => Ok(LocalMutationOutcome::Unavailable),
        }
    }

    pub(crate) async fn delete_management_token(
        &self,
        token_id: &str,
    ) -> Result<bool, DataLayerError> {
        match &self.management_token_writer {
            Some(repository) => repository.delete_management_token(token_id).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn record_management_token_usage(
        &self,
        token_id: &str,
        last_used_ip: Option<&str>,
    ) -> Result<Option<StoredManagementToken>, DataLayerError> {
        match &self.management_token_writer {
            Some(repository) => {
                repository
                    .record_management_token_usage(token_id, last_used_ip)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_reader {
            Some(repository) => repository.find_proxy_node(node_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_reader {
            Some(repository) => repository.list_proxy_nodes().await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_proxy_node_events(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredProxyNodeEvent>, DataLayerError> {
        match &self.proxy_node_reader {
            Some(repository) => repository.list_proxy_node_events(node_id, limit).await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_proxy_node_events_filtered(
        &self,
        node_id: &str,
        query: &super::ProxyNodeEventQuery,
    ) -> Result<Vec<StoredProxyNodeEvent>, DataLayerError> {
        match &self.proxy_node_reader {
            Some(repository) => {
                repository
                    .list_proxy_node_events_filtered(node_id, query)
                    .await
            }
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_proxy_node_metrics(
        &self,
        node_id: &str,
        step: super::ProxyNodeMetricsStep,
        from_unix_secs: u64,
        to_unix_secs: u64,
        limit: usize,
    ) -> Result<Vec<StoredProxyNodeMetricsBucket>, DataLayerError> {
        match &self.proxy_node_reader {
            Some(repository) => {
                repository
                    .list_proxy_node_metrics(node_id, step, from_unix_secs, to_unix_secs, limit)
                    .await
            }
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_proxy_fleet_metrics(
        &self,
        step: super::ProxyNodeMetricsStep,
        from_unix_secs: u64,
        to_unix_secs: u64,
        limit: usize,
    ) -> Result<Vec<StoredProxyFleetMetricsBucket>, DataLayerError> {
        match &self.proxy_node_reader {
            Some(repository) => {
                repository
                    .list_proxy_fleet_metrics(step, from_unix_secs, to_unix_secs, limit)
                    .await
            }
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn register_proxy_node(
        &self,
        mutation: &ProxyNodeRegistrationMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.register_node(mutation).await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn create_manual_proxy_node(
        &self,
        mutation: &ProxyNodeManualCreateMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.create_manual_node(mutation).await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn update_manual_proxy_node(
        &self,
        mutation: &ProxyNodeManualUpdateMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.update_manual_node(mutation).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn reset_stale_proxy_node_tunnel_statuses(
        &self,
    ) -> Result<usize, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.reset_stale_tunnel_statuses().await,
            None => Ok(0),
        }
    }

    pub(crate) async fn cleanup_proxy_node_metrics(
        &self,
        retain_1m_from_unix_secs: u64,
        retain_1h_from_unix_secs: u64,
        delete_limit: usize,
    ) -> Result<super::ProxyNodeMetricsCleanupSummary, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => {
                repository
                    .cleanup_proxy_node_metrics(
                        retain_1m_from_unix_secs,
                        retain_1h_from_unix_secs,
                        delete_limit,
                    )
                    .await
            }
            None => Ok(super::ProxyNodeMetricsCleanupSummary::default()),
        }
    }

    pub(crate) async fn apply_proxy_node_heartbeat(
        &self,
        mutation: &ProxyNodeHeartbeatMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.apply_heartbeat(mutation).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn record_proxy_node_traffic(
        &self,
        mutation: &ProxyNodeTrafficMutation,
    ) -> Result<bool, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.record_traffic(mutation).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn update_proxy_node_tunnel_status(
        &self,
        mutation: &ProxyNodeTunnelStatusMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.update_tunnel_status(mutation).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn unregister_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.unregister_node(node_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn delete_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.delete_node(node_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn update_proxy_node_remote_config(
        &self,
        mutation: &ProxyNodeRemoteConfigMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        match &self.proxy_node_writer {
            Some(repository) => repository.update_remote_config(mutation).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn set_management_token_active(
        &self,
        token_id: &str,
        is_active: bool,
    ) -> Result<Option<StoredManagementToken>, DataLayerError> {
        match &self.management_token_writer {
            Some(repository) => {
                repository
                    .set_management_token_active(token_id, is_active)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn regenerate_management_token_secret(
        &self,
        mutation: &RegenerateManagementTokenSecret,
    ) -> Result<LocalMutationOutcome<StoredManagementToken>, DataLayerError> {
        match &self.management_token_writer {
            Some(repository) => match repository
                .regenerate_management_token_secret(mutation)
                .await
            {
                Ok(Some(token)) => Ok(LocalMutationOutcome::Applied(token)),
                Ok(None) => Ok(LocalMutationOutcome::NotFound),
                Err(DataLayerError::InvalidInput(detail)) => {
                    Ok(LocalMutationOutcome::Invalid(detail))
                }
                Err(err) => Err(err),
            },
            None => Ok(LocalMutationOutcome::Unavailable),
        }
    }

    pub(in crate::data) async fn find_auth_api_key_snapshot(
        &self,
        key: AuthApiKeyLookupKey<'_>,
    ) -> Result<Option<StoredAuthApiKeySnapshot>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => repository.find_api_key_snapshot(key).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn list_auth_api_key_snapshots_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeySnapshot>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => repository.list_api_key_snapshots_by_ids(api_key_ids).await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_auth_api_key_export_records_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => repository.list_export_api_keys_by_user_ids(user_ids).await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_auth_api_key_export_records_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => repository.list_export_api_keys_by_ids(api_key_ids).await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_auth_api_key_export_records_by_name_search(
        &self,
        name_search: &str,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => {
                repository
                    .list_export_api_keys_by_name_search(name_search)
                    .await
            }
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn list_auth_api_key_export_standalone_records_page(
        &self,
        query: &aether_data::repository::auth::StandaloneApiKeyExportListQuery,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => repository.list_export_standalone_api_keys_page(query).await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn count_auth_api_key_export_standalone_records(
        &self,
        is_active: Option<bool>,
    ) -> Result<u64, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => repository.count_export_standalone_api_keys(is_active).await,
            None => Ok(0),
        }
    }

    pub(crate) async fn summarize_auth_api_key_export_records_by_user_ids(
        &self,
        user_ids: &[String],
        now_unix_secs: u64,
    ) -> Result<aether_data::repository::auth::AuthApiKeyExportSummary, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => {
                repository
                    .summarize_export_api_keys_by_user_ids(user_ids, now_unix_secs)
                    .await
            }
            None => Ok(aether_data::repository::auth::AuthApiKeyExportSummary::default()),
        }
    }

    pub(crate) async fn summarize_auth_api_key_export_non_standalone_records(
        &self,
        now_unix_secs: u64,
    ) -> Result<aether_data::repository::auth::AuthApiKeyExportSummary, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => {
                repository
                    .summarize_export_non_standalone_api_keys(now_unix_secs)
                    .await
            }
            None => Ok(aether_data::repository::auth::AuthApiKeyExportSummary::default()),
        }
    }

    pub(crate) async fn list_auth_api_key_export_standalone_records(
        &self,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => repository.list_export_standalone_api_keys().await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn summarize_auth_api_key_export_standalone_records(
        &self,
        now_unix_secs: u64,
    ) -> Result<aether_data::repository::auth::AuthApiKeyExportSummary, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => {
                repository
                    .summarize_export_standalone_api_keys(now_unix_secs)
                    .await
            }
            None => Ok(aether_data::repository::auth::AuthApiKeyExportSummary::default()),
        }
    }

    pub(crate) async fn find_auth_api_key_export_standalone_record_by_id(
        &self,
        api_key_id: &str,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_reader {
            Some(repository) => {
                repository
                    .find_export_standalone_api_key_by_id(api_key_id)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn create_user_api_key(
        &self,
        record: aether_data::repository::auth::CreateUserApiKeyRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => repository.create_user_api_key(record).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn create_standalone_api_key(
        &self,
        record: aether_data::repository::auth::CreateStandaloneApiKeyRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => repository.create_standalone_api_key(record).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn update_user_api_key_basic(
        &self,
        record: aether_data::repository::auth::UpdateUserApiKeyBasicRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => repository.update_user_api_key_basic(record).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn update_standalone_api_key_basic(
        &self,
        record: aether_data::repository::auth::UpdateStandaloneApiKeyBasicRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => repository.update_standalone_api_key_basic(record).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn set_user_api_key_active(
        &self,
        user_id: &str,
        api_key_id: &str,
        is_active: bool,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => {
                repository
                    .set_user_api_key_active(user_id, api_key_id, is_active)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn set_standalone_api_key_active(
        &self,
        api_key_id: &str,
        is_active: bool,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => {
                repository
                    .set_standalone_api_key_active(api_key_id, is_active)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn set_user_api_key_locked(
        &self,
        user_id: &str,
        api_key_id: &str,
        is_locked: bool,
    ) -> Result<bool, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => {
                repository
                    .set_user_api_key_locked(user_id, api_key_id, is_locked)
                    .await
            }
            None => Ok(false),
        }
    }

    pub(crate) async fn set_user_api_key_allowed_providers(
        &self,
        user_id: &str,
        api_key_id: &str,
        allowed_providers: Option<Vec<String>>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => {
                repository
                    .set_user_api_key_allowed_providers(user_id, api_key_id, allowed_providers)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn set_user_api_key_force_capabilities(
        &self,
        user_id: &str,
        api_key_id: &str,
        force_capabilities: Option<serde_json::Value>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => {
                repository
                    .set_user_api_key_force_capabilities(user_id, api_key_id, force_capabilities)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn delete_user_api_key(
        &self,
        user_id: &str,
        api_key_id: &str,
    ) -> Result<bool, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => repository.delete_user_api_key(user_id, api_key_id).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn delete_standalone_api_key(
        &self,
        api_key_id: &str,
    ) -> Result<bool, DataLayerError> {
        match &self.auth_api_key_writer {
            Some(repository) => repository.delete_standalone_api_key(api_key_id).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn read_auth_api_key_snapshot(
        &self,
        user_id: &str,
        api_key_id: &str,
        now_unix_secs: u64,
    ) -> Result<Option<GatewayAuthApiKeySnapshot>, DataLayerError> {
        read_resolved_auth_api_key_snapshot_by_user_api_key_ids(
            self,
            user_id,
            api_key_id,
            now_unix_secs,
        )
        .await
    }

    pub(crate) async fn read_auth_api_key_snapshot_by_key_hash(
        &self,
        key_hash: &str,
        now_unix_secs: u64,
    ) -> Result<Option<GatewayAuthApiKeySnapshot>, DataLayerError> {
        read_resolved_auth_api_key_snapshot_by_key_hash(self, key_hash, now_unix_secs).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aether_data::repository::auth::{
        InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeyExportRecord,
        StoredAuthApiKeySnapshot,
    };

    use crate::data::GatewayDataState;

    fn sample_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        StoredAuthApiKeySnapshot::new(
            user_id.to_string(),
            "alice".to_string(),
            Some("alice@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(200),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
        )
        .expect("snapshot should build")
    }

    #[tokio::test]
    async fn data_state_lists_auth_api_key_export_records() {
        let repository = Arc::new(
            InMemoryAuthApiKeySnapshotRepository::seed(vec![
                (
                    Some("hash-user".to_string()),
                    sample_snapshot("key-user", "user-1"),
                ),
                (
                    Some("hash-standalone".to_string()),
                    sample_snapshot("key-standalone", "admin-1"),
                ),
            ])
            .with_export_records(vec![
                StoredAuthApiKeyExportRecord::new(
                    "user-1".to_string(),
                    "key-user".to_string(),
                    "hash-user".to_string(),
                    Some("enc-user".to_string()),
                    Some("default".to_string()),
                    None,
                    None,
                    Some(serde_json::json!(["gpt-5"])),
                    Some(60),
                    Some(5),
                    Some(serde_json::json!({"cache_1h": true})),
                    true,
                    Some(200),
                    false,
                    9,
                    0,
                    1.75,
                    false,
                )
                .expect("user export record should build"),
                StoredAuthApiKeyExportRecord::new(
                    "admin-1".to_string(),
                    "key-standalone".to_string(),
                    "hash-standalone".to_string(),
                    Some("enc-standalone".to_string()),
                    Some("standalone".to_string()),
                    None,
                    None,
                    None,
                    None,
                    Some(1),
                    None,
                    true,
                    None,
                    true,
                    2,
                    0,
                    0.5,
                    true,
                )
                .expect("standalone export record should build"),
            ]),
        );

        let state = GatewayDataState::with_auth_api_key_reader_for_tests(repository);

        let user_records = state
            .list_auth_api_key_export_records_by_user_ids(&["user-1".to_string()])
            .await
            .expect("user export records should load");
        assert_eq!(user_records.len(), 1);
        assert_eq!(user_records[0].api_key_id, "key-user");
        assert_eq!(user_records[0].total_requests, 9);

        let selected_records = state
            .list_auth_api_key_export_records_by_ids(&[
                "key-standalone".to_string(),
                "missing".to_string(),
                "key-user".to_string(),
            ])
            .await
            .expect("selected export records should load");
        assert_eq!(selected_records.len(), 2);
        assert_eq!(selected_records[0].api_key_id, "key-standalone");
        assert_eq!(selected_records[1].api_key_id, "key-user");

        let paged_records = state
            .list_auth_api_key_export_standalone_records_page(
                &aether_data::repository::auth::StandaloneApiKeyExportListQuery {
                    skip: 0,
                    limit: 10,
                    is_active: Some(true),
                },
            )
            .await
            .expect("paged standalone export records should load");
        assert_eq!(paged_records.len(), 1);
        assert_eq!(paged_records[0].api_key_id, "key-standalone");
        assert_eq!(
            state
                .count_auth_api_key_export_standalone_records(Some(true))
                .await
                .expect("standalone export count should load"),
            1
        );

        let standalone_records = state
            .list_auth_api_key_export_standalone_records()
            .await
            .expect("standalone export records should load");
        assert_eq!(standalone_records.len(), 1);
        assert_eq!(standalone_records[0].api_key_id, "key-standalone");
        assert!(standalone_records[0].is_standalone);
    }
}

use std::collections::BTreeMap;
use std::sync::RwLock;

use async_trait::async_trait;

use super::types::{
    AdjustWalletBalanceInput, AdminPaymentOrderListQuery, AdminWalletLedgerQuery,
    AdminWalletListQuery, AdminWalletRefundRequestListQuery, CompleteAdminWalletRefundInput,
    CreateManualWalletRechargeInput, CreateWalletRechargeOrderInput,
    CreateWalletRechargeOrderOutcome, CreateWalletRefundRequestInput,
    CreateWalletRefundRequestOutcome, CreditAdminPaymentOrderInput, FailAdminWalletRefundInput,
    ProcessAdminWalletRefundInput, ProcessPaymentCallbackInput, ProcessPaymentCallbackOutcome,
    StoredAdminPaymentCallbackPage, StoredAdminPaymentOrder, StoredAdminPaymentOrderPage,
    StoredAdminWalletLedgerPage, StoredAdminWalletListItem, StoredAdminWalletListPage,
    StoredAdminWalletRefundPage, StoredAdminWalletRefundRequestPage,
    StoredAdminWalletTransactionPage, StoredWalletDailyUsageLedger,
    StoredWalletDailyUsageLedgerPage, StoredWalletSnapshot, WalletLookupKey, WalletMutationOutcome,
    WalletReadRepository, WalletWriteRepository,
};
use crate::DataLayerError;

#[derive(Debug, Default)]
pub struct InMemoryWalletRepository {
    wallets_by_id: RwLock<BTreeMap<String, StoredWalletSnapshot>>,
}

impl InMemoryWalletRepository {
    pub fn seed<I>(items: I) -> Self
    where
        I: IntoIterator<Item = StoredWalletSnapshot>,
    {
        let mut wallets_by_id = BTreeMap::new();
        for item in items {
            wallets_by_id.insert(item.id.clone(), item);
        }
        Self {
            wallets_by_id: RwLock::new(wallets_by_id),
        }
    }

    pub(crate) fn with_wallets_mut<R>(
        &self,
        f: impl FnOnce(&mut BTreeMap<String, StoredWalletSnapshot>) -> R,
    ) -> R {
        let mut wallets = self.wallets_by_id.write().expect("wallet repo lock");
        f(&mut wallets)
    }
}

#[async_trait]
impl WalletReadRepository for InMemoryWalletRepository {
    async fn find(
        &self,
        key: WalletLookupKey<'_>,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        Ok(match key {
            WalletLookupKey::WalletId(wallet_id) => wallets.get(wallet_id).cloned(),
            WalletLookupKey::UserId(user_id) => wallets
                .values()
                .find(|wallet| wallet.user_id.as_deref() == Some(user_id))
                .cloned(),
            WalletLookupKey::ApiKeyId(api_key_id) => wallets
                .values()
                .find(|wallet| wallet.api_key_id.as_deref() == Some(api_key_id))
                .cloned(),
        })
    }

    async fn list_wallets_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredWalletSnapshot>, DataLayerError> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }
        let user_set: std::collections::BTreeSet<&str> =
            user_ids.iter().map(String::as_str).collect();
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        Ok(wallets
            .values()
            .filter(|wallet| {
                wallet
                    .user_id
                    .as_deref()
                    .map(|user_id| user_set.contains(user_id))
                    .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn list_wallets_by_api_key_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredWalletSnapshot>, DataLayerError> {
        if api_key_ids.is_empty() {
            return Ok(Vec::new());
        }
        let key_set: std::collections::BTreeSet<&str> =
            api_key_ids.iter().map(String::as_str).collect();
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        Ok(wallets
            .values()
            .filter(|wallet| {
                wallet
                    .api_key_id
                    .as_deref()
                    .map(|api_key_id| key_set.contains(api_key_id))
                    .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn list_admin_wallets(
        &self,
        query: &AdminWalletListQuery,
    ) -> Result<StoredAdminWalletListPage, DataLayerError> {
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        let mut items = wallets
            .values()
            .filter(|wallet| {
                query
                    .status
                    .as_deref()
                    .is_none_or(|expected| wallet.status == expected)
            })
            .filter(|wallet| match query.owner_type.as_deref() {
                Some("user") => wallet.user_id.is_some(),
                Some("api_key") => wallet.api_key_id.is_some(),
                _ => true,
            })
            .map(|wallet| StoredAdminWalletListItem {
                id: wallet.id.clone(),
                user_id: wallet.user_id.clone(),
                api_key_id: wallet.api_key_id.clone(),
                balance: wallet.balance,
                gift_balance: wallet.gift_balance,
                limit_mode: wallet.limit_mode.clone(),
                currency: wallet.currency.clone(),
                status: wallet.status.clone(),
                total_recharged: wallet.total_recharged,
                total_consumed: wallet.total_consumed,
                total_refunded: wallet.total_refunded,
                total_adjusted: wallet.total_adjusted,
                user_name: None,
                api_key_name: None,
                created_at_unix_secs: None,
                updated_at_unix_secs: Some(wallet.updated_at_unix_secs),
            })
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            right
                .updated_at_unix_secs
                .cmp(&left.updated_at_unix_secs)
                .then_with(|| right.id.cmp(&left.id))
        });
        let total = items.len() as u64;
        let items = items
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect::<Vec<_>>();
        Ok(StoredAdminWalletListPage { items, total })
    }

    async fn list_admin_wallet_ledger(
        &self,
        _query: &AdminWalletLedgerQuery,
    ) -> Result<StoredAdminWalletLedgerPage, DataLayerError> {
        Ok(StoredAdminWalletLedgerPage::default())
    }

    async fn list_admin_wallet_refund_requests(
        &self,
        _query: &AdminWalletRefundRequestListQuery,
    ) -> Result<StoredAdminWalletRefundRequestPage, DataLayerError> {
        Ok(StoredAdminWalletRefundRequestPage::default())
    }

    async fn list_admin_wallet_transactions(
        &self,
        _wallet_id: &str,
        _limit: usize,
        _offset: usize,
    ) -> Result<StoredAdminWalletTransactionPage, DataLayerError> {
        Ok(StoredAdminWalletTransactionPage::default())
    }

    async fn find_wallet_today_usage(
        &self,
        _wallet_id: &str,
        _billing_timezone: &str,
    ) -> Result<Option<StoredWalletDailyUsageLedger>, DataLayerError> {
        Ok(None)
    }

    async fn list_wallet_daily_usage_history(
        &self,
        _wallet_id: &str,
        _billing_timezone: &str,
        _limit: usize,
    ) -> Result<StoredWalletDailyUsageLedgerPage, DataLayerError> {
        Ok(StoredWalletDailyUsageLedgerPage::default())
    }

    async fn list_admin_wallet_refunds(
        &self,
        _wallet_id: &str,
        _limit: usize,
        _offset: usize,
    ) -> Result<StoredAdminWalletRefundPage, DataLayerError> {
        Ok(StoredAdminWalletRefundPage::default())
    }

    async fn list_admin_payment_orders(
        &self,
        _query: &AdminPaymentOrderListQuery,
    ) -> Result<StoredAdminPaymentOrderPage, DataLayerError> {
        Ok(StoredAdminPaymentOrderPage::default())
    }

    async fn find_admin_payment_order(
        &self,
        _order_id: &str,
    ) -> Result<Option<StoredAdminPaymentOrder>, DataLayerError> {
        Ok(None)
    }

    async fn list_wallet_payment_orders_by_user_id(
        &self,
        _user_id: &str,
        _limit: usize,
        _offset: usize,
    ) -> Result<StoredAdminPaymentOrderPage, DataLayerError> {
        Ok(StoredAdminPaymentOrderPage::default())
    }

    async fn find_wallet_payment_order_by_user_id(
        &self,
        _user_id: &str,
        _order_id: &str,
    ) -> Result<Option<StoredAdminPaymentOrder>, DataLayerError> {
        Ok(None)
    }

    async fn find_wallet_refund(
        &self,
        _wallet_id: &str,
        _refund_id: &str,
    ) -> Result<Option<super::types::StoredAdminWalletRefund>, DataLayerError> {
        Ok(None)
    }

    async fn list_admin_payment_callbacks(
        &self,
        _payment_method: Option<&str>,
        _limit: usize,
        _offset: usize,
    ) -> Result<StoredAdminPaymentCallbackPage, DataLayerError> {
        Ok(StoredAdminPaymentCallbackPage::default())
    }
}

#[async_trait]
impl WalletWriteRepository for InMemoryWalletRepository {
    async fn create_wallet_recharge_order(
        &self,
        input: CreateWalletRechargeOrderInput,
    ) -> Result<CreateWalletRechargeOrderOutcome, DataLayerError> {
        let mut wallets = self.wallets_by_id.write().expect("wallet repo lock");
        let wallet = wallets
            .values_mut()
            .find(|wallet| wallet.user_id.as_deref() == Some(input.user_id.as_str()));
        if wallet
            .as_ref()
            .is_some_and(|wallet| wallet.status != "active")
        {
            return Ok(CreateWalletRechargeOrderOutcome::WalletInactive);
        }
        let wallet_id = wallet
            .map(|wallet| wallet.id.clone())
            .or(input.preferred_wallet_id)
            .unwrap_or_else(|| "wallet-memory".to_string());
        Ok(CreateWalletRechargeOrderOutcome::Created(
            StoredAdminPaymentOrder {
                id: "payment-order-memory".to_string(),
                order_no: input.order_no,
                wallet_id,
                user_id: Some(input.user_id),
                amount_usd: input.amount_usd,
                pay_amount: input.pay_amount,
                pay_currency: input.pay_currency,
                exchange_rate: input.exchange_rate,
                refunded_amount_usd: 0.0,
                refundable_amount_usd: 0.0,
                payment_method: input.payment_method,
                gateway_order_id: Some(input.gateway_order_id),
                gateway_response: Some(input.gateway_response),
                status: "pending".to_string(),
                created_at_unix_secs: 0,
                paid_at_unix_secs: None,
                credited_at_unix_secs: None,
                expires_at_unix_secs: Some(input.expires_at_unix_secs),
            },
        ))
    }

    async fn create_wallet_refund_request(
        &self,
        input: CreateWalletRefundRequestInput,
    ) -> Result<CreateWalletRefundRequestOutcome, DataLayerError> {
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        let Some(wallet) = wallets.get(&input.wallet_id) else {
            return Ok(CreateWalletRefundRequestOutcome::WalletMissing);
        };
        if input.amount_usd > wallet.balance {
            return Ok(CreateWalletRefundRequestOutcome::RefundAmountExceedsAvailableBalance);
        }
        Ok(CreateWalletRefundRequestOutcome::Created(
            super::types::StoredAdminWalletRefund {
                id: "refund-memory".to_string(),
                refund_no: input.refund_no,
                wallet_id: input.wallet_id,
                user_id: Some(input.user_id),
                payment_order_id: input.payment_order_id,
                source_type: input
                    .source_type
                    .unwrap_or_else(|| "wallet_balance".to_string()),
                source_id: input.source_id,
                refund_mode: input
                    .refund_mode
                    .unwrap_or_else(|| "offline_payout".to_string()),
                amount_usd: input.amount_usd,
                status: "pending_approval".to_string(),
                reason: input.reason,
                failure_reason: None,
                gateway_refund_id: None,
                payout_method: None,
                payout_reference: None,
                payout_proof: None,
                requested_by: None,
                approved_by: None,
                processed_by: None,
                created_at_unix_secs: 0,
                updated_at_unix_secs: 0,
                processed_at_unix_secs: None,
                completed_at_unix_secs: None,
            },
        ))
    }

    async fn process_payment_callback(
        &self,
        _input: ProcessPaymentCallbackInput,
    ) -> Result<ProcessPaymentCallbackOutcome, DataLayerError> {
        Ok(ProcessPaymentCallbackOutcome::Failed {
            duplicate: false,
            error: "payment callback is not supported in memory wallet repository".to_string(),
        })
    }

    async fn adjust_wallet_balance(
        &self,
        _input: AdjustWalletBalanceInput,
    ) -> Result<
        Option<(
            StoredWalletSnapshot,
            super::types::StoredAdminWalletTransaction,
        )>,
        DataLayerError,
    > {
        Ok(None)
    }

    async fn create_manual_wallet_recharge(
        &self,
        _input: CreateManualWalletRechargeInput,
    ) -> Result<Option<(StoredWalletSnapshot, StoredAdminPaymentOrder)>, DataLayerError> {
        Ok(None)
    }

    async fn process_admin_wallet_refund(
        &self,
        _input: ProcessAdminWalletRefundInput,
    ) -> Result<
        WalletMutationOutcome<(
            StoredWalletSnapshot,
            super::types::StoredAdminWalletRefund,
            super::types::StoredAdminWalletTransaction,
        )>,
        DataLayerError,
    > {
        Ok(WalletMutationOutcome::NotFound)
    }

    async fn complete_admin_wallet_refund(
        &self,
        _input: CompleteAdminWalletRefundInput,
    ) -> Result<WalletMutationOutcome<super::types::StoredAdminWalletRefund>, DataLayerError> {
        Ok(WalletMutationOutcome::NotFound)
    }

    async fn fail_admin_wallet_refund(
        &self,
        _input: FailAdminWalletRefundInput,
    ) -> Result<
        WalletMutationOutcome<(
            StoredWalletSnapshot,
            super::types::StoredAdminWalletRefund,
            Option<super::types::StoredAdminWalletTransaction>,
        )>,
        DataLayerError,
    > {
        Ok(WalletMutationOutcome::NotFound)
    }

    async fn expire_admin_payment_order(
        &self,
        _order_id: &str,
    ) -> Result<WalletMutationOutcome<(StoredAdminPaymentOrder, bool)>, DataLayerError> {
        Ok(WalletMutationOutcome::NotFound)
    }

    async fn fail_admin_payment_order(
        &self,
        _order_id: &str,
    ) -> Result<WalletMutationOutcome<StoredAdminPaymentOrder>, DataLayerError> {
        Ok(WalletMutationOutcome::NotFound)
    }

    async fn credit_admin_payment_order(
        &self,
        _input: CreditAdminPaymentOrderInput,
    ) -> Result<WalletMutationOutcome<(StoredAdminPaymentOrder, bool)>, DataLayerError> {
        Ok(WalletMutationOutcome::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryWalletRepository;
    use crate::repository::wallet::{
        AdminWalletListQuery, StoredWalletSnapshot, WalletLookupKey, WalletReadRepository,
    };

    fn sample_wallet() -> StoredWalletSnapshot {
        StoredWalletSnapshot::new(
            "wallet-1".to_string(),
            Some("user-1".to_string()),
            Some("key-1".to_string()),
            10.0,
            2.0,
            "finite".to_string(),
            "USD".to_string(),
            "active".to_string(),
            0.0,
            0.0,
            0.0,
            0.0,
            100,
        )
        .expect("wallet should build")
    }

    #[tokio::test]
    async fn finds_wallet_by_owner() {
        let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
        let wallet = repository
            .find(WalletLookupKey::UserId("user-1"))
            .await
            .expect("lookup should succeed")
            .expect("wallet should exist");
        assert_eq!(wallet.id, "wallet-1");
    }

    #[tokio::test]
    async fn lists_admin_wallets_with_filters_and_pagination() {
        let repository = InMemoryWalletRepository::seed(vec![
            sample_wallet(),
            StoredWalletSnapshot::new(
                "wallet-2".to_string(),
                Some("user-2".to_string()),
                None,
                3.0,
                1.0,
                "finite".to_string(),
                "USD".to_string(),
                "inactive".to_string(),
                0.0,
                0.0,
                0.0,
                0.0,
                90,
            )
            .expect("wallet should build"),
            StoredWalletSnapshot::new(
                "wallet-3".to_string(),
                None,
                Some("key-3".to_string()),
                5.0,
                0.0,
                "unlimited".to_string(),
                "USD".to_string(),
                "active".to_string(),
                0.0,
                0.0,
                0.0,
                0.0,
                110,
            )
            .expect("wallet should build"),
        ]);

        let page = repository
            .list_admin_wallets(&AdminWalletListQuery {
                status: Some("active".to_string()),
                owner_type: Some("api_key".to_string()),
                limit: 1,
                offset: 0,
            })
            .await
            .expect("list should succeed");

        assert_eq!(page.total, 2);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, "wallet-3");
        assert_eq!(page.items[0].updated_at_unix_secs, Some(110));
    }

    #[tokio::test]
    async fn daily_usage_queries_default_to_empty_in_memory() {
        let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
        let today = repository
            .find_wallet_today_usage("wallet-1", "Asia/Shanghai")
            .await
            .expect("lookup should succeed");
        let history = repository
            .list_wallet_daily_usage_history("wallet-1", "Asia/Shanghai", 20)
            .await
            .expect("history should succeed");

        assert!(today.is_none());
        assert_eq!(history.total, 0);
        assert!(history.items.is_empty());
    }
}

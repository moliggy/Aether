use async_trait::async_trait;
use sqlx::{PgPool, Row};

use super::types::{SettlementWriteRepository, StoredUsageSettlement, UsageSettlementInput};
use crate::postgres::PostgresTransactionRunner;
use crate::DataLayerError;

const FINALIZE_USAGE_BILLING_SQL: &str = r#"
UPDATE "usage"
SET
  billing_status = $2,
  finalized_at = COALESCE(finalized_at, to_timestamp($3))
WHERE request_id = $1
"#;

#[derive(Debug, Clone)]
pub struct SqlxSettlementRepository {
    tx_runner: PostgresTransactionRunner,
}

impl SqlxSettlementRepository {
    pub fn new(pool: PgPool) -> Self {
        let tx_runner = PostgresTransactionRunner::new(pool);
        Self { tx_runner }
    }
}

#[async_trait]
impl SettlementWriteRepository for SqlxSettlementRepository {
    async fn settle_usage(
        &self,
        input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        input.validate()?;
        self.tx_runner
            .run_read_write(|tx| {
                Box::pin(async move {
                    let row = sqlx::query(
                        r#"
SELECT
  request_id,
  wallet_id,
  billing_status,
  CAST(wallet_balance_before AS DOUBLE PRECISION) AS wallet_balance_before,
  CAST(wallet_balance_after AS DOUBLE PRECISION) AS wallet_balance_after,
  CAST(wallet_recharge_balance_before AS DOUBLE PRECISION) AS wallet_recharge_balance_before,
  CAST(wallet_recharge_balance_after AS DOUBLE PRECISION) AS wallet_recharge_balance_after,
  CAST(wallet_gift_balance_before AS DOUBLE PRECISION) AS wallet_gift_balance_before,
  CAST(wallet_gift_balance_after AS DOUBLE PRECISION) AS wallet_gift_balance_after,
  provider_id,
  CAST(EXTRACT(EPOCH FROM finalized_at) AS BIGINT) AS finalized_at_unix_secs
FROM "usage"
WHERE request_id = $1
FOR UPDATE
                        "#,
                    )
                    .bind(&input.request_id)
                    .fetch_optional(&mut **tx)
                    .await?;

                    let Some(usage_row) = row else {
                        return Ok(None);
                    };

                    let current_billing_status: String = usage_row.try_get("billing_status")?;
                    if current_billing_status == "settled" || current_billing_status == "void" {
                        return Ok(Some(StoredUsageSettlement {
                            request_id: usage_row.try_get("request_id")?,
                            wallet_id: usage_row.try_get("wallet_id")?,
                            billing_status: current_billing_status,
                            wallet_balance_before: usage_row.try_get("wallet_balance_before")?,
                            wallet_balance_after: usage_row.try_get("wallet_balance_after")?,
                            wallet_recharge_balance_before: usage_row
                                .try_get("wallet_recharge_balance_before")?,
                            wallet_recharge_balance_after: usage_row
                                .try_get("wallet_recharge_balance_after")?,
                            wallet_gift_balance_before: usage_row
                                .try_get("wallet_gift_balance_before")?,
                            wallet_gift_balance_after: usage_row
                                .try_get("wallet_gift_balance_after")?,
                            provider_monthly_used_usd: None,
                            finalized_at_unix_secs: usage_row
                                .try_get::<Option<i64>, _>("finalized_at_unix_secs")?
                                .map(|value| value as u64),
                        }));
                    }

                    let final_billing_status = if input.status == "completed" {
                        "settled"
                    } else {
                        "void"
                    };
                    let finalized_at =
                        i64::try_from(input.finalized_at_unix_secs.unwrap_or_else(|| {
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                        }))
                        .map_err(|_| {
                            DataLayerError::InvalidInput("finalized_at overflow".to_string())
                        })?;

                    let mut settlement = StoredUsageSettlement {
                        request_id: input.request_id.clone(),
                        wallet_id: None,
                        billing_status: final_billing_status.to_string(),
                        wallet_balance_before: None,
                        wallet_balance_after: None,
                        wallet_recharge_balance_before: None,
                        wallet_recharge_balance_after: None,
                        wallet_gift_balance_before: None,
                        wallet_gift_balance_after: None,
                        provider_monthly_used_usd: None,
                        finalized_at_unix_secs: Some(finalized_at as u64),
                    };

                    if final_billing_status == "settled" {
                        let wallet_row = if let Some(api_key_id) = input
                            .api_key_id
                            .as_deref()
                            .filter(|value| !value.is_empty())
                        {
                            sqlx::query(
                                r#"
SELECT
  id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode
FROM wallets
WHERE api_key_id = $1
FOR UPDATE
LIMIT 1
                                "#,
                            )
                            .bind(api_key_id)
                            .fetch_optional(&mut **tx)
                            .await?
                        } else {
                            None
                        };

                        let wallet_row = if wallet_row.is_some() {
                            wallet_row
                        } else if let Some(user_id) =
                            input.user_id.as_deref().filter(|value| !value.is_empty())
                        {
                            sqlx::query(
                                r#"
SELECT
  id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode
FROM wallets
WHERE user_id = $1
FOR UPDATE
LIMIT 1
                                "#,
                            )
                            .bind(user_id)
                            .fetch_optional(&mut **tx)
                            .await?
                        } else {
                            None
                        };

                        if let Some(wallet_row) = wallet_row {
                            let wallet_id: String = wallet_row.try_get("id")?;
                            let before_recharge: f64 = wallet_row.try_get("balance")?;
                            let before_gift: f64 = wallet_row.try_get("gift_balance")?;
                            let limit_mode: String = wallet_row.try_get("limit_mode")?;
                            let before_total = before_recharge + before_gift;
                            let mut after_recharge = before_recharge;
                            let mut after_gift = before_gift;
                            if !limit_mode.eq_ignore_ascii_case("unlimited") {
                                let gift_deduction = before_gift.max(0.0).min(input.total_cost_usd);
                                let recharge_deduction = input.total_cost_usd - gift_deduction;
                                after_gift = before_gift - gift_deduction;
                                after_recharge = before_recharge - recharge_deduction;
                            }
                            sqlx::query(
                                r#"
UPDATE wallets
SET
  balance = $2,
  gift_balance = $3,
  total_consumed = CAST(total_consumed AS DOUBLE PRECISION) + $4,
  updated_at = NOW()
WHERE id = $1
                                "#,
                            )
                            .bind(&wallet_id)
                            .bind(after_recharge)
                            .bind(after_gift)
                            .bind(input.total_cost_usd)
                            .execute(&mut **tx)
                            .await?;

                            settlement.wallet_id = Some(wallet_id.clone());
                            settlement.wallet_balance_before = Some(before_total);
                            settlement.wallet_balance_after = Some(after_recharge + after_gift);
                            settlement.wallet_recharge_balance_before = Some(before_recharge);
                            settlement.wallet_recharge_balance_after = Some(after_recharge);
                            settlement.wallet_gift_balance_before = Some(before_gift);
                            settlement.wallet_gift_balance_after = Some(after_gift);

                            sqlx::query(
                                r#"
UPDATE "usage"
SET
  wallet_id = $2,
  wallet_balance_before = $3,
  wallet_balance_after = $4,
  wallet_recharge_balance_before = $5,
  wallet_recharge_balance_after = $6,
  wallet_gift_balance_before = $7,
  wallet_gift_balance_after = $8
WHERE request_id = $1
                                "#,
                            )
                            .bind(&input.request_id)
                            .bind(&wallet_id)
                            .bind(before_total)
                            .bind(after_recharge + after_gift)
                            .bind(before_recharge)
                            .bind(after_recharge)
                            .bind(before_gift)
                            .bind(after_gift)
                            .execute(&mut **tx)
                            .await?;
                        }

                        if let Some(provider_id) = input
                            .provider_id
                            .as_deref()
                            .filter(|value| !value.is_empty())
                        {
                            let quota_row = sqlx::query(
                                r#"
UPDATE providers
SET
  monthly_used_usd = COALESCE(monthly_used_usd, 0) + $2,
  updated_at = NOW()
WHERE id = $1
RETURNING CAST(monthly_used_usd AS DOUBLE PRECISION) AS monthly_used_usd
                                "#,
                            )
                            .bind(provider_id)
                            .bind(input.actual_total_cost_usd)
                            .fetch_optional(&mut **tx)
                            .await?;
                            settlement.provider_monthly_used_usd =
                                quota_row.and_then(|row| row.try_get("monthly_used_usd").ok());
                        }
                    }

                    sqlx::query(FINALIZE_USAGE_BILLING_SQL)
                        .bind(&input.request_id)
                        .bind(final_billing_status)
                        .bind(finalized_at)
                        .execute(&mut **tx)
                        .await?;

                    Ok(Some(settlement))
                })
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn finalize_usage_billing_sql_does_not_require_usage_updated_at_column() {
        assert!(!super::FINALIZE_USAGE_BILLING_SQL.contains("updated_at"));
    }
}

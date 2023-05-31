use crate::Settings;
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream, mobile_subscriber::SubscriberLocationIngestReport,
};
use futures::{StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use mobile_config::Client;
use rust_decimal::prelude::*;
use sqlx::{PgPool, Postgres, Transaction};
use std::{collections::HashMap, ops::Range, time::Duration};
use tokio::{
    sync::mpsc::Receiver,
    time::{self, MissedTickBehavior},
};

pub type LocationSharingMap = HashMap<Vec<u8>, Vec<Decimal>>;

pub struct SubscriberLocationIngestor {
    pub pool: PgPool,
    pub carrier_keys: Vec<PublicKeyBinary>,
    pub carrier_keys_refresh_interval: Duration,
    _config_client: Client,
    reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
}

impl SubscriberLocationIngestor {
    pub fn from_settings(
        settings: &Settings,
        pool: sqlx::Pool<sqlx::Postgres>,
        config_client: Client,
        carrier_keys: Vec<PublicKeyBinary>,
        reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
    ) -> Self {
        Self {
            pool,
            carrier_keys_refresh_interval: settings.carrier_keys_refresh_interval(),
            carrier_keys,
            _config_client: config_client,
            reports_receiver,
        }
    }
    pub async fn run(mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        let mut refresh_carrier_keys_timer = time::interval(self.carrier_keys_refresh_interval);
        refresh_carrier_keys_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = refresh_carrier_keys_timer.tick() =>
                match self.refresh_carrier_keys().await {
                Ok(()) => (),
                Err(err) => {
                    tracing::error!("error whilst refreshing carrier keys: {err:?}");
                }
                },

                msg = self.reports_receiver.recv() => if let Some(stream) =  msg {
                    self.process_file(stream).await?;
                }
            }
        }
        tracing::info!("stopping subscriber location reports handler");
        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<SubscriberLocationIngestReport>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async move {
                if self.verify_known_carrier_key(&report.report.pubkey) {
                    self.save(
                        report.report.subscriber_id.clone(),
                        report.received_timestamp,
                        &mut transaction,
                    )
                    .await?;
                    metrics::increment_counter!(
                        "oracles_mobile_verifier_valid_subscriber_location_report"
                    );
                } else {
                    metrics::increment_counter!(
                        "oracles_mobile_verifier_invalid_subscriber_location_report"
                    );
                }
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }

    pub async fn save(
        &self,
        subscriber_id: Vec<u8>,
        timestamp: DateTime<Utc>,
        db: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
                INSERT INTO subscriber_loc (subscriber_id, date_bucket, hour_bucket, reward_timestamp)
                VALUES ($1, DATE_TRUNC('hour', $2), DATE_PART('hour', $2), $2)
                ON CONFLICT DO NOTHING;
                "#,
        )
        .bind(subscriber_id)
        .bind(timestamp)
        .execute(&mut *db)
        .await?;
        Ok(())
    }

    //TODO: reinstate check when keys are available
    fn verify_known_carrier_key(&self, _public_key: &PublicKeyBinary) -> bool {
        // self.carrier_keys.contains(public_key)
        true
    }

    // TODO: update when actual API is available
    async fn refresh_carrier_keys(&mut self) -> anyhow::Result<()> {
        // match self
        // .config_client
        // .clone()
        // .resolve_carrier_keys()
        // .await
        // {
        //     Ok(res) => {
        //         self.carrier_keys = res;
        //     }
        //     Err(err) => (),
        // }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
pub struct SubscriberLocationShare {
    pub subscriber_id: Vec<u8>,
    pub hour_bucket: Decimal,
}

pub async fn aggregate_location_shares(
    db: impl sqlx::PgExecutor<'_> + Copy,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<LocationSharingMap, sqlx::Error> {
    let mut rows = sqlx::query_as::<_, SubscriberLocationShare>(
        "select subscriber_id, hour_bucket::numeric from subscriber_loc where reward_timestamp > $1 and reward_timestamp <= $2",
    )
    .bind(reward_period.start)
    .bind(reward_period.end)
    .fetch(db);
    let mut location_shares = LocationSharingMap::new();
    while let Some(share) = rows.try_next().await? {
        location_shares
            .entry(share.subscriber_id)
            .or_insert_with(Vec::new)
            .push(share.hour_bucket)
    }
    Ok(location_shares)
}

pub async fn clear_location_shares(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from subscriber_loc where reward_timestamp <= $1")
        .bind(reward_period.end)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

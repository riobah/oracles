use crate::{
    heartbeats::{Heartbeat, Heartbeats},
    ingest,
    reward_shares::{PocShares, TransferRewards},
    scheduler::Scheduler,
    speedtests::{FetchError, SpeedtestAverages, SpeedtestRollingAverage, SpeedtestStore},
};
use anyhow::bail;
use chrono::{DateTime, Duration, DurationRound, Local, NaiveDateTime, TimeZone, Timelike, Utc};
use db_store::meta;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CellHeartbeatIngestReport, speedtest::CellSpeedtestIngestReport, FileStore,
};
use futures::{stream::Stream, StreamExt};
use helium_proto::RewardManifest;
use mobile_config::{client::ClientError, Client};
use price::PriceTracker;
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, Pool, Postgres};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::Range,
    sync::Arc,
    time,
    pin::pin,
};
use tokio::{sync::mpsc::Receiver, time::sleep};

pub struct VerifierDaemon {
    pub pool: Pool<Postgres>,
    pub heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
    pub speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
    pub radio_rewards: FileSinkClient,
    pub mobile_rewards: FileSinkClient,
    pub reward_manifests: FileSinkClient,
    pub reward_period_hours: i64,
    pub price_tracker: PriceTracker,
    pub data_transfer_ingest: FileStore,
    pub config_client: Client,

    pub next_rewarded_end_time: DateTime<Utc>,
}

impl VerifierDaemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting verifier service");

        let Self {
            pool,
            mut heartbeats,
            speedtests,
            next_rewarded_end_time,
            config_client,
            ..
        } = self;

        let reward_period_length = Duration::hours(self.reward_period_hours);
        //        let verification_period_length = reward_period_length / self.verifications_per_period;

        let heartbeat_pool = pool.clone();
        let heartbeat_config_client = config_client.clone();
        let heartbeat_verification_task = tokio::spawn(async move {
            let heartbeat_cache = Arc::new(Cache::<(String, NaiveDateTime), ()>::new());

            let cache_clone = heartbeat_cache.clone();
            tokio::spawn(async move {
                cache_clone
                    .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 3))
                    .await
            });

            loop {
                let Some(file) = heartbeats.recv().await else {
                    bail!("Heartbeat report file stream was dropped unexpectedly");
                };
                let mut transaction = heartbeat_pool.begin().await?;
                let reports = file.into_stream(&mut transaction).await?;

                let epoch = Utc::now()..Utc::now();
                let mut valid_heartbeats = pin!(
                    Heartbeat::validate_heartbeats(
                        &heartbeat_config_client,
                        reports.map(|h| h.report),
                        &epoch
                    )
                    .await
                );

                while let Some(heartbeat) = valid_heartbeats.next().await.transpose()? {
                    // heartbeat.write(&self.heartbeats).await?;
                    let key = (
                        heartbeat.cbsd_id.clone(),
                        heartbeat.timestamp.duration_trunc(Duration::hours(1))?,
                    );
                    if heartbeat_cache.get(&key).await.is_none() {
                        heartbeat.save(&mut transaction).await?;
                        heartbeat_cache
                            .insert(key, (), time::Duration::from_secs(60 * 60 * 2))
                            .await;
                    }
                }

                transaction.commit().await?;
            }

            Ok(())
        });

        let speedtest_pool = pool.clone();
        let speedtest_config_client = config_client.clone();
        let speedtest_verification_task = tokio::spawn(async move {
            loop {
                let Some(file) = speedtests.recv().await else {
                    bail!("Speedtest report file stream was dropped unexpectedly");
                };
                let mut transaction = speedtest_pool.begin().await?;
                let reports = file.into_stream(&mut transaction).await?;

                let mut valid_speedtests = pin!(
                    SpeedtestRollingAverage::validate_speedtests(
                        &speedtest_config_client,
                        reports.map(|s| s.report),
                        &mut *transaction,
                    ).await
                );
                while let Some(speedtest) = valid_speedtests.next().await.transpose()? {
                    // speedtest.write(&self.speedtest_avgs).await?;
                    speedtest.save(&mut transaction).await?;
                }

                transaction.commit().await?;
            }
        });

        Ok(())

        /*
            loop {
                // Does this seem weird? That's because it is!
                let duration_until_next_reward = self.next_rewarded_end_time - Utc::now();
                sleep_until(Instant::now() + duration_until_next_reward).await;

                tokio::select! {
                    heartbeat

                    _ = sleep_until(time_for_next_reward) => {
                        todo!()
                    }
                }

                let scheduler = Scheduler::new(
                    verification_period_length,
                    reward_period_length,
                    last_verified_end_time(&self.pool).await?,
                    last_rewarded_end_time(&self.pool).await?,
                    next_rewarded_end_time(&self.pool).await?,
                    self.verification_offset,
                );

                if scheduler.should_verify(now) {
                    tracing::info!("Verifying epoch: {:?}", scheduler.verification_period);
                    self.verify(&scheduler).await?;
                }

                if scheduler.should_reward(now) {
                    tracing::info!("Rewarding epoch: {:?}", scheduler.reward_period);
                    self.reward(&scheduler).await?
                }

                let sleep_duration = scheduler.sleep_duration(Utc::now())?;

                tracing::info!(
                    "Sleeping for {}",
                    humantime::format_duration(sleep_duration)
                );
                let shutdown = shutdown.clone();
                tokio::select! {
                    _ = shutdown => return Ok(()),
                    _ = sleep(sleep_duration) => (),
                }
        }
             */
    }

    /*
        pub async fn verify(&mut self, scheduler: &Scheduler) -> anyhow::Result<()> {
            let VerifiedEpoch {
                heartbeats,
                speedtests,
            } = self
                .verifier
                .verify_epoch(&self.pool, &scheduler.verification_period)
                .await?;

            let mut transaction = self.pool.begin().await?;

            pin!(heartbeats);
            pin!(speedtests);

            let mut cache: HashMap<(String, u32), bool> = HashMap::new();

            // TODO: switch to a bulk transaction
            while let Some(heartbeat) = heartbeats.next().await.transpose()? {
                heartbeat.write(&self.heartbeats).await?;
                let key = (heartbeat.cbsd_id.clone(), heartbeat.timestamp.hour());
                if let Entry::Vacant(e) = cache.entry(key) {
                    e.insert(true);
                    heartbeat.save(&mut transaction).await?;
                }
            }

            while let Some(speedtest) = speedtests.next().await.transpose()? {
                speedtest.write(&self.speedtest_avgs).await?;
                speedtest.save(&mut transaction).await?;
            }

            save_last_verified_end_time(&mut transaction, &scheduler.verification_period.end).await?;
            let _ = self.heartbeats.commit().await?.await??;
            let _ = self.speedtest_avgs.commit().await?.await??;
            transaction.commit().await?;

            Ok(())
        }

        pub async fn reward(&mut self, scheduler: &Scheduler) -> anyhow::Result<()> {
            let heartbeats = Heartbeats::validated(&self.pool).await?;
            let speedtests =
                SpeedtestAverages::validated(&self.pool, scheduler.reward_period.end).await?;

            let poc_rewards = PocShares::aggregate(heartbeats, speedtests).await;
            let mobile_price = self
                .price_tracker
                .price(&helium_proto::BlockchainTokenTypeV1::Mobile)
                .await?;
            // Mobile prices are supplied in 10^6, so we must convert them to Decimal
            let mobile_bone_price = Decimal::from(mobile_price)
                / dec!(1_000_000)  // Per Mobile token
                / dec!(1_000_000); // Per Bone
            let transfer_rewards = TransferRewards::from_transfer_sessions(
                mobile_bone_price,
                ingest::ingest_valid_data_transfers(
                    &self.data_transfer_ingest,
                    &scheduler.reward_period,
                )
                .await,
                &poc_rewards,
                &scheduler.reward_period,
            )
            .await;

            // It's important to gauge the scale metric. If this value is < 1.0, we are in
            // big trouble.
            let Some(scale) = transfer_rewards.reward_scale().to_f64() else {
                bail!("The data transfer rewards scale cannot be converted to a float");
            };
            metrics::gauge!("data_transfer_rewards_scale", scale);

            for (radio_reward_share, mobile_reward_share) in
                poc_rewards.into_rewards(&transfer_rewards, &scheduler.reward_period)
            {
                self.radio_rewards
                    .write(radio_reward_share, [])
                    .await?
                    // Await the returned one shot to ensure that we wrote the file
                    .await??;
                self.mobile_rewards
                    .write(mobile_reward_share, [])
                    .await?
                    // Await the returned one shot to ensure that we wrote the file
                    .await??;
            }

            let written_files = self.mobile_rewards.commit().await?.await??;

            let mut transaction = self.pool.begin().await?;
            // Clear the heartbeats table:
            sqlx::query("TRUNCATE TABLE heartbeats;")
                .execute(&mut transaction)
                .await?;

            save_last_rewarded_end_time(&mut transaction, &scheduler.reward_period.end).await?;
            save_next_rewarded_end_time(&mut transaction, &scheduler.next_reward_period().end).await?;
            transaction.commit().await?;

            // now that the db has been purged, safe to write out the manifest
            self.reward_manifests
                .write(
                    RewardManifest {
                        start_timestamp: scheduler.reward_period.start.encode_timestamp(),
                        end_timestamp: scheduler.reward_period.end.encode_timestamp(),
                        written_files,
                    },
                    [],
                )
                .await?
                .await??;

            self.reward_manifests.commit().await?;

            Ok(())
    }
        */
}

/*
pub struct Verifier {
    pub config_client: Client,
    pub ingest: FileStore,
}

impl Verifier {
    pub fn new(config_client: Client, ingest: FileStore) -> Self {
        Self {
            config_client,
            ingest,
        }
    }

    pub async fn verify_epoch<'a>(
        &'a self,
        pool: impl SpeedtestStore + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> file_store::Result<
        VerifiedEpoch<
            impl Stream<Item = Result<Heartbeat, ClientError>> + 'a,
            impl Stream<Item = Result<SpeedtestRollingAverage, FetchError>> + 'a,
        >,
    > {
        let heartbeats = Heartbeat::validate_heartbeats(
            &self.config_client,
            ingest::ingest_heartbeats(&self.ingest, epoch).await,
            epoch,
        )
        .await;

        let speedtests = SpeedtestRollingAverage::validate_speedtests(
            &self.config_client,
            ingest::ingest_speedtests(&self.ingest, epoch).await,
            pool,
        )
        .await;

        Ok(VerifiedEpoch {
            heartbeats,
            speedtests,
        })
    }
}

pub struct VerifiedEpoch<H, S> {
    pub heartbeats: H,
    pub speedtests: S,
}

async fn last_verified_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(exec, "last_verified_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_last_verified_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_verified_end_time", value.timestamp()).await
}

async fn last_rewarded_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(exec, "last_rewarded_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_last_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_rewarded_end_time", value.timestamp()).await
}

async fn next_rewarded_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(exec, "next_rewarded_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_next_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "next_rewarded_end_time", value.timestamp()).await
}
*/

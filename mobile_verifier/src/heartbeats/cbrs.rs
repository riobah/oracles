use super::{process_validated_heartbeats, Heartbeat, ValidatedHeartbeat};
use crate::{
    coverage::{CoverageClaimTimeCache, CoverageObjectCache},
    geofence::GeofenceValidator,
    GatewayResolver,
};

use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CbrsHeartbeatIngestReport,
};
use futures::{stream::StreamExt, TryFutureExt};
use retainer::Cache;
use std::{
    sync::Arc,
    time::{self, Instant},
};
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

pub struct HeartbeatDaemon<GIR, GFV> {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_info_resolver: GIR,
    heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
    modeled_coverage_start: DateTime<Utc>,
    max_distance_to_asserted: u32,
    max_distance_to_coverage: u32,
    heartbeat_sink: FileSinkClient,
    seniority_sink: FileSinkClient,
    geofence: GFV,
}

impl<GIR, GFV> HeartbeatDaemon<GIR, GFV>
where
    GIR: GatewayResolver,
    GFV: GeofenceValidator,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_info_resolver: GIR,
        heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
        modeled_coverage_start: DateTime<Utc>,
        max_distance_to_asserted: u32,
        max_distance_to_coverage: u32,
        heartbeat_sink: FileSinkClient,
        seniority_sink: FileSinkClient,
        geofence: GFV,
    ) -> Self {
        Self {
            pool,
            gateway_info_resolver,
            heartbeats,
            modeled_coverage_start,
            max_distance_to_asserted,
            max_distance_to_coverage,
            heartbeat_sink,
            seniority_sink,
            geofence,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting CBRS HeartbeatDaemon");
        let heartbeat_cache = Arc::new(Cache::<(String, DateTime<Utc>), ()>::new());

        let heartbeat_cache_clone = heartbeat_cache.clone();
        tokio::spawn(async move {
            heartbeat_cache_clone
                .monitor(4, 0.25, time::Duration::from_secs(60 * 60 * 3))
                .await
        });

        let coverage_claim_time_cache = CoverageClaimTimeCache::new();
        let coverage_object_cache = CoverageObjectCache::new(&self.pool);

        loop {
            #[rustfmt::skip]
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("CBRS HeartbeatDaemon shutting down");
                    break;
                }
                Some(file) = self.heartbeats.recv() => {
		    let start = Instant::now();
		    self.process_file(
                        file,
                        &heartbeat_cache,
                        &coverage_claim_time_cache,
                        &coverage_object_cache,
		    ).await?;
		    metrics::histogram!("cbrs_heartbeat_processing_time", start.elapsed());
                }
            }
        }

        Ok(())
    }

    async fn process_file(
        &self,
        file: FileInfoStream<CbrsHeartbeatIngestReport>,
        heartbeat_cache: &Arc<Cache<(String, DateTime<Utc>), ()>>,
        coverage_claim_time_cache: &CoverageClaimTimeCache,
        coverage_object_cache: &CoverageObjectCache,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing CBRS heartbeat file {}", file.file_info.key);
        let mut transaction = self.pool.begin().await?;
        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        let heartbeat_cache_clone = heartbeat_cache.clone();
        let heartbeats = file
            .into_stream(&mut transaction)
            .await?
            .map(Heartbeat::from)
            .filter(move |h| {
                let hb_cache = heartbeat_cache_clone.clone();
                let id = h.id().unwrap();
                async move { hb_cache.get(&id).await.is_none() }
            });
        process_validated_heartbeats(
            ValidatedHeartbeat::validate_heartbeats(
                &self.gateway_info_resolver,
                heartbeats,
                coverage_object_cache,
                self.max_distance_to_asserted,
                self.max_distance_to_coverage,
                &epoch,
                &self.geofence,
            ),
            heartbeat_cache,
            coverage_claim_time_cache,
            self.modeled_coverage_start,
            &self.heartbeat_sink,
            &self.seniority_sink,
            &mut transaction,
        )
        .await?;
        self.heartbeat_sink.commit().await?;
        self.seniority_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl<GIR, GFV> ManagedTask for HeartbeatDaemon<GIR, GFV>
where
    GIR: GatewayResolver,
    GFV: GeofenceValidator,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

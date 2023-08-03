use chrono::{DateTime, Duration, TimeZone, Utc};
use file_store::{FileInfo, FileStore, FileType};
use futures::{
    future::LocalBoxFuture,
    stream::{StreamExt, TryStreamExt},
};
use helium_proto::{BlockchainTokenTypeV1, Message, PriceReportV1};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use task_manager::ManagedTask;
use tokio;
use tokio::sync::{mpsc, watch};

#[derive(thiserror::Error, Debug)]
pub enum PriceTrackerError {
    #[error("invalid timestamp in price: {0}")]
    InvalidTimestamp(u64),
    #[error("price is not currently available")]
    PriceNotAvailable,
    #[error("price too old, price timestamp: {0}")]
    PriceTooOld(DateTime<Utc>),
    #[error("tokio join error")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("file store error")]
    FileStoreError(#[from] file_store::Error),
    #[error("proto decode error")]
    DecodeError(#[from] helium_proto::DecodeError),
    #[error("killed due to {0}")]
    KilledError(String),
    #[error("error sending over mpsc channel")]
    SendError(#[from] mpsc::error::SendError<String>),
}

#[derive(Clone)]
pub struct Price {
    price: u64,
    timestamp: DateTime<Utc>,
}

impl TryFrom<&PriceReportV1> for Price {
    type Error = PriceTrackerError;

    fn try_from(value: &PriceReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            price: value.price,
            timestamp: Utc
                .timestamp_opt(value.timestamp as i64, 0)
                .single()
                .ok_or_else(|| PriceTrackerError::InvalidTimestamp(value.timestamp))?,
        })
    }
}

type Prices = HashMap<BlockchainTokenTypeV1, Price>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    price_duration_minutes: u64,
    file_store: file_store::Settings,
}

impl Settings {
    fn price_duration(&self) -> Duration {
        Duration::minutes(self.price_duration_minutes as i64)
    }
}

#[derive(Clone)]
pub struct PriceTracker {
    price_duration: Duration,
    price_receiver: watch::Receiver<Prices>,
}

impl PriceTracker {
    pub async fn new(settings: &Settings) -> anyhow::Result<(Self, watch::Sender<Prices>)> {
        let (price_sender, price_receiver) = watch::channel(Prices::new());

        Ok((
            Self {
                price_duration: settings.price_duration(),
                price_receiver,
            },
            price_sender,
        ))
    }

    pub async fn price(
        &self,
        token_type: &BlockchainTokenTypeV1,
    ) -> Result<u64, PriceTrackerError> {
        let result = self
            .price_receiver
            .borrow()
            .get(token_type)
            .ok_or(PriceTrackerError::PriceNotAvailable)
            .and_then(|price| {
                if price.timestamp > Utc::now() - self.price_duration {
                    Ok(price.price)
                } else {
                    Err(PriceTrackerError::PriceTooOld(price.timestamp))
                }
            });

        result
    }
}

pub struct PriceTrackerDaemon {
    file_store: FileStore,
    price_sender: watch::Sender<Prices>,
    after: DateTime<Utc>,
}

impl ManagedTask for PriceTrackerDaemon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl PriceTrackerDaemon {
    pub async fn new(
        settings: &Settings,
        price_sender: watch::Sender<Prices>,
    ) -> anyhow::Result<Self> {
        let file_store = FileStore::from_settings(&settings.file_store).await?;
        let price_duration = settings.price_duration();
        let initial_timestamp =
            calculate_initial_prices(&file_store, price_duration, &price_sender).await?;
        Ok(Self {
            file_store,
            price_sender,
            after: initial_timestamp,
        })
    }

    async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        let mut trigger = tokio::time::interval(std::time::Duration::from_secs(30));

        loop {
            let shutdown = shutdown.clone();

            tokio::select! {
                _ = shutdown => {
                    tracing::info!("PriceTracker: shutting down");
                    break;
                }
                _ = trigger.tick() => {
                    let timestamp = process_files(&self.file_store, &self.price_sender, self.after).await?;
                    self.after = timestamp.unwrap_or(self.after);
                }
            }
        }

        Ok(())
    }
}

async fn calculate_initial_prices(
    file_store: &FileStore,
    price_duration: Duration,
    sender: &watch::Sender<Prices>,
) -> Result<DateTime<Utc>, PriceTrackerError> {
    tracing::debug!("PriceTracker: Updating initial prices");
    process_files(file_store, sender, Utc::now() - price_duration)
        .await?
        .ok_or(PriceTrackerError::PriceNotAvailable)
}

async fn process_files(
    file_store: &FileStore,
    sender: &watch::Sender<Prices>,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, PriceTrackerError> {
    file_store
        .list(FileType::PriceReport.to_str(), after, None)
        .map_err(PriceTrackerError::from)
        .and_then(|file| process_file(file_store, file, sender))
        .try_fold(None, |_old, ts| async move { Ok(Some(ts)) })
        .await
}

async fn process_file(
    file_store: &FileStore,
    file: FileInfo,
    sender: &watch::Sender<Prices>,
) -> Result<DateTime<Utc>, PriceTrackerError> {
    tracing::debug!("PriceTracker: processing pricing report file {}", file.key);
    let timestamp = file.timestamp;

    file_store
        .stream_file(file)
        .await?
        .map_err(PriceTrackerError::from)
        .and_then(|buf| async { PriceReportV1::decode(buf).map_err(PriceTrackerError::from) })
        .and_then(|report| async move {
            Price::try_from(&report).map(|price| (report.token_type(), price))
        })
        .map_err(|err| {
            tracing::warn!("PriceTracker: skipping price report due to error {err:?}");
            err
        })
        .filter_map(|result| async { result.ok() })
        .for_each(|(token_type, price)| async move {
            sender.send_modify(|prices| {
                prices.insert(token_type, price);
            });
        })
        .await;

    Ok(timestamp)
}

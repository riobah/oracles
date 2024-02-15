use crate::last_witness::LastWitness;
use futures::future::LocalBoxFuture;
use futures_util::TryFutureExt;
use helium_crypto::PublicKeyBinary;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use task_manager::ManagedTask;
use tokio::{
    sync::mpsc,
    sync::Mutex,
    time::{self, timeout, MissedTickBehavior},
};

const WRITE_INTERVAL: time::Duration = time::Duration::from_secs(60);
const CHECK_INTERVAL: time::Duration = time::Duration::from_secs(5);
pub type WitnessMap = HashMap<PublicKeyBinary, LastWitness>;

pub type MessageSender = mpsc::Sender<Vec<LastWitness>>;
pub type MessageReceiver = mpsc::Receiver<Vec<LastWitness>>;

pub struct WitnessUpdater {
    pool: PgPool,
    pub cache: Arc<Mutex<WitnessMap>>,
    receiver: MessageReceiver,
}

impl ManagedTask for WitnessUpdater {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl WitnessUpdater {
    pub async fn new(
        pool: PgPool,
    ) -> anyhow::Result<(Arc<Mutex<WitnessMap>>, MessageSender, Self)> {
        let cache = Arc::new(Mutex::new(WitnessMap::new()));
        let (sender, receiver) = mpsc::channel(5000);
        Ok((
            cache.clone(),
            sender,
            Self {
                pool,
                cache,
                receiver,
            },
        ))
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting witness updater process");
        let mut check_timer = time::interval(CHECK_INTERVAL);
        check_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut write_timer = time::interval(WRITE_INTERVAL);
        write_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = check_timer.tick() => {
                        tracing::info!("checking for updates");
                        while let Some(update) = self.check_for_updates().await {
                            self.update_cache(update).await;
                        }
                }
                _ = write_timer.tick() => {
                    tracing::info!("writing witness cache to db");
                    self.write_cache().await?;
                }
            }
        }
        tracing::info!("stopping witness updater process");
        Ok(())
    }
    pub async fn check_for_updates(&mut self) -> Option<Vec<LastWitness>> {
        match timeout(time::Duration::from_secs(2), self.receiver.recv()).await {
            Ok(Some(update)) => Some(update),
            Ok(None) => None,
            Err(_) => None,
        }
    }

    pub async fn write_cache(&mut self) -> anyhow::Result<()> {
        let mut cache = self.cache.lock().await;
        if !cache.is_empty() {
            let updates = cache.values().collect::<Vec<_>>();
            LastWitness::bulk_update_last_timestamps(&self.pool, updates).await?;
            cache.clear();
        }
        Ok(())
    }

    pub async fn update_cache(&mut self, updates: Vec<LastWitness>) {
        for update in updates {
            self.cache.lock().await.insert(update.id.clone(), update);
        }
    }
}

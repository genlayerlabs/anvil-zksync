use anvil_zksync_api_decl::EthPubSubServer;
use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::server::IdProvider;
use jsonrpsee::types::SubscriptionId;
use jsonrpsee::PendingSubscriptionSink;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Notify};
use zksync_types::H128;
use zksync_web3_decl::types::{PubSubFilter, PubSubResult};

/// Generates Ethereum-style hex subscription IDs (`0x...`).
#[derive(Debug)]
pub struct EthSubscriptionIdProvider;

impl IdProvider for EthSubscriptionIdProvider {
    fn next_id(&self) -> SubscriptionId<'static> {
        let id = H128::random();
        format!("0x{}", hex::encode(id.0)).into()
    }
}

/// Maximum number of topic positions in an Ethereum log (EVM limit).
const MAX_LOG_TOPICS: usize = 4;

/// Implements `eth_subscribe` / `eth_unsubscribe` over WebSocket.
pub struct EthPubSubNamespace {
    block_tx: broadcast::Sender<Arc<PubSubResult>>,
    log_tx: broadcast::Sender<Arc<PubSubResult>>,
    /// Notified on `anvil_reset` / `evm_revert` to terminate active subscriptions.
    reset_notify: Arc<Notify>,
}

impl EthPubSubNamespace {
    pub fn new(
        block_tx: broadcast::Sender<Arc<PubSubResult>>,
        log_tx: broadcast::Sender<Arc<PubSubResult>>,
        reset_notify: Arc<Notify>,
    ) -> Self {
        Self {
            block_tx,
            log_tx,
            reset_notify,
        }
    }
}

#[async_trait::async_trait]
impl EthPubSubServer for EthPubSubNamespace {
    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        sub_type: String,
        filter: Option<PubSubFilter>,
    ) -> SubscriptionResult {
        match sub_type.as_str() {
            "newHeads" => {
                let sink = pending.accept().await?;
                let mut rx = self.block_tx.subscribe();
                let reset = self.reset_notify.clone();
                tokio::spawn(async move {
                    let closed = sink.closed();
                    tokio::pin!(closed);
                    loop {
                        tokio::select! {
                            _ = &mut closed => break,
                            _ = reset.notified() => break,
                            result = rx.recv() => {
                                match result {
                                    Ok(header) => {
                                        let msg = jsonrpsee::SubscriptionMessage::from_json(&*header)
                                            .expect("PubSubResult is serializable");
                                        if sink.send_timeout(msg, Duration::from_secs(5)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        tracing::warn!("newHeads subscription lagged, dropped {n} events — closing");
                                        break;
                                    }
                                    Err(broadcast::error::RecvError::Closed) => break,
                                }
                            }
                        }
                    }
                });
                Ok(())
            }
            "logs" => {
                // Validate topic count before accepting
                if let Some(ref f) = filter {
                    if let Some(ref topics) = f.topics {
                        if topics.len() > MAX_LOG_TOPICS {
                            pending
                                .reject(jsonrpsee::types::ErrorObject::owned(
                                    -32602,
                                    format!(
                                        "invalid params: topics length {} exceeds maximum of {MAX_LOG_TOPICS}",
                                        topics.len()
                                    ),
                                    None::<()>,
                                ))
                                .await;
                            return Ok(());
                        }
                    }
                }

                let sink = pending.accept().await?;
                let mut rx = self.log_tx.subscribe();
                let filter = filter.unwrap_or_default();
                let reset = self.reset_notify.clone();
                tokio::spawn(async move {
                    let closed = sink.closed();
                    tokio::pin!(closed);
                    loop {
                        tokio::select! {
                            _ = &mut closed => break,
                            _ = reset.notified() => break,
                            result = rx.recv() => {
                                match result {
                                    Ok(event) => {
                                        if let PubSubResult::Log(ref log) = *event {
                                            if !filter.matches(log) {
                                                continue;
                                            }
                                        }
                                        let msg = jsonrpsee::SubscriptionMessage::from_json(&*event)
                                            .expect("PubSubResult is serializable");
                                        if sink.send_timeout(msg, Duration::from_secs(5)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        tracing::warn!("logs subscription lagged, dropped {n} events — closing");
                                        break;
                                    }
                                    Err(broadcast::error::RecvError::Closed) => break,
                                }
                            }
                        }
                    }
                });
                Ok(())
            }
            other => {
                pending
                    .reject(jsonrpsee::types::ErrorObject::owned(
                        -32602,
                        format!("unsupported subscription type: {other}"),
                        None::<()>,
                    ))
                    .await;
                Ok(())
            }
        }
    }
}

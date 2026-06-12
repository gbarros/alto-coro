use alto_types::Block;
use bytes::Bytes;
use celestia_client::tx::TxConfig;
use celestia_client::types::{nmt::Namespace, Blob as CelestiaBlob};
use celestia_client::Client;
use celestia_grpc::{Error as CelestiaGrpcError, GrpcClient, TxInfo};
use commonware_codec::DecodeExt;
use commonware_cryptography::{ed25519, Digestible};
use commonware_runtime::tokio as runtime_tokio;
use coro::backend::CelestiaClientBackend;
use coro::single_sequencer::SingleSequencer;
use coro::{ArchivedBatch, BatchCursor, BatchNumber, BlobCommitment, BlobRef};
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinSet, time};
use tracing::{info, warn};

use crate::celestia::celestia_namespace;
use crate::model::{
    advance_submitted_block, decode_block_batch, decode_block_payloads, next_block, BatchMeta,
    BlockBuilder, BlockMeta, ChainState, EngineError,
};
use crate::util::{hex, hex29, hex32, now_millis};

pub(crate) type AltoSequencer =
    SingleSequencer<runtime_tokio::Context, CelestiaClientBackend, BlockBuilder>;

#[derive(Clone)]
pub(crate) struct SoftBatch {
    sequence: BatchNumber,
    payload: Bytes,
    payload_hash: [u8; 32],
    metadata: BatchMeta,
    blocks: Vec<SoftBatchBlock>,
    soft_confirmed_at: u64,
}

impl SoftBatch {
    fn try_from_archived(batch: ArchivedBatch<BatchMeta>) -> Result<Self, EngineError> {
        let block_payloads = decode_block_payloads(batch.payload.clone())?;
        let blocks = block_payloads
            .iter()
            .cloned()
            .map(Block::decode)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| EngineError::Decode(error.to_string()))?;
        let blocks = blocks
            .into_iter()
            .zip(block_payloads)
            .map(|(block, payload)| {
                let metadata = BlockMeta {
                    height: block.height.get(),
                    digest: block.digest(),
                    timestamp: block.timestamp,
                };
                SoftBatchBlock { metadata, payload }
            })
            .collect();
        Ok(Self {
            sequence: batch.sequence,
            payload: batch.payload,
            payload_hash: batch.payload_hash,
            metadata: batch.metadata,
            blocks,
            soft_confirmed_at: now_millis(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct SoftBatchBlock {
    pub(crate) metadata: BlockMeta,
    pub(crate) payload: Bytes,
}

#[derive(Clone)]
pub(crate) struct SoftCommit {
    pub(crate) tx_hash: String,
    pub(crate) pfb_broadcasted_at_ms: u64,
    pub(crate) celestia_committed_at_ms: u64,
    pub(crate) celestia_block_time_ms: Option<u64>,
    pub(crate) publish_latency_ms: Option<u64>,
    pub(crate) backend_commit_latency_ms: u64,
    pub(crate) batch_wait_ms: u64,
    pub(crate) soft_to_pfb_broadcast_ms: u64,
    pub(crate) broadcast_latency_ms: u64,
    pub(crate) confirmation_wait_ms: u64,
}

#[derive(Clone)]
pub(crate) struct SoftCelestiaCommitter {
    grpc: GrpcClient,
    header_client: Arc<Client>,
    header_time_cache: Arc<tokio::sync::Mutex<HashMap<u64, u64>>>,
    namespace: coro::NamespaceId,
    celestia_namespace: Namespace,
    tx_config: TxConfig,
}

impl SoftCelestiaCommitter {
    pub(crate) fn new(
        grpc: GrpcClient,
        header_client: Arc<Client>,
        namespace: coro::NamespaceId,
        tx_config: TxConfig,
    ) -> Self {
        let celestia_namespace =
            celestia_namespace(namespace).expect("invalid Celestia namespace for soft publisher");
        Self {
            grpc,
            header_client,
            header_time_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            namespace,
            celestia_namespace,
            tx_config,
        }
    }
}

#[derive(Default)]
pub(crate) struct SoftStatusState {
    blocks: BTreeMap<u64, SoftBlock>,
}

#[derive(Default)]
pub(crate) struct SoftStatusIndex {
    state: tokio::sync::Mutex<SoftStatusState>,
}

impl SoftStatusIndex {
    pub(crate) async fn archive(&self, batch: SoftBatch) {
        let mut state = self.state.lock().await;
        for block in batch.blocks {
            state.blocks.insert(
                block.metadata.height,
                SoftBlock {
                    sequence: batch.sequence,
                    metadata: block.metadata,
                    payload: block.payload,
                    soft_confirmed_at: batch.soft_confirmed_at,
                    commit: None,
                },
            );
        }
    }

    pub(crate) async fn publish_height(&self, height: u64, commit: SoftCommit) {
        let mut state = self.state.lock().await;
        if let Some(block) = state.blocks.get_mut(&height) {
            block.commit = Some(commit);
        }
    }

    pub(crate) async fn soft_block(&self, height: u64) -> Option<SoftBlock> {
        self.state.lock().await.blocks.get(&height).cloned()
    }

    pub(crate) async fn commit_height(&self, height: u64) -> Option<SoftCommit> {
        self.state
            .lock()
            .await
            .blocks
            .get(&height)
            .and_then(|block| block.commit.clone())
    }

    pub(crate) async fn archived_head(&self) -> Option<u64> {
        self.state.lock().await.blocks.keys().next_back().copied()
    }

    pub(crate) async fn published_head(&self) -> Option<u64> {
        self.state
            .lock()
            .await
            .blocks
            .iter()
            .rev()
            .find_map(|(height, block)| block.commit.as_ref().map(|_| *height))
    }

    pub(crate) async fn batch_for_height(&self, height: u64) -> Option<BatchNumber> {
        self.state
            .lock()
            .await
            .blocks
            .get(&height)
            .map(|block| block.sequence)
    }

    pub(crate) async fn block_payload(&self, height: u64) -> Option<Bytes> {
        let state = self.state.lock().await;
        state.blocks.get(&height).map(|block| block.payload.clone())
    }
}

#[derive(Clone)]
pub(crate) struct SoftBlock {
    pub(crate) sequence: BatchNumber,
    pub(crate) metadata: BlockMeta,
    pub(crate) payload: Bytes,
    pub(crate) soft_confirmed_at: u64,
    pub(crate) commit: Option<SoftCommit>,
}

pub(crate) async fn restore_chain_from_archive(
    sequencer: &Arc<AltoSequencer>,
    chain: &Arc<Mutex<ChainState>>,
) {
    let head = match sequencer.archived_head().await {
        Ok(Some(head)) => head,
        Ok(None) => return,
        Err(error) => {
            warn!(error = %error, "failed to inspect Coro archive head");
            return;
        }
    };

    let mut restored_batches = 0usize;
    let mut restored_blocks = 0usize;
    for sequence in 0..=head.0 {
        let sequence = BatchNumber(sequence);
        let batch = match sequencer.archived_batch(sequence).await {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                warn!(
                    sequence = sequence.0,
                    "Coro archive batch missing during chain restore"
                );
                return;
            }
            Err(error) => {
                warn!(sequence = sequence.0, error = %error, "failed to load Coro archive batch");
                return;
            }
        };
        let blocks = match decode_block_batch(batch.payload) {
            Ok(blocks) => blocks,
            Err(error) => {
                warn!(
                    sequence = sequence.0,
                    error = %error,
                    "failed to decode archived Alto block batch while restoring chain"
                );
                return;
            }
        };
        {
            let mut chain = chain.lock().expect("chain state lock poisoned");
            for block in blocks {
                let expected_height = chain.height + 1;
                if block.height.get() != expected_height || block.parent != chain.parent {
                    warn!(
                        sequence = sequence.0,
                        expected_height,
                        got_height = block.height.get(),
                        "archived Alto block does not extend restored chain"
                    );
                    return;
                }
                chain.advance(&block);
            }
        }
        restored_batches += 1;
        restored_blocks += batch.metadata.block_count as usize;
    }

    let height = chain.lock().expect("chain state lock poisoned").height;
    info!(
        sequence = head.0,
        height, restored_batches, restored_blocks, "restored Alto chain from Coro archive"
    );
}

pub(crate) async fn enqueue_unpublished_soft_batches(
    sequencer: Arc<AltoSequencer>,
    soft_status: Arc<SoftStatusIndex>,
    publish_tx: mpsc::Sender<SoftBatch>,
) {
    let sequences = match sequencer.unpublished_archived_batches().await {
        Ok(sequences) => sequences,
        Err(error) => {
            warn!(error = %error, "failed to inspect unpublished Coro archive");
            return;
        }
    };

    for sequence in sequences {
        match sequencer.archived_batch(sequence).await {
            Ok(Some(archived)) => match SoftBatch::try_from_archived(archived) {
                Ok(batch) => {
                    soft_status.archive(batch.clone()).await;
                    if publish_tx.send(batch).await.is_err() {
                        warn!("soft publish queue closed while replaying Coro archive");
                        return;
                    }
                }
                Err(error) => warn!(
                    sequence = sequence.0,
                    error = %error,
                    "unpublished Coro batch payload failed to decode"
                ),
            },
            Ok(None) => warn!(
                sequence = sequence.0,
                "unpublished Coro batch missing archive"
            ),
            Err(error) => warn!(
                sequence = sequence.0,
                error = %error,
                "failed to load unpublished Coro batch"
            ),
        }
    }
}

pub(crate) async fn archive_pending_soft_ingress(
    sequencer: Arc<AltoSequencer>,
    soft_status: Arc<SoftStatusIndex>,
    publish_tx: mpsc::Sender<SoftBatch>,
) {
    loop {
        match sequencer.flush_archive().await {
            Ok(Some(archived)) => match SoftBatch::try_from_archived(archived) {
                Ok(batch) => {
                    soft_status.archive(batch.clone()).await;
                    info!(
                        sequence = batch.sequence.0,
                        first_height = batch.metadata.first_height,
                        last_height = batch.metadata.last_height,
                        block_count = batch.metadata.block_count,
                        "archived pending Coro ingress after restart"
                    );
                    if publish_tx.send(batch).await.is_err() {
                        warn!("soft publish queue closed while archiving pending Coro ingress");
                        return;
                    }
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "pending Coro ingress archived to undecodable block batch"
                    );
                    return;
                }
            },
            Ok(None) => return,
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to archive pending Coro ingress after restart"
                );
                return;
            }
        }
    }
}

pub(crate) async fn run_soft_sequencer_loop(
    sequencer: Arc<AltoSequencer>,
    chain: Arc<Mutex<ChainState>>,
    leader: ed25519::PublicKey,
    soft_status: Arc<SoftStatusIndex>,
    publish_tx: mpsc::Sender<SoftBatch>,
    block_time: Duration,
) {
    let mut ticker = time::interval(block_time);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;

        let block = next_block(&chain, &leader);
        match sequencer.submit(block.clone()).await {
            Ok(()) => {
                advance_submitted_block(&chain, &block);
                match sequencer.archive_ready().await {
                    Ok(Some(archived)) => match SoftBatch::try_from_archived(archived) {
                        Ok(batch) => {
                            soft_status.archive(batch.clone()).await;
                            info!(
                                sequence = batch.sequence.0,
                                first_height = batch.metadata.first_height,
                                last_height = batch.metadata.last_height,
                                block_count = batch.metadata.block_count,
                                payload_bytes = batch.payload.len(),
                                "soft-confirmed alto block batch"
                            );
                            if publish_tx.send(batch).await.is_err() {
                                warn!("soft publish queue closed");
                                return;
                            }
                        }
                        Err(error) => warn!(
                            error = %error,
                            "soft sequencer archived undecodable block batch"
                        ),
                    },
                    Ok(None) => {}
                    Err(error) => warn!(error = %error, "soft sequencer failed to archive block"),
                }
            }
            Err(error) => {
                info!(
                    error = %error,
                    "soft sequencer rejected block trigger"
                );
            }
        }
    }
}

pub(crate) async fn run_soft_publish_loop(
    committer: SoftCelestiaCommitter,
    sequencer: Arc<AltoSequencer>,
    soft_status: Arc<SoftStatusIndex>,
    mut publish_rx: mpsc::Receiver<SoftBatch>,
    publish_concurrency: usize,
) {
    let broadcast_concurrency = publish_concurrency.max(1);
    let confirmation_concurrency = broadcast_concurrency.saturating_mul(4).max(1);
    let (confirm_tx, confirm_rx) = mpsc::channel(broadcast_concurrency.saturating_mul(64).max(1));
    let confirm_handle = tokio::spawn(run_soft_confirm_loop(confirm_rx, confirmation_concurrency));

    let mut in_flight = JoinSet::new();
    info!(
        broadcast_concurrency,
        confirmation_concurrency, "soft Celestia publish pipeline started"
    );
    while let Some(batch) = publish_rx.recv().await {
        while in_flight.len() >= broadcast_concurrency {
            if let Some(result) = in_flight.join_next().await {
                if let Err(error) = result {
                    warn!(error = %error, "soft Celestia broadcast task failed");
                }
            }
        }

        let committer = committer.clone();
        let sequencer = sequencer.clone();
        let soft_status = soft_status.clone();
        let confirm_tx = confirm_tx.clone();
        in_flight.spawn(async move {
            broadcast_soft_batch(committer, sequencer, soft_status, batch, confirm_tx).await;
        });
    }

    while let Some(result) = in_flight.join_next().await {
        if let Err(error) = result {
            warn!(error = %error, "soft Celestia broadcast task failed");
        }
    }
    drop(confirm_tx);
    if let Err(error) = confirm_handle.await {
        warn!(error = %error, "soft Celestia confirmation loop failed");
    }
}

type ConfirmFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type ConfirmTxFuture =
    Pin<Box<dyn Future<Output = Result<TxInfo, CelestiaGrpcError>> + Send + 'static>>;

async fn broadcast_soft_batch(
    committer: SoftCelestiaCommitter,
    sequencer: Arc<AltoSequencer>,
    soft_status: Arc<SoftStatusIndex>,
    batch: SoftBatch,
    confirm_tx: mpsc::Sender<ConfirmFuture>,
) {
    let sequence = batch.sequence;
    let (blob, commitment) = match celestia_blob_for_soft_batch(&committer, &batch) {
        Ok(prepared) => prepared,
        Err(error) => {
            warn!(
                sequence = sequence.0,
                error = %error,
                "failed to build Celestia blob for soft-confirmed batch"
            );
            return;
        }
    };

    loop {
        let broadcast_started_at = now_millis();
        let submitted = match committer
            .grpc
            .broadcast_blobs(&[blob.clone()], committer.tx_config.clone())
            .await
        {
            Ok(submitted) => submitted,
            Err(error) => {
                warn!(
                    sequence = sequence.0,
                    error = %error,
                    "soft-confirmed Coro batch broadcast failed; retrying"
                );
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let broadcasted_at = now_millis();
        let tx_hash = hex(submitted.tx_ref().hash.as_bytes());

        let confirm_future: ConfirmFuture = Box::pin(confirm_soft_batch(
            committer,
            sequencer,
            soft_status,
            batch,
            blob,
            commitment,
            Box::pin(submitted.confirm()),
            broadcast_started_at,
            broadcasted_at,
            tx_hash,
        ));
        if confirm_tx.send(confirm_future).await.is_err() {
            warn!(sequence = sequence.0, "soft confirmation queue closed");
        }
        return;
    }
}

async fn run_soft_confirm_loop(
    mut confirm_rx: mpsc::Receiver<ConfirmFuture>,
    confirmation_concurrency: usize,
) {
    let mut in_flight = JoinSet::new();
    while let Some(confirm_future) = confirm_rx.recv().await {
        while in_flight.len() >= confirmation_concurrency {
            if let Some(result) = in_flight.join_next().await {
                if let Err(error) = result {
                    warn!(error = %error, "soft Celestia confirmation task failed");
                }
            }
        }

        in_flight.spawn(confirm_future);
    }

    while let Some(result) = in_flight.join_next().await {
        if let Err(error) = result {
            warn!(error = %error, "soft Celestia confirmation task failed");
        }
    }
}

async fn confirm_soft_batch(
    committer: SoftCelestiaCommitter,
    sequencer: Arc<AltoSequencer>,
    soft_status: Arc<SoftStatusIndex>,
    batch: SoftBatch,
    blob: CelestiaBlob,
    commitment: BlobCommitment,
    submitted: ConfirmTxFuture,
    broadcast_started_at: u64,
    broadcasted_at: u64,
    tx_hash: String,
) {
    let mut submitted = Some(submitted);
    let mut broadcast_started_at = broadcast_started_at;
    let mut broadcasted_at = broadcasted_at;
    let mut tx_hash = tx_hash;
    loop {
        let sequence = batch.sequence;
        let Some(current_submission) = submitted.take() else {
            warn!(
                sequence = sequence.0,
                "soft confirmation task missing submitted tx"
            );
            return;
        };
        match current_submission.await {
            Ok(tx_info) => {
                let committed_at = now_millis();
                let celestia_block_time_ms = match time::timeout(
                    Duration::from_secs(2),
                    celestia_block_time_ms(&committer, tx_info.height),
                )
                .await
                {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!(
                            celestia_height = tx_info.height,
                            "timed out fetching Celestia header timestamp"
                        );
                        None
                    }
                };

                let cursor = BatchCursor {
                    sequence: batch.sequence,
                    blob_ref: BlobRef {
                        height: tx_info.height,
                        namespace: committer.namespace,
                        commitment,
                    },
                    payload_hash: batch.payload_hash,
                };
                match sequencer.record_published_cursor(cursor).await {
                    Ok(Some(_)) => {
                        for block in &batch.blocks {
                            let height = block.metadata.height;
                            let commit = SoftCommit {
                                tx_hash: tx_hash.clone(),
                                pfb_broadcasted_at_ms: broadcasted_at,
                                celestia_committed_at_ms: committed_at,
                                celestia_block_time_ms,
                                publish_latency_ms: celestia_block_time_ms.map(|timestamp| {
                                    timestamp.saturating_sub(block.metadata.timestamp)
                                }),
                                backend_commit_latency_ms: committed_at
                                    .saturating_sub(block.metadata.timestamp),
                                batch_wait_ms: broadcast_started_at
                                    .saturating_sub(batch.soft_confirmed_at),
                                soft_to_pfb_broadcast_ms: broadcasted_at
                                    .saturating_sub(batch.soft_confirmed_at),
                                broadcast_latency_ms: broadcasted_at
                                    .saturating_sub(broadcast_started_at),
                                confirmation_wait_ms: committed_at.saturating_sub(broadcasted_at),
                            };
                            soft_status.publish_height(height, commit).await;
                        }
                    }
                    Ok(None) => warn!(
                        sequence = batch.sequence.0,
                        "Celestia cursor committed for batch missing from Coro archive"
                    ),
                    Err(error) => warn!(
                        sequence = batch.sequence.0,
                        error = %error,
                        "failed to record Celestia cursor in Coro archive"
                    ),
                }

                if let (Some(first), Some(last)) = (batch.blocks.first(), batch.blocks.last()) {
                    let first_latency = celestia_block_time_ms
                        .map(|timestamp| timestamp.saturating_sub(first.metadata.timestamp));
                    let last_latency = celestia_block_time_ms
                        .map(|timestamp| timestamp.saturating_sub(last.metadata.timestamp));
                    info!(
                        sequence = batch.sequence.0,
                        first_height = first.metadata.height,
                        last_height = last.metadata.height,
                        block_count = batch.blocks.len(),
                        celestia_height = tx_info.height,
                        namespace = %hex29(committer.namespace.0),
                        tx_hash = %tx_hash,
                        commitment = %hex32(commitment.0),
                        celestia_block_time_ms = ?celestia_block_time_ms,
                        oldest_publish_latency_ms = ?first_latency,
                        newest_publish_latency_ms = ?last_latency,
                        oldest_queued_for_ms = committed_at.saturating_sub(first.metadata.timestamp),
                        newest_queued_for_ms = committed_at.saturating_sub(last.metadata.timestamp),
                        commit_latency_ms = committed_at.saturating_sub(broadcast_started_at),
                        confirmation_wait_ms = committed_at.saturating_sub(broadcasted_at),
                        "committed soft-confirmed Coro batch to Celestia"
                    );
                }
                break;
            }
            Err(error) => {
                warn!(
                    sequence = sequence.0,
                    error = %error,
                    "soft-confirmed Coro batch confirmation failed; retrying"
                );
                time::sleep(Duration::from_secs(1)).await;

                loop {
                    broadcast_started_at = now_millis();
                    let next_submitted = match committer
                        .grpc
                        .broadcast_blobs(&[blob.clone()], committer.tx_config.clone())
                        .await
                    {
                        Ok(submitted) => submitted,
                        Err(error) => {
                            warn!(
                                sequence = sequence.0,
                                error = %error,
                                "soft-confirmed Coro batch rebroadcast failed; retrying"
                            );
                            time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };
                    broadcasted_at = now_millis();
                    tx_hash = hex(next_submitted.tx_ref().hash.as_bytes());
                    submitted = Some(Box::pin(next_submitted.confirm()));
                    break;
                }
            }
        }
    }
}

fn celestia_blob_for_soft_batch(
    committer: &SoftCelestiaCommitter,
    batch: &SoftBatch,
) -> Result<(CelestiaBlob, BlobCommitment), String> {
    let blob = CelestiaBlob::new(committer.celestia_namespace, batch.payload.to_vec(), None)
        .map_err(|err| err.to_string())?;
    let commitment = BlobCommitment(*blob.commitment.hash());
    Ok((blob, commitment))
}

async fn celestia_block_time_ms(committer: &SoftCelestiaCommitter, height: u64) -> Option<u64> {
    if let Some(timestamp) = committer
        .header_time_cache
        .lock()
        .await
        .get(&height)
        .copied()
    {
        return Some(timestamp);
    }

    match committer.header_client.header().get_by_height(height).await {
        Ok(header) => {
            let nanos = header.time().unix_timestamp_nanos();
            let timestamp = u64::try_from(nanos / 1_000_000).ok()?;
            committer
                .header_time_cache
                .lock()
                .await
                .insert(height, timestamp);
            Some(timestamp)
        }
        Err(error) => {
            warn!(
                celestia_height = height,
                error = %error,
                "failed to fetch Celestia header timestamp"
            );
            None
        }
    }
}

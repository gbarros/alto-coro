use alto_types::{Block, Context, EPOCH};
use async_trait::async_trait;
use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use bytes::{Buf, BufMut, Bytes};
use celestia_client::tx::SigningKey;
use celestia_client::types::state::AccAddress;
use celestia_client::Client;
use celestia_grpc::GrpcClient;
use clap::{Parser, Subcommand};
use commonware_codec::{DecodeExt, Encode, FixedSize, Read, ReadExt, Write};
use commonware_consensus::types::{Height, Round, View};
use commonware_cryptography::{
    ed25519,
    sha256::{Digest, Sha256},
    Digest as _, Digestible, Hasher, Signer,
};
use commonware_formatting::from_hex;
use commonware_runtime::{tokio as runtime_tokio, Runner as _, Supervisor as _};
use coro::{
    backend::CelestiaClientBackend,
    single_sequencer::{
        Application, AppliedBatch, ExecutedBatch, Replica, ReplicaApplication, ReplicaConfig,
        SingleSequencer,
    },
    BatchCursor, BatchNumber, BatchPolicy, ChainConfig, PublishRequest, Publisher, PublisherConfig,
    ReaderConfig, RetryConfig, SubmissionId, VerificationMode,
};
use coro_demo::{HistoryServerConfig, HttpReplicaSource};
use serde::{Deserialize, Serialize};
use sha2::{Digest as Sha2Digest, Sha256 as Sha2};
use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, task::JoinSet, time};
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};

const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_TX_BYTES: usize = 64;
const DEFAULT_MAX_METADATA_BYTES: usize = 128;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 128;

#[derive(Debug, Parser)]
#[command(name = "alto-coro")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Generate a local secp256k1 Celestia account.
    Keygen {
        /// Optional file to write `CELESTIA_PRIVATE_KEY_HEX=<hex>`.
        #[arg(long)]
        out_env: Option<PathBuf>,
    },
    /// Run the single sequencer and publish Alto blocks to Celestia.
    Run { config: PathBuf },
    /// Run a replica that replays sequencer history and falls back to Celestia reads.
    Replica { config: PathBuf },
}

#[derive(Debug, Deserialize)]
struct SequencerFile {
    #[serde(default = "default_storage_dir")]
    storage_dir: PathBuf,
    #[serde(default = "default_partition_prefix")]
    partition_prefix: String,
    #[serde(default = "default_worker_threads")]
    worker_threads: usize,
    #[serde(default = "default_log_level")]
    log_level: String,
    #[serde(default = "default_history_listen")]
    history_listen: SocketAddr,
    #[serde(default = "default_signer_seed")]
    signer_seed: u64,
    #[serde(default)]
    private_key: Option<String>,
    #[serde(default)]
    batch: BatchConfig,
    #[serde(default)]
    celestia: CelestiaConfig,
    #[serde(default = "default_block_time_ms")]
    block_time_ms: u64,
    #[serde(default = "default_confirmation_mode")]
    confirmation_mode: ConfirmationMode,
    #[serde(default = "default_publish_queue")]
    publish_queue: usize,
    #[serde(default = "default_publish_concurrency")]
    publish_concurrency: usize,
    #[serde(default = "default_serve_payloads")]
    serve_payloads: bool,
}

#[derive(Debug, Deserialize)]
struct ReplicaFile {
    #[serde(default = "default_replica_storage_dir")]
    storage_dir: PathBuf,
    #[serde(default = "default_replica_partition_prefix")]
    partition_prefix: String,
    #[serde(default = "default_worker_threads")]
    worker_threads: usize,
    #[serde(default = "default_log_level")]
    log_level: String,
    sequencer_url: String,
    #[serde(default)]
    batch: BatchConfig,
    #[serde(default)]
    celestia: CelestiaConfig,
    #[serde(default = "default_sync_interval_ms")]
    sync_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct CelestiaConfig {
    #[serde(default = "default_env_file")]
    env_file: Option<PathBuf>,
    #[serde(default)]
    rpc_url: Option<String>,
    #[serde(default = "default_rpc_url_env")]
    rpc_url_env: Option<String>,
    #[serde(default)]
    grpc_url: Option<String>,
    #[serde(default = "default_grpc_url_env")]
    grpc_url_env: Option<String>,
    #[serde(default)]
    private_key_hex: Option<String>,
    #[serde(default)]
    private_key_file: Option<PathBuf>,
    #[serde(default = "default_private_key_env")]
    private_key_env: Option<String>,
    namespace: String,
}

impl Default for CelestiaConfig {
    fn default() -> Self {
        Self {
            env_file: default_env_file(),
            rpc_url: None,
            rpc_url_env: default_rpc_url_env(),
            grpc_url: None,
            grpc_url_env: default_grpc_url_env(),
            private_key_hex: None,
            private_key_file: None,
            private_key_env: default_private_key_env(),
            namespace: "00000000000000000000".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BatchConfig {
    #[serde(default = "default_max_txs")]
    max_txs: usize,
    #[serde(default = "default_max_payload_bytes")]
    max_payload_bytes: usize,
    #[serde(default = "default_max_tx_bytes")]
    max_tx_bytes: usize,
    #[serde(default = "default_max_metadata_bytes")]
    max_metadata_bytes: usize,
    #[serde(default = "default_max_output_bytes")]
    max_output_bytes: usize,
    #[serde(default = "default_max_delay_ms")]
    max_delay_ms: u64,
    #[serde(default = "default_readback_timeout_ms")]
    readback_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    read_timeout_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_txs: default_max_txs(),
            max_payload_bytes: default_max_payload_bytes(),
            max_tx_bytes: default_max_tx_bytes(),
            max_metadata_bytes: default_max_metadata_bytes(),
            max_output_bytes: default_max_output_bytes(),
            max_delay_ms: default_max_delay_ms(),
            readback_timeout_ms: default_readback_timeout_ms(),
            read_timeout_ms: default_read_timeout_ms(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ConfirmationMode {
    Soft,
    Canonical,
}

#[derive(Clone, Debug)]
struct ChainState {
    height: u64,
    parent: Digest,
    timestamp: u64,
}

impl ChainState {
    fn genesis() -> Self {
        let context = Context {
            round: Round::new(EPOCH, View::zero()),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::zero(), Digest::EMPTY),
        };
        let block = Block::new(
            context,
            Sha256::hash(b"alto-coro-genesis"),
            Height::zero(),
            0,
        );
        Self {
            height: 0,
            parent: block.digest(),
            timestamp: 0,
        }
    }

    fn next_block(&self, leader: ed25519::PublicKey, timestamp: u64) -> Block {
        let height = self.height + 1;
        let context = Context {
            round: Round::new(EPOCH, View::new(height)),
            leader,
            parent: (View::new(self.height), self.parent),
        };
        Block::new(
            context,
            self.parent,
            Height::new(height),
            timestamp.max(self.timestamp.saturating_add(1)),
        )
    }

    fn advance(&mut self, block: &Block) {
        self.height = block.height.get();
        self.parent = block.digest();
        self.timestamp = block.timestamp;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockMeta {
    height: u64,
    digest: Digest,
    timestamp: u64,
}

impl Write for BlockMeta {
    fn write(&self, buf: &mut impl BufMut) {
        self.height.write(buf);
        self.digest.write(buf);
        self.timestamp.write(buf);
    }
}

impl FixedSize for BlockMeta {
    const SIZE: usize = u64::SIZE + Digest::SIZE + u64::SIZE;
}

impl Read for BlockMeta {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        Ok(Self {
            height: u64::read(buf)?,
            digest: Digest::read(buf)?,
            timestamp: u64::read(buf)?,
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum EngineError {
    #[error("block decode failed: {0}")]
    Decode(String),
    #[error("block height {got} does not extend chain height {chain}")]
    HeightMismatch { chain: u64, got: u64 },
    #[error("block parent does not match chain head")]
    ParentMismatch,
    #[error("block timestamp did not advance")]
    TimestampNotAdvanced,
}

struct BlockBuilder {
    chain: Arc<Mutex<ChainState>>,
    leader: ed25519::PublicKey,
}

impl BlockBuilder {
    fn new(leader: ed25519::PublicKey) -> Self {
        Self {
            chain: Arc::new(Mutex::new(ChainState::genesis())),
            leader,
        }
    }

    fn chain(&self) -> Arc<Mutex<ChainState>> {
        self.chain.clone()
    }
}

#[async_trait]
impl Application for BlockBuilder {
    type Tx = u64;
    type Metadata = BlockMeta;
    type Error = EngineError;

    async fn execute_batch(
        &mut self,
        sequence: BatchNumber,
        _txs: Vec<Self::Tx>,
    ) -> Result<ExecutedBatch<Self::Metadata>, Self::Error> {
        let mut chain = self.chain.lock().expect("chain state lock poisoned");
        let expected_height = chain.height + 1;
        if sequence.0 + 1 != expected_height {
            return Err(EngineError::HeightMismatch {
                chain: chain.height,
                got: sequence.0 + 1,
            });
        }

        let timestamp = now_millis();
        let block = chain.next_block(self.leader.clone(), timestamp);
        let digest = block.digest();
        let payload = block.encode();
        chain.advance(&block);

        info!(
            height = block.height.get(),
            digest = ?digest,
            payload_bytes = payload.len(),
            "built alto block"
        );
        Ok(ExecutedBatch {
            payload,
            metadata: BlockMeta {
                height: block.height.get(),
                digest,
                timestamp: block.timestamp,
            },
        })
    }
}

#[derive(Clone)]
struct SoftBatch {
    sequence: BatchNumber,
    payload: Bytes,
    payload_hash: [u8; 32],
    metadata: BlockMeta,
    cursor: Option<BatchCursor>,
}

#[derive(Default)]
struct SoftHistoryState {
    batches: BTreeMap<BatchNumber, SoftBatch>,
    archived_head: Option<BatchNumber>,
    published_head: Option<BatchNumber>,
}

#[derive(Default)]
struct SoftSequencerHistory {
    state: tokio::sync::Mutex<SoftHistoryState>,
}

impl SoftSequencerHistory {
    async fn archive(&self, batch: SoftBatch) {
        let mut state = self.state.lock().await;
        state.archived_head = Some(batch.sequence);
        state.batches.insert(batch.sequence, batch);
    }

    async fn publish(&self, sequence: BatchNumber, cursor: BatchCursor) {
        let mut state = self.state.lock().await;
        if let Some(batch) = state.batches.get_mut(&sequence) {
            batch.cursor = Some(cursor);
        }

        let mut next = match state.published_head {
            Some(head) => head.0.saturating_add(1),
            None => 0,
        };
        while state
            .batches
            .get(&BatchNumber(next))
            .and_then(|batch| batch.cursor.as_ref())
            .is_some()
        {
            state.published_head = Some(BatchNumber(next));
            next = next.saturating_add(1);
        }
    }
}

#[async_trait]
impl coro_demo::SequencerHistory for SoftSequencerHistory {
    async fn head(&self) -> coro::Result<Option<BatchNumber>> {
        Ok(self.state.lock().await.published_head)
    }

    async fn archived_head(&self) -> coro::Result<Option<BatchNumber>> {
        Ok(self.state.lock().await.archived_head)
    }

    async fn cursor(&self, sequence: BatchNumber) -> coro::Result<Option<BatchCursor>> {
        Ok(self
            .state
            .lock()
            .await
            .batches
            .get(&sequence)
            .and_then(|batch| batch.cursor.clone()))
    }

    async fn status(&self, sequence: BatchNumber) -> coro::Result<Option<coro::BatchStatus>> {
        Ok(self
            .state
            .lock()
            .await
            .batches
            .get(&sequence)
            .map(|batch| match batch.cursor.clone() {
                Some(cursor) => coro::BatchStatus::Published(cursor),
                None => coro::BatchStatus::Archived,
            }))
    }

    async fn payload(&self, sequence: BatchNumber) -> coro::Result<Option<Bytes>> {
        Ok(self
            .state
            .lock()
            .await
            .batches
            .get(&sequence)
            .map(|batch| batch.payload.clone()))
    }
}

struct BlockApplier {
    chain: Arc<Mutex<ChainState>>,
}

impl BlockApplier {
    fn new() -> Self {
        Self {
            chain: Arc::new(Mutex::new(ChainState::genesis())),
        }
    }
}

#[async_trait]
impl ReplicaApplication for BlockApplier {
    type Metadata = BlockMeta;
    type Output = BlockMeta;
    type Error = EngineError;

    async fn apply_batch(
        &mut self,
        sequence: BatchNumber,
        payload: Bytes,
    ) -> Result<AppliedBatch<Self::Metadata, Self::Output>, Self::Error> {
        let block = Block::decode(payload).map_err(|err| EngineError::Decode(err.to_string()))?;
        let mut chain = self.chain.lock().expect("chain state lock poisoned");
        let expected_height = chain.height + 1;
        if block.height.get() != expected_height || sequence.0 + 1 != expected_height {
            return Err(EngineError::HeightMismatch {
                chain: chain.height,
                got: block.height.get(),
            });
        }
        if block.parent != chain.parent {
            return Err(EngineError::ParentMismatch);
        }
        if block.timestamp <= chain.timestamp {
            return Err(EngineError::TimestampNotAdvanced);
        }

        let digest = block.digest();
        chain.advance(&block);
        let meta = BlockMeta {
            height: block.height.get(),
            digest,
            timestamp: block.timestamp,
        };
        info!(height = meta.height, digest = ?meta.digest, "applied alto block");
        Ok(AppliedBatch {
            metadata: meta.clone(),
            output: meta,
        })
    }
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Keygen { out_env } => run_keygen(out_env),
        Command::Run { config } => run_sequencer(config),
        Command::Replica { config } => run_replica(config),
    };
    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run_keygen(out_env: Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let signing_key = SigningKey::random(&mut rand::rngs::OsRng);
    let private_key_hex = hex(signing_key.to_bytes().as_slice());
    let verifying_key = *signing_key.verifying_key();
    let address = AccAddress::new(verifying_key.into());

    println!("address={address}");
    println!("private_key_hex={private_key_hex}");

    if let Some(path) = out_env {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(
            path,
            format!("CELESTIA_PRIVATE_KEY_HEX={private_key_hex}\n"),
        )?;
    }
    Ok(())
}

fn run_sequencer(path: PathBuf) -> Result<(), Box<dyn Error>> {
    let config: SequencerFile = load_yaml(&path)?;
    init_tracing(&config.log_level);
    let runtime = runtime_tokio::Runner::new(
        runtime_tokio::Config::new()
            .with_storage_directory(&config.storage_dir)
            .with_worker_threads(config.worker_threads),
    );

    runtime.start(|context| async move {
        let signer = signer(&config.private_key, config.signer_seed);
        let backend = backend(&config.celestia).await;
        let chain_config = chain_config(&config.celestia, &config.batch);

        match config.confirmation_mode {
            ConfirmationMode::Soft => {
                let history = Arc::new(SoftSequencerHistory::default());
                let history_trait = history.clone() as Arc<dyn coro_demo::SequencerHistory>;
                let history_listen = config.history_listen;
                let history_handle = tokio::spawn(async move {
                    serve_history(
                        history_trait,
                        HistoryServerConfig {
                            serve_payloads: config.serve_payloads,
                        },
                        history_listen,
                    )
                    .await
                    .expect("coro history server exited");
                });
                info!(listen = %history_listen, mode = "soft", "coro history HTTP listening");

                let publisher = Publisher::new(
                    context.child("publisher"),
                    backend,
                    publisher_config(&config.partition_prefix, &chain_config, &config.batch),
                );
                let (publish_tx, publish_rx) = mpsc::channel(config.publish_queue.max(1));
                let publish_handle = tokio::spawn(run_soft_publish_loop(
                    Arc::new(publisher),
                    history.clone(),
                    publish_rx,
                    config.publish_concurrency.max(1),
                ));
                let loop_handle = tokio::spawn(run_soft_sequencer_loop(
                    BlockBuilder::new(signer.public_key()),
                    history,
                    publish_tx,
                    Duration::from_millis(config.block_time_ms),
                ));

                tokio::select! {
                    result = history_handle => error!(?result, "history HTTP task exited"),
                    result = publish_handle => error!(?result, "soft publisher task exited"),
                    result = loop_handle => error!(?result, "soft sequencer loop exited"),
                }
            }
            ConfirmationMode::Canonical => {
                let builder = BlockBuilder::new(signer.public_key());
                let chain = builder.chain();
                let sequencer = Arc::new(SingleSequencer::new(
                    context.child("sequencer"),
                    backend,
                    publisher_config(&config.partition_prefix, &chain_config, &config.batch),
                    reader_config(&chain_config, &config.batch),
                    sequencer_config(&config.partition_prefix, &config.batch),
                    builder,
                ));

                let recovery = sequencer
                    .recover()
                    .await
                    .expect("sequencer recovery failed");
                info!(
                    finalized = recovery.finalized_batches.len(),
                    pending = recovery.pending_batches.len(),
                    resumed = recovery.resumed_batches.len(),
                    "sequencer recovered"
                );

                let history = sequencer.clone() as Arc<dyn coro_demo::SequencerHistory>;
                let history_listen = config.history_listen;
                let history_handle = tokio::spawn(async move {
                    serve_history(
                        history,
                        HistoryServerConfig {
                            serve_payloads: config.serve_payloads,
                        },
                        history_listen,
                    )
                    .await
                    .expect("coro history server exited");
                });
                info!(listen = %history_listen, mode = "canonical", "coro history HTTP listening");

                let loop_handle = tokio::spawn(run_sequencer_loop(
                    sequencer.clone(),
                    chain,
                    Duration::from_millis(config.block_time_ms),
                ));

                tokio::select! {
                    result = history_handle => error!(?result, "history HTTP task exited"),
                    result = loop_handle => error!(?result, "sequencer loop exited"),
                }
            }
        }
    });
    Ok(())
}

fn run_replica(path: PathBuf) -> Result<(), Box<dyn Error>> {
    let config: ReplicaFile = load_yaml(&path)?;
    init_tracing(&config.log_level);
    let runtime = runtime_tokio::Runner::new(
        runtime_tokio::Config::new()
            .with_storage_directory(&config.storage_dir)
            .with_worker_threads(config.worker_threads),
    );

    runtime.start(|context| async move {
        let backend = backend(&config.celestia).await;
        let chain_config = chain_config(&config.celestia, &config.batch);
        let replica = Arc::new(Replica::new(
            context.child("replica"),
            backend,
            reader_config(&chain_config, &config.batch),
            replica_config(&config.partition_prefix, &config.batch),
            BlockApplier::new(),
        ));
        let recovery = replica.recover().await.expect("replica recovery failed");
        info!(
            next_sequence = recovery.next_sequence.0,
            "replica recovered"
        );

        let source = HttpReplicaSource::new(config.sequencer_url);
        let sync_interval = Duration::from_millis(config.sync_interval_ms);
        run_replica_loop(replica, source, sync_interval).await;
    });
    Ok(())
}

async fn run_sequencer_loop(
    sequencer: Arc<SingleSequencer<runtime_tokio::Context, CelestiaClientBackend, BlockBuilder>>,
    chain: Arc<Mutex<ChainState>>,
    block_time: Duration,
) {
    loop {
        let next = {
            let chain = chain.lock().expect("chain state lock poisoned");
            chain.height + 1
        };
        if let Err(error) = sequencer.submit(next).await {
            warn!(error = %error, "sequencer rejected block trigger");
            time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        match sequencer.flush().await {
            Ok(Some(cursor)) => report_cursor(&sequencer, cursor).await,
            Ok(None) => {}
            Err(error) => {
                warn!(error = %error, "sequencer flush failed");
                recover_and_report(&sequencer).await;
            }
        }
        time::sleep(block_time).await;
    }
}

#[derive(Clone)]
struct HistoryApiState {
    history: Arc<dyn coro_demo::SequencerHistory>,
    serve_payloads: bool,
}

fn history_router(
    history: Arc<dyn coro_demo::SequencerHistory>,
    config: HistoryServerConfig,
) -> Router {
    let state = HistoryApiState {
        history: history.clone(),
        serve_payloads: config.serve_payloads,
    };
    Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/head", get(history_head))
        .route("/archived-head", get(history_archived_head))
        .route("/status/{sequence}", get(history_status))
        .route("/cursor/{sequence}", get(history_cursor))
        .route("/payload/{sequence}", get(history_payload))
        .route("/block/{query}", get(block_get))
        .with_state(state)
        .layer(CorsLayer::permissive())
}

async fn serve_history(
    history: Arc<dyn coro_demo::SequencerHistory>,
    config: HistoryServerConfig,
    bind: SocketAddr,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, history_router(history, config)).await
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

#[derive(Serialize)]
struct HeadResponse {
    head: Option<u64>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum StatusResponse {
    Archived,
    Published { cursor: CursorResponse },
}

#[derive(Serialize)]
struct CursorResponse {
    sequence: u64,
    blob_ref: BlobRefResponse,
    payload_hash: String,
}

#[derive(Serialize)]
struct BlobRefResponse {
    height: u64,
    namespace: String,
    commitment: String,
}

impl From<BatchCursor> for CursorResponse {
    fn from(cursor: BatchCursor) -> Self {
        Self {
            sequence: cursor.sequence.0,
            blob_ref: BlobRefResponse {
                height: cursor.blob_ref.height,
                namespace: hex29(cursor.blob_ref.namespace.0),
                commitment: hex32(cursor.blob_ref.commitment.0),
            },
            payload_hash: hex(cursor.payload_hash.as_slice()),
        }
    }
}

impl From<coro::BatchStatus> for StatusResponse {
    fn from(status: coro::BatchStatus) -> Self {
        match status {
            coro::BatchStatus::Archived => Self::Archived,
            coro::BatchStatus::Published(cursor) => Self::Published {
                cursor: cursor.into(),
            },
        }
    }
}

async fn history_head(AxumState(state): AxumState<HistoryApiState>) -> impl IntoResponse {
    match state.history.head().await {
        Ok(head) => axum::Json(HeadResponse {
            head: head.map(|head| head.0),
        })
        .into_response(),
        Err(error) => {
            warn!(error = %error, "failed to load published head");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn history_archived_head(AxumState(state): AxumState<HistoryApiState>) -> impl IntoResponse {
    match state.history.archived_head().await {
        Ok(head) => axum::Json(HeadResponse {
            head: head.map(|head| head.0),
        })
        .into_response(),
        Err(error) => {
            warn!(error = %error, "failed to load archived head");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn history_status(
    AxumPath(sequence): AxumPath<u64>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    match state.history.status(BatchNumber(sequence)).await {
        Ok(Some(status)) => axum::Json(StatusResponse::from(status)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence, error = %error, "failed to load batch status");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn history_cursor(
    AxumPath(sequence): AxumPath<u64>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    match state.history.cursor(BatchNumber(sequence)).await {
        Ok(Some(cursor)) => axum::Json(CursorResponse::from(cursor)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence, error = %error, "failed to load batch cursor");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn history_payload(
    AxumPath(sequence): AxumPath<u64>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    if !state.serve_payloads {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.history.payload(BatchNumber(sequence)).await {
        Ok(Some(payload)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            payload,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence, error = %error, "failed to load batch payload");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn block_get(
    AxumPath(query): AxumPath<String>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    if !state.serve_payloads {
        return StatusCode::NOT_FOUND.into_response();
    }

    let sequence = match payload_sequence(&state.history, &query).await {
        Ok(Some(sequence)) => sequence,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(query, error = %error, "failed to resolve block query");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    match state.history.payload(sequence).await {
        Ok(Some(payload)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            payload,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence = sequence.0, error = %error, "failed to load block payload");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn payload_sequence(
    history: &Arc<dyn coro_demo::SequencerHistory>,
    query: &str,
) -> Result<Option<BatchNumber>, Box<dyn Error>> {
    if query == "latest" {
        return Ok(history.archived_head().await?);
    }

    if let Some(height) = parse_u64_query(query) {
        if height == 0 {
            return Ok(None);
        }
        return Ok(Some(BatchNumber(height - 1)));
    }

    Ok(None)
}

fn parse_u64_query(query: &str) -> Option<u64> {
    if let Ok(value) = query.parse::<u64>() {
        return Some(value);
    }
    let raw = from_hex(query)?;
    if raw.len() != 8 {
        return None;
    }
    let bytes: [u8; 8] = raw.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

async fn run_soft_sequencer_loop(
    mut builder: BlockBuilder,
    history: Arc<SoftSequencerHistory>,
    publish_tx: mpsc::Sender<SoftBatch>,
    block_time: Duration,
) {
    let mut sequence = BatchNumber(0);
    loop {
        match builder.execute_batch(sequence, vec![sequence.0 + 1]).await {
            Ok(executed) => {
                let payload_hash = payload_hash(executed.payload.as_ref());
                let batch = SoftBatch {
                    sequence,
                    payload: executed.payload,
                    payload_hash,
                    metadata: executed.metadata,
                    cursor: None,
                };
                history.archive(batch.clone()).await;
                info!(
                    sequence = sequence.0,
                    height = batch.metadata.height,
                    digest = ?batch.metadata.digest,
                    payload_bytes = batch.payload.len(),
                    "soft-confirmed alto block"
                );
                if publish_tx.send(batch).await.is_err() {
                    warn!("soft publish queue closed");
                    return;
                }
                sequence = BatchNumber(sequence.0.saturating_add(1));
            }
            Err(error) => warn!(error = %error, "soft sequencer failed to build block"),
        }
        time::sleep(block_time).await;
    }
}

async fn run_soft_publish_loop(
    publisher: Arc<Publisher<runtime_tokio::Context, CelestiaClientBackend>>,
    history: Arc<SoftSequencerHistory>,
    mut publish_rx: mpsc::Receiver<SoftBatch>,
    publish_concurrency: usize,
) {
    let mut in_flight = JoinSet::new();
    info!(publish_concurrency, "soft publisher pipeline started");
    while let Some(batch) = publish_rx.recv().await {
        while in_flight.len() >= publish_concurrency {
            if let Some(result) = in_flight.join_next().await {
                if let Err(error) = result {
                    warn!(error = %error, "soft publish task failed");
                }
            }
        }

        let publisher = publisher.clone();
        let history = history.clone();
        in_flight.spawn(async move {
            publish_soft_batch(publisher, history, batch).await;
        });
    }

    while let Some(result) = in_flight.join_next().await {
        if let Err(error) = result {
            warn!(error = %error, "soft publish task failed");
        }
    }
}

async fn publish_soft_batch(
    publisher: Arc<Publisher<runtime_tokio::Context, CelestiaClientBackend>>,
    history: Arc<SoftSequencerHistory>,
    batch: SoftBatch,
) {
    let queued_for = now_millis().saturating_sub(batch.metadata.timestamp);
    loop {
        let started_at = now_millis();
        match publisher
            .publish(PublishRequest {
                id: submission_id(batch.sequence, batch.payload_hash),
                payload: batch.payload.clone(),
            })
            .await
        {
            Ok(receipt) => {
                let cursor = BatchCursor {
                    sequence: batch.sequence,
                    blob_ref: receipt.blob_ref,
                    payload_hash: batch.payload_hash,
                };
                history.publish(batch.sequence, cursor.clone()).await;
                info!(
                    sequence = batch.sequence.0,
                    height = batch.metadata.height,
                    celestia_height = cursor.blob_ref.height,
                    namespace = %hex29(cursor.blob_ref.namespace.0),
                    commitment = %hex32(cursor.blob_ref.commitment.0),
                    queued_for_ms = queued_for,
                    publish_roundtrip_ms = now_millis().saturating_sub(started_at),
                    "published soft-confirmed alto block to Celestia"
                );
                break;
            }
            Err(error) => {
                warn!(
                    sequence = batch.sequence.0,
                    error = %error,
                    "soft-confirmed block publication failed; retrying"
                );
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn recover_and_report(
    sequencer: &Arc<SingleSequencer<runtime_tokio::Context, CelestiaClientBackend, BlockBuilder>>,
) {
    loop {
        time::sleep(Duration::from_secs(1)).await;
        match sequencer.recover().await {
            Ok(report) => {
                for sequence in report.resumed_batches {
                    match sequencer.batch_cursor(sequence).await {
                        Ok(Some(cursor)) => report_cursor(sequencer, cursor).await,
                        Ok(None) => warn!(sequence = sequence.0, "recovered batch has no cursor"),
                        Err(error) => {
                            warn!(sequence = sequence.0, error = %error, "cursor lookup failed")
                        }
                    }
                }
                return;
            }
            Err(error) => warn!(error = %error, "sequencer recovery failed"),
        }
    }
}

async fn report_cursor(
    sequencer: &SingleSequencer<runtime_tokio::Context, CelestiaClientBackend, BlockBuilder>,
    cursor: BatchCursor,
) {
    match sequencer.archived_batch(cursor.sequence).await {
        Ok(Some(batch)) => info!(
            sequence = cursor.sequence.0,
            height = batch.metadata.height,
            celestia_height = cursor.blob_ref.height,
            namespace = %hex29(cursor.blob_ref.namespace.0),
            commitment = %hex32(cursor.blob_ref.commitment.0),
            "published alto block to Celestia"
        ),
        Ok(None) => warn!(
            sequence = cursor.sequence.0,
            "published batch missing from archive"
        ),
        Err(error) => warn!(sequence = cursor.sequence.0, error = %error, "failed to load archive"),
    }
}

async fn run_replica_loop(
    replica: Arc<Replica<runtime_tokio::Context, CelestiaClientBackend, BlockApplier>>,
    source: HttpReplicaSource,
    interval: Duration,
) {
    loop {
        match replica.catch_up(&source).await {
            Ok(batches) if batches.is_empty() => {}
            Ok(batches) => {
                if let Some(last) = batches.last() {
                    info!(
                        applied = batches.len(),
                        height = last.output.height,
                        digest = ?last.output.digest,
                        "replica caught up"
                    );
                }
            }
            Err(error) => warn!(error = %error, "replica catch-up failed"),
        }
        time::sleep(interval).await;
    }
}

async fn backend(config: &CelestiaConfig) -> CelestiaClientBackend {
    let env_file =
        load_env_file(config.env_file.as_deref()).expect("failed to read celestia.env_file");
    let rpc_url = celestia_config_value(
        config.rpc_url.as_ref(),
        config.rpc_url_env.as_ref(),
        &env_file,
        "celestia.rpc_url",
    );
    let grpc_url = celestia_config_value(
        config.grpc_url.as_ref(),
        config.grpc_url_env.as_ref(),
        &env_file,
        "celestia.grpc_url",
    );
    let private_key_hex = private_key_hex(config, &env_file);
    let client = Client::builder()
        .rpc_url(&rpc_url)
        .grpc_url(&grpc_url)
        .private_key_hex(&private_key_hex)
        .build()
        .await
        .expect("failed to build celestia-client");
    let grpc = GrpcClient::builder()
        .url(grpc_url)
        .private_key_hex(&private_key_hex)
        .build()
        .expect("failed to build celestia-grpc client");
    CelestiaClientBackend::with_submit_client(client, grpc)
}

fn private_key_hex(config: &CelestiaConfig, env_file: &HashMap<String, String>) -> String {
    config
        .private_key_hex
        .clone()
        .or_else(|| {
            let path = config.private_key_file.as_ref()?;
            private_key_hex_from_file(path).ok()
        })
        .or_else(|| {
            let name = config.private_key_env.as_ref()?;
            env_value(name, env_file)
        })
        .expect(
            "set celestia.private_key_hex, celestia.private_key_file, or CELESTIA_PRIVATE_KEY_HEX",
        )
}

fn celestia_config_value(
    inline: Option<&String>,
    env_name: Option<&String>,
    env_file: &HashMap<String, String>,
    label: &str,
) -> String {
    inline
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| env_name.and_then(|name| env_value(name, env_file)))
        .unwrap_or_else(|| panic!("set {label} or its configured env var"))
}

fn env_value(name: &str, env_file: &HashMap<String, String>) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            env_file
                .get(name)
                .filter(|value| !value.is_empty())
                .cloned()
        })
}

fn load_env_file(path: Option<&Path>) -> Result<HashMap<String, String>, std::io::Error> {
    let Some(path) = path else {
        return Ok(HashMap::new());
    };
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let contents = std::fs::read_to_string(path)?;
    Ok(parse_env_file(&contents))
}

fn parse_env_file(contents: &str) -> HashMap<String, String> {
    contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (name, value) = line.split_once('=')?;
            let value = value.trim().trim_matches('"').trim_matches('\'');
            Some((name.trim().to_string(), value.to_string()))
        })
        .collect()
}

fn private_key_hex_from_file(path: &Path) -> Result<String, std::io::Error> {
    let contents = std::fs::read_to_string(path)?;
    let trimmed = contents.trim();
    if let Some((_, value)) = trimmed
        .lines()
        .filter_map(|line| line.split_once('='))
        .find(|(name, _)| name.trim() == "CELESTIA_PRIVATE_KEY_HEX")
    {
        let value = value.trim().trim_matches('"').trim_matches('\'');
        if !value.is_empty() {
            return Ok(value.to_string());
        }
    }
    Ok(trimmed.to_string())
}

fn signer(private_key: &Option<String>, seed: u64) -> ed25519::PrivateKey {
    let Some(private_key) = private_key else {
        return ed25519::PrivateKey::from_seed(seed);
    };
    let bytes = from_hex(private_key).expect("private_key must be hex");
    ed25519::PrivateKey::read(&mut &bytes[..]).expect("private_key must decode as ed25519")
}

fn chain_config(config: &CelestiaConfig, batch: &BatchConfig) -> ChainConfig {
    ChainConfig {
        namespace: namespace(&config.namespace),
        max_payload_bytes: batch.max_payload_bytes,
    }
}

fn namespace(value: &str) -> coro::NamespaceId {
    let bytes = from_hex(value).expect("namespace must be hex");
    let mut namespace = [0u8; 29];
    match bytes.len() {
        10 => namespace[19..].copy_from_slice(&bytes),
        29 => namespace.copy_from_slice(&bytes),
        other => {
            panic!("namespace must be 10-byte suffix or full 29-byte namespace, got {other} bytes")
        }
    }
    coro::NamespaceId(namespace)
}

fn publisher_config(prefix: &str, chain: &ChainConfig, batch: &BatchConfig) -> PublisherConfig {
    PublisherConfig {
        chain: chain.clone(),
        partition: format!("{prefix}-publisher"),
        retry: RetryConfig {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            max_attempts: Some(20),
        },
        readback_timeout: Duration::from_millis(batch.readback_timeout_ms),
        tx: Default::default(),
    }
}

fn reader_config(chain: &ChainConfig, batch: &BatchConfig) -> ReaderConfig {
    ReaderConfig {
        chain: chain.clone(),
        verification: VerificationMode::RpcIncluded,
        read_timeout: Duration::from_millis(batch.read_timeout_ms),
    }
}

fn sequencer_config(prefix: &str, batch: &BatchConfig) -> coro::SequencerConfig {
    coro::SequencerConfig {
        partition: format!("{prefix}-sequencer"),
        batch_policy: BatchPolicy {
            max_txs: batch.max_txs,
            max_payload_bytes: batch.max_payload_bytes,
            max_delay: Duration::from_millis(batch.max_delay_ms),
        },
        max_tx_bytes: batch.max_tx_bytes,
        max_metadata_bytes: batch.max_metadata_bytes,
    }
}

fn replica_config(prefix: &str, batch: &BatchConfig) -> ReplicaConfig {
    ReplicaConfig {
        partition: format!("{prefix}-replica"),
        max_payload_bytes: batch.max_payload_bytes,
        max_metadata_bytes: batch.max_metadata_bytes,
        max_output_bytes: batch.max_output_bytes,
    }
}

fn load_yaml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Box<dyn Error>> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&raw)?)
}

fn init_tracing(level: &str) {
    let level = level.parse().unwrap_or(tracing::Level::INFO);
    let _ = tracing_subscriber::fmt().with_max_level(level).try_init();
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis()
        .try_into()
        .expect("millisecond timestamp fits in u64")
}

fn hex32(value: [u8; 32]) -> String {
    hex(value.as_slice())
}

fn hex29(value: [u8; 29]) -> String {
    hex(value.as_slice())
}

fn hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn payload_hash(payload: &[u8]) -> [u8; 32] {
    Sha2::digest(payload).into()
}

fn submission_id(sequence: BatchNumber, payload_hash: [u8; 32]) -> SubmissionId {
    let mut hasher = Sha2::new();
    hasher.update(b"alto-coro.soft-sequencer.v1");
    hasher.update(sequence.0.to_be_bytes());
    hasher.update(payload_hash);
    SubmissionId(hasher.finalize().into())
}

fn default_storage_dir() -> PathBuf {
    PathBuf::from("local/alto-coro-sequencer")
}

fn default_replica_storage_dir() -> PathBuf {
    PathBuf::from("local/alto-coro-replica")
}

fn default_partition_prefix() -> String {
    "alto-coro-sequencer".to_string()
}

fn default_replica_partition_prefix() -> String {
    "alto-coro-replica".to_string()
}

const fn default_worker_threads() -> usize {
    2
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_history_listen() -> SocketAddr {
    "127.0.0.1:8081".parse().expect("valid socket")
}

const fn default_signer_seed() -> u64 {
    0
}

const fn default_max_txs() -> usize {
    1
}

const fn default_max_payload_bytes() -> usize {
    DEFAULT_MAX_PAYLOAD_BYTES
}

const fn default_max_tx_bytes() -> usize {
    DEFAULT_MAX_TX_BYTES
}

const fn default_max_metadata_bytes() -> usize {
    DEFAULT_MAX_METADATA_BYTES
}

const fn default_max_output_bytes() -> usize {
    DEFAULT_MAX_OUTPUT_BYTES
}

const fn default_max_delay_ms() -> u64 {
    1_000
}

const fn default_readback_timeout_ms() -> u64 {
    60_000
}

const fn default_read_timeout_ms() -> u64 {
    60_000
}

const fn default_block_time_ms() -> u64 {
    500
}

const fn default_sync_interval_ms() -> u64 {
    1_000
}

const fn default_serve_payloads() -> bool {
    true
}

const fn default_confirmation_mode() -> ConfirmationMode {
    ConfirmationMode::Soft
}

const fn default_publish_queue() -> usize {
    1024
}

const fn default_publish_concurrency() -> usize {
    8
}

fn default_env_file() -> Option<PathBuf> {
    Some(PathBuf::from(".env"))
}

fn default_rpc_url_env() -> Option<String> {
    Some("CELESTIA_RPC_URL".to_string())
}

fn default_grpc_url_env() -> Option<String> {
    Some("CELESTIA_GRPC_URL".to_string())
}

fn default_private_key_env() -> Option<String> {
    Some("CELESTIA_PRIVATE_KEY_HEX".to_string())
}

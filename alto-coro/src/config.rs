use crate::celestia::namespace;
use coro::single_sequencer::ReplicaConfig;
use coro::{
    BatchPolicy, ChainConfig, PublisherConfig, ReaderConfig, RetryConfig, VerificationMode,
};
use serde::Deserialize;
use std::{
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_TX_BYTES: usize = 4096;
const DEFAULT_MAX_METADATA_BYTES: usize = 128;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 128;

#[derive(Debug, Deserialize)]
pub(crate) struct SequencerFile {
    #[serde(default = "default_storage_dir")]
    pub(crate) storage_dir: PathBuf,
    #[serde(default = "default_partition_prefix")]
    pub(crate) partition_prefix: String,
    #[serde(default = "default_worker_threads")]
    pub(crate) worker_threads: usize,
    #[serde(default = "default_log_level")]
    pub(crate) log_level: String,
    #[serde(default = "default_history_listen")]
    pub(crate) history_listen: SocketAddr,
    #[serde(default = "default_signer_seed")]
    pub(crate) signer_seed: u64,
    #[serde(default)]
    pub(crate) private_key: Option<String>,
    #[serde(default)]
    pub(crate) batch: BatchConfig,
    #[serde(default)]
    pub(crate) celestia: CelestiaConfig,
    #[serde(default = "default_block_time_ms")]
    pub(crate) block_time_ms: u64,
    #[serde(default = "default_confirmation_mode")]
    pub(crate) confirmation_mode: ConfirmationMode,
    #[serde(default = "default_publish_queue")]
    pub(crate) publish_queue: usize,
    #[serde(default = "default_publish_concurrency")]
    pub(crate) publish_concurrency: usize,
    #[serde(default = "default_serve_payloads")]
    pub(crate) serve_payloads: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReplicaFile {
    #[serde(default = "default_replica_storage_dir")]
    pub(crate) storage_dir: PathBuf,
    #[serde(default = "default_replica_partition_prefix")]
    pub(crate) partition_prefix: String,
    #[serde(default = "default_worker_threads")]
    pub(crate) worker_threads: usize,
    #[serde(default = "default_log_level")]
    pub(crate) log_level: String,
    pub(crate) sequencer_url: String,
    #[serde(default)]
    pub(crate) batch: BatchConfig,
    #[serde(default)]
    pub(crate) celestia: CelestiaConfig,
    #[serde(default = "default_sync_interval_ms")]
    pub(crate) sync_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CelestiaConfig {
    #[serde(default = "default_env_file")]
    pub(crate) env_file: Option<PathBuf>,
    #[serde(default)]
    pub(crate) rpc_url: Option<String>,
    #[serde(default = "default_rpc_url_env")]
    pub(crate) rpc_url_env: Option<String>,
    #[serde(default)]
    pub(crate) grpc_url: Option<String>,
    #[serde(default = "default_grpc_url_env")]
    pub(crate) grpc_url_env: Option<String>,
    #[serde(default)]
    pub(crate) private_key_hex: Option<String>,
    #[serde(default)]
    pub(crate) private_key_file: Option<PathBuf>,
    #[serde(default = "default_private_key_env")]
    pub(crate) private_key_env: Option<String>,
    pub(crate) namespace: String,
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
pub(crate) struct BatchConfig {
    #[serde(default = "default_max_txs")]
    pub(crate) max_txs: usize,
    #[serde(default = "default_max_payload_bytes")]
    pub(crate) max_payload_bytes: usize,
    #[serde(default = "default_max_tx_bytes")]
    pub(crate) max_tx_bytes: usize,
    #[serde(default = "default_max_metadata_bytes")]
    pub(crate) max_metadata_bytes: usize,
    #[serde(default = "default_max_output_bytes")]
    pub(crate) max_output_bytes: usize,
    #[serde(default = "default_max_delay_ms")]
    pub(crate) max_delay_ms: u64,
    #[serde(default = "default_readback_timeout_ms")]
    pub(crate) readback_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    pub(crate) read_timeout_ms: u64,
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
pub(crate) enum ConfirmationMode {
    Soft,
    Canonical,
}

pub(crate) fn chain_config(config: &CelestiaConfig, batch: &BatchConfig) -> ChainConfig {
    ChainConfig {
        namespace: namespace(&config.namespace),
        max_payload_bytes: batch.max_payload_bytes,
    }
}

pub(crate) fn publisher_config(
    prefix: &str,
    chain: &ChainConfig,
    batch: &BatchConfig,
) -> PublisherConfig {
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

pub(crate) fn reader_config(chain: &ChainConfig, batch: &BatchConfig) -> ReaderConfig {
    ReaderConfig {
        chain: chain.clone(),
        verification: VerificationMode::RpcIncluded,
        read_timeout: Duration::from_millis(batch.read_timeout_ms),
    }
}

pub(crate) fn sequencer_config(prefix: &str, batch: &BatchConfig) -> coro::SequencerConfig {
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

pub(crate) fn replica_config(prefix: &str, batch: &BatchConfig) -> ReplicaConfig {
    ReplicaConfig {
        partition: format!("{prefix}-replica"),
        max_payload_bytes: batch.max_payload_bytes,
        max_metadata_bytes: batch.max_metadata_bytes,
        max_output_bytes: batch.max_output_bytes,
    }
}

pub(crate) fn load_yaml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Box<dyn Error>> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&raw)?)
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

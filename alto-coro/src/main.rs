mod celestia;
mod config;
mod history;
mod model;
mod soft;
mod util;

use clap::{Parser, Subcommand};
use std::sync::Mutex;
use std::{error::Error, path::PathBuf, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time};
use tracing::{error, info, warn};

use celestia_client::tx::{SigningKey, TxConfig};
use celestia_client::types::state::AccAddress;
use commonware_cryptography::{ed25519, Signer};
use commonware_runtime::{tokio as runtime_tokio, Runner as _, Supervisor as _};
use coro::backend::CelestiaClientBackend;
use coro::single_sequencer::{Replica, SingleSequencer};
use coro::BatchCursor;
use coro_demo::{HistoryServerConfig, HttpReplicaSource};

use crate::celestia::{celestia_clients, read_only_backend};
use crate::config::chain_config;
use crate::config::{
    load_yaml, publisher_config, reader_config, replica_config, sequencer_config, ConfirmationMode,
    ReplicaFile, SequencerFile,
};
use crate::history::serve_history;
use crate::model::{advance_submitted_block, next_block, BlockApplier, BlockBuilder, ChainState};
use crate::soft::{
    archive_pending_soft_ingress, enqueue_unpublished_soft_batches, restore_chain_from_archive,
    run_soft_publish_loop, run_soft_sequencer_loop, SoftCelestiaCommitter, SoftStatusIndex,
};
use crate::util::{hex, hex29, hex32, init_tracing, signer};

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
        let celestia = celestia_clients(&config.celestia).await;
        let chain_config = chain_config(&config.celestia, &config.batch);

        match config.confirmation_mode {
            ConfirmationMode::Soft => {
                let leader = signer.public_key();
                let builder = BlockBuilder::new();
                let chain = builder.chain();
                let producer_chain = Arc::new(Mutex::new(ChainState::genesis()));
                let sequencer = Arc::new(SingleSequencer::new(
                    context.child("sequencer"),
                    celestia.backend,
                    publisher_config(&config.partition_prefix, &chain_config, &config.batch),
                    reader_config(&chain_config, &config.batch),
                    sequencer_config(&config.partition_prefix, &config.batch),
                    builder,
                ));
                restore_chain_from_archive(&sequencer, &chain).await;
                restore_chain_from_archive(&sequencer, &producer_chain).await;

                let soft_status = Arc::new(SoftStatusIndex::default());
                let history = sequencer.clone() as Arc<dyn coro_demo::SequencerHistory>;
                let history_soft_status = Some(soft_status.clone());
                let history_listen = config.history_listen;
                let history_handle = tokio::spawn(async move {
                    serve_history(
                        history,
                        history_soft_status,
                        HistoryServerConfig {
                            serve_payloads: config.serve_payloads,
                        },
                        history_listen,
                    )
                    .await
                    .expect("coro history server exited");
                });
                info!(listen = %history_listen, mode = "soft", "coro history HTTP listening");

                let committer = SoftCelestiaCommitter::new(
                    celestia.grpc.clone(),
                    celestia.header.clone(),
                    chain_config.namespace,
                    TxConfig::default(),
                );
                let (publish_tx, publish_rx) = mpsc::channel(config.publish_queue.max(1));
                let publish_handle = tokio::spawn(run_soft_publish_loop(
                    committer,
                    sequencer.clone(),
                    soft_status.clone(),
                    publish_rx,
                    config.publish_concurrency.max(1),
                ));
                enqueue_unpublished_soft_batches(
                    sequencer.clone(),
                    soft_status.clone(),
                    publish_tx.clone(),
                )
                .await;
                archive_pending_soft_ingress(
                    sequencer.clone(),
                    soft_status.clone(),
                    publish_tx.clone(),
                )
                .await;
                restore_chain_from_archive(&sequencer, &producer_chain).await;
                let loop_handle = tokio::spawn(run_soft_sequencer_loop(
                    sequencer,
                    producer_chain,
                    leader,
                    soft_status,
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
                let leader = signer.public_key();
                let builder = BlockBuilder::new();
                let chain = builder.chain();
                let producer_chain = Arc::new(Mutex::new(ChainState::genesis()));
                let sequencer = Arc::new(SingleSequencer::new(
                    context.child("sequencer"),
                    celestia.backend,
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
                restore_chain_from_archive(&sequencer, &chain).await;
                restore_chain_from_archive(&sequencer, &producer_chain).await;

                let history = sequencer.clone() as Arc<dyn coro_demo::SequencerHistory>;
                let history_listen = config.history_listen;
                let history_handle = tokio::spawn(async move {
                    serve_history(
                        history,
                        None,
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
                    producer_chain,
                    leader,
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
        let backend = read_only_backend(&config.celestia).await;
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
    leader: ed25519::PublicKey,
    block_time: Duration,
) {
    let mut ticker = time::interval(block_time);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let block = next_block(&chain, &leader);
        if let Err(error) = sequencer.submit(block.clone()).await {
            warn!(error = %error, "sequencer rejected block trigger");
            time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        advance_submitted_block(&chain, &block);

        match sequencer.flush().await {
            Ok(Some(cursor)) => report_cursor(&sequencer, cursor).await,
            Ok(None) => {}
            Err(error) => {
                warn!(error = %error, "sequencer flush failed");
                recover_and_report(&sequencer).await;
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
            first_height = batch.metadata.first_height,
            last_height = batch.metadata.last_height,
            block_count = batch.metadata.block_count,
            celestia_height = cursor.blob_ref.height,
            namespace = %hex29(cursor.blob_ref.namespace.0),
            commitment = %hex32(cursor.blob_ref.commitment.0),
            "published alto block batch to Celestia"
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

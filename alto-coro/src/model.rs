use alto_types::{Block, Context, EPOCH};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use commonware_codec::{DecodeExt, Encode, FixedSize, Read, ReadExt, Write};
use commonware_consensus::types::{Height, Round, View};
use commonware_cryptography::{
    ed25519,
    sha256::{Digest, Sha256},
    Digest as _, Digestible, Hasher, Signer,
};
use coro::single_sequencer::{Application, AppliedBatch, ExecutedBatch, ReplicaApplication};
use coro::BatchNumber;
use std::sync::{Arc, Mutex};
use tracing::info;

use crate::util::now_millis;

pub(crate) struct ChainState {
    pub(crate) height: u64,
    pub(crate) parent: Digest,
    pub(crate) timestamp: u64,
}

impl ChainState {
    pub(crate) fn genesis() -> Self {
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

    pub(crate) fn next_block(&self, leader: ed25519::PublicKey, timestamp: u64) -> Block {
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

    pub(crate) fn advance(&mut self, block: &Block) {
        self.height = block.height.get();
        self.parent = block.digest();
        self.timestamp = block.timestamp;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlockMeta {
    pub(crate) height: u64,
    pub(crate) digest: Digest,
    pub(crate) timestamp: u64,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BatchMeta {
    pub(crate) first_height: u64,
    pub(crate) last_height: u64,
    pub(crate) block_count: u64,
    pub(crate) first_digest: Digest,
    pub(crate) last_digest: Digest,
    pub(crate) first_timestamp: u64,
    pub(crate) last_timestamp: u64,
}

impl Write for BatchMeta {
    fn write(&self, buf: &mut impl BufMut) {
        self.first_height.write(buf);
        self.last_height.write(buf);
        self.block_count.write(buf);
        self.first_digest.write(buf);
        self.last_digest.write(buf);
        self.first_timestamp.write(buf);
        self.last_timestamp.write(buf);
    }
}

impl FixedSize for BatchMeta {
    const SIZE: usize = (u64::SIZE * 5) + (Digest::SIZE * 2);
}

impl Read for BatchMeta {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        Ok(Self {
            first_height: u64::read(buf)?,
            last_height: u64::read(buf)?,
            block_count: u64::read(buf)?,
            first_digest: Digest::read(buf)?,
            last_digest: Digest::read(buf)?,
            first_timestamp: u64::read(buf)?,
            last_timestamp: u64::read(buf)?,
        })
    }
}

pub(crate) fn encode_block_batch(blocks: &[Block]) -> Bytes {
    let mut payload = Vec::new();
    (blocks.len() as u64).write(&mut payload);
    for block in blocks {
        let encoded = block.encode();
        (encoded.len() as u64).write(&mut payload);
        payload.extend_from_slice(&encoded);
    }
    payload.into()
}

pub(crate) fn decode_block_payloads(payload: Bytes) -> Result<Vec<Bytes>, EngineError> {
    let mut reader = payload.as_ref();
    let count = u64::read(&mut reader).map_err(|error| EngineError::Decode(error.to_string()))?;
    let count = usize::try_from(count).map_err(|_| {
        EngineError::Decode("encoded Alto block batch count does not fit usize".into())
    })?;
    let mut blocks = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u64::read(&mut reader).map_err(|error| EngineError::Decode(error.to_string()))?;
        let len = usize::try_from(len).map_err(|_| {
            EngineError::Decode("encoded Alto block length does not fit usize".into())
        })?;
        if reader.remaining() < len {
            return Err(EngineError::Decode(
                "encoded Alto block batch ended early".into(),
            ));
        }
        blocks.push(reader.copy_to_bytes(len));
    }
    if reader.has_remaining() {
        return Err(EngineError::Decode(
            "encoded Alto block batch has trailing bytes".into(),
        ));
    }
    Ok(blocks)
}

pub(crate) fn decode_block_batch(payload: Bytes) -> Result<Vec<Block>, EngineError> {
    decode_block_payloads(payload)?
        .into_iter()
        .map(|payload| {
            Block::decode(payload).map_err(|error| EngineError::Decode(error.to_string()))
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EngineError {
    #[error("block decode failed: {0}")]
    Decode(String),
    #[error("block height {got} does not extend chain height {chain}")]
    HeightMismatch { chain: u64, got: u64 },
    #[error("block parent does not match chain head")]
    ParentMismatch,
    #[error("block timestamp did not advance")]
    TimestampNotAdvanced,
    #[error("empty Alto block batch")]
    EmptyBatch,
}

pub(crate) struct BlockBuilder {
    pub(crate) chain: Arc<Mutex<ChainState>>,
}

impl BlockBuilder {
    pub(crate) fn new() -> Self {
        Self {
            chain: Arc::new(Mutex::new(ChainState::genesis())),
        }
    }

    pub(crate) fn chain(&self) -> Arc<Mutex<ChainState>> {
        self.chain.clone()
    }
}

pub(crate) fn next_block(chain: &Arc<Mutex<ChainState>>, leader: &ed25519::PublicKey) -> Block {
    let chain = chain.lock().expect("chain state lock poisoned");
    chain.next_block(leader.clone(), now_millis())
}

pub(crate) fn advance_submitted_block(chain: &Arc<Mutex<ChainState>>, block: &Block) {
    let mut chain = chain.lock().expect("chain state lock poisoned");
    if block.height.get() == chain.height + 1 && block.parent == chain.parent {
        chain.advance(block);
    }
}

#[async_trait]
impl Application for BlockBuilder {
    type Tx = Block;
    type Metadata = BatchMeta;
    type Error = EngineError;

    async fn execute_batch(
        &mut self,
        sequence: BatchNumber,
        txs: Vec<Self::Tx>,
    ) -> Result<ExecutedBatch<Self::Metadata>, Self::Error> {
        if txs.is_empty() {
            return Err(EngineError::EmptyBatch);
        }

        let mut blocks = Vec::with_capacity(txs.len());
        let mut chain = self.chain.lock().expect("chain state lock poisoned");
        for block in txs {
            let expected_height = chain.height + 1;
            if block.height.get() != expected_height {
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
            info!(
                sequence = sequence.0,
                height = block.height.get(),
                digest = ?digest,
                "built alto block"
            );
            blocks.push(block);
        }

        let first = blocks.first().expect("checked non-empty batch");
        let last = blocks.last().expect("checked non-empty batch");
        let metadata = BatchMeta {
            first_height: first.height.get(),
            last_height: last.height.get(),
            block_count: blocks.len() as u64,
            first_digest: first.digest(),
            last_digest: last.digest(),
            first_timestamp: first.timestamp,
            last_timestamp: last.timestamp,
        };
        let payload = encode_block_batch(&blocks);

        info!(
            sequence = sequence.0,
            first_height = metadata.first_height,
            last_height = metadata.last_height,
            block_count = metadata.block_count,
            payload_bytes = payload.len(),
            "built alto block batch"
        );
        Ok(ExecutedBatch { payload, metadata })
    }
}

pub(crate) struct BlockApplier {
    pub(crate) chain: Arc<Mutex<ChainState>>,
}

impl BlockApplier {
    pub(crate) fn new() -> Self {
        Self {
            chain: Arc::new(Mutex::new(ChainState::genesis())),
        }
    }
}

#[async_trait]
impl ReplicaApplication for BlockApplier {
    type Metadata = BatchMeta;
    type Output = BlockMeta;
    type Error = EngineError;

    async fn apply_batch(
        &mut self,
        sequence: BatchNumber,
        payload: Bytes,
    ) -> Result<AppliedBatch<Self::Metadata, Self::Output>, Self::Error> {
        let blocks = decode_block_batch(payload)?;
        if blocks.is_empty() {
            return Err(EngineError::EmptyBatch);
        }
        let mut chain = self.chain.lock().expect("chain state lock poisoned");
        let mut first_meta = None;
        let mut last_meta = None;
        let block_count = blocks.len() as u64;
        for block in blocks {
            let expected_height = chain.height + 1;
            if block.height.get() != expected_height {
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
            info!(
                sequence = sequence.0,
                height = meta.height,
                digest = ?meta.digest,
                "applied alto block"
            );
            if first_meta.is_none() {
                first_meta = Some(meta.clone());
            }
            last_meta = Some(meta);
        }

        let first = first_meta.expect("checked non-empty block batch");
        let output = last_meta.expect("checked non-empty block batch");
        let metadata = BatchMeta {
            first_height: first.height,
            last_height: output.height,
            block_count,
            first_digest: first.digest,
            last_digest: output.digest,
            first_timestamp: first.timestamp,
            last_timestamp: output.timestamp,
        };
        Ok(AppliedBatch { metadata, output })
    }
}

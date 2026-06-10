# alto-coro

`alto-coro` is a minimal Alto proof-of-concept that replaces Alto's Simplex
validator network with [Coro](https://github.com/celestiaorg/coro) sequencing
and Celestia blob publication.

The demo path is intentionally small:

- one sequencer builds simple `alto_types::Block` payloads,
- soft-confirmed blocks are held in memory and served immediately,
- the soft path batches encoded blocks as Celestia blobs in the background,
- the sequencer serves Coro history over HTTP,
- one or more replicas follow that history and verify Alto block continuity.

This is not a multi-validator Alto network. It is a single-writer DA-backed PoC
for quickly testing Alto-shaped blocks on Celestia Mocha.

## Architecture

```text
alto-coro sequencer
  -> builds Alto block
  -> stores in-memory soft-confirmation archive
  -> commits rolling batches of blobs to Celestia in the background
  -> serves Coro history and raw Alto block compatibility endpoints

alto-coro replica
  -> polls sequencer history
  -> fetches payload from history server or Celestia exact refs
  -> checks parent/height/timestamp continuity
```

By default `/head` is the canonical published head. `/archived-head` exposes
the soft-confirmed head, which can run ahead of Celestia publication.

The history server also exposes the subset of the original Alto indexer API
that can be answered honestly in this PoC:

- `GET /health`
- `GET /block/latest`
- `GET /block/<height>`

Those block endpoints return raw encoded `alto_types::Block` payloads. They do
not fabricate Simplex notarization or finalization certificates.

The `alto-coro` executable does not start:

- Simplex consensus,
- Alto validator p2p,
- threshold BLS certificates,
- the Alto indexer upload path.

## Configuration

Example configs live in [`examples/`](./examples).

Mocha configs read connection details from `.env`. The sequencer also needs a
funded Celestia private key for PFB submission. Replicas are read-only and only
need the Celestia RPC endpoint plus the shared namespace.

The `.env` file is ignored by git and must not be committed.

Create it from the example:

```sh
cp alto-coro/.env.example .env
```

Then fill in:

```sh
CELESTIA_RPC_URL=<celestia-node-rpc-url>
CELESTIA_GRPC_URL=<celestia-consensus-grpc-url>
CELESTIA_PRIVATE_KEY_HEX=<funded-mocha-private-key-hex>
```

`CELESTIA_RPC_URL` is the celestia-node JSON-RPC endpoint used for blob
readback. `CELESTIA_GRPC_URL` is the consensus gRPC endpoint used by the
sequencer for PayForBlobs. `CELESTIA_PRIVATE_KEY_HEX` is only required by the
sequencer.

The sequencer config includes submit credentials:

```yaml
celestia:
  env_file: .env
  rpc_url_env: CELESTIA_RPC_URL
  grpc_url_env: CELESTIA_GRPC_URL
  private_key_env: CELESTIA_PRIVATE_KEY_HEX
  namespace: 0000008e5f679bf7116c
```

Replica configs use the same namespace but do not need gRPC or a private key:

```yaml
celestia:
  env_file: .env
  rpc_url_env: CELESTIA_RPC_URL
  namespace: 0000008e5f679bf7116c
```

The namespace can be either a 10-byte hex suffix or a full 29-byte Celestia
namespace.

The demo sequencer defaults to soft confirmations:

```yaml
confirmation_mode: soft
block_time_ms: 500
publish_queue: 1024
publish_concurrency: 8
publish_batch_max_blocks: 4
publish_batch_max_delay_ms: 1500
```

Set `confirmation_mode: canonical` to restore the older behavior where each
block waits for Celestia publication/readback before the next block is produced.

## Generate a Mocha Account

Generate a local Celestia secp256k1 key:

```sh
cargo run -p alto-coro -- keygen --out-env local/alto-coro-mocha.env
```

The command prints the public Celestia address and writes the private key to an
ignored local file. Copy the generated `CELESTIA_PRIVATE_KEY_HEX` value into
`.env`, then fund the printed address with Mocha TIA.

## Run the Sequencer

From the repository root:

```sh
cargo run -p alto-coro -- run alto-coro/examples/sequencer-mocha.yaml
```

The sequencer listens on `127.0.0.1:8081` by default and logs each successful
publication with:

- Alto sequence,
- Alto height,
- Celestia height,
- namespace,
- blob commitment.

The soft-confirmation interval is controlled by:

```yaml
block_time_ms: 500
```

In `soft` mode, block production is no longer gated by Celestia block time until
the background publication queue fills. Soft publication uses a rolling Celestia
batch window: it collects pending Alto blocks until either
`publish_batch_max_blocks` is reached or `publish_batch_max_delay_ms` elapses,
then submits those blocks as separate blobs in one PFB transaction. The default
settings target roughly 4 Alto blocks per Mocha block at a 500ms soft interval
while keeping the oldest-block batch wait below the expected Mocha block time.
`publish_concurrency` controls how many batch transactions can be waiting for
Celestia commit at once. Canonical publication still depends on Celestia
submit/readback latency.

Publication logs include:

- `oldest_publish_latency_ms` / `newest_publish_latency_ms`: time from Alto
  block timestamp to the Mocha block timestamp that committed the PFB
  transaction, when the Celestia header is available,
- `oldest_queued_for_ms` / `newest_queued_for_ms`: backend-observed time from
  Alto block timestamp until the Celestia transaction commit response,
- `commit_latency_ms`: backend-observed time spent broadcasting and waiting for
  Celestia commit.

The history `GET /status/<sequence>` response also includes backend timing in
soft mode:

- `soft.soft_latency_ms`: block timestamp to local soft archive insert,
- `commit.batch_wait_ms`: local soft confirmation until the batch publisher
  starts broadcasting the PFB transaction,
- `commit.soft_to_pfb_broadcast_ms`: local soft confirmation until PFB
  broadcast acceptance, which is the explorer's Coro `Submit Delay` metric,
- `commit.publish_latency_ms`: block timestamp to Mocha block timestamp,
- `commit.backend_commit_latency_ms`: block timestamp to backend-observed commit
  response, used as a fallback if the Mocha header timestamp is unavailable.

In this temporary soft path, `soft.soft_latency_ms` is expected to be very low
because block construction and local archive insertion happen in the same loop.
`Submit Delay` is the more useful tuning metric until the default path moves to
Coro-owned durable archived batches.

Current `soft` mode is ephemeral demo mode. Its soft and published history are
kept in memory, so a sequencer restart forgets that history and starts again
from sequence `0`. For clean demo runs after restarting the sequencer, restart
replicas with fresh local storage too. Durable restart semantics belong to the
future Coro-owned archive/publish split.

## Run a Replica

In a second terminal:

```sh
cargo run -p alto-coro -- replica alto-coro/examples/replica-mocha.yaml
```

The replica polls:

```yaml
sequencer_url: http://127.0.0.1:8081
```

Multiple replicas can follow the same sequencer. Give each replica a distinct
`storage_dir` and `partition_prefix`. Replicas currently follow canonical
published head, not soft head. Replicas use a read-only Celestia client and do
not require `CELESTIA_GRPC_URL` or `CELESTIA_PRIVATE_KEY_HEX`.

Do not run multiple sequencers against the same namespace as peers. This PoC has
no multi-writer consensus or fork choice; use one active sequencer, or give
independent sequencers separate namespaces.

## Run the Explorer

The Alto explorer has a dedicated Coro mode that talks to the sequencer history
server on `127.0.0.1:8081`.

In a third terminal:

```sh
cd explorer
npm ci
REACT_APP_MODE=coro npm start
```

Open `http://localhost:3000`.

Coro mode keeps the original explorer for the pieces that still match Alto
blocks, but changes the live feed semantics:

- **Soft** means the block is in the sequencer's local archive.
- **Published** means the block's blob transaction is committed on Celestia and
  the sequencer has a height, namespace, and commitment for it. The soft path no
  longer waits for blob readback before reporting this state.

In Coro mode the explorer uses backend-provided timings. `Submit Delay` is the
time from local soft confirmation until PFB broadcast acceptance. Publish
latency prefers the Mocha block timestamp metric (`commit.publish_latency_ms`)
rather than the browser's polling time.

It does not display Simplex seeds, notarizations, or finalizations because this
PoC does not produce those artifacts.

## Reset Local State

Fresh storage is recommended between incompatible config or code changes:

```sh
rm -rf local/alto-coro-sequencer local/alto-coro-replica
```

## Troubleshooting

`401` while building `celestia-client` means the RPC provider rejected
`CELESTIA_RPC_URL`. Check that the endpoint token is current and the endpoint is
enabled.

TLS or connection errors before an HTTP status usually mean the provider
endpoint is paused, unavailable, or not reachable from the current network.

If the sequencer publishes but the replica does not advance, confirm that:

- the sequencer is still running,
- `sequencer_url` points at the sequencer history server,
- sequencer and replica use the same namespace,
- replica storage is fresh or compatible with the current run,
- if the sequencer was restarted in `soft` mode, the replica was restarted with
  fresh local storage too.

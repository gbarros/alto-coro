# alto-coro

`alto-coro` is a minimal Alto proof-of-concept that replaces Alto's Simplex
validator network with [Coro](https://github.com/celestiaorg/coro) sequencing
and Celestia blob publication.

The demo path is intentionally small:

- one sequencer builds simple `alto_types::Block` payloads,
- soft-confirmed blocks are held in memory and served immediately,
- Coro publishes each encoded block as a Celestia blob in the background,
- the sequencer serves Coro history over HTTP,
- one or more replicas follow that history and verify Alto block continuity.

This is not a multi-validator Alto network. It is a single-writer DA-backed PoC
for quickly testing Alto-shaped blocks on Celestia Mocha.

## Architecture

```text
alto-coro sequencer
  -> builds Alto block
  -> stores in-memory soft-confirmation archive
  -> publishes blob to Celestia in the background
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

Both Mocha configs read connection details and signing material from `.env`.
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
readback. `CELESTIA_GRPC_URL` is the consensus gRPC endpoint used for
PayForBlobs.

The sequencer and replicas must use the same Celestia namespace:

```yaml
celestia:
  env_file: .env
  rpc_url_env: CELESTIA_RPC_URL
  grpc_url_env: CELESTIA_GRPC_URL
  private_key_env: CELESTIA_PRIVATE_KEY_HEX
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
the background publication queue fills. Soft publication is pipelined with
`publish_concurrency` in-flight Celestia submissions so multiple Alto block
blobs can be confirmed in the same Mocha block. Canonical publication still
depends on Celestia submit/readback latency.

Publication logs include:

- `queued_for_ms`: time from Alto block timestamp until its publish task starts,
- `publish_roundtrip_ms`: time spent broadcasting, confirming, and reading back
  that blob.

Unpublished soft-confirmed blocks are not recovered after a sequencer restart in
this PoC. Once a block is published, replicas can follow it through the
canonical published head.

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
published head, not soft head.

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
- **Published** means Coro has produced a Celestia blob reference.

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
- replica storage is fresh or compatible with the current run.

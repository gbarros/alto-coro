# alto-coro

`alto-coro` is a minimal Alto proof-of-concept that replaces Alto's Simplex
validator network with [Coro](https://github.com/celestiaorg/coro) sequencing
and Celestia blob publication.

The demo path is intentionally small:

- one sequencer builds simple `alto_types::Block` payloads,
- Coro publishes each encoded block as a Celestia blob,
- the sequencer serves Coro history over HTTP,
- one or more replicas follow that history and verify Alto block continuity.

This is not a multi-validator Alto network. It is a single-writer DA-backed PoC
for quickly testing Alto-shaped blocks on Celestia Mocha.

## Architecture

```text
alto-coro sequencer
  -> builds Alto block
  -> submits batch through Coro
  -> publishes blob to Celestia
  -> stores local archive
  -> serves /head, /cursor/:sequence, /payload/:sequence

alto-coro replica
  -> polls sequencer history
  -> fetches payload from history server or Celestia exact refs
  -> checks parent/height/timestamp continuity
```

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

The default block interval is controlled by:

```yaml
block_time_ms: 6000
```

Effective production time is `Celestia submit/readback latency + block_time_ms`.

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
`storage_dir` and `partition_prefix`.

Do not run multiple sequencers against the same namespace as peers. This PoC has
no multi-writer consensus or fork choice; use one active sequencer, or give
independent sequencers separate namespaces.

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

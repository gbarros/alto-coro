# alto-coro

`alto-coro` is the minimal Coro-backed Alto PoC path. It does not start Alto's
Simplex validator set or p2p stack. A single sequencer emits Alto blocks,
publishes each encoded block as a Celestia blob through Coro, and serves Coro
history for replicas. Replicas trust the sequencer history order and re-check
Alto block parent/height/timestamp continuity while fetching payloads from the
history server or Celestia exact refs.

## Config

Example configs are in [`examples/`](./examples).

`sequencer.yaml`:

```yaml
storage_dir: local/alto-coro-sequencer
partition_prefix: alto-coro-sequencer
history_listen: 127.0.0.1:8081
block_time_ms: 6000

celestia:
  env_file: .env
  rpc_url_env: CELESTIA_RPC_URL
  grpc_url_env: CELESTIA_GRPC_URL
  private_key_env: CELESTIA_PRIVATE_KEY_HEX
  namespace: 0000008e5f679bf7116c
```

`replica.yaml`:

```yaml
storage_dir: local/alto-coro-replica
partition_prefix: alto-coro-replica
sequencer_url: http://127.0.0.1:8081

celestia:
  env_file: .env
  rpc_url_env: CELESTIA_RPC_URL
  grpc_url_env: CELESTIA_GRPC_URL
  private_key_env: CELESTIA_PRIVATE_KEY_HEX
  namespace: 0000008e5f679bf7116c
```

The namespace can be either a 10-byte hex suffix or a full 29-byte Celestia
namespace. The key in `.env` must be funded on Mocha.

## Run

```sh
cargo run -p alto-coro -- keygen --out-env local/alto-coro-mocha.env

cat > .env <<'EOF'
CELESTIA_RPC_URL=<celestia-node-rpc-url>
CELESTIA_GRPC_URL=<celestia-consensus-grpc-url>
CELESTIA_PRIVATE_KEY_HEX=<funded-mocha-private-key-hex>
EOF

cargo run -p alto-coro -- run alto-coro/examples/sequencer-mocha.yaml
cargo run -p alto-coro -- replica alto-coro/examples/replica-mocha.yaml
```

The sequencer logs the Alto height, block digest, Celestia height, namespace,
and commitment after each successful blob publication.

Fresh storage is recommended between incompatible runs:

```sh
rm -rf local/alto-coro-sequencer local/alto-coro-replica
```

## What This Removes

The `alto-coro` executable path does not start:

- Simplex consensus,
- validator p2p,
- threshold BLS certificates,
- the Alto indexer upload path.

The payload is still an `alto_types::Block`, so it keeps Alto's block type and
basic parent/height/timestamp continuity while Coro handles sequencing, local
archive, Celestia publishing, and replica replay.

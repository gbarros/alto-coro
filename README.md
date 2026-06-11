# alto-coro

`alto-coro` is a fork of Commonware's `alto` demo that experiments with a
Celestia-backed sequencing path.

The original Alto codebase is a minimal blockchain demo built on the Commonware
stack. This fork keeps the Alto block shape and explorer experience, but adds a
small proof-of-concept path where:

- Alto blocks are produced by a single local sequencer,
- blocks are submitted into [Coro](https://github.com/celestiaorg/coro),
- Coro locally archives batches for soft confirmation,
- archived batches are published to Celestia as blobs,
- replicas follow Coro history and verify Alto block continuity.

This is intentionally not a full Alto validator network. It is a focused PoC for
testing Alto-shaped block production over Coro and Celestia DA, currently aimed
at Mocha demo runs.

For architecture, configuration, Mocha setup, replica usage, and explorer notes,
see [alto-coro/README.md](./alto-coro/README.md).

## Repository Layout

- [alto-coro](./alto-coro/README.md): Coro/Celestia sequencer and replica PoC.
- [explorer](./explorer/README.md): Alto explorer with a Coro mode.
- [types](./types/README.md): Shared Alto block types used by the PoC.
- `chain`, `client`, `deploy`, `follower`, `indexer`, `inspector`, and
  `validator`: original Alto components retained from the upstream fork.

## Dependency Note

This branch is pinned to Coro commit
`2c195362884d146c8eea79cb16a2290237c7e4f8`, which includes the
archive/publish split used by the PoC.

## Licensing

This repository preserves Alto's dual license under both the
[Apache 2.0](./LICENSE-APACHE) and [MIT](./LICENSE-MIT) licenses. You may choose
either license when employing this code.

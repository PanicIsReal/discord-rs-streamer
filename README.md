# discord-rs-streamer

Clean-room Rust foundation for Discord desktop streaming transport.

This repo is intended to replace the `discord-video-stream` portion of the current Neko stack. It does **not** modify Neko, own desktop capture, or own encode in v1. The first shipped slice focuses on the daemon contract, ingest wiring, pacing core, and transport boundaries that a clean-room Discord implementation will plug into.

## What is implemented now

- Rust workspace with the production crate boundaries
- daemon HTTP API:
  - `POST /v1/session/connect`
  - `POST /v1/stream/start`
  - `POST /v1/stream/stop`
  - `POST /v1/dave/init`
  - `POST /v1/dave/external-sender`
  - `POST /v1/dave/key-package`
  - `POST /v1/dave/proposals`
  - `POST /v1/dave/welcome`
  - `POST /v1/dave/commit`
  - `GET /v1/dave/state`
  - `GET /v1/health`
  - `GET /v1/metrics`
- Unix socket and stdin ingest primitives for H.264 Annex B and Opus elementary streams
- bounded packet pacing core with counters for drops, sends, bytes, and queue pressure
- real DAVE session control backed by the Rust MLS implementation already used in the current bridge stack
- CLI for basic operator flow against the daemon
- tests for pacing, ingest, and API lifecycle

## What is intentionally not implemented yet

- real Discord gateway login/session handling
- real voice/stage stream negotiation
- RTP/RTCP wire compatibility with Discord

Those pieces are represented as explicit traits and state machines so they can be implemented without reworking the daemon contract.

## Quick start

Run the daemon:

```bash
cargo run -p daemon
```

Inspect health:

```bash
curl http://127.0.0.1:7331/v1/health
```

Connect a prototype session:

```bash
cargo run -p cli -- session connect \
  --token demo-token \
  --guild-id 123 \
  --channel-id 456
```

Start a Unix-socket ingest source:

```bash
cargo run -p cli -- stream start \
  --source unix \
  --video-socket /tmp/discord-rs-streamer/video.sock \
  --audio-socket /tmp/discord-rs-streamer/audio.sock
```

Initialize DAVE and create a key package:

```bash
cargo run -p cli -- dave init --protocol-version 1 --user-id 42 --channel-id 7
cargo run -p cli -- dave key-package
```

## Layout

- `crates/discord_gateway`: clean-room session/gateway abstractions
- `crates/discord_transport`: pacing, queueing, transport loop
- `crates/dave`: encryption boundary
- `crates/media_ingest`: encoded-media ingest primitives and harness helpers
- `crates/daemon`: HTTP daemon and orchestration
- `crates/cli`: operator CLI
- `crates/test_harness`: mocks and test helpers
- `docs/`: architecture and clean-room notes

## Development

```bash
cargo test
cargo clippy --workspace --all-targets
```

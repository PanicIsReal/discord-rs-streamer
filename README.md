# discord-rs-streamer

`discord-rs-streamer` is a clean-room Rust Discord streaming transport with a daemon + CLI operator surface.

The current `1.0.0` release candidate supports:
- Discord session login and channel targeting
- voice/media negotiation
- Discord Go Live publishing
- Unix-socket ingest for H.264 video and Opus audio
- a high-level `play` workflow for either:
  - local media files
  - X11 + Pulse capture, including the current Neko integration path

## What ships in the current release candidate

- `discord-rs-streamer-daemon`: the long-running HTTP control plane
- `discord-rs-streamer`: the operator CLI
- file playback through `ffmpeg` adapters
- X11 desktop capture through `ffmpeg`
- Pulse audio capture through `ffmpeg`
- DAVE session handling
- container packaging for the Neko integration stack

The quickest way to understand the surface area is the operator guide in [`docs/operator-guide.md`](docs/operator-guide.md).

## Quick start

Stream a local file:

```bash
export DISCORD_TOKEN=...
export DISCORD_GUILD_ID=...
export DISCORD_CHANNEL_ID=...

cargo run -p cli -- play \
  --input /path/to/video.mp4
```

Stream an X11 desktop with Pulse audio:

```bash
export DISCORD_TOKEN=...
export DISCORD_GUILD_ID=...
export DISCORD_CHANNEL_ID=...

cargo run -p cli -- play \
  --x11-display :99.0 \
  --pulse-source audio_output.monitor
```

Inspect daemon health:

```bash
curl http://127.0.0.1:7331/v1/health
curl http://127.0.0.1:7331/v1/media/health
```

## Security and publishing hygiene

- Do not commit live `.env` files or personal tokens.
- The CLI accepts `--token`, but production use should prefer environment injection or container env wiring so secrets do not appear in process arguments.
- Local logs, release zips, and other operator artifacts are intentionally ignored by [`.gitignore`](.gitignore).
- The Neko integration repo keeps the token in local `.env`; it is not expected to be committed.

## Layout

- `crates/discord_gateway`: Discord session and gateway abstractions
- `crates/discord_voice`: voice gateway and WebRTC publisher/session logic
- `crates/discord_transport`: packet pacing and transport metrics
- `crates/dave`: DAVE session control
- `crates/media_ingest`: encoded-media ingest primitives
- `crates/daemon`: daemon HTTP API and orchestration
- `crates/cli`: CLI, ingest helpers, and `play` workflow
- `docs/`: operator, architecture, and clean-room notes
- `docker/`: container entrypoint wiring

## Development

```bash
cargo test
cargo clippy --workspace --all-targets -- -D warnings
```

# Operator Guide

## Overview

`discord-rs-streamer` has two binaries:

- `discord-rs-streamer-daemon`
- `discord-rs-streamer`

The daemon owns Discord session state, voice/media negotiation, DAVE state, ingest servers, and health/metrics endpoints.

The CLI is the operator surface. The normal command is `play`. The rest of the commands exist for inspection, debugging, or manual orchestration.

## Normal operator flow

For normal use, run one of these:

```bash
export DISCORD_TOKEN=...
export DISCORD_GUILD_ID=...
export DISCORD_CHANNEL_ID=...

discord-rs-streamer play \
  --input /path/to/file.mp4
```

```bash
export DISCORD_TOKEN=...
export DISCORD_GUILD_ID=...
export DISCORD_CHANNEL_ID=...

discord-rs-streamer play \
  --x11-display :99.0 \
  --pulse-source audio_output.monitor
```

`play` will:

1. Start or attach to the daemon.
2. Connect the Discord session.
3. Connect the media publisher.
4. Open Unix ingest sockets on the daemon.
5. Spawn `ffmpeg`.
6. Feed H.264 video and Ogg/Opus audio into the daemon.
7. Shut everything down cleanly on EOF or Ctrl+C.

Environment fallback is supported for:

- `DISCORD_TOKEN`
- `DISCORD_GUILD_ID`
- `DISCORD_CHANNEL_ID`

Prefer environment injection over `--token` in production so the token does not appear in process arguments or shell history.

## What the daemon does

The daemon exposes these routes:

- `POST /v1/session/connect`
- `GET /v1/session/media`
- `POST /v1/voice/connect`
- `GET /v1/voice/health`
- `GET /v1/voice/state`
- `POST /v1/voice/disconnect`
- `POST /v1/media/connect`
- `GET /v1/media/health`
- `POST /v1/media/disconnect`
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

In practical terms, the daemon is the long-running control plane. It keeps the live Discord and media state in memory while the CLI drives it.

## CLI commands

### `play`

The product command.

Use it when you want the CLI to handle session connect, media connect, ingest setup, `ffmpeg`, and cleanup for you.

Supported sources:

- `--input <file>`
- `--x11-display <display>`

Relevant flags:

- `--pulse-source <name>`
- `--no-audio`
- `--loop`
- `--stream-kind go-live|camera`
- `--daemon-url`
- `--daemon-bind`
- `--ffmpeg-bin`
- `--fps`
- `--width`
- `--height`
- `--video-bitrate-kbps`
- `--audio-bitrate-kbps`
- `--x264-preset`
- `--keyframe-interval-seconds`

### `session connect`

Logs into Discord and binds the daemon to a guild/channel/stream kind.

### `session media`

Returns the Discord media session metadata currently known by the daemon.

### `voice connect`

Connects only the voice gateway layer.

### `voice health`

Shows voice gateway health.

### `voice state`

Shows the current voice session state object.

### `voice disconnect`

Closes the voice session.

### `media connect`

Negotiates the media publisher and gets the stream ready to accept video/audio.

### `media health`

Shows the most useful live streaming debug state:

- publisher connection state
- track-open flags
- frame counters
- last send error
- `streaming_announced`

### `media disconnect`

Closes the active media session.

### `stream start`

Starts ingest inside the daemon.

This is the low-level way to tell the daemon where media will arrive from:

- Unix sockets
- stdin

### `stream stop`

Stops the active ingest stream.

### `ingest send-framed`

Writes framed chunks to a Unix ingest socket.

### `ingest send-h264-annex-b`

Parses H.264 Annex B access units and forwards them to a Unix ingest socket.

### `ingest send-ogg-opus`

Reads Ogg/Opus and forwards packets to a Unix ingest socket.

### `dave init`

Initializes DAVE state explicitly.

### `dave state`

Shows current DAVE state.

### `dave key-package`

Returns the current DAVE key package.

### `health`

Returns overall daemon health.

### `metrics`

Returns transport, ingest, and publisher metrics.

## Health checks

The main runtime checks are:

```bash
curl http://127.0.0.1:7331/v1/health
curl http://127.0.0.1:7331/v1/media/health
```

Healthy stream indicators:

- `status = "ok"`
- `discord.loggedIn = true`
- `discord.joinedVoice = true`
- `discord.streaming = true`
- `media.publisher.connection_state = "Connected"`
- `media.session_state.streaming_announced = true`
- increasing `audio_frames_sent` and `video_frames_sent`

## Neko integration

The active Neko integration uses the same `play` command inside Docker.

The container path is:

- mount Neko's X11 socket
- mount Neko's Pulse socket
- run `discord-rs-streamer play --x11-display :99.0 --pulse-source audio_output.monitor ...`

This means the current integration uses a stock Neko image plus runtime wiring. It does not require a custom Neko source fork for the Rust streaming path.

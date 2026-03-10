# Changelog

All notable changes to this project will be documented in this file.

## [1.0.1] - 2026-03-10

### Added

- Media profile negotiation from the CLI into `/v1/media/connect` so Discord media setup uses the active playback settings instead of fixed defaults.
- Timed framed-ingest metadata for capture time, duration, and keyframe signaling across the CLI-to-daemon media path.
- Bounded WebRTC publish scheduling with startup buffering, overload drop accounting, and publish diagnostics in `/v1/media/health` and `/v1/metrics`.
- Gateway reconnect and invalid-session recovery for Discord gateway and voice/media session flows, including stream replay after recovered joins.
- Gateway event recording and expanded operator documentation for live protocol debugging.
- Persistent project tracking in `TODO.md` for implemented work, deferred follow-ups, and completion history.

### Changed

- RTP timestamps are now derived from media timing instead of sequence-based stepping.
- Live X11 and Pulse playback now use a shared monotonic media timeline between audio and video forwarding.
- Voice/media negotiation now advertises the Discord codec and stream-control details required by the current Go Live flow.
- Stream health only reports ready when full transport details are present, including stream RTC server state.
- Video publishing now reacts to Discord `MEDIA_SINK_WANTS` hints, including explicit pause/resume for zero-quality requests.

### Fixed

- Go Live streams now send the missing Discord `SPEAKING` signal after media negotiation so active streaming is announced correctly.
- Startup playback no longer silently drops the first decodable video burst while waiting for tracks to become ready.
- Voice-state churn, stream restart, voice-server rotation, and transient websocket disconnects now recover automatically instead of leaving the bot stuck in-channel but not streaming.
- Audio capture now forces low-latency Ogg flushing so Opus packets are emitted in small pages instead of large buffered bursts that caused heartbeat-style playback.
- Untimed raw and legacy framed ingest now fall back to duration-stepped RTP timestamps so compatibility senders do not pin media timestamps at zero.
- Gateway event recorder now redacts session and token secrets before writing debug traces to disk.

### Deferred

- Full RTCP-driven adaptive bitrate control.
- Single-process `ffmpeg` capture if shared-clock pacing needs further simplification later.
- Broader Discord recovery for stream-key rotation and stream-token refresh edge cases once real event traces are captured.

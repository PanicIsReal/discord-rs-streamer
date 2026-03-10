# TODO

## Implemented

- Daemon and CLI control flow for Discord session, voice/media connect, ingest, and cleanup
- Unix-socket ingest for H.264 video and Opus audio
- Discord media negotiation and WebRTC publisher flow
- X11 video capture and Pulse audio capture through `ffmpeg`
- Health and metrics endpoints for daemon, voice, media, and transport state
- Timed framed-ingest metadata for capture time, duration, and keyframe signaling
- Media profile wiring from `play` into `/v1/media/connect`
- Shared monotonic media timeline for CLI audio and video forwarding
- RTP timestamp generation from media timing instead of packet sequence
- Bounded WebRTC publish queue with startup buffering and overload drop accounting
- Live `ffmpeg` cadence tightening for FPS, GOP, repeat headers, and `20 ms` Opus frames
- Publish diagnostics in `/v1/media/health` and `/v1/metrics`
- Codec/timing simplification sweep started
- Daemon-level regression tests for startup buffering, burst trimming, and scheduler ordering
- Ignored simulated ten-minute scheduler soak coverage
- Operator documentation for media profile negotiation and publish metrics
- Ready-transition scheduler coverage and mixed audio/video jitter metric assertions
- Voice gateway reconnect/resume attempts with reconnect counters and heartbeat RTT tracking
- Automatic media-session renegotiation after stalled startup/publisher recovery failures
- Discord gateway websocket reconnect/resume for transient session drops
- Discord gateway invalid-session handling for both resumable retry and fresh-identify voice-state rejoin
- Discord stream restart replay after invalid-session fallback and recovered voice rejoin
- Discord voice-state churn recovery with automatic rejoin plus stale voice/stream session invalidation on refresh events
- Discord voice-server rotation recovery with automatic stream restart and stale stream-session invalidation
- Discord stream-session health now requires full stream transport details, including `rtc_server_id`
- Optional gateway event recorder for live Discord trace capture and deferred recovery analysis
- Container entrypoint waits for Neko X11 and Pulse sockets and retries transient `play` startup failures instead of crashing the stack
- Daemon media-session setup now retries stream establishment in-process before failing the overall play workflow
- Voice WebRTC negotiation now advertises the full Discord codec set and clock rates used by the known-good bridge
- Stream media sessions now send Discord `SPEAKING` control updates for Go Live audio sharing so the live stream is explicitly marked active after media negotiation
- Voice session health now preserves Discord auxiliary gateway payloads like `MEDIA_SINK_WANTS`, `FLAGS`, and `PLATFORM` instead of discarding them as unsupported opcodes
- Daemon publish scheduling now applies Discord `MEDIA_SINK_WANTS` video quality hints to live video cadence and exposes the active sink policy in media health/metrics
- Zero-quality `MEDIA_SINK_WANTS` now explicitly pauses Discord video signaling and resumes on the next keyframe when sink demand returns
- CLI audio capture now forces low-latency Ogg page flushing so Opus packets are emitted in 20 ms pages instead of large muxer bursts
- Untimed raw and legacy framed ingest now fall back to duration-stepped RTP timestamps instead of pinning timestamps at zero
- Gateway event recorder now redacts session and token secrets before writing debug traces to disk

## Deferred Follow-Ups

- Full RTCP-driven adaptive bitrate control
- Single-process `ffmpeg` capture if shared-clock pacing still shows material drift
- Broader Discord session recovery across full stream-key rotation and Discord-side stream token refresh edge cases

## Completion Log (latest: 2026-03-10)

- Added persistent optimization tracking so completed and deferred playback work remains explicit.
- Completed timing and negotiation correctness changes across CLI ingest, media connect, and RTP timestamping.
- Added bounded WebRTC publish scheduling, startup buffering, drop accounting, and publish health metrics.
- Tightened live capture encoder cadence and added regression coverage for timed ingest metadata.
- Added scheduler burst/ordering coverage, an ignored simulated soak test, and operator-guide updates for the new media profile flow.
- Added ready-transition and mixed jitter metric scheduler assertions, clearing the remaining active checkpoint.
- Added voice gateway reconnect/resume attempts plus health reporting for reconnect count and heartbeat RTT.
- Added daemon-side media-session renegotiation when startup buffering stalls on a dead publisher.
- Added Discord gateway websocket reconnect/resume for transient disconnects, with end-to-end local gateway coverage.
- Added Discord gateway invalid-session handling for resumable retry plus fresh identify and automatic voice-state rejoin.
- Added automatic stream restart replay after invalid-session fallback once voice rejoin recovers.
- Added automatic voice rejoin and stream restart after unexpected leave events, and cleared stale voice/stream session resources during refresh churn.
- Added automatic stream restart after voice-server rotation and cleared stale stream-session resources during voice transport refresh.
- Tightened stream-session health so the daemon only reports streaming when full stream transport details are present.
- Added an opt-in gateway event recorder so real Discord event sequences can be captured for the remaining deferred recovery cases.
- Hardened the container entrypoint so Neko-backed deployments wait for X11/Pulse readiness and retry transient Discord startup failures in-process.
- Added in-daemon media-session establishment retries so Discord stream setup can settle without tearing down the full play process between attempts.
- Aligned voice codec advertisement with the known-good bridge by sending the full Discord codec list plus expected clock rates during WebRTC negotiation.
- Added Go Live `SPEAKING` control signaling and verified the redeployed `thedock` container reaches live `discord_state: "streaming"` with active audio/video frame counters.
- Preserved Discord voice `MEDIA_SINK_WANTS`/auxiliary opcode payloads in session health and verified the live `thedock` stream exposes `media_sink_wants` without regressing active streaming.
- Applied Discord `MEDIA_SINK_WANTS` to live video scheduling, redeployed on `thedock`, and verified sink-policy metrics are exposed while the stream remains healthy.
- Added explicit video pause/resume handling for zero-quality sink requests, including keyframe-gated resume, and redeployed it on `thedock` with live `sink_video_paused` visibility.
- Forced low-latency Ogg audio flushing in the CLI playback pipeline so Pulse/file Opus capture no longer batches into the muxer's default one-second pages.
- Restored compatibility for untimed ingest senders by falling back to default RTP duration stepping when capture timestamps are absent.
- Redacted voice/session secrets in gateway event recorder output so live trace capture stays useful without persisting raw tokens.

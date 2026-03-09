# Clean-Room Rules

- Do not translate JavaScript implementation details from `discord-video-stream`.
- Use only:
  - runtime traces from our own deployments
  - packet captures from our own sessions
  - public RTP/RTCP/WebRTC references
  - protocol notes written in this repo
- Keep protocol observations and implementation separated.
- Treat the existing Node stack as a behavior oracle, not a source port.

## Phase checklist

1. Capture and document session transitions.
2. Capture and document media packet expectations.
3. Implement state machines against those notes.
4. Validate behavior using replay and soak tests.

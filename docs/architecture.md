# Architecture

## Objective

Provide a clean-room Rust transport daemon that can replace the current Node-based Discord streaming layer without changing Neko's Go code or the browser-control flow.

## Current implementation slice

The repo currently implements the stable outer shell:

- daemon control API
- ingest sources
- pacing core
- transport abstraction
- gateway abstraction
- DAVE abstraction

This allows the system to be integrated and performance-tested before the Discord protocol implementation is complete.

## Boundaries

### Outside the repo

- Neko hosting and browser control
- X11/Pulse capture
- H.264/Opus encode

### Inside the repo

- ingest acceptance for encoded media
- stream lifecycle
- queueing and pacing
- transport metrics
- clean-room Discord gateway/transport interfaces

## Migration target

The existing sidecar should eventually hand H.264 and Opus streams to this daemon over Unix sockets, then drop the `discord-video-stream` and `davey` patching path completely.

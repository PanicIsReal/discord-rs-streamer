FROM rust:1.92-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN cargo build --release -p daemon -p cli --features webrtc-media

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        ffmpeg \
        libstdc++6 \
        openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/daemon /usr/local/bin/discord-rs-streamer-daemon
COPY --from=builder /app/target/release/discord-rs-streamer /usr/local/bin/discord-rs-streamer
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh \
    && ln -sf /usr/local/bin/discord-rs-streamer /usr/local/bin/discord-rs-streamer-cli

ENV DISCORD_RS_STREAMER_BIND=0.0.0.0:7331

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

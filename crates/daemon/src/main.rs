use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use daemon::{DaemonController, build_router};
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let bind =
        std::env::var("DISCORD_RS_STREAMER_BIND").unwrap_or_else(|_| "127.0.0.1:7331".to_owned());
    let addr: SocketAddr = bind.parse().context("invalid DISCORD_RS_STREAMER_BIND")?;
    let listener = TcpListener::bind(addr).await?;
    let controller = Arc::new(DaemonController::prototype());

    tracing::info!(%addr, "discord-rs-streamer daemon listening");
    axum::serve(listener, build_router(controller)).await?;
    Ok(())
}

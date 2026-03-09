use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Video,
    Audio,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet {
    pub kind: MediaKind,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub payload: Bytes,
}

impl Packet {
    pub fn size_bytes(&self) -> usize {
        self.payload.len()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PacerConfig {
    pub bytes_per_second: usize,
    pub window_ms: u64,
    pub max_queue_packets: usize,
}

impl PacerConfig {
    pub const DEFAULT_BYTES_PER_SECOND: usize = 4_000_000;
    pub const DEFAULT_WINDOW_MS: u64 = 5;
    pub const DEFAULT_MAX_QUEUE_PACKETS: usize = 1_024;
}

impl Default for PacerConfig {
    fn default() -> Self {
        Self {
            bytes_per_second: Self::DEFAULT_BYTES_PER_SECOND,
            window_ms: Self::DEFAULT_WINDOW_MS,
            max_queue_packets: Self::DEFAULT_MAX_QUEUE_PACKETS,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct TransportMetrics {
    pub sent_packets: u64,
    pub dropped_packets: u64,
    pub sent_bytes: u64,
    pub queue_depth: usize,
    pub queue_high_watermark: usize,
    pub last_send_latency_ms: u64,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport queue is full")]
    QueueFull,
    #[error("transport loop is closed")]
    Closed,
    #[error("{0}")]
    Sink(String),
}

#[async_trait]
pub trait PacketSink: Send + Sync + 'static {
    async fn send(&self, packet: Packet) -> Result<(), TransportError>;
}

#[async_trait]
impl<T> PacketSink for Arc<T>
where
    T: PacketSink + ?Sized,
{
    async fn send(&self, packet: Packet) -> Result<(), TransportError> {
        (**self).send(packet).await
    }
}

#[derive(Debug, Default)]
pub struct NullPacketSink;

#[async_trait]
impl PacketSink for NullPacketSink {
    async fn send(&self, _packet: Packet) -> Result<(), TransportError> {
        Ok(())
    }
}

#[derive(Debug)]
struct MetricState {
    sent_packets: AtomicU64,
    dropped_packets: AtomicU64,
    sent_bytes: AtomicU64,
    queue_depth: AtomicUsize,
    queue_high_watermark: AtomicUsize,
    last_send_latency_ms: AtomicU64,
}

impl Default for MetricState {
    fn default() -> Self {
        Self {
            sent_packets: AtomicU64::new(0),
            dropped_packets: AtomicU64::new(0),
            sent_bytes: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
            queue_high_watermark: AtomicUsize::new(0),
            last_send_latency_ms: AtomicU64::new(0),
        }
    }
}

impl MetricState {
    fn snapshot(&self) -> TransportMetrics {
        TransportMetrics {
            sent_packets: self.sent_packets.load(Ordering::Relaxed),
            dropped_packets: self.dropped_packets.load(Ordering::Relaxed),
            sent_bytes: self.sent_bytes.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_high_watermark: self.queue_high_watermark.load(Ordering::Relaxed),
            last_send_latency_ms: self.last_send_latency_ms.load(Ordering::Relaxed),
        }
    }

    fn queue_inc(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.queue_high_watermark
            .fetch_max(depth, Ordering::Relaxed);
    }

    fn queue_dec(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct PacedPacketSender<S: ?Sized> {
    sender: mpsc::Sender<Packet>,
    join_handle: Option<JoinHandle<Result<(), TransportError>>>,
    metrics: Arc<MetricState>,
    _sink: Arc<S>,
}

impl<S> PacedPacketSender<S>
where
    S: PacketSink + ?Sized,
{
    pub fn spawn(config: PacerConfig, sink: Arc<S>) -> Self {
        let (sender, receiver) = mpsc::channel(config.max_queue_packets);
        let metrics = Arc::new(MetricState::default());
        let join_handle = tokio::spawn(run_transport_loop(
            config,
            receiver,
            Arc::clone(&sink),
            Arc::clone(&metrics),
        ));

        Self {
            sender,
            join_handle: Some(join_handle),
            metrics,
            _sink: sink,
        }
    }

    pub fn try_enqueue(&self, packet: Packet) -> Result<(), TransportError> {
        self.metrics.queue_inc();
        match self.sender.try_send(packet) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.queue_dec();
                self.metrics.dropped_packets.fetch_add(1, Ordering::Relaxed);
                match err {
                    mpsc::error::TrySendError::Full(_) => Err(TransportError::QueueFull),
                    mpsc::error::TrySendError::Closed(_) => Err(TransportError::Closed),
                }
            }
        }
    }

    pub fn metrics(&self) -> TransportMetrics {
        self.metrics.snapshot()
    }

    pub async fn close(mut self) -> Result<(), TransportError> {
        drop(self.sender);
        let Some(join_handle) = self.join_handle.take() else {
            return Ok(());
        };
        match join_handle.await {
            Ok(result) => result,
            Err(err) => Err(TransportError::Sink(err.to_string())),
        }
    }
}

impl<S> Clone for PacedPacketSender<S>
where
    S: ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            join_handle: None,
            metrics: Arc::clone(&self.metrics),
            _sink: Arc::clone(&self._sink),
        }
    }
}

async fn run_transport_loop<S>(
    config: PacerConfig,
    mut receiver: mpsc::Receiver<Packet>,
    sink: Arc<S>,
    metrics: Arc<MetricState>,
) -> Result<(), TransportError>
where
    S: PacketSink + ?Sized,
{
    let bytes_per_second = config.bytes_per_second as f64;
    let mut budget = bytes_per_second;
    let max_budget = bytes_per_second;
    let mut last_tick = Instant::now();

    while let Some(packet) = receiver.recv().await {
        metrics.queue_dec();

        budget = refill_budget(budget, max_budget, &mut last_tick, config.bytes_per_second);

        let packet_size = packet.size_bytes() as f64;
        if budget < packet_size {
            let bytes_short = packet_size - budget;
            let minimum_wait = Duration::from_millis(config.window_ms);
            let needed_wait = Duration::from_secs_f64(bytes_short / bytes_per_second);
            sleep(needed_wait.max(minimum_wait)).await;
            budget = refill_budget(budget, max_budget, &mut last_tick, config.bytes_per_second);
        }

        let started = Instant::now();
        sink.send(packet).await?;
        let latency_ms = started.elapsed().as_millis() as u64;
        metrics
            .last_send_latency_ms
            .store(latency_ms, Ordering::Relaxed);
        metrics.sent_packets.fetch_add(1, Ordering::Relaxed);
        metrics
            .sent_bytes
            .fetch_add(packet_size as u64, Ordering::Relaxed);

        budget = (budget - packet_size).max(0.0);
    }

    Ok(())
}

fn refill_budget(
    budget: f64,
    max_budget: f64,
    last_tick: &mut Instant,
    bytes_per_second: usize,
) -> f64 {
    let now = Instant::now();
    let elapsed = now.duration_since(*last_tick).as_secs_f64();
    *last_tick = now;
    (budget + elapsed * bytes_per_second as f64).min(max_budget)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        packets: Mutex<Vec<Instant>>,
    }

    #[async_trait]
    impl PacketSink for RecordingSink {
        async fn send(&self, _packet: Packet) -> Result<(), TransportError> {
            self.packets.lock().await.push(Instant::now());
            Ok(())
        }
    }

    fn packet(sequence: u64, size: usize) -> Packet {
        Packet {
            kind: MediaKind::Video,
            sequence,
            timestamp_ms: sequence,
            payload: Bytes::from(vec![0; size]),
        }
    }

    #[tokio::test]
    async fn drops_when_queue_is_full() {
        let sink = Arc::new(RecordingSink::default());
        let transport = PacedPacketSender::spawn(
            PacerConfig {
                bytes_per_second: 1,
                window_ms: 5,
                max_queue_packets: 1,
            },
            sink,
        );

        transport.try_enqueue(packet(1, 128)).expect("first packet");
        let err = transport
            .try_enqueue(packet(2, 128))
            .expect_err("queue should be full");
        assert!(matches!(err, TransportError::QueueFull));
    }

    #[tokio::test]
    async fn spaces_packets_over_time() {
        let sink = Arc::new(RecordingSink::default());
        let transport = PacedPacketSender::spawn(
            PacerConfig {
                bytes_per_second: 500,
                window_ms: 1,
                max_queue_packets: 8,
            },
            Arc::clone(&sink),
        );

        transport.try_enqueue(packet(1, 400)).expect("enqueue");
        transport.try_enqueue(packet(2, 400)).expect("enqueue");
        tokio::time::sleep(Duration::from_millis(900)).await;

        let sent = sink.packets.lock().await.clone();
        assert_eq!(sent.len(), 2);
        assert!(sent[1].duration_since(sent[0]) >= Duration::from_millis(300));
    }
}

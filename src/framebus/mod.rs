pub mod drop_report;
pub mod governor;

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::mpsc;

use crate::config::QueueConfig;

pub use drop_report::{DropBatch, DropMetadata, DropRange, DropReason, DropReport};

#[derive(Clone, Debug, Default, Serialize)]
pub struct BusSnapshot {
    pub capacity: usize,
    pub consumed: u64,
    pub depth: usize,
    pub dropped_queue_full: u64,
    pub dropped_shutdown: u64,
    pub dropped_ttl: u64,
    pub enqueued: u64,
    pub received: u64,
    pub ttl_secs: u64,
}

impl BusSnapshot {
    pub fn dropped_total(&self) -> u64 {
        self.dropped_queue_full
            .saturating_add(self.dropped_ttl)
            .saturating_add(self.dropped_shutdown)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct MetricsSnapshot {
    pub decode_workers: usize,
    pub decoded_queue_depth: usize,
    pub dropped_queue_full: u64,
    pub dropped_total: u64,
    pub dropped_ttl: u64,
    pub events_emitted: u64,
    pub load1: f64,
    pub mem_available_mb: u64,
    pub queue_capacity: usize,
    pub received_total: u64,
    pub seg_ms_p50: u64,
    pub seg_ms_p90: u64,
    pub workers_busy: usize,
}

pub struct PushMetrics {
    decode_alive: AtomicUsize,
    decode_busy: AtomicUsize,
    decode_target: AtomicUsize,
    in_flight: AtomicU64,
    processed: AtomicU64,
    events_emitted: AtomicU64,
    mem_available_mb: AtomicU64,
    load1_bits: AtomicU64,
    decoded_capacity: AtomicUsize,
    decoded_ttl_secs: AtomicU64,
    decoded_bus: RwLock<Option<BusObserver>>,
    governor_status: RwLock<String>,
    segment_durations_ms: Mutex<VecDeque<u64>>,
    drops: DropReport,
}

impl Default for PushMetrics {
    fn default() -> Self {
        Self::new(&QueueConfig::default())
    }
}

impl PushMetrics {
    pub fn new(queue: &QueueConfig) -> Self {
        Self {
            decode_alive: AtomicUsize::new(0),
            decode_busy: AtomicUsize::new(0),
            decode_target: AtomicUsize::new(0),
            in_flight: AtomicU64::new(0),
            processed: AtomicU64::new(0),
            events_emitted: AtomicU64::new(0),
            mem_available_mb: AtomicU64::new(0),
            load1_bits: AtomicU64::new(0),
            decoded_capacity: AtomicUsize::new(queue.decoded_capacity),
            decoded_ttl_secs: AtomicU64::new(queue.ttl_secs),
            decoded_bus: RwLock::new(None),
            governor_status: RwLock::new(String::new()),
            segment_durations_ms: Mutex::new(VecDeque::new()),
            drops: DropReport::new(queue.report_max_ids, queue.report_max_ranges),
        }
    }

    pub fn configure_queue(&self, queue: &QueueConfig) {
        self.decoded_capacity
            .store(queue.decoded_capacity, Ordering::Relaxed);
        self.decoded_ttl_secs
            .store(queue.ttl_secs, Ordering::Relaxed);
        self.drops
            .set_limits(queue.report_max_ids, queue.report_max_ranges);
    }

    pub fn drop_report(&self) -> DropReport {
        self.drops.clone()
    }

    pub fn register_decoded_bus(&self, observer: BusObserver) {
        *lock_write(&self.decoded_bus) = Some(observer);
    }

    pub fn decoded_bus_snapshot(&self) -> BusSnapshot {
        if let Some(observer) = lock_read(&self.decoded_bus).as_ref() {
            return observer.snapshot();
        }
        BusSnapshot {
            capacity: self.decoded_capacity.load(Ordering::Relaxed),
            ttl_secs: self.decoded_ttl_secs.load(Ordering::Relaxed),
            ..BusSnapshot::default()
        }
    }

    pub fn snapshot(&self, segment: &BusSnapshot, decoded: &BusSnapshot) -> MetricsSnapshot {
        let durations = lock_mutex(&self.segment_durations_ms);
        let (seg_ms_p50, seg_ms_p90) = percentiles(&durations);
        drop(durations);
        let dropped_queue_full = segment
            .dropped_queue_full
            .saturating_add(decoded.dropped_queue_full);
        let dropped_ttl = segment.dropped_ttl.saturating_add(decoded.dropped_ttl);
        MetricsSnapshot {
            decode_workers: self.decode_alive(),
            decoded_queue_depth: decoded.depth,
            dropped_queue_full,
            dropped_total: segment
                .dropped_total()
                .saturating_add(decoded.dropped_total()),
            dropped_ttl,
            events_emitted: self.events_emitted(),
            load1: f64::from_bits(self.load1_bits.load(Ordering::Relaxed)),
            mem_available_mb: self.mem_available_mb.load(Ordering::Relaxed),
            queue_capacity: segment.capacity,
            received_total: segment.received.saturating_add(decoded.received),
            seg_ms_p50,
            seg_ms_p90,
            workers_busy: self.decode_busy(),
        }
    }

    pub fn segment_started(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn segment_finished(&self, duration: Duration) {
        decrement(&self.in_flight);
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.record_segment_duration(duration);
    }

    pub fn segment_abandoned(&self) {
        decrement(&self.in_flight);
    }

    pub fn record_segment_duration(&self, duration: Duration) {
        let millis = duration.as_millis().min(u64::MAX as u128) as u64;
        let mut durations = lock_mutex(&self.segment_durations_ms);
        if durations.len() == 1_024 {
            durations.pop_front();
        }
        durations.push_back(millis);
    }

    pub fn add_events(&self, count: u64) {
        self.events_emitted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn decode_worker_started(&self) {
        self.decode_alive.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decode_worker_finished(&self) {
        decrement_usize(&self.decode_alive);
    }

    pub fn decode_worker_busy(&self) {
        self.decode_busy.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decode_worker_idle(&self) {
        decrement_usize(&self.decode_busy);
    }

    pub fn set_decode_alive(&self, value: usize) {
        self.decode_alive.store(value, Ordering::Relaxed);
    }

    pub fn set_decode_busy(&self, value: usize) {
        self.decode_busy.store(value, Ordering::Relaxed);
    }

    pub fn set_decode_target(&self, value: usize) {
        self.decode_target.store(value, Ordering::Relaxed);
    }

    pub fn decode_alive(&self) -> usize {
        self.decode_alive.load(Ordering::Relaxed)
    }

    pub fn decode_busy(&self) -> usize {
        self.decode_busy.load(Ordering::Relaxed)
    }

    pub fn decode_target(&self) -> usize {
        self.decode_target.load(Ordering::Relaxed)
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    pub fn events_emitted(&self) -> u64 {
        self.events_emitted.load(Ordering::Relaxed)
    }

    pub fn governor_status(&self) -> String {
        lock_read(&self.governor_status).clone()
    }

    pub fn health_status(&self) -> &'static str {
        if self.decode_target() > 0 && self.decode_alive() == 0 {
            "degraded"
        } else {
            "healthy"
        }
    }

    pub(crate) fn update_system(&self, mem_available_mb: u64, load1: f64) {
        self.mem_available_mb
            .store(mem_available_mb, Ordering::Relaxed);
        self.load1_bits.store(load1.to_bits(), Ordering::Relaxed);
    }

    pub(crate) fn set_governor_status(&self, status: String) {
        *lock_write(&self.governor_status) = status;
    }
}

pub trait DropItem: Send + 'static {
    fn drop_metadata(&self) -> DropMetadata;

    fn on_drop(self)
    where
        Self: Sized,
    {
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SendError {
    pub reason: DropReason,
}

impl fmt::Display for SendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "segment dropped: {}", self.reason.as_str())
    }
}

impl Error for SendError {}

#[derive(Debug)]
struct Queued<T> {
    item: T,
    enqueued_at: Instant,
}

#[derive(Debug, Default)]
struct Counters {
    consumed: AtomicU64,
    depth: AtomicUsize,
    dropped_queue_full: AtomicU64,
    dropped_shutdown: AtomicU64,
    dropped_ttl: AtomicU64,
    enqueued: AtomicU64,
    received: AtomicU64,
}

#[derive(Debug)]
struct Shared {
    capacity: usize,
    ttl_secs: u64,
    counters: Counters,
    drops: DropReport,
}

impl Shared {
    fn snapshot(&self) -> BusSnapshot {
        BusSnapshot {
            capacity: self.capacity,
            consumed: self.counters.consumed.load(Ordering::Relaxed),
            depth: self.counters.depth.load(Ordering::Relaxed),
            dropped_queue_full: self.counters.dropped_queue_full.load(Ordering::Relaxed),
            dropped_shutdown: self.counters.dropped_shutdown.load(Ordering::Relaxed),
            dropped_ttl: self.counters.dropped_ttl.load(Ordering::Relaxed),
            enqueued: self.counters.enqueued.load(Ordering::Relaxed),
            received: self.counters.received.load(Ordering::Relaxed),
            ttl_secs: self.ttl_secs,
        }
    }

    fn note_drop(&self, reason: DropReason) {
        match reason {
            DropReason::QueueFull => {
                self.counters
                    .dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            DropReason::TtlExpired => {
                self.counters.dropped_ttl.fetch_add(1, Ordering::Relaxed);
            }
            DropReason::Shutdown => {
                self.counters
                    .dropped_shutdown
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn expired<T>(&self, queued: &Queued<T>) -> bool {
        self.ttl_secs > 0 && queued.enqueued_at.elapsed() >= Duration::from_secs(self.ttl_secs)
    }
}

#[derive(Clone, Debug)]
pub struct BusObserver {
    shared: Arc<Shared>,
}

impl BusObserver {
    pub fn snapshot(&self) -> BusSnapshot {
        self.shared.snapshot()
    }
}

#[derive(Debug)]
pub struct BusSender<T: DropItem> {
    sender: mpsc::Sender<Queued<T>>,
    shared: Arc<Shared>,
}

impl<T: DropItem> Clone for BusSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            shared: self.shared.clone(),
        }
    }
}

impl<T: DropItem> BusSender<T> {
    pub fn observer(&self) -> BusObserver {
        BusObserver {
            shared: self.shared.clone(),
        }
    }

    pub fn snapshot(&self) -> BusSnapshot {
        self.shared.snapshot()
    }

    pub async fn send(&self, item: T) -> Result<(), SendError> {
        self.try_send(item)
    }

    pub fn try_send(&self, item: T) -> Result<(), SendError> {
        self.shared
            .counters
            .received
            .fetch_add(1, Ordering::Relaxed);
        self.shared.counters.depth.fetch_add(1, Ordering::Relaxed);
        let queued = Queued {
            item,
            enqueued_at: Instant::now(),
        };
        match self.sender.try_send(queued) {
            Ok(()) => {
                self.shared
                    .counters
                    .enqueued
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(queued)) => {
                self.shared.counters.depth.fetch_sub(1, Ordering::Relaxed);
                drop_queued(&self.shared, queued, DropReason::QueueFull);
                Err(SendError {
                    reason: DropReason::QueueFull,
                })
            }
            Err(mpsc::error::TrySendError::Closed(queued)) => {
                self.shared.counters.depth.fetch_sub(1, Ordering::Relaxed);
                drop_queued(&self.shared, queued, DropReason::Shutdown);
                Err(SendError {
                    reason: DropReason::Shutdown,
                })
            }
        }
    }
}

#[derive(Debug)]
pub struct BusReceiver<T: DropItem> {
    receiver: mpsc::Receiver<Queued<T>>,
    shared: Arc<Shared>,
}

impl<T: DropItem> BusReceiver<T> {
    pub fn observer(&self) -> BusObserver {
        BusObserver {
            shared: self.shared.clone(),
        }
    }

    pub fn snapshot(&self) -> BusSnapshot {
        self.shared.snapshot()
    }

    pub async fn recv(&mut self) -> Option<T> {
        loop {
            let queued = self.receiver.recv().await?;
            self.shared.counters.depth.fetch_sub(1, Ordering::Relaxed);
            if self.shared.expired(&queued) {
                drop_queued(&self.shared, queued, DropReason::TtlExpired);
                continue;
            }
            self.shared
                .counters
                .consumed
                .fetch_add(1, Ordering::Relaxed);
            return Some(queued.item);
        }
    }

    pub fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        loop {
            let queued = self.receiver.try_recv()?;
            self.shared.counters.depth.fetch_sub(1, Ordering::Relaxed);
            if self.shared.expired(&queued) {
                drop_queued(&self.shared, queued, DropReason::TtlExpired);
                continue;
            }
            self.shared
                .counters
                .consumed
                .fetch_add(1, Ordering::Relaxed);
            return Ok(queued.item);
        }
    }

    pub fn close(&mut self) {
        self.receiver.close();
    }
}

impl<T: DropItem> Drop for BusReceiver<T> {
    fn drop(&mut self) {
        self.receiver.close();
        while let Ok(queued) = self.receiver.try_recv() {
            self.shared.counters.depth.fetch_sub(1, Ordering::Relaxed);
            drop_queued(&self.shared, queued, DropReason::Shutdown);
        }
    }
}

pub fn channel<T: DropItem>(
    capacity: usize,
    ttl_secs: u64,
    on_full: &str,
    drops: DropReport,
) -> (BusSender<T>, BusReceiver<T>) {
    if on_full != "drop_newest" {
        tracing::warn!(on_full, "unsupported queue policy; using drop_newest");
    }
    let capacity = capacity.max(1);
    let (sender, receiver) = mpsc::channel(capacity);
    let shared = Arc::new(Shared {
        capacity,
        ttl_secs,
        counters: Counters::default(),
        drops,
    });
    (
        BusSender {
            sender,
            shared: shared.clone(),
        },
        BusReceiver { receiver, shared },
    )
}

fn drop_queued<T: DropItem>(shared: &Shared, queued: Queued<T>, reason: DropReason) {
    shared.note_drop(reason);
    shared.drops.record(queued.item.drop_metadata(), reason);
    queued.item.on_drop();
}

fn percentiles(durations: &VecDeque<u64>) -> (u64, u64) {
    if durations.is_empty() {
        return (0, 0);
    }
    let mut sorted = durations.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let p50 = sorted[(sorted.len() - 1) * 50 / 100];
    let p90 = sorted[(sorted.len() - 1) * 90 / 100];
    (p50, p90)
}

fn decrement(counter: &AtomicU64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

fn decrement_usize(counter: &AtomicUsize) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

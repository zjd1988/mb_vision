use std::fs;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::watch;

use super::PushMetrics;
use crate::config::GovernorConfig;

#[derive(Clone, Debug, Default, Serialize)]
pub struct SystemSnapshot {
    pub mem_available_mb: u64,
    pub load1: f64,
    pub cgroup_limit_mb: Option<u64>,
}

pub struct Governor {
    config: GovernorConfig,
    metrics: Arc<PushMetrics>,
    max_workers: usize,
    last_scale: Mutex<Option<Instant>>,
}

impl Governor {
    pub fn new(config: GovernorConfig, metrics: Arc<PushMetrics>) -> Self {
        let processors = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        let max_workers = if config.max_decode_workers == 0 {
            processors
        } else {
            config.max_decode_workers
        };
        let sample = probe();
        let mut initial = max_workers.min(processors).max(1);
        if sample.mem_available_mb > 0
            && config.mem_low_mb > 0
            && sample.mem_available_mb < config.mem_low_mb
        {
            initial = 1;
        }
        metrics.set_decode_target(initial);
        metrics.update_system(sample.mem_available_mb, sample.load1);
        Self {
            config,
            metrics,
            max_workers,
            last_scale: Mutex::new(None),
        }
    }

    pub fn target(&self) -> usize {
        self.metrics.decode_target()
    }

    pub fn tick(&self) -> SystemSnapshot {
        let sample = probe();
        self.metrics
            .update_system(sample.mem_available_mb, sample.load1);
        if !self.config.enabled || sample.mem_available_mb == 0 {
            return sample;
        }

        let now = Instant::now();
        let cooldown = Duration::from_secs(self.config.scale_cooldown_secs);
        let mut last_scale = lock(&self.last_scale);
        if last_scale.is_some_and(|last| now.duration_since(last) < cooldown) {
            return sample;
        }

        let target = self.target();
        if self.config.mem_low_mb > 0
            && sample.mem_available_mb < self.config.mem_low_mb
            && target > 1
        {
            let target = target - 1;
            self.metrics.set_decode_target(target);
            self.metrics.set_governor_status(format!(
                "scale-down: mem_available={}MB below={}MB decode_target={target}",
                sample.mem_available_mb, self.config.mem_low_mb
            ));
            *last_scale = Some(now);
        } else if self.config.mem_high_mb > 0
            && sample.mem_available_mb > self.config.mem_high_mb
            && target < self.max_workers
        {
            let target = target + 1;
            self.metrics.set_decode_target(target);
            self.metrics.set_governor_status(format!(
                "scale-up: mem_available={}MB above={}MB decode_target={target}",
                sample.mem_available_mb, self.config.mem_high_mb
            ));
            *last_scale = Some(now);
        }
        sample
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        loop {
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(interval) => {
                    self.tick();
                }
            }
        }
    }
}

pub async fn run(
    config: GovernorConfig,
    metrics: Arc<PushMetrics>,
    shutdown: watch::Receiver<bool>,
) {
    Governor::new(config, metrics).run(shutdown).await;
}

pub fn probe() -> SystemSnapshot {
    let host_available = read_mem_available_mb().unwrap_or(0);
    let cgroup = read_cgroup_memory();
    let mut available = host_available;
    let mut cgroup_limit_mb = None;
    if let Some((limit_bytes, usage_bytes)) = cgroup {
        if let Some(limit_bytes) = limit_bytes {
            let cgroup_available = limit_bytes.saturating_sub(usage_bytes) / (1024 * 1024);
            available = if available == 0 {
                cgroup_available
            } else {
                available.min(cgroup_available)
            };
            cgroup_limit_mb = Some(limit_bytes / (1024 * 1024));
        }
    }
    SystemSnapshot {
        mem_available_mb: available,
        load1: read_load1().unwrap_or(0.0),
        cgroup_limit_mb,
    }
}

fn read_mem_available_mb() -> Option<u64> {
    let text = fs::read_to_string("/proc/meminfo").ok()?;
    let mut available = None;
    let mut free = None;
    let mut buffers = 0;
    let mut cached = 0;
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let key = parts.next()?;
        let value = parts.next().and_then(|value| value.parse::<u64>().ok());
        match (key, value) {
            ("MemAvailable:", value) => available = value,
            ("MemFree:", value) => free = value,
            ("Buffers:", Some(value)) => buffers = value,
            ("Cached:", Some(value)) => cached = value,
            _ => {}
        }
    }
    let kib = available.or_else(|| free.map(|free| free + buffers + cached))?;
    Some(kib / 1024)
}

fn read_load1() -> Option<f64> {
    fs::read_to_string("/proc/loadavg")
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn read_cgroup_memory() -> Option<(Option<u64>, u64)> {
    if let Ok(limit) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let usage = fs::read_to_string("/sys/fs/cgroup/memory.current")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        return Some((parse_cgroup_limit(&limit), usage));
    }
    if let Ok(limit) = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        let usage = fs::read_to_string("/sys/fs/cgroup/memory/memory.usage_in_bytes")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        return Some((parse_cgroup_limit(&limit), usage));
    }
    None
}

fn parse_cgroup_limit(value: &str) -> Option<u64> {
    let limit = value.trim().parse::<u64>().ok()?;
    (limit < (1 << 62)).then_some(limit)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

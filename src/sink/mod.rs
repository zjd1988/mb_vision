pub mod gate;
pub mod nvr_api;
pub mod snapshot;
pub mod sqlite;

pub use nvr_api::NvrClient;
#[allow(unused_imports)]
pub use snapshot::{Snapshot, SnapshotSink};
#[allow(unused_imports)]
pub use sqlite::{EventRecord, ProcessedSegment, Storage};

use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};

pub fn event_occurred_at(
    started_at: Option<DateTime<Utc>>,
    timestamp_sec: f64,
) -> Result<DateTime<Utc>> {
    if !timestamp_sec.is_finite() {
        bail!("事件时间戳不是有限数值: {timestamp_sec}");
    }
    let milliseconds = timestamp_sec * 1000.0;
    if milliseconds < i64::MIN as f64 || milliseconds > i64::MAX as f64 {
        bail!("事件时间戳超出支持范围: {timestamp_sec}");
    }
    let base = started_at
        .or_else(|| DateTime::from_timestamp(0, 0))
        .expect("Unix epoch must be representable");
    base.checked_add_signed(Duration::milliseconds(milliseconds.trunc() as i64))
        .ok_or_else(|| anyhow::anyhow!("事件时间戳超出日期范围: {timestamp_sec}"))
}

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    QueueFull,
    TtlExpired,
    Shutdown,
}

impl DropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::TtlExpired => "ttl_expired",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DropMetadata {
    pub camera_id: String,
    pub recording_id: String,
    pub started_at: String,
    pub ended_at: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DropRange {
    pub camera_id: String,
    pub reason: String,
    pub count: u64,
    pub from: String,
    pub to: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DropBatch {
    pub seq: u64,
    pub ranges: Vec<DropRange>,
}

#[derive(Debug)]
struct Entry {
    seq: u64,
    range: DropRange,
}

#[derive(Debug)]
struct State {
    next_seq: u64,
    report_max_ids: usize,
    report_max_ranges: usize,
    entries: Vec<Entry>,
}

#[derive(Clone, Debug)]
pub struct DropReport {
    inner: Arc<Mutex<State>>,
}

impl Default for DropReport {
    fn default() -> Self {
        Self::new(32, 256)
    }
}

impl DropReport {
    pub fn new(report_max_ids: usize, report_max_ranges: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                next_seq: 0,
                report_max_ids,
                report_max_ranges: report_max_ranges.max(1),
                entries: Vec::new(),
            })),
        }
    }

    pub fn set_limits(&self, report_max_ids: usize, report_max_ranges: usize) {
        let mut state = self.lock();
        state.report_max_ids = report_max_ids;
        state.report_max_ranges = report_max_ranges.max(1);
    }

    pub fn record(&self, metadata: DropMetadata, reason: DropReason) {
        let fallback = rfc3339_now();
        let from = if !metadata.started_at.is_empty() {
            metadata.started_at
        } else if !metadata.ended_at.is_empty() {
            metadata.ended_at.clone()
        } else {
            fallback
        };
        let to = if !metadata.ended_at.is_empty() {
            metadata.ended_at
        } else {
            from.clone()
        };
        let reason = reason.as_str();
        let mut state = self.lock();
        let max_ids = state.report_max_ids;
        let existing = state.entries.iter().rposition(|entry| {
            entry.seq == 0
                && entry.range.camera_id == metadata.camera_id
                && entry.range.reason == reason
        });

        if let Some(index) = existing {
            let can_merge = max_ids == 0
                || metadata.recording_id.is_empty()
                || state.entries[index].range.ids.len() < max_ids
                || state.entries[index]
                    .range
                    .ids
                    .contains(&metadata.recording_id);
            if can_merge {
                let range = &mut state.entries[index].range;
                range.count = range.count.saturating_add(1);
                if from < range.from {
                    range.from = from;
                }
                if to > range.to {
                    range.to = to;
                }
                if max_ids > 0
                    && !metadata.recording_id.is_empty()
                    && !range.ids.contains(&metadata.recording_id)
                {
                    range.ids.push(metadata.recording_id);
                }
                return;
            }
        }

        let ids = if max_ids > 0 && !metadata.recording_id.is_empty() {
            vec![metadata.recording_id]
        } else {
            Vec::new()
        };
        state.entries.push(Entry {
            seq: 0,
            range: DropRange {
                camera_id: metadata.camera_id,
                reason: reason.to_owned(),
                count: 1,
                from,
                to,
                ids,
            },
        });
    }

    pub fn next_report(&self) -> Option<DropBatch> {
        let mut state = self.lock();
        let selected = state.entries.len().min(state.report_max_ranges);
        if selected == 0 {
            return None;
        }

        let has_unreported = state.entries[..selected].iter().any(|entry| entry.seq == 0);
        if has_unreported {
            state.next_seq = state.next_seq.saturating_add(1);
        }
        let new_seq = state.next_seq;
        let mut seq = 0;
        let mut ranges = Vec::with_capacity(selected);
        for entry in &mut state.entries[..selected] {
            if entry.seq == 0 {
                entry.seq = new_seq;
            }
            seq = seq.max(entry.seq);
            ranges.push(entry.range.clone());
        }
        Some(DropBatch { seq, ranges })
    }

    pub fn ack(&self, seq: u64) {
        if seq == 0 {
            return;
        }
        self.lock()
            .entries
            .retain(|entry| entry.seq == 0 || entry.seq > seq);
    }

    pub fn ranges(&self) -> Vec<DropRange> {
        let state = self.lock();
        state.entries[..state.entries.len().min(state.report_max_ranges)]
            .iter()
            .map(|entry| entry.range.clone())
            .collect()
    }

    pub fn pending_count(&self) -> u64 {
        self.lock()
            .entries
            .iter()
            .map(|entry| entry.range.count)
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().entries.is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn rfc3339_now() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = day_seconds / 3_600;
    let minute = (day_seconds % 3_600) / 60;
    let second = day_seconds % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

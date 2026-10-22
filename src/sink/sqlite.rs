use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use turso::{Builder, Connection, Database, Row, Value};

use crate::detect::Detection;
use crate::event::zone::ZoneEvent;
use crate::sink::event_occurred_at;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS processed_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camera_id TEXT NOT NULL,
    path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    processed_at TEXT NOT NULL,
    processed_at_s INTEGER NOT NULL,
    frames_sampled INTEGER NOT NULL DEFAULT 0,
    UNIQUE(camera_id, path)
);
CREATE INDEX IF NOT EXISTS idx_segments_camera ON processed_segments(camera_id);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camera_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    zone_name TEXT NOT NULL,
    track_id INTEGER NOT NULL,
    class_id INTEGER NOT NULL,
    confidence REAL NOT NULL,
    frame_idx INTEGER NOT NULL,
    timestamp_sec REAL NOT NULL,
    occurred_at TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT 'zone_intrusion',
    UNIQUE(camera_id, zone_name, track_id, event_type)
);
CREATE INDEX IF NOT EXISTS idx_events_camera_time ON events(camera_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_zone ON events(zone_name);

CREATE TABLE IF NOT EXISTS detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camera_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    frame_idx INTEGER NOT NULL,
    track_id INTEGER,
    class_id INTEGER NOT NULL,
    confidence REAL NOT NULL,
    x_min REAL NOT NULL,
    y_min REAL NOT NULL,
    x_max REAL NOT NULL,
    y_max REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_detections_source ON detections(camera_id, source_path);
"#;

const EVENT_COLUMNS: &str = "camera_id, source_path, zone_name, track_id, class_id, confidence, frame_idx, timestamp_sec, occurred_at, event_type";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub camera_id: String,
    pub source_path: String,
    pub zone_name: String,
    pub track_id: usize,
    pub class_id: usize,
    pub confidence: f64,
    pub frame_idx: u64,
    pub timestamp_sec: f64,
    pub occurred_at: String,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedSegment {
    pub id: i64,
    pub camera_id: String,
    pub path: String,
    pub file_size: u64,
    pub processed_at: String,
    pub processed_at_s: i64,
    pub frames_sampled: u64,
}

#[derive(Clone)]
pub struct Storage {
    _db: Database,
    conn: Connection,
    store_detections: bool,
}

impl Storage {
    pub async fn new(path: &Path, store_detections: bool) -> Result<Self> {
        let path_string = path_text(path)?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("创建 SQLite 目录失败 {}", parent.display()))?;
        }

        let db = Builder::new_local(path_string)
            .build()
            .await
            .with_context(|| format!("打开 SQLite 数据库失败 {}", path.display()))?;
        let conn = db.connect().context("连接 SQLite 数据库失败")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        let storage = Self {
            _db: db,
            conn,
            store_detections,
        };
        storage.migrate().await?;
        Ok(storage)
    }

    pub async fn open(path: &Path, store_detections: bool) -> Result<Self> {
        Self::new(path, store_detections).await
    }

    async fn migrate(&self) -> Result<()> {
        self.conn
            .execute_batch(SCHEMA)
            .await
            .context("初始化 SQLite schema 失败")?;

        let mut rows = self
            .conn
            .query("PRAGMA table_info(events)", ())
            .await
            .context("检查 events schema 失败")?;
        let mut has_event_type = false;
        while let Some(row) = rows.next().await? {
            if row_text(&row, 1)? == "event_type" {
                has_event_type = true;
            }
        }
        drop(rows);

        if !has_event_type {
            self.conn
                .execute(
                    "ALTER TABLE events ADD COLUMN event_type TEXT NOT NULL DEFAULT 'zone_intrusion'",
                    (),
                )
                .await
                .context("迁移 events.event_type 失败")?;
        }
        Ok(())
    }

    pub async fn write_event<P: AsRef<Path>>(
        &self,
        camera_id: &str,
        source_path: P,
        event: &ZoneEvent,
        started_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        let source_path = path_text(source_path.as_ref())?;
        let occurred_at = event_occurred_at(started_at, event.timestamp_sec)?
            .to_rfc3339_opts(SecondsFormat::Secs, true);
        let track_id = i64::try_from(event.track_id).context("track_id 超出 SQLite 范围")?;
        let class_id = i64::try_from(event.class_id).context("class_id 超出 SQLite 范围")?;
        let frame_idx = i64::try_from(event.frame_idx).context("frame_idx 超出 SQLite 范围")?;

        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO events (camera_id, source_path, zone_name, track_id, class_id, confidence, frame_idx, timestamp_sec, occurred_at, event_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                camera_id,
                source_path,
                event.zone_name.as_str(),
                track_id,
                class_id,
                event.confidence,
                frame_idx,
                event.timestamp_sec,
                occurred_at,
                event.event_type.as_str(),
            ),
        ).await?;
        Ok(changed != 0)
    }

    pub async fn write_detections<P: AsRef<Path>>(
        &self,
        camera_id: &str,
        source_path: P,
        frame_idx: u64,
        detections: &[Detection],
    ) -> Result<usize> {
        if !self.store_detections {
            return Ok(0);
        }
        let source_path = path_text(source_path.as_ref())?;
        let frame_idx = i64::try_from(frame_idx).context("frame_idx 超出 SQLite 范围")?;
        let mut written = 0;
        for detection in detections {
            let Some(track_id) = detection.track_id else {
                continue;
            };
            let track_id = i64::try_from(track_id).context("track_id 超出 SQLite 范围")?;
            let class_id =
                i64::try_from(detection.class_id).context("class_id 超出 SQLite 范围")?;
            self.conn.execute(
                "INSERT INTO detections (camera_id, source_path, frame_idx, track_id, class_id, confidence, x_min, y_min, x_max, y_max) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    camera_id,
                    source_path,
                    frame_idx,
                    track_id,
                    class_id,
                    detection.confidence,
                    detection.bbox[0],
                    detection.bbox[1],
                    detection.bbox[2],
                    detection.bbox[3],
                ),
            ).await?;
            written += 1;
        }
        Ok(written)
    }

    pub async fn recent_events(&self, limit: usize) -> Result<Vec<EventRecord>> {
        let sql = format!("SELECT {EVENT_COLUMNS} FROM events ORDER BY occurred_at DESC LIMIT ?");
        let mut rows = self.conn.query(sql, (limit_i64(limit),)).await?;
        collect_events(&mut rows).await
    }

    pub async fn recent_events_for_camera(
        &self,
        camera_id: &str,
        limit: usize,
    ) -> Result<Vec<EventRecord>> {
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE camera_id = ? ORDER BY occurred_at DESC LIMIT ?"
        );
        let mut rows = self.conn.query(sql, (camera_id, limit_i64(limit))).await?;
        collect_events(&mut rows).await
    }

    pub async fn mark_processed<P: AsRef<Path>>(
        &self,
        camera_id: &str,
        path: P,
        file_size: u64,
        frames_sampled: u64,
    ) -> Result<()> {
        let path = path_text(path.as_ref())?;
        let file_size = i64::try_from(file_size).context("file_size 超出 SQLite 范围")?;
        let frames_sampled =
            i64::try_from(frames_sampled).context("frames_sampled 超出 SQLite 范围")?;
        let now = Utc::now();
        let processed_at = now.to_rfc3339_opts(SecondsFormat::Secs, true);
        self.conn.execute(
            "INSERT INTO processed_segments (camera_id, path, file_size, processed_at, processed_at_s, frames_sampled) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(camera_id, path) DO UPDATE SET file_size = excluded.file_size, processed_at = excluded.processed_at, processed_at_s = excluded.processed_at_s, frames_sampled = excluded.frames_sampled",
            (
                camera_id,
                path,
                file_size,
                processed_at,
                now.timestamp(),
                frames_sampled,
            ),
        ).await?;
        Ok(())
    }

    pub async fn processed_segment<P: AsRef<Path>>(
        &self,
        camera_id: &str,
        path: P,
    ) -> Result<Option<ProcessedSegment>> {
        let path = path_text(path.as_ref())?;
        let mut rows = self.conn.query(
            "SELECT id, camera_id, path, file_size, processed_at, processed_at_s, frames_sampled FROM processed_segments WHERE camera_id = ? AND path = ? LIMIT 1",
            (camera_id, path),
        ).await?;
        match rows.next().await? {
            Some(row) => Ok(Some(processed_segment_from_row(&row)?)),
            None => Ok(None),
        }
    }

    pub async fn is_processed<P: AsRef<Path>>(
        &self,
        camera_id: &str,
        path: P,
        file_size: u64,
    ) -> Result<bool> {
        Ok(self
            .processed_segment(camera_id, path)
            .await?
            .is_some_and(|segment| segment.file_size == file_size))
    }

    pub async fn processed_segments(&self, limit: usize) -> Result<Vec<ProcessedSegment>> {
        let mut rows = self.conn.query(
            "SELECT id, camera_id, path, file_size, processed_at, processed_at_s, frames_sampled FROM processed_segments ORDER BY processed_at_s DESC, id DESC LIMIT ?",
            (limit_i64(limit),),
        ).await?;
        let mut segments = Vec::new();
        while let Some(row) = rows.next().await? {
            segments.push(processed_segment_from_row(&row)?);
        }
        Ok(segments)
    }
}

async fn collect_events(rows: &mut turso::Rows) -> Result<Vec<EventRecord>> {
    let mut events = Vec::new();
    while let Some(row) = rows.next().await? {
        events.push(EventRecord {
            camera_id: row_text(&row, 0)?,
            source_path: row_text(&row, 1)?,
            zone_name: row_text(&row, 2)?,
            track_id: row_usize(&row, 3)?,
            class_id: row_usize(&row, 4)?,
            confidence: row_real(&row, 5)?,
            frame_idx: row_u64(&row, 6)?,
            timestamp_sec: row_real(&row, 7)?,
            occurred_at: row_text(&row, 8)?,
            event_type: row_text(&row, 9)?,
        });
    }
    Ok(events)
}

fn processed_segment_from_row(row: &Row) -> Result<ProcessedSegment> {
    Ok(ProcessedSegment {
        id: row_integer(row, 0)?,
        camera_id: row_text(row, 1)?,
        path: row_text(row, 2)?,
        file_size: row_u64(row, 3)?,
        processed_at: row_text(row, 4)?,
        processed_at_s: row_integer(row, 5)?,
        frames_sampled: row_u64(row, 6)?,
    })
}

fn row_text(row: &Row, index: usize) -> Result<String> {
    match row.get_value(index)? {
        Value::Text(value) => Ok(value),
        value => bail!("SQLite 列 {index} 应为 TEXT，实际为 {value:?}"),
    }
}

fn row_integer(row: &Row, index: usize) -> Result<i64> {
    match row.get_value(index)? {
        Value::Integer(value) => Ok(value),
        value => bail!("SQLite 列 {index} 应为 INTEGER，实际为 {value:?}"),
    }
}

fn row_real(row: &Row, index: usize) -> Result<f64> {
    match row.get_value(index)? {
        Value::Real(value) => Ok(value),
        Value::Integer(value) => Ok(value as f64),
        value => bail!("SQLite 列 {index} 应为 REAL，实际为 {value:?}"),
    }
}

fn row_u64(row: &Row, index: usize) -> Result<u64> {
    u64::try_from(row_integer(row, index)?).map_err(|_| anyhow!("SQLite 列 {index} 不能转换为 u64"))
}

fn row_usize(row: &Row, index: usize) -> Result<usize> {
    usize::try_from(row_integer(row, index)?)
        .map_err(|_| anyhow!("SQLite 列 {index} 不能转换为 usize"))
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| anyhow!("路径包含非 UTF-8 字符: {}", path.display()))
}

fn limit_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

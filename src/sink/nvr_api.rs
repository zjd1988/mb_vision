use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::StreamExt;
use reqwest::{header::CONTENT_TYPE, Client, Method, RequestBuilder, Response};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::config::NvrConfig;
use crate::detect::Detection;
use crate::event::zone::ZoneEvent;
use crate::sink::event_occurred_at;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraProbe {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub recording_enabled: Option<bool>,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub sub_stream_url: String,
    #[serde(default)]
    pub protocol: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AiStatusRequest<'a> {
    pub ai_status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ai_error: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiEventRequest {
    pub camera_id: String,
    pub recording_id: String,
    pub event_type: String,
    pub severity: String,
    pub class_name: String,
    pub confidence: f32,
    pub bbox: [f32; 4],
    pub frame_idx: u64,
    pub timestamp: String,
    pub metadata: AiEventMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiEventMetadata {
    pub track_id: usize,
    pub zone_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiEventResponse {
    pub id: i64,
    pub status: String,
}

impl AiEventRequest {
    pub fn from_zone_event(
        camera_id: impl Into<String>,
        recording_id: impl Into<String>,
        event: &ZoneEvent,
        frame_width: u32,
        frame_height: u32,
        started_at: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        if frame_width == 0 || frame_height == 0 {
            bail!("AI 事件帧尺寸必须大于零: {frame_width}x{frame_height}");
        }
        let width = frame_width as f32;
        let height = frame_height as f32;
        let detection = Detection::new(event.bbox, event.confidence, event.class_id);
        let occurred_at = event_occurred_at(started_at, event.timestamp_sec)?;
        Ok(Self {
            camera_id: camera_id.into(),
            recording_id: recording_id.into(),
            event_type: event.event_type.clone(),
            severity: "warning".into(),
            class_name: detection.class_name().into(),
            confidence: event.confidence,
            bbox: [
                event.bbox[0] / width,
                event.bbox[1] / height,
                (event.bbox[2] - event.bbox[0]) / width,
                (event.bbox[3] - event.bbox[1]) / height,
            ],
            frame_idx: event.frame_idx,
            timestamp: occurred_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            metadata: AiEventMetadata {
                track_id: event.track_id,
                zone_name: event.zone_name.clone(),
            },
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DropReport {
    pub seq: i64,
    pub ranges: Vec<DropRange>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DropRange {
    pub camera_id: String,
    pub reason: String,
    pub count: u64,
    pub from: String,
    pub to: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatMetrics {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub device: String,
    pub drops: Option<DropReport>,
    pub last_progress_secs: Option<u64>,
    pub metrics: HeartbeatMetrics,
    pub processed_count: u64,
    pub protocol: u32,
    pub queue_depth: usize,
    pub skip_cameras: Vec<String>,
    pub status: String,
}

impl Default for HeartbeatRequest {
    fn default() -> Self {
        Self {
            device: "cpu".into(),
            drops: None,
            last_progress_secs: None,
            metrics: HeartbeatMetrics::default(),
            processed_count: 0,
            protocol: 2,
            queue_depth: 0,
            skip_cameras: Vec::new(),
            status: "healthy".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub push_enabled: bool,
    #[serde(default)]
    pub ack_drops: Option<i64>,
}

#[derive(Clone)]
pub struct NvrClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl NvrClient {
    pub fn new(config: &NvrConfig) -> Self {
        Self {
            client: Client::new(),
            base_url: config.api_base_url.trim_end_matches('/').into(),
            api_key: config.api_key.clone(),
        }
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let request = self
            .client
            .request(method, format!("{}{path}", self.base_url));
        if self.api_key.is_empty() {
            request
        } else {
            request.bearer_auth(&self.api_key)
        }
    }

    pub async fn cameras(&self) -> Result<Vec<CameraProbe>> {
        let response = Self::send(
            self.request(Method::GET, "/api/cameras"),
            "GET /api/cameras",
        )
        .await?;
        Self::parse_json(response, "GET /api/cameras").await
    }

    pub async fn probe(&self) -> Result<Vec<CameraProbe>> {
        self.cameras().await
    }

    pub async fn download_recording(
        &self,
        recording_id: &str,
        temp_dir: &std::path::Path,
    ) -> Result<std::path::PathBuf> {
        let path = format!("/api/recordings/{recording_id}/download");
        let response =
            Self::send(self.request(Method::GET, &path), "GET recording download").await?;
        tokio::fs::create_dir_all(temp_dir)
            .await
            .with_context(|| format!("创建录像下载目录失败 {}", temp_dir.display()))?;
        let safe_id = recording_id
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                    character
                } else {
                    '_'
                }
            })
            .collect::<String>();
        let output_path = temp_dir.join(format!("{safe_id}.mp4"));
        let mut output = tokio::fs::File::create(&output_path)
            .await
            .with_context(|| format!("创建录像下载文件失败 {}", output_path.display()))?;
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.context("读取录像下载响应失败")?;
            output
                .write_all(&chunk)
                .await
                .context("写入录像下载文件失败")?;
        }
        output.flush().await.context("刷新录像下载文件失败")?;
        Ok(output_path)
    }

    pub async fn update_ai_status(
        &self,
        recording_id: &str,
        ai_status: &str,
        ai_error: Option<&str>,
    ) -> Result<()> {
        let path = format!("/api/recordings/{recording_id}/ai-status");
        let body = AiStatusRequest {
            ai_status,
            ai_error,
        };
        Self::send(
            self.request(Method::PATCH, &path).json(&body),
            "PATCH ai-status",
        )
        .await?;
        Ok(())
    }

    pub async fn create_ai_event(&self, event: &AiEventRequest) -> Result<AiEventResponse> {
        let response = Self::send(
            self.request(Method::POST, "/api/ai/events").json(event),
            "POST /api/ai/events",
        )
        .await?;
        Self::parse_json(response, "POST /api/ai/events").await
    }

    pub async fn upload_snapshot(&self, event_id: i64, jpeg: &[u8]) -> Result<()> {
        let path = format!("/api/ai/events/{event_id}/snapshot");
        Self::send(
            self.request(Method::POST, &path)
                .header(CONTENT_TYPE, "image/jpeg")
                .body(jpeg.to_vec()),
            "POST AI event snapshot",
        )
        .await?;
        Ok(())
    }

    pub async fn heartbeat(&self, heartbeat: &HeartbeatRequest) -> Result<HeartbeatResponse> {
        let request = self
            .request(Method::POST, "/api/vision/heartbeat")
            .json(heartbeat);
        let request = if self.api_key.is_empty() {
            request.bearer_auth("")
        } else {
            request
        };
        let response = Self::send(request, "POST /api/vision/heartbeat").await?;
        Self::parse_json(response, "POST /api/vision/heartbeat").await
    }

    async fn send(request: RequestBuilder, operation: &str) -> Result<Response> {
        let response = request
            .send()
            .await
            .with_context(|| format!("{operation} 请求发送失败"))?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response
            .text()
            .await
            .unwrap_or_else(|error| format!("<读取响应正文失败: {error}>"));
        bail!("{operation} 返回 HTTP {status}: {body}")
    }

    async fn parse_json<T: DeserializeOwned>(response: Response, operation: &str) -> Result<T> {
        let body = response
            .bytes()
            .await
            .with_context(|| format!("{operation} 响应读取失败"))?;
        serde_json::from_slice(&body).with_context(|| {
            format!(
                "{operation} 响应解析失败: {}",
                String::from_utf8_lossy(&body)
            )
        })
    }
}

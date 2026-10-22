use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, Duration};

use crate::config::NvrConfig;

#[derive(Clone, Debug, Deserialize)]
pub struct SegmentCompleted {
    pub camera_id: String,
    pub file_path: String,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub encoding: String,
    pub started_at: String,
    #[serde(default)]
    pub ended_at: String,
    pub file_size: i64,
    pub recording_id: String,
    #[serde(default)]
    pub layer: i32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SegmentDeleted {
    pub recording_id: String,
    #[serde(default)]
    pub camera_id: String,
    #[serde(default)]
    pub file_path: String,
    #[serde(default)]
    pub reason: String,
}

#[derive(Clone, Debug)]
pub enum SegmentEvent {
    Completed(SegmentCompleted),
    Deleted(SegmentDeleted),
}

#[derive(Deserialize)]
struct SseEnvelope {
    #[serde(rename = "Topic")]
    topic: String,
    #[serde(rename = "Data")]
    data: serde_json::Value,
}

pub async fn run(
    config: NvrConfig,
    camera: Option<String>,
    sender: mpsc::Sender<SegmentEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let client = reqwest::Client::new();
    let mut attempts = 0u32;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        tracing::info!("建立 SSE 连接...");
        let base = config.api_base_url.trim_end_matches('/');
        let mut request = client
            .get(format!("{base}/api/events"))
            .query(&[("filter", config.sse_filter.clone())])
            .header(reqwest::header::ACCEPT, "text/event-stream");
        if !config.api_key.is_empty() {
            request = request.bearer_auth(&config.api_key);
        }
        let response = request.send().await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                retry_delay(attempts, "SSE 连接失败,指数退避后重试", Some(error)).await;
                attempts = attempts.saturating_add(1);
                continue;
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            tracing::warn!(%status, "SSE 连接返回");
            retry_delay(attempts, "SSE 连接失败,指数退避后重试", None).await;
            attempts = attempts.saturating_add(1);
            continue;
        }
        attempts = 0;
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();
        let mut event_name = String::new();
        let mut data = String::new();
        let mut stream_error = None;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                }
                chunk = stream.next() => {
                    match chunk {
                        None => break,
                        Some(Err(error)) => {
                            stream_error = Some(error);
                            break;
                        }
                        Some(Ok(chunk)) => {
                            buffer.push_str(&String::from_utf8_lossy(&chunk));
                            while let Some(position) = buffer.find('\n') {
                                let mut line = buffer.drain(..=position).collect::<String>();
                                line = line.trim_end_matches(['\r', '\n']).to_string();
                                if line.is_empty() {
                                    dispatch_event(&event_name, &data, &config, camera.as_deref(), &sender).await?;
                                    event_name.clear();
                                    data.clear();
                                } else if let Some(comment) = line.strip_prefix(':') {
                                    let _ = comment;
                                } else if let Some(value) = line.strip_prefix("event:") {
                                    event_name = value.trim().to_string();
                                } else if let Some(value) = line.strip_prefix("data:") {
                                    if !data.is_empty() {
                                        data.push('\n');
                                    }
                                    data.push_str(value.trim_start());
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(error) = stream_error {
            tracing::warn!(%error, "SSE 流读取失败");
        }
        if *shutdown.borrow() {
            return Ok(());
        }
        retry_delay(attempts, "SSE 流结束,指数退避后重连", None).await;
        attempts = attempts.saturating_add(1);
    }
}

async fn dispatch_event(
    event_name: &str,
    data: &str,
    config: &NvrConfig,
    camera: Option<&str>,
    sender: &mpsc::Sender<SegmentEvent>,
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let envelope: SseEnvelope = serde_json::from_str(data).context("解析 SSE 事件失败")?;
    let topic = if event_name.is_empty() {
        envelope.topic.as_str()
    } else {
        event_name
    };
    match topic {
        "segment.completed" => {
            let segment: SegmentCompleted = serde_json::from_value(envelope.data)?;
            if camera_allowed(&segment.camera_id, config, camera) {
                sender.send(SegmentEvent::Completed(segment)).await?;
            }
        }
        "segment.deleted" => {
            let segment: SegmentDeleted = serde_json::from_value(envelope.data)?;
            if segment.camera_id.is_empty() || camera_allowed(&segment.camera_id, config, camera) {
                sender.send(SegmentEvent::Deleted(segment)).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn camera_allowed(camera_id: &str, config: &NvrConfig, camera: Option<&str>) -> bool {
    if camera.is_some_and(|camera| camera != camera_id) {
        return false;
    }
    let _ = config;
    true
}

async fn retry_delay(attempts: u32, message: &'static str, error: Option<reqwest::Error>) {
    let maximum = 60.0;
    let base = 2.0f64.powi(attempts.min(5) as i32);
    let jitter = rand::random::<f64>() * 3.0 + 2.0;
    let delay = (base * jitter).min(maximum);
    if let Some(error) = error {
        tracing::warn!(e = ?error, attempts, delay, "{message}");
    } else {
        tracing::warn!(attempts, delay, "{message}");
    }
    sleep(Duration::from_secs_f64(delay)).await;
}

pub fn parse_segment_completed(data: &str) -> Result<SegmentCompleted> {
    let envelope: SseEnvelope = serde_json::from_str(data)?;
    if envelope.topic != "segment.completed" {
        bail!("不是 segment.completed 事件");
    }
    Ok(serde_json::from_value(envelope.data)?)
}

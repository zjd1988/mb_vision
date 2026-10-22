use std::convert::Infallible;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::assets::PUSH_MONITOR_HTML;
use crate::config::{AppConfig, QueueConfig};
use crate::framebus::drop_report::DropMetadata;
use crate::framebus::governor::Governor;
use crate::framebus::{channel, BusReceiver, BusSender, DropItem, DropReason, SendError};

pub use crate::framebus::PushMetrics;

pub type SegmentSender = BusSender<UploadSegment>;
pub type SegmentReceiver = BusReceiver<UploadSegment>;

#[derive(Debug)]
pub struct UploadSegment {
    pub path: PathBuf,
    pub recording_id: String,
    pub camera_id: String,
    pub format: String,
    pub encoding: String,
    pub started_at: String,
    pub ended_at: String,
    pub file_size: u64,
    pub declared_file_size: Option<u64>,
    pub layer: String,
    pub received_at: SystemTime,
}

impl UploadSegment {
    pub fn cleanup(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            result => result,
        }
    }
}

impl DropItem for UploadSegment {
    fn drop_metadata(&self) -> DropMetadata {
        DropMetadata {
            camera_id: self.camera_id.clone(),
            recording_id: self.recording_id.clone(),
            started_at: self.started_at.clone(),
            ended_at: self.ended_at.clone(),
        }
    }

    fn on_drop(self) {
        let _ = self.cleanup();
    }
}

#[derive(Clone)]
struct State {
    config: Arc<AppConfig>,
    sender: SegmentSender,
    metrics: Arc<PushMetrics>,
}

#[derive(Serialize)]
struct MonitorResponse {
    decode_alive: usize,
    decode_busy: usize,
    decode_target: usize,
    decoded_bus: crate::framebus::BusSnapshot,
    drop_ranges: Vec<crate::framebus::DropRange>,
    governor: String,
    in_flight: u64,
    metrics: crate::framebus::MetricsSnapshot,
    pending_drops: u64,
    processed: u64,
    seg_bus: crate::framebus::BusSnapshot,
    status: &'static str,
}

pub fn segment_channel(
    config: &QueueConfig,
    metrics: &PushMetrics,
) -> (SegmentSender, SegmentReceiver) {
    channel(
        config.capacity,
        config.ttl_secs,
        &config.on_full,
        metrics.drop_report(),
    )
}

pub async fn run<A, C>(
    addr: A,
    config: C,
    sender: SegmentSender,
    metrics: Arc<PushMetrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()>
where
    A: tokio::net::ToSocketAddrs,
    C: Into<Arc<AppConfig>>,
{
    let config = config.into();
    metrics.configure_queue(&config.queue);
    tokio::fs::create_dir_all(&config.transport.http_temp_dir)
        .await
        .with_context(|| {
            format!(
                "创建上传临时目录失败 {}",
                config.transport.http_temp_dir.display()
            )
        })?;
    let listener = TcpListener::bind(addr)
        .await
        .context("绑定推送 HTTP 地址失败")?;
    let governor = Governor::new(config.governor.clone(), metrics.clone());
    let governor_task = tokio::spawn(governor.run(shutdown.clone()));
    let state = State {
        config,
        sender,
        metrics,
    };

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(%error, "接受推送 HTTP 连接失败");
                        continue;
                    }
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let state = state.clone();
                        async move { Ok::<_, Infallible>(handle(request, state).await) }
                    });
                    let connection = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service);
                    if let Err(error) = connection.await {
                        tracing::debug!(%error, %peer, "推送 HTTP 连接结束");
                    }
                });
            }
        }
    }

    governor_task.await.context("governor 任务异常结束")?;
    Ok(())
}

async fn handle(request: Request<Incoming>, state: State) -> Response<Full<Bytes>> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => {
            json_bytes(StatusCode::OK, Bytes::from_static(b"{\"status\":\"ok\"}"))
        }
        (&Method::GET, "/monitor") => response(
            StatusCode::OK,
            "text/html; charset=utf-8",
            Bytes::from_static(PUSH_MONITOR_HTML.as_bytes()),
        ),
        (&Method::GET, "/monitor/json") => monitor_json(&state),
        (&Method::POST, "/vision/segment/upload") => upload(request, state).await,
        _ => error_response(StatusCode::NOT_FOUND, "not found"),
    }
}

fn monitor_json(state: &State) -> Response<Full<Bytes>> {
    let segment = state.sender.snapshot();
    let decoded = state.metrics.decoded_bus_snapshot();
    let metrics = state.metrics.snapshot(&segment, &decoded);
    let drops = state.metrics.drop_report();
    let snapshot = MonitorResponse {
        decode_alive: state.metrics.decode_alive(),
        decode_busy: state.metrics.decode_busy(),
        decode_target: state.metrics.decode_target(),
        decoded_bus: decoded,
        drop_ranges: drops.ranges(),
        governor: state.metrics.governor_status(),
        in_flight: state.metrics.in_flight(),
        metrics,
        pending_drops: drops.pending_count(),
        processed: state.metrics.processed(),
        seg_bus: segment,
        status: state.metrics.health_status(),
    };
    json(StatusCode::OK, &snapshot)
}

struct UploadHeaders {
    recording_id: String,
    camera_id: String,
    format: String,
    encoding: String,
    started_at: String,
    ended_at: String,
    declared_file_size: Option<u64>,
    layer: String,
    content_length: u64,
}

impl UploadHeaders {
    fn parse(headers: &HeaderMap) -> std::result::Result<Self, Response<Full<Bytes>>> {
        let recording_id = header(headers, "x-recording-id");
        let camera_id = header(headers, "x-camera-id");
        let (Some(recording_id), Some(camera_id)) = (recording_id, camera_id) else {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "missing recording-id or camera-id",
            ));
        };
        let content_length = headers
            .get(CONTENT_LENGTH)
            .ok_or_else(|| error_response(StatusCode::LENGTH_REQUIRED, "missing content-length"))?
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| error_response(StatusCode::BAD_REQUEST, "invalid content-length"))?;
        Ok(Self {
            recording_id,
            camera_id,
            format: header(headers, "x-format").unwrap_or_default(),
            encoding: header(headers, "x-encoding").unwrap_or_default(),
            started_at: header(headers, "x-started-at").unwrap_or_default(),
            ended_at: header(headers, "x-ended-at").unwrap_or_default(),
            declared_file_size: header(headers, "x-file-size").and_then(|value| value.parse().ok()),
            layer: header(headers, "x-layer").unwrap_or_default(),
            content_length,
        })
    }
}

async fn upload(request: Request<Incoming>, state: State) -> Response<Full<Bytes>> {
    let headers = match UploadHeaders::parse(request.headers()) {
        Ok(headers) => headers,
        Err(response) => return response,
    };
    let camera = safe_component(&headers.camera_id, "camera");
    let recording = safe_component(&headers.recording_id, "recording");
    let extension = safe_extension(&headers.format);
    let (path, mut file) = match create_upload_file(
        &state.config.transport.http_temp_dir,
        &camera,
        &recording,
        &extension,
    )
    .await
    {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(%error, "创建上传临时文件失败");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to store upload");
        }
    };

    let written = match write_body(request.into_body(), &mut file).await {
        Ok(written) => written,
        Err(BodyWriteError::Body(error)) => {
            drop(file);
            let _ = tokio::fs::remove_file(&path).await;
            tracing::debug!(%error, "读取上传请求体失败");
            return error_response(StatusCode::BAD_REQUEST, "invalid upload body");
        }
        Err(BodyWriteError::Io(error)) => {
            drop(file);
            let _ = tokio::fs::remove_file(&path).await;
            tracing::warn!(%error, "写入上传临时文件失败");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to store upload");
        }
    };
    drop(file);
    if written != headers.content_length {
        let _ = tokio::fs::remove_file(&path).await;
        return error_response(StatusCode::BAD_REQUEST, "content-length mismatch");
    }

    let segment = UploadSegment {
        path,
        recording_id: headers.recording_id,
        camera_id: headers.camera_id,
        format: headers.format,
        encoding: headers.encoding,
        started_at: headers.started_at,
        ended_at: headers.ended_at,
        file_size: written,
        declared_file_size: headers.declared_file_size,
        layer: headers.layer,
        received_at: SystemTime::now(),
    };
    match state.sender.send(segment).await {
        Ok(()) => json_bytes(StatusCode::ACCEPTED, Bytes::from_static(b"{\"ok\":true}")),
        Err(SendError {
            reason: DropReason::QueueFull,
        }) => error_response(StatusCode::SERVICE_UNAVAILABLE, "queue full"),
        Err(_) => error_response(StatusCode::SERVICE_UNAVAILABLE, "queue unavailable"),
    }
}

enum BodyWriteError {
    Body(hyper::Error),
    Io(io::Error),
}

async fn write_body(body: Incoming, file: &mut File) -> std::result::Result<u64, BodyWriteError> {
    let mut body = body;
    let mut written = 0u64;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(BodyWriteError::Body)?;
        if let Ok(data) = frame.into_data() {
            file.write_all(data.as_ref())
                .await
                .map_err(BodyWriteError::Io)?;
            written = written.saturating_add(data.len() as u64);
        }
    }
    file.flush().await.map_err(BodyWriteError::Io)?;
    Ok(written)
}

async fn create_upload_file(
    directory: &Path,
    camera: &str,
    recording: &str,
    extension: &str,
) -> io::Result<(PathBuf, File)> {
    let path = directory.join(format!("{camera}_{recording}.{extension}"));
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await?;
    Ok((path, file))
}

fn safe_component(value: &str, fallback: &str) -> String {
    let mut output = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .take(80)
        .collect::<String>();
    if output.is_empty() {
        output = fallback.to_owned();
    }
    if output.chars().all(|character| character == '.') {
        output = "_".repeat(output.len());
    }
    output
}

fn safe_extension(format: &str) -> String {
    let extension = format
        .trim()
        .trim_start_matches('.')
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(12)
        .collect::<String>()
        .to_ascii_lowercase();
    match extension.as_str() {
        "mp4" | "mkv" | "dav" | "flv" | "avi" => extension,
        _ => "mp4".to_owned(),
    }
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)?
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn json<T: Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
    match serde_json::to_vec(value) {
        Ok(body) => json_bytes(status, Bytes::from(body)),
        Err(_) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "serialization failed"),
    }
}

fn error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    json(status, &serde_json::json!({ "error": message }))
}

fn json_bytes(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
    response(status, "application/json", body)
}

fn response(status: StatusCode, content_type: &'static str, body: Bytes) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .header(CONNECTION, "close")
        .body(Full::new(body))
        .expect("static HTTP response")
}

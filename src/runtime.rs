use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use tokio::sync::{mpsc, watch, Mutex};

use crate::assets;
use crate::config::AppConfig;
use crate::framebus::{DropBatch, DropItem, DropMetadata, MetricsSnapshot, PushMetrics};
use crate::integration::push_server;
use crate::integration::sse_client::{self, SegmentEvent};
use crate::pipeline::{decode_video, file_size, Pipeline, SegmentJob};
use crate::sink::nvr_api::{
    DropRange as NvrDropRange, DropReport as NvrDropReport, HeartbeatRequest,
};
use crate::sink::NvrClient;
use crate::source::VideoFrame;
use crate::storage::Storage;
use crate::webui::{self, WebUiState};

struct DecodedSegment {
    job: SegmentJob,
    frames: Vec<VideoFrame>,
    decode_failures: u64,
    started_at: Instant,
    started_at_text: String,
    ended_at_text: String,
    metrics: Arc<PushMetrics>,
}

impl DropItem for DecodedSegment {
    fn drop_metadata(&self) -> DropMetadata {
        DropMetadata {
            camera_id: self.job.camera_id.clone(),
            recording_id: self.job.recording_id.clone(),
            started_at: self.started_at_text.clone(),
            ended_at: self.ended_at_text.clone(),
        }
    }

    fn on_drop(self) {
        let _ = std::fs::remove_file(&self.job.source_path);
        self.metrics.segment_abandoned();
    }
}

#[derive(Clone)]
struct DecodeWorkerContext {
    receiver: Arc<Mutex<push_server::SegmentReceiver>>,
    sender: crate::framebus::BusSender<DecodedSegment>,
    metrics: Arc<PushMetrics>,
    config: Arc<AppConfig>,
    shutdown: watch::Receiver<bool>,
}

pub async fn run_watch(config: AppConfig, camera: Option<String>) -> Result<()> {
    config.validate_lite()?;
    ensure_model(&config)?;
    println!(
        "连接 NVR SSE: {} (filter={})",
        config.nvr.api_base_url, config.nvr.sse_filter
    );
    let (sender, mut receiver) = mpsc::channel(config.queue.capacity.max(1));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sse_task = tokio::spawn(sse_client::run(
        config.nvr.clone(),
        camera.clone(),
        sender,
        shutdown_rx,
    ));
    let mut pipeline = Pipeline::new(config.clone()).await?;
    let mut seen = HashSet::new();

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                let _ = shutdown_tx.send(true);
                sse_task.abort();
                return Ok(());
            }
            result = &mut sse_task => {
                result.context("SSE 任务异常结束")??;
                return Ok(());
            }
            event = receiver.recv() => {
                let Some(event) = event else { return Ok(()); };
                match event {
                    SegmentEvent::Completed(segment) => {
                        if !seen.insert(segment.recording_id.clone()) {
                            continue;
                        }
                        if !camera_selected(&config, camera.as_deref(), &segment.camera_id) {
                            continue;
                        }
                        match segment_job(&config, &segment).await {
                            Ok(job) => {
                                let recording_id = segment.recording_id.clone();
                                let cleanup_download = config.transport.r#type == "http_download";
                                let source_path = job.source_path.clone();
                                if let Err(error) = pipeline.process(job).await {
                                    tracing::warn!(recording_id, ?error, "片段处理失败");
                                }
                                if cleanup_download {
                                    let _ = std::fs::remove_file(source_path);
                                }
                            }
                            Err(error) => tracing::warn!(recording_id = segment.recording_id, ?error, "准备录像失败"),
                        }
                    }
                    SegmentEvent::Deleted(segment) => {
                        seen.remove(&segment.recording_id);
                        tracing::info!(recording_id = segment.recording_id, "录像已被删除(收到 segment.deleted)");
                    }
                }
            }
        }
    }
}

pub async fn run_push(config: AppConfig, addr: String) -> Result<()> {
    config.validate_lite()?;
    ensure_model(&config)?;
    println!(
        "推送模式启动(两段式): {} → NVR {} (心跳 30s, 协议 v2)",
        addr, config.nvr.api_base_url
    );
    let config = Arc::new(config);
    let metrics = Arc::new(PushMetrics::new(&config.queue));
    metrics.set_decode_target(config.governor.max_decode_workers.max(1));
    let (sender, receiver) = push_server::segment_channel(&config.queue, &metrics);
    let (decoded_sender, mut decoded_receiver) = crate::framebus::channel(
        config.queue.decoded_capacity,
        config.queue.ttl_secs,
        &config.queue.on_full,
        metrics.drop_report(),
    );
    metrics.register_decoded_bus(decoded_sender.observer());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_context = DecodeWorkerContext {
        receiver: Arc::new(Mutex::new(receiver)),
        sender: decoded_sender,
        metrics: metrics.clone(),
        config: config.clone(),
        shutdown: shutdown_rx.clone(),
    };
    let mut workers = tokio::spawn(decode_worker_supervisor(worker_context));
    let mut server = tokio::spawn(push_server::run(
        addr,
        config.clone(),
        sender.clone(),
        metrics.clone(),
        shutdown_rx,
    ));
    let heartbeat_shutdown = shutdown_tx.subscribe();
    let heartbeat_metrics = metrics.clone();
    let heartbeat_sender = sender.clone();
    let heartbeat_config = config.as_ref().clone();
    let mut heartbeat = tokio::spawn(heartbeat_loop(
        heartbeat_config,
        heartbeat_metrics,
        heartbeat_sender,
        heartbeat_shutdown,
    ));
    let mut pipeline = Pipeline::new(config.as_ref().clone()).await?;

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                let _ = shutdown_tx.send(true);
                workers.abort();
                server.abort();
                heartbeat.abort();
                return Ok(());
            }
            result = &mut workers => {
                result.context("解码 worker 池异常结束")??;
                return Ok(());
            }
            result = &mut server => {
                result.context("推送服务器异常结束")??;
                return Ok(());
            }
            result = &mut heartbeat => {
                result.context("心跳任务异常结束")??;
                return Ok(());
            }
            segment = decoded_receiver.recv() => {
                let Some(segment) = segment else { return Ok(()); };
                let recording_id = segment.job.recording_id.clone();
                let started = segment.started_at;
                let result = pipeline
                    .process_materialized(segment.job, segment.frames, segment.decode_failures)
                    .await;
                metrics.segment_finished(started.elapsed());
                match result {
                    Ok(report) => {
                        metrics.add_events(report.events);
                        tracing::info!(
                            recording_id,
                            frames = report.frames_processed,
                            events = report.events,
                            "片段处理完成"
                        );
                    }
                    Err(error) => tracing::warn!(recording_id, ?error, "片段处理失败"),
                }
            }
        }
    }
}

async fn decode_worker_supervisor(context: DecodeWorkerContext) -> Result<()> {
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut next_worker_id = 0usize;
    let mut shutdown = context.shutdown.clone();
    let mut interval = tokio::time::interval(Duration::from_millis(200));
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    for handle in &handles {
                        handle.abort();
                    }
                    return Ok(());
                }
            }
            _ = interval.tick() => {
                handles.retain(|handle: &tokio::task::JoinHandle<()>| !handle.is_finished());
                let target = context.metrics.decode_target();
                while handles.len() < target {
                    let worker_id = next_worker_id;
                    next_worker_id += 1;
                    handles.push(tokio::spawn(decode_worker(worker_id, context.clone())));
                }
            }
        }
    }
}

async fn decode_worker(worker_id: usize, context: DecodeWorkerContext) {
    context.metrics.decode_worker_started();
    loop {
        if *context.shutdown.borrow() || worker_id >= context.metrics.decode_target() {
            break;
        }
        let segment = {
            let mut receiver = context.receiver.lock().await;
            match tokio::time::timeout(Duration::from_millis(200), receiver.recv()).await {
                Ok(Some(segment)) => segment,
                Ok(None) => break,
                Err(_) => continue,
            }
        };
        decode_upload_segment(worker_id, segment, &context).await;
    }
    context.metrics.decode_worker_finished();
}

async fn decode_upload_segment(
    worker_id: usize,
    segment: push_server::UploadSegment,
    context: &DecodeWorkerContext,
) {
    let layer = if segment.layer.is_empty() {
        "main"
    } else {
        segment.layer.as_str()
    };
    tracing::info!(
        recording_id = segment.recording_id,
        camera_id = segment.camera_id,
        layer,
        size_bytes = segment.file_size,
        "收到 NVR 视频推送"
    );
    context.metrics.segment_started();
    context.metrics.decode_worker_busy();
    decode_upload_segment_inner(worker_id, segment, context).await;
    context.metrics.decode_worker_idle();
}

async fn decode_upload_segment_inner(
    worker_id: usize,
    segment: push_server::UploadSegment,
    context: &DecodeWorkerContext,
) {
    let started_at = Instant::now();
    let job = SegmentJob {
        camera_id: segment.camera_id.clone(),
        recording_id: segment.recording_id.clone(),
        source_path: segment.path.clone(),
        file_size: segment.file_size,
        report_nvr: true,
        mark_processed: false,
        realtime: false,
        headers: None,
    };
    let nvr = NvrClient::new(&context.config.nvr);
    if let Err(error) = nvr
        .update_ai_status(&job.recording_id, "processing", None)
        .await
    {
        tracing::warn!(
            recording_id = job.recording_id,
            ?error,
            "回写 AI 状态失败,跳过该段"
        );
        let _ = segment.cleanup();
        context.metrics.segment_abandoned();
        return;
    }
    tracing::info!(
        recording_id = job.recording_id,
        camera_id = job.camera_id,
        worker_id,
        "开始解码推送段(视频已接收)"
    );
    let (frames, decode_failures) = match decode_video(
        &job.source_path,
        context.config.source.nvr_files.sample_fps as f64,
        context.config.source.nvr_files.max_side,
        None,
    ) {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(?error, path = ?job.source_path, "打开视频失败,跳过");
            (Vec::new(), 0)
        }
    };
    let _ = segment.cleanup();
    let decoded = DecodedSegment {
        job,
        frames,
        decode_failures,
        started_at,
        started_at_text: segment.started_at,
        ended_at_text: segment.ended_at,
        metrics: context.metrics.clone(),
    };
    if let Err(error) = context.sender.send(decoded).await {
        tracing::warn!(reason = %error.reason.as_str(), "decoded 队列丢弃上传段");
    }
}

pub async fn run_serve(config_path: PathBuf, config: AppConfig, addr: String) -> Result<()> {
    let listen = addr.clone();
    let (reload_tx, mut reload_rx) = watch::channel(config.clone());
    let state = Arc::new(WebUiState::new(config_path, config, reload_tx));
    refresh_recent_events(&state).await;
    println!("配置页: http://{}(保存后自动重启管线)", listen);
    let (web_shutdown_tx, web_shutdown_rx) = watch::channel(false);
    let web_state = state.clone();
    let mut web_task = tokio::spawn(async move {
        webui::run(addr, web_state, async move {
            let mut shutdown = web_shutdown_rx;
            loop {
                if *shutdown.borrow() {
                    break;
                }
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
    });
    tracing::info!(listen, "配置页已启动");

    let initial = state.config.read().await.clone();
    let (mut pipeline_task, mut pipeline_shutdown) = match startable_pipeline_config(&initial) {
        Ok(()) => {
            let (task, shutdown) = spawn_watch_pipeline(&state, initial);
            (Some(task), Some(shutdown))
        }
        Err(error) => {
            *state.last_pipeline_error.write().await = Some(error.to_string());
            (None, None)
        }
    };
    let mut refresh = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                let _ = web_shutdown_tx.send(true);
                if let Some(shutdown) = pipeline_shutdown.take() {
                    let _ = shutdown.send(true);
                }
                if let Some(task) = pipeline_task.take() {
                    task.abort();
                }
                web_task.abort();
                return Ok(());
            }
            result = &mut web_task => {
                result.context("配置页任务异常结束")??;
                return Ok(());
            }
            _ = refresh.tick() => refresh_recent_events(&state).await,
            changed = reload_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                if let Some(shutdown) = pipeline_shutdown.take() {
                    let _ = shutdown.send(true);
                }
                if let Some(task) = pipeline_task.take() {
                    task.abort();
                }
                let next = reload_rx.borrow().clone();
                let version = state.version.load(std::sync::atomic::Ordering::Relaxed);
                tracing::info!(version, "配置已变更,重启管线");
                match startable_pipeline_config(&next) {
                    Ok(()) => {
                        let (task, shutdown) = spawn_watch_pipeline(&state, next);
                        pipeline_task = Some(task);
                        pipeline_shutdown = Some(shutdown);
                    }
                    Err(error) => {
                        state.pipeline_running.store(false, std::sync::atomic::Ordering::Relaxed);
                        *state.last_pipeline_error.write().await = Some(error.to_string());
                    }
                }
            }
        }
    }
}

pub async fn run_watch_stream(config: AppConfig, camera: String) -> Result<()> {
    config.validate_lite()?;
    ensure_model(&config)?;
    let (source, headers) = if !config.transport.rtsp_url_template.is_empty() {
        (
            config
                .transport
                .rtsp_url_template
                .replace("{camera_id}", &camera)
                .replace("{id}", &camera),
            None,
        )
    } else if config.nvr.mode == "rtsp_pull" {
        let nvr = NvrClient::new(&config.nvr);
        let cameras = nvr.cameras().await?;
        let camera_info = cameras
            .iter()
            .find(|item| item.id == camera)
            .with_context(|| format!("NVR 中不存在摄像头 {camera}"))?;
        let url =
            if camera_info.url.starts_with("rtsp://") || camera_info.url.starts_with("rtsps://") {
                camera_info.url.clone()
            } else {
                camera_info.sub_stream_url.clone()
            };
        if url.is_empty() {
            bail!("摄像头 {camera} 没有可用 RTSP 地址");
        }
        (url, None)
    } else {
        println!("连接摄像头 {} 的实时流(FLV)...", camera);
        if config.nvr.admin_user.is_empty() || config.nvr.admin_password.is_empty() {
            bail!("watch-stream 需要 [nvr] admin_user / admin_password 配置(用于 FLV 拉流 Basic Auth)");
        }
        let credentials = format!("{}:{}", config.nvr.admin_user, config.nvr.admin_password);
        let token = general_purpose::STANDARD.encode(credentials);
        (
            format!(
                "{}/api/cameras/{}/stream.flv",
                config.nvr.api_base_url.trim_end_matches('/'),
                camera
            ),
            Some(format!("Authorization: Basic {token}\r\n")),
        )
    };
    if headers.is_some() {
        tracing::info!(camera_id = camera, "连接实时流(FLV)");
    } else {
        tracing::info!(camera_id = camera, source, "连接实时流(RTSP)");
    }
    let mut pipeline = Pipeline::new(config).await?;
    let job = SegmentJob {
        camera_id: camera.clone(),
        recording_id: String::new(),
        source_path: PathBuf::from(source),
        file_size: 0,
        report_nvr: false,
        mark_processed: false,
        realtime: true,
        headers,
    };
    let report = pipeline.process_realtime(job).await?;
    tracing::info!(
        camera,
        frames = report.frames_processed,
        "实时流结束,watch-stream 退出"
    );
    Ok(())
}

fn startable_pipeline_config(config: &AppConfig) -> Result<()> {
    ensure_model(config)?;
    config.validate_lite()?;
    if config.nvr.api_base_url.trim().is_empty() {
        bail!("未配置 [nvr].api_base_url,管线未启动(请在配置页填写 NVR 地址)");
    }
    Ok(())
}

fn spawn_watch_pipeline(
    state: &Arc<WebUiState>,
    config: AppConfig,
) -> (tokio::task::JoinHandle<Result<()>>, watch::Sender<bool>) {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    state
        .pipeline_running
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let pipeline_state = state.clone();
    let task = tokio::spawn(async move {
        let result = run_watch_until_shutdown(config, shutdown_rx).await;
        if let Err(error) = &result {
            *pipeline_state.last_pipeline_error.write().await = Some(error.to_string());
        }
        pipeline_state
            .pipeline_running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        result
    });
    (task, shutdown_tx)
}

async fn run_watch_until_shutdown(
    config: AppConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if *shutdown.borrow() {
        return Ok(());
    }
    run_watch(config, None).await
}

async fn segment_job(
    config: &AppConfig,
    segment: &sse_client::SegmentCompleted,
) -> Result<SegmentJob> {
    let source_path = match config.transport.r#type.as_str() {
        "local_file" => {
            let path = PathBuf::from(&segment.file_path);
            if path.is_absolute() {
                path
            } else {
                config.nvr.storage_root.join(&segment.file_path)
            }
        }
        "http_download" => {
            NvrClient::new(&config.nvr)
                .download_recording(&segment.recording_id, &config.transport.http_temp_dir)
                .await?
        }
        other => bail!("不支持的 transport.type `{other}`"),
    };
    let size = file_size(&source_path)?;
    Ok(SegmentJob {
        camera_id: segment.camera_id.clone(),
        recording_id: segment.recording_id.clone(),
        source_path,
        file_size: size,
        report_nvr: true,
        mark_processed: true,
        realtime: false,
        headers: None,
    })
}

fn camera_selected(config: &AppConfig, override_camera: Option<&str>, camera_id: &str) -> bool {
    if override_camera.is_some_and(|camera| camera != camera_id) {
        return false;
    }
    let whitelist = config
        .source
        .camera_whitelist
        .iter()
        .filter(|camera| !camera.starts_with('<'))
        .collect::<Vec<_>>();
    if !whitelist.is_empty() && !whitelist.iter().any(|camera| camera.as_str() == camera_id) {
        return false;
    }
    !config
        .source
        .camera_blacklist
        .iter()
        .any(|camera| camera == camera_id)
}

async fn heartbeat_loop(
    config: AppConfig,
    metrics: Arc<PushMetrics>,
    sender: push_server::SegmentSender,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = interval.tick() => {
                let nvr = NvrClient::new(&config.nvr);
                let request = heartbeat_request(&config, &metrics, &sender);
                match nvr.heartbeat(&request).await {
                    Ok(response) => {
                        if let Some(ack) = response.ack_drops {
                            metrics.drop_report().ack(ack as u64);
                        }
                    }
                    Err(error) => tracing::warn!(e = ?error, "心跳发送失败(丢弃暂存将随下次心跳重发)"),
                }
            }
        }
    }
}

fn heartbeat_request(
    config: &AppConfig,
    metrics: &PushMetrics,
    sender: &push_server::SegmentSender,
) -> HeartbeatRequest {
    let segment = sender.snapshot();
    let decoded = metrics.decoded_bus_snapshot();
    let snapshot = metrics.snapshot(&segment, &decoded);
    let drops = metrics.drop_report().next_report();
    HeartbeatRequest {
        device: config.detect.device.clone(),
        drops: drops.map(convert_drops),
        last_progress_secs: None,
        metrics: convert_metrics(snapshot),
        processed_count: metrics.processed(),
        protocol: 2,
        queue_depth: segment.depth,
        skip_cameras: config.source.camera_blacklist.clone(),
        status: metrics.health_status().into(),
    }
}

fn convert_metrics(metrics: MetricsSnapshot) -> crate::sink::nvr_api::HeartbeatMetrics {
    crate::sink::nvr_api::HeartbeatMetrics {
        decode_workers: metrics.decode_workers,
        decoded_queue_depth: metrics.decoded_queue_depth,
        dropped_queue_full: metrics.dropped_queue_full,
        dropped_total: metrics.dropped_total,
        dropped_ttl: metrics.dropped_ttl,
        events_emitted: metrics.events_emitted,
        load1: metrics.load1,
        mem_available_mb: metrics.mem_available_mb,
        queue_capacity: metrics.queue_capacity,
        received_total: metrics.received_total,
        seg_ms_p50: metrics.seg_ms_p50,
        seg_ms_p90: metrics.seg_ms_p90,
        workers_busy: metrics.workers_busy,
    }
}

fn convert_drops(batch: DropBatch) -> NvrDropReport {
    NvrDropReport {
        seq: batch.seq as i64,
        ranges: batch
            .ranges
            .into_iter()
            .map(|range| NvrDropRange {
                camera_id: range.camera_id,
                reason: range.reason,
                count: range.count,
                from: range.from,
                to: range.to,
                ids: range.ids,
            })
            .collect(),
    }
}

async fn refresh_recent_events(state: &WebUiState) {
    let config = state.config.read().await.clone();
    let Some(sqlite) = config.sink.sqlite.clone() else {
        return;
    };
    let path = sqlite.path.unwrap_or(config.general.db_path);
    if !path.exists() {
        return;
    }
    if let Ok(storage) = Storage::new(&path, sqlite.store_detections).await {
        if let Ok(events) = storage.recent_events(10).await {
            *state.recent_events.write().await = events
                .into_iter()
                .filter_map(|event| serde_json::to_value(event).ok())
                .collect();
        }
    }
}

fn ensure_model(config: &AppConfig) -> Result<()> {
    if assets::ensure_model(&config.detect.model_path)? {
        tracing::info!(p = ?config.detect.model_path, "内置模型已落盘(首次运行)");
    }
    Ok(())
}

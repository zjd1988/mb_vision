use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::AppConfig;
use crate::detect::{Detection, YoloDetector};
use crate::event::zone::ZoneEngine;
use crate::sink::gate::Gate;
use crate::sink::nvr_api::{AiEventRequest, NvrClient};
use crate::sink::snapshot::SnapshotSink;
use crate::sink::sqlite::Storage;
use crate::source::{VideoDecoder, VideoFrame};
use crate::track::bytetrack::Tracker;

#[derive(Clone, Debug)]
pub struct SegmentJob {
    pub camera_id: String,
    pub recording_id: String,
    pub source_path: PathBuf,
    pub file_size: u64,
    pub report_nvr: bool,
    pub mark_processed: bool,
    pub realtime: bool,
    pub headers: Option<String>,
}

impl SegmentJob {
    pub fn local(camera_id: String, source_path: PathBuf, file_size: u64) -> Self {
        Self {
            camera_id,
            recording_id: String::new(),
            source_path,
            file_size,
            report_nvr: false,
            mark_processed: false,
            realtime: false,
            headers: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessReport {
    pub frames_processed: u64,
    pub frames_failed: u64,
    pub events: u64,
}

pub struct Pipeline {
    config: AppConfig,
    detector: YoloDetector,
    storage: Option<Storage>,
    snapshots: Option<SnapshotSink>,
    nvr: NvrClient,
}

impl Pipeline {
    pub async fn new(config: AppConfig) -> Result<Self> {
        let detector = YoloDetector::new(&config.detect)?;
        let storage = match &config.sink.sqlite {
            Some(sqlite) => {
                let path = sqlite.path.as_deref().unwrap_or(&config.general.db_path);
                Some(Storage::new(path, sqlite.store_detections).await?)
            }
            None => None,
        };
        let snapshots = config.sink.snapshot.clone().map(SnapshotSink::new);
        let nvr = NvrClient::new(&config.nvr);
        Ok(Self {
            config,
            detector,
            storage,
            snapshots,
            nvr,
        })
    }

    pub async fn process(&mut self, job: SegmentJob) -> Result<ProcessReport> {
        if job.report_nvr {
            self.nvr
                .update_ai_status(&job.recording_id, "processing", None)
                .await?;
        }
        let (frames, decode_failures) = match decode_video(
            &job.source_path,
            self.config.source.nvr_files.sample_fps as f64,
            self.config.source.nvr_files.max_side,
            job.headers.as_deref(),
        ) {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(?error, path = ?job.source_path, "打开视频失败,跳过");
                (Vec::new(), 0)
            }
        };
        let result = self.process_frames(&job, frames, decode_failures).await;
        self.finish_ai_status(&job, &result).await?;
        result
    }

    pub async fn process_materialized(
        &mut self,
        job: SegmentJob,
        frames: Vec<VideoFrame>,
        decode_failures: u64,
    ) -> Result<ProcessReport> {
        let result = self.process_frames(&job, frames, decode_failures).await;
        self.finish_ai_status(&job, &result).await?;
        result
    }

    async fn finish_ai_status(
        &self,
        job: &SegmentJob,
        result: &Result<ProcessReport>,
    ) -> Result<()> {
        if !job.report_nvr {
            return Ok(());
        }
        match result {
            Ok(_) => {
                self.nvr
                    .update_ai_status(&job.recording_id, "completed", None)
                    .await
            }
            Err(error) => {
                self.nvr
                    .update_ai_status(&job.recording_id, "failed", Some(&error.to_string()))
                    .await
            }
        }
    }

    pub async fn process_realtime(&mut self, job: SegmentJob) -> Result<ProcessReport> {
        let mut report = ProcessReport::default();
        let mut decoder = match job.headers.as_deref() {
            Some(headers) => VideoDecoder::open_with_headers(
                &job.source_path,
                headers,
                self.config.source.nvr_files.sample_fps as f64,
                self.config.source.nvr_files.max_side,
            )?,
            None => VideoDecoder::open(
                &job.source_path,
                self.config.source.nvr_files.sample_fps as f64,
                self.config.source.nvr_files.max_side,
            )?,
        };
        let mut tracker = self
            .config
            .track
            .enabled
            .then(|| Tracker::new(&self.config.track));
        let mut zones = ZoneEngine::new(&self.config.event.zones)?;
        let mut gate = Gate::new(self.config.gate.clone(), self.config.nvr.clone());
        tracing::info!(camera = job.camera_id, "开始实时处理,Ctrl-C 停止");
        loop {
            let frame = match decoder.next_frame() {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    report.frames_failed += 1;
                    tracing::warn!(?error, "取帧失败,跳过");
                    break;
                }
            };
            report.frames_processed += 1;
            let (frame_width, frame_height) = frame.image.dimensions();
            let mut detections = match self.detector.detect(&frame.image) {
                Ok(detections) => detections,
                Err(error) => {
                    report.frames_failed += 1;
                    tracing::warn!(
                        ?error,
                        frame_idx = frame.frame_idx,
                        "实时事件触发检测失败,跳过该帧"
                    );
                    continue;
                }
            };
            if let Some(tracker) = &mut tracker {
                if let Err(error) = tracker.update(&mut detections) {
                    tracing::warn!(?error, "跟踪失败,跳过该帧");
                    continue;
                }
            }
            if let Some(storage) = &self.storage {
                storage
                    .write_detections(
                        &job.camera_id,
                        &job.source_path,
                        frame.frame_idx,
                        &detections,
                    )
                    .await?;
            }
            gate.update(&job.camera_id, &detections, frame_width, frame_height)
                .await?;
            let events = zones.update(
                &detections,
                frame.frame_idx,
                frame.timestamp_sec,
                frame_width,
                frame_height,
            );
            if !events.is_empty() {
                tracing::info!(
                    frame = frame.frame_idx + 1,
                    events = events.len(),
                    "实时事件触发"
                );
            }
            for event in events {
                let inserted = match &self.storage {
                    Some(storage) => {
                        storage
                            .write_event(&job.camera_id, &job.source_path, &event, None)
                            .await?
                    }
                    None => true,
                };
                if !inserted {
                    continue;
                }
                report.events += 1;
                if let Some(snapshots) = &self.snapshots {
                    let detection = Detection::new(event.bbox, event.confidence, event.class_id);
                    snapshots.save(
                        &job.camera_id,
                        &job.source_path,
                        frame.frame_idx,
                        &frame.image,
                        &[detection],
                    )?;
                }
            }
        }
        Ok(report)
    }

    async fn process_frames(
        &mut self,
        job: &SegmentJob,
        frames: Vec<VideoFrame>,
        decode_failures: u64,
    ) -> Result<ProcessReport> {
        let mut report = ProcessReport::default();
        report.frames_failed += decode_failures;
        tracing::info!(
            recording_id = job.recording_id,
            camera_id = job.camera_id,
            frames = frames.len(),
            "开始推理段(帧已物化)"
        );
        let mut tracker = self
            .config
            .track
            .enabled
            .then(|| Tracker::new(&self.config.track));
        let mut zones = ZoneEngine::new(&self.config.event.zones)?;
        let mut gate = job
            .realtime
            .then(|| Gate::new(self.config.gate.clone(), self.config.nvr.clone()));

        for frame in frames {
            report.frames_processed += 1;
            let (frame_width, frame_height) = frame.image.dimensions();
            let mut detections = match self.detector.detect(&frame.image) {
                Ok(detections) => detections,
                Err(error) => {
                    report.frames_failed += 1;
                    tracing::warn!(
                        ?error,
                        frame_idx = frame.frame_idx,
                        "实时事件触发检测失败,跳过该帧"
                    );
                    continue;
                }
            };
            if let Some(tracker) = &mut tracker {
                if let Err(error) = tracker.update(&mut detections) {
                    tracing::warn!(?error, "跟踪失败,跳过该帧");
                    continue;
                }
            }
            if let Some(storage) = &self.storage {
                storage
                    .write_detections(
                        &job.camera_id,
                        &job.source_path,
                        frame.frame_idx,
                        &detections,
                    )
                    .await?;
            }
            if let Some(gate) = &mut gate {
                gate.update(&job.camera_id, &detections, frame_width, frame_height)
                    .await?;
            }
            let events = zones.update(
                &detections,
                frame.frame_idx,
                frame.timestamp_sec,
                frame_width,
                frame_height,
            );
            if let Some(snapshots) = &self.snapshots {
                let on_event_only = self
                    .config
                    .sink
                    .snapshot
                    .as_ref()
                    .is_none_or(|config| config.on_event_only);
                if !on_event_only && !detections.is_empty() {
                    snapshots.save(
                        &job.camera_id,
                        &job.source_path,
                        frame.frame_idx,
                        &frame.image,
                        &detections,
                    )?;
                }
            }
            for event in events {
                let inserted = match &self.storage {
                    Some(storage) => {
                        storage
                            .write_event(&job.camera_id, &job.source_path, &event, None)
                            .await?
                    }
                    None => true,
                };
                if !inserted {
                    continue;
                }
                report.events += 1;
                let event_detection = Detection::new(event.bbox, event.confidence, event.class_id);
                if let Some(snapshots) = &self.snapshots {
                    snapshots.save(
                        &job.camera_id,
                        &job.source_path,
                        frame.frame_idx,
                        &frame.image,
                        &[event_detection.clone()],
                    )?;
                }
                if job.report_nvr {
                    let request = AiEventRequest::from_zone_event(
                        &job.camera_id,
                        &job.recording_id,
                        &event,
                        frame_width,
                        frame_height,
                        None,
                    )?;
                    let response = self.nvr.create_ai_event(&request).await?;
                    tracing::info!(event_id = response.id, "AI 事件已回写 NVR");
                    if self.config.nvr.upload_snapshots {
                        let jpeg = SnapshotSink::encode(
                            &frame.image,
                            &[event_detection],
                            self.config.nvr.snapshot_upload_draw_boxes,
                            self.config.nvr.snapshot_upload_quality,
                        )?;
                        self.nvr.upload_snapshot(response.id, &jpeg).await?;
                    }
                }
            }
        }
        if job.mark_processed {
            self.mark_processed(job, report.frames_processed).await?;
        }
        Ok(report)
    }

    async fn mark_processed(&self, job: &SegmentJob, frames: u64) -> Result<()> {
        if let Some(storage) = &self.storage {
            storage
                .mark_processed(&job.camera_id, &job.source_path, job.file_size, frames)
                .await?;
        }
        Ok(())
    }

    pub fn detector(&mut self) -> &mut YoloDetector {
        &mut self.detector
    }
}

pub fn file_size(path: &Path) -> Result<u64> {
    Ok(std::fs::metadata(path)
        .with_context(|| format!("读取视频文件大小失败 {}", path.display()))?
        .len())
}

pub fn decode_video(
    path: &Path,
    sample_fps: f64,
    max_side: u32,
    headers: Option<&str>,
) -> Result<(Vec<VideoFrame>, u64)> {
    let mut decoder = match headers {
        Some(headers) => VideoDecoder::open_with_headers(path, headers, sample_fps, max_side)?,
        None => VideoDecoder::open(path, sample_fps, max_side)?,
    };
    let mut frames = Vec::new();
    let mut failures = 0u64;
    loop {
        match decoder.next_frame() {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => break,
            Err(error) => {
                failures += 1;
                tracing::warn!(?error, "取帧失败,跳过");
                break;
            }
        }
    }
    Ok((frames, failures))
}

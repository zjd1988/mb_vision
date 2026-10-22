mod assets;
#[allow(dead_code)]
mod cli;
mod config;
#[allow(dead_code)]
mod detect;
mod event;
#[allow(dead_code)]
mod framebus;
#[allow(dead_code)]
mod integration;
#[allow(dead_code)]
mod pipeline;
mod runtime;
#[allow(dead_code)]
mod sink;
#[allow(dead_code)]
mod source;
#[allow(dead_code)]
mod storage;
mod track;
mod webui;

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use clap::Parser;
use cli::{Cli, Command, ConfigCommand};
use config::AppConfig;
use detect::yolo::YoloDetector;
use pipeline::{file_size, Pipeline, SegmentJob};
use sink::snapshot::SnapshotSink;
use source::{scan_nvr_files, VideoDecoder};
use storage::Storage;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let explicit_config = cli.config.is_some();
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from("config/config.toml"));
    let config = match &cli.command {
        Command::Config {
            command: ConfigCommand::Init,
        } if !explicit_config => AppConfig::default(),
        _ => load_config(&config_path, explicit_config)?,
    };
    init_logging(Some(
        cli.log_level
            .as_deref()
            .unwrap_or(config.general.log_level.as_str()),
    ));

    tracing::info!(
        edition = "lite",
        camera_cap = 1,
        targets = "linux-x86_64, linux-aarch64",
        "MiBeeVision 启动"
    );

    match &cli.command {
        Command::Config {
            command: ConfigCommand::Init,
        } => config::init_file(&config_path),
        Command::Config {
            command: ConfigCommand::Check,
        } => {
            config.print_summary();
            config.validate_lite()?;
            println!("版本规则校验通过。");
            Ok(())
        }
        Command::Scan { dir } => run_scan(config, dir.clone()).await,
        Command::Detect { file, output } => run_detect(config, file, output).await,
        Command::Events { camera, since } => {
            run_events(config, camera.as_deref(), since.as_deref()).await
        }
        Command::Watch { camera } => runtime::run_watch(config, camera.clone()).await,
        Command::Push { addr } => runtime::run_push(config, addr.clone()).await,
        Command::Serve { addr } => runtime::run_serve(config_path, config, addr.clone()).await,
        Command::WatchStream { camera } => runtime::run_watch_stream(config, camera.clone()).await,
    }
}

fn load_config(path: &Path, explicit: bool) -> Result<AppConfig> {
    if explicit {
        AppConfig::load(path)
    } else {
        AppConfig::load_or_default(path)
    }
}

async fn run_scan(mut config: AppConfig, dir: Option<PathBuf>) -> Result<()> {
    if let Some(dir) = dir {
        config.source.nvr_files.dir = dir;
    }
    config.validate_lite()?;
    ensure_model(&config)?;
    let files = scan_nvr_files(&config.source.nvr_files)?;
    if files.is_empty() {
        println!("目录中无视频文件。");
        return Ok(());
    }
    println!("扫描到 {} 个视频文件,开始批处理...", files.len());
    let mut pipeline = Pipeline::new(config).await?;
    let mut total = pipeline::ProcessReport::default();
    for file in files {
        let size = file_size(&file.path)?;
        let job = SegmentJob::local(file.camera_id, file.path, size);
        let report = pipeline.process(job).await?;
        total.frames_processed += report.frames_processed;
        total.frames_failed += report.frames_failed;
        total.events += report.events;
    }
    println!("批处理完成:");
    println!("  帧处理: {}", total.frames_processed);
    println!("  帧失败: {}", total.frames_failed);
    println!("  事件触发: {}", total.events);
    Ok(())
}

async fn run_detect(config: AppConfig, file: &Path, output: &Path) -> Result<()> {
    ensure_model(&config)?;
    if is_image(file) {
        let image = image::open(file)
            .with_context(|| format!("加载图片失败 {}", file.display()))?
            .to_rgb8();
        tracing::info!(file = ?file, w = image.width(), h = image.height(), "已加载图片");
        let mut detector = YoloDetector::new(&config.detect)?;
        let detections = detector.detect(&image)?;
        println!("检测到 {} 个目标:", detections.len());
        for detection in &detections {
            print_detection(detection);
        }
        let path = debug_snapshot_path(output, file, None);
        let bytes = SnapshotSink::encode(&image, &detections, true, 85)?;
        std::fs::create_dir_all(output)
            .with_context(|| format!("创建输出目录失败 {}", output.display()))?;
        std::fs::write(&path, bytes)
            .with_context(|| format!("创建快照文件失败: {}", path.display()))?;
        println!("带框快照已保存: {}", path.display());
        return Ok(());
    }

    let mut decoder = VideoDecoder::open(
        file,
        config.source.nvr_files.sample_fps as f64,
        config.source.nvr_files.max_side,
    )?;
    let mut detector = YoloDetector::new(&config.detect)?;
    let mut frames = 0u64;
    let mut frames_with_detections = 0u64;
    while let Some(frame) = decoder.next_frame()? {
        frames += 1;
        let detections = detector.detect(&frame.image)?;
        if detections.is_empty() {
            continue;
        }
        frames_with_detections += 1;
        println!(
            "\n帧 #{}/{:.2}s/{}x{}: {} 个目标",
            frame.frame_idx,
            frame.timestamp_sec,
            frame.image.width(),
            frame.image.height(),
            detections.len()
        );
        for detection in &detections {
            print_detection(detection);
        }
        let path = debug_snapshot_path(output, file, Some(frame.frame_idx));
        let bytes = SnapshotSink::encode(&frame.image, &detections, true, 85)?;
        std::fs::create_dir_all(output)
            .with_context(|| format!("创建输出目录失败 {}", output.display()))?;
        std::fs::write(&path, bytes)
            .with_context(|| format!("创建快照文件失败: {}", path.display()))?;
    }
    println!(
        "\n视频处理完成: 抽帧 {} 张,其中 {} 张有检测目标。",
        frames, frames_with_detections
    );
    Ok(())
}

async fn run_events(config: AppConfig, camera: Option<&str>, since: Option<&str>) -> Result<()> {
    let sqlite = config.sink.sqlite.as_ref();
    let path = sqlite
        .and_then(|sqlite| sqlite.path.as_deref())
        .unwrap_or(&config.general.db_path);
    if !path.exists() {
        println!("⚠ `events`: 数据库文件不存在: {}", path.display());
        println!("  先运行 `mibeevision scan` 产生事件后再查询。");
        println!("  路线图详见 docs/architecture.md");
        return Ok(());
    }
    let store_detections = sqlite.is_some_and(|sqlite| sqlite.store_detections);
    let storage = Storage::new(path, store_detections).await?;
    let mut events = if let Some(camera) = camera {
        storage.recent_events_for_camera(camera, 100).await?
    } else {
        storage.recent_events(100).await?
    };
    if let Some(since) = since {
        let range = parse_since(since)?;
        events.retain(|event| {
            DateTime::parse_from_rfc3339(&event.occurred_at)
                .map(|time| {
                    time.with_timezone(&Utc) >= range.0 && time.with_timezone(&Utc) < range.1
                })
                .unwrap_or(false)
        });
    }
    if events.is_empty() {
        println!("(无事件记录)");
        return Ok(());
    }
    println!("时间(UTC)              摄像头            类型             区域         track  class  置信度      源文件                     ");
    for event in &events {
        println!(
            "{:<22} {:<15} {:<15} {:<12} {:<6} {:<6} {:<9.2} {}",
            event.occurred_at,
            event.camera_id,
            event.event_type,
            event.zone_name,
            event.track_id,
            event.class_id,
            event.confidence,
            event.source_path
        );
    }
    println!("\n共 {} 条(最多显示 100 条)", events.len());
    Ok(())
}

fn ensure_model(config: &AppConfig) -> Result<()> {
    if assets::ensure_model(&config.detect.model_path)? {
        tracing::info!(p = ?config.detect.model_path, "内置模型已落盘(首次运行)");
    }
    Ok(())
}

fn print_detection(detection: &detect::Detection) {
    println!(
        "  [{:<14}] {:5.1}%  bbox=({},{})-({},{})",
        detection.class_name(),
        detection.confidence * 100.0,
        detection.bbox[0].round() as i32,
        detection.bbox[1].round() as i32,
        detection.bbox[2].round() as i32,
        detection.bbox[3].round() as i32
    );
}

fn debug_snapshot_path(output: &Path, source: &Path, frame_idx: Option<u64>) -> PathBuf {
    let stem = source
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_else(|| "frame".into());
    match frame_idx {
        Some(frame_idx) => output.join(format!("{stem}_f{frame_idx:04}_det.jpg")),
        None => output.join(format!("{stem}_det.jpg")),
    }
}

fn is_image(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "jpg" | "jpeg" | "png" | "bmp" | "gif" | "webp" | "tif" | "tiff"
            )
        })
        .unwrap_or(false)
}

fn parse_since(value: &str) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    if let Some(hours) = value.strip_suffix('h') {
        let hours: i64 = hours.parse().context("无效时间范围")?;
        if hours <= 0 {
            bail!("无效时间范围 `{value}`");
        }
        return Ok((
            Utc::now() - Duration::hours(hours),
            Utc::now() + Duration::seconds(1),
        ));
    }
    let (start, end) = value.split_once("..").context("无效时间范围")?;
    let start = parse_day(start)?;
    let end = parse_day(end)? + Duration::days(1);
    Ok((start, end))
}

fn parse_day(value: &str) -> Result<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").context("无效日期")?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid")))
}

fn init_logging(log_level: Option<&str>) {
    let level = log_level
        .and_then(|level| level.parse().ok())
        .unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(level)
        .init();
}

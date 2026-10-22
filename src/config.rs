use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub general: GeneralConfig,
    #[serde(default)]
    pub source: SourceConfig,
    #[serde(default)]
    pub detect: DetectConfig,
    #[serde(default)]
    pub track: TrackConfig,
    #[serde(default)]
    pub event: EventConfig,
    #[serde(default)]
    pub sink: SinkConfig,
    #[serde(default)]
    pub nvr: NvrConfig,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default)]
    pub gate: GateConfig,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub governor: GovernorConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            general: GeneralConfig::default(),
            source: SourceConfig::default(),
            detect: DetectConfig::default(),
            track: TrackConfig::default(),
            event: EventConfig {
                zones: vec![ZoneConfig::default()],
            },
            sink: SinkConfig {
                sqlite: Some(SqliteConfig::default()),
                snapshot: Some(SnapshotConfig::default()),
            },
            nvr: NvrConfig::default(),
            transport: TransportConfig::default(),
            gate: GateConfig::default(),
            queue: QueueConfig::default(),
            governor: GovernorConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置文件失败 {}", path.display()))?;
        Self::parse(&text, path)
    }

    pub fn load_or_default(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text, path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => {
                Err(error).with_context(|| format!("读取配置文件失败 {}", path.display()))
            }
        }
    }

    fn parse(text: &str, path: &Path) -> Result<Self> {
        toml::from_str(text).with_context(|| format!("解析配置文件失败 {}", path.display()))
    }

    pub fn validate_lite(&self) -> Result<()> {
        let cameras = &self.source.camera_whitelist;
        if cameras.len() > 1 {
            bail!(
                "配置不符合当前版本(camera_cap): Lite 版仅支持 1 路摄像头,当前选择了 {} 路;多摄像头请使用 Pro 版",
                cameras.len()
            );
        }
        if cameras.first().is_none_or(|camera| camera.starts_with('<')) {
            bail!("配置不符合当前版本(camera_required): Lite 版必须选择 1 路摄像头:请在 [source].camera_whitelist 填入摄像头 ID(如 [\"cam-xxxxxxxx\"])");
        }
        Ok(())
    }

    pub fn print_summary(&self) {
        println!("配置解析成功,摘要:");
        println!("  [general]");
        println!("    log_level = {}", self.general.log_level);
        println!("    db_path   = {}", self.general.db_path.display());
        println!("    workers   = {}", self.general.workers);
        println!("  [detect]");
        println!("    model_path     = {}", self.detect.model_path.display());
        println!("    conf_threshold = {}", self.detect.conf_threshold);
        println!("    input_size     = {}", self.detect.input_size);
        println!("    device         = {}", self.detect.device);
        println!("  [track]");
        println!("    enabled = {}", self.track.enabled);
        println!("  [event]");
        println!("    zones   = {} 个", self.event.zones.len());
        println!("  [sink]");
        if let Some(sqlite) = &self.sink.sqlite {
            let path = sqlite.path.as_ref().unwrap_or(&self.general.db_path);
            println!(
                "    sqlite   = true (path={}, store_detections={})",
                path.display(),
                sqlite.store_detections
            );
        } else {
            println!("    sqlite   = false");
        }
        println!("    snapshot = {}", self.sink.snapshot.is_some());
        println!("  [edition]");
        println!("    edition    = lite");
        println!("    camera_cap = 1");
        println!("    targets    = linux-x86_64, linux-aarch64");
    }
}

pub fn init_file(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("创建配置目录失败 {}", parent.display()))?;
    }

    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            bail!("配置文件已存在: {} (如需覆盖请先删除)", path.display());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("创建配置文件失败 {}", path.display()));
        }
    };

    let rendered = toml::to_string_pretty(&AppConfig::default()).context("序列化默认配置失败")?;
    let contents = format!(
        "# 由 `mibeevision config init` 生成\n\n{}\n\n",
        rendered.trim_end()
    );
    file.write_all(contents.as_bytes())
        .with_context(|| format!("写入配置文件失败 {}", path.display()))?;
    println!("已生成默认配置: {}", path.display());
    println!("请按需修改后运行 `mibeevision config check` 验证。");
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GeneralConfig {
    pub log_level: String,
    pub db_path: PathBuf,
    pub workers: usize,
}

impl Default for GeneralConfig {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            db_path: PathBuf::from("data/mibeevision.db"),
            workers: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SourceConfig {
    pub camera_whitelist: Vec<String>,
    pub camera_blacklist: Vec<String>,
    pub nvr_files: NvrFilesConfig,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            camera_whitelist: vec!["<camera-id>".into()],
            camera_blacklist: Vec::new(),
            nvr_files: NvrFilesConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NvrFilesConfig {
    pub dir: PathBuf,
    pub camera_id: Option<String>,
    pub extensions: Vec<String>,
    pub sample_fps: f32,
    pub max_side: u32,
}

impl Default for NvrFilesConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/recordings/cam01"),
            camera_id: None,
            extensions: ["mp4", "mkv", "dav", "flv", "avi"]
                .into_iter()
                .map(String::from)
                .collect(),
            sample_fps: 2.0,
            max_side: 1280,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DetectConfig {
    pub model_path: PathBuf,
    pub conf_threshold: f32,
    pub model_format: String,
    pub iou_threshold: f32,
    pub input_size: u32,
    pub classes: Vec<usize>,
    pub device: String,
}

impl Default for DetectConfig {
    fn default() -> Self {
        Self {
            model_path: PathBuf::from("models/yolov8n.onnx"),
            conf_threshold: 0.25,
            model_format: "auto".into(),
            iou_threshold: 0.45,
            input_size: 640,
            classes: Vec::new(),
            device: "cpu".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TrackConfig {
    pub enabled: bool,
    pub track_threshold: f32,
    pub high_threshold: f32,
    pub max_lost: u32,
}

impl Default for TrackConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            track_threshold: 0.5,
            high_threshold: 0.6,
            max_lost: 30,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EventConfig {
    pub zones: Vec<ZoneConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ZoneConfig {
    pub name: String,
    pub zone_type: String,
    pub points: Vec<[f32; 2]>,
    pub classes: Vec<usize>,
    pub min_duration_frames: u32,
    pub min_duration_secs: Option<f64>,
    pub min_motion: Option<f32>,
}

impl Default for ZoneConfig {
    fn default() -> Self {
        Self {
            name: "示例区域".into(),
            zone_type: "intrusion".into(),
            points: vec![[0.2, 0.2], [0.8, 0.2], [0.8, 0.8], [0.2, 0.8]],
            classes: vec![0],
            min_duration_frames: 3,
            min_duration_secs: None,
            min_motion: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SinkConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlite: Option<SqliteConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SqliteConfig {
    pub store_detections: bool,
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotConfig {
    pub dir: PathBuf,
    pub draw_boxes: bool,
    pub on_event_only: bool,
    pub quality: u8,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("snapshots"),
            draw_boxes: true,
            on_event_only: true,
            quality: 85,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NvrConfig {
    pub mode: String,
    pub api_base_url: String,
    pub api_key: String,
    pub sse_filter: String,
    pub storage_root: PathBuf,
    pub admin_user: String,
    pub admin_password: String,
    pub upload_snapshots: bool,
    pub snapshot_upload_quality: u8,
    pub snapshot_upload_draw_boxes: bool,
}

impl Default for NvrConfig {
    fn default() -> Self {
        Self {
            mode: "local".into(),
            api_base_url: "http://192.0.2.10:9090".into(),
            api_key: String::new(),
            sse_filter: "segment.".into(),
            storage_root: PathBuf::from("/data/recordings"),
            admin_user: String::new(),
            admin_password: String::new(),
            upload_snapshots: true,
            snapshot_upload_quality: 82,
            snapshot_upload_draw_boxes: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TransportConfig {
    #[serde(rename = "type")]
    pub r#type: String,
    pub http_temp_dir: PathBuf,
    pub rtsp_url_template: String,
    pub min_disk_margin_gb: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            r#type: "local_file".into(),
            http_temp_dir: PathBuf::from("/tmp/mibeevision-dl"),
            rtsp_url_template: String::new(),
            min_disk_margin_gb: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GateConfig {
    pub enabled: bool,
    pub classes: Vec<usize>,
    pub hold_secs: u64,
    pub renew_secs: u64,
    pub min_hits: u32,
    pub miss_frames: u32,
    pub trigger_source: String,
    pub cameras: Vec<GateCameraConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GateCameraConfig {
    pub id: String,
    pub masks: Vec<Vec<[f32; 2]>>,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            classes: vec![0],
            hold_secs: 90,
            renew_secs: 60,
            min_hits: 3,
            miss_frames: 60,
            trigger_source: "vision-person".into(),
            cameras: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    pub capacity: usize,
    pub ttl_secs: u64,
    pub on_full: String,
    pub decoded_capacity: usize,
    pub report_max_ids: usize,
    pub report_max_ranges: usize,
    pub early_send_threshold: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            capacity: 64,
            ttl_secs: 300,
            on_full: "drop_newest".into(),
            decoded_capacity: 4,
            report_max_ids: 32,
            report_max_ranges: 256,
            early_send_threshold: 16,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GovernorConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub mem_low_mb: u64,
    pub mem_high_mb: u64,
    pub scale_cooldown_secs: u64,
    pub max_decode_workers: usize,
}

impl Default for GovernorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 15,
            mem_low_mb: 600,
            mem_high_mb: 1200,
            scale_cooldown_secs: 120,
            max_decode_workers: 4,
        }
    }
}

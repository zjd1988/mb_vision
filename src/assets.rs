use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};

pub const YOLOV8N_ONNX: &[u8] = include_bytes!("../assets/yolov8n.onnx");
pub const CONFIG_HTML: &str = include_str!("../assets/config.html");
pub const PUSH_MONITOR_HTML: &str = include_str!("../assets/push_monitor.html");

pub fn ensure_model(path: &Path) -> Result<bool> {
    if path.exists() {
        return Ok(false);
    }

    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("创建模型目录失败 {}", parent.display()))?;
    }

    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("创建模型文件失败 {}", path.display()));
        }
    };
    file.write_all(YOLOV8N_ONNX)
        .with_context(|| format!("写入模型文件失败 {}", path.display()))?;
    Ok(true)
}

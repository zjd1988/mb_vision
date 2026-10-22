use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

pub use crate::config::NvrFilesConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NvrFile {
    pub path: PathBuf,
    pub camera_id: String,
}

pub fn scan_nvr_files(config: &NvrFilesConfig) -> Result<Vec<NvrFile>> {
    let root = &config.dir;
    let metadata = fs::symlink_metadata(root)
        .with_context(|| format!("读取录像扫描根目录失败 {}", root.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "录像扫描根目录必须是不经符号链接的真实目录 {}",
            root.display()
        );
    }

    let camera_id = match &config.camera_id {
        Some(camera_id) => camera_id.clone(),
        None => root
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| anyhow::anyhow!("无法从扫描根目录推导 camera_id {}", root.display()))?,
    };
    let extensions = config
        .extensions
        .iter()
        .map(|extension| {
            extension
                .trim()
                .trim_start_matches('.')
                .to_ascii_lowercase()
        })
        .filter(|extension| !extension.is_empty())
        .collect::<HashSet<_>>();

    let mut files = Vec::new();
    scan_directory(root, &camera_id, &extensions, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));

    tracing::info!(dir = ?root, found = files.len(), "目录扫描完成");
    Ok(files)
}

fn scan_directory(
    directory: &Path,
    camera_id: &str,
    extensions: &HashSet<String>,
    files: &mut Vec<NvrFile>,
) -> Result<()> {
    let entries = fs::read_dir(directory)
        .with_context(|| format!("读取录像目录失败 {}", directory.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("读取录像目录项失败 {}", directory.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("读取录像文件类型失败 {}", entry.path().display()))?;
        if file_type.is_symlink() {
            continue;
        }

        let path = entry.path();
        if file_type.is_dir() {
            scan_directory(&path, camera_id, extensions, files)?;
        } else if file_type.is_file() && has_enabled_extension(&path, extensions) {
            files.push(NvrFile {
                path,
                camera_id: camera_id.to_owned(),
            });
        }
    }
    Ok(())
}

fn has_enabled_extension(path: &Path, extensions: &HashSet<String>) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extensions.contains(&extension.to_ascii_lowercase()))
        .unwrap_or(false)
}

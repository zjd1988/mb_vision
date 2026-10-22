use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

use super::VideoDecoder;
use crate::config::TransportConfig;

pub trait HttpDownloader {
    fn download(&self, url: &str, temp_dir: &Path) -> Result<PathBuf>;
}

pub struct SourceFactory<'a> {
    transport: &'a TransportConfig,
    http_downloader: Option<&'a dyn HttpDownloader>,
}

impl<'a> SourceFactory<'a> {
    pub fn new(transport: &'a TransportConfig) -> Self {
        Self {
            transport,
            http_downloader: None,
        }
    }

    pub fn with_http_downloader(mut self, downloader: &'a dyn HttpDownloader) -> Self {
        self.http_downloader = Some(downloader);
        self
    }

    pub fn open<P: AsRef<OsStr>>(
        &self,
        location: P,
        sample_fps: f64,
        max_side: u32,
    ) -> Result<VideoDecoder> {
        match self.transport.r#type.as_str() {
            "local_file" => VideoDecoder::open(location, sample_fps, max_side),
            "http_download" => self.open_http_download(location.as_ref(), sample_fps, max_side),
            transport_type => {
                bail!("不支持的 transport.type `{transport_type}`,仅支持 local_file/http_download")
            }
        }
    }

    fn open_http_download(
        &self,
        location: &OsStr,
        sample_fps: f64,
        max_side: u32,
    ) -> Result<VideoDecoder> {
        let url = location
            .to_str()
            .ok_or_else(|| anyhow!("http_download 的 URL 必须是 UTF-8"))?;
        let downloader = self.http_downloader.ok_or_else(|| {
            anyhow!("transport.type=http_download 需要集成模块提供 HttpDownloader")
        })?;
        let path = downloader
            .download(url, &self.transport.http_temp_dir)
            .with_context(|| format!("下载视频输入失败 {url}"))?;
        VideoDecoder::open(path, sample_fps, max_side)
    }
}

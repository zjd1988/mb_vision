use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use image::{Rgb, RgbImage};

use crate::config::SnapshotConfig;
use crate::detect::Detection;

const GREEN: Rgb<u8> = Rgb([0, 255, 0]);
const BOX_THICKNESS: u32 = 2;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub path: PathBuf,
    pub bytes: Vec<u8>,
}

pub struct SnapshotSink {
    config: SnapshotConfig,
}

impl SnapshotSink {
    pub fn new(config: SnapshotConfig) -> Self {
        Self { config }
    }

    pub fn save<P: AsRef<Path>>(
        &self,
        camera_id: &str,
        source_path: P,
        frame_idx: u64,
        frame: &RgbImage,
        detections: &[Detection],
    ) -> Result<Snapshot> {
        let bytes = Self::encode(
            frame,
            detections,
            self.config.draw_boxes,
            self.config.quality,
        )?;
        std::fs::create_dir_all(&self.config.dir)
            .with_context(|| format!("创建快照目录失败 {}", self.config.dir.display()))?;

        let source_name = source_path
            .as_ref()
            .file_stem()
            .unwrap_or_else(|| OsStr::new("frame"))
            .to_string_lossy();
        let path = self
            .config
            .dir
            .join(format!("{camera_id}_{source_name}_f{frame_idx}.jpg"));
        std::fs::write(&path, &bytes)
            .with_context(|| format!("写入快照失败 {}", path.display()))?;
        Ok(Snapshot { path, bytes })
    }

    pub fn encode(
        frame: &RgbImage,
        detections: &[Detection],
        draw_boxes: bool,
        quality: u8,
    ) -> Result<Vec<u8>> {
        let mut output = frame.clone();
        if draw_boxes {
            for detection in detections {
                draw_box(&mut output, detection.bbox);
            }
        }

        let mut bytes = Vec::new();
        let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, quality);
        encoder
            .encode_image(&output)
            .context("编码 JPEG 快照失败")?;
        Ok(bytes)
    }
}

fn draw_box(image: &mut RgbImage, bbox: [f32; 4]) {
    let (width, height) = (image.width(), image.height());
    if width == 0
        || height == 0
        || bbox.iter().any(|value| !value.is_finite())
        || bbox[2] < 0.0
        || bbox[3] < 0.0
        || bbox[0] >= width as f32
        || bbox[1] >= height as f32
    {
        return;
    }

    let max_x = width - 1;
    let max_y = height - 1;
    let x1 = bbox[0].floor().clamp(0.0, max_x as f32) as u32;
    let y1 = bbox[1].floor().clamp(0.0, max_y as f32) as u32;
    let x2 = bbox[2].ceil().clamp(0.0, max_x as f32) as u32;
    let y2 = bbox[3].ceil().clamp(0.0, max_y as f32) as u32;
    if x2 < x1 || y2 < y1 {
        return;
    }

    for x in x1..=x2 {
        for offset in 0..BOX_THICKNESS {
            if let Some(y) = y1.checked_add(offset).filter(|y| *y <= y2) {
                image.put_pixel(x, y, GREEN);
            }
            if let Some(y) = y2.checked_sub(offset).filter(|y| *y >= y1) {
                image.put_pixel(x, y, GREEN);
            }
        }
    }
    for y in y1..=y2 {
        for offset in 0..BOX_THICKNESS {
            if let Some(x) = x1.checked_add(offset).filter(|x| *x <= x2) {
                image.put_pixel(x, y, GREEN);
            }
            if let Some(x) = x2.checked_sub(offset).filter(|x| *x >= x1) {
                image.put_pixel(x, y, GREEN);
            }
        }
    }
}

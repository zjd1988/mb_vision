use anyhow::{bail, Context, Result};
use image::RgbImage;
use ort::{Session, Tensor, ValueType};

use crate::config::DetectConfig;

use super::letterbox::{letterbox, Letterbox};
use super::nms::non_max_suppression;
use super::Detection;

#[derive(Clone, Copy, Debug)]
enum ModelFormat {
    AnchorScores,
    EndToEnd,
}

pub struct YoloDetector {
    session: Session,
    config: DetectConfig,
    format: ModelFormat,
    num_classes: usize,
}

impl YoloDetector {
    pub fn new(config: &DetectConfig) -> Result<Self> {
        if !matches!(config.model_format.as_str(), "auto" | "yolov8" | "yolo26") {
            bail!(
                "model_format 必须是 auto/yolov8/yolo26,当前 '{}'",
                config.model_format
            );
        }
        let session = Session::builder()
            .context("Failed to create ONNX Runtime environment")?
            .commit_from_file(&config.model_path)
            .with_context(|| format!("加载模型失败 {}", config.model_path.display()))?;
        let input = session.inputs.first().context("模型没有输入张量")?;
        let output = session.outputs.first().context("模型没有输出张量")?;
        let dimensions = match &output.output_type {
            ValueType::Tensor { dimensions, .. } => dimensions.clone(),
            _ => bail!("YOLO 输出不是张量"),
        };
        let format = match config.model_format.as_str() {
            "yolov8" => ModelFormat::AnchorScores,
            "yolo26" => ModelFormat::EndToEnd,
            _ if dimensions.last() == Some(&6) => ModelFormat::EndToEnd,
            _ => ModelFormat::AnchorScores,
        };
        let num_classes = match format {
            ModelFormat::EndToEnd => 80,
            ModelFormat::AnchorScores => {
                if dimensions.len() != 3 {
                    bail!("YOLOv8 输出应为三维张量,实际 {dimensions:?}");
                }
                let attributes = if dimensions[1] < dimensions[2] {
                    dimensions[1]
                } else {
                    dimensions[2]
                };
                if attributes <= 4 {
                    bail!("YOLOv8 输出属性数不足: {dimensions:?}");
                }
                (attributes - 4) as usize
            }
        };
        tracing::info!(
            model = %config.model_path.display(),
            input = %input.name,
            output = %output.name,
            format = ?format,
            num_classes,
            input_size = config.input_size,
            conf = config.conf_threshold,
            "YOLO 检测器已加载"
        );
        Ok(Self {
            session,
            config: config.clone(),
            format,
            num_classes,
        })
    }

    pub fn detect(&mut self, source: &RgbImage) -> Result<Vec<Detection>> {
        let (image, transform) = letterbox(source, self.config.input_size);
        let input = image_to_tensor(&image);
        let tensor = Tensor::from_array((
            [
                1usize,
                3,
                self.config.input_size as usize,
                self.config.input_size as usize,
            ],
            input.into_boxed_slice(),
        ))?;
        let outputs = self.session.run(ort::inputs![tensor]?)?;
        let (dimensions, values) = outputs[0].try_extract_raw_tensor::<f32>()?;
        let detections = match self.format {
            ModelFormat::AnchorScores => {
                self.parse_anchor_scores(&dimensions, values, transform)?
            }
            ModelFormat::EndToEnd => self.parse_end_to_end(&dimensions, values, transform)?,
        };
        Ok(non_max_suppression(detections, self.config.iou_threshold))
    }

    fn parse_anchor_scores(
        &self,
        dimensions: &[i64],
        values: &[f32],
        transform: Letterbox,
    ) -> Result<Vec<Detection>> {
        if dimensions.len() != 3 || dimensions[0] != 1 {
            bail!("YOLOv8 输出应为 [1, C, N] 或 [1, N, C],实际 {dimensions:?}");
        }
        let channels_first = dimensions[1] < dimensions[2];
        let (attributes, boxes) = if channels_first {
            (dimensions[1] as usize, dimensions[2] as usize)
        } else {
            (dimensions[2] as usize, dimensions[1] as usize)
        };
        if attributes != self.num_classes + 4 || values.len() != attributes * boxes {
            bail!("YOLOv8 输出维度与数据长度不一致: {dimensions:?}");
        }
        let value = |box_index: usize, attribute: usize| {
            if channels_first {
                values[attribute * boxes + box_index]
            } else {
                values[box_index * attributes + attribute]
            }
        };
        let mut detections = Vec::new();
        for box_index in 0..boxes {
            let mut class_id = 0;
            let mut confidence = f32::NEG_INFINITY;
            for class in 0..self.num_classes {
                let score = value(box_index, class + 4);
                if score > confidence {
                    confidence = score;
                    class_id = class;
                }
            }
            if confidence < self.config.conf_threshold || !self.class_enabled(class_id) {
                continue;
            }
            let center_x = value(box_index, 0);
            let center_y = value(box_index, 1);
            let width = value(box_index, 2);
            let height = value(box_index, 3);
            let bbox = transform.to_original([
                center_x - width * 0.5,
                center_y - height * 0.5,
                center_x + width * 0.5,
                center_y + height * 0.5,
            ]);
            detections.push(Detection::new(bbox, confidence, class_id));
        }
        Ok(detections)
    }

    fn parse_end_to_end(
        &self,
        dimensions: &[i64],
        values: &[f32],
        transform: Letterbox,
    ) -> Result<Vec<Detection>> {
        if dimensions.len() != 3 || dimensions[0] != 1 || dimensions[2] != 6 {
            bail!("yolo26 e2e 输出应为 [1, N, 6],实际 {dimensions:?}");
        }
        let mut detections = Vec::new();
        for row in values.chunks_exact(6) {
            let confidence = row[4];
            let class_id = row[5] as usize;
            if confidence < self.config.conf_threshold
                || class_id >= self.num_classes
                || !self.class_enabled(class_id)
            {
                continue;
            }
            detections.push(Detection::new(
                transform.to_original([row[0], row[1], row[2], row[3]]),
                confidence,
                class_id,
            ));
        }
        Ok(detections)
    }

    fn class_enabled(&self, class_id: usize) -> bool {
        self.config.classes.is_empty() || self.config.classes.contains(&class_id)
    }
}

fn image_to_tensor(image: &RgbImage) -> Vec<f32> {
    let (width, height) = image.dimensions();
    let plane = (width * height) as usize;
    let mut output = vec![0.0; plane * 3];
    for (index, pixel) in image.pixels().enumerate() {
        output[index] = pixel[0] as f32 / 255.0;
        output[plane + index] = pixel[1] as f32 / 255.0;
        output[plane * 2 + index] = pixel[2] as f32 / 255.0;
    }
    output
}

pub mod letterbox;
pub mod nms;
pub mod yolo;

pub use yolo::YoloDetector;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Detection {
    pub bbox: [f32; 4],
    pub confidence: f32,
    pub class_id: usize,
    pub track_id: Option<usize>,
}

impl Detection {
    pub fn new(bbox: [f32; 4], confidence: f32, class_id: usize) -> Self {
        Self {
            bbox,
            confidence,
            class_id,
            track_id: None,
        }
    }

    pub fn class_name(&self) -> &'static str {
        COCO_CLASSES
            .get(self.class_id)
            .copied()
            .unwrap_or("unknown")
    }

    pub fn center(&self) -> [f32; 2] {
        [
            (self.bbox[0] + self.bbox[2]) * 0.5,
            (self.bbox[1] + self.bbox[3]) * 0.5,
        ]
    }

    pub fn width(&self) -> f32 {
        (self.bbox[2] - self.bbox[0]).max(0.0)
    }

    pub fn height(&self) -> f32 {
        (self.bbox[3] - self.bbox[1]).max(0.0)
    }
}

pub const COCO_CLASSES: [&str; 80] = [
    "person",
    "bicycle",
    "car",
    "motorcycle",
    "airplane",
    "bus",
    "train",
    "truck",
    "boat",
    "traffic light",
    "fire hydrant",
    "stop sign",
    "parking meter",
    "bench",
    "bird",
    "cat",
    "dog",
    "horse",
    "sheep",
    "cow",
    "elephant",
    "bear",
    "zebra",
    "giraffe",
    "backpack",
    "umbrella",
    "handbag",
    "tie",
    "suitcase",
    "frisbee",
    "skis",
    "snowboard",
    "sports ball",
    "kite",
    "baseball bat",
    "baseball glove",
    "skateboard",
    "surfboard",
    "tennis racket",
    "bottle",
    "wine glass",
    "cup",
    "fork",
    "knife",
    "spoon",
    "bowl",
    "banana",
    "apple",
    "sandwich",
    "orange",
    "broccoli",
    "carrot",
    "hot dog",
    "pizza",
    "donut",
    "cake",
    "chair",
    "couch",
    "potted plant",
    "bed",
    "dining table",
    "toilet",
    "tv",
    "laptop",
    "mouse",
    "remote",
    "keyboard",
    "cell phone",
    "microwave",
    "oven",
    "toaster",
    "sink",
    "refrigerator",
    "book",
    "clock",
    "vase",
    "scissors",
    "teddy bear",
    "hair drier",
    "toothbrush",
];

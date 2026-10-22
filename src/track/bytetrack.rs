use jamtrack_rs::byte_tracker::ByteTracker;
use jamtrack_rs::object::Object;
use jamtrack_rs::rect::Rect;
use jamtrack_rs::TrackError;

use crate::config::TrackConfig;
use crate::detect::nms::intersection_over_union;
use crate::detect::Detection;

pub struct Tracker {
    inner: ByteTracker,
}

impl Tracker {
    pub fn new(config: &TrackConfig) -> Self {
        Self {
            inner: ByteTracker::new(
                30,
                config.max_lost as usize,
                config.track_threshold,
                config.high_threshold,
                0.8,
            ),
        }
    }

    pub fn update(&mut self, detections: &mut [Detection]) -> Result<(), TrackError> {
        let objects: Vec<Object> = detections
            .iter()
            .map(|detection| {
                Object::new(
                    Rect::from_xyxy(
                        detection.bbox[0],
                        detection.bbox[1],
                        detection.bbox[2],
                        detection.bbox[3],
                    ),
                    detection.confidence,
                    None,
                )
            })
            .collect();
        let tracks = self.inner.update(&objects)?;
        for detection in detections.iter_mut() {
            detection.track_id = None;
        }
        for track in tracks {
            let Some(track_id) = track.get_track_id() else {
                continue;
            };
            let track_bbox = track.get_rect().get_xyxy();
            let best = detections
                .iter_mut()
                .enumerate()
                .filter(|(_, detection)| detection.track_id.is_none())
                .map(|(index, detection)| {
                    let mut score = intersection_over_union(&track_bbox, &detection.bbox);
                    if (track.get_prob() - detection.confidence).abs() < 0.000_01 {
                        score += 0.001;
                    }
                    (index, score)
                })
                .max_by(|a, b| a.1.total_cmp(&b.1));
            if let Some((index, score)) = best {
                if score > 0.0 {
                    detections[index].track_id = Some(track_id);
                }
            }
        }
        Ok(())
    }
}

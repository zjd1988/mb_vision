use super::Detection;

pub fn intersection_over_union(a: &[f32; 4], b: &[f32; 4]) -> f32 {
    let left = a[0].max(b[0]);
    let top = a[1].max(b[1]);
    let right = a[2].min(b[2]);
    let bottom = a[3].min(b[3]);
    let intersection = (right - left).max(0.0) * (bottom - top).max(0.0);
    if intersection <= 0.0 {
        return 0.0;
    }
    let area_a = (a[2] - a[0]).max(0.0) * (a[3] - a[1]).max(0.0);
    let area_b = (b[2] - b[0]).max(0.0) * (b[3] - b[1]).max(0.0);
    let union = area_a + area_b - intersection;
    if union <= 0.0 {
        0.0
    } else {
        intersection / union
    }
}

pub fn non_max_suppression(detections: Vec<Detection>, iou_threshold: f32) -> Vec<Detection> {
    let mut sorted = detections;
    sorted.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    let mut kept: Vec<Detection> = Vec::with_capacity(sorted.len());
    'candidate: for candidate in sorted {
        for previous in &kept {
            if previous.class_id == candidate.class_id
                && intersection_over_union(&previous.bbox, &candidate.bbox) > iou_threshold
            {
                continue 'candidate;
            }
        }
        kept.push(candidate);
    }
    kept
}

use std::collections::HashMap;

use anyhow::{bail, Result};

use crate::config::ZoneConfig;
use crate::detect::Detection;

#[derive(Clone, Debug)]
pub struct ZoneEvent {
    pub zone_name: String,
    pub event_type: String,
    pub track_id: usize,
    pub class_id: usize,
    pub confidence: f32,
    pub bbox: [f32; 4],
    pub frame_idx: u64,
    pub timestamp_sec: f64,
}

pub struct ZoneEngine {
    zones: Vec<ZoneRuntime>,
}

struct ZoneRuntime {
    config: ZoneConfig,
    states: HashMap<usize, TrackState>,
}

#[derive(Clone)]
struct TrackState {
    hits: u32,
    confidence_sum: f32,
    previous_center: [f32; 2],
    first_seen_sec: f64,
    motion: f32,
    inside: bool,
    emitted: bool,
}

impl TrackState {
    fn new(center: [f32; 2], timestamp_sec: f64) -> Self {
        Self {
            hits: 0,
            confidence_sum: 0.0,
            previous_center: center,
            first_seen_sec: timestamp_sec,
            motion: 0.0,
            inside: false,
            emitted: false,
        }
    }
}

impl ZoneEngine {
    pub fn new(configs: &[ZoneConfig]) -> Result<Self> {
        for config in configs {
            match config.zone_type.as_str() {
                "intrusion" | "loitering" => {
                    if config.points.len() < 3 {
                        bail!(
                            "{} 规则 '{}' 至少需要 3 个顶点,实际 {}",
                            config.zone_type,
                            config.name,
                            config.points.len()
                        );
                    }
                }
                "line_crossing" => {
                    if config.points.len() != 2 {
                        bail!(
                            "line_crossing 规则 '{}' 恰好需要 2 个端点,实际 {}",
                            config.name,
                            config.points.len()
                        );
                    }
                }
                other => bail!("未知事件规则类型: {other}"),
            }
        }
        Ok(Self {
            zones: configs
                .iter()
                .cloned()
                .map(|config| ZoneRuntime {
                    config,
                    states: HashMap::new(),
                })
                .collect(),
        })
    }

    pub fn update(
        &mut self,
        detections: &[Detection],
        frame_idx: u64,
        timestamp_sec: f64,
        frame_width: u32,
        frame_height: u32,
    ) -> Vec<ZoneEvent> {
        let mut events = Vec::new();
        for zone in &mut self.zones {
            for detection in detections {
                let Some(track_id) = detection.track_id else {
                    continue;
                };
                if !zone.config.classes.is_empty()
                    && !zone.config.classes.contains(&detection.class_id)
                {
                    continue;
                }
                let center = detection.center();
                let normalized = [
                    center[0] / frame_width as f32,
                    center[1] / frame_height as f32,
                ];
                let state = zone
                    .states
                    .entry(track_id)
                    .or_insert_with(|| TrackState::new(normalized, timestamp_sec));
                if state.emitted {
                    continue;
                }
                let triggered = match zone.config.zone_type.as_str() {
                    "intrusion" => {
                        let inside = point_in_polygon(normalized, &zone.config.points);
                        if !inside {
                            state.hits = 0;
                            state.confidence_sum = 0.0;
                            state.previous_center = normalized;
                            state.first_seen_sec = timestamp_sec;
                            None
                        } else {
                            let required = zone.config.min_duration_frames.max(1);
                            let fire = state.hits + 1 >= required;
                            let confidence = if state.hits == 0 {
                                detection.confidence
                            } else {
                                state.confidence_sum / state.hits as f32
                            };
                            state.hits += 1;
                            state.confidence_sum += detection.confidence;
                            state.previous_center = normalized;
                            fire.then_some(confidence)
                        }
                    }
                    "line_crossing" => {
                        let previous = state.previous_center;
                        state.previous_center = normalized;
                        state.hits += 1;
                        state.confidence_sum += detection.confidence;
                        let confidence = state.confidence_sum / state.hits as f32;
                        if previous == normalized {
                            None
                        } else {
                            let a = zone.config.points[0];
                            let b = zone.config.points[1];
                            let crossed =
                                orientation(a, b, previous) * orientation(a, b, normalized) < 0.0
                                    && segments_intersect(previous, normalized, a, b);
                            crossed.then_some(confidence)
                        }
                    }
                    "loitering" => {
                        if !point_in_polygon(normalized, &zone.config.points) {
                            state.first_seen_sec = timestamp_sec;
                            state.previous_center = normalized;
                            state.hits = 0;
                            state.confidence_sum = 0.0;
                            state.motion = 0.0;
                            state.inside = false;
                            None
                        } else {
                            if !state.inside {
                                state.first_seen_sec = timestamp_sec;
                                state.hits = 0;
                                state.confidence_sum = 0.0;
                                state.motion = 0.0;
                            }
                            state.inside = true;
                            let previous = state.previous_center;
                            state.motion += distance(previous, normalized);
                            state.previous_center = normalized;
                            let duration = zone.config.min_duration_secs.unwrap_or(5.0);
                            let enough_time = timestamp_sec - state.first_seen_sec >= duration;
                            let enough_motion = zone
                                .config
                                .min_motion
                                .is_none_or(|minimum| state.motion >= minimum);
                            let confidence = if state.hits == 0 {
                                detection.confidence
                            } else {
                                state.confidence_sum / state.hits as f32
                            };
                            state.hits += 1;
                            state.confidence_sum += detection.confidence;
                            (enough_time && enough_motion).then_some(confidence)
                        }
                    }
                    _ => None,
                };
                if let Some(confidence) = triggered {
                    state.emitted = true;
                    events.push(ZoneEvent {
                        zone_name: zone.config.name.clone(),
                        event_type: match zone.config.zone_type.as_str() {
                            "intrusion" => "zone_intrusion",
                            "line_crossing" => "line_crossing",
                            "loitering" => "loitering",
                            _ => unreachable!(),
                        }
                        .into(),
                        track_id,
                        class_id: detection.class_id,
                        confidence,
                        bbox: detection.bbox,
                        frame_idx,
                        timestamp_sec,
                    });
                }
            }
        }
        events
    }
}

fn distance(a: [f32; 2], b: [f32; 2]) -> f32 {
    ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2)).sqrt()
}

fn orientation(a: [f32; 2], b: [f32; 2], p: [f32; 2]) -> f32 {
    (b[0] - a[0]) * (p[1] - a[1]) - (b[1] - a[1]) * (p[0] - a[0])
}

fn segments_intersect(a: [f32; 2], b: [f32; 2], c: [f32; 2], d: [f32; 2]) -> bool {
    let ab_c = orientation(a, b, c);
    let ab_d = orientation(a, b, d);
    let cd_a = orientation(c, d, a);
    let cd_b = orientation(c, d, b);
    ab_c * ab_d <= 0.0 && cd_a * cd_b <= 0.0
}

pub fn point_in_polygon(point: [f32; 2], polygon: &[[f32; 2]]) -> bool {
    let mut inside = false;
    let mut previous = polygon.len() - 1;
    for current in 0..polygon.len() {
        let a = polygon[current];
        let b = polygon[previous];
        let crosses = (a[1] > point[1]) != (b[1] > point[1]);
        if crosses {
            let x = (b[0] - a[0]) * (point[1] - a[1]) / (b[1] - a[1]) + a[0];
            if point[0] < x {
                inside = !inside;
            }
        }
        previous = current;
    }
    inside
}

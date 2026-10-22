use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use serde_json::json;

use crate::config::{GateCameraConfig, GateConfig, NvrConfig};
use crate::detect::Detection;
use crate::event::zone::point_in_polygon;

pub struct Gate {
    config: GateConfig,
    nvr: NvrConfig,
    client: reqwest::Client,
    hits: u32,
    misses: u32,
    last_trigger: Option<Instant>,
}

impl Gate {
    pub fn new(config: GateConfig, nvr: NvrConfig) -> Self {
        Self {
            config,
            nvr,
            client: reqwest::Client::new(),
            hits: 0,
            misses: 0,
            last_trigger: None,
        }
    }

    pub async fn update(
        &mut self,
        camera_id: &str,
        detections: &[Detection],
        frame_width: u32,
        frame_height: u32,
    ) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        let camera = self.camera(camera_id);
        if camera.is_none() {
            return Ok(false);
        }
        let masks = camera.map_or(&[][..], |camera| camera.masks.as_slice());
        let hit = detections.iter().any(|detection| {
            if !self.config.classes.is_empty() && !self.config.classes.contains(&detection.class_id)
            {
                return false;
            }
            let center = detection.center();
            let normalized = [
                center[0] / frame_width as f32,
                center[1] / frame_height as f32,
            ];
            !masks
                .iter()
                .filter(|mask| mask.len() >= 3)
                .any(|mask| point_in_polygon(normalized, mask))
        });
        if hit {
            self.hits = self.hits.saturating_add(1);
            self.misses = 0;
        } else {
            self.misses = self.misses.saturating_add(1);
            if self.misses >= self.config.miss_frames.max(1) {
                self.hits = 0;
            }
            return Ok(false);
        }
        if self.hits < self.config.min_hits.max(1) {
            return Ok(false);
        }
        let should_trigger = self.last_trigger.is_none_or(|last| {
            last.elapsed() >= Duration::from_secs(self.config.renew_secs.max(1))
        });
        if !should_trigger {
            return Ok(false);
        }
        let base = self.nvr.api_base_url.trim_end_matches('/');
        let response = self
            .client
            .post(format!("{base}/api/cameras/{camera_id}/adaptive/trigger"))
            .bearer_auth(&self.nvr.api_key)
            .json(&json!({
                "source": self.config.trigger_source,
                "hold": format!("{}s", self.config.hold_secs),
            }))
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("自适应录像触发返回 {status}: {body}");
        }
        self.last_trigger = Some(Instant::now());
        Ok(true)
    }

    fn camera(&self, camera_id: &str) -> Option<&GateCameraConfig> {
        self.config
            .cameras
            .iter()
            .find(|camera| camera.id == camera_id)
    }
}

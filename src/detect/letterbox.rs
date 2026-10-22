use image::{imageops, Rgb, RgbImage};

#[derive(Clone, Copy, Debug)]
pub struct Letterbox {
    pub scale: f32,
    pub pad_x: f32,
    pub pad_y: f32,
    pub source_width: u32,
    pub source_height: u32,
}

impl Letterbox {
    pub fn to_original(self, mut bbox: [f32; 4]) -> [f32; 4] {
        bbox[0] = (bbox[0] - self.pad_x) / self.scale;
        bbox[1] = (bbox[1] - self.pad_y) / self.scale;
        bbox[2] = (bbox[2] - self.pad_x) / self.scale;
        bbox[3] = (bbox[3] - self.pad_y) / self.scale;
        bbox[0] = bbox[0].clamp(0.0, self.source_width as f32);
        bbox[2] = bbox[2].clamp(0.0, self.source_width as f32);
        bbox[1] = bbox[1].clamp(0.0, self.source_height as f32);
        bbox[3] = bbox[3].clamp(0.0, self.source_height as f32);
        bbox
    }
}

pub fn letterbox(source: &RgbImage, input_size: u32) -> (RgbImage, Letterbox) {
    let (source_width, source_height) = source.dimensions();
    let scale =
        (input_size as f32 / source_width as f32).min(input_size as f32 / source_height as f32);
    let resized_width = ((source_width as f32 * scale).round() as u32).max(1);
    let resized_height = ((source_height as f32 * scale).round() as u32).max(1);
    let resized = imageops::resize(
        source,
        resized_width,
        resized_height,
        imageops::FilterType::Nearest,
    );
    let pad_left = (input_size - resized_width) / 2;
    let pad_top = (input_size - resized_height) / 2;
    let mut output = RgbImage::from_pixel(input_size, input_size, Rgb([114, 114, 114]));
    imageops::overlay(&mut output, &resized, pad_left as i64, pad_top as i64);
    (
        output,
        Letterbox {
            scale,
            pad_x: pad_left as f32,
            pad_y: pad_top as f32,
            source_width,
            source_height,
        },
    )
}

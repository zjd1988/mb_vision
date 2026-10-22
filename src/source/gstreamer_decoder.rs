use std::ffi::OsStr;

use anyhow::Result;

use super::VideoDecoder;

pub fn open_with_ffmpeg_fallback<P: AsRef<OsStr>>(
    input: P,
    sample_fps: f64,
    max_side: u32,
) -> Result<VideoDecoder> {
    VideoDecoder::open(input, sample_fps, max_side)
}

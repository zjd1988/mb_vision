use std::ffi::OsStr;
use std::io;

use anyhow::{anyhow, bail, Context, Result};
use ffmpeg::codec::decoder::Video as FfmpegVideoDecoder;
use ffmpeg::format::context::Input;
use ffmpeg::format::Pixel;
use ffmpeg::frame::Video as FfmpegFrame;
use ffmpeg::software::scaling::{context::Context as ScalingContext, Flags as ScalingFlags};
use ffmpeg_the_third as ffmpeg;
use image::RgbImage;

#[derive(Debug, Clone)]
pub struct VideoFrame {
    pub image: RgbImage,
    pub frame_idx: u64,
    pub timestamp_sec: f64,
}

pub struct VideoDecoder {
    input: Input,
    stream_index: usize,
    decoder: FfmpegVideoDecoder,
    scaler: Option<ScalingContext>,
    time_base: ffmpeg::Rational,
    first_timestamp: Option<i64>,
    source_fps: f64,
    sample_fps: f64,
    max_side: u32,
    src_w: u32,
    src_h: u32,
    dst_w: u32,
    dst_h: u32,
    decoded_frame_idx: u64,
    output_frame_idx: u64,
    draining: bool,
    finished: bool,
    failed: bool,
}

impl VideoDecoder {
    pub fn open<P: AsRef<OsStr>>(input: P, sample_fps: f64, max_side: u32) -> Result<Self> {
        prepare_ffmpeg(sample_fps)?;
        let source_name = input.as_ref().to_string_lossy().into_owned();
        let input_context = ffmpeg::format::input(input.as_ref())
            .with_context(|| format!("打开视频输入失败 {source_name}"))?;
        Self::from_input(source_name, input_context, sample_fps, max_side)
    }

    pub fn open_with_headers<P: AsRef<OsStr>>(
        input: P,
        headers: &str,
        sample_fps: f64,
        max_side: u32,
    ) -> Result<Self> {
        prepare_ffmpeg(sample_fps)?;
        let source_name = input.as_ref().to_string_lossy().into_owned();
        let options = ffmpeg::Dictionary::from_iter([("headers", headers)]);
        let input_context = ffmpeg::format::input_with_dictionary(input.as_ref(), options)
            .with_context(|| format!("打开视频输入失败 {source_name}"))?;
        Self::from_input(source_name, input_context, sample_fps, max_side)
    }

    fn from_input(
        source_name: String,
        input_context: Input,
        sample_fps: f64,
        max_side: u32,
    ) -> Result<Self> {
        let (stream_index, time_base, stream_fps) = {
            let stream = input_context
                .streams()
                .best(ffmpeg::media::Type::Video)
                .ok_or_else(|| anyhow!("视频输入中没有可解码的视频流 {source_name}"))?;
            let fps = valid_fps(stream.avg_frame_rate()).or_else(|| valid_fps(stream.rate()));
            (stream.index(), stream.time_base(), fps)
        };

        let decoder_context = {
            let stream = input_context
                .stream(stream_index)
                .ok_or_else(|| anyhow!("视频流已失效 {source_name}"))?;
            ffmpeg::codec::context::Context::from_parameters(stream.parameters())
                .with_context(|| format!("读取视频解码参数失败 {source_name}"))?
        };
        let decoder = decoder_context
            .decoder()
            .video()
            .with_context(|| format!("打开视频解码器失败 {source_name}"))?;

        let source_fps = stream_fps
            .or_else(|| decoder.frame_rate().and_then(valid_fps))
            .ok_or_else(|| anyhow!("无法确定源视频帧率 {source_name}"))?;
        let src_w = decoder.width();
        let src_h = decoder.height();
        if src_w == 0 || src_h == 0 {
            bail!("视频分辨率无效 {source_name}: {src_w}x{src_h}");
        }
        let (dst_w, dst_h) = scaled_dimensions(src_w, src_h, max_side);

        tracing::info!(
            file = ?source_name,
            src_w,
            src_h,
            dst_w,
            dst_h,
            source_fps,
            sample_fps,
            "视频解码器已初始化"
        );

        Ok(Self {
            input: input_context,
            stream_index,
            decoder,
            scaler: None,
            time_base,
            first_timestamp: None,
            source_fps,
            sample_fps,
            max_side,
            src_w,
            src_h,
            dst_w,
            dst_h,
            decoded_frame_idx: 0,
            output_frame_idx: 0,
            draining: false,
            finished: false,
            failed: false,
        })
    }

    pub fn source_fps(&self) -> f64 {
        self.source_fps
    }

    pub fn sample_fps(&self) -> f64 {
        self.sample_fps
    }

    pub fn source_dimensions(&self) -> (u32, u32) {
        (self.src_w, self.src_h)
    }

    pub fn output_dimensions(&self) -> (u32, u32) {
        (self.dst_w, self.dst_h)
    }

    pub fn next_frame(&mut self) -> Result<Option<VideoFrame>> {
        if self.finished || self.failed {
            return Ok(None);
        }

        let result = self.poll_next_frame();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn poll_next_frame(&mut self) -> Result<Option<VideoFrame>> {
        loop {
            match self.receive_decoded_frame()? {
                ReceiveResult::Frame(Some(frame)) => return Ok(Some(frame)),
                ReceiveResult::Frame(None) => continue,
                ReceiveResult::Eof => {
                    self.finished = true;
                    return Ok(None);
                }
                ReceiveResult::NeedInput if self.draining => {
                    self.finished = true;
                    return Ok(None);
                }
                ReceiveResult::NeedInput => {}
            }

            let mut packet = ffmpeg::Packet::empty();
            match packet.read(&mut self.input) {
                Ok(()) if packet.stream() != self.stream_index => continue,
                Ok(()) => self
                    .decoder
                    .send_packet(&packet)
                    .context("向 FFmpeg 解码器发送视频数据包失败")?,
                Err(ffmpeg::Error::Eof) => {
                    self.decoder
                        .send_eof()
                        .context("结束 FFmpeg 解码器输入失败")?;
                    self.draining = true;
                }
                Err(error) if is_would_block(&error) => continue,
                Err(error) => return Err(error).context("读取视频数据包失败"),
            }
        }
    }

    fn receive_decoded_frame(&mut self) -> Result<ReceiveResult> {
        let mut decoded = FfmpegFrame::empty();
        match self.decoder.receive_frame(&mut decoded) {
            Ok(()) => {
                let decoded_idx = self.decoded_frame_idx;
                self.decoded_frame_idx = self
                    .decoded_frame_idx
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("源视频帧序号溢出"))?;
                let source_time = self.source_time(&decoded, decoded_idx);
                if !self.should_sample(decoded_idx, source_time) {
                    return Ok(ReceiveResult::Frame(None));
                }

                let image = self.to_rgb(decoded)?;
                let frame_idx = self.output_frame_idx;
                self.output_frame_idx = self
                    .output_frame_idx
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("输出视频帧序号溢出"))?;
                let timestamp_sec = frame_idx as f64 / self.sample_fps;
                Ok(ReceiveResult::Frame(Some(VideoFrame {
                    image,
                    frame_idx,
                    timestamp_sec,
                })))
            }
            Err(ffmpeg::Error::Eof) => Ok(ReceiveResult::Eof),
            Err(error) if is_would_block(&error) => Ok(ReceiveResult::NeedInput),
            Err(error) => Err(error).context("从 FFmpeg 解码器接收视频帧失败"),
        }
    }

    fn source_time(&mut self, decoded: &FfmpegFrame, decoded_idx: u64) -> f64 {
        let time_base = f64::from(self.time_base);
        if time_base.is_finite() && time_base > 0.0 {
            if let Some(timestamp) = decoded.timestamp() {
                let first = *self.first_timestamp.get_or_insert(timestamp);
                return timestamp.saturating_sub(first) as f64 * time_base;
            }
        }
        decoded_idx as f64 / self.source_fps
    }

    fn should_sample(&self, decoded_idx: u64, source_time: f64) -> bool {
        if decoded_idx == 0 {
            return true;
        }
        let target_time = self.output_frame_idx as f64 / self.sample_fps;
        let tolerance = f64::EPSILON * source_time.abs().max(target_time).max(1.0) * 8.0;
        source_time + tolerance >= target_time
    }

    fn to_rgb(&mut self, decoded: FfmpegFrame) -> Result<RgbImage> {
        let src_w = decoded.width();
        let src_h = decoded.height();
        let src_format = decoded.format();
        if src_w == 0 || src_h == 0 || src_format == Pixel::None {
            bail!("FFmpeg 返回了无效视频帧: {src_w}x{src_h} {src_format:?}");
        }
        let (dst_w, dst_h) = scaled_dimensions(src_w, src_h, self.max_side);

        let rebuild_scaler = match &self.scaler {
            None => true,
            Some(scaler) => {
                let input = scaler.input();
                let output = scaler.output();
                input.format != src_format
                    || input.width != src_w
                    || input.height != src_h
                    || output.width != dst_w
                    || output.height != dst_h
            }
        };
        if rebuild_scaler {
            self.scaler = Some(
                ScalingContext::get(
                    src_format,
                    src_w,
                    src_h,
                    Pixel::RGB24,
                    dst_w,
                    dst_h,
                    ScalingFlags::BILINEAR,
                )
                .context("创建 FFmpeg RGB24 转换器失败")?,
            );
        }

        let mut rgb = FfmpegFrame::empty();
        self.scaler
            .as_mut()
            .expect("scaler initialized")
            .run(&decoded, &mut rgb)
            .context("FFmpeg 转换 RGB24 视频帧失败")?;

        let row_len = (dst_w as usize)
            .checked_mul(3)
            .ok_or_else(|| anyhow!("RGB 行长度溢出"))?;
        let stride = rgb.stride(0);
        if stride < row_len {
            bail!("FFmpeg RGB 行跨度异常: stride={stride}, row_len={row_len}");
        }
        let capacity = row_len
            .checked_mul(dst_h as usize)
            .ok_or_else(|| anyhow!("RGB 图像长度溢出"))?;
        let src = rgb.data(0);
        let mut packed = Vec::with_capacity(capacity);
        for row in 0..dst_h as usize {
            let start = row
                .checked_mul(stride)
                .ok_or_else(|| anyhow!("RGB 行偏移溢出"))?;
            let end = start
                .checked_add(row_len)
                .ok_or_else(|| anyhow!("RGB 行偏移溢出"))?;
            let bytes = src
                .get(start..end)
                .ok_or_else(|| anyhow!("FFmpeg RGB 数据长度不足"))?;
            packed.extend_from_slice(bytes);
        }

        RgbImage::from_raw(dst_w, dst_h, packed)
            .ok_or_else(|| anyhow!("创建 RGB 图像失败 {dst_w}x{dst_h}"))
    }
}

impl Iterator for VideoDecoder {
    type Item = Result<VideoFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_frame() {
            Ok(Some(frame)) => Some(Ok(frame)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

enum ReceiveResult {
    Frame(Option<VideoFrame>),
    NeedInput,
    Eof,
}

fn prepare_ffmpeg(sample_fps: f64) -> Result<()> {
    if !sample_fps.is_finite() || sample_fps <= 0.0 {
        bail!("sample_fps 必须是大于 0 的有限数值,当前为 {sample_fps}");
    }
    ffmpeg::init().context("初始化 FFmpeg 失败")?;
    ffmpeg::format::network::init();
    Ok(())
}

fn valid_fps(rate: ffmpeg::Rational) -> Option<f64> {
    if rate.numerator() <= 0 || rate.denominator() <= 0 {
        return None;
    }
    let fps = f64::from(rate);
    (fps.is_finite() && fps > 0.0).then_some(fps)
}

fn scaled_dimensions(src_w: u32, src_h: u32, max_side: u32) -> (u32, u32) {
    let longest = src_w.max(src_h);
    if max_side == 0 || longest <= max_side {
        return (src_w, src_h);
    }

    let scale = max_side as f64 / longest as f64;
    let dst_w = ((src_w as f64 * scale).round() as u32).max(1);
    let dst_h = ((src_h as f64 * scale).round() as u32).max(1);
    (dst_w, dst_h)
}

fn is_would_block(error: &ffmpeg::Error) -> bool {
    match error {
        ffmpeg::Error::Other { errno } => {
            io::Error::from_raw_os_error(*errno).kind() == io::ErrorKind::WouldBlock
        }
        _ => false,
    }
}

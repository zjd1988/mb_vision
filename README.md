# MiBeeVision

MiBeeVision Lite 是一个轻量级视频 AI 分析服务（Rust 实现），与 MiBeeNvr 录像系统配套使用，对监控录像/实时流执行目标检测（YOLOv8n + ONNX Runtime）、多目标跟踪（ByteTrack）与行为分析（区域入侵、越线、徘徊），并将 AI 事件、快照与处理状态回写到 NVR。

本仓库是对发布二进制 `mibee-vision-lite-v0.1.0-linux-x86_64` 的**功能等价重建**（逆向重建源码，非原始源码逐字恢复）。行为已与原程序对齐验证：默认配置逐字节一致，视频扫描帧数/事件数/触发帧一致，置信度仅有像素级浮点差异。

## 功能特性

- **目标检测**：YOLOv8n（ONNX），letterbox 预处理，按类别 NMS
- **多目标跟踪**：ByteTrack（vendored `jamtrack-rs` v0.7.0）
- **行为分析**：区域入侵、越线、徘徊规则引擎
- **多种接入模式**：
  - `scan`：批处理扫描 NVR 录像目录（可完全脱离 NVR 使用）
  - `watch`：监听 NVR SSE 事件流，录像完成后自动触发 AI 处理
  - `push`：内置 HTTP 上传服务，接收 NVR 推送的录像片段（4 个解码 worker 并行解码 + 单推理 worker 串行推理的两段式管线）
  - `watch-stream`：直接拉取 RTSP 实时流分析
  - `serve`：Web 配置页 + FLV 实时流（Basic Auth）
- **单机具**：`detect`（单图/单视频调试）、`events`（查询 SQLite 事件库）、`config init/check`
- **数据落盘**：SQLite 事件库、事件快照、NVR AI 状态/事件/心跳回写

## 构建

依赖：Rust 工具链、`nasm`、`libclang-dev`（ffmpeg-the-third 编译需要）。

```bash
sudo apt install nasm libclang-dev
cargo build --release
```

产出：`target/release/mibeevision`

## 运行

ONNX Runtime 动态库不随仓库分发（体积与许可证原因），需自行准备并通过环境变量指定：

```bash
export ORT_DYLIB_PATH=/path/to/libonnxruntime-1.17.1-linux-x64.so

# 生成默认配置（./config/config.toml）
./target/release/mibeevision config init

# 校验配置
./target/release/mibeevision config check

# 扫描本地录像目录（可脱离 NVR 独立使用）
./target/release/mibeevision scan --config config/config.toml

# 启动 push 接收服务
./target/release/mibeevision push --config config/config.toml

# 监听 NVR SSE 事件流
./target/release/mibeevision watch --config config/config.toml
```

完整命令列表见 `mibeevision --help` 及各子命令 `--help`。

## 与 MiBeeNvr 的关系

| 模式 | 能否脱离 MiBeeNvr |
| --- | --- |
| `scan` / `detect` / `events` | 可以，完全独立 |
| `watch-stream` | 可以，配置 `transport.rtsp_url_template` 直接拉流 |
| `watch` | 需要 NVR 或兼容 API（SSE、录像下载、AI 状态/事件回写） |
| `push` | 上传端点可单独调用，但仍要求 heartbeat/状态/事件/快照接口 |
| `serve` | 配置页可独立启动，自动 SSE 管线需要 NVR |

## 仓库结构

```text
src/
  cli.rs            命令行入口与子命令定义
  config.rs         配置模型与校验
  detect/           YOLOv8 检测（letterbox / NMS / 推理）
  track/            ByteTrack 封装
  event/            行为规则（区域入侵 / 越线 / 徘徊）
  source/           视频源（FFmpeg/GStreamer 解码、NVR 文件、RTSP）
  pipeline/         解码→推理两段式管线
  framebus/         段总线、governor 调度、丢弃上报
  integration/      push HTTP 服务、SSE 客户端、REST 客户端
  sink/             SQLite / 快照 / NVR API / 门控回写
  webui/            配置页与热重启
  storage/          存储抽象
assets/             yolov8n.onnx、Web 页面
vendor/jamtrack-rs/ ByteTrack 实现（基于 kadu-v/jamtrack-rs v0.7.0）
```

## 已知限制

- 少数检测框/置信度与原程序存在像素级浮点差异（事件触发结果一致）。
- Lite 版限制单摄像头。
- 尚无正式的 `[nvr].enabled=false` / standalone 配置项。

## 许可证

`vendor/jamtrack-rs/` 遵循其自带 LICENSE（见该目录）。其余部分为逆向重建代码，仅供学习研究使用。

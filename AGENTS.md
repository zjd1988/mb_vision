# AGENTS.md

本文件为 AI 编码助手（Kimi Code / Claude Code 等）在本仓库工作的指南。

## 项目背景

本仓库是对发布二进制 `mibee-vision-lite-v0.1.0-linux-x86_64` 的功能等价逆向重建（Rust）。参考行为以原二进制为准，参考仓库为配套的 `MiBeeNvr`（NVR 服务）与 `rea`（Rust/ELF 检查工具链）。重建目标是**行为对齐**，不是逐字恢复原始源码。

## 构建与验证命令

```bash
# 依赖（ffmpeg-the-third 编译需要）
sudo apt install nasm libclang-dev

cargo fmt --all -- --check
cargo check
cargo test --release
cargo build --release
```

提交前以上四条必须全部通过。`cargo metadata --locked --no-deps --format-version 1` 用于校验 lockfile 一致性。

## 运行时环境

- ONNX Runtime 动态库不进仓库，运行时通过 `ORT_DYLIB_PATH` 指定，例如：
  `export ORT_DYLIB_PATH=<repo>/libonnxruntime-1.17.1-linux-x64.so`
- 默认配置由 `mibeevision config init` 生成到 `./config/config.toml`。
- 运行时产物目录（`config/`、`data/`、`models/`、`snapshots/`、`out/`）均在 `.gitignore` 中，不要提交。

## 架构约定（改动时不要破坏）

- push 管线为两段式：HTTP 上传 → segment bus → governor 管理的 N 个解码 worker（默认 4，PATCH 状态→FFmpeg 解码→删除临时文件）→ decoded bus（有容量/TTL/drop_newest/drop report/ack）→ **单个推理 worker 串行消费**（复用同一 ONNX session）。
- 推理侧必须是单 worker 串行：ONNX session 复用与事件顺序依赖这一约束。改并发前先确认这两点。
- governor 依据 `decode_target` 真实增删解码 worker，监控指标（`decode_alive`、`decode_target`、`decoded_bus.*`、`in_flight`）须保持真实统计，不要用占位值。
- ByteTrack 使用 `vendor/jamtrack-rs/` 的 vendored 副本，不要改为 crates.io 依赖（版本对齐原程序 v0.7.0）。
- 依赖版本全部用 `=` 精确锁定（见 Cargo.toml），升级依赖需同时验证行为对齐。

## 行为对齐基准

修改检测/跟踪/规则相关代码后，用原二进制做对照验证：

- `config init` 输出须与原程序逐字节一致（SHA-256: `8654d6a0b323693c673038a34f49848a129e3ea2e602643ab33c3a9f4f84e08b`）。
- 同一测试视频扫描：帧数、有目标帧数、事件数、事件类型与触发帧须一致；置信度允许像素级浮点差异。
- 区域入侵、越线、徘徊规则须在原程序相同帧触发。

## 代码风格

- 默认不写注释；代码与 git 历史已能说明意图。仅在确有必要（如对齐原程序的魔数、非显然的并发约束）时写简短注释。
- 不新增依赖，除非能力确实缺失；新增前先确认 Cargo.lock 中无现成可用项，并遵循 `=` 精确版本约定。
- 本仓库无独立测试目录结构要求；已有测试随模块走，新增测试遵循现有惯例。

## 不要提交的内容

- `target/`、运行时产物目录
- 原始二进制 `mibee-vision-lite-v0.1.0-linux-x86_64`、`libonnxruntime-*.so`
- 任何凭据、token、私钥

以上均已被 `.gitignore` 覆盖，新增产物目录时同步更新 `.gitignore`。

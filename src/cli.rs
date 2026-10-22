use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "MiBeeVision - 视频流 AI 后期自动化处理引擎(MiBeeNVR 优先,预留通用接口)",
    arg_required_else_help = true,
    propagate_version = true
)]
pub struct Cli {
    #[arg(
        short,
        long,
        global = true,
        env = "MIBEEVISION_CONFIG",
        help = "配置文件路径(默认查 ./config/config.toml,再退化为内置默认值)。"
    )]
    pub config: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        env = "MIBEEVISION_LOG",
        help = "覆盖日志级别(trace/debug/info/warn/error)。"
    )]
    pub log_level: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(about = "扫描 NVR 录像目录,做端到端批处理(检测→跟踪→事件→入库/快照)。")]
    Scan {
        #[arg(short, long, help = "录像根目录(覆盖配置中的 source.nvr_files.dir)。")]
        dir: Option<PathBuf>,
    },

    #[command(about = "监听 NVR SSE 事件流,录像完成时自动触发 AI 处理(跨服务器事件驱动)。")]
    Watch {
        #[arg(long, help = "仅监听指定摄像头(留空=全部)。")]
        camera: Option<String>,
    },

    #[command(
        about = "推送模式:启动 HTTP 服务器接收 NVR 主动推送的段通知(替代 SSE 拉取)。",
        long_about = "推送模式:启动 HTTP 服务器接收 NVR 主动推送的段通知(替代 SSE 拉取)。\n\nNVR 判断 Vision 健康(心跳)后,在段刚落盘时主动推送 file_path, Vision 直读文件处理。解决 SSE 模式下 78% 下载 404 的问题。"
    )]
    Push {
        #[arg(
            long,
            default_value = "0.0.0.0:9091",
            help = "监听地址(默认 0.0.0.0:9091)。"
        )]
        addr: String,
    },

    #[command(
        about = "常驻:配置页(默认 0.0.0.0:9092)+ 单摄像头管线(SSE 驱动,配置保存后热重启)。 **Lite 推荐入口**:零命令行,浏览器填 NVR 地址/API Key/选摄像头即用。"
    )]
    Serve {
        #[arg(long, default_value = "0.0.0.0:9092", help = "配置页监听地址。")]
        addr: String,
    },

    #[command(
        about = "实时拉取指定摄像头的 RTSP 流做 AI 处理(持续检测,不经录像/segment 事件)。",
        long_about = "实时拉取指定摄像头的 RTSP 流做 AI 处理(持续检测,不经录像/segment 事件)。\n\n从 NVR `GET /api/cameras` 取该摄像头原始 RTSP URL 直拉。适用于需要真实时 分析的场景(与 watch 的\"录像后处理\"互补)。Ctrl-C 停止。"
    )]
    WatchStream {
        #[arg(long, help = "摄像头 ID(必填,从 GET /api/cameras 获取)。")]
        camera: String,
    },

    #[command(about = "对单个视频/图片做检测,用于调试模型与参数。")]
    Detect {
        #[arg(short, long, help = "输入文件路径。")]
        file: PathBuf,

        #[arg(short, long, default_value = "./out", help = "输出目录(快照/JSON)。")]
        output: PathBuf,
    },

    #[command(about = "查询已入库事件。")]
    Events {
        #[arg(long, help = "按摄像头过滤。")]
        camera: Option<String>,

        #[arg(
            long,
            help = "时间范围,如 \"2h\"(最近 2 小时)/ \"2026-06-01..2026-06-15\"。"
        )]
        since: Option<String>,
    },

    #[command(about = "配置相关操作。", arg_required_else_help = true)]
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "scan",
            Self::Watch { .. } => "watch",
            Self::Push { .. } => "push",
            Self::Serve { .. } => "serve",
            Self::WatchStream { .. } => "watch-stream",
            Self::Detect { .. } => "detect",
            Self::Events { .. } => "events",
            Self::Config { .. } => "config",
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    #[command(about = "在 ./config/config.toml 生成默认配置文件(若已存在则报错)。")]
    Init,

    #[command(about = "校验配置文件能否正确解析,并打印解析结果摘要。")]
    Check,
}

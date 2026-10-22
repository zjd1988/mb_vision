use std::convert::Infallible;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CACHE_CONTROL, CONNECTION, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{watch, RwLock};

use crate::assets::CONFIG_HTML;
use crate::config::AppConfig;

const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const NVR_TIMEOUT: Duration = Duration::from_secs(5);

pub struct WebUiState {
    pub config_path: PathBuf,
    pub config: Arc<RwLock<AppConfig>>,
    pub pipeline_running: Arc<AtomicBool>,
    pub last_pipeline_error: Arc<RwLock<Option<String>>>,
    pub recent_events: Arc<RwLock<Vec<Value>>>,
    pub version: Arc<AtomicU64>,
    pub reload: watch::Sender<AppConfig>,
    pub started_at: Instant,
    http_client: reqwest::Client,
}

impl WebUiState {
    pub fn new(config_path: PathBuf, config: AppConfig, reload: watch::Sender<AppConfig>) -> Self {
        Self {
            config_path,
            config: Arc::new(RwLock::new(config)),
            pipeline_running: Arc::new(AtomicBool::new(false)),
            last_pipeline_error: Arc::new(RwLock::new(None)),
            recent_events: Arc::new(RwLock::new(Vec::new())),
            version: Arc::new(AtomicU64::new(0)),
            reload,
            started_at: Instant::now(),
            http_client: reqwest::Client::new(),
        }
    }
}

pub async fn run<A, F>(addr: A, state: Arc<WebUiState>, shutdown: F) -> Result<()>
where
    A: ToSocketAddrs,
    F: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind(addr)
        .await
        .context("绑定 WebUI 监听地址失败")?;
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("接受 WebUI 连接失败")?;
                let state = state.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let state = state.clone();
                        async move { Ok::<_, Infallible>(route(request, state).await) }
                    });
                    if let Err(error) = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        tracing::warn!(%error, "WebUI HTTP 连接处理失败");
                    }
                });
            }
        }
    }

    Ok(())
}

type HttpResponse = Response<Full<Bytes>>;

async fn route(request: Request<Incoming>, state: Arc<WebUiState>) -> HttpResponse {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();

    match (method.clone(), path.as_str()) {
        (Method::GET, "/") => html_response(CONFIG_HTML),
        (Method::GET, "/api/config") => get_config(&state).await,
        (Method::PUT, "/api/config") => put_config(request, &state).await,
        (Method::POST, "/api/nvr/probe") => probe_nvr(request, &state).await,
        (Method::GET, "/api/nvr/cameras") => saved_nvr_cameras(&state).await,
        (Method::GET, "/api/status") => status(&state).await,
        _ => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("未知路由 {method} {path}"),
        ),
    }
}

async fn get_config(state: &WebUiState) -> HttpResponse {
    let config = state.config.read().await;
    match config_value(&config) {
        Ok(value) => json_response(StatusCode::OK, value),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "config_serialization_failed",
            format!("序列化配置失败: {error}"),
        ),
    }
}

async fn put_config(request: Request<Incoming>, state: &WebUiState) -> HttpResponse {
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                format!("读取请求体失败: {error}"),
            );
        }
    };
    let next = match serde_json::from_slice::<AppConfig>(&body) {
        Ok(config) => config,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                format!("解析配置 JSON 失败: {error}"),
            );
        }
    };
    if let Err(error) = next.validate_lite() {
        return error_response(StatusCode::BAD_REQUEST, "invalid_config", error.to_string());
    }

    let mut current = state.config.write().await;
    if let Err(error) = write_config(&state.config_path, &next).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "config_write_failed",
            format!("保存配置失败: {error:#}"),
        );
    }

    *current = next.clone();
    let version = state.version.fetch_add(1, Ordering::Relaxed) + 1;
    drop(current);
    tracing::info!(
        version,
        path = ?state.config_path,
        "配置已保存(管线将热重启)"
    );
    state.reload.send_replace(next);

    json_response(
        StatusCode::OK,
        json!({"ok": true, "restarting": true, "version": version}),
    )
}

async fn write_config(path: &Path, config: &AppConfig) -> Result<()> {
    let rendered = toml::to_string_pretty(config).context("序列化配置为 TOML 失败")?;
    let contents = format!("# 由 MiBeeVision 配置页保存\n\n{rendered}");
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("创建配置目录失败 {}", parent.display()))?;
    }
    tokio::fs::write(path, contents)
        .await
        .with_context(|| format!("写入配置文件失败 {}", path.display()))
}

#[derive(Debug, Deserialize)]
struct ProbeRequest {
    base: String,
    key: String,
}

async fn probe_nvr(request: Request<Incoming>, state: &WebUiState) -> HttpResponse {
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                format!("读取请求体失败: {error}"),
            );
        }
    };
    let probe = match serde_json::from_slice::<ProbeRequest>(&body) {
        Ok(probe) => probe,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                format!("解析 NVR 探测请求失败: {error}"),
            );
        }
    };

    match fetch_cameras(&state.http_client, &probe.base, &probe.key).await {
        Ok(cameras) => json_response(StatusCode::OK, json!({"ok": true, "cameras": cameras})),
        Err(message) => json_response(StatusCode::OK, json!({"ok": false, "message": message})),
    }
}

async fn saved_nvr_cameras(state: &WebUiState) -> HttpResponse {
    let (base, key) = {
        let config = state.config.read().await;
        (config.nvr.api_base_url.clone(), config.nvr.api_key.clone())
    };

    match fetch_cameras(&state.http_client, &base, &key).await {
        Ok(cameras) => json_response(StatusCode::OK, json!({"ok": true, "cameras": cameras})),
        Err(message) => error_response(StatusCode::BAD_GATEWAY, "nvr_unreachable", message),
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct NvrCamera {
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    recording_enabled: Option<bool>,
}

async fn fetch_cameras(
    client: &reqwest::Client,
    base: &str,
    key: &str,
) -> std::result::Result<Vec<NvrCamera>, String> {
    let response = client
        .get(nvr_url(base, "/api/cameras"))
        .bearer_auth(key)
        .timeout(NVR_TIMEOUT)
        .send()
        .await
        .map_err(|error| format!("请求 NVR 失败: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("请求 NVR 失败: NVR 返回 HTTP {status}"));
    }
    response
        .json::<Vec<NvrCamera>>()
        .await
        .map_err(|error| format!("请求 NVR 失败: 解析摄像头响应失败: {error}"))
}

async fn status(state: &WebUiState) -> HttpResponse {
    let (camera, nvr_base) = {
        let config = state.config.read().await;
        (
            config.source.camera_whitelist.first().cloned(),
            config.nvr.api_base_url.clone(),
        )
    };
    let nvr = nvr_health(&state.http_client, &nvr_base).await;
    let last_pipeline_error = state.last_pipeline_error.read().await.clone();
    let recent_events = state.recent_events.read().await.clone();

    json_response(
        StatusCode::OK,
        json!({
            "camera": camera,
            "camera_cap": 1,
            "edition": "lite",
            "last_pipeline_error": last_pipeline_error,
            "nvr": nvr,
            "pipeline_running": state.pipeline_running.load(Ordering::Relaxed),
            "recent_events": recent_events,
            "targets": "linux-x86_64, linux-aarch64",
            "uptime_secs": state.started_at.elapsed().as_secs(),
            "version": state.version.load(Ordering::Relaxed),
        }),
    )
}

async fn nvr_health(client: &reqwest::Client, base: &str) -> Value {
    let response = match client
        .get(nvr_url(base, "/api/health"))
        .timeout(NVR_TIMEOUT)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => return json!({"reachable": false, "reason": error.to_string()}),
    };
    let status = response.status();
    if status.is_success() {
        json!({"reachable": true, "status": status.as_u16()})
    } else {
        json!({"reachable": false, "reason": format!("NVR 返回 HTTP {status}")})
    }
}

fn nvr_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

fn config_value(config: &AppConfig) -> serde_json::Result<Value> {
    let mut value = serde_json::to_value(config)?;
    if let Some(sink) = value.get_mut("sink").and_then(Value::as_object_mut) {
        sink.entry("sqlite").or_insert(Value::Null);
        sink.entry("snapshot").or_insert(Value::Null);
    }
    Ok(value)
}

fn html_response(contents: &str) -> HttpResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/html; charset=utf-8")
        .header(CACHE_CONTROL, "no-store")
        .header(CONNECTION, "close")
        .body(Full::new(Bytes::from(contents.to_owned())))
        .expect("静态 HTML 响应必须有效")
}

fn json_response(status: StatusCode, value: Value) -> HttpResponse {
    let body = serde_json::to_vec(&value).expect("JSON Value 必须可序列化");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
        .header(CACHE_CONTROL, "no-store")
        .header(CONNECTION, "close")
        .body(Full::new(Bytes::from(body)))
        .expect("静态 JSON 响应必须有效")
}

fn error_response(status: StatusCode, code: &str, message: impl Into<String>) -> HttpResponse {
    json_response(
        status,
        json!({"code": code, "message": message.into(), "ok": false}),
    )
}

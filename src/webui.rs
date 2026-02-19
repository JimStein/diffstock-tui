use crate::{config, data, inference, paper_trading, portfolio, train};
use anyhow::Result;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

const INDEX_HTML: &str = include_str!("../web/index.html");
const APP_JS: &str = include_str!("../web/app.js");

#[derive(Clone)]
struct WebState {
    backend_default: config::ComputeBackend,
    train: Arc<Mutex<TrainRuntimeState>>,
    paper: Arc<Mutex<PaperRuntimeState>>,
    forecast: Arc<Mutex<ForecastRuntimeState>>,
    portfolio: Arc<Mutex<PortfolioRuntimeState>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum ApiComputeBackend {
    Auto,
    Cuda,
    Directml,
    Cpu,
}

impl From<ApiComputeBackend> for config::ComputeBackend {
    fn from(value: ApiComputeBackend) -> Self {
        match value {
            ApiComputeBackend::Auto => config::ComputeBackend::Auto,
            ApiComputeBackend::Cuda => config::ComputeBackend::Cuda,
            ApiComputeBackend::Directml => config::ComputeBackend::Directml,
            ApiComputeBackend::Cpu => config::ComputeBackend::Cpu,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ApiError {
    error: String,
}

#[derive(Clone, Debug, Serialize)]
struct TrainRuntimeState {
    running: bool,
    started_at: Option<String>,
    finished_at: Option<String>,
    last_message: Option<String>,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Default)]
struct ForecastRuntimeState {
    last_request: Option<ForecastRequestState>,
    last_result: Option<ForecastResponse>,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ForecastRequestState {
    symbol: String,
    horizon: usize,
    simulations: usize,
    compute_backend: String,
}

#[derive(Clone, Debug, Serialize, Default)]
struct PortfolioRuntimeState {
    last_symbols: Vec<String>,
    last_allocation: Option<portfolio::PortfolioAllocation>,
    last_error: Option<String>,
    updated_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct FullUiState {
    forecast: ForecastRuntimeState,
    portfolio: PortfolioRuntimeState,
    train: TrainRuntimeState,
    paper: PaperRuntimeState,
}

impl Default for TrainRuntimeState {
    fn default() -> Self {
        Self {
            running: false,
            started_at: None,
            finished_at: None,
            last_message: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct PaperRuntimeState {
    running: bool,
    paused: bool,
    started_at: Option<String>,
    strategy_file: Option<String>,
    runtime_file: Option<String>,
    latest_snapshot: Option<paper_trading::MinutePortfolioSnapshot>,
    snapshots: Vec<paper_trading::MinutePortfolioSnapshot>,
    last_analysis: Option<paper_trading::AnalysisRecord>,
    logs: Vec<String>,
    #[serde(skip_serializing)]
    cmd_tx: Option<mpsc::Sender<paper_trading::PaperCommand>>,
}

impl Default for PaperRuntimeState {
    fn default() -> Self {
        Self {
            running: false,
            paused: false,
            started_at: None,
            strategy_file: None,
            runtime_file: None,
            latest_snapshot: None,
            snapshots: Vec::new(),
            last_analysis: None,
            logs: Vec::new(),
            cmd_tx: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct ForecastRequest {
    symbol: String,
    horizon: Option<usize>,
    simulations: Option<usize>,
    use_cuda: Option<bool>,
    compute_backend: Option<ApiComputeBackend>,
}

#[derive(Clone, Debug, Serialize)]
struct PricePoint {
    time: i64,
    value: f64,
}

#[derive(Clone, Debug, Serialize)]
struct ForecastResponse {
    symbol: String,
    history: Vec<PricePoint>,
    p10: Vec<PricePoint>,
    p30: Vec<PricePoint>,
    p50: Vec<PricePoint>,
    p70: Vec<PricePoint>,
    p90: Vec<PricePoint>,
}

#[derive(Debug, Deserialize)]
struct PortfolioRequest {
    symbols: Vec<String>,
    use_cuda: Option<bool>,
    compute_backend: Option<ApiComputeBackend>,
}

#[derive(Debug, Deserialize)]
struct QuotesRequest {
    symbols: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QuotesResponse {
    prices: HashMap<String, f64>,
}

#[derive(Debug, Deserialize)]
struct TrainStartRequest {
    epochs: Option<usize>,
    batch_size: Option<usize>,
    learning_rate: Option<f64>,
    patience: Option<usize>,
    use_cuda: Option<bool>,
    compute_backend: Option<ApiComputeBackend>,
}

#[derive(Debug, Deserialize)]
struct PaperTarget {
    symbol: String,
    weight: f64,
}

#[derive(Debug, Deserialize)]
struct PaperStartRequest {
    targets: Vec<PaperTarget>,
    initial_capital: Option<f64>,
    time1: Option<String>,
    time2: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PaperLoadRequest {
    strategy_file: String,
}

pub async fn run_webui_server(port: u16, backend_default: config::ComputeBackend) -> Result<()> {
    let state = WebState {
        backend_default,
        train: Arc::new(Mutex::new(TrainRuntimeState::default())),
        paper: Arc::new(Mutex::new(PaperRuntimeState::default())),
        forecast: Arc::new(Mutex::new(ForecastRuntimeState::default())),
        portfolio: Arc::new(Mutex::new(PortfolioRuntimeState::default())),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/api/health", get(health))
        .route("/api/state", get(full_state))
        .route("/api/forecast", post(forecast))
        .route("/api/portfolio", post(portfolio_opt))
        .route("/api/quotes", post(quotes))
        .route("/api/train/start", post(start_train))
        .route("/api/train/status", get(train_status))
        .route("/api/paper/start", post(start_paper))
        .route("/api/paper/load", post(load_paper))
        .route("/api/paper/status", get(paper_status))
        .route("/api/paper/pause", post(paper_pause))
        .route("/api/paper/resume", post(paper_resume))
        .route("/api/paper/stop", post(paper_stop))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("WebUI listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_js() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/javascript; charset=utf-8")], APP_JS)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn forecast(
    State(state): State<WebState>,
    Json(req): Json<ForecastRequest>,
) -> Result<Json<ForecastResponse>, (StatusCode, Json<ApiError>)> {
    let symbol = req.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "symbol is required"));
    }

    let requested_backend = match req.use_cuda {
        Some(true) => config::ComputeBackend::Cuda,
        Some(false) => config::ComputeBackend::Cpu,
        None => req.compute_backend.map(Into::into).unwrap_or(state.backend_default),
    };
    let backend = config::resolve_compute_backend(requested_backend, "webui-forecast");
    let horizon = req.horizon.unwrap_or(10);
    let simulations = req.simulations.unwrap_or(500);
    let backend_label = format!("{:?}", backend).to_lowercase();

    {
        let mut fs = state.forecast.lock().await;
        fs.last_request = Some(ForecastRequestState {
            symbol: symbol.clone(),
            horizon,
            simulations,
            compute_backend: backend_label.clone(),
        });
        fs.last_error = None;
    }

    let data = data::fetch_range(&symbol, "1y")
        .await
        .map_err(internal_err)?;
    let history = data
        .history
        .iter()
        .map(|c| PricePoint {
            time: c.date.timestamp(),
            value: c.close,
        })
        .collect::<Vec<_>>();

    let forecast = inference::run_inference_with_backend(
        Arc::new(data),
        horizon,
        simulations,
        None,
        backend,
    )
    .await
    .map_err(internal_err)?;

    let map_points = |v: Vec<(f64, f64)>| {
        v.into_iter()
            .map(|(t, p)| PricePoint {
                time: t as i64,
                value: p,
            })
            .collect::<Vec<_>>()
    };

    let response = ForecastResponse {
        symbol,
        history,
        p10: map_points(forecast.p10),
        p30: map_points(forecast.p30),
        p50: map_points(forecast.p50),
        p70: map_points(forecast.p70),
        p90: map_points(forecast.p90),
    };

    {
        let mut fs = state.forecast.lock().await;
        fs.last_result = Some(response.clone());
        fs.last_error = None;
    }

    Ok(Json(response))
}

async fn portfolio_opt(
    State(state): State<WebState>,
    Json(req): Json<PortfolioRequest>,
) -> Result<Json<portfolio::PortfolioAllocation>, (StatusCode, Json<ApiError>)> {
    let mut symbols: Vec<String> = req
        .symbols
        .iter()
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    symbols.sort();
    symbols.dedup();

    if symbols.len() < 2 {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            "at least 2 symbols are required",
        ));
    }

    let requested_backend = match req.use_cuda {
        Some(true) => config::ComputeBackend::Cuda,
        Some(false) => config::ComputeBackend::Cpu,
        None => req.compute_backend.map(Into::into).unwrap_or(state.backend_default),
    };
    let backend = config::resolve_compute_backend(requested_backend, "webui-portfolio");
    let alloc = portfolio::run_portfolio_optimization_with_backend(&symbols, backend)
        .await
        .map_err(internal_err)?;

    {
        let mut ps = state.portfolio.lock().await;
        ps.last_symbols = symbols;
        ps.last_allocation = Some(alloc.clone());
        ps.last_error = None;
        ps.updated_at = Some(chrono::Local::now().to_rfc3339());
    }

    Ok(Json(alloc))
}

async fn quotes(
    Json(req): Json<QuotesRequest>,
) -> Result<Json<QuotesResponse>, (StatusCode, Json<ApiError>)> {
    let mut symbols: Vec<String> = req
        .symbols
        .iter()
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    symbols.sort();
    symbols.dedup();

    if symbols.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "symbols cannot be empty"));
    }

    let mut prices = HashMap::new();
    for symbol in symbols {
        if let Ok(price) = data::fetch_latest_price_1m(&symbol).await {
            prices.insert(symbol, price);
        }
    }

    Ok(Json(QuotesResponse { prices }))
}

async fn full_state(
    State(state): State<WebState>,
) -> Result<Json<FullUiState>, (StatusCode, Json<ApiError>)> {
    let mut paper = state.paper.lock().await.clone();
    paper.cmd_tx = None;

    Ok(Json(FullUiState {
        forecast: state.forecast.lock().await.clone(),
        portfolio: state.portfolio.lock().await.clone(),
        train: state.train.lock().await.clone(),
        paper,
    }))
}

async fn start_train(
    State(state): State<WebState>,
    Json(req): Json<TrainStartRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    {
        let mut train_state = state.train.lock().await;
        if train_state.running {
            return Err(api_err(
                StatusCode::CONFLICT,
                "training is already running",
            ));
        }
        train_state.running = true;
        train_state.started_at = Some(chrono::Local::now().to_rfc3339());
        train_state.finished_at = None;
        train_state.last_error = None;
        train_state.last_message = Some("Training started".to_string());
    }

    let train_state = state.train.clone();
    let requested_backend = match req.use_cuda {
        Some(true) => config::ComputeBackend::Cuda,
        Some(false) => config::ComputeBackend::Cpu,
        None => req.compute_backend.map(Into::into).unwrap_or(state.backend_default),
    };
    let backend = config::resolve_compute_backend(requested_backend, "webui-train");
    let use_cuda = if matches!(backend, config::ComputeBackend::Cuda) {
        true
    } else {
        if matches!(backend, config::ComputeBackend::Directml) {
            warn!("WebUI train requested directml; training path still uses candle and falls back to CPU.");
        }
        false
    };
    tokio::spawn(async move {
        let result = train::train_model(
            req.epochs,
            req.batch_size,
            req.learning_rate,
            req.patience,
            use_cuda,
        )
        .await;

        let mut state = train_state.lock().await;
        state.running = false;
        state.finished_at = Some(chrono::Local::now().to_rfc3339());
        match result {
            Ok(_) => {
                state.last_message = Some("Training completed".to_string());
                state.last_error = None;
            }
            Err(err) => {
                state.last_message = Some("Training failed".to_string());
                state.last_error = Some(err.to_string());
            }
        }
    });

    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn train_status(
    State(state): State<WebState>,
) -> Result<Json<TrainRuntimeState>, (StatusCode, Json<ApiError>)> {
    Ok(Json(state.train.lock().await.clone()))
}

async fn start_paper(
    State(state): State<WebState>,
    Json(req): Json<PaperStartRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    if req.targets.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "targets cannot be empty"));
    }

    let weights = req
        .targets
        .iter()
        .map(|t| (t.symbol.trim().to_uppercase(), t.weight))
        .collect::<Vec<_>>();

    let capital_str = req.initial_capital.map(|v| format!("{v}"));
    let t1 = req.time1.unwrap_or_else(|| "23:30".to_string());
    let t2 = req.time2.unwrap_or_else(|| "02:30".to_string());
    let cfg = paper_trading::build_config(capital_str.as_deref(), &t1, &t2)
        .map_err(|e| api_err(StatusCode::BAD_REQUEST, &e.to_string()))?;

    let (event_tx, mut event_rx) = mpsc::channel(1024);
    let (cmd_tx, cmd_rx) = mpsc::channel(64);

    {
        let mut paper_state = state.paper.lock().await;
        paper_state.running = true;
        paper_state.paused = false;
        paper_state.started_at = Some(chrono::Local::now().to_rfc3339());
        paper_state.strategy_file = None;
        paper_state.runtime_file = None;
        paper_state.latest_snapshot = None;
        paper_state.snapshots.clear();
        paper_state.last_analysis = None;
        paper_state.logs.clear();
        paper_state.cmd_tx = Some(cmd_tx.clone());
    }

    let paper_state_for_events = state.paper.clone();
    tokio::spawn(async move {
        while let Some(ev) = event_rx.recv().await {
            let mut ps = paper_state_for_events.lock().await;
            match ev {
                paper_trading::PaperEvent::Started {
                    strategy_file,
                    runtime_file,
                } => {
                    ps.strategy_file = Some(strategy_file);
                    ps.runtime_file = Some(runtime_file);
                    ps.logs.push("Paper trading started".to_string());
                }
                paper_trading::PaperEvent::Info(msg) => {
                    ps.logs.push(msg);
                }
                paper_trading::PaperEvent::Warning(msg) => {
                    ps.logs.push(format!("Warning: {}", msg));
                }
                paper_trading::PaperEvent::Analysis(a) => {
                    ps.last_analysis = Some(a);
                }
                paper_trading::PaperEvent::Minute(m) => {
                    ps.latest_snapshot = Some(m.clone());
                    ps.snapshots.push(m);
                    if ps.snapshots.len() > 6000 {
                        let keep_from = ps.snapshots.len().saturating_sub(6000);
                        ps.snapshots = ps.snapshots.split_off(keep_from);
                    }
                }
                paper_trading::PaperEvent::Error(msg) => {
                    ps.logs.push(format!("Error: {}", msg));
                    ps.running = false;
                    ps.paused = false;
                }
            }
            if ps.logs.len() > 200 {
                let keep_from = ps.logs.len().saturating_sub(200);
                ps.logs = ps.logs.split_off(keep_from);
            }
        }
    });

    let paper_state_runner = state.paper.clone();
    tokio::spawn(async move {
        let res = paper_trading::run_paper_trading(weights, cfg, event_tx, cmd_rx).await;
        let mut ps = paper_state_runner.lock().await;
        ps.running = false;
        ps.paused = false;
        ps.cmd_tx = None;
        if let Err(err) = res {
            ps.logs.push(format!("Error: {}", err));
        } else {
            ps.logs.push("Paper trading stopped".to_string());
        }
    });

    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn load_paper(
    State(state): State<WebState>,
    Json(req): Json<PaperLoadRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let strategy_file = req.strategy_file.trim().to_string();
    if strategy_file.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "strategy_file cannot be empty"));
    }

    let (event_tx, mut event_rx) = mpsc::channel(1024);
    let (cmd_tx, cmd_rx) = mpsc::channel(64);

    {
        let mut paper_state = state.paper.lock().await;
        paper_state.running = true;
        paper_state.paused = false;
        paper_state.started_at = Some(chrono::Local::now().to_rfc3339());
        paper_state.strategy_file = Some(strategy_file.clone());
        paper_state.runtime_file = None;
        paper_state.latest_snapshot = None;
        paper_state.snapshots.clear();
        paper_state.last_analysis = None;
        paper_state.logs.clear();
        paper_state.cmd_tx = Some(cmd_tx.clone());
    }

    let paper_state_for_events = state.paper.clone();
    tokio::spawn(async move {
        while let Some(ev) = event_rx.recv().await {
            let mut ps = paper_state_for_events.lock().await;
            match ev {
                paper_trading::PaperEvent::Started {
                    strategy_file,
                    runtime_file,
                } => {
                    ps.strategy_file = Some(strategy_file);
                    ps.runtime_file = Some(runtime_file);
                    ps.logs.push("Paper trading loaded".to_string());
                }
                paper_trading::PaperEvent::Info(msg) => {
                    ps.logs.push(msg);
                }
                paper_trading::PaperEvent::Warning(msg) => {
                    ps.logs.push(format!("Warning: {}", msg));
                }
                paper_trading::PaperEvent::Analysis(a) => {
                    ps.last_analysis = Some(a);
                }
                paper_trading::PaperEvent::Minute(m) => {
                    ps.latest_snapshot = Some(m.clone());
                    ps.snapshots.push(m);
                    if ps.snapshots.len() > 6000 {
                        let keep_from = ps.snapshots.len().saturating_sub(6000);
                        ps.snapshots = ps.snapshots.split_off(keep_from);
                    }
                }
                paper_trading::PaperEvent::Error(msg) => {
                    ps.logs.push(format!("Error: {}", msg));
                    ps.running = false;
                    ps.paused = false;
                }
            }
            if ps.logs.len() > 200 {
                let keep_from = ps.logs.len().saturating_sub(200);
                ps.logs = ps.logs.split_off(keep_from);
            }
        }
    });

    let paper_state_runner = state.paper.clone();
    tokio::spawn(async move {
        let res = paper_trading::run_paper_trading_from_strategy_file(&strategy_file, event_tx, cmd_rx).await;
        let mut ps = paper_state_runner.lock().await;
        ps.running = false;
        ps.paused = false;
        ps.cmd_tx = None;
        if let Err(err) = res {
            ps.logs.push(format!("Error: {}", err));
        } else {
            ps.logs.push("Paper trading stopped".to_string());
        }
    });

    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn paper_status(
    State(state): State<WebState>,
) -> Result<Json<PaperRuntimeState>, (StatusCode, Json<ApiError>)> {
    let mut status = state.paper.lock().await.clone();
    status.cmd_tx = None;
    Ok(Json(status))
}

async fn paper_pause(
    State(state): State<WebState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let tx = {
        let mut ps = state.paper.lock().await;
        ps.paused = true;
        ps.cmd_tx.clone()
    };
    if let Some(tx) = tx {
        tx.send(paper_trading::PaperCommand::Pause)
            .await
            .map_err(internal_err)?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn paper_resume(
    State(state): State<WebState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let tx = {
        let mut ps = state.paper.lock().await;
        ps.paused = false;
        ps.cmd_tx.clone()
    };
    if let Some(tx) = tx {
        tx.send(paper_trading::PaperCommand::Resume)
            .await
            .map_err(internal_err)?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn paper_stop(
    State(state): State<WebState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let tx = {
        let mut ps = state.paper.lock().await;
        ps.running = false;
        ps.paused = false;
        ps.cmd_tx.take()
    };
    if let Some(tx) = tx {
        tx.send(paper_trading::PaperCommand::Stop)
            .await
            .map_err(internal_err)?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

fn api_err(status: StatusCode, message: &str) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            error: message.to_string(),
        }),
    )
}

fn internal_err<E: std::fmt::Display>(err: E) -> (StatusCode, Json<ApiError>) {
    api_err(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string())
}

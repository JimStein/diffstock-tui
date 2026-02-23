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

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(default)]
struct ForecastRuntimeState {
    last_request: Option<ForecastRequestState>,
    last_result: Option<ForecastResponse>,
    last_error: Option<String>,
    cached_results: HashMap<String, CachedForecastEntry>,
}

const FORECAST_CACHE_FILE: &str = "log/webui_forecast_cache.json";

fn forecast_cache_path() -> std::path::PathBuf {
    config::project_root_path().join(FORECAST_CACHE_FILE)
}

fn load_forecast_state() -> Result<ForecastRuntimeState> {
    let path = forecast_cache_path();
    if !path.exists() {
        return Ok(ForecastRuntimeState::default());
    }
    let raw = std::fs::read_to_string(path)?;
    let state = serde_json::from_str::<ForecastRuntimeState>(&raw)?;
    Ok(state)
}

fn save_forecast_state(state: &ForecastRuntimeState) -> Result<()> {
    let path = forecast_cache_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(state)?;
    std::fs::write(path, json)?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ForecastRequestState {
    symbol: String,
    horizon: usize,
    simulations: usize,
    compute_backend: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CachedForecastEntry {
    request: ForecastRequestState,
    result: ForecastResponse,
    forecasted_at: String,
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
    data_live_source: String,
    data_ws_connected: bool,
    data_ws_diagnostics: data::WsDiagnostics,
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
    auto_optimizing: bool,
    optimization_time_local: Option<String>,
    optimization_weekdays: Vec<u32>,
    auto_opt_retry_count: u32,
    auto_opt_retry_max: u32,
    auto_opt_retry_next_at: Option<String>,
    started_at: Option<String>,
    strategy_file: Option<String>,
    runtime_file: Option<String>,
    candidate_symbols: Vec<String>,
    target_weights: Vec<PaperTargetState>,
    latest_snapshot: Option<paper_trading::MinutePortfolioSnapshot>,
    snapshots: Vec<paper_trading::MinutePortfolioSnapshot>,
    last_analysis: Option<paper_trading::AnalysisRecord>,
    trade_history: Vec<paper_trading::TradeRecord>,
    logs: Vec<String>,
    data_live_source: String,
    data_ws_connected: bool,
    data_ws_diagnostics: data::WsDiagnostics,
    #[serde(skip_serializing)]
    cmd_tx: Option<mpsc::Sender<paper_trading::PaperCommand>>,
}

impl Default for PaperRuntimeState {
    fn default() -> Self {
        Self {
            running: false,
            paused: false,
            auto_optimizing: false,
            optimization_time_local: None,
            optimization_weekdays: Vec::new(),
            auto_opt_retry_count: 0,
            auto_opt_retry_max: 10,
            auto_opt_retry_next_at: None,
            started_at: None,
            strategy_file: None,
            runtime_file: None,
            candidate_symbols: Vec::new(),
            target_weights: Vec::new(),
            latest_snapshot: None,
            snapshots: Vec::new(),
            last_analysis: None,
            trade_history: Vec::new(),
            logs: Vec::new(),
            data_live_source: "Unknown".to_string(),
            data_ws_connected: false,
            data_ws_diagnostics: data::WsDiagnostics::default(),
            cmd_tx: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct PaperTargetState {
    symbol: String,
    weight: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct ForecastRequest {
    symbol: String,
    horizon: Option<usize>,
    simulations: Option<usize>,
    use_cuda: Option<bool>,
    compute_backend: Option<ApiComputeBackend>,
}

#[derive(Clone, Debug, Deserialize)]
struct ForecastBatchRequest {
    symbols: Vec<String>,
    horizon: Option<usize>,
    simulations: Option<usize>,
    use_cuda: Option<bool>,
    compute_backend: Option<ApiComputeBackend>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PricePoint {
    time: i64,
    value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ForecastResponse {
    symbol: String,
    history: Vec<PricePoint>,
    p10: Vec<PricePoint>,
    p30: Vec<PricePoint>,
    p50: Vec<PricePoint>,
    p70: Vec<PricePoint>,
    p90: Vec<PricePoint>,
    forecasted_at: Option<String>,
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
    exchange_ts_ms: HashMap<String, i64>,
    sources: HashMap<String, String>,
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
}

#[derive(Debug, Deserialize)]
struct PaperStartRequest {
    targets: Vec<PaperTarget>,
    initial_capital: Option<f64>,
    time1: Option<String>,
    time2: Option<String>,
    optimization_time: Option<String>,
    optimization_weekdays: Option<Vec<u32>>,
}

#[derive(Debug, Deserialize)]
struct PaperLoadRequest {
    strategy_file: String,
}

#[derive(Debug, Deserialize)]
struct PaperTargetsUpdateRequest {
    symbols: Vec<String>,
    apply_now: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct PaperOptimizationUpdateRequest {
    optimization_time: Option<String>,
    optimization_weekdays: Option<Vec<u32>>,
}

pub async fn run_webui_server(port: u16, backend_default: config::ComputeBackend) -> Result<()> {
    let forecast_state = match load_forecast_state() {
        Ok(state) => state,
        Err(err) => {
            warn!("failed to load forecast cache: {}", err);
            ForecastRuntimeState::default()
        }
    };

    let state = WebState {
        backend_default,
        train: Arc::new(Mutex::new(TrainRuntimeState::default())),
        paper: Arc::new(Mutex::new(PaperRuntimeState::default())),
        forecast: Arc::new(Mutex::new(forecast_state)),
        portfolio: Arc::new(Mutex::new(PortfolioRuntimeState::default())),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/api/health", get(health))
        .route("/api/state", get(full_state))
        .route("/api/forecast", post(forecast))
        .route("/api/forecast/batch", post(forecast_batch))
        .route("/api/portfolio", post(portfolio_opt))
        .route("/api/quotes", post(quotes))
        .route("/api/train/start", post(start_train))
        .route("/api/train/status", get(train_status))
        .route("/api/paper/start", post(start_paper))
        .route("/api/paper/load", post(load_paper))
        .route("/api/paper/status", get(paper_status))
        .route("/api/paper/targets", post(paper_targets_update))
        .route("/api/paper/optimization", post(paper_optimization_update))
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
    let request_state = ForecastRequestState {
        symbol: symbol.clone(),
        horizon,
        simulations,
        compute_backend: backend_label.clone(),
    };

    {
        let mut fs = state.forecast.lock().await;
        fs.last_request = Some(request_state.clone());
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

    let forecasted_at = chrono::Local::now().to_rfc3339();

    let response = ForecastResponse {
        symbol,
        history,
        p10: map_points(forecast.p10),
        p30: map_points(forecast.p30),
        p50: map_points(forecast.p50),
        p70: map_points(forecast.p70),
        p90: map_points(forecast.p90),
        forecasted_at: Some(forecasted_at.clone()),
    };

    let forecast_snapshot = {
        let mut fs = state.forecast.lock().await;
        fs.last_result = Some(response.clone());
        fs.last_error = None;
        fs.cached_results.insert(
            response.symbol.clone(),
            CachedForecastEntry {
                request: request_state,
                result: response.clone(),
                forecasted_at,
            },
        );
        fs.clone()
    };

    if let Err(err) = save_forecast_state(&forecast_snapshot) {
        warn!("failed to persist forecast cache: {}", err);
    }

    Ok(Json(response))
}

async fn forecast_batch(
    State(state): State<WebState>,
    Json(req): Json<ForecastBatchRequest>,
) -> Result<Json<Vec<ForecastResponse>>, (StatusCode, Json<ApiError>)> {
    let mut symbols: Vec<String> = req
        .symbols
        .iter()
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    symbols.sort();
    symbols.dedup();

    if symbols.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "symbols is required"));
    }

    let requested_backend = match req.use_cuda {
        Some(true) => config::ComputeBackend::Cuda,
        Some(false) => config::ComputeBackend::Cpu,
        None => req.compute_backend.map(Into::into).unwrap_or(state.backend_default),
    };
    let backend = config::resolve_compute_backend(requested_backend, "webui-forecast-batch");
    let horizon = req.horizon.unwrap_or(10);
    let simulations = req.simulations.unwrap_or(500);
    let backend_label = format!("{:?}", backend).to_lowercase();

    {
        let mut fs = state.forecast.lock().await;
        fs.last_request = Some(ForecastRequestState {
            symbol: symbols.join(","),
            horizon,
            simulations,
            compute_backend: backend_label,
        });
        fs.last_error = None;
    }

    let prefetched = data::fetch_ranges_prefetch(&symbols, "1y")
        .await
        .map_err(internal_err)?;

    let mut responses = Vec::with_capacity(symbols.len());
    for symbol in &symbols {
        let data = prefetched
            .get(symbol)
            .cloned()
            .ok_or(api_err(StatusCode::INTERNAL_SERVER_ERROR, "prefetched data missing"))?;

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

        responses.push(ForecastResponse {
            symbol: symbol.clone(),
            history,
            p10: map_points(forecast.p10),
            p30: map_points(forecast.p30),
            p50: map_points(forecast.p50),
            p70: map_points(forecast.p70),
            p90: map_points(forecast.p90),
            forecasted_at: Some(chrono::Local::now().to_rfc3339()),
        });
    }

    let forecast_snapshot = {
        let mut fs = state.forecast.lock().await;
        if let Some(first) = responses.first().cloned() {
            fs.last_result = Some(first);
        }
        for row in &responses {
            let req_row = ForecastRequestState {
                symbol: row.symbol.clone(),
                horizon,
                simulations,
                compute_backend: format!("{:?}", backend).to_lowercase(),
            };
            fs.cached_results.insert(
                row.symbol.clone(),
                CachedForecastEntry {
                    request: req_row,
                    result: row.clone(),
                    forecasted_at: row
                        .forecasted_at
                        .clone()
                        .unwrap_or_else(|| chrono::Local::now().to_rfc3339()),
                },
            );
        }
        fs.clone()
    };

    if let Err(err) = save_forecast_state(&forecast_snapshot) {
        warn!("failed to persist forecast cache: {}", err);
    }

    Ok(Json(responses))
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
    let mut exchange_ts_ms = HashMap::new();
    let mut sources = HashMap::new();
    for symbol in symbols {
        if let Ok(quote) = data::fetch_latest_price_with_meta(&symbol).await {
            prices.insert(symbol.clone(), quote.price);
            exchange_ts_ms.insert(symbol.clone(), quote.exchange_ts_ms);
            sources.insert(symbol, quote.source);
        }
    }

    Ok(Json(QuotesResponse {
        prices,
        exchange_ts_ms,
        sources,
    }))
}

async fn full_state(
    State(state): State<WebState>,
) -> Result<Json<FullUiState>, (StatusCode, Json<ApiError>)> {
    let mut paper = state.paper.lock().await.clone();
    paper.cmd_tx = None;
    let data_live_source = data::current_live_data_source().await;
    let data_ws_connected = data::polygon_ws_connected().await;
    let data_ws_diagnostics = data::current_ws_diagnostics().await;
    paper.data_live_source = data_live_source.clone();
    paper.data_ws_connected = data_ws_connected;
    paper.data_ws_diagnostics = data_ws_diagnostics.clone();

    Ok(Json(FullUiState {
        forecast: state.forecast.lock().await.clone(),
        portfolio: state.portfolio.lock().await.clone(),
        train: state.train.lock().await.clone(),
        paper,
        data_live_source,
        data_ws_connected,
        data_ws_diagnostics,
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

    let mut symbols = req
        .targets
        .iter()
        .map(|t| t.symbol.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();
    if symbols.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "targets cannot be empty"));
    }

    let optimized_targets = optimize_candidate_pool_targets(&symbols, state.backend_default)
        .await
        .map_err(internal_err)?;
    let weights = optimized_targets
        .iter()
        .map(|t| (t.symbol.clone(), t.target_weight))
        .collect::<Vec<_>>();

    let capital_str = req.initial_capital.map(|v| format!("{v}"));
    let t1 = req.time1.unwrap_or_else(|| "23:30".to_string());
    let t2 = req.time2.unwrap_or_else(|| "02:30".to_string());
    let cfg = paper_trading::build_config(
        capital_str.as_deref(),
        &t1,
        &t2,
        req.optimization_time.as_deref(),
        req.optimization_weekdays.as_deref(),
        state.backend_default,
    )
    .map_err(|e| api_err(StatusCode::BAD_REQUEST, &e.to_string()))?;

    let (event_tx, mut event_rx) = mpsc::channel(1024);
    let (cmd_tx, cmd_rx) = mpsc::channel(64);

    {
        let mut paper_state = state.paper.lock().await;
        paper_state.running = true;
        paper_state.paused = false;
        paper_state.auto_optimizing = false;
        paper_state.optimization_time_local = cfg
            .optimization_time_local
            .map(|time| time.format("%H:%M").to_string());
        paper_state.optimization_weekdays = cfg.optimization_weekdays.clone();
        paper_state.started_at = Some(chrono::Local::now().to_rfc3339());
        paper_state.strategy_file = None;
        paper_state.runtime_file = None;
        paper_state.candidate_symbols = symbols.clone();
        paper_state.target_weights = optimized_targets
            .iter()
            .map(|target| PaperTargetState {
                symbol: target.symbol.clone(),
                weight: target.target_weight,
            })
            .collect();
        paper_state.latest_snapshot = None;
        paper_state.snapshots.clear();
        paper_state.last_analysis = None;
        paper_state.trade_history.clear();
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
                paper_trading::PaperEvent::AutoOptimizationStatus { running } => {
                    ps.auto_optimizing = running;
                }
                paper_trading::PaperEvent::AutoOptimizationRetryStatus {
                    retry_count,
                    max_retries,
                    next_retry_at,
                } => {
                    ps.auto_opt_retry_count = retry_count;
                    ps.auto_opt_retry_max = max_retries;
                    ps.auto_opt_retry_next_at = next_retry_at;
                }
                paper_trading::PaperEvent::TargetsUpdated { targets } => {
                    ps.target_weights = targets
                        .iter()
                        .map(|target| PaperTargetState {
                            symbol: target.symbol.clone(),
                            weight: target.target_weight,
                        })
                        .collect();
                }
                paper_trading::PaperEvent::Warning(msg) => {
                    ps.logs.push(format!("WARNING: {}", msg));
                }
                paper_trading::PaperEvent::Analysis(a) => {
                    ps.trade_history.extend(a.trades.clone());
                    if ps.trade_history.len() > 6000 {
                        let keep_from = ps.trade_history.len().saturating_sub(6000);
                        ps.trade_history = ps.trade_history.split_off(keep_from);
                    }
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
                    ps.auto_optimizing = false;
                }
            }
            if ps.logs.len() > 200 {
                let keep_from = ps.logs.len().saturating_sub(200);
                ps.logs = ps.logs.split_off(keep_from);
            }
        }
    });

    let paper_state_runner = state.paper.clone();
    let candidate_symbols = symbols.clone();
    tokio::spawn(async move {
        let res = paper_trading::run_paper_trading(candidate_symbols, weights, cfg, event_tx, cmd_rx).await;
        let mut ps = paper_state_runner.lock().await;
        ps.running = false;
        ps.paused = false;
        ps.auto_optimizing = false;
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
    let strategy_summary = load_strategy_summary(&strategy_file);
    let historical_trades = strategy_summary
        .as_ref()
        .map(|summary| summary.trades.clone())
        .unwrap_or_default();
    let historical_targets = strategy_summary
        .as_ref()
        .map(|summary| summary.targets.clone())
        .unwrap_or_default();
    let historical_candidate_symbols = strategy_summary
        .as_ref()
        .map(|summary| summary.candidate_symbols.clone())
        .unwrap_or_default();
    let historical_optimization_time = strategy_summary
        .as_ref()
        .and_then(|summary| summary.optimization_time_local.clone());
    let historical_optimization_weekdays = strategy_summary
        .as_ref()
        .map(|summary| summary.optimization_weekdays.clone())
        .unwrap_or_default();

    {
        let mut paper_state = state.paper.lock().await;
        paper_state.running = true;
        paper_state.paused = false;
        paper_state.auto_optimizing = false;
        paper_state.optimization_time_local = historical_optimization_time;
        paper_state.optimization_weekdays = historical_optimization_weekdays;
        paper_state.started_at = Some(chrono::Local::now().to_rfc3339());
        paper_state.strategy_file = Some(strategy_file.clone());
        paper_state.runtime_file = None;
        paper_state.candidate_symbols = historical_candidate_symbols;
        paper_state.target_weights = historical_targets;
        paper_state.latest_snapshot = None;
        paper_state.snapshots.clear();
        paper_state.last_analysis = None;
        paper_state.trade_history = historical_trades;
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
                    ps.logs
                        .push("Paper history loaded · Restored holdings and running".to_string());
                }
                paper_trading::PaperEvent::Info(msg) => {
                    ps.logs.push(msg);
                }
                paper_trading::PaperEvent::AutoOptimizationStatus { running } => {
                    ps.auto_optimizing = running;
                }
                paper_trading::PaperEvent::AutoOptimizationRetryStatus {
                    retry_count,
                    max_retries,
                    next_retry_at,
                } => {
                    ps.auto_opt_retry_count = retry_count;
                    ps.auto_opt_retry_max = max_retries;
                    ps.auto_opt_retry_next_at = next_retry_at;
                }
                paper_trading::PaperEvent::TargetsUpdated { targets } => {
                    ps.target_weights = targets
                        .iter()
                        .map(|target| PaperTargetState {
                            symbol: target.symbol.clone(),
                            weight: target.target_weight,
                        })
                        .collect();
                }
                paper_trading::PaperEvent::Warning(msg) => {
                    ps.logs.push(format!("WARNING: {}", msg));
                }
                paper_trading::PaperEvent::Analysis(a) => {
                    ps.trade_history.extend(a.trades.clone());
                    if ps.trade_history.len() > 6000 {
                        let keep_from = ps.trade_history.len().saturating_sub(6000);
                        ps.trade_history = ps.trade_history.split_off(keep_from);
                    }
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
                    ps.auto_optimizing = false;
                }
            }
            if ps.logs.len() > 200 {
                let keep_from = ps.logs.len().saturating_sub(200);
                ps.logs = ps.logs.split_off(keep_from);
            }
        }
    });

    let paper_state_runner = state.paper.clone();
    let backend = state.backend_default;
    tokio::spawn(async move {
        let res = paper_trading::run_paper_trading_from_strategy_file(
            &strategy_file,
            backend,
            event_tx,
            cmd_rx,
        )
        .await;
        let mut ps = paper_state_runner.lock().await;
        ps.running = false;
        ps.paused = false;
        ps.auto_optimizing = false;
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
    status.data_live_source = data::current_live_data_source().await;
    status.data_ws_connected = data::polygon_ws_connected().await;
    status.data_ws_diagnostics = data::current_ws_diagnostics().await;
    Ok(Json(status))
}

async fn paper_targets_update(
    State(state): State<WebState>,
    Json(req): Json<PaperTargetsUpdateRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
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

    let next_target_cmd = optimize_candidate_pool_targets(&symbols, state.backend_default)
        .await
        .map_err(internal_err)?;
    let next_target_state = next_target_cmd
        .iter()
        .map(|target| PaperTargetState {
            symbol: target.symbol.clone(),
            weight: target.target_weight,
        })
        .collect::<Vec<_>>();

    let apply_now = req.apply_now.unwrap_or(false);

    if apply_now {
        let mut preflight_symbols = symbols.clone();
        let snapshot_holdings = {
            let ps = state.paper.lock().await;
            ps.latest_snapshot
                .as_ref()
                .map(|snap| snap.holdings_symbols.clone())
                .unwrap_or_default()
        };
        preflight_symbols.extend(snapshot_holdings);
        preflight_symbols.push(paper_trading::BENCHMARK_SYMBOL.to_string());
        preflight_symbols.sort();
        preflight_symbols.dedup();

        for symbol in &preflight_symbols {
            if let Err(error) = data::fetch_latest_price_1m(symbol).await {
                return Err(api_err(
                    StatusCode::BAD_GATEWAY,
                    &format!(
                        "Data fetch failed for {}. Apply-now optimization stopped: {}",
                        symbol,
                        error
                    ),
                ));
            }
        }
    }

    let tx = {
        let mut ps = state.paper.lock().await;
        ps.candidate_symbols = symbols.clone();
        ps.target_weights = next_target_state;
        ps.cmd_tx.clone()
    };

    if let Some(tx) = tx {
        tx.send(paper_trading::PaperCommand::UpdateTargets {
            candidate_symbols: symbols.clone(),
            targets: next_target_cmd,
            apply_now,
        })
            .await
            .map_err(internal_err)?;
    }

    Ok(Json(serde_json::json!({ "ok": true, "symbols": symbols, "apply_now": apply_now })))
}

async fn paper_optimization_update(
    State(state): State<WebState>,
    Json(req): Json<PaperOptimizationUpdateRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let parsed_time = match req.optimization_time.as_deref().map(str::trim) {
        Some("") => None,
        Some(text) => Some(
            chrono::NaiveTime::parse_from_str(text, "%H:%M")
                .map_err(|_| api_err(StatusCode::BAD_REQUEST, "Invalid optimization_time, use HH:MM"))?,
        ),
        None => None,
    };

    let normalized_days_opt = req.optimization_weekdays.map(|days| {
        let mut normalized = days
            .into_iter()
            .filter(|day| (1..=7).contains(day))
            .collect::<Vec<_>>();
        normalized.sort_unstable();
        normalized.dedup();
        if normalized.is_empty() {
            vec![1, 2, 3, 4, 5]
        } else {
            normalized
        }
    });

    let (tx, running, effective_time_text, effective_time_naive, effective_days) = {
        let mut ps = state.paper.lock().await;

        let next_time_text = if req.optimization_time.is_some() {
            parsed_time.map(|time| time.format("%H:%M").to_string())
        } else {
            ps.optimization_time_local.clone()
        };

        let mut next_days = if let Some(days) = normalized_days_opt {
            days
        } else if !ps.optimization_weekdays.is_empty() {
            ps.optimization_weekdays.clone()
        } else {
            vec![1, 2, 3, 4, 5]
        };
        next_days.sort_unstable();
        next_days.dedup();
        if next_days.is_empty() {
            next_days = vec![1, 2, 3, 4, 5];
        }

        ps.optimization_time_local = next_time_text.clone();
        ps.optimization_weekdays = next_days.clone();

        let next_time_naive = next_time_text
            .as_deref()
            .and_then(|text| chrono::NaiveTime::parse_from_str(text, "%H:%M").ok());

        (
            ps.cmd_tx.clone(),
            ps.running,
            next_time_text,
            next_time_naive,
            next_days,
        )
    };

    if running {
        if let Some(tx) = tx {
            tx.send(paper_trading::PaperCommand::UpdateOptimizationSchedule {
                optimization_time_local: effective_time_naive,
                optimization_weekdays: effective_days.clone(),
            })
            .await
            .map_err(internal_err)?;
        }
    }

    Ok(Json(serde_json::json!({
        "ok": true,
        "optimization_time": effective_time_text,
        "optimization_weekdays": effective_days,
    })))
}

async fn optimize_candidate_pool_targets(
    symbols: &[String],
    backend: config::ComputeBackend,
) -> Result<Vec<paper_trading::TargetWeight>> {
    if symbols.len() == 1 {
        return Ok(vec![paper_trading::TargetWeight {
            symbol: symbols[0].clone(),
            target_weight: 1.0,
        }]);
    }

    let allocation = portfolio::run_portfolio_optimization_with_backend(symbols, backend).await?;
    let mut out = allocation
        .weights
        .into_iter()
        .filter_map(|(symbol, target_weight)| {
            let weight = target_weight.clamp(0.0, 1.0);
            if weight > 0.0 {
                Some(paper_trading::TargetWeight {
                    symbol,
                    target_weight: weight,
                })
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if out.is_empty() {
        return Err(anyhow::anyhow!("Portfolio optimization returned empty weights"));
    }

    let total_weight: f64 = out.iter().map(|x| x.target_weight).sum();
    if total_weight > 1.0 {
        for target in &mut out {
            target.target_weight /= total_weight;
        }
    }

    Ok(out)
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

#[derive(Clone)]
struct StrategySummary {
    trades: Vec<paper_trading::TradeRecord>,
    candidate_symbols: Vec<String>,
    targets: Vec<PaperTargetState>,
    optimization_time_local: Option<String>,
    optimization_weekdays: Vec<u32>,
}

fn load_strategy_summary(strategy_file: &str) -> Option<StrategySummary> {
    let input = std::path::PathBuf::from(strategy_file);
    let resolved = if input.is_absolute() {
        input
    } else if input.exists() {
        input
    } else {
        config::project_root_path().join(input)
    };

    let Ok(raw) = std::fs::read_to_string(resolved) else {
        return None;
    };
    let Ok(strategy_log) = serde_json::from_str::<paper_trading::StrategyLog>(&raw) else {
        return None;
    };

    let mut trades = Vec::new();
    for analysis in &strategy_log.analyses {
        trades.extend(analysis.trades.clone());
    }

    let targets = strategy_log
        .targets
        .iter()
        .map(|target| PaperTargetState {
            symbol: target.symbol.clone(),
            weight: target.target_weight,
        })
        .collect::<Vec<_>>();

    let mut candidate_symbols = if strategy_log.candidate_symbols.is_empty() {
        strategy_log
            .targets
            .iter()
            .map(|target| target.symbol.clone())
            .collect::<Vec<_>>()
    } else {
        strategy_log.candidate_symbols.clone()
    };
    candidate_symbols = candidate_symbols
        .into_iter()
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    candidate_symbols.sort_unstable();
    candidate_symbols.dedup();

    let optimization_time_local = strategy_log.optimization_time_local.clone();
    let mut optimization_weekdays = strategy_log
        .optimization_weekdays
        .iter()
        .copied()
        .filter(|day| (1..=7).contains(day))
        .collect::<Vec<_>>();
    optimization_weekdays.sort_unstable();
    optimization_weekdays.dedup();

    Some(StrategySummary {
        trades,
        candidate_symbols,
        targets,
        optimization_time_local,
        optimization_weekdays,
    })
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

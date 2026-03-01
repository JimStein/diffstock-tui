use crate::{config, data, futu, inference, paper_trading, portfolio, train};
use anyhow::Result;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
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
    futu: Arc<Mutex<FutuRuntimeState>>,
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
    futu: FutuRuntimeState,
    data_live_source: String,
    data_ws_connected: bool,
    data_ws_diagnostics: data::WsDiagnostics,
    data_live_fetch_diagnostics: data::LiveFetchDiagnostics,
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
    data_live_fetch_diagnostics: data::LiveFetchDiagnostics,
    #[serde(skip_serializing)]
    cmd_tx: Option<mpsc::Sender<paper_trading::PaperCommand>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FutuPositionState {
    symbol: String,
    quantity: f64,
    avg_cost: f64,
    market_price: f64,
    updated_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct FutuRuntimeState {
    running: bool,
    connected: bool,
    started_at: Option<String>,
    strategy_file: Option<String>,
    runtime_file: Option<String>,
    preferred_acc_id: Option<String>,
    conn_host: String,
    conn_port: u16,
    conn_market: String,
    conn_security_firm: String,
    account_cash_usd: f64,
    account_buying_power_usd: f64,
    selected_acc_id: Option<String>,
    selected_trd_env: Option<String>,
    selected_market: Option<String>,
    selected_account: Option<serde_json::Value>,
    latest_snapshot: Option<paper_trading::MinutePortfolioSnapshot>,
    snapshots: Vec<paper_trading::MinutePortfolioSnapshot>,
    positions: Vec<FutuPositionState>,
    open_orders: Vec<serde_json::Value>,
    cancel_history: Vec<serde_json::Value>,
    trade_history: Vec<serde_json::Value>,
    history_orders: Vec<serde_json::Value>,
    history_order_range_days: u32,
    logs: Vec<String>,
    data_live_source: String,
    data_ws_connected: bool,
    data_ws_diagnostics: data::WsDiagnostics,
    data_live_fetch_diagnostics: data::LiveFetchDiagnostics,
    #[serde(skip_serializing)]
    modify_order_request_ms: Vec<i64>,
    #[serde(skip_serializing)]
    last_modify_order_request_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FutuSimStrategyLog {
    created_at: String,
    started_at: String,
    conn_host: String,
    conn_port: u16,
    conn_market: String,
    conn_security_firm: String,
    selected_acc_id: Option<String>,
    selected_trd_env: Option<String>,
    selected_market: Option<String>,
    runtime_file: String,
    #[serde(default)]
    latest_snapshot: Option<paper_trading::MinutePortfolioSnapshot>,
    #[serde(default)]
    latest_positions: Vec<FutuPositionState>,
    #[serde(default)]
    latest_logs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FutuSimRuntimeLine {
    timestamp: String,
    conn_host: String,
    conn_port: u16,
    conn_market: String,
    conn_security_firm: String,
    selected_acc_id: Option<String>,
    selected_trd_env: Option<String>,
    selected_market: Option<String>,
    account_cash_usd: f64,
    account_buying_power_usd: f64,
    total_value_usd: f64,
    pnl_usd: f64,
    pnl_pct: f64,
    positions: Vec<FutuPositionState>,
    snapshot: paper_trading::MinutePortfolioSnapshot,
    #[serde(default)]
    opend_account_list: Option<serde_json::Value>,
    #[serde(default)]
    opend_selected_account: Option<serde_json::Value>,
    #[serde(default)]
    opend_account_info_raw: Option<serde_json::Value>,
    #[serde(default)]
    opend_positions_raw: Option<serde_json::Value>,
}

impl Default for FutuRuntimeState {
    fn default() -> Self {
        Self {
            running: true,
            connected: false,
            started_at: Some(chrono::Local::now().to_rfc3339()),
            strategy_file: None,
            runtime_file: None,
            preferred_acc_id: Some("9468130".to_string()),
            conn_host: "127.0.0.1".to_string(),
            conn_port: 11111,
            conn_market: "US".to_string(),
            conn_security_firm: "FUTUSECURITIES".to_string(),
            account_cash_usd: 0.0,
            account_buying_power_usd: 0.0,
            selected_acc_id: None,
            selected_trd_env: None,
            selected_market: None,
            selected_account: None,
            latest_snapshot: None,
            snapshots: Vec::new(),
            positions: Vec::new(),
            open_orders: Vec::new(),
            cancel_history: Vec::new(),
            trade_history: Vec::new(),
            history_orders: Vec::new(),
            history_order_range_days: 30,
            logs: vec![runtime_log_with_ts(
                "Futu execution initialized (waiting for OpenD / API configuration)",
            )],
            data_live_source: "Unknown".to_string(),
            data_ws_connected: false,
            data_ws_diagnostics: data::WsDiagnostics::default(),
            data_live_fetch_diagnostics: data::LiveFetchDiagnostics::default(),
            modify_order_request_ms: Vec::new(),
            last_modify_order_request_ms: 0,
        }
    }
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
            data_live_fetch_diagnostics: data::LiveFetchDiagnostics::default(),
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

#[derive(Debug, Deserialize)]
struct FutuLoadRequest {
    runtime_file: Option<String>,
    strategy_file: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FutuActivityConfigRequest {
    history_order_range_days: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct FutuManualOrderRequest {
    symbol: String,
    side: String,
    quantity: f64,
    price: Option<f64>,
    order_type: Option<String>,
    time_in_force: Option<String>,
    fill_outside_rth: Option<bool>,
    session: Option<String>,
    remark: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FutuModifyOrderApiRequest {
    modify_order_op: String,
    order_id: String,
    qty: Option<f64>,
    price: Option<f64>,
    adjust_limit: Option<f64>,
    trd_env: Option<String>,
    acc_id: Option<String>,
    aux_price: Option<f64>,
    trail_type: Option<String>,
    trail_value: Option<f64>,
    trail_spread: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FutuAccountApplyRequest {
    acc_id: String,
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
        futu: Arc::new(Mutex::new(FutuRuntimeState::default())),
        forecast: Arc::new(Mutex::new(forecast_state)),
        portfolio: Arc::new(Mutex::new(PortfolioRuntimeState::default())),
    };

    if std::env::var("FUTU_API_ACC_ID")
        .ok()
        .map(|v| v.trim().is_empty())
        .unwrap_or(true)
    {
        unsafe {
            std::env::set_var("FUTU_API_ACC_ID", "9468130");
        }
    }

    tokio::spawn(futu_execution_loop(state.futu.clone(), state.paper.clone()));

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
        .route("/api/futu/status", get(futu_status))
        .route("/api/futu/activity-config", post(futu_activity_config))
        .route("/api/futu/manual-order", post(futu_manual_order))
        .route("/api/futu/modify-order", post(futu_modify_order))
        .route("/api/futu/account-list", get(futu_account_list))
        .route("/api/futu/account-apply", post(futu_account_apply))
        .route("/api/futu/start", post(futu_start))
        .route("/api/futu/load", post(futu_load))
        .route("/api/futu/stop", post(futu_stop))
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

    let quotes = data::fetch_latest_prices_with_meta_ws_only(&symbols)
        .await
        .map_err(internal_err)?;

    let mut prices = HashMap::new();
    let mut exchange_ts_ms = HashMap::new();
    let mut sources = HashMap::new();
    for (symbol, quote) in quotes {
        prices.insert(symbol.clone(), quote.price);
        exchange_ts_ms.insert(symbol.clone(), quote.exchange_ts_ms);
        sources.insert(symbol, quote.source);
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
    let mut futu = state.futu.lock().await.clone();
    paper.cmd_tx = None;
    let data_live_source = data::current_live_data_source().await;
    let data_ws_connected = data::polygon_ws_connected().await;
    let data_ws_diagnostics = data::current_ws_diagnostics().await;
    let data_live_fetch_diagnostics = data::current_live_fetch_diagnostics().await;
    paper.data_live_source = data_live_source.clone();
    paper.data_ws_connected = data_ws_connected;
    paper.data_ws_diagnostics = data_ws_diagnostics.clone();
    paper.data_live_fetch_diagnostics = data_live_fetch_diagnostics.clone();
    futu.data_live_source = data_live_source.clone();
    futu.data_ws_connected = data_ws_connected;
    futu.data_ws_diagnostics = data_ws_diagnostics.clone();
    futu.data_live_fetch_diagnostics = data_live_fetch_diagnostics.clone();

    Ok(Json(FullUiState {
        forecast: state.forecast.lock().await.clone(),
        portfolio: state.portfolio.lock().await.clone(),
        train: state.train.lock().await.clone(),
        paper,
        futu,
        data_live_source,
        data_ws_connected,
        data_ws_diagnostics,
        data_live_fetch_diagnostics,
    }))
}

async fn futu_status(
    State(state): State<WebState>,
) -> Result<Json<FutuRuntimeState>, (StatusCode, Json<ApiError>)> {
    let mut status = state.futu.lock().await.clone();
    status.data_live_source = data::current_live_data_source().await;
    status.data_ws_connected = data::polygon_ws_connected().await;
    status.data_ws_diagnostics = data::current_ws_diagnostics().await;
    status.data_live_fetch_diagnostics = data::current_live_fetch_diagnostics().await;
    Ok(Json(status))
}

async fn futu_activity_config(
    State(state): State<WebState>,
    Json(req): Json<FutuActivityConfigRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let range_days = req.history_order_range_days.unwrap_or(30).clamp(0, 3650);

    let mut fs = state.futu.lock().await;
    fs.history_order_range_days = range_days;
    fs.logs.push(runtime_log_with_ts(format!(
        "Futu activity config updated => history_order_range_days={}d",
        range_days
    )));
    if fs.logs.len() > 200 {
        let keep_from = fs.logs.len().saturating_sub(200);
        fs.logs = fs.logs.split_off(keep_from);
    }

    Ok(Json(serde_json::json!({
        "ok": true,
        "history_order_range_days": range_days,
    })))
}

async fn futu_manual_order(
    State(state): State<WebState>,
    Json(req): Json<FutuManualOrderRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let symbol_input = req.symbol.trim().to_uppercase();
    if symbol_input.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "symbol is required"));
    }

    if !req.quantity.is_finite() || req.quantity <= 0.0 {
        return Err(api_err(StatusCode::BAD_REQUEST, "quantity must be > 0"));
    }

    let side = req.side.trim().to_uppercase();
    if side != "BUY" && side != "SELL" {
        return Err(api_err(StatusCode::BAD_REQUEST, "side must be BUY or SELL"));
    }

    let order_type = req
        .order_type
        .clone()
        .unwrap_or_else(|| "NORMAL".to_string())
        .trim()
        .to_uppercase();

    let market;
    let acc_id;
    let trd_env;
    {
        let fs = state.futu.lock().await;
        market = fs
            .selected_market
            .clone()
            .unwrap_or_else(|| fs.conn_market.clone())
            .to_uppercase();
        acc_id = fs.selected_acc_id.clone();
        trd_env = fs
            .selected_trd_env
            .clone()
            .unwrap_or_else(|| "SIMULATE".to_string())
            .to_uppercase();
    }

    if trd_env != "SIMULATE" {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            "manual order is allowed only in SIMULATE mode",
        ));
    }

    if order_type == "NORMAL" {
        let price_ok = req.price.map(|p| p.is_finite() && p > 0.0).unwrap_or(false);
        if !price_ok {
            return Err(api_err(
                StatusCode::BAD_REQUEST,
                "LIMIT (NORMAL) order requires price > 0",
            ));
        }
    }

    let symbol = normalize_futu_symbol_for_market(&symbol_input, &market);

    let mut client = futu::FutuApiClient::from_env().map_err(internal_err)?;
    client.set_account_id_override(acc_id.clone());

    let request = futu::FutuPlaceOrderRequest {
        symbol: symbol.clone(),
        side: side.clone(),
        quantity: req.quantity,
        price: req.price,
        order_type: Some(order_type.clone()),
        market: Some(market.clone()),
        trd_env: Some("SIMULATE".to_string()),
        acc_id: acc_id.clone(),
        adjust_limit: Some(0.0),
        remark: req.remark.clone(),
        time_in_force: req.time_in_force.clone(),
        fill_outside_rth: req.fill_outside_rth,
        session: req.session.clone(),
        aux_price: None,
        trail_type: None,
        trail_value: None,
        trail_spread: None,
    };

    let response = client.place_order(&request).await.map_err(internal_err)?;
    let order_id = response
        .get("order_id")
        .and_then(|v| v.as_str())
        .unwrap_or("--")
        .to_string();

    {
        let mut fs = state.futu.lock().await;
        fs.logs.push(runtime_log_with_ts(format!(
            "Futu SIM manual order {} {} x {:.2} @ {} (type={} tif={} session={} order_id={})",
            side,
            symbol,
            req.quantity,
            req.price
                .map(|p| format!("{:.4}", p))
                .unwrap_or_else(|| "MKT".to_string()),
            order_type,
            req.time_in_force.clone().unwrap_or_else(|| "DAY".to_string()),
            req.session.clone().unwrap_or_else(|| "NONE".to_string()),
            order_id,
        )));
        if fs.logs.len() > 200 {
            let keep_from = fs.logs.len().saturating_sub(200);
            fs.logs = fs.logs.split_off(keep_from);
        }
    }

    Ok(Json(serde_json::json!({
        "ok": true,
        "order_id": order_id,
        "symbol": symbol,
        "side": side,
        "quantity": req.quantity,
        "price": req.price,
        "order_type": order_type,
        "time_in_force": req.time_in_force,
        "session": req.session,
        "response": response,
    })))
}

async fn futu_modify_order(
    State(state): State<WebState>,
    Json(req): Json<FutuModifyOrderApiRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    const MODIFY_ORDER_WINDOW_MS: i64 = 30_000;
    const MODIFY_ORDER_MAX_REQ: usize = 20;
    const MODIFY_ORDER_MIN_INTERVAL_MS: i64 = 40;

    let order_id = req.order_id.trim().to_string();
    if order_id.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "order_id is required"));
    }

    let op_raw = req.modify_order_op.trim().to_uppercase();
    let modify_order_op = if op_raw.contains("CANCEL") || op_raw.contains("DELETE") {
        "CANCEL".to_string()
    } else if op_raw.contains("NORMAL") {
        "NORMAL".to_string()
    } else {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            "modify_order_op must be NORMAL or CANCEL",
        ));
    };

    let qty = req.qty.unwrap_or(0.0);
    let price = req.price.unwrap_or(0.0);
    if modify_order_op == "NORMAL" {
        if !qty.is_finite() || qty <= 0.0 {
            return Err(api_err(StatusCode::BAD_REQUEST, "qty must be > 0 for NORMAL"));
        }
        if !price.is_finite() || price <= 0.0 {
            return Err(api_err(StatusCode::BAD_REQUEST, "price must be > 0 for NORMAL"));
        }
    }

    let mut acc_id;
    let mut trd_env;
    let mut order_symbol = "--".to_string();
    let mut order_side = "--".to_string();
    let mut order_status = "--".to_string();
    {
        let fs = state.futu.lock().await;
        acc_id = fs.selected_acc_id.clone();
        trd_env = fs
            .selected_trd_env
            .clone()
            .unwrap_or_else(|| "SIMULATE".to_string())
            .to_uppercase();

        if let Some(row) = fs.open_orders.iter().find(|row| {
            futu_json_get_string(row, &["order_id", "id"]).unwrap_or_default() == order_id
        }) {
            order_symbol = futu_json_get_string(row, &["code", "symbol", "ticker"])
                .unwrap_or_else(|| "--".to_string());
            order_side =
                futu_json_get_string(row, &["trd_side", "side"]).unwrap_or_else(|| "--".to_string());
            order_status = futu_json_get_string(row, &["order_status", "status"])
                .unwrap_or_else(|| "--".to_string());
        }
    }

    if let Some(req_env) = req.trd_env.as_ref().map(|v| v.trim().to_uppercase()) {
        if !req_env.is_empty() {
            trd_env = req_env;
        }
    }
    if let Some(req_acc) = req.acc_id.as_ref().map(|v| v.trim().to_string()) {
        if !req_acc.is_empty() {
            acc_id = Some(req_acc);
        }
    }

    if trd_env != "SIMULATE" {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            "modify/cancel is allowed only in SIMULATE mode",
        ));
    }

    let mut wait_ms = 0_i64;
    {
        let mut fs = state.futu.lock().await;
        let now_ms = chrono::Utc::now().timestamp_millis();
        fs.modify_order_request_ms
            .retain(|ts| now_ms.saturating_sub(*ts) <= MODIFY_ORDER_WINDOW_MS);
        if fs.modify_order_request_ms.len() >= MODIFY_ORDER_MAX_REQ {
            return Err(api_err(
                StatusCode::TOO_MANY_REQUESTS,
                "modify/cancel rate limit exceeded: max 20 requests per 30 seconds",
            ));
        }
        if fs.last_modify_order_request_ms > 0 {
            let elapsed = now_ms.saturating_sub(fs.last_modify_order_request_ms);
            if elapsed < MODIFY_ORDER_MIN_INTERVAL_MS {
                wait_ms = MODIFY_ORDER_MIN_INTERVAL_MS - elapsed;
            }
        }
        if wait_ms == 0 {
            fs.last_modify_order_request_ms = now_ms;
            fs.modify_order_request_ms.push(now_ms);
        }
    }

    if wait_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(wait_ms as u64)).await;
        let mut fs = state.futu.lock().await;
        let now_ms = chrono::Utc::now().timestamp_millis();
        fs.modify_order_request_ms
            .retain(|ts| now_ms.saturating_sub(*ts) <= MODIFY_ORDER_WINDOW_MS);
        if fs.modify_order_request_ms.len() >= MODIFY_ORDER_MAX_REQ {
            return Err(api_err(
                StatusCode::TOO_MANY_REQUESTS,
                "modify/cancel rate limit exceeded: max 20 requests per 30 seconds",
            ));
        }
        fs.last_modify_order_request_ms = now_ms;
        fs.modify_order_request_ms.push(now_ms);
    }

    let mut client = futu::FutuApiClient::from_env().map_err(internal_err)?;
    client.set_account_id_override(acc_id.clone());

    let request = futu::FutuModifyOrderRequest {
        order_id: order_id.clone(),
        action: modify_order_op.clone(),
        quantity: Some(if modify_order_op == "CANCEL" { 0.0 } else { qty }),
        price: Some(if modify_order_op == "CANCEL" { 0.0 } else { price }),
        adjust_limit: req.adjust_limit.or(Some(0.0)),
        trd_env: Some(trd_env.clone()),
        acc_id: acc_id.clone(),
        aux_price: req.aux_price,
        trail_type: req.trail_type.clone(),
        trail_value: req.trail_value,
        trail_spread: req.trail_spread,
    };

    let response = client.modify_or_cancel_order(&request).await.map_err(internal_err)?;

    {
        let mut fs = state.futu.lock().await;
        fs.logs.push(runtime_log_with_ts(format!(
            "Futu SIM manual {} order_id={} symbol={} side={} qty={:.4} price={:.4}",
            modify_order_op,
            order_id,
            order_symbol,
            order_side,
            qty,
            price,
        )));

        if modify_order_op == "CANCEL" {
            fs.cancel_history.push(serde_json::json!({
                "timestamp": chrono::Local::now().to_rfc3339(),
                "order_id": order_id,
                "symbol": order_symbol,
                "trd_side": order_side,
                "qty": qty,
                "price": price,
                "order_status": order_status,
                "reason": "Manual cancel from Open Orders",
                "signal_id": "manual",
            }));
            if fs.cancel_history.len() > 500 {
                let keep_from = fs.cancel_history.len().saturating_sub(500);
                fs.cancel_history = fs.cancel_history.split_off(keep_from);
            }
        }

        if fs.logs.len() > 200 {
            let keep_from = fs.logs.len().saturating_sub(200);
            fs.logs = fs.logs.split_off(keep_from);
        }
    }

    Ok(Json(serde_json::json!({
        "ok": true,
        "modify_order_op": modify_order_op,
        "order_id": request.order_id,
        "qty": request.quantity,
        "price": request.price,
        "trd_env": request.trd_env,
        "acc_id": request.acc_id,
        "response": response,
    })))
}

async fn futu_account_list() -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let client = futu::FutuApiClient::from_env().map_err(internal_err)?;
    let mut payload = client.get_account_list().await.map_err(internal_err)?;
    stringify_acc_id_fields(&mut payload);
    Ok(Json(payload))
}

async fn futu_account_apply(
    State(state): State<WebState>,
    Json(req): Json<FutuAccountApplyRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let acc_id = req.acc_id.trim().to_string();
    if acc_id.is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "acc_id cannot be empty"));
    }

    {
        let mut fs = state.futu.lock().await;
        fs.preferred_acc_id = Some(acc_id.clone());
        fs.logs.push(runtime_log_with_ts(format!(
            "Futu preferred account applied => {}",
            acc_id
        )));
        if fs.logs.len() > 200 {
            let keep_from = fs.logs.len().saturating_sub(200);
            fs.logs = fs.logs.split_off(keep_from);
        }
    }

    unsafe {
        std::env::set_var("FUTU_API_ACC_ID", &acc_id);
    }

    let mut refreshed = false;
    if let Ok(mut client) = futu::FutuApiClient::from_env() {
        client.set_account_id_override(Some(acc_id.clone()));
        match client.poll_execution_snapshot().await {
            Ok(payload) => {
                let mut fs = state.futu.lock().await;
                fs.connected = true;
                fs.account_cash_usd = payload.cash_usd;
                fs.account_buying_power_usd = payload.buying_power_usd;
                fs.selected_acc_id = payload.selected_acc_id.clone();
                fs.selected_trd_env = payload.selected_trd_env.clone();
                fs.selected_market = payload.selected_market.clone();
                fs.selected_account = payload.opend_selected_account.clone();
                fs.positions = payload
                    .positions
                    .iter()
                    .map(|p| FutuPositionState {
                        symbol: p.symbol.clone(),
                        quantity: p.quantity,
                        avg_cost: p.avg_cost,
                        market_price: p.market_price,
                        updated_at: p.updated_at.clone(),
                    })
                    .collect();
                fs.logs.push(runtime_log_with_ts(format!(
                    "Futu account switched immediately => preferred={} active={}",
                    acc_id,
                    payload.selected_acc_id.as_deref().unwrap_or("--")
                )));
                if fs.logs.len() > 200 {
                    let keep_from = fs.logs.len().saturating_sub(200);
                    fs.logs = fs.logs.split_off(keep_from);
                }
                refreshed = true;
            }
            Err(err) => {
                warn!("futu account apply immediate refresh failed: {}", err);
            }
        }
    }

    Ok(Json(serde_json::json!({ "ok": true, "acc_id": acc_id, "refreshed": refreshed })))
}

fn stringify_acc_id_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(acc_id) = map.get_mut("acc_id") {
                let as_text = match acc_id {
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    serde_json::Value::String(s) => Some(s.clone()),
                    _ => None,
                };
                if let Some(text) = as_text {
                    *acc_id = serde_json::Value::String(text);
                }
            }
            for v in map.values_mut() {
                stringify_acc_id_fields(v);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                stringify_acc_id_fields(item);
            }
        }
        _ => {}
    }
}

async fn futu_start(
    State(state): State<WebState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let mut fs = state.futu.lock().await;
    fs.running = true;
    if fs.started_at.is_none() {
        fs.started_at = Some(chrono::Local::now().to_rfc3339());
    }
    let preferred = fs
        .preferred_acc_id
        .clone()
        .unwrap_or_else(|| "9468130".to_string());
    fs.logs.push(runtime_log_with_ts(format!(
        "Futu simulation started (preferred acc_id={})",
        preferred
    )));
    if fs.logs.len() > 200 {
        let keep_from = fs.logs.len().saturating_sub(200);
        fs.logs = fs.logs.split_off(keep_from);
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn futu_load(
    State(state): State<WebState>,
    Json(req): Json<FutuLoadRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let strategy_input = req
        .strategy_file
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    let runtime_input = req
        .runtime_file
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();

    let mut resolved_strategy_path: Option<PathBuf> = None;
    let mut resolved_runtime_path: Option<PathBuf> = None;

    if !strategy_input.is_empty() {
        let strategy_path = resolve_input_path(&strategy_input);
        let strategy_log = load_futu_strategy_log(&strategy_path)
            .map_err(|e| api_err(StatusCode::BAD_REQUEST, &format!("invalid strategy_file: {}", e)))?;
        resolved_runtime_path = Some(resolve_input_path(&strategy_log.runtime_file));
        resolved_strategy_path = Some(strategy_path);
    }

    if resolved_runtime_path.is_none() {
        if !runtime_input.is_empty() {
            resolved_runtime_path = Some(resolve_input_path(&runtime_input));
        } else {
            resolved_runtime_path = find_latest_log_file("futu_sim_runtime_", ".jsonl");
        }
    }

    if resolved_strategy_path.is_none() {
        if !strategy_input.is_empty() {
            resolved_strategy_path = Some(resolve_input_path(&strategy_input));
        } else {
            resolved_strategy_path = find_latest_log_file("futu_sim_strategy_", ".json");
        }
    }

    let runtime_path = resolved_runtime_path
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "runtime_file is required or no futu_sim_runtime_*.jsonl found"))?;

    let runtime_lines = load_futu_runtime_lines(&runtime_path).map_err(internal_err)?;
    if runtime_lines.is_empty() {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            "runtime file has no valid snapshots",
        ));
    }

    let snapshots = runtime_lines
        .iter()
        .map(|line| line.snapshot.clone())
        .collect::<Vec<_>>();
    let last = runtime_lines
        .last()
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "runtime file has no valid snapshots"))?;

    let mut fs = state.futu.lock().await;
    fs.running = true;
    fs.connected = false;
    fs.started_at = Some(chrono::Local::now().to_rfc3339());
    fs.strategy_file = resolved_strategy_path
        .as_ref()
        .map(|p| p.display().to_string());
    fs.runtime_file = Some(runtime_path.display().to_string());
    fs.conn_host = last.conn_host.clone();
    fs.conn_port = last.conn_port;
    fs.conn_market = last.conn_market.clone();
    fs.conn_security_firm = last.conn_security_firm.clone();
    fs.account_cash_usd = last.account_cash_usd;
    fs.account_buying_power_usd = last.account_buying_power_usd;
    fs.selected_acc_id = last.selected_acc_id.clone();
    fs.selected_trd_env = last.selected_trd_env.clone();
    fs.selected_market = last.selected_market.clone();
    fs.selected_account = last.opend_selected_account.clone();
    fs.positions = last.positions.clone();
    fs.latest_snapshot = Some(last.snapshot.clone());
    fs.snapshots = snapshots;
    let loaded_snapshot_count = fs.snapshots.len();
    if fs.preferred_acc_id.is_none() {
        fs.preferred_acc_id = Some("9468130".to_string());
    }
    fs.logs.push(runtime_log_with_ts(format!(
        "Futu history loaded => {} snapshots from {}",
        loaded_snapshot_count,
        runtime_path.display()
    )));
    if fs.logs.len() > 200 {
        let keep_from = fs.logs.len().saturating_sub(200);
        fs.logs = fs.logs.split_off(keep_from);
    }

    Ok(Json(serde_json::json!({
        "ok": true,
        "runtime_file": runtime_path.display().to_string(),
        "snapshots": runtime_lines.len(),
    })))
}

async fn futu_stop(
    State(state): State<WebState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let mut fs = state.futu.lock().await;
    fs.running = false;
    fs.connected = false;
    fs.logs.push(runtime_log_with_ts("Futu simulation stopped"));
    if fs.logs.len() > 200 {
        let keep_from = fs.logs.len().saturating_sub(200);
        fs.logs = fs.logs.split_off(keep_from);
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

fn normalize_futu_symbol_for_market(symbol: &str, market: &str) -> String {
    let trimmed = symbol.trim().to_uppercase();
    if trimmed.is_empty() {
        return trimmed;
    }
    if trimmed.contains('.') {
        return trimmed;
    }
    format!("{}.{}", market.trim().to_uppercase(), trimmed)
}

fn futu_history_date_range(days: u32) -> (Option<String>, Option<String>) {
    if days == 0 {
        return (None, None);
    }

    let end = chrono::Local::now().date_naive();
    let start = end - chrono::Duration::days(days as i64);
    (
        Some(start.format("%Y-%m-%d").to_string()),
        Some(end.format("%Y-%m-%d").to_string()),
    )
}

fn futu_json_get_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(raw) = value.get(*key) {
            if let Some(text) = raw.as_str() {
                let normalized = text.trim();
                if !normalized.is_empty() {
                    return Some(normalized.to_string());
                }
            } else if let Some(v) = raw.as_i64() {
                return Some(v.to_string());
            } else if let Some(v) = raw.as_u64() {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn futu_json_get_bool(value: &serde_json::Value, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(raw) = value.get(*key) {
            if let Some(v) = raw.as_bool() {
                return Some(v);
            }
            if let Some(v) = raw.as_i64() {
                return Some(v != 0);
            }
            if let Some(v) = raw.as_u64() {
                return Some(v != 0);
            }
            if let Some(text) = raw.as_str() {
                let normalized = text.trim().to_ascii_lowercase();
                if normalized == "true" || normalized == "1" || normalized == "yes" || normalized == "y" {
                    return Some(true);
                }
                if normalized == "false" || normalized == "0" || normalized == "no" || normalized == "n" {
                    return Some(false);
                }
            }
        }
    }
    None
}

fn futu_extract_order_rows(value: &serde_json::Value) -> Vec<serde_json::Value> {
    if let Some(rows) = value.as_array() {
        return rows.clone();
    }

    for key in ["data", "orders", "order_list", "items", "rows"] {
        if let Some(rows) = value.get(key).and_then(|v| v.as_array()) {
            return rows.clone();
        }
    }

    if let Some(data) = value.get("data") {
        for key in ["orders", "order_list", "items", "rows"] {
            if let Some(rows) = data.get(key).and_then(|v| v.as_array()) {
                return rows.clone();
            }
        }
    }

    Vec::new()
}

fn futu_order_is_open(row: &serde_json::Value) -> bool {
    if futu_json_get_bool(row, &["can_cancel", "is_can_cancel", "can_cancelled"]).unwrap_or(false)
    {
        return true;
    }

    let status = futu_json_get_string(row, &["order_status", "status", "orderStatus"])
        .unwrap_or_default()
        .to_uppercase();

    if status.is_empty() {
        return true;
    }

    let is_terminal = [
        "FILLED_ALL",
        "FILLED",
        "CANCELLED_ALL",
        "CANCELLED_PART",
        "CANCELLED",
        "FAILED",
        "DELETED",
        "DISABLED",
        "WITHDRAWN",
        "REJECTED",
        "EXPIRED",
    ]
    .iter()
    .any(|terminal| status.contains(terminal));

    !is_terminal
}

async fn futu_execution_loop(
    futu_state: Arc<Mutex<FutuRuntimeState>>,
    paper_state: Arc<Mutex<PaperRuntimeState>>,
) {
    let mut prev_prices: HashMap<String, f64> = HashMap::new();
    let mut prev_qty: HashMap<String, f64> = HashMap::new();
    let mut initial_capital: Option<f64> = None;
    let mut last_account_binding: Option<String> = None;
    let mut last_opend_dump: Option<String> = None;
    let mut last_rebalance_signal: Option<String> = None;

    loop {
        let is_running = {
            let fs = futu_state.lock().await;
            fs.running
        };

        if !is_running {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }

        let (preferred_acc_id, history_order_range_days) = {
            let fs = futu_state.lock().await;
            (fs.preferred_acc_id.clone(), fs.history_order_range_days)
        };

        let (payload, conn_info, config_err) = match futu::FutuApiClient::from_env() {
            Ok(mut client) => {
                client.set_account_id_override(preferred_acc_id.clone());
                let info = client.connection_info();
                (client.poll_execution_snapshot().await, Some(info), None)
            }
            Err(err) => (Err(anyhow::anyhow!(err.to_string())), None, Some(err.to_string())),
        };

        if let Some((host, port, market, firm)) = conn_info {
            let mut fs = futu_state.lock().await;
            fs.conn_host = host;
            fs.conn_port = port;
            fs.conn_market = market;
            fs.conn_security_firm = firm;
        } else if let Some(err_text) = config_err {
            let mut fs = futu_state.lock().await;
            if fs.logs.last().map(|x| x.contains("Futu config error")).unwrap_or(false) {
                // keep log volume stable
            } else {
                fs.logs
                    .push(runtime_log_with_ts(format!("WARNING: Futu config error: {}", err_text)));
            }
        }

        match payload {
            Ok(payload) => {
                let symbols = payload
                    .positions
                    .iter()
                    .filter(|p| p.quantity.abs() > 0.0)
                    .map(|p| p.symbol.clone())
                    .collect::<Vec<_>>();

                let quote_map = if symbols.is_empty() {
                    HashMap::new()
                } else {
                    match data::fetch_latest_prices_with_meta_ws_only(&symbols).await {
                        Ok(quotes) => quotes,
                        Err(err) => {
                            let mut fs = futu_state.lock().await;
                            fs.logs.push(runtime_log_with_ts(format!(
                                "WARNING: Futu execution quote sync failed, fallback to broker last price: {}",
                                err
                            )));
                            if fs.logs.len() > 200 {
                                let keep_from = fs.logs.len().saturating_sub(200);
                                fs.logs = fs.logs.split_off(keep_from);
                            }
                            HashMap::new()
                        }
                    }
                };

                let (rebalance_signal, target_weights) = {
                    let ps = paper_state.lock().await;
                    let signal = ps.last_analysis.as_ref().map(|a| a.timestamp.clone());
                    let targets = ps
                        .target_weights
                        .iter()
                        .map(|t| (t.symbol.clone(), t.weight))
                        .collect::<Vec<_>>();
                    (signal, targets)
                };

                let mut rebalance_logs: Vec<String> = Vec::new();
                let mut cancel_events_this_cycle: Vec<serde_json::Value> = Vec::new();

                if let Some(signal_id) = rebalance_signal {
                    if last_rebalance_signal.as_deref() != Some(signal_id.as_str()) {
                        let mut consume_rebalance_signal = true;
                        let selected_env = payload
                            .selected_trd_env
                            .as_deref()
                            .unwrap_or("SIMULATE")
                            .to_uppercase();

                        if selected_env == "SIMULATE" && !target_weights.is_empty() {
                            let selected_market = payload
                                .selected_market
                                .clone()
                                .unwrap_or_else(|| "US".to_string())
                                .to_uppercase();

                            let mut live_price_by_symbol: HashMap<String, f64> = HashMap::new();
                            let mut current_qty_by_symbol: HashMap<String, f64> = HashMap::new();

                            for position in &payload.positions {
                                let fallback_prev_price = prev_prices
                                    .get(&position.symbol)
                                    .copied()
                                    .filter(|v| v.is_finite() && *v > 0.0);
                                let live_price = quote_map
                                    .get(&position.symbol)
                                    .map(|q| q.price)
                                    .filter(|v| v.is_finite() && *v > 0.0)
                                    .or_else(|| {
                                        if position.market_price.is_finite() && position.market_price > 0.0 {
                                            Some(position.market_price)
                                        } else {
                                            None
                                        }
                                    })
                                    .or(fallback_prev_price)
                                    .unwrap_or(position.avg_cost.max(0.0));

                                if live_price.is_finite() && live_price > 0.0 {
                                    live_price_by_symbol.insert(position.symbol.clone(), live_price);
                                }
                                if position.quantity.abs() > 1e-9 {
                                    current_qty_by_symbol.insert(position.symbol.clone(), position.quantity);
                                }
                            }

                            let mut candidate_symbols = target_weights
                                .iter()
                                .filter_map(|(symbol, _)| {
                                    let normalized =
                                        normalize_futu_symbol_for_market(symbol, &selected_market);
                                    if normalized.is_empty() {
                                        None
                                    } else {
                                        Some(normalized)
                                    }
                                })
                                .collect::<Vec<_>>();
                            candidate_symbols.sort();
                            candidate_symbols.dedup();

                            if candidate_symbols.is_empty() {
                                rebalance_logs.push(runtime_log_with_ts(format!(
                                    "Futu SIM rebalance signal {} ignored: empty candidate pool",
                                    signal_id
                                )));
                            } else {
                                let mut target_weight_map = HashMap::<String, f64>::new();
                                for (symbol, weight) in &target_weights {
                                    if !weight.is_finite() {
                                        continue;
                                    }
                                    let normalized = normalize_futu_symbol_for_market(symbol, &selected_market);
                                    if normalized.is_empty() {
                                        continue;
                                    }
                                    let entry = target_weight_map.entry(normalized).or_insert(0.0);
                                    *entry += weight.max(0.0);
                                }

                                let total_positive_weight = target_weight_map
                                    .values()
                                    .copied()
                                    .filter(|w| w.is_finite() && *w > 0.0)
                                    .sum::<f64>();

                                let normalized_targets = if total_positive_weight > 0.0 {
                                    target_weight_map
                                        .into_iter()
                                        .filter_map(|(symbol, weight)| {
                                            if weight > 0.0 {
                                                Some((symbol, weight / total_positive_weight))
                                            } else {
                                                None
                                            }
                                        })
                                        .collect::<HashMap<_, _>>()
                                } else {
                                    HashMap::new()
                                };

                                let needed_symbols = candidate_symbols.clone();

                                let missing_price_symbols = needed_symbols
                                    .iter()
                                    .filter(|symbol| !live_price_by_symbol.contains_key(*symbol))
                                    .cloned()
                                    .collect::<Vec<_>>();

                                if !missing_price_symbols.is_empty() {
                                    if let Ok(extra_quotes) =
                                        data::fetch_latest_prices_with_meta_ws_only(&missing_price_symbols).await
                                    {
                                        for (symbol, quote) in extra_quotes {
                                            if quote.price.is_finite() && quote.price > 0.0 {
                                                live_price_by_symbol.insert(symbol, quote.price);
                                            }
                                        }
                                    }
                                }

                                let mut total_holdings_value = 0.0;
                                for symbol in &candidate_symbols {
                                    let qty = current_qty_by_symbol.get(symbol).copied().unwrap_or(0.0);
                                    if qty.abs() < 1e-9 {
                                        continue;
                                    }
                                    if let Some(price) = live_price_by_symbol.get(symbol) {
                                        total_holdings_value += qty * price;
                                    }
                                }
                                let total_portfolio_value = payload.cash_usd + total_holdings_value;

                                if total_portfolio_value <= 0.0 {
                                    rebalance_logs.push(runtime_log_with_ts(format!(
                                        "Futu SIM rebalance signal {} ignored: non-positive portfolio value",
                                        signal_id
                                    )));
                                } else {
                                    let mut sell_orders: Vec<(String, f64, f64)> = Vec::new();
                                    let mut buy_orders: Vec<(String, f64, f64)> = Vec::new();

                                    for symbol in &candidate_symbols {
                                        let Some(price) = live_price_by_symbol.get(symbol).copied() else {
                                            rebalance_logs.push(runtime_log_with_ts(format!(
                                                "WARNING: Futu SIM rebalance skipped {} due to missing live price",
                                                symbol
                                            )));
                                            continue;
                                        };

                                        let current_qty = current_qty_by_symbol.get(symbol).copied().unwrap_or(0.0);
                                        let target_weight = normalized_targets.get(symbol).copied().unwrap_or(0.0);
                                        let target_value = total_portfolio_value * target_weight;
                                        let target_qty = (target_value / price).floor().max(0.0);
                                        let delta_qty = target_qty - current_qty;

                                        if delta_qty.abs() < 1.0 {
                                            continue;
                                        }

                                        if delta_qty > 0.0 {
                                            buy_orders.push((symbol.clone(), delta_qty.floor(), price));
                                        } else {
                                            sell_orders.push((symbol.clone(), (-delta_qty).floor(), price));
                                        }
                                    }

                                    if sell_orders.is_empty() && buy_orders.is_empty() {
                                        rebalance_logs.push(runtime_log_with_ts(format!(
                                            "Futu SIM rebalance signal {} -> no executable deltas",
                                            signal_id
                                        )));
                                    } else {
                                        match futu::FutuApiClient::from_env() {
                                            Ok(mut trade_client) => {
                                                trade_client.set_account_id_override(payload.selected_acc_id.clone());

                                                let mut available_cash = payload.cash_usd.max(0.0);
                                                let env_value = payload.selected_trd_env.clone();
                                                let acc_value = payload.selected_acc_id.clone();
                                                let market_value = payload.selected_market.clone();

                                                let mut cancel_stage_ok = true;
                                                match trade_client.get_order_list().await {
                                                    Ok(order_list_raw) => {
                                                        let mut pending_open_orders = Vec::<serde_json::Value>::new();
                                                        let rows = futu_extract_order_rows(&order_list_raw);
                                                        for row in rows {
                                                            let order_env = futu_json_get_string(
                                                                &row,
                                                                &["trd_env", "trade_env", "env"],
                                                            )
                                                            .unwrap_or_default()
                                                            .to_uppercase();
                                                            if let Some(env) = env_value.as_ref() {
                                                                if !order_env.is_empty()
                                                                    && order_env != env.trim().to_uppercase()
                                                                {
                                                                    continue;
                                                                }
                                                            }

                                                            let row_acc_id = futu_json_get_string(
                                                                &row,
                                                                &["acc_id", "account_id", "account"],
                                                            )
                                                            .unwrap_or_default();
                                                            if let Some(acc_id) = acc_value.as_ref() {
                                                                if !row_acc_id.is_empty()
                                                                    && row_acc_id.trim() != acc_id.trim()
                                                                {
                                                                    continue;
                                                                }
                                                            }

                                                            if !futu_order_is_open(&row) {
                                                                continue;
                                                            }

                                                            if let Some(order_id) = futu_json_get_string(
                                                                &row,
                                                                &["order_id", "id"],
                                                            ) {
                                                                pending_open_orders.push(serde_json::json!({
                                                                    "order_id": order_id,
                                                                    "symbol": futu_json_get_string(&row, &["code", "symbol", "ticker"]),
                                                                    "trd_side": futu_json_get_string(&row, &["trd_side", "side"]),
                                                                    "qty": row.get("qty").cloned().or_else(|| row.get("quantity").cloned()),
                                                                    "price": row.get("price").cloned(),
                                                                    "order_status": futu_json_get_string(&row, &["order_status", "status"]),
                                                                    "order_type": futu_json_get_string(&row, &["order_type"]),
                                                                }));
                                                            }
                                                        }

                                                        pending_open_orders.sort_by(|a, b| {
                                                            let a_id = futu_json_get_string(a, &["order_id"]).unwrap_or_default();
                                                            let b_id = futu_json_get_string(b, &["order_id"]).unwrap_or_default();
                                                            a_id.cmp(&b_id)
                                                        });
                                                        pending_open_orders.dedup_by(|a, b| {
                                                            futu_json_get_string(a, &["order_id"]).unwrap_or_default()
                                                                == futu_json_get_string(b, &["order_id"]).unwrap_or_default()
                                                        });

                                                        if pending_open_orders.len() > 20 {
                                                            consume_rebalance_signal = false;
                                                            rebalance_logs.push(runtime_log_with_ts(format!(
                                                                "WARNING: Futu SIM found {} open orders; canceling first 20 due to API limit, then will retry remaining orders",
                                                                pending_open_orders.len()
                                                            )));
                                                        }

                                                        let cancel_targets = pending_open_orders
                                                            .into_iter()
                                                            .take(20)
                                                            .collect::<Vec<_>>();

                                                        if !cancel_targets.is_empty() {
                                                            rebalance_logs.push(runtime_log_with_ts(format!(
                                                                "Futu SIM rebalance found {} open orders; canceling before new orders",
                                                                cancel_targets.len()
                                                            )));

                                                            for cancel_item in cancel_targets {
                                                                let order_id = futu_json_get_string(&cancel_item, &["order_id"])
                                                                    .unwrap_or_default();
                                                                if order_id.is_empty() {
                                                                    continue;
                                                                }
                                                                let cancel_req = futu::FutuModifyOrderRequest {
                                                                    order_id: order_id.clone(),
                                                                    action: "CANCEL".to_string(),
                                                                    quantity: None,
                                                                    price: None,
                                                                    adjust_limit: None,
                                                                    trd_env: env_value.clone(),
                                                                    acc_id: acc_value.clone(),
                                                                    aux_price: None,
                                                                    trail_type: None,
                                                                    trail_value: None,
                                                                    trail_spread: None,
                                                                };
                                                                match trade_client
                                                                    .modify_or_cancel_order(&cancel_req)
                                                                    .await
                                                                {
                                                                    Ok(_) => {
                                                                        let cancel_time = chrono::Local::now().to_rfc3339();
                                                                        rebalance_logs.push(runtime_log_with_ts(format!(
                                                                            "Futu SIM canceled open order {}",
                                                                            order_id
                                                                        )));
                                                                        cancel_events_this_cycle.push(serde_json::json!({
                                                                            "timestamp": cancel_time,
                                                                            "order_id": order_id,
                                                                            "symbol": futu_json_get_string(&cancel_item, &["symbol"]),
                                                                            "trd_side": futu_json_get_string(&cancel_item, &["trd_side"]),
                                                                            "qty": cancel_item.get("qty").cloned(),
                                                                            "price": cancel_item.get("price").cloned(),
                                                                            "order_status": futu_json_get_string(&cancel_item, &["order_status"]),
                                                                            "order_type": futu_json_get_string(&cancel_item, &["order_type"]),
                                                                            "reason": "Rebalance pre-cancel pending order",
                                                                            "signal_id": signal_id,
                                                                        }));
                                                                    }
                                                                    Err(err) => {
                                                                        cancel_stage_ok = false;
                                                                        consume_rebalance_signal = false;
                                                                        rebalance_logs.push(runtime_log_with_ts(format!(
                                                                            "WARNING: Futu SIM failed to cancel order {}: {}",
                                                                            order_id, err
                                                                        )));
                                                                    }
                                                                }

                                                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                                            }
                                                        }
                                                    }
                                                    Err(err) => {
                                                        cancel_stage_ok = false;
                                                        consume_rebalance_signal = false;
                                                        rebalance_logs.push(runtime_log_with_ts(format!(
                                                            "WARNING: Futu SIM rebalance skipped due to order-list query failure: {}",
                                                            err
                                                        )));
                                                    }
                                                }

                                                if !cancel_stage_ok {
                                                    rebalance_logs.push(runtime_log_with_ts(
                                                        "Futu SIM rebalance aborted before new orders because cancel stage failed",
                                                    ));
                                                    consume_rebalance_signal = false;
                                                } else {

                                                for (symbol, qty, price) in sell_orders {
                                                    if qty < 1.0 {
                                                        continue;
                                                    }
                                                    let request = futu::FutuPlaceOrderRequest {
                                                        symbol: symbol.clone(),
                                                        side: "SELL".to_string(),
                                                        quantity: qty,
                                                        price: Some(price),
                                                        order_type: Some("MARKET".to_string()),
                                                        market: market_value.clone(),
                                                        trd_env: env_value.clone(),
                                                        acc_id: acc_value.clone(),
                                                        adjust_limit: None,
                                                        remark: None,
                                                        time_in_force: None,
                                                        fill_outside_rth: None,
                                                        session: None,
                                                        aux_price: None,
                                                        trail_type: None,
                                                        trail_value: None,
                                                        trail_spread: None,
                                                    };
                                                    match trade_client.place_order(&request).await {
                                                        Ok(res) => {
                                                            available_cash += qty * price;
                                                            let order_id = res
                                                                .get("order_id")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("--");
                                                            rebalance_logs.push(runtime_log_with_ts(format!(
                                                                "Futu SIM rebalance SELL {} x {:.0} @ {:.4} (order_id={})",
                                                                symbol, qty, price, order_id
                                                            )));
                                                        }
                                                        Err(err) => {
                                                            rebalance_logs.push(runtime_log_with_ts(format!(
                                                                "WARNING: Futu SIM SELL failed {} x {:.0}: {}",
                                                                symbol, qty, err
                                                            )));
                                                        }
                                                    }
                                                }

                                                for (symbol, qty, price) in buy_orders {
                                                    if qty < 1.0 {
                                                        continue;
                                                    }
                                                    let affordable_qty = (available_cash / price).floor().max(0.0);
                                                    let execute_qty = qty.min(affordable_qty);
                                                    if execute_qty < 1.0 {
                                                        rebalance_logs.push(runtime_log_with_ts(format!(
                                                            "WARNING: Futu SIM BUY skipped {} due to insufficient cash (need {:.2}, have {:.2})",
                                                            symbol,
                                                            qty * price,
                                                            available_cash
                                                        )));
                                                        continue;
                                                    }

                                                    let request = futu::FutuPlaceOrderRequest {
                                                        symbol: symbol.clone(),
                                                        side: "BUY".to_string(),
                                                        quantity: execute_qty,
                                                        price: Some(price),
                                                        order_type: Some("MARKET".to_string()),
                                                        market: market_value.clone(),
                                                        trd_env: env_value.clone(),
                                                        acc_id: acc_value.clone(),
                                                        adjust_limit: None,
                                                        remark: None,
                                                        time_in_force: None,
                                                        fill_outside_rth: None,
                                                        session: None,
                                                        aux_price: None,
                                                        trail_type: None,
                                                        trail_value: None,
                                                        trail_spread: None,
                                                    };
                                                    match trade_client.place_order(&request).await {
                                                        Ok(res) => {
                                                            available_cash -= execute_qty * price;
                                                            let order_id = res
                                                                .get("order_id")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("--");
                                                            rebalance_logs.push(runtime_log_with_ts(format!(
                                                                "Futu SIM rebalance BUY {} x {:.0} @ {:.4} (order_id={})",
                                                                symbol, execute_qty, price, order_id
                                                            )));
                                                        }
                                                        Err(err) => {
                                                            rebalance_logs.push(runtime_log_with_ts(format!(
                                                                "WARNING: Futu SIM BUY failed {} x {:.0}: {}",
                                                                symbol, execute_qty, err
                                                            )));
                                                        }
                                                    }
                                                }
                                                }
                                            }
                                            Err(err) => {
                                                consume_rebalance_signal = false;
                                                rebalance_logs.push(runtime_log_with_ts(format!(
                                                    "WARNING: Futu SIM rebalance skipped: failed to init trading client: {}",
                                                    err
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                        } else if selected_env == "REAL" {
                            rebalance_logs.push(runtime_log_with_ts(format!(
                                "Futu rebalance signal {} detected but skipped in REAL mode",
                                signal_id
                            )));
                        }

                        if consume_rebalance_signal {
                            last_rebalance_signal = Some(signal_id);
                        }
                    }
                }

                let timestamp = chrono::Local::now().to_rfc3339();
                let mut minute_symbols = Vec::new();
                let mut minute_holdings = Vec::new();
                let mut holdings_symbols = Vec::new();
                let mut total_holdings_value = 0.0;

                for position in &payload.positions {
                    let fallback_prev_price = prev_prices
                        .get(&position.symbol)
                        .copied()
                        .filter(|v| v.is_finite() && *v > 0.0);

                    let live_price = quote_map
                        .get(&position.symbol)
                        .map(|q| q.price)
                        .filter(|v| v.is_finite() && *v > 0.0)
                        .or_else(|| {
                            if position.market_price.is_finite() && position.market_price > 0.0 {
                                Some(position.market_price)
                            } else {
                                None
                            }
                        })
                        .or(fallback_prev_price)
                        .unwrap_or(position.avg_cost.max(0.0));

                    let prev = prev_prices.get(&position.symbol).copied().unwrap_or(live_price);
                    let change_1m = live_price - prev;
                    let change_1m_pct = if prev > 0.0 {
                        (change_1m / prev) * 100.0
                    } else {
                        0.0
                    };

                    minute_symbols.push(paper_trading::MinuteSymbolSnapshot {
                        symbol: position.symbol.clone(),
                        price: live_price,
                        change_1m,
                        change_1m_pct,
                    });

                    if position.quantity.abs() > 1e-9 {
                        let asset_value = live_price * position.quantity;
                        minute_holdings.push(paper_trading::MinuteHoldingSnapshot {
                            symbol: position.symbol.clone(),
                            quantity: position.quantity,
                            price: live_price,
                            asset_value,
                            avg_cost: position.avg_cost,
                        });
                        holdings_symbols.push(position.symbol.clone());
                        total_holdings_value += asset_value;
                    }

                    prev_prices.insert(position.symbol.clone(), live_price);
                }

                holdings_symbols.sort();
                minute_symbols.sort_by(|a, b| a.symbol.cmp(&b.symbol));
                minute_holdings.sort_by(|a, b| a.symbol.cmp(&b.symbol));

                let total_value = payload.cash_usd + total_holdings_value;
                if initial_capital.is_none() && total_value.is_finite() && total_value > 0.0 {
                    initial_capital = Some(total_value);
                }
                let base = initial_capital.unwrap_or(total_value.max(1.0));
                let pnl_usd = total_value - base;
                let pnl_pct = if base > 0.0 { (pnl_usd / base) * 100.0 } else { 0.0 };

                let snapshot = paper_trading::MinutePortfolioSnapshot {
                    timestamp: timestamp.clone(),
                    total_value,
                    cash_usd: payload.cash_usd,
                    pnl_usd,
                    pnl_pct,
                    benchmark_return_pct: 0.0,
                    symbols: minute_symbols,
                    holdings: minute_holdings,
                    holdings_symbols,
                };

                let mut qty_changed = false;
                let mut new_qty_map = HashMap::new();
                for position in &payload.positions {
                    if position.quantity.abs() <= 1e-9 {
                        continue;
                    }
                    new_qty_map.insert(position.symbol.clone(), position.quantity);
                    let prev = prev_qty.get(&position.symbol).copied().unwrap_or(0.0);
                    if (prev - position.quantity).abs() > 1e-6 {
                        qty_changed = true;
                    }
                }
                if prev_qty.len() != new_qty_map.len() {
                    qty_changed = true;
                }
                prev_qty = new_qty_map;

                let mut open_orders_snapshot: Vec<serde_json::Value> = Vec::new();
                let mut trade_history_snapshot: Vec<serde_json::Value> = Vec::new();
                let mut history_orders_snapshot: Vec<serde_json::Value> = Vec::new();
                if let Ok(mut query_client) = futu::FutuApiClient::from_env() {
                    query_client.set_account_id_override(payload.selected_acc_id.clone());
                    if let Ok(order_list_raw) = query_client.get_order_list().await {
                        open_orders_snapshot = futu_extract_order_rows(&order_list_raw);
                    }
                    if let Ok(trade_list_raw) = query_client.get_today_executed_trades().await {
                        trade_history_snapshot = futu_extract_order_rows(&trade_list_raw);
                    }
                    let (history_start, history_end) = futu_history_date_range(history_order_range_days);
                    if let Ok(history_order_list_raw) = query_client
                        .get_historical_order_list_in_range(history_start, history_end)
                        .await
                    {
                        history_orders_snapshot = futu_extract_order_rows(&history_order_list_raw);
                    }
                }

                let mut fs = futu_state.lock().await;
                let was_connected = fs.connected;
                fs.connected = true;
                fs.account_cash_usd = payload.cash_usd;
                fs.account_buying_power_usd = payload.buying_power_usd;
                if fs.preferred_acc_id.is_none() {
                    fs.preferred_acc_id = Some("9468130".to_string());
                }
                fs.selected_acc_id = payload.selected_acc_id.clone();
                fs.selected_trd_env = payload.selected_trd_env.clone();
                fs.selected_market = payload.selected_market.clone();
                fs.selected_account = payload.opend_selected_account.clone();
                fs.positions = payload
                    .positions
                    .iter()
                    .map(|position| FutuPositionState {
                        symbol: position.symbol.clone(),
                        quantity: position.quantity,
                        avg_cost: position.avg_cost,
                        market_price: position.market_price,
                        updated_at: position.updated_at.clone(),
                    })
                    .collect();
                fs.open_orders = open_orders_snapshot;
                fs.trade_history = trade_history_snapshot;
                fs.history_orders = history_orders_snapshot;
                if !cancel_events_this_cycle.is_empty() {
                    fs.cancel_history.extend(cancel_events_this_cycle);
                    if fs.cancel_history.len() > 500 {
                        let keep_from = fs.cancel_history.len().saturating_sub(500);
                        fs.cancel_history = fs.cancel_history.split_off(keep_from);
                    }
                }
                fs.latest_snapshot = Some(snapshot.clone());
                fs.snapshots.push(snapshot);
                if fs.snapshots.len() > 6000 {
                    let keep_from = fs.snapshots.len().saturating_sub(6000);
                    fs.snapshots = fs.snapshots.split_off(keep_from);
                }

                if fs.runtime_file.is_none() {
                    if let Ok((strategy_path, runtime_path)) = create_futu_output_paths() {
                        fs.strategy_file = Some(strategy_path.display().to_string());
                        fs.runtime_file = Some(runtime_path.display().to_string());
                        fs.logs.push(runtime_log_with_ts(format!(
                            "Futu runtime file created => {}",
                            runtime_path.display()
                        )));
                        fs.logs.push(runtime_log_with_ts(format!(
                            "Futu strategy file created => {}",
                            strategy_path.display()
                        )));
                    }
                }

                if !was_connected {
                    fs.logs.push(runtime_log_with_ts("Futu execution connected"));
                }

                let binding_key = format!(
                    "acc_id={} env={} market={}",
                    payload
                        .selected_acc_id
                        .as_deref()
                        .unwrap_or("--"),
                    payload
                        .selected_trd_env
                        .as_deref()
                        .unwrap_or("--"),
                    payload
                        .selected_market
                        .as_deref()
                        .unwrap_or("--")
                );

                if last_account_binding.as_deref() != Some(binding_key.as_str()) {
                    fs.logs.push(runtime_log_with_ts(format!(
                        "Futu account binding => {}",
                        binding_key
                    )));
                    last_account_binding = Some(binding_key);
                }

                if let Some(selected_account) = payload.opend_selected_account.as_ref() {
                    fs.logs.push(runtime_log_with_ts(format!(
                        "Futu OpenD selected account row => {}",
                        selected_account
                    )));
                }

                let opend_dump = serde_json::json!({
                    "selected_acc_id": payload.selected_acc_id,
                    "selected_trd_env": payload.selected_trd_env,
                    "selected_market": payload.selected_market,
                    "account_list": payload.opend_account_list,
                    "selected_account": payload.opend_selected_account,
                    "account_info_raw": payload.opend_account_info_raw,
                    "positions_raw": payload.opend_positions_raw,
                    "cash_usd": payload.cash_usd,
                    "buying_power_usd": payload.buying_power_usd,
                })
                .to_string();

                if last_opend_dump.as_deref() != Some(opend_dump.as_str()) {
                    fs.logs.push(runtime_log_with_ts(format!(
                        "Futu OpenD full payload => {}",
                        opend_dump
                    )));
                    last_opend_dump = Some(opend_dump);
                }

                if qty_changed {
                    let holdings_count = fs
                        .latest_snapshot
                        .as_ref()
                        .map(|snap| snap.holdings_symbols.len())
                        .unwrap_or(0);
                    fs.logs.push(runtime_log_with_ts(format!(
                        "Futu execution synced holdings ({} symbols)",
                        holdings_count
                    )));
                }
                if !rebalance_logs.is_empty() {
                    fs.logs.extend(rebalance_logs);
                }
                if fs.logs.len() > 200 {
                    let keep_from = fs.logs.len().saturating_sub(200);
                    fs.logs = fs.logs.split_off(keep_from);
                }

                let runtime_path = fs.runtime_file.clone();
                let strategy_path = fs.strategy_file.clone();
                let line = FutuSimRuntimeLine {
                    timestamp: timestamp.clone(),
                    conn_host: fs.conn_host.clone(),
                    conn_port: fs.conn_port,
                    conn_market: fs.conn_market.clone(),
                    conn_security_firm: fs.conn_security_firm.clone(),
                    selected_acc_id: fs.selected_acc_id.clone(),
                    selected_trd_env: fs.selected_trd_env.clone(),
                    selected_market: fs.selected_market.clone(),
                    account_cash_usd: fs.account_cash_usd,
                    account_buying_power_usd: fs.account_buying_power_usd,
                    total_value_usd: fs.latest_snapshot.as_ref().map(|x| x.total_value).unwrap_or(0.0),
                    pnl_usd: fs.latest_snapshot.as_ref().map(|x| x.pnl_usd).unwrap_or(0.0),
                    pnl_pct: fs.latest_snapshot.as_ref().map(|x| x.pnl_pct).unwrap_or(0.0),
                    positions: fs.positions.clone(),
                    snapshot: fs.latest_snapshot.clone().unwrap_or(paper_trading::MinutePortfolioSnapshot {
                        timestamp: timestamp.clone(),
                        total_value: 0.0,
                        cash_usd: 0.0,
                        pnl_usd: 0.0,
                        pnl_pct: 0.0,
                        benchmark_return_pct: 0.0,
                        symbols: Vec::new(),
                        holdings: Vec::new(),
                        holdings_symbols: Vec::new(),
                    }),
                    opend_account_list: payload.opend_account_list.clone(),
                    opend_selected_account: payload.opend_selected_account.clone(),
                    opend_account_info_raw: payload.opend_account_info_raw.clone(),
                    opend_positions_raw: payload.opend_positions_raw.clone(),
                };

                let strategy_log = FutuSimStrategyLog {
                    created_at: chrono::Local::now().to_rfc3339(),
                    started_at: fs.started_at.clone().unwrap_or_else(|| chrono::Local::now().to_rfc3339()),
                    conn_host: fs.conn_host.clone(),
                    conn_port: fs.conn_port,
                    conn_market: fs.conn_market.clone(),
                    conn_security_firm: fs.conn_security_firm.clone(),
                    selected_acc_id: fs.selected_acc_id.clone(),
                    selected_trd_env: fs.selected_trd_env.clone(),
                    selected_market: fs.selected_market.clone(),
                    runtime_file: runtime_path.clone().unwrap_or_default(),
                    latest_snapshot: fs.latest_snapshot.clone(),
                    latest_positions: fs.positions.clone(),
                    latest_logs: fs.logs.iter().rev().take(80).cloned().collect::<Vec<_>>().into_iter().rev().collect(),
                };
                drop(fs);

                if let Some(path) = runtime_path {
                    if let Err(err) = append_jsonl_line(Path::new(&path), &line) {
                        let mut fs = futu_state.lock().await;
                        fs.logs.push(runtime_log_with_ts(format!(
                            "WARNING: failed writing futu runtime log: {}",
                            err
                        )));
                    }
                }

                if let Some(path) = strategy_path {
                    if let Err(err) = write_json_pretty(Path::new(&path), &strategy_log) {
                        let mut fs = futu_state.lock().await;
                        fs.logs.push(runtime_log_with_ts(format!(
                            "WARNING: failed writing futu strategy log: {}",
                            err
                        )));
                    }
                }
            }
            Err(err) => {
                let mut fs = futu_state.lock().await;
                if fs.connected {
                    fs.logs.push(runtime_log_with_ts(format!(
                        "WARNING: Futu execution disconnected: {}",
                        err
                    )));
                }
                fs.connected = false;
                if fs.logs.len() > 200 {
                    let keep_from = fs.logs.len().saturating_sub(200);
                    fs.logs = fs.logs.split_off(keep_from);
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    }
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
                    ps.logs.push(runtime_log_with_ts("Paper trading started"));
                }
                paper_trading::PaperEvent::Info(msg) => {
                    ps.logs.push(runtime_log_with_ts(msg));
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
                    ps.logs.push(runtime_log_with_ts(format!("WARNING: {}", msg)));
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
                    ps.logs.push(runtime_log_with_ts(format!("Error: {}", msg)));
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
            ps.logs.push(runtime_log_with_ts(format!("Error: {}", err)));
        } else {
            ps.logs.push(runtime_log_with_ts("Paper trading stopped"));
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
                        .push(runtime_log_with_ts("Paper history loaded · Restored holdings and running"));
                }
                paper_trading::PaperEvent::Info(msg) => {
                    ps.logs.push(runtime_log_with_ts(msg));
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
                    ps.logs.push(runtime_log_with_ts(format!("WARNING: {}", msg)));
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
                    ps.logs.push(runtime_log_with_ts(format!("Error: {}", msg)));
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
            ps.logs.push(runtime_log_with_ts(format!("Error: {}", err)));
        } else {
            ps.logs.push(runtime_log_with_ts("Paper trading stopped"));
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
    status.data_live_fetch_diagnostics = data::current_live_fetch_diagnostics().await;
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

        match data::fetch_latest_prices_1m_prefetch(&preflight_symbols).await {
            Ok(prices) => {
                if prices.len() < preflight_symbols.len() {
                    let missing = preflight_symbols
                        .iter()
                        .filter(|s| !prices.contains_key(*s))
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(",");
                    return Err(api_err(
                        StatusCode::BAD_GATEWAY,
                        &format!(
                            "Data fetch partial success in apply-now prefetch; missing symbols: {}",
                            missing
                        ),
                    ));
                }
            }
            Err(error) => {
                return Err(api_err(
                    StatusCode::BAD_GATEWAY,
                    &format!(
                        "Data fetch failed in apply-now batch prefetch. Apply-now optimization stopped: {}",
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

fn create_futu_output_paths() -> Result<(PathBuf, PathBuf)> {
    let log_dir = config::project_root_path().join("log");
    if !log_dir.exists() {
        std::fs::create_dir_all(&log_dir)?;
    }

    let suffix = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
    let strategy_path = log_dir.join(format!("futu_sim_strategy_{}.json", suffix));
    let runtime_path = log_dir.join(format!("futu_sim_runtime_{}.jsonl", suffix));
    Ok((strategy_path, runtime_path))
}

fn append_jsonl_line<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let line = serde_json::to_string(value)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{}", line)?;
    Ok(())
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let raw = serde_json::to_string_pretty(value)?;
    std::fs::write(path, raw)?;
    Ok(())
}

fn resolve_input_path(raw: &str) -> PathBuf {
    let input = PathBuf::from(raw);
    if input.is_absolute() {
        return input;
    }
    if input.exists() {
        return input;
    }
    config::project_root_path().join(input)
}

fn find_latest_log_file(prefix: &str, suffix: &str) -> Option<PathBuf> {
    let log_dir = config::project_root_path().join("log");
    let entries = std::fs::read_dir(&log_dir).ok()?;
    let mut files = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with(prefix) && name.ends_with(suffix))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    files.sort();
    files.pop()
}

fn load_futu_strategy_log(path: &Path) -> Result<FutuSimStrategyLog> {
    let raw = std::fs::read_to_string(path)?;
    let parsed = serde_json::from_str::<FutuSimStrategyLog>(&raw)?;
    Ok(parsed)
}

fn load_futu_runtime_lines(path: &Path) -> Result<Vec<FutuSimRuntimeLine>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let raw = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Ok(parsed) = serde_json::from_str::<FutuSimRuntimeLine>(trimmed) {
            out.push(parsed);
            continue;
        }

        if let Ok(snapshot) = serde_json::from_str::<paper_trading::MinutePortfolioSnapshot>(trimmed) {
            out.push(FutuSimRuntimeLine {
                timestamp: snapshot.timestamp.clone(),
                conn_host: "127.0.0.1".to_string(),
                conn_port: 11111,
                conn_market: "US".to_string(),
                conn_security_firm: "FUTUSECURITIES".to_string(),
                selected_acc_id: None,
                selected_trd_env: None,
                selected_market: None,
                account_cash_usd: snapshot.cash_usd,
                account_buying_power_usd: snapshot.cash_usd,
                total_value_usd: snapshot.total_value,
                pnl_usd: snapshot.pnl_usd,
                pnl_pct: snapshot.pnl_pct,
                positions: Vec::new(),
                snapshot,
                opend_account_list: None,
                opend_selected_account: None,
                opend_account_info_raw: None,
                opend_positions_raw: None,
            });
        }
    }
    Ok(out)
}

fn api_err(status: StatusCode, message: &str) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            error: message.to_string(),
        }),
    )
}

fn runtime_log_with_ts(message: impl AsRef<str>) -> String {
    format!("[{}] {}", chrono::Local::now().format("%H:%M:%S"), message.as_ref())
}

fn internal_err<E: std::fmt::Display>(err: E) -> (StatusCode, Json<ApiError>) {
    api_err(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string())
}

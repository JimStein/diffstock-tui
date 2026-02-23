use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use futures_util::{SinkExt, StreamExt};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use tracing::{info, warn};
use std::sync::OnceLock;
use tokio::sync::RwLock;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

static LIVE_DATA_SOURCE: OnceLock<RwLock<String>> = OnceLock::new();
static POLYGON_WS_CLIENT: OnceLock<PolygonWsClient> = OnceLock::new();
static DATA_PROVIDER_MODE: OnceLock<DataProviderMode> = OnceLock::new();
static POLYGON_WS_CONNECTED: OnceLock<RwLock<bool>> = OnceLock::new();
static WS_PRIORITY_RTH_ONLY: OnceLock<bool> = OnceLock::new();
static WS_DIAGNOSTICS: OnceLock<RwLock<WsDiagnosticsState>> = OnceLock::new();
static LIVE_FETCH_DIAGNOSTICS: OnceLock<RwLock<LiveFetchDiagnosticsState>> = OnceLock::new();
static WS_TIMEOUT_STRIKES: OnceLock<RwLock<u32>> = OnceLock::new();
static WS_FAILOVER_ACTIVE: OnceLock<RwLock<bool>> = OnceLock::new();
static WS_LAST_STRIKE_AT_MS: OnceLock<RwLock<i64>> = OnceLock::new();

const WS_TIMEOUT_WINDOW_MS: i64 = 120_000;
const WS_TIMEOUT_FAILOVER_THRESHOLD: u32 = 3;
const WS_STRIKE_COOLDOWN_MS: i64 = 30_000;

#[derive(Clone, Debug, Serialize, Default)]
pub struct WsDiagnostics {
    pub connected: bool,
    pub timeout_strikes_total: u32,
    pub failover_active: bool,
    pub text_messages_total: u64,
    pub status_messages_total: u64,
    pub parse_failures_total: u64,
    pub data_events_total: u64,
    pub accepted_price_events_total: u64,
    pub dropped_invalid_price_events_total: u64,
    pub last_text_at_ms: Option<i64>,
    pub last_data_event_at_ms: Option<i64>,
    pub last_parse_failure_at_ms: Option<i64>,
    pub last_parse_failure: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct WsDiagnosticsState {
    connected: bool,
    text_messages_total: u64,
    status_messages_total: u64,
    parse_failures_total: u64,
    data_events_total: u64,
    accepted_price_events_total: u64,
    dropped_invalid_price_events_total: u64,
    last_text_at_ms: Option<i64>,
    last_data_event_at_ms: Option<i64>,
    last_parse_failure_at_ms: Option<i64>,
    last_parse_failure: Option<String>,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct LiveFetchDiagnostics {
    pub prefetch_calls_total: u64,
    pub prefetch_symbols_total: u64,
    pub prefetch_fallback_total: u64,
    pub last_prefetch_symbol_count: u64,
    pub last_prefetch_success_count: u64,
    pub last_prefetch_missing_count: u64,
    pub last_prefetch_duration_ms: u64,
    pub last_prefetch_mode: String,
    pub last_prefetch_at_ms: Option<i64>,
    pub last_prefetch_error: Option<String>,
    pub last_history_symbol: Option<String>,
    pub last_history_range: Option<String>,
    pub last_history_source: Option<String>,
    pub last_history_at_ms: Option<i64>,
    pub last_history_error: Option<String>,
    pub history_source_log: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct LiveFetchDiagnosticsState {
    prefetch_calls_total: u64,
    prefetch_symbols_total: u64,
    prefetch_fallback_total: u64,
    last_prefetch_symbol_count: u64,
    last_prefetch_success_count: u64,
    last_prefetch_missing_count: u64,
    last_prefetch_duration_ms: u64,
    last_prefetch_mode: String,
    last_prefetch_at_ms: Option<i64>,
    last_prefetch_error: Option<String>,
    last_history_symbol: Option<String>,
    last_history_range: Option<String>,
    last_history_source: Option<String>,
    last_history_at_ms: Option<i64>,
    last_history_error: Option<String>,
    history_source_log: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataProviderMode {
    Polygon,
    Yfinance,
}

impl DataProviderMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Polygon => "polygon",
            Self::Yfinance => "yfinance",
        }
    }
}

pub fn configured_data_provider_mode() -> DataProviderMode {
    *DATA_PROVIDER_MODE.get_or_init(|| {
        let provider = std::env::var("DIFFSTOCK_DATA_PROVIDER")
            .unwrap_or_else(|_| "yfinance".to_string())
            .trim()
            .to_ascii_lowercase();

        match provider.as_str() {
            "polygon" => DataProviderMode::Polygon,
            "yfinance" | "yahoo" => DataProviderMode::Yfinance,
            "auto" => {
                warn!(
                    "DIFFSTOCK_DATA_PROVIDER=auto is deprecated and disabled; defaulting to yfinance. Set polygon or yfinance explicitly."
                );
                DataProviderMode::Yfinance
            }
            other => {
                warn!(
                    "Unknown DIFFSTOCK_DATA_PROVIDER={} ; defaulting to yfinance. Allowed values: polygon | yfinance",
                    other
                );
                DataProviderMode::Yfinance
            }
        }
    })
}

fn live_data_source_cell() -> &'static RwLock<String> {
    LIVE_DATA_SOURCE.get_or_init(|| RwLock::new("Unknown".to_string()))
}

fn polygon_ws_connected_cell() -> &'static RwLock<bool> {
    POLYGON_WS_CONNECTED.get_or_init(|| RwLock::new(false))
}

fn ws_diagnostics_cell() -> &'static RwLock<WsDiagnosticsState> {
    WS_DIAGNOSTICS.get_or_init(|| RwLock::new(WsDiagnosticsState::default()))
}

fn live_fetch_diagnostics_cell() -> &'static RwLock<LiveFetchDiagnosticsState> {
    LIVE_FETCH_DIAGNOSTICS.get_or_init(|| RwLock::new(LiveFetchDiagnosticsState::default()))
}

fn ws_timeout_strikes_cell() -> &'static RwLock<u32> {
    WS_TIMEOUT_STRIKES.get_or_init(|| RwLock::new(0))
}

fn ws_failover_active_cell() -> &'static RwLock<bool> {
    WS_FAILOVER_ACTIVE.get_or_init(|| RwLock::new(false))
}

fn ws_last_strike_at_ms_cell() -> &'static RwLock<i64> {
    WS_LAST_STRIKE_AT_MS.get_or_init(|| RwLock::new(0))
}

fn now_ts_ms() -> i64 {
    Utc::now().timestamp_millis()
}

async fn set_live_data_source(source: &str) {
    let mut guard = live_data_source_cell().write().await;
    *guard = source.to_string();
}

pub async fn current_live_data_source() -> String {
    live_data_source_cell().read().await.clone()
}

pub async fn polygon_ws_connected() -> bool {
    *polygon_ws_connected_cell().read().await
}

pub async fn current_ws_diagnostics() -> WsDiagnostics {
    let timeout_strikes_total = *ws_timeout_strikes_cell().read().await;
    let failover_active = *ws_failover_active_cell().read().await;
    let diag = ws_diagnostics_cell().read().await;
    WsDiagnostics {
        connected: diag.connected,
        timeout_strikes_total,
        failover_active,
        text_messages_total: diag.text_messages_total,
        status_messages_total: diag.status_messages_total,
        parse_failures_total: diag.parse_failures_total,
        data_events_total: diag.data_events_total,
        accepted_price_events_total: diag.accepted_price_events_total,
        dropped_invalid_price_events_total: diag.dropped_invalid_price_events_total,
        last_text_at_ms: diag.last_text_at_ms,
        last_data_event_at_ms: diag.last_data_event_at_ms,
        last_parse_failure_at_ms: diag.last_parse_failure_at_ms,
        last_parse_failure: diag.last_parse_failure.clone(),
    }
}

pub async fn current_live_fetch_diagnostics() -> LiveFetchDiagnostics {
    let diag = live_fetch_diagnostics_cell().read().await;
    LiveFetchDiagnostics {
        prefetch_calls_total: diag.prefetch_calls_total,
        prefetch_symbols_total: diag.prefetch_symbols_total,
        prefetch_fallback_total: diag.prefetch_fallback_total,
        last_prefetch_symbol_count: diag.last_prefetch_symbol_count,
        last_prefetch_success_count: diag.last_prefetch_success_count,
        last_prefetch_missing_count: diag.last_prefetch_missing_count,
        last_prefetch_duration_ms: diag.last_prefetch_duration_ms,
        last_prefetch_mode: diag.last_prefetch_mode.clone(),
        last_prefetch_at_ms: diag.last_prefetch_at_ms,
        last_prefetch_error: diag.last_prefetch_error.clone(),
        last_history_symbol: diag.last_history_symbol.clone(),
        last_history_range: diag.last_history_range.clone(),
        last_history_source: diag.last_history_source.clone(),
        last_history_at_ms: diag.last_history_at_ms,
        last_history_error: diag.last_history_error.clone(),
        history_source_log: diag.history_source_log.clone(),
    }
}

async fn mark_history_source(symbol: &str, range: &str, source: &str, error: Option<&str>) {
    let now_ms = now_ts_ms();
    let mut diag = live_fetch_diagnostics_cell().write().await;
    diag.last_history_symbol = Some(symbol.to_string());
    diag.last_history_range = Some(range.to_string());
    diag.last_history_source = Some(source.to_string());
    diag.last_history_at_ms = Some(now_ms);
    diag.last_history_error = error.map(|v| v.to_string());

    let ts = chrono::Local::now().format("%H:%M:%S").to_string();
    let line = if let Some(err) = error {
        format!("{} {} {} -> {} (error: {})", ts, symbol, range, source, err)
    } else {
        format!("{} {} {} -> {}", ts, symbol, range, source)
    };
    diag.history_source_log.insert(0, line);
    if diag.history_source_log.len() > 10 {
        diag.history_source_log.truncate(10);
    }
}

async fn mark_live_prefetch_success(
    symbol_count: usize,
    success_count: usize,
    duration_ms: u64,
    mode: &str,
    fallback_used: bool,
    warning: Option<&str>,
) {
    let mut diag = live_fetch_diagnostics_cell().write().await;
    diag.prefetch_calls_total = diag.prefetch_calls_total.saturating_add(1);
    diag.prefetch_symbols_total = diag.prefetch_symbols_total.saturating_add(symbol_count as u64);
    if fallback_used {
        diag.prefetch_fallback_total = diag.prefetch_fallback_total.saturating_add(1);
    }
    diag.last_prefetch_symbol_count = symbol_count as u64;
    diag.last_prefetch_success_count = success_count as u64;
    diag.last_prefetch_missing_count = symbol_count.saturating_sub(success_count) as u64;
    diag.last_prefetch_duration_ms = duration_ms;
    diag.last_prefetch_mode = mode.to_string();
    diag.last_prefetch_at_ms = Some(now_ts_ms());
    diag.last_prefetch_error = warning.map(|v| v.to_string());
}

async fn mark_live_prefetch_failure(symbol_count: usize, duration_ms: u64, mode: &str, error: &str) {
    let mut diag = live_fetch_diagnostics_cell().write().await;
    diag.prefetch_calls_total = diag.prefetch_calls_total.saturating_add(1);
    diag.prefetch_symbols_total = diag.prefetch_symbols_total.saturating_add(symbol_count as u64);
    if mode.contains("fallback") {
        diag.prefetch_fallback_total = diag.prefetch_fallback_total.saturating_add(1);
    }
    diag.last_prefetch_symbol_count = symbol_count as u64;
    diag.last_prefetch_success_count = 0;
    diag.last_prefetch_missing_count = symbol_count as u64;
    diag.last_prefetch_duration_ms = duration_ms;
    diag.last_prefetch_mode = mode.to_string();
    diag.last_prefetch_at_ms = Some(now_ts_ms());
    diag.last_prefetch_error = Some(error.to_string());
}

async fn ws_diag_set_connected(connected: bool) {
    let mut diag = ws_diagnostics_cell().write().await;
    diag.connected = connected;
}

async fn ws_diag_mark_text() {
    let mut diag = ws_diagnostics_cell().write().await;
    diag.text_messages_total = diag.text_messages_total.saturating_add(1);
    diag.last_text_at_ms = Some(now_ts_ms());
}

async fn ws_diag_mark_status(count: u64) {
    if count == 0 {
        return;
    }
    let mut diag = ws_diagnostics_cell().write().await;
    diag.status_messages_total = diag.status_messages_total.saturating_add(count);
}

async fn ws_diag_mark_parse_failure(message: &str) {
    let mut diag = ws_diagnostics_cell().write().await;
    diag.parse_failures_total = diag.parse_failures_total.saturating_add(1);
    diag.last_parse_failure_at_ms = Some(now_ts_ms());
    diag.last_parse_failure = Some(message.to_string());
}

async fn ws_diag_mark_data_batch(accepted_count: u64, dropped_count: u64, _last_accepted_ts_ms: Option<i64>) {
    if accepted_count == 0 && dropped_count == 0 {
        return;
    }
    let mut diag = ws_diagnostics_cell().write().await;
    let total = accepted_count.saturating_add(dropped_count);
    diag.data_events_total = diag.data_events_total.saturating_add(total);
    if accepted_count > 0 {
        diag.accepted_price_events_total = diag
            .accepted_price_events_total
            .saturating_add(accepted_count);
        diag.last_data_event_at_ms = Some(now_ts_ms());
    }
    if dropped_count > 0 {
        diag.dropped_invalid_price_events_total = diag
            .dropped_invalid_price_events_total
            .saturating_add(dropped_count);
    }
}

async fn ws_recently_healthy() -> bool {
    let diag = ws_diagnostics_cell().read().await;
    if !diag.connected {
        return false;
    }
    let Some(last_ms) = diag.last_text_at_ms else {
        return false;
    };
    now_ts_ms().saturating_sub(last_ms) <= WS_TIMEOUT_WINDOW_MS
}

async fn ws_record_timeout_strike_if_needed() -> u32 {
    let now = now_ts_ms();
    {
        let last = ws_last_strike_at_ms_cell().read().await;
        if now.saturating_sub(*last) < WS_STRIKE_COOLDOWN_MS {
            return *ws_timeout_strikes_cell().read().await;
        }
    }

    {
        let mut last = ws_last_strike_at_ms_cell().write().await;
        *last = now;
    }

    let mut strikes = ws_timeout_strikes_cell().write().await;
    *strikes = strikes.saturating_add(1);
    let current = *strikes;
    drop(strikes);

    if current >= WS_TIMEOUT_FAILOVER_THRESHOLD {
        let mut failover = ws_failover_active_cell().write().await;
        if !*failover {
            warn!(
                "Polygon WS timeout strikes reached {}/{}; switching temporary live-source failover to Snapshot",
                current,
                WS_TIMEOUT_FAILOVER_THRESHOLD
            );
        }
        *failover = true;
    }

    current
}

async fn ws_reset_timeout_state(reason: &str) {
    {
        let mut strikes = ws_timeout_strikes_cell().write().await;
        *strikes = 0;
    }
    {
        let mut failover = ws_failover_active_cell().write().await;
        if *failover {
            info!("Polygon WS recovered ({}); switching live-source back to WS", reason);
        }
        *failover = false;
    }
}

async fn ws_failover_active() -> bool {
    *ws_failover_active_cell().read().await
}

fn ws_priority_rth_only() -> bool {
    *WS_PRIORITY_RTH_ONLY.get_or_init(|| {
        let raw = std::env::var("DIFFSTOCK_WS_PRIORITY_RTH_ONLY")
            .unwrap_or_else(|_| "true".to_string())
            .trim()
            .to_ascii_lowercase();

        match raw.as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            other => {
                warn!(
                    "Unknown DIFFSTOCK_WS_PRIORITY_RTH_ONLY={} ; defaulting to true",
                    other
                );
                true
            }
        }
    })
}

fn is_us_regular_trading_session_now() -> bool {
    let now_ny = Utc::now().with_timezone(&New_York);
    let weekday = now_ny.weekday();
    if matches!(weekday, Weekday::Sat | Weekday::Sun) {
        return false;
    }

    let minutes = now_ny.hour() * 60 + now_ny.minute();
    let open_minutes = 9 * 60 + 30;
    let close_minutes = 16 * 60;
    minutes >= open_minutes && minutes < close_minutes
}

struct PolygonWsClient {
    latest_prices: std::sync::Arc<RwLock<HashMap<String, TimedPrice>>>,
    subscriptions: std::sync::Arc<RwLock<HashSet<String>>>,
    subscribe_tx: mpsc::UnboundedSender<Vec<String>>,
}

#[derive(Clone, Copy, Debug)]
struct TimedPrice {
    price: f64,
    timestamp_ms: i64,
    source: &'static str,
}

#[derive(Clone, Debug)]
pub struct LivePrice {
    pub price: f64,
    pub exchange_ts_ms: i64,
    pub source: String,
}

#[derive(Serialize)]
struct PolygonWsAuthReq {
    action: &'static str,
    params: String,
}

#[derive(Serialize)]
struct PolygonWsSubscribeReq {
    action: &'static str,
    params: String,
}

#[derive(Deserialize, Debug)]
struct PolygonWsEvent {
    ev: Option<String>,
    sym: Option<String>,
    p: Option<f64>,
    t: Option<i64>,
    c: Option<f64>,
    s: Option<i64>,
    e: Option<i64>,
}

fn polygon_ws_client() -> Option<&'static PolygonWsClient> {
    let api_key = polygon_api_key()?;
    Some(POLYGON_WS_CLIENT.get_or_init(|| {
        let latest_prices = std::sync::Arc::new(RwLock::new(HashMap::new()));
        let subscriptions = std::sync::Arc::new(RwLock::new(HashSet::new()));
        let (subscribe_tx, subscribe_rx) = mpsc::unbounded_channel::<Vec<String>>();

        let latest_prices_bg = latest_prices.clone();
        let subscriptions_bg = subscriptions.clone();
        tokio::spawn(async move {
            run_polygon_ws_loop(api_key, latest_prices_bg, subscriptions_bg, subscribe_rx).await;
        });

        PolygonWsClient {
            latest_prices,
            subscriptions,
            subscribe_tx,
        }
    }))
}

async fn run_polygon_ws_loop(
    api_key: String,
    latest_prices: std::sync::Arc<RwLock<HashMap<String, TimedPrice>>>,
    subscriptions: std::sync::Arc<RwLock<HashSet<String>>>,
    mut subscribe_rx: mpsc::UnboundedReceiver<Vec<String>>,
) {
    let custom_ws = std::env::var("POLYGON_WS_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let ws_candidates = if let Some(url) = custom_ws {
        vec![url]
    } else {
        vec![
            "wss://delayed.massive.com/stocks".to_string(),
            "wss://socket.massive.com/stocks".to_string(),
        ]
    };

    loop {
        let mut connected = None;
        for ws_url in &ws_candidates {
            match connect_async(ws_url).await {
                Ok(v) => {
                    connected = Some(v);
                    break;
                }
                Err(err) => {
                    warn!("Polygon/Massive WebSocket connect failed {}: {}", ws_url, err);
                }
            }
        }

        let (ws_stream, _) = match connected {
            Some(v) => v,
            None => {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        info!("Polygon WebSocket connected");
        {
            let mut ws_connected = polygon_ws_connected_cell().write().await;
            *ws_connected = true;
        }
        ws_diag_set_connected(true).await;
        let (mut write, mut read) = ws_stream.split();

        let auth = PolygonWsAuthReq {
            action: "auth",
            params: api_key.clone(),
        };
        if let Ok(payload) = serde_json::to_string(&auth) {
            if let Err(err) = write.send(Message::Text(payload)).await {
                warn!("Polygon WebSocket auth send failed: {}", err);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }

        let initial_symbols: Vec<String> = subscriptions.read().await.iter().cloned().collect();
        if !initial_symbols.is_empty() {
            let params = initial_symbols
                .iter()
                .flat_map(|s| [format!("A.{}", s), format!("AM.{}", s)])
                .collect::<Vec<_>>()
                .join(",");
            info!("Polygon WS initial subscribe: {}", params);
            let sub = PolygonWsSubscribeReq {
                action: "subscribe",
                params,
            };
            if let Ok(payload) = serde_json::to_string(&sub) {
                let _ = write.send(Message::Text(payload)).await;
            }
        }

        let mut disconnected = false;

        while !disconnected {
            tokio::select! {
                maybe_syms = subscribe_rx.recv() => {
                    match maybe_syms {
                        Some(symbols) => {
                            if symbols.is_empty() {
                                continue;
                            }
                            let params = symbols
                                .iter()
                                .flat_map(|s| [format!("A.{}", s), format!("AM.{}", s)])
                                .collect::<Vec<_>>()
                                .join(",");
                            info!("Polygon WS incremental subscribe: {}", params);
                            let sub = PolygonWsSubscribeReq {
                                action: "subscribe",
                                params,
                            };
                            if let Ok(payload) = serde_json::to_string(&sub) {
                                if let Err(err) = write.send(Message::Text(payload)).await {
                                    warn!("Polygon WebSocket subscribe send failed: {}", err);
                                    disconnected = true;
                                }
                            }
                        }
                        None => {
                            disconnected = true;
                        }
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            ws_diag_mark_text().await;
                            handle_polygon_ws_text(&text, &latest_prices).await;
                        }
                        Some(Ok(Message::Binary(_))) => {}
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            disconnected = true;
                        }
                        Some(Err(err)) => {
                            warn!("Polygon WebSocket read error: {}", err);
                            disconnected = true;
                        }
                        None => {
                            disconnected = true;
                        }
                        _ => {}
                    }
                }
            }
        }

        warn!("Polygon WebSocket disconnected; reconnecting...");
        {
            let mut ws_connected = polygon_ws_connected_cell().write().await;
            *ws_connected = false;
        }
        ws_diag_set_connected(false).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn handle_polygon_ws_text(
    text: &str,
    latest_prices: &std::sync::Arc<RwLock<HashMap<String, TimedPrice>>>,
) {
    let parsed = serde_json::from_str::<serde_json::Value>(text);
    let value = match parsed {
        Ok(v) => v,
        Err(err) => {
            ws_diag_mark_parse_failure(&format!("invalid json: {}", err)).await;
            return;
        }
    };

    let mut status_count: u64 = 0;
    if let Some(arr) = value.as_array() {
        status_count = arr
            .iter()
            .filter(|item| item.get("status").is_some())
            .count() as u64;
    } else if value.get("status").is_some() {
        status_count = 1;
    }
    ws_diag_mark_status(status_count).await;

    let events: Vec<PolygonWsEvent> = if value.is_array() {
        match serde_json::from_value::<Vec<PolygonWsEvent>>(value) {
            Ok(batch) => batch,
            Err(err) => {
                ws_diag_mark_parse_failure(&format!("invalid event payload: {}", err)).await;
                return;
            }
        }
    } else if value.is_object() {
        match serde_json::from_value::<PolygonWsEvent>(value) {
            Ok(single) => vec![single],
            Err(err) => {
                ws_diag_mark_parse_failure(&format!("invalid event payload: {}", err)).await;
                return;
            }
        }
    } else {
        return;
    };

    if events.is_empty() {
        return;
    }

    let mut accepted_events: u64 = 0;
    let mut dropped_events: u64 = 0;
    let mut last_accepted_ts_ms: Option<i64> = None;

    let mut latest = latest_prices.write().await;
    for event in events {
        let ev = event.ev.as_deref();
        if ev != Some("A") && ev != Some("AM") {
            continue;
        }

        let price_opt = event
            .c
            .or(event.p)
            .filter(|price| valid_live_price(*price));
        let ts_ms = event
            .e
            .or(event.s)
            .or(event.t)
            .map(normalize_polygon_timestamp_to_ms)
            .unwrap_or_else(now_ts_ms);

        if let (Some(sym), Some(price)) = (event.sym, price_opt) {
            accepted_events = accepted_events.saturating_add(1);
            last_accepted_ts_ms = Some(ts_ms);
            let source = if ev == Some("A") {
                "Polygon-WS-SecondAgg"
            } else {
                "Polygon-WS-MinuteAgg"
            };

            latest.insert(
                sym.to_uppercase(),
                TimedPrice {
                    price,
                    timestamp_ms: ts_ms,
                    source,
                },
            );
        } else {
            dropped_events = dropped_events.saturating_add(1);
        }
    }
    drop(latest);

    ws_diag_mark_data_batch(accepted_events, dropped_events, last_accepted_ts_ms).await;
}

async fn polygon_ws_subscribe_symbols(symbols: &[String]) {
    let Some(client) = polygon_ws_client() else {
        return;
    };

    let mut to_subscribe = Vec::new();
    {
        let mut set = client.subscriptions.write().await;
        for symbol in symbols {
            let upper = symbol.trim().to_uppercase();
            if upper.is_empty() {
                continue;
            }
            if set.insert(upper.clone()) {
                to_subscribe.push(upper);
            }
        }
    }

    if !to_subscribe.is_empty() {
        let _ = client.subscribe_tx.send(to_subscribe);
    }
}

async fn polygon_ws_latest_price(symbol: &str) -> Option<TimedPrice> {
    let client = polygon_ws_client()?;
    let map = client.latest_prices.read().await;
    map.get(&symbol.to_uppercase()).copied()
}

/// Represents a single candlestick data point (OHLCV).
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct Candle {
    pub date: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Holds historical stock data for a specific symbol.
#[derive(Clone, Debug)]
pub struct StockData {
    pub symbol: String,
    pub history: Vec<Candle>,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooChartResponse {
    chart: YahooChart,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooChart {
    result: Vec<YahooResult>,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooResult {
    timestamp: Vec<i64>,
    indicators: YahooIndicators,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooIndicators {
    quote: Vec<YahooQuote>,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooQuote {
    open: Vec<Option<f64>>,
    high: Vec<Option<f64>>,
    low: Vec<Option<f64>>,
    close: Vec<Option<f64>>,
    volume: Vec<Option<f64>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonAggsResponse {
    status: Option<String>,
    results: Option<Vec<PolygonAgg>>,
    results_count: Option<usize>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonAgg {
    #[serde(rename = "t")]
    timestamp_ms: i64,
    #[serde(rename = "o")]
    open: f64,
    #[serde(rename = "h")]
    high: f64,
    #[serde(rename = "l")]
    low: f64,
    #[serde(rename = "c")]
    close: f64,
    #[serde(rename = "v")]
    volume: f64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonSnapshotResponse {
    ticker: Option<PolygonSnapshotTicker>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonSnapshotTicker {
    #[serde(rename = "lastTrade")]
    last_trade: Option<PolygonLastTrade>,
    day: Option<PolygonDayBar>,
    #[serde(rename = "prevDay")]
    prev_day: Option<PolygonDayBar>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonLastTrade {
    p: Option<f64>,
    t: Option<i64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonDayBar {
    c: Option<f64>,
}

fn normalize_polygon_timestamp_to_ms(ts: i64) -> i64 {
    if ts > 10_000_000_000_000_000 {
        ts / 1_000_000
    } else if ts > 10_000_000_000_000 {
        ts / 1_000
    } else {
        ts
    }
}

fn valid_live_price(price: f64) -> bool {
    price.is_finite() && price > 0.0
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonRangeCache {
    aggs: PolygonAggsResponse,
    indicators: Option<PolygonIndicatorsSnapshot>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
struct PolygonIndicatorResponse {
    results: Option<PolygonIndicatorResult>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
struct PolygonIndicatorResult {
    values: Option<Vec<PolygonIndicatorValue>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PolygonIndicatorValue {
    value: Option<f64>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
struct PolygonIndicatorsSnapshot {
    sma20: Option<f64>,
    ema20: Option<f64>,
    rsi14: Option<f64>,
}

fn parse_days_from_range(range: &str) -> i64 {
    let trimmed = range.trim().to_ascii_lowercase();
    if trimmed == "max" {
        return 365 * 5;
    }

    let parse_num = |s: &str| s.parse::<i64>().ok();

    if let Some(v) = trimmed.strip_suffix("y").and_then(parse_num) {
        return (v * 365).max(1);
    }
    if let Some(v) = trimmed.strip_suffix("mo").and_then(parse_num) {
        return (v * 30).max(1);
    }
    if let Some(v) = trimmed.strip_suffix("m").and_then(parse_num) {
        return v.max(1);
    }
    if let Some(v) = trimmed.strip_suffix("d").and_then(parse_num) {
        return v.max(1);
    }

    365
}

fn polygon_api_key() -> Option<String> {
    std::env::var("POLYGON_API_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn polygon_retry_attempts() -> usize {
    std::env::var("DIFFSTOCK_POLYGON_RETRY_ATTEMPTS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .map(|v| v.clamp(1, 8))
        .unwrap_or(4)
}

fn batch_fetch_delay_ms() -> u64 {
    std::env::var("DIFFSTOCK_BATCH_FETCH_DELAY_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|v| v.clamp(0, 5_000))
        .unwrap_or(350)
}

async fn polygon_retry_sleep(attempt: usize) {
    let millis = (250_u64 * (attempt as u64)).min(1500);
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
}

async fn fetch_polygon_aggs(symbol: &str, range: &str, api_key: &str) -> Result<PolygonAggsResponse> {
    let mut days = parse_days_from_range(range);
    if days > 365 * 5 {
        warn!(
            "Polygon plan supports up to ~5y history; clamping requested range {} for {} to 5y",
            range,
            symbol
        );
        days = 365 * 5;
    }

    let end = Utc::now().date_naive();
    let start = (Utc::now() - Duration::days(days)).date_naive();
    let url = format!(
        "https://api.polygon.io/v2/aggs/ticker/{}/range/1/day/{}/{}?adjusted=true&sort=asc&limit=50000&apiKey={}",
        symbol,
        start.format("%Y-%m-%d"),
        end.format("%Y-%m-%d"),
        api_key
    );

    let attempts = 5usize;
    let client = reqwest::Client::new();
    let mut last_err: Option<anyhow::Error> = None;
    let mut saw_non_timeout_error = false;

    for attempt in 1..=attempts {
        match client
            .get(&url)
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
        {
            Ok(resp) => match resp.error_for_status() {
                Ok(ok_resp) => match ok_resp.json::<PolygonAggsResponse>().await {
                    Ok(parsed) => return Ok(parsed),
                    Err(err) => {
                        if !err.is_timeout() {
                            saw_non_timeout_error = true;
                        }
                        last_err = Some(err.into());
                    }
                },
                Err(err) => {
                    if !err.is_timeout() {
                        saw_non_timeout_error = true;
                    }
                    last_err = Some(err.into());
                }
            },
            Err(err) => {
                if !err.is_timeout() {
                    saw_non_timeout_error = true;
                }
                last_err = Some(err.into());
            }
        }

        if attempt < attempts {
            warn!(
                "Polygon history fetch retry for {} ({}/{})",
                symbol,
                attempt,
                attempts
            );
            polygon_retry_sleep(attempt).await;
        }
    }

    if !saw_non_timeout_error {
        return Err(anyhow::anyhow!(
            "Polygon history request timed out after {} attempts for {}",
            attempts,
            symbol
        ));
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Polygon history fetch failed for {}", symbol)))
}

async fn fetch_polygon_indicator(
    client: &reqwest::Client,
    symbol: &str,
    api_key: &str,
    indicator: &str,
    window: usize,
) -> Option<f64> {
    let url = format!(
        "https://api.polygon.io/v1/indicators/{}/{}?timespan=day&window={}&series_type=close&order=desc&limit=1&apiKey={}",
        indicator, symbol, window, api_key
    );

    let response = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;

    let parsed = response.json::<PolygonIndicatorResponse>().await.ok()?;
    parsed
        .results
        .and_then(|r| r.values)
        .and_then(|values| values.into_iter().find_map(|v| v.value))
}

async fn fetch_polygon_indicators_snapshot(symbol: &str, api_key: &str) -> PolygonIndicatorsSnapshot {
    let client = reqwest::Client::new();
    PolygonIndicatorsSnapshot {
        sma20: fetch_polygon_indicator(&client, symbol, api_key, "sma", 20).await,
        ema20: fetch_polygon_indicator(&client, symbol, api_key, "ema", 20).await,
        rsi14: fetch_polygon_indicator(&client, symbol, api_key, "rsi", 14).await,
    }
}

fn polygon_aggs_to_stock_data(symbol: &str, response: &PolygonAggsResponse) -> Result<StockData> {
    let results = response
        .results
        .as_ref()
        .ok_or(anyhow::anyhow!("Polygon response missing results for {}", symbol))?;

    if results.is_empty() {
        return Err(anyhow::anyhow!("No Polygon OHLC data found for {}", symbol));
    }

    let history = results
        .iter()
        .map(|bar| Candle {
            date: Utc.timestamp_millis_opt(bar.timestamp_ms).single().unwrap_or_else(Utc::now),
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
        })
        .collect::<Vec<_>>();

    Ok(StockData {
        symbol: symbol.to_uppercase(),
        history,
    })
}

async fn fetch_polygon_range_with_cache(symbol: &str, range: &str, api_key: &str) -> Result<StockData> {
    let cache_dir = crate::config::project_root_path().join(".cache");
    if !cache_dir.exists() {
        std::fs::create_dir_all(&cache_dir)?;
    }

    let cache_file = cache_dir.join(format!("{}_{}_polygon.json", symbol, range));

    let cache_payload: PolygonRangeCache = if cache_file.exists() {
        let metadata = std::fs::metadata(&cache_file)?;
        let modified = metadata.modified()?;
        let age = std::time::SystemTime::now().duration_since(modified)?;

        if age.as_secs() < 86400 {
            info!("Loading {} from Polygon cache...", symbol);
            let file = std::fs::File::open(&cache_file)?;
            let reader = std::io::BufReader::new(file);
            serde_json::from_reader(reader)?
        } else {
            info!("Polygon cache expired for {}, fetching...", symbol);
            match fetch_polygon_aggs(symbol, range, api_key).await {
                Ok(aggs) => {
                    let indicators = Some(fetch_polygon_indicators_snapshot(symbol, api_key).await);
                    let payload = PolygonRangeCache { aggs, indicators };
                    let file = std::fs::File::create(&cache_file)?;
                    let writer = std::io::BufWriter::new(file);
                    serde_json::to_writer(writer, &payload)?;
                    payload
                }
                Err(fetch_err) => {
                    warn!(
                        "Polygon refresh failed for {} ({}), using stale cache",
                        symbol,
                        fetch_err
                    );
                    let file = std::fs::File::open(&cache_file)?;
                    let reader = std::io::BufReader::new(file);
                    serde_json::from_reader(reader)?
                }
            }
        }
    } else {
        info!("Polygon cache miss for {}, fetching...", symbol);
        let aggs = fetch_polygon_aggs(symbol, range, api_key).await?;
        let indicators = Some(fetch_polygon_indicators_snapshot(symbol, api_key).await);
        let payload = PolygonRangeCache { aggs, indicators };
        let file = std::fs::File::create(&cache_file)?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer(writer, &payload)?;
        payload
    };

    if let Some(indicators) = &cache_payload.indicators {
        info!(
            "Polygon indicators for {} => SMA20={:?}, EMA20={:?}, RSI14={:?}",
            symbol,
            indicators.sma20,
            indicators.ema20,
            indicators.rsi14
        );
    }

    let data = polygon_aggs_to_stock_data(symbol, &cache_payload.aggs)?;
    set_live_data_source("Polygon-History").await;
    Ok(data)
}

async fn fetch_latest_price_1m_polygon(symbol: &str, api_key: &str) -> Result<TimedPrice> {
    let end = Utc::now().date_naive();
    let start = (Utc::now() - Duration::days(3)).date_naive();
    let url = format!(
        "https://api.polygon.io/v2/aggs/ticker/{}/range/1/minute/{}/{}?adjusted=true&sort=desc&limit=1&apiKey={}",
        symbol,
        start.format("%Y-%m-%d"),
        end.format("%Y-%m-%d"),
        api_key
    );

    let attempts = polygon_retry_attempts();
    let client = reqwest::Client::new();
    let mut last_err: Option<anyhow::Error> = None;
    let mut parsed_opt: Option<PolygonAggsResponse> = None;

    for attempt in 1..=attempts {
        match client
            .get(&url)
            .timeout(std::time::Duration::from_secs(12))
            .send()
            .await
        {
            Ok(resp) => match resp.error_for_status() {
                Ok(ok_resp) => match ok_resp.json::<PolygonAggsResponse>().await {
                    Ok(parsed) => {
                        parsed_opt = Some(parsed);
                        break;
                    }
                    Err(err) => last_err = Some(err.into()),
                },
                Err(err) => last_err = Some(err.into()),
            },
            Err(err) => last_err = Some(err.into()),
        }

        if attempt < attempts {
            polygon_retry_sleep(attempt).await;
        }
    }

    let parsed = parsed_opt.ok_or_else(|| {
        last_err.unwrap_or_else(|| anyhow::anyhow!("Polygon minute aggregate fetch failed for {}", symbol))
    })?;

    let latest_bar = parsed
        .results
        .as_ref()
        .and_then(|bars| bars.first())
        .ok_or(anyhow::anyhow!("No minute aggregate data for {} from Polygon", symbol))?;

    if !valid_live_price(latest_bar.close) {
        return Err(anyhow::anyhow!(
            "Invalid minute aggregate price for {} from Polygon: {}",
            symbol,
            latest_bar.close
        ));
    }

    Ok(TimedPrice {
        price: latest_bar.close,
        timestamp_ms: latest_bar.timestamp_ms,
        source: "Polygon-MinuteAgg",
    })
}

async fn fetch_latest_price_snapshot_polygon(symbol: &str, api_key: &str) -> Result<TimedPrice> {
    let url = format!(
        "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{}?apiKey={}",
        symbol,
        api_key
    );

    let attempts = polygon_retry_attempts();
    let client = reqwest::Client::new();
    let mut last_err: Option<anyhow::Error> = None;
    let mut parsed_opt: Option<PolygonSnapshotResponse> = None;

    for attempt in 1..=attempts {
        match client
            .get(&url)
            .timeout(std::time::Duration::from_secs(12))
            .send()
            .await
        {
            Ok(resp) => match resp.error_for_status() {
                Ok(ok_resp) => match ok_resp.json::<PolygonSnapshotResponse>().await {
                    Ok(parsed) => {
                        parsed_opt = Some(parsed);
                        break;
                    }
                    Err(err) => last_err = Some(err.into()),
                },
                Err(err) => last_err = Some(err.into()),
            },
            Err(err) => last_err = Some(err.into()),
        }

        if attempt < attempts {
            polygon_retry_sleep(attempt).await;
        }
    }

    let parsed = parsed_opt.ok_or_else(|| {
        last_err.unwrap_or_else(|| anyhow::anyhow!("Polygon snapshot fetch failed for {}", symbol))
    })?;

    let ticker = parsed
        .ticker
        .ok_or(anyhow::anyhow!("No snapshot ticker data for {}", symbol))?;

    if let Some(last_trade) = ticker.last_trade {
        if let Some(price) = last_trade.p {
            if !valid_live_price(price) {
                return Err(anyhow::anyhow!(
                    "Invalid snapshot lastTrade price for {} from Polygon: {}",
                    symbol,
                    price
                ));
            }
            let ts_ms = last_trade.t.map(normalize_polygon_timestamp_to_ms).unwrap_or_else(|| Utc::now().timestamp_millis());
            return Ok(TimedPrice { price, timestamp_ms: ts_ms, source: "Polygon-Snapshot" });
        }
    }

    let fallback_price = ticker
        .day
        .and_then(|d| d.c)
        .filter(|p| valid_live_price(*p))
        .or_else(|| ticker.prev_day.and_then(|d| d.c).filter(|p| valid_live_price(*p)))
        .ok_or(anyhow::anyhow!("No minute aggregate data for {} from Polygon", symbol))?;

    Ok(TimedPrice {
        price: fallback_price,
        timestamp_ms: Utc::now().timestamp_millis(),
        source: "Polygon-Snapshot",
    })
}

async fn fetch_latest_price_1m_yahoo(symbol: &str) -> Result<(f64, i64)> {
    let urls = [
        format!(
            "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1m&range=1d",
            symbol
        ),
        format!(
            "https://query2.finance.yahoo.com/v8/finance/chart/{}?interval=1m&range=1d",
            symbol
        ),
    ];

    let client = reqwest::Client::new();
    let max_attempts = 3;
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=max_attempts {
        for url in &urls {
            let response = match client
                .get(url)
                .header("User-Agent", "Mozilla/5.0")
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(error) => {
                    last_error = Some(error.into());
                    continue;
                }
            };

            match response.json::<YahooChartResponse>().await {
                Ok(parsed) => {
                    let result = parsed
                        .chart
                        .result
                        .first()
                        .ok_or(anyhow::anyhow!("No chart result for {}", symbol))?;
                    let quote = result
                        .indicators
                        .quote
                        .first()
                        .ok_or(anyhow::anyhow!("No quote result for {}", symbol))?;

                    for idx in (0..quote.close.len()).rev() {
                        let close = quote.close.get(idx).and_then(|v| *v);
                        let ts = result.timestamp.get(idx).copied();
                        if let (Some(latest), Some(ts_sec)) = (close, ts) {
                            if valid_live_price(latest) {
                                set_live_data_source("Yfinance").await;
                                return Ok((latest, ts_sec * 1000));
                            }
                        }
                    }

                    last_error = Some(anyhow::anyhow!("No valid 1m close for {}", symbol));
                }
                Err(error) => {
                    last_error = Some(error.into());
                }
            }
        }

        if attempt < max_attempts {
            warn!(
                "1m Yahoo price fetch failed for {} (attempt {}/{}), retrying...",
                symbol,
                attempt,
                max_attempts
            );
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to fetch 1m Yahoo price for {}", symbol)))
}

/// Fetches historical stock data from Yahoo Finance.
///
/// # Arguments
/// * `symbol` - The stock ticker symbol (e.g., "AAPL").
/// * `range` - The time range to fetch (e.g., "1y", "5y").
pub async fn fetch_range_with_source(symbol: &str, range: &str) -> Result<(StockData, String)> {
    if matches!(configured_data_provider_mode(), DataProviderMode::Polygon) {
        let api_key = polygon_api_key()
            .ok_or(anyhow::anyhow!("DIFFSTOCK_DATA_PROVIDER=polygon but POLYGON_API_KEY is missing"))?;
        match fetch_polygon_range_with_cache(symbol, range, &api_key).await {
            Ok(data) => return Ok((data, "Polygon-History".to_string())),
            Err(err) => {
                let err_text = err.to_string();
                if err_text.contains("Polygon history request timed out after") {
                    warn!(
                        "Polygon history timeout for {} (range={}): {}. Falling back to Yahoo history.",
                        symbol,
                        range,
                        err
                    );
                } else {
                    return Err(anyhow::anyhow!("Polygon range fetch failed for {}: {}", symbol, err));
                }
            }
        }
    }

    let cache_dir = crate::config::project_root_path().join(".cache");
    if !cache_dir.exists() {
        std::fs::create_dir_all(&cache_dir)?;
    }
    
    let cache_file = cache_dir.join(format!("{}_{}.json", symbol, range));
    
    let response: YahooChartResponse = if cache_file.exists() {
        // Check if cache is fresh (e.g. < 24 hours)
        let metadata = std::fs::metadata(&cache_file)?;
        let modified = metadata.modified()?;
        let age = std::time::SystemTime::now().duration_since(modified)?;
        
        if age.as_secs() < 86400 {
            info!("Loading {} from cache...", symbol);
            let file = std::fs::File::open(&cache_file)?;
            let reader = std::io::BufReader::new(file);
            serde_json::from_reader(reader)?
        } else {
            info!("Cache expired for {}, fetching...", symbol);
            fetch_from_api(symbol, range, &cache_file).await?
        }
    } else {
        info!("Cache miss for {}, fetching...", symbol);
        fetch_from_api(symbol, range, &cache_file).await?
    };

    let result = response.chart.result.first().ok_or(anyhow::anyhow!("No data found"))?;
    
    let mut history = Vec::new();
    let quotes = &result.indicators.quote[0];
    
    for (i, &timestamp) in result.timestamp.iter().enumerate() {
        if let (Some(open), Some(high), Some(low), Some(close), Some(volume)) = (
            quotes.open[i],
            quotes.high[i],
            quotes.low[i],
            quotes.close[i],
            quotes.volume[i],
        ) {
            history.push(Candle {
                date: Utc.timestamp_opt(timestamp, 0).unwrap(),
                open,
                high,
                low,
                close,
                volume,
            });
        }
    }
    
    let data = StockData {
        symbol: symbol.to_uppercase(),
        history,
    };
    set_live_data_source("Yfinance-History").await;
    Ok((data, "Yfinance-History".to_string()))
}

pub async fn fetch_range(symbol: &str, range: &str) -> Result<StockData> {
    let (data, source) = fetch_range_with_source(symbol, range).await?;
    set_live_data_source(&source).await;
    Ok(data)
}

pub async fn fetch_ranges_prefetch(
    symbols: &[String],
    range: &str,
) -> Result<HashMap<String, StockData>> {
    let mut out: HashMap<String, StockData> = HashMap::new();
    let delay_ms = batch_fetch_delay_ms();
    let provider = configured_data_provider_mode().as_str();

    for (idx, raw_symbol) in symbols.iter().enumerate() {
        let symbol = raw_symbol.trim().to_uppercase();
        if symbol.is_empty() {
            return Err(anyhow::anyhow!(
                "historical prefetch failed before computation: symbol=<empty>, provider={}, range={}, reason=empty symbol",
                provider,
                range
            ));
        }
        if out.contains_key(&symbol) {
            continue;
        }

        let (data, source) = fetch_range_with_source(&symbol, range)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "historical prefetch failed before computation: symbol={}, provider={}, range={}, reason={}",
                    symbol,
                    provider,
                    range,
                    e
                )
            })?;
        set_live_data_source(&source).await;
        out.insert(symbol.clone(), data);

        if idx + 1 < symbols.len() && delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
    }

    Ok(out)
}

async fn fetch_from_api(symbol: &str, range: &str, cache_path: &std::path::Path) -> Result<YahooChartResponse> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?range={}&interval=1d",
        symbol, range
    );
    
    let mut attempts = 0;
    let max_attempts = 3;
    
    loop {
        attempts += 1;
        match reqwest::Client::new()
            .get(&url)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await 
        {
            Ok(resp) => {
                match resp.json::<YahooChartResponse>().await {
                    Ok(resp_json) => {
                        // Save to cache
                        let file = std::fs::File::create(cache_path)?;
                        let writer = std::io::BufWriter::new(file);
                        serde_json::to_writer(writer, &resp_json)?;
                        
                        return Ok(resp_json);
                    }
                    Err(e) => {
                        if attempts >= max_attempts {
                            return Err(e.into());
                        }
                        warn!("Failed to parse JSON for {} (attempt {}/{}): {}", symbol, attempts, max_attempts, e);
                    }
                }
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(e.into());
                }
                warn!("Failed to fetch data for {} (attempt {}/{}): {}", symbol, attempts, max_attempts, e);
            }
        }
        
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Fetches the latest 1-minute close price from Yahoo Finance.
///
/// Uses `interval=1m` and `range=1d`, then returns the most recent non-null close.
pub async fn fetch_latest_price_1m(symbol: &str) -> Result<f64> {
    Ok(fetch_latest_price_with_meta(symbol).await?.price)
}

pub async fn fetch_latest_prices_with_meta_prefetch(
    symbols: &[String],
) -> Result<HashMap<String, LivePrice>> {
    let mut normalized: Vec<String> = symbols
        .iter()
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    normalized.sort();
    normalized.dedup();

    if normalized.is_empty() {
        return Err(anyhow::anyhow!("live prefetch failed before computation: symbol list is empty"));
    }

    if matches!(configured_data_provider_mode(), DataProviderMode::Polygon) {
        polygon_ws_subscribe_symbols(&normalized).await;
    }

    let delay_ms = batch_fetch_delay_ms();
    let provider = configured_data_provider_mode().as_str();
    let started = Instant::now();
    let total_symbols = normalized.len();
    let mut out: HashMap<String, LivePrice> = HashMap::new();
    let mut batch_failed: Option<String> = None;

    for (idx, symbol) in normalized.iter().enumerate() {
        let quote = match fetch_latest_price_with_meta(symbol).await {
            Ok(q) => q,
            Err(e) => {
                batch_failed = Some(format!(
                    "live batch prefetch failed: symbol={}, provider={}, reason={}",
                    symbol,
                    provider,
                    e
                ));
                break;
            }
        };
        out.insert(symbol.clone(), quote);

        if idx + 1 < normalized.len() && delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
    }

    if batch_failed.is_none() {
        mark_live_prefetch_success(
            total_symbols,
            out.len(),
            started.elapsed().as_millis() as u64,
            "batch",
            false,
            None,
        )
        .await;
        return Ok(out);
    }

    let batch_err = batch_failed.unwrap_or_else(|| "unknown batch prefetch failure".to_string());
    warn!(
        "{}; degrading to per-symbol live fetch for this cycle",
        batch_err
    );

    out.clear();
    let mut fallback_errors: Vec<String> = Vec::new();
    for symbol in &normalized {
        match fetch_latest_price_with_meta(symbol).await {
            Ok(q) => {
                out.insert(symbol.clone(), q);
            }
            Err(e) => {
                fallback_errors.push(format!("{}: {}", symbol, e));
            }
        }
    }

    if out.is_empty() {
        let msg = format!(
            "{}; fallback single-symbol fetch also failed for all symbols: {}",
            batch_err,
            fallback_errors.join(" | ")
        );
        mark_live_prefetch_failure(
            total_symbols,
            started.elapsed().as_millis() as u64,
            "fallback-single",
            &msg,
        )
        .await;
        return Err(anyhow::anyhow!(msg));
    }

    let warning = if fallback_errors.is_empty() {
        Some(format!("{}; fallback-single recovered all symbols", batch_err))
    } else {
        Some(format!(
            "{}; fallback-single partial success, missing {}/{} symbols: {}",
            batch_err,
            total_symbols.saturating_sub(out.len()),
            total_symbols,
            fallback_errors.join(" | ")
        ))
    };
    mark_live_prefetch_success(
        total_symbols,
        out.len(),
        started.elapsed().as_millis() as u64,
        "fallback-single",
        true,
        warning.as_deref(),
    )
    .await;

    Ok(out)
}

pub async fn fetch_latest_prices_1m_prefetch(symbols: &[String]) -> Result<HashMap<String, f64>> {
    let quotes = fetch_latest_prices_with_meta_prefetch(symbols).await?;
    let mut prices = HashMap::new();
    for (symbol, quote) in quotes {
        prices.insert(symbol, quote.price);
    }
    Ok(prices)
}

pub async fn fetch_latest_prices_with_meta_ws_only(
    symbols: &[String],
) -> Result<HashMap<String, LivePrice>> {
    let mut normalized: Vec<String> = symbols
        .iter()
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    normalized.sort();
    normalized.dedup();

    if normalized.is_empty() {
        return Err(anyhow::anyhow!("ws-only live quotes failed: symbol list is empty"));
    }

    if !matches!(configured_data_provider_mode(), DataProviderMode::Polygon) {
        return fetch_latest_prices_with_meta_prefetch(&normalized).await;
    }

    polygon_ws_subscribe_symbols(&normalized).await;

    let mut out: HashMap<String, LivePrice> = HashMap::new();
    for symbol in &normalized {
        if let Some(price) = polygon_ws_latest_price(symbol).await {
            set_live_data_source(price.source).await;
            out.insert(
                symbol.clone(),
                LivePrice {
                    price: price.price,
                    exchange_ts_ms: price.timestamp_ms,
                    source: price.source.to_string(),
                },
            );
        }
    }

    Ok(out)
}

pub async fn fetch_latest_price_with_meta(symbol: &str) -> Result<LivePrice> {
    match configured_data_provider_mode() {
        DataProviderMode::Polygon => {
            let api_key = polygon_api_key()
                .ok_or(anyhow::anyhow!("DIFFSTOCK_DATA_PROVIDER=polygon but POLYGON_API_KEY is missing"))?;

            polygon_ws_subscribe_symbols(&[symbol.to_uppercase()]).await;
            let prefer_ws = if ws_priority_rth_only() {
                is_us_regular_trading_session_now()
            } else {
                true
            };
            let rounds = polygon_retry_attempts();
            let mut last_error: Option<anyhow::Error> = None;

            let ws_candidate0 = polygon_ws_latest_price(symbol).await;
            let ws_healthy_now = ws_recently_healthy().await;
            if ws_healthy_now {
                ws_reset_timeout_state("recent ws event").await;
            } else {
                let strikes = ws_record_timeout_strike_if_needed().await;
                warn!(
                    "Polygon WS appears stale for {}; timeout strikes={}/{}",
                    symbol,
                    strikes,
                    WS_TIMEOUT_FAILOVER_THRESHOLD
                );
            }

            if prefer_ws && !ws_failover_active().await {
                if let Some(price) = ws_candidate0 {
                    set_live_data_source(price.source).await;
                    return Ok(LivePrice {
                        price: price.price,
                        exchange_ts_ms: price.timestamp_ms,
                        source: price.source.to_string(),
                    });
                }
            }

            for round in 1..=rounds {
                let mut candidates: Vec<TimedPrice> = Vec::new();

                let ws_candidate = polygon_ws_latest_price(symbol).await;
                if ws_failover_active().await {
                    if let Some(price) = ws_candidate {
                        if ws_recently_healthy().await {
                            ws_reset_timeout_state("ws data available during failover").await;
                            set_live_data_source(price.source).await;
                            return Ok(LivePrice {
                                price: price.price,
                                exchange_ts_ms: price.timestamp_ms,
                                source: price.source.to_string(),
                            });
                        }
                    }
                } else if prefer_ws {
                    if let Some(price) = ws_candidate {
                        set_live_data_source(price.source).await;
                        return Ok(LivePrice {
                            price: price.price,
                            exchange_ts_ms: price.timestamp_ms,
                            source: price.source.to_string(),
                        });
                    }
                } else if let Some(price) = ws_candidate {
                    candidates.push(price);
                }

                match fetch_latest_price_snapshot_polygon(symbol, &api_key).await {
                    Ok(price) => candidates.push(price),
                    Err(e) => {
                        last_error = Some(e);
                    }
                }

                match fetch_latest_price_1m_polygon(symbol, &api_key).await {
                    Ok(price) => candidates.push(price),
                    Err(e) => {
                        last_error = Some(e);
                    }
                }

                if let Some(best) = candidates.into_iter().max_by_key(|c| c.timestamp_ms) {
                    set_live_data_source(best.source).await;
                    return Ok(LivePrice {
                        price: best.price,
                        exchange_ts_ms: best.timestamp_ms,
                        source: best.source.to_string(),
                    });
                }

                if round < rounds {
                    warn!(
                        "Polygon live fetch round {}/{} produced no candidate for {}, retrying",
                        round,
                        rounds,
                        symbol
                    );
                    polygon_retry_sleep(round).await;
                }
            }

            return Err(last_error.unwrap_or_else(|| {
                anyhow::anyhow!(
                    "Polygon live price fetch failed for {} after {} rounds",
                    symbol,
                    rounds
                )
            }));
        }
        DataProviderMode::Yfinance => {
            let (price, ts_ms) = fetch_latest_price_1m_yahoo(symbol).await?;
            Ok(LivePrice {
                price,
                exchange_ts_ms: ts_ms,
                source: "Yfinance".to_string(),
            })
        }
    }
}

impl StockData {
    pub async fn fetch(symbol: &str) -> Result<Self> {
        Self::fetch_range(symbol, "1y").await
    }

    pub async fn fetch_range(symbol: &str, range: &str) -> Result<Self> {
        fetch_range(symbol, range).await
    }

    #[allow(dead_code)]
    pub fn log_returns(&self) -> Vec<f64> {
        self.history
            .windows(2)
            .map(|w| (w[1].close / w[0].close).ln())
            .collect()
    }

    #[allow(dead_code)]
    pub fn stats(&self) -> (f64, f64) {
        let returns = self.log_returns();
        let n = returns.len() as f64;
        if n == 0.0 { return (0.0, 0.0); }

        let mean = returns.iter().sum::<f64>() / n;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        
        (mean, variance.sqrt())
    }

    pub fn analyze(&self) -> Analysis {
        let last = self.history.last().unwrap();
        let current_price = last.close;
        let pivot = (last.high + last.low + last.close) / 3.0;
        
        let support = self.history.iter().map(|c| c.low).fold(f64::INFINITY, |a, b| a.min(b));
        let resistance = self.history.iter().map(|c| c.high).fold(f64::NEG_INFINITY, |a, b| a.max(b));

        Analysis { current_price, support, resistance, pivot }
    }

    #[allow(dead_code)]
    pub fn new_mock(symbol: &str, days: usize) -> Self {
        let mut rng = rand::thread_rng();
        let mut history = Vec::with_capacity(days);
        let mut current_price: f64 = 100.0;
        let mut current_date = Utc::now() - Duration::days(days as i64);

        for _ in 0..days {
            let volatility = 0.02; // 2% daily volatility
            let change_pct: f64 = rng.gen_range(-volatility..volatility);
            let open = current_price;
            let close = open * (1.0 + change_pct);
            let high = open.max(close) * (1.0 + rng.gen_range(0.0..0.01));
            let low = open.min(close) * (1.0 - rng.gen_range(0.0..0.01));
            let volume = rng.gen_range(1000.0..10000.0);

            history.push(Candle {
                date: current_date,
                open,
                high,
                low,
                close,
                volume,
            });

            current_price = close;
            current_date += Duration::days(1);
        }

        Self {
            symbol: symbol.to_string(),
            history,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Analysis {
    pub current_price: f64,
    pub support: f64,
    pub resistance: f64,
    pub pivot: f64,
}

pub struct TrainingDataset {
    pub features: Vec<Vec<f64>>, // [seq_len, 2] (Close Return, Overnight Return)
    pub targets: Vec<Vec<f64>>,  // [forecast_len, 1] (Close Return)
    pub asset_ids: Vec<usize>,   // [1] Asset ID for each sample
}

impl TrainingDataset {
    pub fn split(self, train_ratio: f64) -> (Self, Self) {
        let n = self.features.len();
        let train_size = (n as f64 * train_ratio) as usize;
        
        let (train_features, val_features) = self.features.split_at(train_size);
        let (train_targets, val_targets) = self.targets.split_at(train_size);
        let (train_ids, val_ids) = self.asset_ids.split_at(train_size);
        
        (
            Self {
                features: train_features.to_vec(),
                targets: train_targets.to_vec(),
                asset_ids: train_ids.to_vec(),
            },
            Self {
                features: val_features.to_vec(),
                targets: val_targets.to_vec(),
                asset_ids: val_ids.to_vec(),
            }
        )
    }
}

impl StockData {
    /// Prepares sliding window datasets for training the diffusion model.
    ///
    /// # Arguments
    /// * `lookback` - Number of past days to use as input context.
    /// * `forecast` - Number of future days to predict.
    /// * `asset_id` - Unique identifier for the asset.
    pub fn prepare_training_data(&self, lookback: usize, forecast: usize, asset_id: usize) -> TrainingDataset {
        let mut features = Vec::new();
        let mut targets = Vec::new();
        let mut asset_ids = Vec::new();

        // Calculate returns
        // We need at least lookback + forecast + 1 data points
        if self.history.len() < lookback + forecast + 1 {
            return TrainingDataset { features, targets, asset_ids };
        }
        
        let mut all_close_returns = Vec::with_capacity(self.history.len());
        let mut all_overnight_returns = Vec::with_capacity(self.history.len());

        for i in 1..self.history.len() {
            let close_ret = (self.history[i].close / self.history[i-1].close).ln();
            let overnight_ret = (self.history[i].open / self.history[i-1].close).ln();
            
            all_close_returns.push(close_ret);
            all_overnight_returns.push(overnight_ret);
        }

        // Create sliding windows
        let total_returns = all_close_returns.len();
        if total_returns < lookback + forecast {
             return TrainingDataset { features, targets, asset_ids };
        }

        for j in 0..total_returns - lookback - forecast {
            let mut window_features = Vec::with_capacity(lookback);
            for k in 0..lookback {
                window_features.push(vec![
                    all_close_returns[j+k],
                    all_overnight_returns[j+k]
                ]);
            }

            let mut window_targets = Vec::with_capacity(forecast);
            for k in 0..forecast {
                window_targets.push(all_close_returns[j+lookback+k]);
            }
            
            // Z-Score Normalization per window
            let close_vals: Vec<f64> = window_features.iter().map(|f| f[0]).collect();
            let mean = close_vals.iter().sum::<f64>() / lookback as f64;
            let variance = close_vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (lookback as f64 - 1.0);
            let std = variance.sqrt() + 1e-6;

            let normalized_features: Vec<f64> = window_features.iter().flat_map(|f| {
                vec![
                    (f[0] - mean) / std,
                    (f[1] - mean) / std
                ]
            }).collect();

            let normalized_targets: Vec<f64> = window_targets.iter().map(|t| (t - mean) / std).collect();

            features.push(normalized_features);
            targets.push(normalized_targets);
            asset_ids.push(asset_id);
        }

        TrainingDataset { features, targets, asset_ids }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_training_data() {
        let mock_data = StockData::new_mock("TEST", 100);
        let lookback = 10;
        let forecast = 5;
        let asset_id = 0;
        
        let dataset = mock_data.prepare_training_data(lookback, forecast, asset_id);
        
        // Check if we have data
        assert!(!dataset.features.is_empty());
        assert!(!dataset.targets.is_empty());
        assert!(!dataset.asset_ids.is_empty());
        assert_eq!(dataset.features.len(), dataset.targets.len());
        assert_eq!(dataset.features.len(), dataset.asset_ids.len());
        assert_eq!(dataset.asset_ids[0], asset_id);
        
        // Check dimensions
        let first_feature = &dataset.features[0];
        assert_eq!(first_feature.len(), lookback * 2); // 2 features per step
        
        let first_target = &dataset.targets[0];
        assert_eq!(first_target.len(), forecast);
        
        // Check normalization (mean should be close to 0, std close to 1)
        // This is per-window normalization, so we check one window
        let close_vals: Vec<f64> = first_feature.iter().step_by(2).cloned().collect();
        let _mean = close_vals.iter().sum::<f64>() / close_vals.len() as f64;
        // Since we normalized, the mean of the *original* window was subtracted.
        // The values in `first_feature` are already normalized.
        // So their mean should be ~0 and std ~1.
        
        let _feat_mean = first_feature.iter().sum::<f64>() / first_feature.len() as f64;
        // Note: we normalize close and overnight returns together? 
        // In prepare_training_data:
        // let normalized_features: Vec<f64> = window_features.iter().flat_map(|f| {
        //     vec![
        //         (f[0] - mean) / std,
        //         (f[1] - mean) / std
        //     ]
        // }).collect();
        // We use the same mean/std (calculated from close returns) for both features.
        // So the mean of the normalized close returns should be 0.
        
        let norm_close_vals: Vec<f64> = first_feature.iter().step_by(2).cloned().collect();
        let norm_mean = norm_close_vals.iter().sum::<f64>() / norm_close_vals.len() as f64;
        
        assert!(norm_mean.abs() < 1e-5);
    }
}

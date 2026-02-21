use crate::{config, data, portfolio};
use anyhow::{anyhow, Result};
use chrono::{Datelike, Local, NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Duration, MissedTickBehavior};

pub const DEFAULT_INITIAL_CAPITAL_USD: f64 = 80_000.0;
pub const TRADING_FEE_RATE: f64 = 0.0005;
pub const PRICE_POLL_INTERVAL_SECS: u64 = 60;
pub const BENCHMARK_SYMBOL: &str = "QQQ";

#[derive(Clone, Debug)]
pub struct PaperTradingConfig {
    pub initial_capital_usd: f64,
    pub analysis_times_local: Vec<NaiveTime>,
    pub optimization_time_local: Option<NaiveTime>,
    pub optimization_weekdays: Vec<u32>,
    pub optimization_backend: config::ComputeBackend,
}

impl PaperTradingConfig {
    pub fn with_defaults() -> Self {
        Self {
            initial_capital_usd: DEFAULT_INITIAL_CAPITAL_USD,
            analysis_times_local: vec![
                NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(23, 30, 0).unwrap(),
            ],
            optimization_time_local: NaiveTime::from_hms_opt(22, 0, 0),
            optimization_weekdays: vec![1, 2, 3, 4, 5],
            optimization_backend: config::ComputeBackend::Auto,
        }
    }
}

pub fn build_config(
    initial_capital_input: Option<&str>,
    time1_input: &str,
    time2_input: &str,
    optimization_time_input: Option<&str>,
    optimization_weekdays_input: Option<&[u32]>,
    optimization_backend: config::ComputeBackend,
) -> Result<PaperTradingConfig> {
    let mut cfg = PaperTradingConfig::with_defaults();
    cfg.optimization_backend = optimization_backend;

    if let Some(raw) = initial_capital_input {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let capital = trimmed
                .parse::<f64>()
                .map_err(|_| anyhow!("Invalid initial capital: {}", trimmed))?;
            if capital <= 0.0 {
                return Err(anyhow!("Initial capital must be > 0"));
            }
            cfg.initial_capital_usd = capital;
        }
    }

    let t1 = parse_hhmm_local_time(time1_input)?;
    let t2 = parse_hhmm_local_time(time2_input)?;
    if t1 == t2 {
        return Err(anyhow!("Two analysis times must be different"));
    }

    let mut times = vec![t1, t2];
    times.sort();
    cfg.analysis_times_local = times;

    if let Some(raw) = optimization_time_input {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            cfg.optimization_time_local = Some(parse_hhmm_local_time(trimmed)?);
        }
    }

    if let Some(days) = optimization_weekdays_input {
        let mut normalized = days
            .iter()
            .copied()
            .filter(|d| (1..=7).contains(d))
            .collect::<Vec<_>>();
        normalized.sort_unstable();
        normalized.dedup();
        if !normalized.is_empty() {
            cfg.optimization_weekdays = normalized;
        }
    }

    Ok(cfg)
}

fn parse_hhmm_local_time(input: &str) -> Result<NaiveTime> {
    let trimmed = input.trim();
    NaiveTime::parse_from_str(trimmed, "%H:%M")
        .map_err(|_| anyhow!("Invalid time '{}', use HH:MM local time", trimmed))
}

fn compute_optimization_last_run_date(
    now_local: chrono::DateTime<Local>,
    optimization_time_local: Option<NaiveTime>,
    optimization_weekdays: &[u32],
) -> Option<NaiveDate> {
    optimization_time_local.and_then(|time| {
        let weekday = now_local.weekday().number_from_monday();
        if now_local.time() >= time && optimization_weekdays.contains(&weekday) {
            Some(now_local.date_naive())
        } else {
            None
        }
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TargetWeight {
    pub symbol: String,
    pub target_weight: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradeRecord {
    pub timestamp: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub price: f64,
    pub notional: f64,
    pub fee: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnalysisRecord {
    pub timestamp: String,
    pub portfolio_value_before: f64,
    pub portfolio_value_after: f64,
    pub cash_after: f64,
    pub trades: Vec<TradeRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyLog {
    pub created_at: String,
    pub initial_capital_usd: f64,
    pub trading_fee_rate: f64,
    pub analysis_times_local: Vec<String>,
    pub benchmark_symbol: String,
    #[serde(default)]
    pub benchmark_initial_price: Option<f64>,
    #[serde(default)]
    pub optimization_time_local: Option<String>,
    #[serde(default)]
    pub optimization_weekdays: Vec<u32>,
    pub targets: Vec<TargetWeight>,
    pub analyses: Vec<AnalysisRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinuteSymbolSnapshot {
    pub symbol: String,
    pub price: f64,
    pub change_1m: f64,
    pub change_1m_pct: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinuteHoldingSnapshot {
    pub symbol: String,
    pub quantity: f64,
    pub price: f64,
    pub asset_value: f64,
    #[serde(default)]
    pub avg_cost: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinutePortfolioSnapshot {
    pub timestamp: String,
    pub total_value: f64,
    #[serde(default)]
    pub cash_usd: f64,
    pub pnl_usd: f64,
    pub pnl_pct: f64,
    pub benchmark_return_pct: f64,
    pub symbols: Vec<MinuteSymbolSnapshot>,
    #[serde(default)]
    pub holdings: Vec<MinuteHoldingSnapshot>,
    #[serde(default)]
    pub holdings_symbols: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum PaperEvent {
    Started {
        strategy_file: String,
        runtime_file: String,
    },
    AutoOptimizationStatus {
        running: bool,
    },
    Info(String),
    Warning(String),
    Analysis(AnalysisRecord),
    Minute(MinutePortfolioSnapshot),
    Error(String),
}

#[derive(Clone, Debug)]
pub enum PaperCommand {
    Pause,
    Resume,
    Stop,
    UpdateOptimizationSchedule {
        optimization_time_local: Option<NaiveTime>,
        optimization_weekdays: Vec<u32>,
    },
    UpdateTargets {
        targets: Vec<TargetWeight>,
        apply_now: bool,
    },
}

struct PaperRuntime {
    initial_capital_usd: f64,
    cash_usd: f64,
    holdings_shares: HashMap<String, f64>,
    holdings_avg_cost: HashMap<String, f64>,
    previous_prices: HashMap<String, f64>,
    benchmark_initial_price: f64,
    strategy_log: StrategyLog,
    strategy_path: PathBuf,
    runtime_path: PathBuf,
}

pub async fn run_paper_trading(
    target_weights: Vec<(String, f64)>,
    config: PaperTradingConfig,
    event_tx: Sender<PaperEvent>,
    mut command_rx: Receiver<PaperCommand>,
) -> Result<()> {
    let target_weights = normalize_weights(&target_weights);
    if target_weights.is_empty() {
        return Err(anyhow!("No target weights for paper trading"));
    }

    let (strategy_path, runtime_path) = create_output_paths()?;

    let mut current_prices = fetch_prices_for_symbols(&tracked_symbols(&target_weights)).await?;
    let benchmark_initial_price = *current_prices
        .get(BENCHMARK_SYMBOL)
        .ok_or(anyhow!("Benchmark symbol {} price missing", BENCHMARK_SYMBOL))?;

    let mut holdings_shares = HashMap::new();
    let mut holdings_avg_cost = HashMap::new();
    for target in &target_weights {
        holdings_shares.insert(target.symbol.clone(), 0.0);
        holdings_avg_cost.insert(target.symbol.clone(), 0.0);
    }

    let mut runtime = PaperRuntime {
        initial_capital_usd: config.initial_capital_usd,
        cash_usd: config.initial_capital_usd,
        holdings_shares,
        holdings_avg_cost,
        previous_prices: current_prices.clone(),
        benchmark_initial_price,
        strategy_log: StrategyLog {
            created_at: Local::now().to_rfc3339(),
            initial_capital_usd: config.initial_capital_usd,
            trading_fee_rate: TRADING_FEE_RATE,
            analysis_times_local: config
                .analysis_times_local
                .iter()
                .map(|t| t.format("%H:%M").to_string())
                .collect(),
            benchmark_symbol: BENCHMARK_SYMBOL.to_string(),
            benchmark_initial_price: Some(benchmark_initial_price),
            optimization_time_local: config
                .optimization_time_local
                .map(|t| t.format("%H:%M").to_string()),
            optimization_weekdays: config.optimization_weekdays.clone(),
            targets: target_weights.clone(),
            analyses: Vec::new(),
        },
        strategy_path,
        runtime_path,
    };

    write_strategy_json(&runtime.strategy_path, &runtime.strategy_log)?;

    let _ = event_tx
        .send(PaperEvent::Started {
            strategy_file: runtime.strategy_path.display().to_string(),
            runtime_file: runtime.runtime_path.display().to_string(),
        })
        .await;

    let immediate_analysis = run_analysis_once(&mut runtime, &current_prices)?;
    let _ = event_tx.send(PaperEvent::Analysis(immediate_analysis)).await;

    let now_local = Local::now();
    let today = now_local.date_naive();
    let mut schedule_last_run_dates: Vec<Option<NaiveDate>> = config
        .analysis_times_local
        .iter()
        .map(|time| if now_local.time() >= *time { Some(today) } else { None })
        .collect();
    let mut optimization_time_local = config.optimization_time_local;
    let mut optimization_weekdays = if config.optimization_weekdays.is_empty() {
        vec![1, 2, 3, 4, 5]
    } else {
        config.optimization_weekdays.clone()
    };
    let mut optimization_last_run_date =
        compute_optimization_last_run_date(now_local, optimization_time_local, &optimization_weekdays);

    let mut interval = tokio::time::interval(Duration::from_secs(PRICE_POLL_INTERVAL_SECS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut paused = false;

    loop {
        interval.tick().await;

        while let Ok(command) = command_rx.try_recv() {
            match command {
                PaperCommand::Pause => {
                    paused = true;
                    let _ = event_tx
                        .send(PaperEvent::Info("Simulation paused".to_string()))
                        .await;
                }
                PaperCommand::Resume => {
                    paused = false;
                    let _ = event_tx
                        .send(PaperEvent::Info("Simulation resumed".to_string()))
                        .await;
                }
                PaperCommand::Stop => {
                    let _ = event_tx
                        .send(PaperEvent::Info("Simulation stopped".to_string()))
                        .await;
                    return Ok(());
                }
                PaperCommand::UpdateOptimizationSchedule {
                    optimization_time_local: next_opt_time,
                    optimization_weekdays: next_opt_days,
                } => {
                    let mut normalized_days = next_opt_days
                        .into_iter()
                        .filter(|day| (1..=7).contains(day))
                        .collect::<Vec<_>>();
                    normalized_days.sort_unstable();
                    normalized_days.dedup();
                    if normalized_days.is_empty() {
                        normalized_days = vec![1, 2, 3, 4, 5];
                    }

                    optimization_time_local = next_opt_time;
                    optimization_weekdays = normalized_days.clone();
                    runtime.strategy_log.optimization_time_local = optimization_time_local
                        .map(|time| time.format("%H:%M").to_string());
                    runtime.strategy_log.optimization_weekdays = normalized_days;
                    write_strategy_json(&runtime.strategy_path, &runtime.strategy_log)?;

                    optimization_last_run_date = compute_optimization_last_run_date(
                        Local::now(),
                        optimization_time_local,
                        &optimization_weekdays,
                    );

                    let _ = event_tx
                        .send(PaperEvent::Info(
                            "Optimization schedule updated".to_string(),
                        ))
                        .await;
                }
                PaperCommand::UpdateTargets { targets: next_targets, apply_now } => {
                    let tuples = next_targets
                        .iter()
                        .map(|t| (t.symbol.clone(), t.target_weight))
                        .collect::<Vec<_>>();
                    let normalized = normalize_weights(&tuples);
                    if normalized.is_empty() {
                        let _ = event_tx
                            .send(PaperEvent::Warning("Ignored empty target update".to_string()))
                            .await;
                        continue;
                    }

                    runtime.strategy_log.targets = normalized;
                    for target in &runtime.strategy_log.targets {
                        runtime
                            .holdings_shares
                            .entry(target.symbol.clone())
                            .or_insert(0.0);
                        runtime
                            .holdings_avg_cost
                            .entry(target.symbol.clone())
                            .or_insert(0.0);
                    }
                    write_strategy_json(&runtime.strategy_path, &runtime.strategy_log)?;
                    let listed = runtime
                        .strategy_log
                        .targets
                        .iter()
                        .map(|t| t.symbol.clone())
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = event_tx
                        .send(PaperEvent::Info(format!(
                            "Target pool updated{}: {}",
                            if apply_now { " (apply-now)" } else { "" },
                            listed
                        )))
                        .await;

                    if apply_now {
                        let runtime_symbols = tracked_symbols_for_runtime(&runtime);
                        match fetch_prices_for_symbols(&runtime_symbols).await {
                            Ok(prices) => {
                                current_prices = prices;
                                let analysis = run_analysis_once(&mut runtime, &current_prices)?;
                                let _ = event_tx.send(PaperEvent::Analysis(analysis)).await;
                                let minute_snapshot = build_minute_snapshot(&mut runtime, &current_prices)?;
                                append_jsonl(&runtime.runtime_path, &minute_snapshot)?;
                                let _ = event_tx.send(PaperEvent::Minute(minute_snapshot)).await;
                            }
                            Err(error) => {
                                let _ = event_tx
                                    .send(PaperEvent::Warning(format!(
                                        "Apply-now rebalance skipped due to price fetch error: {}",
                                        error
                                    )))
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        if paused {
            continue;
        }

        let runtime_symbols = tracked_symbols_for_runtime(&runtime);
        match fetch_prices_for_symbols(&runtime_symbols).await {
            Ok(prices) => {
                current_prices = prices;
            }
            Err(error) => {
                let _ = event_tx
                    .send(PaperEvent::Warning(format!(
                        "Price polling failed (will retry): {}",
                        error
                    )))
                    .await;
                continue;
            }
        }

        let minute_snapshot = build_minute_snapshot(&mut runtime, &current_prices)?;
        append_jsonl(&runtime.runtime_path, &minute_snapshot)?;
        let _ = event_tx.send(PaperEvent::Minute(minute_snapshot)).await;

        let now_local = Local::now();
        let now_date = now_local.date_naive();
        let now_time = now_local.time();

        for (index, scheduled_time) in config.analysis_times_local.iter().enumerate() {
            let already_ran_today = schedule_last_run_dates[index]
                .map(|date| date == now_date)
                .unwrap_or(false);

            if !already_ran_today && now_time >= *scheduled_time {
                let analysis = run_analysis_once(&mut runtime, &current_prices)?;
                let _ = event_tx.send(PaperEvent::Analysis(analysis)).await;
                schedule_last_run_dates[index] = Some(now_date);
            }
        }

        if let Some(opt_time) = optimization_time_local {
            let already_optimized_today = optimization_last_run_date
                .map(|date| date == now_date)
                .unwrap_or(false);
            let should_optimize_today = optimization_weekdays
                .contains(&now_local.weekday().number_from_monday());

            if should_optimize_today && !already_optimized_today && now_time >= opt_time {
                let _ = event_tx
                    .send(PaperEvent::AutoOptimizationStatus { running: true })
                    .await;
                let _ = event_tx
                    .send(PaperEvent::Info(
                        "Scheduled optimization started".to_string(),
                    ))
                    .await;
                match optimize_targets_from_candidate_pool(&mut runtime, config.optimization_backend).await {
                    Ok(updated) => {
                        if updated {
                            let listed = runtime
                                .strategy_log
                                .targets
                                .iter()
                                .map(|t| format!("{}:{:.4}", t.symbol, t.target_weight))
                                .collect::<Vec<_>>()
                                .join(", ");
                            let _ = event_tx
                                .send(PaperEvent::Info(format!(
                                    "Scheduled optimization updated targets (rebalance at next schedule): {}",
                                    listed
                                )))
                                .await;
                        }
                        optimization_last_run_date = Some(now_date);
                    }
                    Err(error) => {
                        let _ = event_tx
                            .send(PaperEvent::Warning(format!(
                                "Scheduled optimization failed: {}",
                                error
                            )))
                            .await;
                    }
                }
                let _ = event_tx
                    .send(PaperEvent::AutoOptimizationStatus { running: false })
                    .await;
            }
        }
    }
}

pub async fn run_paper_trading_from_strategy_file(
    strategy_file: &str,
    optimization_backend: config::ComputeBackend,
    event_tx: Sender<PaperEvent>,
    mut command_rx: Receiver<PaperCommand>,
) -> Result<()> {
    let strategy_path = resolve_strategy_path(strategy_file);
    if !strategy_path.exists() {
        return Err(anyhow!("Strategy file not found: {}", strategy_file));
    }

    let raw = std::fs::read_to_string(&strategy_path)?;
    let strategy_log: StrategyLog = serde_json::from_str(&raw)
        .map_err(|e| anyhow!("Failed to parse strategy JSON: {}", e))?;

    if strategy_log.targets.is_empty() {
        return Err(anyhow!("No targets found in strategy JSON"));
    }

    let mut analysis_times_local = Vec::new();
    for t in &strategy_log.analysis_times_local {
        analysis_times_local.push(parse_hhmm_local_time(t)?);
    }
    if analysis_times_local.is_empty() {
        return Err(anyhow!("No analysis schedule found in strategy JSON"));
    }

    let mut cfg = PaperTradingConfig::with_defaults();
    cfg.initial_capital_usd = strategy_log.initial_capital_usd;
    cfg.analysis_times_local = analysis_times_local;
    cfg.optimization_backend = optimization_backend;
    cfg.optimization_time_local = strategy_log
        .optimization_time_local
        .as_deref()
        .map(parse_hhmm_local_time)
        .transpose()?;
    if !strategy_log.optimization_weekdays.is_empty() {
        let mut weekdays = strategy_log
            .optimization_weekdays
            .iter()
            .copied()
            .filter(|d| (1..=7).contains(d))
            .collect::<Vec<_>>();
        weekdays.sort_unstable();
        weekdays.dedup();
        if !weekdays.is_empty() {
            cfg.optimization_weekdays = weekdays;
        }
    }

    let mut holdings_shares = HashMap::new();
    let mut holdings_avg_cost = HashMap::new();
    for target in &strategy_log.targets {
        holdings_shares.insert(target.symbol.clone(), 0.0);
        holdings_avg_cost.insert(target.symbol.clone(), 0.0);
    }

    let mut cash_usd = strategy_log.initial_capital_usd;
    for analysis in &strategy_log.analyses {
        for trade in &analysis.trades {
            let entry = holdings_shares.entry(trade.symbol.clone()).or_insert(0.0);
            let avg_entry = holdings_avg_cost.entry(trade.symbol.clone()).or_insert(0.0);
            match trade.side.as_str() {
                "BUY" => {
                    let prev_qty = *entry;
                    let new_qty = prev_qty + trade.quantity;
                    if new_qty > 1e-9 {
                        *avg_entry = ((*avg_entry * prev_qty) + (trade.price * trade.quantity)) / new_qty;
                    }
                    *entry += trade.quantity;
                }
                "SELL" => {
                    *entry -= trade.quantity;
                    if *entry <= 1e-9 {
                        *entry = 0.0;
                        *avg_entry = 0.0;
                    }
                }
                _ => {}
            }
        }
        cash_usd = analysis.cash_after;
    }

    let mut bootstrap_symbols = tracked_symbols(&strategy_log.targets);
    for (symbol, quantity) in &holdings_shares {
        if quantity.abs() > 1e-9 && !bootstrap_symbols.iter().any(|known| known == symbol) {
            bootstrap_symbols.push(symbol.clone());
        }
    }
    bootstrap_symbols.sort();
    bootstrap_symbols.dedup();

    let mut current_prices = fetch_prices_for_symbols(&bootstrap_symbols).await?;

    let benchmark_initial_price = strategy_log
        .benchmark_initial_price
        .unwrap_or_else(|| *current_prices.get(BENCHMARK_SYMBOL).unwrap_or(&1.0));

    let runtime_path = strategy_path
        .to_string_lossy()
        .replace("paper_strategy_", "paper_runtime_")
        .replace(".json", ".jsonl");

    let mut runtime = PaperRuntime {
        initial_capital_usd: cfg.initial_capital_usd,
        cash_usd,
        holdings_shares,
        holdings_avg_cost,
        previous_prices: current_prices.clone(),
        benchmark_initial_price,
        strategy_log,
        strategy_path: strategy_path.clone(),
        runtime_path: PathBuf::from(runtime_path),
    };

    let historical_snapshots = load_runtime_snapshots(&runtime.runtime_path)?;
    if let Some(last_runtime_snapshot) = historical_snapshots.last() {
        let mut prev_prices = HashMap::new();
        for symbol in &last_runtime_snapshot.symbols {
            prev_prices.insert(symbol.symbol.clone(), symbol.price);
        }
        if !prev_prices.is_empty() {
            runtime.previous_prices = prev_prices;
        }
    }

    let _ = event_tx
        .send(PaperEvent::Started {
            strategy_file: runtime.strategy_path.display().to_string(),
            runtime_file: runtime.runtime_path.display().to_string(),
        })
        .await;
    let _ = event_tx
        .send(PaperEvent::Info(format!(
            "Resumed from strategy file with {} historical analyses",
            runtime.strategy_log.analyses.len()
        )))
        .await;

    for snapshot in historical_snapshots {
        let _ = event_tx.send(PaperEvent::Minute(snapshot)).await;
    }

    if let Some(last_analysis) = runtime.strategy_log.analyses.last().cloned() {
        let _ = event_tx.send(PaperEvent::Analysis(last_analysis)).await;
    }

    let immediate_snapshot = build_minute_snapshot(&mut runtime, &current_prices)?;
    append_jsonl(&runtime.runtime_path, &immediate_snapshot)?;
    let _ = event_tx.send(PaperEvent::Minute(immediate_snapshot)).await;

    let now_local = Local::now();
    let today = now_local.date_naive();
    let mut schedule_last_run_dates: Vec<Option<NaiveDate>> = cfg
        .analysis_times_local
        .iter()
        .map(|time| if now_local.time() >= *time { Some(today) } else { None })
        .collect();
    let mut optimization_time_local = cfg.optimization_time_local;
    let mut optimization_weekdays = if cfg.optimization_weekdays.is_empty() {
        vec![1, 2, 3, 4, 5]
    } else {
        cfg.optimization_weekdays.clone()
    };
    let mut optimization_last_run_date =
        compute_optimization_last_run_date(now_local, optimization_time_local, &optimization_weekdays);

    let mut interval = tokio::time::interval(Duration::from_secs(PRICE_POLL_INTERVAL_SECS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut paused = false;

    loop {
        interval.tick().await;

        while let Ok(command) = command_rx.try_recv() {
            match command {
                PaperCommand::Pause => {
                    paused = true;
                    let _ = event_tx
                        .send(PaperEvent::Info("Simulation paused".to_string()))
                        .await;
                }
                PaperCommand::Resume => {
                    paused = false;
                    let _ = event_tx
                        .send(PaperEvent::Info("Simulation resumed".to_string()))
                        .await;
                }
                PaperCommand::Stop => {
                    let _ = event_tx
                        .send(PaperEvent::Info("Simulation stopped".to_string()))
                        .await;
                    return Ok(());
                }
                PaperCommand::UpdateOptimizationSchedule {
                    optimization_time_local: next_opt_time,
                    optimization_weekdays: next_opt_days,
                } => {
                    let mut normalized_days = next_opt_days
                        .into_iter()
                        .filter(|day| (1..=7).contains(day))
                        .collect::<Vec<_>>();
                    normalized_days.sort_unstable();
                    normalized_days.dedup();
                    if normalized_days.is_empty() {
                        normalized_days = vec![1, 2, 3, 4, 5];
                    }

                    optimization_time_local = next_opt_time;
                    optimization_weekdays = normalized_days.clone();
                    runtime.strategy_log.optimization_time_local = optimization_time_local
                        .map(|time| time.format("%H:%M").to_string());
                    runtime.strategy_log.optimization_weekdays = normalized_days;
                    write_strategy_json(&runtime.strategy_path, &runtime.strategy_log)?;

                    optimization_last_run_date = compute_optimization_last_run_date(
                        Local::now(),
                        optimization_time_local,
                        &optimization_weekdays,
                    );

                    let _ = event_tx
                        .send(PaperEvent::Info(
                            "Optimization schedule updated".to_string(),
                        ))
                        .await;
                }
                PaperCommand::UpdateTargets { targets: next_targets, apply_now } => {
                    let tuples = next_targets
                        .iter()
                        .map(|t| (t.symbol.clone(), t.target_weight))
                        .collect::<Vec<_>>();
                    let normalized = normalize_weights(&tuples);
                    if normalized.is_empty() {
                        let _ = event_tx
                            .send(PaperEvent::Warning("Ignored empty target update".to_string()))
                            .await;
                        continue;
                    }

                    runtime.strategy_log.targets = normalized;
                    for target in &runtime.strategy_log.targets {
                        runtime
                            .holdings_shares
                            .entry(target.symbol.clone())
                            .or_insert(0.0);
                        runtime
                            .holdings_avg_cost
                            .entry(target.symbol.clone())
                            .or_insert(0.0);
                    }
                    write_strategy_json(&runtime.strategy_path, &runtime.strategy_log)?;
                    let listed = runtime
                        .strategy_log
                        .targets
                        .iter()
                        .map(|t| t.symbol.clone())
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = event_tx
                        .send(PaperEvent::Info(format!(
                            "Target pool updated{}: {}",
                            if apply_now { " (apply-now)" } else { "" },
                            listed
                        )))
                        .await;

                    if apply_now {
                        let runtime_symbols = tracked_symbols_for_runtime(&runtime);
                        match fetch_prices_for_symbols(&runtime_symbols).await {
                            Ok(prices) => {
                                current_prices = prices;
                                let analysis = run_analysis_once(&mut runtime, &current_prices)?;
                                let _ = event_tx.send(PaperEvent::Analysis(analysis)).await;
                                let minute_snapshot = build_minute_snapshot(&mut runtime, &current_prices)?;
                                append_jsonl(&runtime.runtime_path, &minute_snapshot)?;
                                let _ = event_tx.send(PaperEvent::Minute(minute_snapshot)).await;
                            }
                            Err(error) => {
                                let _ = event_tx
                                    .send(PaperEvent::Warning(format!(
                                        "Apply-now rebalance skipped due to price fetch error: {}",
                                        error
                                    )))
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        if paused {
            continue;
        }

        let runtime_symbols = tracked_symbols_for_runtime(&runtime);
        match fetch_prices_for_symbols(&runtime_symbols).await {
            Ok(prices) => {
                current_prices = prices;
            }
            Err(error) => {
                let _ = event_tx
                    .send(PaperEvent::Warning(format!(
                        "Price polling failed (will retry): {}",
                        error
                    )))
                    .await;
                continue;
            }
        }

        let minute_snapshot = build_minute_snapshot(&mut runtime, &current_prices)?;
        append_jsonl(&runtime.runtime_path, &minute_snapshot)?;
        let _ = event_tx.send(PaperEvent::Minute(minute_snapshot)).await;

        let now_local = Local::now();
        let now_date = now_local.date_naive();
        let now_time = now_local.time();

        for (index, scheduled_time) in cfg.analysis_times_local.iter().enumerate() {
            let already_ran_today = schedule_last_run_dates[index]
                .map(|date| date == now_date)
                .unwrap_or(false);

            if !already_ran_today && now_time >= *scheduled_time {
                let analysis = run_analysis_once(&mut runtime, &current_prices)?;
                let _ = event_tx.send(PaperEvent::Analysis(analysis)).await;
                schedule_last_run_dates[index] = Some(now_date);
            }
        }

        if let Some(opt_time) = optimization_time_local {
            let already_optimized_today = optimization_last_run_date
                .map(|date| date == now_date)
                .unwrap_or(false);
            let should_optimize_today = optimization_weekdays
                .contains(&now_local.weekday().number_from_monday());

            if should_optimize_today && !already_optimized_today && now_time >= opt_time {
                let _ = event_tx
                    .send(PaperEvent::AutoOptimizationStatus { running: true })
                    .await;
                let _ = event_tx
                    .send(PaperEvent::Info(
                        "Scheduled optimization started".to_string(),
                    ))
                    .await;
                match optimize_targets_from_candidate_pool(&mut runtime, cfg.optimization_backend).await {
                    Ok(updated) => {
                        if updated {
                            let listed = runtime
                                .strategy_log
                                .targets
                                .iter()
                                .map(|t| format!("{}:{:.4}", t.symbol, t.target_weight))
                                .collect::<Vec<_>>()
                                .join(", ");
                            let _ = event_tx
                                .send(PaperEvent::Info(format!(
                                    "Scheduled optimization updated targets (rebalance at next schedule): {}",
                                    listed
                                )))
                                .await;
                        }
                        optimization_last_run_date = Some(now_date);
                    }
                    Err(error) => {
                        let _ = event_tx
                            .send(PaperEvent::Warning(format!(
                                "Scheduled optimization failed: {}",
                                error
                            )))
                            .await;
                    }
                }
                let _ = event_tx
                    .send(PaperEvent::AutoOptimizationStatus { running: false })
                    .await;
            }
        }
    }
}

async fn optimize_targets_from_candidate_pool(
    runtime: &mut PaperRuntime,
    backend: config::ComputeBackend,
) -> Result<bool> {
    let mut candidate_symbols = runtime
        .strategy_log
        .targets
        .iter()
        .map(|t| t.symbol.clone())
        .collect::<Vec<_>>();
    candidate_symbols.sort();
    candidate_symbols.dedup();

    if candidate_symbols.is_empty() {
        return Ok(false);
    }

    let next_targets = if candidate_symbols.len() == 1 {
        vec![TargetWeight {
            symbol: candidate_symbols[0].clone(),
            target_weight: 1.0,
        }]
    } else {
        let alloc = portfolio::run_portfolio_optimization_with_backend(&candidate_symbols, backend).await?;
        normalize_weights(&alloc.weights)
    };

    if next_targets.is_empty() {
        return Ok(false);
    }

    runtime.strategy_log.targets = next_targets;
    for target in &runtime.strategy_log.targets {
        runtime
            .holdings_shares
            .entry(target.symbol.clone())
            .or_insert(0.0);
        runtime
            .holdings_avg_cost
            .entry(target.symbol.clone())
            .or_insert(0.0);
    }
    write_strategy_json(&runtime.strategy_path, &runtime.strategy_log)?;
    Ok(true)
}

fn run_analysis_once(runtime: &mut PaperRuntime, current_prices: &HashMap<String, f64>) -> Result<AnalysisRecord> {
    let now = Local::now().to_rfc3339();
    let portfolio_before = total_portfolio_value(runtime, current_prices)?;
    let mut trades = Vec::new();

    let target_symbols: HashSet<String> = runtime
        .strategy_log
        .targets
        .iter()
        .map(|t| t.symbol.clone())
        .collect();

    let symbols_to_liquidate = runtime
        .holdings_shares
        .iter()
        .filter_map(|(symbol, qty)| {
            if *qty > 1e-9 && !target_symbols.contains(symbol) {
                Some(symbol.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    for symbol in symbols_to_liquidate {
        let price = *current_prices
            .get(&symbol)
            .ok_or(anyhow!("Missing price for {}", symbol))?;
        let current_shares = *runtime.holdings_shares.get(&symbol).unwrap_or(&0.0);
        let execute_qty = current_shares.floor().max(0.0);
        if execute_qty < 1.0 {
            continue;
        }

        let notional = execute_qty * price;
        let fee = notional * TRADING_FEE_RATE;
        runtime.cash_usd += notional - fee;
        runtime.holdings_shares.insert(symbol.clone(), 0.0);
        runtime.holdings_avg_cost.insert(symbol.clone(), 0.0);

        trades.push(TradeRecord {
            timestamp: now.clone(),
            symbol,
            side: "SELL".to_string(),
            quantity: execute_qty,
            price,
            notional,
            fee,
        });
    }

    for target in &runtime.strategy_log.targets {
        let price = *current_prices
            .get(&target.symbol)
            .ok_or(anyhow!("Missing price for {}", target.symbol))?;

        let current_shares = *runtime.holdings_shares.get(&target.symbol).unwrap_or(&0.0);
        let target_dollar = portfolio_before * target.target_weight;
        let target_shares = (target_dollar / price).floor();
        let delta_shares = target_shares - current_shares;

        if delta_shares.abs() < 1.0 {
            continue;
        }

        if delta_shares > 0.0 {
            let requested_qty = delta_shares.floor();
            let affordable_qty = (runtime.cash_usd / (price * (1.0 + TRADING_FEE_RATE))).floor();
            let execute_qty = requested_qty.min(affordable_qty).max(0.0);

            if execute_qty >= 1.0 {
                let notional = execute_qty * price;
                let fee = notional * TRADING_FEE_RATE;
                runtime.cash_usd -= notional + fee;
                let prev_qty = *runtime.holdings_shares.get(&target.symbol).unwrap_or(&0.0);
                let prev_avg = *runtime.holdings_avg_cost.get(&target.symbol).unwrap_or(&0.0);
                let new_qty = prev_qty + execute_qty;
                let new_avg = if new_qty > 1e-9 {
                    ((prev_avg * prev_qty) + (price * execute_qty)) / new_qty
                } else {
                    0.0
                };
                runtime
                    .holdings_shares
                    .entry(target.symbol.clone())
                    .and_modify(|quantity| *quantity += execute_qty)
                    .or_insert(execute_qty);
                runtime
                    .holdings_avg_cost
                    .insert(target.symbol.clone(), new_avg);

                trades.push(TradeRecord {
                    timestamp: now.clone(),
                    symbol: target.symbol.clone(),
                    side: "BUY".to_string(),
                    quantity: execute_qty,
                    price,
                    notional,
                    fee,
                });
            }
        } else {
            let requested_qty = (-delta_shares).floor();
            let available_qty = current_shares.floor();
            let execute_qty = requested_qty.min(available_qty).max(0.0);

            if execute_qty >= 1.0 {
                let notional = execute_qty * price;
                let fee = notional * TRADING_FEE_RATE;
                runtime.cash_usd += notional - fee;
                let mut next_qty = current_shares - execute_qty;
                if next_qty <= 1e-9 {
                    next_qty = 0.0;
                }
                runtime
                    .holdings_shares
                    .entry(target.symbol.clone())
                    .and_modify(|quantity| *quantity = next_qty)
                    .or_insert(next_qty);
                if next_qty <= 1e-9 {
                    runtime
                        .holdings_avg_cost
                        .insert(target.symbol.clone(), 0.0);
                }

                trades.push(TradeRecord {
                    timestamp: now.clone(),
                    symbol: target.symbol.clone(),
                    side: "SELL".to_string(),
                    quantity: execute_qty,
                    price,
                    notional,
                    fee,
                });
            }
        }
    }

    let portfolio_after = total_portfolio_value(runtime, current_prices)?;
    let analysis = AnalysisRecord {
        timestamp: now,
        portfolio_value_before: portfolio_before,
        portfolio_value_after: portfolio_after,
        cash_after: runtime.cash_usd,
        trades,
    };

    runtime.strategy_log.analyses.push(analysis.clone());
    write_strategy_json(&runtime.strategy_path, &runtime.strategy_log)?;

    Ok(analysis)
}

fn build_minute_snapshot(
    runtime: &mut PaperRuntime,
    current_prices: &HashMap<String, f64>,
) -> Result<MinutePortfolioSnapshot> {
    let timestamp = Local::now().to_rfc3339();
    let total_value = total_portfolio_value(runtime, current_prices)?;
    let pnl_usd = total_value - runtime.initial_capital_usd;
    let pnl_pct = if runtime.initial_capital_usd.abs() > 1e-9 {
        pnl_usd / runtime.initial_capital_usd * 100.0
    } else {
        0.0
    };

    let benchmark_price = *current_prices
        .get(BENCHMARK_SYMBOL)
        .ok_or(anyhow!("Missing benchmark price"))?;
    let benchmark_return_pct = (benchmark_price / runtime.benchmark_initial_price - 1.0) * 100.0;

    let mut symbols = Vec::new();
    let mut holdings = Vec::new();
    let mut snapshot_symbols: HashSet<String> = runtime
        .strategy_log
        .targets
        .iter()
        .map(|target| target.symbol.clone())
        .collect();
    for (symbol, qty) in &runtime.holdings_shares {
        if *qty > 1e-9 {
            snapshot_symbols.insert(symbol.clone());
        }
    }
    let mut ordered_symbols: Vec<String> = snapshot_symbols.into_iter().collect();
    ordered_symbols.sort();

    for symbol in &ordered_symbols {
        let price = *current_prices
            .get(symbol)
            .ok_or(anyhow!("Missing symbol price for {}", symbol))?;

        let previous_price = runtime
            .previous_prices
            .get(symbol)
            .copied()
            .unwrap_or(price);
        let change_1m = price - previous_price;
        let change_1m_pct = if previous_price.abs() > 1e-9 {
            change_1m / previous_price * 100.0
        } else {
            0.0
        };

        symbols.push(MinuteSymbolSnapshot {
            symbol: symbol.clone(),
            price,
            change_1m,
            change_1m_pct,
        });

        let quantity = *runtime.holdings_shares.get(symbol).unwrap_or(&0.0);
        if quantity.abs() > 1e-9 {
            let avg_cost = *runtime.holdings_avg_cost.get(symbol).unwrap_or(&0.0);
            holdings.push(MinuteHoldingSnapshot {
                symbol: symbol.clone(),
                quantity,
                price,
                asset_value: quantity * price,
                avg_cost,
            });
        }
    }

    runtime.previous_prices = current_prices.clone();

    let mut holdings_symbols: Vec<String> = runtime
        .holdings_shares
        .iter()
        .filter_map(|(symbol, quantity)| {
            if quantity.abs() > 1e-9 {
                Some(symbol.clone())
            } else {
                None
            }
        })
        .collect();
    holdings_symbols.sort();
    holdings.sort_by(|a, b| a.symbol.cmp(&b.symbol));

    Ok(MinutePortfolioSnapshot {
        timestamp,
        total_value,
        cash_usd: runtime.cash_usd,
        pnl_usd,
        pnl_pct,
        benchmark_return_pct,
        symbols,
        holdings,
        holdings_symbols,
    })
}

fn total_portfolio_value(runtime: &PaperRuntime, prices: &HashMap<String, f64>) -> Result<f64> {
    let mut total = runtime.cash_usd;

    for (symbol, quantity) in &runtime.holdings_shares {
        if quantity.abs() <= 1e-9 {
            continue;
        }
        let price = prices
            .get(symbol)
            .ok_or(anyhow!("Missing price for holdings symbol {}", symbol))?;
        total += quantity * price;
    }

    Ok(total)
}

fn normalize_weights(weights: &[(String, f64)]) -> Vec<TargetWeight> {
    let mut filtered: Vec<TargetWeight> = weights
        .iter()
        .map(|(symbol, weight)| TargetWeight {
            symbol: symbol.clone(),
            target_weight: weight.clamp(0.0, 1.0),
        })
        .filter(|position| position.target_weight > 0.0)
        .collect();

    let total_weight: f64 = filtered.iter().map(|position| position.target_weight).sum();
    if total_weight > 1.0 {
        for position in &mut filtered {
            position.target_weight /= total_weight;
        }
    }

    filtered
}

fn tracked_symbols(targets: &[TargetWeight]) -> Vec<String> {
    let mut symbols: Vec<String> = targets.iter().map(|target| target.symbol.clone()).collect();
    if !symbols.iter().any(|symbol| symbol == BENCHMARK_SYMBOL) {
        symbols.push(BENCHMARK_SYMBOL.to_string());
    }
    symbols
}

fn tracked_symbols_for_runtime(runtime: &PaperRuntime) -> Vec<String> {
    let mut symbols: HashSet<String> = runtime
        .strategy_log
        .targets
        .iter()
        .map(|target| target.symbol.clone())
        .collect();
    for (symbol, qty) in &runtime.holdings_shares {
        if *qty > 1e-9 {
            symbols.insert(symbol.clone());
        }
    }
    symbols.insert(BENCHMARK_SYMBOL.to_string());

    let mut out: Vec<String> = symbols.into_iter().collect();
    out.sort();
    out
}

async fn fetch_prices_for_symbols(symbols: &[String]) -> Result<HashMap<String, f64>> {
    let mut prices = HashMap::new();
    for symbol in symbols {
        let price = data::fetch_latest_price_1m(symbol).await?;
        prices.insert(symbol.clone(), price);
    }
    Ok(prices)
}

fn create_output_paths() -> Result<(PathBuf, PathBuf)> {
    let log_dir = Path::new("log");
    if !log_dir.exists() {
        std::fs::create_dir_all(log_dir)?;
    }

    let suffix = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let strategy_path = log_dir.join(format!("paper_strategy_{}.json", suffix));
    let runtime_path = log_dir.join(format!("paper_runtime_{}.jsonl", suffix));

    Ok((strategy_path, runtime_path))
}

fn resolve_strategy_path(strategy_file: &str) -> PathBuf {
    let input = PathBuf::from(strategy_file);
    if input.is_absolute() {
        return input;
    }

    if input.exists() {
        return input;
    }

    config::project_root_path().join(input)
}

fn write_strategy_json(path: &Path, strategy: &StrategyLog) -> Result<()> {
    let json = serde_json::to_string_pretty(strategy)?;
    std::fs::write(path, json)?;
    Ok(())
}

fn append_jsonl<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let line = serde_json::to_string(value)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{}", line)?;
    Ok(())
}

fn load_runtime_snapshots(path: &Path) -> Result<Vec<MinutePortfolioSnapshot>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content = std::fs::read_to_string(path)?;
    let mut snapshots = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(snapshot) = serde_json::from_str::<MinutePortfolioSnapshot>(trimmed) {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

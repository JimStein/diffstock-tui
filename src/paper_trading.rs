use crate::data;
use anyhow::{anyhow, Result};
use chrono::{Local, NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
}

impl PaperTradingConfig {
    pub fn with_defaults() -> Self {
        Self {
            initial_capital_usd: DEFAULT_INITIAL_CAPITAL_USD,
            analysis_times_local: vec![
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
            ],
        }
    }
}

pub fn build_config(
    initial_capital_input: Option<&str>,
    time1_input: &str,
    time2_input: &str,
) -> Result<PaperTradingConfig> {
    let mut cfg = PaperTradingConfig::with_defaults();

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
    Ok(cfg)
}

fn parse_hhmm_local_time(input: &str) -> Result<NaiveTime> {
    let trimmed = input.trim();
    NaiveTime::parse_from_str(trimmed, "%H:%M")
        .map_err(|_| anyhow!("Invalid time '{}', use HH:MM local time", trimmed))
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
pub struct MinutePortfolioSnapshot {
    pub timestamp: String,
    pub total_value: f64,
    pub pnl_usd: f64,
    pub pnl_pct: f64,
    pub benchmark_return_pct: f64,
    pub symbols: Vec<MinuteSymbolSnapshot>,
}

#[derive(Clone, Debug)]
pub enum PaperEvent {
    Started {
        strategy_file: String,
        runtime_file: String,
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
}

struct PaperRuntime {
    initial_capital_usd: f64,
    cash_usd: f64,
    holdings_shares: HashMap<String, f64>,
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
    let tracked_symbols = tracked_symbols(&target_weights);

    let mut current_prices = fetch_prices_for_symbols(&tracked_symbols).await?;
    let benchmark_initial_price = *current_prices
        .get(BENCHMARK_SYMBOL)
        .ok_or(anyhow!("Benchmark symbol {} price missing", BENCHMARK_SYMBOL))?;

    let mut holdings_shares = HashMap::new();
    for target in &target_weights {
        holdings_shares.insert(target.symbol.clone(), 0.0);
    }

    let mut runtime = PaperRuntime {
        initial_capital_usd: config.initial_capital_usd,
        cash_usd: config.initial_capital_usd,
        holdings_shares,
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
            }
        }

        if paused {
            continue;
        }

        match fetch_prices_for_symbols(&tracked_symbols).await {
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
    }
}

fn run_analysis_once(runtime: &mut PaperRuntime, current_prices: &HashMap<String, f64>) -> Result<AnalysisRecord> {
    let now = Local::now().to_rfc3339();
    let portfolio_before = total_portfolio_value(runtime, current_prices)?;
    let mut trades = Vec::new();

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
                runtime
                    .holdings_shares
                    .entry(target.symbol.clone())
                    .and_modify(|quantity| *quantity += execute_qty)
                    .or_insert(execute_qty);

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
                runtime
                    .holdings_shares
                    .entry(target.symbol.clone())
                    .and_modify(|quantity| *quantity -= execute_qty)
                    .or_insert(0.0);

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
    for target in &runtime.strategy_log.targets {
        let symbol = &target.symbol;
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
    }

    runtime.previous_prices = current_prices.clone();

    Ok(MinutePortfolioSnapshot {
        timestamp,
        total_value,
        pnl_usd,
        pnl_pct,
        benchmark_return_pct,
        symbols,
    })
}

fn total_portfolio_value(runtime: &PaperRuntime, prices: &HashMap<String, f64>) -> Result<f64> {
    let mut total = runtime.cash_usd;

    for (symbol, quantity) in &runtime.holdings_shares {
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

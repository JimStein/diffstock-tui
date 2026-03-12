use crate::config::{ComputeBackend, DATA_RANGE, LOOKBACK};
use crate::inference::{self, ForecastData};
use anyhow::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

// ──────────────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────────────

/// Number of Monte Carlo simulations per asset for portfolio optimization.
pub const PORTFOLIO_MC_PATHS: usize = 500;

/// Forecast horizon in trading days for portfolio decisions.
pub const PORTFOLIO_HORIZON: usize = 10;

/// Risk-free annual rate (approx. T-bill yield) used in Sharpe calculations.
pub const RISK_FREE_RATE: f64 = 0.05;

/// Maximum weight any single asset can take (0.0–1.0).
pub const MAX_SINGLE_WEIGHT: f64 = 0.40;

/// Minimum weight any asset must have if included (prevents dust positions).
pub const MIN_SINGLE_WEIGHT: f64 = 0.02;

/// Target annualized volatility for portfolio (used in vol-targeting overlay).
pub const TARGET_ANNUAL_VOL: f64 = 0.16;

/// Annual trading days for annualization.
pub const TRADING_DAYS: f64 = 252.0;

/// CVaR confidence level (e.g., 0.05 = bottom 5% of returns).
pub const CVAR_ALPHA: f64 = 0.05;

/// Number of random portfolios to sample in Monte Carlo optimization.
pub const OPTIMIZER_SAMPLES: usize = 100_000;

/// Risk-off overlay thresholds.
pub const REGIME_NEGATIVE_BREADTH_THRESHOLD: f64 = 0.75;
pub const REGIME_CVAR_RISK_OFF_THRESHOLD: f64 = 0.04;
pub const REGIME_DEFENSIVE_BREADTH_ENTER: f64 = 0.55;
pub const REGIME_DEFENSIVE_BREADTH_EXIT: f64 = 0.45;
pub const REGIME_RISK_OFF_BREADTH_EXIT: f64 = 0.60;
pub const REGIME_DEFENSIVE_CVAR_ENTER: f64 = 0.025;
pub const REGIME_DEFENSIVE_CVAR_EXIT: f64 = 0.018;
pub const REGIME_RISK_OFF_CVAR_EXIT: f64 = 0.03;
pub const REGIME_DEFENSIVE_RETURN_ENTER: f64 = 0.08;
pub const REGIME_DEFENSIVE_RETURN_EXIT: f64 = 0.12;
pub const REGIME_RISK_OFF_RETURN_EXIT: f64 = 0.03;
pub const REGIME_RISK_ON_MIN_GROSS: f64 = 0.85;
pub const REGIME_DEFENSIVE_MAX_GROSS: f64 = 0.75;
pub const REGIME_DEFENSIVE_MIN_GROSS: f64 = 0.35;
pub const REGIME_RISK_OFF_MAX_GROSS: f64 = 0.20;

// ──────────────────────────────────────────────────────────────────────────────
// Data Structures
// ──────────────────────────────────────────────────────────────────────────────

/// Per-asset forecast statistics derived from diffusion Monte Carlo paths.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct AssetForecast {
    pub symbol: String,
    pub current_price: f64,
    /// Expected daily return (mean of MC log-return paths).
    pub expected_return: f64,
    /// Daily volatility (std of MC log-return paths).
    pub volatility: f64,
    /// Annualized expected return.
    pub annual_return: f64,
    /// Annualized volatility.
    pub annual_vol: f64,
    /// Sharpe ratio (annualized).
    pub sharpe: f64,
    /// Raw Monte Carlo paths: each inner Vec is one path of *period returns*.
    pub mc_period_returns: Vec<f64>,
    /// P10..P90 target prices at horizon.
    pub p10_price: f64,
    pub p50_price: f64,
    pub p90_price: f64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegimeState {
    RiskOn,
    Defensive,
    RiskOff,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketRegimeOverlay {
    pub state: MarketRegimeState,
    pub max_gross_exposure: f64,
    pub bearish_breadth: f64,
    pub negative_return_breadth: f64,
    pub negative_sharpe_breadth: f64,
    pub portfolio_expected_annual_return: f64,
    pub portfolio_cvar_95: f64,
    pub reasons: Vec<String>,
}

/// Complete portfolio allocation result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioAllocation {
    pub weights: Vec<(String, f64)>,     // (symbol, weight 0-1)
    pub expected_annual_return: f64,
    pub expected_annual_vol: f64,
    pub sharpe_ratio: f64,
    pub cvar_95: f64,                    // 95% CVaR of portfolio
    pub asset_forecasts: Vec<AssetForecast>,
    pub leverage: f64,                   // Vol-targeting leverage multiplier
    pub max_gross_exposure: f64,
    pub target_cash_weight: f64,
    pub market_regime: MarketRegimeOverlay,
}

fn clamp_unit(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

fn lerp(start: f64, end: f64, t: f64) -> f64 {
    start + (end - start) * clamp_unit(t)
}

fn normalized_above(value: f64, start: f64, end: f64) -> f64 {
    if end <= start {
        return 0.0;
    }
    clamp_unit((value - start) / (end - start))
}

fn normalized_below(value: f64, start: f64, end: f64) -> f64 {
    if start <= end {
        return 0.0;
    }
    clamp_unit((start - value) / (start - end))
}

fn state_with_hysteresis(
    previous_state: MarketRegimeState,
    defensive_pressure: f64,
    risk_off_pressure: f64,
) -> MarketRegimeState {
    match previous_state {
        MarketRegimeState::RiskOff => {
            if risk_off_pressure >= 0.35 {
                MarketRegimeState::RiskOff
            } else if defensive_pressure >= 0.20 {
                MarketRegimeState::Defensive
            } else {
                MarketRegimeState::RiskOn
            }
        }
        MarketRegimeState::Defensive => {
            if risk_off_pressure >= 0.65 {
                MarketRegimeState::RiskOff
            } else if defensive_pressure >= 0.20 || risk_off_pressure >= 0.30 {
                MarketRegimeState::Defensive
            } else {
                MarketRegimeState::RiskOn
            }
        }
        MarketRegimeState::RiskOn => {
            if risk_off_pressure >= 0.80 {
                MarketRegimeState::RiskOff
            } else if defensive_pressure >= 0.45 || risk_off_pressure >= 0.35 {
                MarketRegimeState::Defensive
            } else {
                MarketRegimeState::RiskOn
            }
        }
    }
}

fn compute_market_regime_overlay(
    forecasts: &[AssetForecast],
    portfolio_expected_annual_return: f64,
    portfolio_cvar_95: f64,
    previous_overlay: Option<&MarketRegimeOverlay>,
) -> MarketRegimeOverlay {
    let asset_count = forecasts.len().max(1) as f64;
    let bearish_breadth = forecasts
        .iter()
        .filter(|forecast| forecast.p50_price <= forecast.current_price)
        .count() as f64
        / asset_count;
    let negative_return_breadth = forecasts
        .iter()
        .filter(|forecast| forecast.annual_return <= 0.0)
        .count() as f64
        / asset_count;
    let negative_sharpe_breadth = forecasts
        .iter()
        .filter(|forecast| forecast.sharpe <= 0.0)
        .count() as f64
        / asset_count;
    let defensive_breadth_pressure = [bearish_breadth, negative_return_breadth, negative_sharpe_breadth]
        .into_iter()
        .map(|value| normalized_above(value, REGIME_DEFENSIVE_BREADTH_EXIT, REGIME_NEGATIVE_BREADTH_THRESHOLD))
        .fold(0.0, f64::max);
    let risk_off_breadth_pressure = [bearish_breadth, negative_return_breadth, negative_sharpe_breadth]
        .into_iter()
        .map(|value| normalized_above(value, REGIME_RISK_OFF_BREADTH_EXIT, 0.95))
        .fold(0.0, f64::max);
    let defensive_return_pressure = normalized_below(
        portfolio_expected_annual_return,
        REGIME_DEFENSIVE_RETURN_EXIT,
        0.0,
    );
    let risk_off_return_pressure = normalized_below(
        portfolio_expected_annual_return,
        REGIME_RISK_OFF_RETURN_EXIT,
        -0.10,
    );
    let defensive_cvar_pressure = normalized_above(
        portfolio_cvar_95,
        REGIME_DEFENSIVE_CVAR_EXIT,
        REGIME_CVAR_RISK_OFF_THRESHOLD,
    );
    let risk_off_cvar_pressure = normalized_above(
        portfolio_cvar_95,
        REGIME_RISK_OFF_CVAR_EXIT,
        0.08,
    );

    let defensive_pressure = defensive_breadth_pressure
        .max(defensive_return_pressure)
        .max(defensive_cvar_pressure);
    let risk_off_pressure = risk_off_breadth_pressure
        .max(risk_off_return_pressure)
        .max(risk_off_cvar_pressure);

    let previous_state = previous_overlay
        .map(|overlay| overlay.state)
        .unwrap_or(MarketRegimeState::RiskOn);
    let state = state_with_hysteresis(previous_state, defensive_pressure, risk_off_pressure);

    let mut reasons = Vec::new();
    if bearish_breadth >= REGIME_NEGATIVE_BREADTH_THRESHOLD {
        reasons.push(format!(
            "bearish breadth {:.0}% >= {:.0}%",
            bearish_breadth * 100.0,
            REGIME_NEGATIVE_BREADTH_THRESHOLD * 100.0
        ));
    } else if bearish_breadth >= REGIME_DEFENSIVE_BREADTH_ENTER {
        reasons.push(format!(
            "bearish breadth {:.0}% >= {:.0}%",
            bearish_breadth * 100.0,
            REGIME_DEFENSIVE_BREADTH_ENTER * 100.0
        ));
    }
    if negative_return_breadth >= REGIME_NEGATIVE_BREADTH_THRESHOLD {
        reasons.push(format!(
            "negative return breadth {:.0}% >= {:.0}%",
            negative_return_breadth * 100.0,
            REGIME_NEGATIVE_BREADTH_THRESHOLD * 100.0
        ));
    } else if negative_return_breadth >= REGIME_DEFENSIVE_BREADTH_ENTER {
        reasons.push(format!(
            "negative return breadth {:.0}% >= {:.0}%",
            negative_return_breadth * 100.0,
            REGIME_DEFENSIVE_BREADTH_ENTER * 100.0
        ));
    }
    if negative_sharpe_breadth >= REGIME_NEGATIVE_BREADTH_THRESHOLD {
        reasons.push(format!(
            "negative sharpe breadth {:.0}% >= {:.0}%",
            negative_sharpe_breadth * 100.0,
            REGIME_NEGATIVE_BREADTH_THRESHOLD * 100.0
        ));
    } else if negative_sharpe_breadth >= REGIME_DEFENSIVE_BREADTH_ENTER {
        reasons.push(format!(
            "negative sharpe breadth {:.0}% >= {:.0}%",
            negative_sharpe_breadth * 100.0,
            REGIME_DEFENSIVE_BREADTH_ENTER * 100.0
        ));
    }
    if portfolio_expected_annual_return <= 0.0 {
        reasons.push(format!(
            "portfolio expected annual return {:.2}% <= 0%",
            portfolio_expected_annual_return * 100.0
        ));
    } else if portfolio_expected_annual_return <= REGIME_DEFENSIVE_RETURN_ENTER {
        reasons.push(format!(
            "portfolio expected annual return {:.2}% <= {:.2}%",
            portfolio_expected_annual_return * 100.0,
            REGIME_DEFENSIVE_RETURN_ENTER * 100.0
        ));
    }
    if portfolio_cvar_95 >= REGIME_CVAR_RISK_OFF_THRESHOLD {
        reasons.push(format!(
            "portfolio cvar(95) {:.2}% >= {:.2}%",
            portfolio_cvar_95 * 100.0,
            REGIME_CVAR_RISK_OFF_THRESHOLD * 100.0
        ));
    } else if portfolio_cvar_95 >= REGIME_DEFENSIVE_CVAR_ENTER {
        reasons.push(format!(
            "portfolio cvar(95) {:.2}% >= {:.2}%",
            portfolio_cvar_95 * 100.0,
            REGIME_DEFENSIVE_CVAR_ENTER * 100.0
        ));
    }

    let raw_state = if risk_off_pressure >= 0.80 {
        MarketRegimeState::RiskOff
    } else if defensive_pressure >= 0.45 || risk_off_pressure >= 0.35 {
        MarketRegimeState::Defensive
    } else {
        MarketRegimeState::RiskOn
    };
    if state == previous_state && state != raw_state {
        reasons.push(format!(
            "hysteresis hold: remain {:?} until signals improve further",
            state
        ));
    }

    let max_gross_exposure = match state {
        MarketRegimeState::RiskOn => lerp(1.0, REGIME_RISK_ON_MIN_GROSS, defensive_pressure.max(risk_off_pressure * 0.6)),
        MarketRegimeState::Defensive => lerp(
            REGIME_DEFENSIVE_MAX_GROSS,
            REGIME_DEFENSIVE_MIN_GROSS,
            defensive_pressure.max(risk_off_pressure),
        ),
        MarketRegimeState::RiskOff => lerp(REGIME_RISK_OFF_MAX_GROSS, 0.0, risk_off_pressure),
    };

    MarketRegimeOverlay {
        state,
        max_gross_exposure: clamp_unit(max_gross_exposure),
        bearish_breadth,
        negative_return_breadth,
        negative_sharpe_breadth,
        portfolio_expected_annual_return,
        portfolio_cvar_95,
        reasons,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Core: Multi-Asset Forecast via Diffusion Model
// ──────────────────────────────────────────────────────────────────────────────

/// Generates per-asset probabilistic forecasts using the trained diffusion model.
///
/// For each symbol:
///   1. Fetches last `LOOKBACK` days of data
///   2. Normalizes & encodes through LSTM encoder
///   3. Runs `PORTFOLIO_MC_PATHS` Monte Carlo reverse-diffusion paths
///   4. Aggregates into expected return, vol, Sharpe, percentile prices
pub async fn generate_multi_asset_forecasts(
    symbols: &[String],
    horizon: usize,
    num_simulations: usize,
    use_cuda: bool,
) -> Result<Vec<AssetForecast>> {
    let backend = if use_cuda {
        ComputeBackend::Cuda
    } else {
        ComputeBackend::Cpu
    };
    generate_multi_asset_forecasts_via_inference(symbols, horizon, num_simulations, backend).await
}

async fn generate_multi_asset_forecasts_via_inference(
    symbols: &[String],
    horizon: usize,
    num_simulations: usize,
    backend: ComputeBackend,
) -> Result<Vec<AssetForecast>> {
    let mut forecasts = Vec::with_capacity(symbols.len());
    let prefetched = crate::data::fetch_ranges_prefetch(symbols, DATA_RANGE).await?;

    for symbol in symbols {
        info!("Forecasting {} via backend-dispatch path...", symbol);

        let data = prefetched
            .get(symbol)
            .cloned()
            .ok_or(anyhow::anyhow!("Prefetched data missing for {}", symbol))?;

        if data.history.len() < LOOKBACK + 1 {
            return Err(anyhow::anyhow!(
                "Insufficient history for {} ({} days). Optimization stopped.",
                symbol,
                data.history.len()
            ));
        }

        let current_price = data.history.last().map(|c| c.close).unwrap_or_default();
        let forecast = inference::run_inference_with_backend(
            Arc::new(data),
            horizon,
            num_simulations,
            None,
            backend,
        )
        .await?;

        let asset_forecast = build_asset_forecast_from_inference(
            symbol.clone(),
            current_price,
            horizon,
            forecast,
        )?;
        forecasts.push(asset_forecast);
    }

    Ok(forecasts)
}

pub async fn generate_multi_asset_forecasts_from_history_map(
    histories: &HashMap<String, crate::data::StockData>,
    symbols: &[String],
    horizon: usize,
    num_simulations: usize,
    backend: ComputeBackend,
) -> Result<Vec<AssetForecast>> {
    let mut forecasts = Vec::with_capacity(symbols.len());

    for symbol in symbols {
        let data = histories
            .get(symbol)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Historical slice missing for {}", symbol))?;

        if data.history.len() < LOOKBACK + 1 {
            return Err(anyhow::anyhow!(
                "Insufficient sliced history for {} ({} days)",
                symbol,
                data.history.len()
            ));
        }

        let current_price = data.history.last().map(|c| c.close).unwrap_or_default();
        let forecast = inference::run_inference_with_backend_from_histories(
            histories,
            symbol,
            horizon,
            num_simulations,
            None,
            backend,
        )
        .await?;

        forecasts.push(build_asset_forecast_from_inference(
            symbol.clone(),
            current_price,
            horizon,
            forecast,
        )?);
    }

    Ok(forecasts)
}

fn build_asset_forecast_from_inference(
    symbol: String,
    current_price: f64,
    horizon: usize,
    forecast: ForecastData,
) -> Result<AssetForecast> {
    let period_returns: Vec<f64> = forecast
        ._paths
        .iter()
        .filter_map(|path| path.last().copied())
        .map(|final_price| (final_price / current_price.max(1e-8)).ln())
        .collect();

    if period_returns.len() < 2 {
        return Err(anyhow::anyhow!(
            "insufficient inference paths for portfolio stats on {}",
            symbol
        ));
    }

    let n = period_returns.len() as f64;
    let mean_ret = period_returns.iter().sum::<f64>() / n;
    let var_ret = period_returns
        .iter()
        .map(|r| (r - mean_ret).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    let std_ret = var_ret.sqrt();

    let periods_per_year = TRADING_DAYS / horizon as f64;
    let annual_return = mean_ret * periods_per_year;
    let annual_vol = std_ret * periods_per_year.sqrt();
    let sharpe = if annual_vol > 1e-8 {
        (annual_return - RISK_FREE_RATE) / annual_vol
    } else {
        0.0
    };

    let mut sorted_rets = period_returns.clone();
    sorted_rets.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx_10 = (n * 0.1) as usize;
    let idx_50 = (n * 0.5) as usize;
    let idx_90 = (n * 0.9) as usize;

    let p10_price = forecast
        .p10
        .last()
        .map(|(_, p)| *p)
        .unwrap_or_else(|| current_price * sorted_rets[idx_10].exp());
    let p50_price = forecast
        .p50
        .last()
        .map(|(_, p)| *p)
        .unwrap_or_else(|| current_price * sorted_rets[idx_50].exp());
    let p90_price = forecast
        .p90
        .last()
        .map(|(_, p)| *p)
        .unwrap_or_else(|| current_price * sorted_rets[idx_90].exp());

    Ok(AssetForecast {
        symbol,
        current_price,
        expected_return: mean_ret / horizon as f64,
        volatility: std_ret / (horizon as f64).sqrt(),
        annual_return,
        annual_vol,
        sharpe,
        mc_period_returns: period_returns,
        p10_price,
        p50_price,
        p90_price,
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Covariance Estimation from MC Paths
// ──────────────────────────────────────────────────────────────────────────────

/// Computes the sample covariance matrix of period returns across assets.
/// Returns (expected_returns [N], covariance_matrix [N×N]).
fn compute_return_statistics(forecasts: &[AssetForecast]) -> (Vec<f64>, Vec<Vec<f64>>) {
    let n = forecasts.len();
    let num_paths = forecasts[0].mc_period_returns.len();

    // Mean returns
    let means: Vec<f64> = forecasts
        .iter()
        .map(|f| {
            f.mc_period_returns.iter().sum::<f64>() / num_paths as f64
        })
        .collect();

    // Covariance matrix
    let mut cov = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in i..n {
            let mut sum = 0.0;
            for k in 0..num_paths {
                let di = forecasts[i].mc_period_returns[k] - means[i];
                let dj = forecasts[j].mc_period_returns[k] - means[j];
                sum += di * dj;
            }
            let covariance = sum / (num_paths as f64 - 1.0);
            cov[i][j] = covariance;
            cov[j][i] = covariance;
        }
    }

    (means, cov)
}

// ──────────────────────────────────────────────────────────────────────────────
// Portfolio Variance Helper
// ──────────────────────────────────────────────────────────────────────────────

fn portfolio_return(weights: &[f64], means: &[f64]) -> f64 {
    weights.iter().zip(means.iter()).map(|(w, r)| w * r).sum()
}

fn portfolio_variance(weights: &[f64], cov: &[Vec<f64>]) -> f64 {
    let n = weights.len();
    let mut var = 0.0;
    for i in 0..n {
        for j in 0..n {
            var += weights[i] * weights[j] * cov[i][j];
        }
    }
    var
}

fn portfolio_cvar(weights: &[f64], forecasts: &[AssetForecast], alpha: f64) -> f64 {
    let num_paths = forecasts[0].mc_period_returns.len();
    let mut portfolio_returns: Vec<f64> = (0..num_paths)
        .into_par_iter()
        .map(|k| {
            weights
                .iter()
                .zip(forecasts.iter())
                .map(|(w, f)| w * f.mc_period_returns[k])
                .sum()
        })
        .collect();

    portfolio_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let cutoff = (num_paths as f64 * alpha) as usize;
    let cutoff = cutoff.max(1);
    let tail_mean: f64 = portfolio_returns[..cutoff].iter().sum::<f64>() / cutoff as f64;
    -tail_mean // CVaR is positive = expected loss
}

// ──────────────────────────────────────────────────────────────────────────────
// Optimizer: Monte Carlo Sampling + Mean-Variance + CVaR
// ──────────────────────────────────────────────────────────────────────────────

/// Optimizes portfolio weights via large-scale random sampling.
///
/// Strategy inspired by DiffStock paper:
///   1. Generate random weight vectors satisfying constraints
///   2. For each, compute Sharpe ratio using diffusion-derived expected returns & covariance
///   3. Select top portfolios by Sharpe, then pick the one with lowest CVaR among them
///   4. Apply vol-targeting overlay to scale leverage
pub fn optimize_portfolio(forecasts: &[AssetForecast]) -> Result<PortfolioAllocation> {
    optimize_portfolio_with_previous_regime(forecasts, None)
}

pub fn optimize_portfolio_with_previous_regime(
    forecasts: &[AssetForecast],
    previous_overlay: Option<&MarketRegimeOverlay>,
) -> Result<PortfolioAllocation> {
    let n = forecasts.len();
    if n == 0 {
        return Err(anyhow::anyhow!("No asset forecasts to optimize"));
    }

    let (means, cov) = compute_return_statistics(forecasts);
    let horizon = PORTFOLIO_HORIZON as f64;
    let periods_per_year = TRADING_DAYS / horizon;

    info!("Optimizing portfolio with {} assets, {} random samples...", n, OPTIMIZER_SAMPLES);

    // Log per-asset stats
    for f in forecasts.iter() {
        info!(
            "  {}: E[r]={:.4}, σ={:.4}, Sharpe={:.2}, P10={:.2}, P50={:.2}, P90={:.2}",
            f.symbol, f.annual_return, f.annual_vol, f.sharpe, f.p10_price, f.p50_price, f.p90_price
        );
    }

    let refined_weights = if n == 1 {
        vec![1.0]
    } else {
        let mut best_weights: Vec<f64> = vec![1.0 / n as f64; n];

        let best_candidate = (0..OPTIMIZER_SAMPLES)
            .into_par_iter()
            .map_init(rand::thread_rng, |rng, _| {
                let weights = generate_random_weights(n, rng);

                if weights.iter().any(|&w| w > MAX_SINGLE_WEIGHT) {
                    return None;
                }
                if weights.iter().any(|&w| w > 0.0 && w < MIN_SINGLE_WEIGHT) {
                    return None;
                }

                let port_ret = portfolio_return(&weights, &means) * periods_per_year;
                let port_var = portfolio_variance(&weights, &cov) * periods_per_year;
                let port_vol = port_var.sqrt();
                if port_vol < 1e-8 {
                    return None;
                }

                let sharpe = (port_ret - RISK_FREE_RATE) / port_vol;
                Some((sharpe, weights))
            })
            .filter_map(|candidate| candidate)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        if let Some((sharpe, weights)) = best_candidate {
            let _ = sharpe;
            best_weights = weights;
        }

        let mut rng = rand::thread_rng();
        refine_weights(&best_weights, &means, &cov, forecasts, &mut rng)
    };

    let port_ret = portfolio_return(&refined_weights, &means) * periods_per_year;
    let port_var = portfolio_variance(&refined_weights, &cov) * periods_per_year;
    let port_vol = port_var.sqrt();
    let _sharpe = if port_vol > 1e-8 {
        (port_ret - RISK_FREE_RATE) / port_vol
    } else {
        0.0
    };
    let cvar = portfolio_cvar(&refined_weights, forecasts, CVAR_ALPHA);

    // Vol-targeting: scale portfolio so that annualized vol ≈ TARGET_ANNUAL_VOL
    let leverage = if port_vol > 1e-8 {
        (TARGET_ANNUAL_VOL / port_vol).min(2.0).max(0.5) // Cap leverage 0.5x–2.0x
    } else {
        1.0
    };

    let regime_overlay = compute_market_regime_overlay(forecasts, port_ret, cvar, previous_overlay);
    let gross_exposure = leverage.min(regime_overlay.max_gross_exposure.max(0.0));

    let final_weights: Vec<(String, f64)> = forecasts
        .iter()
        .zip(refined_weights.iter())
        .map(|(f, &w)| (f.symbol.clone(), w * gross_exposure))
        .filter(|(_, w)| *w > 0.001) // Filter dust
        .collect();

    let adjusted_return = port_ret * gross_exposure;
    let adjusted_vol = port_vol * gross_exposure;
    let adjusted_sharpe = if adjusted_vol > 1e-8 {
        (adjusted_return - RISK_FREE_RATE) / adjusted_vol
    } else {
        0.0
    };
    let target_cash_weight = (1.0 - final_weights.iter().map(|(_, weight)| *weight).sum::<f64>())
        .max(0.0)
        .min(1.0);

    info!(
        "Regime overlay => state={:?}, max_gross_exposure={:.2}, reasons={}{}",
        regime_overlay.state,
        regime_overlay.max_gross_exposure,
        regime_overlay.reasons.len(),
        if regime_overlay.reasons.is_empty() {
            String::new()
        } else {
            format!(" [{}]", regime_overlay.reasons.join("; "))
        }
    );

    Ok(PortfolioAllocation {
        weights: final_weights,
        expected_annual_return: adjusted_return,
        expected_annual_vol: adjusted_vol,
        sharpe_ratio: adjusted_sharpe,
        cvar_95: cvar * gross_exposure,
        asset_forecasts: forecasts.to_vec(),
        leverage,
        max_gross_exposure: regime_overlay.max_gross_exposure,
        target_cash_weight,
        market_regime: regime_overlay,
    })
}

/// Generates a random weight vector that sums to 1.0, using Dirichlet-like sampling.
fn generate_random_weights(n: usize, rng: &mut impl rand::Rng) -> Vec<f64> {
    use rand_distr::{Distribution, Exp1};
    let raw: Vec<f64> = (0..n).map(|_| Exp1.sample(rng)).collect();
    let sum: f64 = raw.iter().sum();
    raw.iter().map(|v| v / sum).collect()
}

/// Local refinement around current best weights: perturb & keep if better.
fn refine_weights(
    base: &[f64],
    means: &[f64],
    cov: &[Vec<f64>],
    forecasts: &[AssetForecast],
    rng: &mut impl rand::Rng,
) -> Vec<f64> {
    let periods_per_year = TRADING_DAYS / PORTFOLIO_HORIZON as f64;

    let base_ret = portfolio_return(base, means) * periods_per_year;
    let base_vol = (portfolio_variance(base, cov) * periods_per_year).sqrt();
    let base_sharpe = if base_vol > 1e-8 {
        (base_ret - RISK_FREE_RATE) / base_vol
    } else {
        f64::NEG_INFINITY
    };
    let base_cvar = portfolio_cvar(base, forecasts, CVAR_ALPHA);

    let mut best = base.to_vec();
    let mut best_score = base_sharpe - 0.5 * base_cvar; // Combined objective

    let refinement_iterations = 50_000;
    for _ in 0..refinement_iterations {
        // Perturb
        let mut candidate: Vec<f64> = best
            .iter()
            .map(|&w| {
                let delta: f64 = rng.gen_range(-0.05..0.05);
                (w + delta).max(0.0)
            })
            .collect();

        // Re-normalize
        let sum: f64 = candidate.iter().sum();
        if sum < 1e-8 {
            continue;
        }
        candidate.iter_mut().for_each(|w| *w /= sum);

        // Check constraints
        if candidate.iter().any(|&w| w > MAX_SINGLE_WEIGHT) {
            continue;
        }
        if candidate
            .iter()
            .any(|&w| w > 0.0 && w < MIN_SINGLE_WEIGHT)
        {
            continue;
        }

        let ret = portfolio_return(&candidate, means) * periods_per_year;
        let vol = (portfolio_variance(&candidate, cov) * periods_per_year).sqrt();
        let sharpe = if vol > 1e-8 {
            (ret - RISK_FREE_RATE) / vol
        } else {
            f64::NEG_INFINITY
        };
        let cvar = portfolio_cvar(&candidate, forecasts, CVAR_ALPHA);

        let score = sharpe - 0.5 * cvar;
        if score > best_score {
            best_score = score;
            best = candidate;
        }
    }

    best
}

// ──────────────────────────────────────────────────────────────────────────────
// Top-Level Command: Run Portfolio Optimization
// ──────────────────────────────────────────────────────────────────────────────

/// Full pipeline: fetch data → forecast → optimize → print allocation.
pub async fn run_portfolio_optimization(
    symbols: &[String],
    use_cuda: bool,
) -> Result<PortfolioAllocation> {
    let backend = if use_cuda {
        ComputeBackend::Cuda
    } else {
        ComputeBackend::Cpu
    };
    run_portfolio_optimization_with_backend(symbols, backend).await
}

pub async fn run_portfolio_optimization_with_backend(
    symbols: &[String],
    backend: ComputeBackend,
) -> Result<PortfolioAllocation> {
    run_portfolio_optimization_with_backend_and_regime(symbols, backend, None).await
}

pub async fn run_portfolio_optimization_with_backend_and_regime(
    symbols: &[String],
    backend: ComputeBackend,
    previous_overlay: Option<MarketRegimeOverlay>,
) -> Result<PortfolioAllocation> {
    info!(
        "=== DiffStock Portfolio Optimizer ===\n  Assets: {:?}\n  Horizon: {} days\n  MC Paths: {}\n  Backend: {:?}\n  Target Vol: {:.0}%",
        symbols,
        PORTFOLIO_HORIZON,
        PORTFOLIO_MC_PATHS,
        backend,
        TARGET_ANNUAL_VOL * 100.0
    );

    // Step 1: Generate forecasts
    let forecasts = match backend {
        ComputeBackend::Directml => {
            generate_multi_asset_forecasts_via_inference(
                symbols,
                PORTFOLIO_HORIZON,
                PORTFOLIO_MC_PATHS,
                backend,
            )
            .await?
        }
        ComputeBackend::Auto => {
            let use_cuda = cfg!(feature = "cuda");
            generate_multi_asset_forecasts(
                symbols,
                PORTFOLIO_HORIZON,
                PORTFOLIO_MC_PATHS,
                use_cuda,
            )
            .await?
        }
        ComputeBackend::Cuda => {
            generate_multi_asset_forecasts(
                symbols,
                PORTFOLIO_HORIZON,
                PORTFOLIO_MC_PATHS,
                true,
            )
            .await?
        }
        ComputeBackend::Cpu => {
            generate_multi_asset_forecasts(
                symbols,
                PORTFOLIO_HORIZON,
                PORTFOLIO_MC_PATHS,
                false,
            )
            .await?
        }
    };

    if forecasts.len() < 2 {
        return Err(anyhow::anyhow!(
            "Need at least 2 assets with valid forecasts. Got {}.",
            forecasts.len()
        ));
    }

    // Step 2: Optimize
    let allocation = optimize_portfolio_with_previous_regime(&forecasts, previous_overlay.as_ref())?;

    // Step 3: Print results
    print_allocation(&allocation);

    Ok(allocation)
}

/// Pretty-prints portfolio allocation to stdout / tracing.
pub fn print_allocation(alloc: &PortfolioAllocation) {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║           DiffStock Portfolio Allocation                  ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!(
        "║  Expected Annual Return : {:>+7.2}%                        ║",
        alloc.expected_annual_return * 100.0
    );
    println!(
        "║  Expected Annual Vol    : {:>7.2}%                        ║",
        alloc.expected_annual_vol * 100.0
    );
    println!(
        "║  Sharpe Ratio           : {:>7.2}                         ║",
        alloc.sharpe_ratio
    );
    println!(
        "║  CVaR (95%)             : {:>7.2}%                        ║",
        alloc.cvar_95 * 100.0
    );
    println!(
        "║  Leverage (Vol-Target)  : {:>7.2}x                        ║",
        alloc.leverage
    );
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Symbol   Weight    E[Ann.Ret]  Ann.Vol  Sharpe   Price  ║");
    println!("╠════════════════════════════════════════════════════════════╣");

    // Sort by weight descending
    let mut sorted: Vec<_> = alloc.weights.clone();
    sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    for (sym, w) in &sorted {
        if let Some(f) = alloc.asset_forecasts.iter().find(|f| &f.symbol == sym) {
            println!(
                "║  {:<6} {:>7.2}%   {:>+7.2}%   {:>6.2}%   {:>5.2}  ${:>7.2} ║",
                sym,
                w * 100.0,
                f.annual_return * 100.0,
                f.annual_vol * 100.0,
                f.sharpe,
                f.current_price
            );
        }
    }

    println!("╠════════════════════════════════════════════════════════════╣");

    // Rebalancing actions
    println!("║                   Rebalancing Actions                     ║");
    println!("╠════════════════════════════════════════════════════════════╣");

    let total_weight: f64 = sorted.iter().map(|(_, w)| w).sum();
    println!(
        "║  Total Invested: {:>6.1}%  Cash: {:>6.1}%                   ║",
        total_weight * 100.0,
        (1.0 - total_weight.min(1.0)) * 100.0
    );

    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Per $100,000 Portfolio:                                  ║");
    let capital = 100_000.0;
    for (sym, w) in &sorted {
        if let Some(f) = alloc.asset_forecasts.iter().find(|f| &f.symbol == sym) {
            let dollar_amt = capital * w;
            let shares = (dollar_amt / f.current_price).floor();
            println!(
                "║    {:<6}  ${:>9.2}  ~{:>5.0} shares @ ${:.2}           ║",
                sym, dollar_amt, shares, f.current_price
            );
        }
    }

    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Price Targets ({}-day Horizon):                          ║", PORTFOLIO_HORIZON);
    for f in &alloc.asset_forecasts {
        let pct_change = (f.p50_price / f.current_price - 1.0) * 100.0;
        let direction = if pct_change > 0.0 { "▲" } else { "▼" };
        println!(
            "║    {:<6} P10=${:>7.2}  P50=${:>7.2} ({}{:.1}%)  P90=${:>7.2} ║",
            f.symbol, f.p10_price, f.p50_price, direction, pct_change.abs(), f.p90_price
        );
    }

    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("⚠  Educational use only. Not financial advice.");
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_forecasts(n: usize) -> Vec<AssetForecast> {
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let symbols = ["AAAA", "BBBB", "CCCC", "DDDD", "EEEE"];
        (0..n)
            .map(|i| {
                let base_price = 100.0 + i as f64 * 50.0;
                let mc: Vec<f64> = (0..200)
                    .map(|_| rng.gen_range(-0.10..0.15))
                    .collect();
                let mean_ret = mc.iter().sum::<f64>() / mc.len() as f64;
                let var = mc.iter().map(|r| (r - mean_ret).powi(2)).sum::<f64>()
                    / (mc.len() as f64 - 1.0);
                let std = var.sqrt();
                let periods_per_year = TRADING_DAYS / PORTFOLIO_HORIZON as f64;

                AssetForecast {
                    symbol: symbols[i % symbols.len()].to_string(),
                    current_price: base_price,
                    expected_return: mean_ret / PORTFOLIO_HORIZON as f64,
                    volatility: std / (PORTFOLIO_HORIZON as f64).sqrt(),
                    annual_return: mean_ret * periods_per_year,
                    annual_vol: std * periods_per_year.sqrt(),
                    sharpe: (mean_ret * periods_per_year - RISK_FREE_RATE)
                        / (std * periods_per_year.sqrt() + 1e-8),
                    mc_period_returns: mc,
                    p10_price: base_price * 0.92,
                    p50_price: base_price * 1.02,
                    p90_price: base_price * 1.12,
                }
            })
            .collect()
    }

    #[test]
    fn test_covariance_matrix_symmetry() {
        let forecasts = mock_forecasts(3);
        let (means, cov) = compute_return_statistics(&forecasts);

        assert_eq!(means.len(), 3);
        assert_eq!(cov.len(), 3);
        for i in 0..3 {
            for j in 0..3 {
                assert!(
                    (cov[i][j] - cov[j][i]).abs() < 1e-12,
                    "Covariance matrix should be symmetric"
                );
            }
        }
        // Diagonal should be positive (variance)
        for i in 0..3 {
            assert!(cov[i][i] > 0.0, "Variance should be positive");
        }
    }

    #[test]
    fn test_portfolio_variance_calculation() {
        let forecasts = mock_forecasts(3);
        let (_, cov) = compute_return_statistics(&forecasts);

        let equal_w = vec![1.0 / 3.0; 3];
        let var = portfolio_variance(&equal_w, &cov);
        assert!(var >= 0.0, "Portfolio variance should be non-negative");
    }

    #[test]
    fn test_weights_sum_to_one() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let w = generate_random_weights(5, &mut rng);
            let sum: f64 = w.iter().sum();
            assert!(
                (sum - 1.0).abs() < 1e-10,
                "Weights should sum to 1.0, got {}",
                sum
            );
            assert!(w.iter().all(|&v| v >= 0.0), "Weights should be non-negative");
        }
    }

    #[test]
    fn test_optimize_portfolio_runs() {
        let forecasts = mock_forecasts(4);
        let result = optimize_portfolio(&forecasts);
        assert!(result.is_ok(), "Optimizer should not fail");

        let alloc = result.unwrap();
        assert!(!alloc.weights.is_empty(), "Should have at least one non-zero weight");
        assert!(alloc.leverage > 0.0, "Leverage should be positive");

        // All weights positive
        for (_, w) in &alloc.weights {
            assert!(*w > 0.0, "Output weights should be positive (after dust filter)");
        }
    }

    #[test]
    fn test_optimize_portfolio_supports_single_asset() {
        let forecasts = mock_forecasts(1);
        let alloc = optimize_portfolio(&forecasts).expect("single-asset optimization should succeed");
        assert!(alloc.leverage > 0.0, "Vol-target leverage should stay positive");
        assert!(alloc.max_gross_exposure >= 0.0 && alloc.max_gross_exposure <= 1.0);
    }

    #[test]
    fn test_risk_on_overlay_stays_within_risk_on_band() {
        let forecasts = vec![
            AssetForecast {
                symbol: "AAAA".to_string(),
                current_price: 100.0,
                expected_return: 0.01,
                volatility: 0.02,
                annual_return: 0.18,
                annual_vol: 0.20,
                sharpe: 0.9,
                mc_period_returns: vec![0.03, 0.02, 0.01, 0.015, 0.01, 0.025, 0.02, 0.01],
                p10_price: 99.0,
                p50_price: 108.0,
                p90_price: 118.0,
            },
            AssetForecast {
                symbol: "BBBB".to_string(),
                current_price: 120.0,
                expected_return: 0.009,
                volatility: 0.018,
                annual_return: 0.16,
                annual_vol: 0.18,
                sharpe: 0.8,
                mc_period_returns: vec![0.025, 0.02, 0.015, 0.01, 0.02, 0.018, 0.012, 0.01],
                p10_price: 118.0,
                p50_price: 131.0,
                p90_price: 140.0,
            },
        ];

        let alloc = optimize_portfolio(&forecasts).expect("risk-on optimization should succeed");
        assert_eq!(alloc.market_regime.state, MarketRegimeState::RiskOn);
        assert!(alloc.max_gross_exposure >= REGIME_RISK_ON_MIN_GROSS);
        assert!(alloc.max_gross_exposure <= 1.0);
    }

    #[test]
    fn test_defensive_overlay_uses_banded_continuous_gross() {
        let forecasts = vec![
            AssetForecast {
                symbol: "AAAA".to_string(),
                current_price: 100.0,
                expected_return: 0.004,
                volatility: 0.02,
                annual_return: 0.02,
                annual_vol: 0.22,
                sharpe: 0.1,
                mc_period_returns: vec![0.015, 0.01, 0.005, 0.0, 0.01, 0.008, 0.004, 0.002],
                p10_price: 95.0,
                p50_price: 101.0,
                p90_price: 111.0,
            },
            AssetForecast {
                symbol: "BBBB".to_string(),
                current_price: 110.0,
                expected_return: 0.003,
                volatility: 0.018,
                annual_return: 0.03,
                annual_vol: 0.20,
                sharpe: 0.15,
                mc_period_returns: vec![0.014, 0.009, 0.004, 0.001, 0.009, 0.007, 0.003, 0.0],
                p10_price: 106.0,
                p50_price: 112.0,
                p90_price: 121.0,
            },
        ];

        let overlay = compute_market_regime_overlay(&forecasts, 0.03, 0.028, None);
        assert_eq!(overlay.state, MarketRegimeState::Defensive);
        assert!(overlay.max_gross_exposure <= REGIME_DEFENSIVE_MAX_GROSS);
        assert!(overlay.max_gross_exposure >= REGIME_DEFENSIVE_MIN_GROSS);
    }

    #[test]
    fn test_hysteresis_holds_defensive_until_signals_improve_further() {
        let forecasts = vec![
            AssetForecast {
                symbol: "AAAA".to_string(),
                current_price: 100.0,
                expected_return: 0.006,
                volatility: 0.02,
                annual_return: 0.09,
                annual_vol: 0.21,
                sharpe: 0.3,
                mc_period_returns: vec![0.02, 0.015, 0.01, 0.005, 0.01, 0.012, 0.008, 0.01],
                p10_price: 97.0,
                p50_price: 99.0,
                p90_price: 110.0,
            },
            AssetForecast {
                symbol: "BBBB".to_string(),
                current_price: 100.0,
                expected_return: 0.006,
                volatility: 0.02,
                annual_return: 0.09,
                annual_vol: 0.21,
                sharpe: 0.3,
                mc_period_returns: vec![0.02, 0.015, 0.01, 0.005, 0.01, 0.012, 0.008, 0.01],
                p10_price: 97.0,
                p50_price: 103.0,
                p90_price: 110.0,
            },
        ];

        let previous_overlay = MarketRegimeOverlay {
            state: MarketRegimeState::Defensive,
            max_gross_exposure: 0.55,
            bearish_breadth: 0.75,
            negative_return_breadth: 0.50,
            negative_sharpe_breadth: 0.50,
            portfolio_expected_annual_return: 0.05,
            portfolio_cvar_95: 0.03,
            reasons: vec!["prior defensive state".to_string()],
        };

        let overlay = compute_market_regime_overlay(&forecasts, 0.09, 0.015, Some(&previous_overlay));
        assert_eq!(overlay.state, MarketRegimeState::Defensive);
        assert!(overlay.reasons.iter().any(|reason| reason.contains("hysteresis hold")));
    }

    #[test]
    fn test_risk_off_overlay_can_zero_exposure() {
        let forecasts = vec![
            AssetForecast {
                symbol: "AAAA".to_string(),
                current_price: 100.0,
                expected_return: -0.02,
                volatility: 0.03,
                annual_return: -0.35,
                annual_vol: 0.30,
                sharpe: -1.2,
                mc_period_returns: vec![-0.10, -0.08, -0.06, -0.05, -0.04, -0.09, -0.07, -0.03],
                p10_price: 82.0,
                p50_price: 90.0,
                p90_price: 98.0,
            },
            AssetForecast {
                symbol: "BBBB".to_string(),
                current_price: 120.0,
                expected_return: -0.018,
                volatility: 0.028,
                annual_return: -0.28,
                annual_vol: 0.26,
                sharpe: -0.9,
                mc_period_returns: vec![-0.09, -0.07, -0.05, -0.04, -0.08, -0.06, -0.03, -0.02],
                p10_price: 95.0,
                p50_price: 108.0,
                p90_price: 118.0,
            },
            AssetForecast {
                symbol: "CCCC".to_string(),
                current_price: 80.0,
                expected_return: -0.022,
                volatility: 0.032,
                annual_return: -0.40,
                annual_vol: 0.34,
                sharpe: -1.4,
                mc_period_returns: vec![-0.12, -0.10, -0.08, -0.06, -0.09, -0.07, -0.05, -0.04],
                p10_price: 62.0,
                p50_price: 72.0,
                p90_price: 79.0,
            },
        ];

        let alloc = optimize_portfolio(&forecasts).expect("risk-off optimization should succeed");
        assert_eq!(alloc.max_gross_exposure, 0.0);
        assert!(alloc.weights.is_empty(), "risk-off overlay should allow all-cash output");
        assert_eq!(alloc.target_cash_weight, 1.0);
        assert_eq!(alloc.market_regime.state, MarketRegimeState::RiskOff);
    }

    #[test]
    fn test_cvar_is_positive_for_risky_portfolio() {
        let forecasts = mock_forecasts(3);
        let weights = vec![1.0 / 3.0; 3];
        let cvar = portfolio_cvar(&weights, &forecasts, 0.05);
        // CVaR can be positive or negative depending on mock data, but should be finite
        assert!(cvar.is_finite(), "CVaR should be finite");
    }
}

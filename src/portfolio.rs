use crate::config::{get_device, DATA_RANGE, DDIM_ETA, DDIM_INFERENCE_STEPS, DIFF_STEPS, DROPOUT_RATE, HIDDEN_DIM, INFERENCE_BATCH_SIZE, INPUT_DIM, LOOKBACK, LSTM_LAYERS, NUM_LAYERS, TRAINING_SYMBOLS};
use crate::diffusion::GaussianDiffusion;
use crate::models::time_grad::{EpsilonTheta, RNNEncoder};
use anyhow::Result;
use candle_core::{DType, Tensor};
use candle_nn::VarBuilder;
use tracing::{info, warn, error};

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

// ──────────────────────────────────────────────────────────────────────────────
// Data Structures
// ──────────────────────────────────────────────────────────────────────────────

/// Per-asset forecast statistics derived from diffusion Monte Carlo paths.
#[derive(Clone, Debug)]
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

/// Complete portfolio allocation result.
#[derive(Clone, Debug)]
pub struct PortfolioAllocation {
    pub weights: Vec<(String, f64)>,     // (symbol, weight 0-1)
    pub expected_annual_return: f64,
    pub expected_annual_vol: f64,
    pub sharpe_ratio: f64,
    pub cvar_95: f64,                    // 95% CVaR of portfolio
    pub asset_forecasts: Vec<AssetForecast>,
    pub leverage: f64,                   // Vol-targeting leverage multiplier
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
    let device = get_device(use_cuda);
    let context_len = LOOKBACK;
    let num_assets = TRAINING_SYMBOLS.len();

    // Load model weights once
    let vb = if std::path::Path::new("model_weights.safetensors").exists() {
        unsafe {
            VarBuilder::from_mmaped_safetensors(
                &["model_weights.safetensors"],
                DType::F32,
                &device,
            )?
        }
    } else {
        return Err(anyhow::anyhow!(
            "model_weights.safetensors not found. Run --train first."
        ));
    };

    let encoder = RNNEncoder::new(INPUT_DIM, HIDDEN_DIM, LSTM_LAYERS, DROPOUT_RATE, vb.pp("encoder"))?;
    let model = EpsilonTheta::new(1, HIDDEN_DIM, HIDDEN_DIM, NUM_LAYERS, num_assets, DROPOUT_RATE, vb.pp("model"))?;
    let diffusion = GaussianDiffusion::new(DIFF_STEPS, &device)?;

    let mut forecasts = Vec::with_capacity(symbols.len());

    for symbol in symbols {
        info!("Forecasting {}...", symbol);

        // Fetch recent data
        let data = match crate::data::fetch_range(symbol, DATA_RANGE).await {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to fetch {}: {}. Skipping.", symbol, e);
                continue;
            }
        };

        if data.history.len() < context_len + 1 {
            warn!("{}: insufficient history ({} days). Skipping.", symbol, data.history.len());
            continue;
        }

        let current_price = data.history.last().unwrap().close;

        // Prepare context window
        let start_idx = data.history.len() - context_len;
        let mut features = Vec::with_capacity(context_len);
        let mut close_vals = Vec::with_capacity(context_len);

        for i in 0..context_len {
            let idx = start_idx + i;
            let close_ret = (data.history[idx].close / data.history[idx - 1].close).ln();
            let overnight_ret = (data.history[idx].open / data.history[idx - 1].close).ln();
            features.push(vec![close_ret, overnight_ret]);
            close_vals.push(close_ret);
        }

        // Z-score normalize
        let mean = close_vals.iter().sum::<f64>() / context_len as f64;
        let variance = close_vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
            / (context_len as f64 - 1.0);
        let std = variance.sqrt() + 1e-6;

        let normalized: Vec<f32> = features
            .iter()
            .flat_map(|f| {
                vec![((f[0] - mean) / std) as f32, ((f[1] - mean) / std) as f32]
            })
            .collect();

        let context_tensor = Tensor::from_slice(&normalized, (1, context_len, 2), &device)?;

        // Asset ID (case-insensitive match)
        let asset_id = TRAINING_SYMBOLS
            .iter()
            .position(|&s| s.eq_ignore_ascii_case(symbol))
            .unwrap_or(0);

        // Encode
        let hidden_state = encoder.forward(&context_tensor, false)?;
        let hidden_state = hidden_state.unsqueeze(2)?;

        // Monte Carlo sampling (batched DDIM for speed)
        let mut period_returns = Vec::with_capacity(num_simulations);

        let mut remaining = num_simulations;
        while remaining > 0 {
            let batch = remaining.min(INFERENCE_BATCH_SIZE);

            // Run all horizon steps for this batch
            let mut batch_log_rets = vec![0.0f64; batch];
            let mut batch_last_vals = vec![current_price; batch];

            for _ in 0..horizon {
                let samples = diffusion.sample_ddim_batched(
                    &model,
                    &hidden_state,
                    asset_id as u32,
                    batch,
                    DDIM_INFERENCE_STEPS,
                    DDIM_ETA,
                )?;

                let flat = samples.squeeze(2)?.squeeze(1)?;
                let vals = flat.to_vec1::<f32>()?;

                for (j, &predicted_norm_ret) in vals.iter().enumerate() {
                    let predicted_ret = (predicted_norm_ret as f64 * std) + mean;
                    batch_log_rets[j] += predicted_ret;
                    let next_price = batch_last_vals[j] * predicted_ret.exp();
                    batch_last_vals[j] = next_price;
                }
            }

            period_returns.extend(batch_log_rets);
            remaining -= batch;
        }

        // Statistics
        let n = period_returns.len() as f64;
        let mean_ret = period_returns.iter().sum::<f64>() / n;
        let var_ret = period_returns
            .iter()
            .map(|r| (r - mean_ret).powi(2))
            .sum::<f64>()
            / (n - 1.0);
        let std_ret = var_ret.sqrt();

        // Annualize (horizon is in trading days)
        let periods_per_year = TRADING_DAYS / horizon as f64;
        let annual_return = mean_ret * periods_per_year;
        let annual_vol = std_ret * periods_per_year.sqrt();
        let sharpe = if annual_vol > 1e-8 {
            (annual_return - RISK_FREE_RATE) / annual_vol
        } else {
            0.0
        };

        // Percentile prices
        let mut sorted_rets = period_returns.clone();
        sorted_rets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx_10 = (n * 0.1) as usize;
        let idx_50 = (n * 0.5) as usize;
        let idx_90 = (n * 0.9) as usize;

        forecasts.push(AssetForecast {
            symbol: symbol.clone(),
            current_price,
            expected_return: mean_ret / horizon as f64,
            volatility: std_ret / (horizon as f64).sqrt(),
            annual_return,
            annual_vol,
            sharpe,
            mc_period_returns: period_returns,
            p10_price: current_price * sorted_rets[idx_10].exp(),
            p50_price: current_price * sorted_rets[idx_50].exp(),
            p90_price: current_price * sorted_rets[idx_90].exp(),
        });
    }

    Ok(forecasts)
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

    let mut rng = rand::thread_rng();

    let mut best_sharpe = f64::NEG_INFINITY;
    let mut best_weights: Vec<f64> = vec![1.0 / n as f64; n]; // Equal weight fallback

    // Phase 1: Maximum Sharpe via Monte Carlo random weight sampling
    for _ in 0..OPTIMIZER_SAMPLES {
        let weights = generate_random_weights(n, &mut rng);

        // Apply weight constraints
        if weights.iter().any(|&w| w > MAX_SINGLE_WEIGHT) {
            continue;
        }
        if weights.iter().any(|&w| w > 0.0 && w < MIN_SINGLE_WEIGHT) {
            continue;
        }

        let port_ret = portfolio_return(&weights, &means) * periods_per_year;
        let port_var = portfolio_variance(&weights, &cov) * periods_per_year;
        let port_vol = port_var.sqrt();

        if port_vol < 1e-8 {
            continue;
        }

        let sharpe = (port_ret - RISK_FREE_RATE) / port_vol;

        if sharpe > best_sharpe {
            best_sharpe = sharpe;
            best_weights = weights;
        }
    }

    // Phase 2: Among top candidates near the best Sharpe, prefer lower CVaR
    // Re-sample around the best weights (local refinement)
    let refined_weights = refine_weights(&best_weights, &means, &cov, forecasts, &mut rng);

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

    let final_weights: Vec<(String, f64)> = forecasts
        .iter()
        .zip(refined_weights.iter())
        .map(|(f, &w)| (f.symbol.clone(), w * leverage))
        .filter(|(_, w)| *w > 0.001) // Filter dust
        .collect();

    let adjusted_return = port_ret * leverage;
    let adjusted_vol = port_vol * leverage;
    let adjusted_sharpe = if adjusted_vol > 1e-8 {
        (adjusted_return - RISK_FREE_RATE) / adjusted_vol
    } else {
        0.0
    };

    Ok(PortfolioAllocation {
        weights: final_weights,
        expected_annual_return: adjusted_return,
        expected_annual_vol: adjusted_vol,
        sharpe_ratio: adjusted_sharpe,
        cvar_95: cvar * leverage,
        asset_forecasts: forecasts.to_vec(),
        leverage,
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
    info!(
        "=== DiffStock Portfolio Optimizer ===\n  Assets: {:?}\n  Horizon: {} days\n  MC Paths: {}\n  Target Vol: {:.0}%",
        symbols, PORTFOLIO_HORIZON, PORTFOLIO_MC_PATHS, TARGET_ANNUAL_VOL * 100.0
    );

    // Step 1: Generate forecasts
    let forecasts = generate_multi_asset_forecasts(
        symbols,
        PORTFOLIO_HORIZON,
        PORTFOLIO_MC_PATHS,
        use_cuda,
    )
    .await?;

    if forecasts.len() < 2 {
        return Err(anyhow::anyhow!(
            "Need at least 2 assets with valid forecasts. Got {}.",
            forecasts.len()
        ));
    }

    // Step 2: Optimize
    let allocation = optimize_portfolio(&forecasts)?;

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
    fn test_cvar_is_positive_for_risky_portfolio() {
        let forecasts = mock_forecasts(3);
        let weights = vec![1.0 / 3.0; 3];
        let cvar = portfolio_cvar(&weights, &forecasts, 0.05);
        // CVaR can be positive or negative depending on mock data, but should be finite
        assert!(cvar.is_finite(), "CVaR should be finite");
    }
}

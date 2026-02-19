use crate::data::StockData;
use crate::diffusion::GaussianDiffusion;
use crate::models::time_grad::{EpsilonTheta, RNNEncoder};
use crate::config::{get_device, ComputeBackend, CUDA_INFERENCE_BATCH_SIZE, DDIM_ETA, DDIM_INFERENCE_STEPS, DIFF_STEPS, DROPOUT_RATE, FORECAST, HIDDEN_DIM, INFERENCE_BATCH_SIZE, INPUT_DIM, LOOKBACK, LSTM_LAYERS, NUM_LAYERS, TRAINING_SYMBOLS};
use anyhow::Result;
use candle_core::{DType, Tensor};
use candle_nn::VarBuilder;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use chrono::Duration;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct ForecastData {
    pub p10: Vec<(f64, f64)>, // (time, price)
    pub p30: Vec<(f64, f64)>,
    pub p50: Vec<(f64, f64)>,
    pub p70: Vec<(f64, f64)>,
    pub p90: Vec<(f64, f64)>,
    pub _paths: Vec<Vec<f64>>, // Raw paths for potential detailed inspection
}

pub async fn run_inference_with_backend(
    data: Arc<StockData>,
    horizon: usize,
    num_simulations: usize,
    progress_tx: Option<Sender<f64>>,
    backend: ComputeBackend,
) -> Result<ForecastData> {
    match backend {
        ComputeBackend::Cuda => {
            run_inference(data, horizon, num_simulations, progress_tx, true).await
        }
        ComputeBackend::Cpu => {
            run_inference(data, horizon, num_simulations, progress_tx, false).await
        }
        ComputeBackend::Auto => {
            run_inference(
                data,
                horizon,
                num_simulations,
                progress_tx,
                cfg!(feature = "cuda"),
            )
            .await
        }
        ComputeBackend::Directml => {
            match crate::config::find_directml_onnx_model_path() {
                Some(model_path) => {
                    match crate::ort_directml::probe_directml_session(&model_path) {
                        Ok(_) => warn!(
                            "DirectML session probe succeeded with model '{}', but full ONNX forecast graph execution is not yet wired. Falling back to CPU path.",
                            model_path.display()
                        ),
                        Err(e) => warn!(
                            "DirectML backend requested but ORT DirectML probe failed: {}. Falling back to CPU path.",
                            e
                        ),
                    }
                }
                None => warn!(
                    "DirectML backend requested but no ONNX model found. Set DIFFSTOCK_ORT_MODEL or place model_weights.onnx/model.onnx. Falling back to CPU path."
                ),
            }
            run_inference(data, horizon, num_simulations, progress_tx, false).await
        }
    }
}

pub async fn run_inference(
    data: Arc<StockData>,
    horizon: usize,
    num_simulations: usize,
    progress_tx: Option<Sender<f64>>,
    use_cuda: bool,
) -> Result<ForecastData> {
    // 1. Setup Device and Data
    let device = get_device(use_cuda);
    
    // Prepare Context Data (Last LOOKBACK days)
    let context_len = LOOKBACK;
    if data.history.len() < context_len + 1 {
        return Err(anyhow::anyhow!("Not enough history data (need at least {} days)", context_len + 1));
    }

    let start_idx = data.history.len() - context_len;
    
    // Calculate features for the context window
    let mut features = Vec::with_capacity(context_len);
    let mut close_vals = Vec::with_capacity(context_len);

    for i in 0..context_len {
        let idx = start_idx + i;
        let close_ret = (data.history[idx].close / data.history[idx-1].close).ln();
        let overnight_ret = (data.history[idx].open / data.history[idx-1].close).ln();
        features.push(vec![close_ret, overnight_ret]);
        close_vals.push(close_ret);
    }

    // Normalize Context
    let mean = close_vals.iter().sum::<f64>() / context_len as f64;
    let variance = close_vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (context_len as f64 - 1.0);
    let std = variance.sqrt() + 1e-6;

    let normalized_features: Vec<f32> = features.iter().flat_map(|f| {
        vec![
            ((f[0] - mean) / std) as f32,
            ((f[1] - mean) / std) as f32
        ]
    }).collect();

    // [1, SeqLen, 2]
    let context_tensor = Tensor::from_slice(&normalized_features, (1, context_len, 2), &device)?;

    // Determine Asset ID (case-insensitive match)
    let symbol_upper = data.symbol.to_uppercase();
    let asset_id = TRAINING_SYMBOLS.iter().position(|&s| s.eq_ignore_ascii_case(&symbol_upper)).unwrap_or_else(|| {
        warn!("Symbol {} not found in training set. Using default asset ID 0.", data.symbol);
        0
    });

    // 2. Initialize Model
    let num_assets = TRAINING_SYMBOLS.len();

    // Load weights if available
    let vb = if std::path::Path::new("model_weights.safetensors").exists() {
        unsafe { VarBuilder::from_mmaped_safetensors(&["model_weights.safetensors"], DType::F32, &device)? }
    } else {
        warn!("model_weights.safetensors not found — model is untrained. Predictions will be meaningless. Run with --train first.");
        VarBuilder::zeros(DType::F32, &device)
    };

    let encoder = RNNEncoder::new(INPUT_DIM, HIDDEN_DIM, LSTM_LAYERS, DROPOUT_RATE, vb.pp("encoder"))?;
    let model = EpsilonTheta::new(1, HIDDEN_DIM, HIDDEN_DIM, NUM_LAYERS, num_assets, DROPOUT_RATE, vb.pp("model"))?;
    let diffusion = GaussianDiffusion::new(DIFF_STEPS, &device)?;

    // 3. Encode History
    let hidden_state = encoder.forward(&context_tensor, false)?;
    let hidden_state = hidden_state.unsqueeze(2)?; // [1, 1, 1]

    // 4. Autoregressive Forecasting Loop (batched DDIM for speed)
    let mut all_paths = Vec::with_capacity(num_simulations);
    let start_date = data.history.last().unwrap().date;
    let inference_batch_size = if use_cuda {
        CUDA_INFERENCE_BATCH_SIZE
    } else {
        INFERENCE_BATCH_SIZE
    };

    let chunk_len = FORECAST.max(1);
    let chunks_per_path = horizon.div_ceil(chunk_len);
    let total_horizon_batches = num_simulations.div_ceil(inference_batch_size);
    let total_steps = total_horizon_batches * chunks_per_path;
    let mut completed_steps = 0;

    let mut remaining = num_simulations;
    while remaining > 0 {
        let batch = remaining.min(inference_batch_size);
        let mut batch_paths: Vec<Vec<f64>> = (0..batch).map(|_| Vec::with_capacity(horizon)).collect();
        let mut last_vals: Vec<f64> = vec![data.history.last().unwrap().close; batch];

        let mut produced = 0;
        while produced < horizon {
            let current_chunk = (horizon - produced).min(chunk_len);

            // Batched DDIM chunk sampling: generate multiple future returns in one forward pass
            let samples = diffusion.sample_ddim_batched(
                &model,
                &hidden_state,
                asset_id as u32,
                batch,
                current_chunk,
                DDIM_INFERENCE_STEPS,
                DDIM_ETA,
            )?;

            // samples: [batch, 1, current_chunk] -> [batch, current_chunk]
            let chunk_vals = samples.squeeze(1)?.to_vec2::<f32>()?;

            for (path_idx, returns) in chunk_vals.iter().enumerate() {
                for &predicted_norm_ret in returns {
                    let predicted_ret = (predicted_norm_ret as f64 * std) + mean;
                    let next_price = last_vals[path_idx] * predicted_ret.exp();
                    batch_paths[path_idx].push(next_price);
                    last_vals[path_idx] = next_price;
                }
            }

            completed_steps += 1;
            if let Some(tx) = &progress_tx {
                let _ = tx.send(completed_steps as f64 / total_steps as f64).await;
            }

            produced += current_chunk;
        }

        all_paths.extend(batch_paths);
        remaining -= batch;
    }

    // 5. Calculate Percentiles
    let mut p10 = Vec::with_capacity(horizon);
    let mut p30 = Vec::with_capacity(horizon);
    let mut p50 = Vec::with_capacity(horizon);
    let mut p70 = Vec::with_capacity(horizon);
    let mut p90 = Vec::with_capacity(horizon);

    for t in 0..horizon {
        let mut time_slice: Vec<f64> = all_paths.iter().map(|p| p[t]).collect();
        time_slice.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let idx_10 = (num_simulations as f64 * 0.1) as usize;
        let idx_30 = (num_simulations as f64 * 0.3) as usize;
        let idx_50 = (num_simulations as f64 * 0.5) as usize;
        let idx_70 = (num_simulations as f64 * 0.7) as usize;
        let idx_90 = (num_simulations as f64 * 0.9) as usize;

        let time_point = (start_date + Duration::days(t as i64 + 1)).timestamp() as f64;
        p10.push((time_point, time_slice[idx_10]));
        p30.push((time_point, time_slice[idx_30]));
        p50.push((time_point, time_slice[idx_50]));
        p70.push((time_point, time_slice[idx_70]));
        p90.push((time_point, time_slice[idx_90]));
    }

    Ok(ForecastData {
        p10,
        p30,
        p50,
        p70,
        p90,
        _paths: all_paths,
    })
}

pub async fn run_backtest(data: Arc<StockData>, use_cuda: bool) -> Result<()> {
    run_backtest_with_params(data, use_cuda, 50).await
}

pub async fn run_backtest_with_params(
    data: Arc<StockData>,
    use_cuda: bool,
    hidden_days: usize,
) -> Result<()> {
    println!("Running Backtest...");
    let horizon = 10;
    let num_simulations = 500;

    if data.history.len() < hidden_days + 51 {
        return Err(anyhow::anyhow!("Not enough data for backtest"));
    }

    // Create a subset of data
    let train_len = data.history.len() - hidden_days;
    let train_history = data.history[..train_len].to_vec();
    let test_history = data.history[train_len..train_len+horizon].to_vec(); // Test on next 'horizon' days

    let train_data = Arc::new(StockData {
        symbol: data.symbol.clone(),
        history: train_history,
    });

    let forecast = run_inference(train_data, horizon, num_simulations, None, use_cuda).await?;

    // Calculate Coverage
    let mut inside_cone = 0;
    for (i, candle) in test_history.iter().enumerate() {
        let price = candle.close;
        let lower = forecast.p10[i].1;
        let upper = forecast.p90[i].1;
        
        if price >= lower && price <= upper {
            inside_cone += 1;
        }
        println!("Day {}: Price={:.2}, P10={:.2}, P90={:.2} [{}]", 
            i+1, price, lower, upper, if price >= lower && price <= upper { "INSIDE" } else { "OUTSIDE" });
    }

    println!("Coverage Probability (P10-P90): {:.2}%", (inside_cone as f64 / horizon as f64) * 100.0);
    Ok(())
}

pub async fn run_backtest_rolling(
    data: Arc<StockData>,
    use_cuda: bool,
    windows: usize,
    step_days: usize,
    hidden_days: usize,
) -> Result<()> {
    if windows == 0 {
        return Err(anyhow::anyhow!("backtest windows must be >= 1"));
    }
    if step_days == 0 {
        return Err(anyhow::anyhow!("backtest step days must be >= 1"));
    }

    println!(
        "Running rolling backtest... windows={}, step_days={}, hidden_days={} (SPY)",
        windows, step_days, hidden_days
    );

    let horizon = 10usize;
    let num_simulations = 500usize;
    let min_required = hidden_days + (windows - 1) * step_days + 51;
    if data.history.len() < min_required {
        return Err(anyhow::anyhow!(
            "Not enough data for rolling backtest: need at least {}, got {}",
            min_required,
            data.history.len()
        ));
    }

    let mut coverages = Vec::with_capacity(windows);
    let mut outside_days = Vec::with_capacity(windows);

    for window_idx in 0..windows {
        let window_hidden_days = hidden_days + window_idx * step_days;
        let train_len = data.history.len() - window_hidden_days;
        let train_history = data.history[..train_len].to_vec();
        let test_history = data.history[train_len..train_len + horizon].to_vec();

        let train_data = Arc::new(StockData {
            symbol: data.symbol.clone(),
            history: train_history,
        });

        let forecast = run_inference(train_data, horizon, num_simulations, None, use_cuda).await?;

        let mut inside_cone = 0usize;
        for (i, candle) in test_history.iter().enumerate() {
            let price = candle.close;
            let lower = forecast.p10[i].1;
            let upper = forecast.p90[i].1;
            if price >= lower && price <= upper {
                inside_cone += 1;
            }
        }

        let coverage = (inside_cone as f64 / horizon as f64) * 100.0;
        let outside = horizon - inside_cone;
        coverages.push(coverage);
        outside_days.push(outside as f64);

        println!(
            "Window {:>2}/{:>2}: hidden_days={}, coverage={:.2}% (inside={}, outside={})",
            window_idx + 1,
            windows,
            window_hidden_days,
            coverage,
            inside_cone,
            outside
        );
    }

    let mean = coverages.iter().sum::<f64>() / coverages.len() as f64;
    let min = coverages.iter().copied().fold(f64::INFINITY, f64::min);
    let max = coverages
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let variance = coverages
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / coverages.len() as f64;
    let std = variance.sqrt();
    let avg_outside = outside_days.iter().sum::<f64>() / outside_days.len() as f64;

    println!("Rolling Coverage Summary (P10-P90):");
    println!("  windows={} step_days={} hidden_days_start={}", windows, step_days, hidden_days);
    println!(
        "  mean={:.2}% std={:.2} min={:.2}% max={:.2}% avg_outside_days={:.2}/{}",
        mean, std, min, max, avg_outside, horizon
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FORECAST, LOOKBACK};
    use crate::data::StockData;
    use crate::train::train_model_with_data;

    #[tokio::test]
    async fn test_inference_with_mock_data() {
        // 1. Create mock data and train 1 epoch to produce weights
        let mock_train = StockData::new_mock("SPY", 200);
        let dataset = mock_train.prepare_training_data(LOOKBACK, FORECAST, 0);
        let (train_data, val_data) = dataset.split(0.8);

        train_model_with_data(
            train_data,
            val_data,
            Some(1),
            Some(16),
            Some(1e-3),
            None,
            false,
        )
        .await
        .expect("training should succeed");

        // 2. Run inference with small horizon/sims
        let mock_infer = StockData::new_mock("SPY", 200);
        let data = Arc::new(mock_infer);
        let horizon = 5;
        let num_sims = 20;

        let forecast = run_inference(data, horizon, num_sims, None, false)
            .await
            .expect("inference should succeed");

        // 3. Verify output structure
        assert_eq!(forecast.p10.len(), horizon);
        assert_eq!(forecast.p50.len(), horizon);
        assert_eq!(forecast.p90.len(), horizon);

        // Prices should be positive
        for i in 0..horizon {
            assert!(forecast.p10[i].1 > 0.0, "p10 price should be > 0");
            assert!(forecast.p50[i].1 > 0.0, "p50 price should be > 0");
            assert!(forecast.p90[i].1 > 0.0, "p90 price should be > 0");
        }

        // Percentile ordering: p10 <= p50 <= p90
        for i in 0..horizon {
            assert!(
                forecast.p10[i].1 <= forecast.p50[i].1,
                "p10 ({}) should <= p50 ({}) at step {}",
                forecast.p10[i].1,
                forecast.p50[i].1,
                i
            );
            assert!(
                forecast.p50[i].1 <= forecast.p90[i].1,
                "p50 ({}) should <= p90 ({}) at step {}",
                forecast.p50[i].1,
                forecast.p90[i].1,
                i
            );
        }

        // Cleanup
        if std::path::Path::new("model_weights.safetensors").exists() {
            std::fs::remove_file("model_weights.safetensors").unwrap();
        }
    }

    #[tokio::test]
    async fn test_inference_without_weights() {
        // Ensure no weights file exists
        let _ = std::fs::remove_file("model_weights.safetensors");

        let mock_data = StockData::new_mock("SPY", 200);
        let data = Arc::new(mock_data);

        // Should run without panicking (zeros fallback)
        let result = run_inference(data, 3, 10, None, false).await;
        assert!(result.is_ok(), "inference with zeros fallback should not panic");

        let forecast = result.unwrap();
        assert_eq!(forecast.p50.len(), 3);

        // Cleanup (shouldn't exist, but just in case)
        let _ = std::fs::remove_file("model_weights.safetensors");
    }
}

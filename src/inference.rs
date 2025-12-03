use crate::data::StockData;
use crate::diffusion::GaussianDiffusion;
use crate::models::time_grad::{EpsilonTheta, RNNEncoder};
use crate::config::TRAINING_SYMBOLS;
use anyhow::Result;
use candle_core::{DType, Device, Tensor};
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

pub async fn run_inference(
    data: Arc<StockData>,
    horizon: usize,
    num_simulations: usize,
    progress_tx: Option<Sender<f64>>,
) -> Result<ForecastData> {
    // 1. Setup Device and Data
    let device = Device::Cpu;
    
    // Prepare Context Data (Last 50 days)
    let context_len = 50;
    if data.history.len() < context_len + 1 {
        return Err(anyhow::anyhow!("Not enough history data (need at least 51 days)"));
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

    // Determine Asset ID
    let asset_id = TRAINING_SYMBOLS.iter().position(|&s| s == data.symbol).unwrap_or_else(|| {
        warn!("Symbol {} not found in training set. Using default asset ID 0.", data.symbol);
        0
    });
    let asset_id_tensor = Tensor::new(&[asset_id as u32], &device)?;

    // 2. Initialize Model
    let input_dim = 2;
    let hidden_dim = 64;
    let num_layers = 3;
    let diff_steps = 100;
    let num_assets = TRAINING_SYMBOLS.len();

    // Load weights if available
    let vb = if std::path::Path::new("model_weights.safetensors").exists() {
        unsafe { VarBuilder::from_mmaped_safetensors(&["model_weights.safetensors"], DType::F32, &device)? }
    } else {
        // Warn user? For now just random init
        VarBuilder::zeros(DType::F32, &device)
    };

    let encoder = RNNEncoder::new(input_dim, hidden_dim, vb.pp("encoder"))?;
    let model = EpsilonTheta::new(1, hidden_dim, hidden_dim, num_layers, num_assets, vb.pp("model"))?;
    let diffusion = GaussianDiffusion::new(diff_steps, &device)?;

    // 3. Encode History
    let hidden_state = encoder.forward(&context_tensor)?;
    let hidden_state = hidden_state.unsqueeze(2)?; // [1, 1, 1]

    // 4. Autoregressive Forecasting Loop
    let mut all_paths = Vec::with_capacity(num_simulations);
    let start_date = data.history.last().unwrap().date;
    let total_steps = num_simulations * horizon;
    let mut completed_steps = 0;

    for _ in 0..num_simulations {
        let mut current_path = Vec::with_capacity(horizon);
        let current_hidden = hidden_state.clone();
        let mut last_val = data.history.last().unwrap().close;

        for _ in 0..horizon {
            // Sample next step (Close Return)
            // Note: sample() calls model.forward(). We need to update sample() signature or model.forward() usage inside sample().
            // Wait, diffusion.sample() calls model.forward().
            // I need to update diffusion.sample() to accept asset_ids!
            let sample = diffusion.sample(&model, &current_hidden, &asset_id_tensor, (1, 1, 1))?;
            
            let predicted_norm_ret = sample.squeeze(2)?.squeeze(1)?.get(0)?.to_scalar::<f32>()? as f64;
            
            // Denormalize
            let predicted_ret = (predicted_norm_ret * std) + mean;
            
            let next_price = last_val * predicted_ret.exp();
            
            current_path.push(next_price);
            last_val = next_price;

            completed_steps += 1;
            if completed_steps % 10 == 0 {
                if let Some(tx) = &progress_tx {
                    let _ = tx.send(completed_steps as f64 / total_steps as f64).await;
                }
            }
        }
        all_paths.push(current_path);
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

pub async fn run_backtest(data: Arc<StockData>) -> Result<()> {
    println!("Running Backtest...");
    let horizon = 10;
    let num_simulations = 500;
    
    // Hide last 50 days (or just horizon?)
    // The prompt says "hides the last 50 days".
    let hidden_days = 50;
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

    let forecast = run_inference(train_data, horizon, num_simulations, None).await?;

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

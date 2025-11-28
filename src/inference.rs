use crate::data::StockData;
use crate::diffusion::GaussianDiffusion;
use crate::models::time_grad::{EpsilonTheta, RNNEncoder};
use anyhow::Result;
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

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
    let device = Device::Cpu; // Use CPU for TUI app compatibility
    let history_prices: Vec<f64> = data.history.iter().map(|c| c.close).collect();
    
    // Normalize data (simple standardization for the model)
    let (mean, std) = data.stats();
    
    // For the model, we need a context window. Let's take the last 100 points or all if less.
    let context_len = 100;
    let start_idx = history_prices.len().saturating_sub(context_len);
    let context_data = &history_prices[start_idx..];
    
    // Convert to Tensor: Shape (1, seq_len, 1) for LSTM
    // RNNEncoder expects [batch, seq_len, input_dim]
    let context_tensor = Tensor::from_slice(context_data, (1, context_data.len(), 1), &device)?.to_dtype(DType::F32)?;

    // 2. Initialize Model Components (Randomly initialized for this demo)
    let input_size = 1;
    let hidden_size = 32;
    let num_layers = 2;
    let diff_steps = 50; // Reduced to 50 to cut sim time in half

    // Create a VarBuilder with random initialization
    let vb = VarBuilder::zeros(DType::F32, &device);

    let encoder = RNNEncoder::new(input_size, hidden_size, vb.pp("encoder"))?;
    let model = EpsilonTheta::new(input_size, hidden_size, hidden_size, num_layers, vb.pp("model"))?;
    let diffusion = GaussianDiffusion::new(diff_steps, &device)?;

    // 3. Encode History
    // The encoder produces the hidden state to condition the diffusion
    // Output: [batch, 1]
    let hidden_state = encoder.forward(&context_tensor)?;
    // Reshape for Conv1d conditioning: [batch, 1, 1]
    let hidden_state = hidden_state.unsqueeze(2)?;

    // 4. Autoregressive Forecasting Loop
    let mut all_paths = Vec::with_capacity(num_simulations);
    let start_time_idx = data.history.len() as f64;
    let total_steps = num_simulations * horizon;
    let mut completed_steps = 0;

    for _ in 0..num_simulations {
        let mut current_path = Vec::with_capacity(horizon);
        let current_hidden = hidden_state.clone();
        let mut last_val = *context_data.last().unwrap_or(&0.0); 

        for _ in 0..horizon {
            // Sample next step x_t given condition h_{t-1}
            // Shape of sample: (1, 1, 1) -> (Batch, Channel, Time)
            let sample = diffusion.sample(&model, &current_hidden, (1, 1, 1))?;
            
            let predicted_val = sample.squeeze(2)?.squeeze(1)?.get(0)?.to_scalar::<f32>()? as f64;
            
            // Simplified logic: treat prediction as a shock/return
            // In a real model, we'd update the RNN state here.
            let shock = predicted_val * std; 
            let next_price = last_val * (mean + shock).exp();
            
            current_path.push(next_price);
            last_val = next_price;

            completed_steps += 1;
            if completed_steps % 10 == 0 { // Update every 10 steps to avoid channel overhead
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

        let time_point = start_time_idx + (t as f64);
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

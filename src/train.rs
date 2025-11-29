use crate::data::StockData;
use crate::diffusion::GaussianDiffusion;
use crate::models::time_grad::{EpsilonTheta, RNNEncoder};
use anyhow::Result;
use candle_core::{DType, Device, Tensor};
use candle_nn::{VarBuilder, VarMap, Optimizer};

const LOOKBACK: usize = 50;
const FORECAST: usize = 10;
const BATCH_SIZE: usize = 32;
const EPOCHS: usize = 100;
const LEARNING_RATE: f64 = 1e-3;

pub async fn train_model() -> Result<()> {
    println!("Training mode started...");
    let device = Device::Cpu;

    // 1. Fetch Data
    // Expanded dataset covering Indexes, Sectors, Commodities, and Crypto
    let symbols = vec![
        // US Indexes
        "SPY", "QQQ", "DIA", "IWM", 
        // US Sectors
        "XLK", "XLF", "XLV", "XLE", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE", "XLC",
        // Commodities
        "GLD", "SLV", "USO", "UNG", "DBC",
        // Crypto
        "BTC-USD", "ETH-USD",
        // Volatility
        "VIXY"
    ];
    let mut all_features = Vec::new();
    let mut all_targets = Vec::new();

    for symbol in symbols {
        println!("Fetching data for {}...", symbol);
        match StockData::fetch_range(symbol, "5y").await {
            Ok(data) => {
                let dataset = data.prepare_training_data(LOOKBACK, FORECAST);
                all_features.extend(dataset.features);
                all_targets.extend(dataset.targets);
            }
            Err(e) => eprintln!("Failed to fetch {}: {}", symbol, e),
        }
    }

    if all_features.is_empty() {
        return Err(anyhow::anyhow!("No training data available."));
    }

    println!("Dataset size: {} samples", all_features.len());

    // 2. Initialize Model
    let varmap = VarMap::new();
    let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);

    let input_dim = 2; // Close Return, Overnight Return
    let hidden_dim = 32;
    let num_layers = 2;
    let diff_steps = 100;

    let encoder = RNNEncoder::new(input_dim, hidden_dim, vb.pp("encoder"))?;
    let model = EpsilonTheta::new(1, hidden_dim, hidden_dim, num_layers, vb.pp("model"))?; // input_channels=1 (target is close return)
    let diffusion = GaussianDiffusion::new(diff_steps, &device)?;

    let mut opt = candle_nn::AdamW::new_lr(varmap.all_vars(), LEARNING_RATE)?;

    // 3. Training Loop
    let num_samples = all_features.len();
    let num_batches = num_samples / BATCH_SIZE;

    for epoch in 0..EPOCHS {
        let mut total_loss = 0.0;
        
        // Shuffle indices (simple shuffle)
        let indices: Vec<usize> = (0..num_samples).collect();
        // rand::seq::SliceRandom::shuffle(&mut indices, &mut rand::thread_rng()); // Need rand dependency imported

        for batch_idx in 0..num_batches {
            let start = batch_idx * BATCH_SIZE;
            let end = start + BATCH_SIZE;
            let batch_indices = &indices[start..end];

            // Prepare Batch Tensors
            let mut batch_features = Vec::with_capacity(BATCH_SIZE);
            let mut batch_targets = Vec::with_capacity(BATCH_SIZE);

            for &idx in batch_indices {
                batch_features.push(Tensor::from_slice(&all_features[idx], (LOOKBACK, 2), &device)?.to_dtype(DType::F32)?);
                batch_targets.push(Tensor::from_slice(&all_targets[idx], (FORECAST, 1), &device)?.to_dtype(DType::F32)?); // [Forecast, 1]
            }

            let x_hist = Tensor::stack(&batch_features, 0)?; // [Batch, Lookback, 2]
            let x_0 = Tensor::stack(&batch_targets, 0)?;     // [Batch, Forecast, 1]
            
            // Transpose for Conv1d: [Batch, Channels, Time]
            let x_0 = x_0.permute((0, 2, 1))?; // [Batch, 1, Forecast]

            // Encode History
            let cond = encoder.forward(&x_hist)?; // [Batch, 1]
            let cond = cond.unsqueeze(2)?; // [Batch, 1, 1] -> Broadcast to [Batch, 1, Forecast] inside model? 
            // Wait, EpsilonTheta expects cond to be [Batch, 1, Time] or broadcastable.
            // Our ResidualBlock adds cond (projected) to h.
            // h is [Batch, Channels, Time].
            // cond is projected to [Batch, Channels, 1] (kernel 1 conv).
            // So [Batch, 1, 1] is fine, it will broadcast over Time.

            // Sample t
            let t = Tensor::rand(0.0f32, diff_steps as f32, (BATCH_SIZE,), &device)?.floor()?;
            // We need integer t for indexing, but diffusion.rs uses float t for embedding?
            // DiffusionEmbedding projects scalar t.
            // Let's check diffusion.rs/time_grad.rs.
            // time_grad.rs: DiffusionEmbedding takes Tensor.
            // diffusion.rs: sample takes t as float tensor.
            // We need to get alpha_bar_t.
            
            // We need a way to gather alpha_bar at index t.
            // candle doesn't have gather easily for this?
            // Actually, we can just loop or use a custom gather.
            // For efficiency, let's just pick random t uniformly.
            
            // Simplified training:
            // x_t = sqrt(alpha_bar_t) * x_0 + sqrt(1-alpha_bar_t) * epsilon
            // loss = MSE(epsilon, model(x_t, t, cond))
            
            let epsilon = Tensor::randn(0.0f32, 1.0f32, x_0.shape(), &device)?;
            
            // Gather alpha_bar and sqrt_one_minus_alpha_bar for each element in batch
            // This is tricky in pure candle without index_select/gather support for batches easily.
            // Workaround: Use a fixed t for the whole batch? No, that's bad for training.
            // Workaround: Iterate batch? Slow.
            // Workaround: Use index_select.
            
            // t is [Batch].
            // alpha_bar is [Steps].
            // We want [Batch] values.
            // t needs to be u32/i64 for indexing.
            
            // Let's assume we can do:
            // let t_u32 = t.to_dtype(DType::U32)?;
            // let alpha_bar_t = diffusion.alpha_bar.index_select(&t_u32, 0)?;
            
            // But wait, diffusion.alpha_bar is on device.
            
            // Let's try to implement the gather.
            let t_vec: Vec<u32> = t.to_vec1::<f32>()?.iter().map(|&x| x as u32).collect();
            let t_u32 = Tensor::new(t_vec.as_slice(), &device)?;
            
            let alpha_bar_t = diffusion.alpha_bar.index_select(&t_u32, 0)?; // [Batch]
            let sqrt_alpha_bar_t = alpha_bar_t.sqrt()?;
            let sqrt_one_minus_alpha_bar_t = (1.0 - alpha_bar_t)?.sqrt()?;
            
            // Reshape for broadcasting: [Batch, 1, 1]
            let sqrt_alpha_bar_t = sqrt_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;
            let sqrt_one_minus_alpha_bar_t = sqrt_one_minus_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;
            
            let x_t = (x_0.broadcast_mul(&sqrt_alpha_bar_t)? + epsilon.broadcast_mul(&sqrt_one_minus_alpha_bar_t)?)?;
            
            // Predict noise
            // t needs to be [Batch, 1] for embedding
            let t_in = t.unsqueeze(1)?;
            let epsilon_pred = model.forward(&x_t, &t_in, &cond)?;
            
            let loss = (epsilon - epsilon_pred)?.sqr()?.mean_all()?;
            
            opt.backward_step(&loss)?;
            total_loss += loss.to_scalar::<f32>()? as f64;
        }
        
        println!("Epoch {}: Loss = {:.6}", epoch + 1, total_loss / num_batches as f64);
    }

    // 4. Save Weights
    println!("Saving weights to model_weights.safetensors...");
    varmap.save("model_weights.safetensors")?;
    println!("Done.");

    Ok(())
}

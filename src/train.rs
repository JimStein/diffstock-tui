use crate::config::{get_device, AUGMENTATION_COPIES, AUGMENTATION_NOISE, BATCH_SIZE, CUDA_BATCH_SIZE, DATA_RANGE, DIFF_STEPS, DROPOUT_RATE, EPOCHS, FORECAST, HIDDEN_DIM, INPUT_DIM, LEARNING_RATE, LOOKBACK, LSTM_LAYERS, NUM_LAYERS, PATIENCE, TRAIN_LOG_INTERVAL_BATCHES, TRAINING_SYMBOLS, WEIGHT_DECAY};
use crate::data::{StockData, TrainingDataset};
use crate::diffusion::GaussianDiffusion;
use crate::models::time_grad::{EpsilonTheta, RNNEncoder};
use crate::gui::TrainMessage;
use anyhow::Result;
use candle_core::{DType, Tensor};
use candle_nn::{VarBuilder, VarMap, Optimizer};
use chrono::Utc;
use rand::seq::SliceRandom;
use serde::Serialize;
use tracing::{info, error, warn};
use tokio::sync::mpsc;
use rayon::prelude::*;
use std::io::Write;
use std::time::Instant;

#[derive(Serialize)]
struct EpochLogEntry {
    epoch: usize,
    train_loss: f64,
    val_loss: f64,
}

#[derive(Serialize)]
struct TrainingRunLog {
    run_type: String,
    started_at: String,
    finished_at: String,
    use_cuda: bool,
    data_range: String,
    symbols: Vec<String>,
    epochs_requested: usize,
    epochs_completed: usize,
    batch_size: usize,
    learning_rate: f64,
    patience: usize,
    best_val_loss: f64,
    stop_reason: Option<String>,
    epoch_metrics: Vec<EpochLogEntry>,
}

#[derive(Serialize)]
struct RealtimeLogEvent {
    event: String,
    timestamp: String,
    run_type: String,
    epoch: Option<usize>,
    train_loss: Option<f64>,
    val_loss: Option<f64>,
    best_val_loss: Option<f64>,
    message: Option<String>,
}

fn create_realtime_log_file(run_type: &str) -> Result<std::path::PathBuf> {
    let log_dir = std::path::Path::new("log");
    if !log_dir.exists() {
        std::fs::create_dir_all(log_dir)?;
    }

    let file_name = format!(
        "training_live_{}_{}_{}.jsonl",
        run_type,
        Utc::now().format("%Y%m%d_%H%M%S"),
        std::process::id()
    );
    let file_path = log_dir.join(file_name);
    std::fs::File::create(&file_path)?;
    Ok(file_path)
}

fn append_realtime_event(log_path: &std::path::Path, event: &RealtimeLogEvent) -> Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(log_path)?;
    let line = serde_json::to_string(event)?;
    writeln!(f, "{}", line)?;
    Ok(())
}

fn compute_window_trend(epoch_metrics: &[EpochLogEntry], window: usize) -> Option<(f64, f64, f64, f64)> {
    if epoch_metrics.len() < window || window < 2 {
        return None;
    }
    let slice = &epoch_metrics[epoch_metrics.len() - window..];
    let train_avg = slice.iter().map(|e| e.train_loss).sum::<f64>() / window as f64;
    let val_avg = slice.iter().map(|e| e.val_loss).sum::<f64>() / window as f64;
    let train_slope = (slice[window - 1].train_loss - slice[0].train_loss) / (window as f64 - 1.0);
    let val_slope = (slice[window - 1].val_loss - slice[0].val_loss) / (window as f64 - 1.0);
    Some((train_avg, val_avg, train_slope, val_slope))
}

fn persist_training_log(run_log: &TrainingRunLog) -> Result<std::path::PathBuf> {
    let log_dir = std::path::Path::new("log");
    if !log_dir.exists() {
        std::fs::create_dir_all(log_dir)?;
    }

    let file_name = format!(
        "training_{}_{}.json",
        Utc::now().format("%Y%m%d_%H%M%S"),
        std::process::id()
    );
    let file_path = log_dir.join(file_name);

    let file = std::fs::File::create(&file_path)?;
    let writer = std::io::BufWriter::new(file);
    serde_json::to_writer_pretty(writer, run_log)?;

    Ok(file_path)
}

pub async fn train_model(
    epochs: Option<usize>,
    batch_size: Option<usize>,
    learning_rate: Option<f64>,
    patience: Option<usize>,
    use_cuda: bool,
) -> Result<()> {
    info!("Training mode started...");

    info!("Configuration: Epochs={}, Batch Size={}, LR={}",
        epochs.unwrap_or(EPOCHS),
        batch_size.unwrap_or(BATCH_SIZE),
        learning_rate.unwrap_or(LEARNING_RATE)
    );

    let (train_data, val_data) = fetch_training_data().await?;

    if train_data.features.is_empty() {
        return Err(anyhow::anyhow!("No training data available."));
    }

    train_model_with_data(train_data, val_data, epochs, batch_size, learning_rate, patience, use_cuda).await
}

/// Training entry point with GUI progress channel.
pub async fn train_model_with_progress(
    epochs: Option<usize>,
    batch_size: Option<usize>,
    learning_rate: Option<f64>,
    patience: Option<usize>,
    use_cuda: bool,
    tx: mpsc::Sender<TrainMessage>,
) -> Result<()> {
    let _ = tx.send(TrainMessage::Log("Fetching training data...".to_string())).await;

    let (train_data, val_data) = fetch_training_data().await?;

    if train_data.features.is_empty() {
        return Err(anyhow::anyhow!("No training data available."));
    }

    let _ = tx.send(TrainMessage::Log(format!(
        "Data ready: {} train / {} val samples",
        train_data.features.len(),
        val_data.features.len()
    ))).await;

    train_loop_with_progress(train_data, val_data, epochs, batch_size, learning_rate, patience, use_cuda, tx).await
}

/// Core training loop that sends per-epoch progress to the GUI.
async fn train_loop_with_progress(
    train_data: TrainingDataset,
    val_data: TrainingDataset,
    epochs: Option<usize>,
    batch_size: Option<usize>,
    learning_rate: Option<f64>,
    patience: Option<usize>,
    use_cuda: bool,
    tx: mpsc::Sender<TrainMessage>,
) -> Result<()> {
    let started_at = Utc::now();
    let live_log_path = create_realtime_log_file("gui_progress").ok();
    if let Some(path) = &live_log_path {
        let _ = tx.send(TrainMessage::Log(format!("Realtime training log: {}", path.display()))).await;
        let start_event = RealtimeLogEvent {
            event: "start".to_string(),
            timestamp: Utc::now().to_rfc3339(),
            run_type: "gui_progress".to_string(),
            epoch: None,
            train_loss: None,
            val_loss: None,
            best_val_loss: None,
            message: Some("training_started".to_string()),
        };
        let _ = append_realtime_event(path, &start_event);
    }
    let device = get_device(use_cuda);
    let epochs = epochs.unwrap_or(EPOCHS);
    let default_batch_size = if use_cuda { CUDA_BATCH_SIZE } else { BATCH_SIZE };
    let batch_size = batch_size.unwrap_or(default_batch_size);
    let learning_rate = learning_rate.unwrap_or(LEARNING_RATE);

    let varmap = VarMap::new();
    let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
    let num_assets = TRAINING_SYMBOLS.len();

    let encoder = RNNEncoder::new(INPUT_DIM, HIDDEN_DIM, LSTM_LAYERS, DROPOUT_RATE, vb.pp("encoder"))?;
    let model = EpsilonTheta::new(1, HIDDEN_DIM, HIDDEN_DIM, NUM_LAYERS, num_assets, DROPOUT_RATE, vb.pp("model"))?;
    let diffusion = GaussianDiffusion::new(DIFF_STEPS, &device)?;

    let params = candle_nn::ParamsAdamW {
        lr: learning_rate,
        weight_decay: WEIGHT_DECAY,
        ..Default::default()
    };
    let mut opt = candle_nn::AdamW::new(varmap.all_vars(), params)?;

    let num_train_samples = train_data.features.len();
    let num_train_batches = num_train_samples / batch_size;
    let num_val_samples = val_data.features.len();
    let num_val_batches = if num_val_samples > 0 { num_val_samples / batch_size } else { 0 };

    let mut best_val_loss = f64::INFINITY;
    let patience = patience.unwrap_or(PATIENCE);
    let mut epochs_without_improvement: usize = 0;
    let mut epoch_metrics: Vec<EpochLogEntry> = Vec::with_capacity(epochs);
    let mut stop_reason: Option<String> = None;

    let _ = tx.send(TrainMessage::Log(format!(
        "Model initialized (~25M params). {} train batches, {} val batches per epoch.",
        num_train_batches, num_val_batches
    ))).await;

    for epoch in 0..epochs {
        let epoch_start = Instant::now();
        let mut total_train_loss = 0.0;

        let mut indices: Vec<usize> = (0..num_train_samples).collect();
        indices.shuffle(&mut rand::thread_rng());

        for batch_idx in 0..num_train_batches {
            let start = batch_idx * batch_size;
            let end = start + batch_size;
            let batch_indices = &indices[start..end];

            let mut batch_features = Vec::with_capacity(batch_size);
            let mut batch_targets = Vec::with_capacity(batch_size);
            let mut batch_asset_ids = Vec::with_capacity(batch_size);

            for &idx in batch_indices {
                batch_features.push(Tensor::from_slice(&train_data.features[idx], (LOOKBACK, 2), &device)?.to_dtype(DType::F32)?);
                batch_targets.push(Tensor::from_slice(&train_data.targets[idx], (FORECAST, 1), &device)?.to_dtype(DType::F32)?);
                batch_asset_ids.push(train_data.asset_ids[idx] as u32);
            }

            let x_hist = Tensor::stack(&batch_features, 0)?;
            let x_0 = Tensor::stack(&batch_targets, 0)?;
            let asset_ids = Tensor::new(batch_asset_ids.as_slice(), &device)?;
            let x_0 = x_0.permute((0, 2, 1))?;

            let cond = encoder.forward(&x_hist, true)?;
            let cond = cond.unsqueeze(2)?;

            let t = Tensor::rand(0.0f32, DIFF_STEPS as f32, (batch_size,), &device)?
                .floor()?
                .clamp(0.0, (DIFF_STEPS - 1) as f64)?;
            let epsilon = Tensor::randn(0.0f32, 1.0f32, x_0.shape(), &device)?;

            let t_u32 = t.to_dtype(DType::U32)?;
            let alpha_bar_t = diffusion.alpha_bar.index_select(&t_u32, 0)?;
            let sqrt_alpha_bar_t = alpha_bar_t.sqrt()?;
            let sqrt_one_minus_alpha_bar_t = (1.0 - alpha_bar_t)?.sqrt()?;
            let sqrt_alpha_bar_t = sqrt_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;
            let sqrt_one_minus_alpha_bar_t = sqrt_one_minus_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;

            let x_t = (x_0.broadcast_mul(&sqrt_alpha_bar_t)? + epsilon.broadcast_mul(&sqrt_one_minus_alpha_bar_t)?)?;
            let t_in = t.unsqueeze(1)?;
            let epsilon_pred = model.forward(&x_t, &t_in, &asset_ids, &cond, true)?;

            let loss = (epsilon - epsilon_pred)?.sqr()?.mean_all()?;
            opt.backward_step(&loss)?;
            total_train_loss += loss.to_scalar::<f32>()? as f64;

            let batch_no = batch_idx + 1;
            let should_log = batch_no % TRAIN_LOG_INTERVAL_BATCHES == 0 || batch_no == num_train_batches;
            if should_log {
                let progress = (batch_no as f64 / num_train_batches.max(1) as f64) * 100.0;
                let _ = tx.send(TrainMessage::Log(format!(
                    "Epoch {}/{} progress: {}/{} batches ({:.1}%), elapsed: {:.1}s",
                    epoch + 1,
                    epochs,
                    batch_no,
                    num_train_batches,
                    progress,
                    epoch_start.elapsed().as_secs_f64()
                ))).await;
            }
        }

        let avg_train_loss = total_train_loss / num_train_batches.max(1) as f64;

        let mut total_val_loss = 0.0;
        if num_val_batches > 0 {
            for batch_idx in 0..num_val_batches {
                let start = batch_idx * batch_size;
                let end = start + batch_size;

                let mut batch_features = Vec::with_capacity(batch_size);
                let mut batch_targets = Vec::with_capacity(batch_size);
                let mut batch_asset_ids = Vec::with_capacity(batch_size);

                for idx in start..end {
                    batch_features.push(Tensor::from_slice(&val_data.features[idx], (LOOKBACK, 2), &device)?.to_dtype(DType::F32)?);
                    batch_targets.push(Tensor::from_slice(&val_data.targets[idx], (FORECAST, 1), &device)?.to_dtype(DType::F32)?);
                    batch_asset_ids.push(val_data.asset_ids[idx] as u32);
                }

                let x_hist = Tensor::stack(&batch_features, 0)?;
                let x_0 = Tensor::stack(&batch_targets, 0)?;
                let asset_ids = Tensor::new(batch_asset_ids.as_slice(), &device)?;
                let x_0 = x_0.permute((0, 2, 1))?;

                let cond = encoder.forward(&x_hist, false)?;
                let cond = cond.unsqueeze(2)?;

                let t = Tensor::rand(0.0f32, DIFF_STEPS as f32, (batch_size,), &device)?
                    .floor()?
                    .clamp(0.0, (DIFF_STEPS - 1) as f64)?;
                let epsilon = Tensor::randn(0.0f32, 1.0f32, x_0.shape(), &device)?;

                let t_u32 = t.to_dtype(DType::U32)?;
                let alpha_bar_t = diffusion.alpha_bar.index_select(&t_u32, 0)?;
                let sqrt_alpha_bar_t = alpha_bar_t.sqrt()?;
                let sqrt_one_minus_alpha_bar_t = (1.0 - alpha_bar_t)?.sqrt()?;
                let sqrt_alpha_bar_t = sqrt_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;
                let sqrt_one_minus_alpha_bar_t = sqrt_one_minus_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;

                let x_t = (x_0.broadcast_mul(&sqrt_alpha_bar_t)? + epsilon.broadcast_mul(&sqrt_one_minus_alpha_bar_t)?)?;
                let t_in = t.unsqueeze(1)?;
                let epsilon_pred = model.forward(&x_t, &t_in, &asset_ids, &cond, false)?;

                let loss = (epsilon - epsilon_pred)?.sqr()?.mean_all()?;
                total_val_loss += loss.to_scalar::<f32>()? as f64;
            }
        }

        let avg_val_loss = if num_val_batches > 0 { total_val_loss / num_val_batches as f64 } else { 0.0 };
        epoch_metrics.push(EpochLogEntry {
            epoch: epoch + 1,
            train_loss: avg_train_loss,
            val_loss: avg_val_loss,
        });

        // Send epoch result to GUI
        let _ = tx.send(TrainMessage::Epoch {
            epoch: epoch + 1,
            train_loss: avg_train_loss,
            val_loss: avg_val_loss,
        }).await;

        if let Some(path) = &live_log_path {
            let epoch_event = RealtimeLogEvent {
                event: "epoch".to_string(),
                timestamp: Utc::now().to_rfc3339(),
                run_type: "gui_progress".to_string(),
                epoch: Some(epoch + 1),
                train_loss: Some(avg_train_loss),
                val_loss: Some(avg_val_loss),
                best_val_loss: Some(best_val_loss.min(avg_val_loss)),
                message: None,
            };
            if let Err(e) = append_realtime_event(path, &epoch_event) {
                let _ = tx.send(TrainMessage::Log(format!("Failed to append realtime log: {}", e))).await;
            }
        }

        if (epoch + 1) % 10 == 0 {
            if let Some((train_avg, val_avg, train_slope, val_slope)) = compute_window_trend(&epoch_metrics, 10) {
                let trend_msg = format!(
                    "Trend@{} (last 10): train_avg={:.6}, val_avg={:.6}, train_slope={:+.6}/epoch, val_slope={:+.6}/epoch",
                    epoch + 1,
                    train_avg,
                    val_avg,
                    train_slope,
                    val_slope
                );
                let _ = tx.send(TrainMessage::Log(trend_msg.clone())).await;

                if let Some(path) = &live_log_path {
                    let trend_event = RealtimeLogEvent {
                        event: "trend".to_string(),
                        timestamp: Utc::now().to_rfc3339(),
                        run_type: "gui_progress".to_string(),
                        epoch: Some(epoch + 1),
                        train_loss: Some(train_avg),
                        val_loss: Some(val_avg),
                        best_val_loss: Some(best_val_loss.min(avg_val_loss)),
                        message: Some(format!(
                            "window=10,train_slope={:+.6},val_slope={:+.6}",
                            train_slope,
                            val_slope
                        )),
                    };
                    let _ = append_realtime_event(path, &trend_event);
                }
            }
        }

        if avg_val_loss < best_val_loss {
            best_val_loss = avg_val_loss;
            epochs_without_improvement = 0;
            let _ = tx.send(TrainMessage::Log(format!(
                "Epoch {}: New best model! Val loss: {:.6}. Saving weights...",
                epoch + 1, best_val_loss
            ))).await;
            varmap.save("model_weights.safetensors")?;
        } else {
            epochs_without_improvement += 1;
            if epochs_without_improvement >= patience {
                let _ = tx.send(TrainMessage::Log(format!(
                    "Early stopping at epoch {}. Best val loss: {:.6}",
                    epoch + 1, best_val_loss
                ))).await;
                stop_reason = Some(format!(
                    "early_stopping_after_{}_epochs_without_improvement",
                    patience
                ));
                break;
            }
        }

        if (epoch + 1) % 50 == 0 {
            let current_lr = opt.learning_rate();
            opt.set_learning_rate(current_lr * 0.5);
            let _ = tx.send(TrainMessage::Log(format!(
                "LR decay -> {:.6}", current_lr * 0.5
            ))).await;
        }
    }

    let _ = tx.send(TrainMessage::Log(format!(
        "Training complete. Best val loss: {:.6}", best_val_loss
    ))).await;

    if let Some(path) = &live_log_path {
        let end_event = RealtimeLogEvent {
            event: "end".to_string(),
            timestamp: Utc::now().to_rfc3339(),
            run_type: "gui_progress".to_string(),
            epoch: Some(epoch_metrics.len()),
            train_loss: epoch_metrics.last().map(|e| e.train_loss),
            val_loss: epoch_metrics.last().map(|e| e.val_loss),
            best_val_loss: Some(best_val_loss),
            message: Some(stop_reason.clone().unwrap_or_else(|| "finished".to_string())),
        };
        let _ = append_realtime_event(path, &end_event);
    }

    let run_log = TrainingRunLog {
        run_type: "gui_progress".to_string(),
        started_at: started_at.to_rfc3339(),
        finished_at: Utc::now().to_rfc3339(),
        use_cuda,
        data_range: DATA_RANGE.to_string(),
        symbols: TRAINING_SYMBOLS.iter().map(|s| s.to_string()).collect(),
        epochs_requested: epochs,
        epochs_completed: epoch_metrics.len(),
        batch_size,
        learning_rate,
        patience,
        best_val_loss,
        stop_reason,
        epoch_metrics,
    };

    match persist_training_log(&run_log) {
        Ok(path) => {
            let _ = tx.send(TrainMessage::Log(format!(
                "Training JSON log saved: {}",
                path.display()
            ))).await;
        }
        Err(e) => {
            let _ = tx.send(TrainMessage::Log(format!(
                "Failed to save training JSON log: {}",
                e
            ))).await;
        }
    }

    Ok(())
}

pub async fn train_model_with_data(
    train_data: TrainingDataset,
    val_data: TrainingDataset,
    epochs: Option<usize>,
    batch_size: Option<usize>,
    learning_rate: Option<f64>,
    patience: Option<usize>,
    use_cuda: bool,
) -> Result<()> {
    let started_at = Utc::now();
    let live_log_path = create_realtime_log_file("cli").ok();
    if let Some(path) = &live_log_path {
        let start_event = RealtimeLogEvent {
            event: "start".to_string(),
            timestamp: Utc::now().to_rfc3339(),
            run_type: "cli".to_string(),
            epoch: None,
            train_loss: None,
            val_loss: None,
            best_val_loss: None,
            message: Some("training_started".to_string()),
        };
        let _ = append_realtime_event(path, &start_event);
        info!("Realtime training log: {}", path.display());
    }
    let device = get_device(use_cuda);

    let epochs = epochs.unwrap_or(EPOCHS);
    let default_batch_size = if use_cuda { CUDA_BATCH_SIZE } else { BATCH_SIZE };
    let batch_size = batch_size.unwrap_or(default_batch_size);
    let learning_rate = learning_rate.unwrap_or(LEARNING_RATE);

    info!("Training Set: {} samples", train_data.features.len());
    info!("Validation Set: {} samples", val_data.features.len());

    // 2. Initialize Model
    let varmap = VarMap::new();
    let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);

    let num_assets = TRAINING_SYMBOLS.len();

    let encoder = RNNEncoder::new(INPUT_DIM, HIDDEN_DIM, LSTM_LAYERS, DROPOUT_RATE, vb.pp("encoder"))?;
    let model = EpsilonTheta::new(1, HIDDEN_DIM, HIDDEN_DIM, NUM_LAYERS, num_assets, DROPOUT_RATE, vb.pp("model"))?;
    let diffusion = GaussianDiffusion::new(DIFF_STEPS, &device)?;

    let params = candle_nn::ParamsAdamW {
        lr: learning_rate,
        weight_decay: WEIGHT_DECAY,
        ..Default::default()
    };
    let mut opt = candle_nn::AdamW::new(varmap.all_vars(), params)?;

    // 3. Training Loop
    let num_train_samples = train_data.features.len();
    let num_train_batches = num_train_samples / batch_size;
    
    let num_val_samples = val_data.features.len();
    let num_val_batches = if num_val_samples > 0 { num_val_samples / batch_size } else { 0 };

    let mut best_val_loss = f64::INFINITY;
    let patience = patience.unwrap_or(PATIENCE);
    let mut epochs_without_improvement: usize = 0;
    let mut epoch_metrics: Vec<EpochLogEntry> = Vec::with_capacity(epochs);
    let mut stop_reason: Option<String> = None;

    let device_label = if use_cuda && cfg!(feature = "cuda") {
        "CUDA"
    } else {
        "CPU"
    };
    info!(
        "Training on {} with batch_size={}, epochs={}, lr={:.6}",
        device_label,
        batch_size,
        epochs,
        learning_rate
    );

    for epoch in 0..epochs {
        let epoch_start = Instant::now();
        let mut total_train_loss = 0.0;
        
        // --- Training Phase ---
        // Shuffle indices
        let indices: Vec<usize> = (0..num_train_samples).collect();
        let mut indices = indices;
        indices.shuffle(&mut rand::thread_rng());

        for batch_idx in 0..num_train_batches {
            let start = batch_idx * batch_size;
            let end = start + batch_size;
            let batch_indices = &indices[start..end];

            // Prepare Batch Tensors
            let mut batch_features = Vec::with_capacity(batch_size);
            let mut batch_targets = Vec::with_capacity(batch_size);
            let mut batch_asset_ids = Vec::with_capacity(batch_size);

            for &idx in batch_indices {
                batch_features.push(Tensor::from_slice(&train_data.features[idx], (LOOKBACK, 2), &device)?.to_dtype(DType::F32)?);
                batch_targets.push(Tensor::from_slice(&train_data.targets[idx], (FORECAST, 1), &device)?.to_dtype(DType::F32)?);
                batch_asset_ids.push(train_data.asset_ids[idx] as u32);
            }

            let x_hist = Tensor::stack(&batch_features, 0)?; 
            let x_0 = Tensor::stack(&batch_targets, 0)?;     
            let asset_ids = Tensor::new(batch_asset_ids.as_slice(), &device)?;
            
            let x_0 = x_0.permute((0, 2, 1))?; 

            // Encode History
            let cond = encoder.forward(&x_hist, true)?; 
            let cond = cond.unsqueeze(2)?; 

            // Sample t
            let t = Tensor::rand(0.0f32, DIFF_STEPS as f32, (batch_size,), &device)?
                .floor()?
                .clamp(0.0, (DIFF_STEPS - 1) as f64)?;

            let epsilon = Tensor::randn(0.0f32, 1.0f32, x_0.shape(), &device)?;

            let t_u32 = t.to_dtype(DType::U32)?;
            
            let alpha_bar_t = diffusion.alpha_bar.index_select(&t_u32, 0)?; 
            let sqrt_alpha_bar_t = alpha_bar_t.sqrt()?;
            let sqrt_one_minus_alpha_bar_t = (1.0 - alpha_bar_t)?.sqrt()?;
            
            let sqrt_alpha_bar_t = sqrt_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;
            let sqrt_one_minus_alpha_bar_t = sqrt_one_minus_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;
            
            let x_t = (x_0.broadcast_mul(&sqrt_alpha_bar_t)? + epsilon.broadcast_mul(&sqrt_one_minus_alpha_bar_t)?)?;
            
            let t_in = t.unsqueeze(1)?;
            let epsilon_pred = model.forward(&x_t, &t_in, &asset_ids, &cond, true)?;
            
            let loss = (epsilon - epsilon_pred)?.sqr()?.mean_all()?;
            
            opt.backward_step(&loss)?;
            total_train_loss += loss.to_scalar::<f32>()? as f64;

            let batch_no = batch_idx + 1;
            let should_log = batch_no % TRAIN_LOG_INTERVAL_BATCHES == 0 || batch_no == num_train_batches;
            if should_log {
                let progress = (batch_no as f64 / num_train_batches.max(1) as f64) * 100.0;
                info!(
                    "Epoch {}/{} progress: {}/{} batches ({:.1}%), elapsed: {:.1}s",
                    epoch + 1,
                    epochs,
                    batch_no,
                    num_train_batches,
                    progress,
                    epoch_start.elapsed().as_secs_f64()
                );
            }
        }
        
        let avg_train_loss = total_train_loss / num_train_batches.max(1) as f64;

        // --- Validation Phase ---
        let mut total_val_loss = 0.0;
        if num_val_batches > 0 {
            for batch_idx in 0..num_val_batches {
                let start = batch_idx * batch_size;
                let end = start + batch_size;
                
                // No shuffle for validation
                let mut batch_features = Vec::with_capacity(batch_size);
                let mut batch_targets = Vec::with_capacity(batch_size);
                let mut batch_asset_ids = Vec::with_capacity(batch_size);

                for idx in start..end {
                    batch_features.push(Tensor::from_slice(&val_data.features[idx], (LOOKBACK, 2), &device)?.to_dtype(DType::F32)?);
                    batch_targets.push(Tensor::from_slice(&val_data.targets[idx], (FORECAST, 1), &device)?.to_dtype(DType::F32)?);
                    batch_asset_ids.push(val_data.asset_ids[idx] as u32);
                }

                let x_hist = Tensor::stack(&batch_features, 0)?;
                let x_0 = Tensor::stack(&batch_targets, 0)?;
                let asset_ids = Tensor::new(batch_asset_ids.as_slice(), &device)?;
                let x_0 = x_0.permute((0, 2, 1))?;

                let cond = encoder.forward(&x_hist, false)?;
                let cond = cond.unsqueeze(2)?;

                let t = Tensor::rand(0.0f32, DIFF_STEPS as f32, (batch_size,), &device)?
                    .floor()?
                    .clamp(0.0, (DIFF_STEPS - 1) as f64)?;
                let epsilon = Tensor::randn(0.0f32, 1.0f32, x_0.shape(), &device)?;

                let t_u32 = t.to_dtype(DType::U32)?;

                let alpha_bar_t = diffusion.alpha_bar.index_select(&t_u32, 0)?;
                let sqrt_alpha_bar_t = alpha_bar_t.sqrt()?;
                let sqrt_one_minus_alpha_bar_t = (1.0 - alpha_bar_t)?.sqrt()?;

                let sqrt_alpha_bar_t = sqrt_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;
                let sqrt_one_minus_alpha_bar_t = sqrt_one_minus_alpha_bar_t.unsqueeze(1)?.unsqueeze(2)?;

                let x_t = (x_0.broadcast_mul(&sqrt_alpha_bar_t)? + epsilon.broadcast_mul(&sqrt_one_minus_alpha_bar_t)?)?;

                let t_in = t.unsqueeze(1)?;
                let epsilon_pred = model.forward(&x_t, &t_in, &asset_ids, &cond, false)?;

                let loss = (epsilon - epsilon_pred)?.sqr()?.mean_all()?;
                total_val_loss += loss.to_scalar::<f32>()? as f64;
            }
        }
        
        let avg_val_loss = if num_val_batches > 0 { total_val_loss / num_val_batches as f64 } else { 0.0 };
        epoch_metrics.push(EpochLogEntry {
            epoch: epoch + 1,
            train_loss: avg_train_loss,
            val_loss: avg_val_loss,
        });

        if let Some(path) = &live_log_path {
            let epoch_event = RealtimeLogEvent {
                event: "epoch".to_string(),
                timestamp: Utc::now().to_rfc3339(),
                run_type: "cli".to_string(),
                epoch: Some(epoch + 1),
                train_loss: Some(avg_train_loss),
                val_loss: Some(avg_val_loss),
                best_val_loss: Some(best_val_loss.min(avg_val_loss)),
                message: None,
            };
            if let Err(e) = append_realtime_event(path, &epoch_event) {
                warn!("Failed to append realtime log: {}", e);
            }
        }

        if (epoch + 1) % 10 == 0 {
            if let Some((train_avg, val_avg, train_slope, val_slope)) = compute_window_trend(&epoch_metrics, 10) {
                info!(
                    "Trend@{} (last 10): train_avg={:.6}, val_avg={:.6}, train_slope={:+.6}/epoch, val_slope={:+.6}/epoch",
                    epoch + 1,
                    train_avg,
                    val_avg,
                    train_slope,
                    val_slope
                );

                if let Some(path) = &live_log_path {
                    let trend_event = RealtimeLogEvent {
                        event: "trend".to_string(),
                        timestamp: Utc::now().to_rfc3339(),
                        run_type: "cli".to_string(),
                        epoch: Some(epoch + 1),
                        train_loss: Some(train_avg),
                        val_loss: Some(val_avg),
                        best_val_loss: Some(best_val_loss.min(avg_val_loss)),
                        message: Some(format!(
                            "window=10,train_slope={:+.6},val_slope={:+.6}",
                            train_slope,
                            val_slope
                        )),
                    };
                    let _ = append_realtime_event(path, &trend_event);
                }
            }
        }

        info!("Epoch {}: Train Loss = {:.6}, Val Loss = {:.6}", epoch + 1, avg_train_loss, avg_val_loss);

        // Checkpoint
        if avg_val_loss < best_val_loss {
            best_val_loss = avg_val_loss;
            epochs_without_improvement = 0;
            info!("New best model found! Saving weights...");
            varmap.save("model_weights.safetensors")?;
        } else {
            epochs_without_improvement += 1;
            if epochs_without_improvement >= patience {
                info!("Early stopping: no improvement for {} epochs. Best val loss: {:.6}", patience, best_val_loss);
                stop_reason = Some(format!(
                    "early_stopping_after_{}_epochs_without_improvement",
                    patience
                ));
                break;
            }
        }

        if (epoch + 1) % 50 == 0 {
            let current_lr = opt.learning_rate();
            opt.set_learning_rate(current_lr * 0.5);
            info!("Decaying learning rate to {:.6}", current_lr * 0.5);
        }
    }

    info!("Training finished. Best Validation Loss: {:.6}", best_val_loss);

    if let Some(path) = &live_log_path {
        let end_event = RealtimeLogEvent {
            event: "end".to_string(),
            timestamp: Utc::now().to_rfc3339(),
            run_type: "cli".to_string(),
            epoch: Some(epoch_metrics.len()),
            train_loss: epoch_metrics.last().map(|e| e.train_loss),
            val_loss: epoch_metrics.last().map(|e| e.val_loss),
            best_val_loss: Some(best_val_loss),
            message: Some(stop_reason.clone().unwrap_or_else(|| "finished".to_string())),
        };
        let _ = append_realtime_event(path, &end_event);
    }

    let run_log = TrainingRunLog {
        run_type: "cli".to_string(),
        started_at: started_at.to_rfc3339(),
        finished_at: Utc::now().to_rfc3339(),
        use_cuda,
        data_range: DATA_RANGE.to_string(),
        symbols: TRAINING_SYMBOLS.iter().map(|s| s.to_string()).collect(),
        epochs_requested: epochs,
        epochs_completed: epoch_metrics.len(),
        batch_size,
        learning_rate,
        patience,
        best_val_loss,
        stop_reason,
        epoch_metrics,
    };

    match persist_training_log(&run_log) {
        Ok(path) => info!("Training JSON log saved: {}", path.display()),
        Err(e) => warn!("Failed to save training JSON log: {}", e),
    }

    Ok(())
}

async fn fetch_training_data() -> Result<(TrainingDataset, TrainingDataset)> {
    let symbols = TRAINING_SYMBOLS.to_vec();
    
    // Parallel data fetching — download all symbols concurrently
    info!("Fetching {} symbols in parallel...", symbols.len());
    let mut handles = Vec::with_capacity(symbols.len());
    for (id, symbol) in symbols.iter().enumerate() {
        let sym = symbol.to_string();
        handles.push(tokio::spawn(async move {
            info!("Fetching {} (ID: {})...", sym, id);
            match StockData::fetch_range(&sym, DATA_RANGE).await {
                Ok(data) => {
                    let dataset = data.prepare_training_data(LOOKBACK, FORECAST, id);
                    Ok((id, sym, dataset))
                }
                Err(e) => Err((sym, e))
            }
        }));
    }

    let mut all_features = Vec::new();
    let mut all_targets = Vec::new();
    let mut all_asset_ids = Vec::new();

    // Collect results
    let mut results: Vec<_> = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await?);
    }
    // Sort by asset ID to ensure deterministic ordering
    results.sort_by_key(|r| match r {
        Ok((id, _, _)) => *id,
        Err(_) => usize::MAX,
    });

    for result in results {
        match result {
            Ok((_id, sym, dataset)) => {
                info!("{}: {} samples", sym, dataset.features.len());
                all_features.extend(dataset.features);
                all_targets.extend(dataset.targets);
                all_asset_ids.extend(dataset.asset_ids);
            }
            Err((sym, e)) => error!("Failed to fetch {}: {}", sym, e),
        }
    }

    info!("Original samples: {}", all_features.len());

    // Data augmentation: add Gaussian noise copies (parallelized with rayon)
    let original_len = all_features.len();
    let augmented: Vec<(Vec<f64>, Vec<f64>, usize)> = (0..AUGMENTATION_COPIES)
        .into_par_iter()
        .flat_map(|_| {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let mut batch = Vec::with_capacity(original_len);
            for i in 0..original_len {
                let aug_features: Vec<f64> = all_features[i]
                    .iter()
                    .map(|&v| v + rng.gen_range(-AUGMENTATION_NOISE..AUGMENTATION_NOISE))
                    .collect();
                let aug_targets: Vec<f64> = all_targets[i]
                    .iter()
                    .map(|&v| v + rng.gen_range(-AUGMENTATION_NOISE * 0.5..AUGMENTATION_NOISE * 0.5))
                    .collect();
                batch.push((aug_features, aug_targets, all_asset_ids[i]));
            }
            batch
        })
        .collect();

    for (feat, tgt, aid) in augmented {
        all_features.push(feat);
        all_targets.push(tgt);
        all_asset_ids.push(aid);
    }

    info!("After augmentation ({}x): {} samples", AUGMENTATION_COPIES + 1, all_features.len());
    
    let full_dataset = TrainingDataset {
        features: all_features,
        targets: all_targets,
        asset_ids: all_asset_ids,
    };

    // Split 80% Train, 20% Validation
    Ok(full_dataset.split(0.8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::StockData;

    #[tokio::test]
    async fn test_train_model_integration() {
        // 1. Create Mock Data
        let mock_data = StockData::new_mock("TEST", 200);
        let dataset = mock_data.prepare_training_data(LOOKBACK, FORECAST, 0);
        let (train_data, val_data) = dataset.split(0.8);

        // 2. Run Training (Short run)
        let result = train_model_with_data(
            train_data,
            val_data,
            Some(1), // 1 Epoch
            Some(16), // Small batch
            Some(1e-3),
            None, // Default patience
            false, // CPU for tests
        ).await;

        assert!(result.is_ok());
        
        // Cleanup
        if std::path::Path::new("model_weights.safetensors").exists() {
            std::fs::remove_file("model_weights.safetensors").unwrap();
        }
    }
}

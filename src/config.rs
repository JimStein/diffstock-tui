use candle_core::Device;
use rayon::ThreadPoolBuilder;
use std::sync::OnceLock;
use tracing::{info, warn};

static RAYON_INIT: OnceLock<()> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComputeBackend {
    Auto,
    Cuda,
    Directml,
    Cpu,
}

pub fn init_cpu_parallelism() {
    RAYON_INIT.get_or_init(|| {
        let num_threads = num_cpus::get().max(1);
        match ThreadPoolBuilder::new().num_threads(num_threads).build_global() {
            Ok(_) => info!(
                "Initialized Rayon thread pool with {} threads (all logical CPU cores)",
                num_threads
            ),
            Err(e) => warn!(
                "Rayon thread pool already initialized or unavailable ({}). Using existing configuration.",
                e
            ),
        }
    });
}

pub fn resolve_compute_backend(requested: ComputeBackend, context: &str) -> ComputeBackend {
    match requested {
        ComputeBackend::Auto => {
            if cfg!(feature = "cuda") {
                info!("Compute backend=auto for {} -> selected cuda", context);
                ComputeBackend::Cuda
            } else {
                info!("Compute backend=auto for {} -> selected cpu", context);
                ComputeBackend::Cpu
            }
        }
        ComputeBackend::Cuda => {
            if cfg!(feature = "cuda") {
                info!("Compute backend=cuda for {}", context);
                ComputeBackend::Cuda
            } else {
                warn!(
                    "Compute backend=cuda requested for {}, but binary lacks CUDA feature. Falling back to cpu.",
                    context
                );
                ComputeBackend::Cpu
            }
        }
        ComputeBackend::Directml => {
            info!("Compute backend=directml requested for {}", context);
            ComputeBackend::Directml
        }
        ComputeBackend::Cpu => {
            info!("Compute backend=cpu for {}", context);
            ComputeBackend::Cpu
        }
    }
}

pub fn find_directml_onnx_model_path() -> Option<std::path::PathBuf> {
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();

    if let Ok(path) = std::env::var("DIFFSTOCK_ORT_MODEL") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            candidates.push(std::path::PathBuf::from(trimmed));
        }
    }

    candidates.push(std::path::PathBuf::from("model_weights.onnx"));
    candidates.push(std::path::PathBuf::from("model.onnx"));
    candidates.push(std::path::PathBuf::from("onnx/model.onnx"));

    candidates.into_iter().find(|p| p.exists() && p.is_file())
}

pub fn project_root_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn project_file_path(file_name: &str) -> std::path::PathBuf {
    project_root_path().join(file_name)
}

pub fn model_weights_safetensors_path() -> std::path::PathBuf {
    project_file_path("model_weights.safetensors")
}

pub fn find_model_weights_safetensors_path() -> Option<std::path::PathBuf> {
    let path = model_weights_safetensors_path();
    if path.exists() && path.is_file() {
        Some(path)
    } else {
        None
    }
}

pub fn get_device(use_cuda: bool) -> Device {
    if use_cuda {
        #[cfg(feature = "cuda")]
        {
            match Device::new_cuda(0) {
                Ok(device) => {
                    info!("Using CUDA device 0");
                    return device;
                }
                Err(e) => {
                    warn!("Failed to initialize CUDA: {}. Falling back to CPU.", e);
                }
            }
        }
        #[cfg(not(feature = "cuda"))]
        {
            warn!("--cuda flag set but binary was compiled without the 'cuda' feature. Falling back to CPU.");
        }
    }

    #[cfg(feature = "mkl")]
    info!("Using CPU device with Intel MKL BLAS acceleration");

    #[cfg(not(feature = "mkl"))]
    info!("Using CPU device (tip: compile with --features mkl for 3-5x faster matrix ops)");

    let num_threads = num_cpus::get();
    info!("CPU threads available: {}", num_threads);

    Device::Cpu
}

pub const LOOKBACK: usize = 60;
pub const FORECAST: usize = 10;
pub const BATCH_SIZE: usize = 128;
pub const CUDA_BATCH_SIZE: usize = 256;
pub const EPOCHS: usize = 500;
pub const LEARNING_RATE: f64 = 1.5e-4;
pub const INPUT_DIM: usize = 2;
pub const HIDDEN_DIM: usize = 512;
pub const NUM_LAYERS: usize = 8;
pub const DIFF_STEPS: usize = 200;
pub const PATIENCE: usize = 120;
pub const LSTM_LAYERS: usize = 2;
pub const DROPOUT_RATE: f64 = 0.10;
pub const WEIGHT_DECAY: f64 = 0.01;
/// Range of historical data to fetch for training (e.g., "5y", "10y", "max")
pub const DATA_RANGE: &str = "10y";
/// Gaussian noise stddev for data augmentation on normalized returns
pub const AUGMENTATION_NOISE: f64 = 0.01;
/// Number of augmented copies per original sample
pub const AUGMENTATION_COPIES: usize = 1;

// ── Inference Performance Settings ──────────────────────────────────────────
/// Number of DDIM steps for fast inference (vs DIFF_STEPS for training)
/// 25 steps provides ~8x speedup over 200 steps with minimal quality loss
pub const DDIM_INFERENCE_STEPS: usize = 100;
/// Batch size for Monte Carlo simulations during inference/portfolio
/// Higher = faster on CPU with large RAM (64GB can handle 256 easily)
pub const INFERENCE_BATCH_SIZE: usize = 256;
/// Higher inference batch size for modern GPUs like RTX 3090.
pub const CUDA_INFERENCE_BATCH_SIZE: usize = 512;
/// Emit training progress every N batches (CLI/GPU visibility).
pub const TRAIN_LOG_INTERVAL_BATCHES: usize = 20;
/// DDIM eta parameter: 0.0 = deterministic, 1.0 = same as DDPM
pub const DDIM_ETA: f64 = 0.0;

pub const TRAINING_SYMBOLS: &[&str] = &[
    "SPY", "DIA", "QQQ", "XLK", "XLI", "XLF", "XLC", "XLY", "XLRE", "XLV", "XLU", "XLP", "XLE",
    "XLB", "ARKK", "NVDA", "QQQI", "RDVI", "AMZN", "META", "GOOGL", "AAPL","MSFT",
    "IAU","SLV","ETN","TSLA","TLT","URA","COPX"
];

/// Default symbols for portfolio optimization when none are specified.
#[allow(dead_code)]
pub const DEFAULT_PORTFOLIO_SYMBOLS: &[&str] = &[
    "NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "QQQ", "SPY",
];

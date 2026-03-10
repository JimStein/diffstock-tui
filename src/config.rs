use candle_core::Device;
use rayon::ThreadPoolBuilder;
use std::path::{Path, PathBuf};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrtPrecision {
    Fp16,
    Fp32,
}

impl OrtPrecision {
    pub fn as_str(self) -> &'static str {
        match self {
            OrtPrecision::Fp16 => "fp16",
            OrtPrecision::Fp32 => "fp32",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirectmlModelSelection {
    pub path: PathBuf,
    pub precision: OrtPrecision,
    pub requested_precision: OrtPrecision,
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

pub fn configured_directml_precision() -> OrtPrecision {
    match std::env::var("DIFFSTOCK_ORT_PRECISION") {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "fp16" | "float16" | "16" | "half" => OrtPrecision::Fp16,
            "fp32" | "float32" | "32" | "single" => OrtPrecision::Fp32,
            other => {
                warn!(
                    "Unrecognized DIFFSTOCK_ORT_PRECISION='{}'. Falling back to fp32.",
                    other
                );
                OrtPrecision::Fp32
            }
        },
        Err(_) => OrtPrecision::Fp32,
    }
}

fn detect_onnx_precision_from_path(path: &Path) -> OrtPrecision {
    let lower = path
        .file_name()
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_else(|| path.to_string_lossy().to_ascii_lowercase());

    if lower.contains(".fp16") || lower.contains("_fp16") || lower.contains("-fp16") {
        OrtPrecision::Fp16
    } else {
        OrtPrecision::Fp32
    }
}

fn directml_onnx_candidates_for(precision: OrtPrecision, project_root: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    match precision {
        OrtPrecision::Fp16 => {
            candidates.push(project_root.join("model_weights.fp16.onnx"));
            candidates.push(project_root.join("model.fp16.onnx"));
            candidates.push(project_root.join("onnx/model.fp16.onnx"));
            candidates.push(PathBuf::from("model_weights.fp16.onnx"));
            candidates.push(PathBuf::from("model.fp16.onnx"));
            candidates.push(PathBuf::from("onnx/model.fp16.onnx"));
        }
        OrtPrecision::Fp32 => {
            candidates.push(project_root.join("model_weights.onnx"));
            candidates.push(project_root.join("model.onnx"));
            candidates.push(project_root.join("onnx/model.onnx"));
            candidates.push(PathBuf::from("model_weights.onnx"));
            candidates.push(PathBuf::from("model.onnx"));
            candidates.push(PathBuf::from("onnx/model.onnx"));
        }
    }
    candidates
}

pub fn select_directml_onnx_model() -> Option<DirectmlModelSelection> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    let project_root = project_root_path();
    let requested_precision = configured_directml_precision();

    if let Ok(path) = std::env::var("DIFFSTOCK_ORT_MODEL") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            let env_path = PathBuf::from(trimmed);
            candidates.push(env_path.clone());
            if !env_path.is_absolute() {
                candidates.push(project_root.join(env_path));
            }
        }
    }

    candidates.extend(directml_onnx_candidates_for(requested_precision, &project_root));
    candidates.extend(directml_onnx_candidates_for(
        match requested_precision {
            OrtPrecision::Fp16 => OrtPrecision::Fp32,
            OrtPrecision::Fp32 => OrtPrecision::Fp16,
        },
        &project_root,
    ));

    candidates
        .into_iter()
        .find(|path| path.exists() && path.is_file())
        .map(|path| DirectmlModelSelection {
            precision: detect_onnx_precision_from_path(&path),
            path,
            requested_precision,
        })
}

pub fn find_directml_onnx_model_path() -> Option<PathBuf> {
    select_directml_onnx_model().map(|selection| selection.path)
}

pub fn project_root_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn project_file_path(file_name: &str) -> std::path::PathBuf {
    project_root_path().join(file_name)
}

fn parse_env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

pub fn configured_training_batch_size(use_cuda: bool) -> usize {
    if use_cuda {
        parse_env_usize("DIFFSTOCK_CUDA_BATCH_SIZE")
            .or_else(|| parse_env_usize("DIFFSTOCK_BATCH_SIZE"))
            .unwrap_or(CUDA_BATCH_SIZE)
    } else {
        parse_env_usize("DIFFSTOCK_BATCH_SIZE").unwrap_or(BATCH_SIZE)
    }
}

pub fn configured_onnx_opset() -> usize {
    parse_env_usize("DIFFSTOCK_ONNX_OPSET").unwrap_or(ONNX_OPSET)
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
pub const CUDA_BATCH_SIZE: usize = 512;
pub const EPOCHS: usize = 500;
pub const LEARNING_RATE: f64 = 1.5e-4;
pub const INPUT_DIM: usize = 2;
pub const HIDDEN_DIM: usize = 512;
pub const NUM_LAYERS: usize = 8;
pub const DIFF_STEPS: usize = 200;
pub const ONNX_OPSET: usize = 18;
pub const PATIENCE: usize = 120;
pub const LSTM_LAYERS: usize = 2;
pub const DROPOUT_RATE: f64 = 0.10;
pub const WEIGHT_DECAY: f64 = 0.01;
/// Range of historical data to fetch for training (e.g., "5y", "10y", "max")
pub const DATA_RANGE: &str = "5y";
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

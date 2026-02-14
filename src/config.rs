use candle_core::Device;
use tracing::{info, warn};

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
    info!("Using CPU device");
    Device::Cpu
}

pub const LOOKBACK: usize = 50;
pub const FORECAST: usize = 10;
pub const BATCH_SIZE: usize = 64;
pub const EPOCHS: usize = 200;
pub const LEARNING_RATE: f64 = 1e-3;
pub const INPUT_DIM: usize = 2;
pub const HIDDEN_DIM: usize = 128;
pub const NUM_LAYERS: usize = 4;
pub const DIFF_STEPS: usize = 100;
pub const PATIENCE: usize = 20;

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

# DiffStock-TUI

**DiffStock-TUI** is a Rust-based TUI/GUI application for probabilistic stock price forecasting using a **Conditional Diffusion Model**. It uses Hugging Face's `candle` framework to generate high-fidelity future price paths.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-2024-orange.svg)
![AI](https://img.shields.io/badge/AI-Diffusion%20Model-purple.svg)

## Features

*   **Generative AI**: TimeGrad-inspired diffusion model (LSTM Encoder + WaveNet Denoiser).
*   **Multi-Asset Training**: Learns asset-specific patterns using ID embeddings.
*   **Robust Training**: Includes validation sets, model checkpointing (saves best weights), early stopping, and configurable hyperparameters.
*   **Probabilistic Inference**: Generates P10-P90 confidence intervals via 500+ Monte Carlo paths.
*   **Efficient Data**: Local caching (`.cache/`) and retry logic for Yahoo Finance API.
*   **Dual Interface**: Keyboard-driven TUI (`ratatui`) and interactive GUI (`egui`).

## Requirements

*   **Rust**: Stable or Nightly (2024 edition support required).
*   **CUDA Toolkit** (optional): Required only for GPU acceleration via the `cuda` feature.

## Installation

```bash
git clone https://github.com/sudorambo/diffstock-tui.git
cd diffstock-tui

# Build for CPU (default)
cargo build --release

# Build with CUDA GPU support (requires CUDA toolkit)
cargo build --release --features cuda
```

## Usage

### 1. Training
Train the model on 5 years of historical data (SPY, QQQ, etc.). The model automatically splits data (80/20) and saves the best weights to `model_weights.safetensors`.

**Default Training:**
```bash
cargo run --release -- --train
```

**Custom Hyperparameters:**
```bash
cargo run --release -- --train --epochs 100 --batch-size 32 --learning-rate 0.0005 --patience 30
```

**Train on GPU:**
```bash
cargo run --release --features cuda -- --train --cuda
```

**Select Compute Backend (new):**
```bash
# Auto (default): prefer CUDA when available, otherwise CPU
cargo run --release -- --train --compute-backend auto

# Force CUDA (requires build with --features cuda)
cargo run --release --features cuda -- --train --compute-backend cuda

# Request DirectML path (current build falls back to CPU with warning)
cargo run --release -- --train --compute-backend directml

# Force CPU
cargo run --release -- --train --compute-backend cpu
```

**DirectML (Windows AMD/Intel iGPU) build path:**
```bash
# Build/run with ORT DirectML support enabled
cargo run --features directml -- --webui --compute-backend directml

# Optional: explicitly provide ONNX model path
set DIFFSTOCK_ORT_MODEL=D:\path\to\model.onnx
cargo run --features directml -- --webui --compute-backend directml
```

When `--compute-backend directml` is selected, the runtime searches ONNX in this order:
1. `%DIFFSTOCK_ORT_MODEL%`
2. `model_weights.onnx`
3. `model.onnx`
4. `onnx/model.onnx`

If no ONNX model is found, runtime logs a warning and falls back to CPU.

### Automatic dual-artifact checkpointing (CUDA training)

During training, when a new best checkpoint is found:
- `model_weights.safetensors` is always saved.
- If training is running with CUDA, the trainer also attempts to export `model_weights.onnx` to the same directory.

Default ONNX export hook:
```bash
python tools/export_to_onnx.py --input model_weights.safetensors --output model_weights.onnx
```

You can override exporter and script via env vars:
```bash
set DIFFSTOCK_ONNX_EXPORTER=python
set DIFFSTOCK_ONNX_EXPORT_SCRIPT=tools/export_to_onnx.py
```

Note: `tools/export_to_onnx.py` is a project hook entrypoint. Replace it with your model-specific converter implementation.
Training stops early if validation loss doesn't improve for `--patience` epochs (default: 20). Configuration (tickers, defaults) can be edited in `src/config.rs`.

### 2. Forecasting
Run the interactive interface to visualize forecasts.

**Terminal UI (Default):**
```bash
cargo run --release
```
*   **Controls**: Type ticker -> `Enter` to fetch, `r` to reset, `q` to quit.

**Graphical UI:**
```bash
cargo run --release -- --gui
```

Add `--compute-backend <auto|cuda|directml|cpu>` to any command above to choose compute backend. `--cuda` remains as a compatibility shortcut for `--compute-backend cuda`.

### 3. Backtesting
Validate performance on historical SPY data.
```bash
cargo run --release -- --backtest
```

## Technical Architecture

*   **ML Framework**: `candle-core` & `candle-nn` (Rust-native).
*   **Model**: Conditional Gaussian Diffusion with LSTM context and Asset Embeddings.
*   **Backend**: `tokio` async runtime with `tracing` for structured logging.
*   **Frontend**: `ratatui` (TUI) and `egui` (GUI).

## Disclaimer

**Educational use only.** Do not use these forecasts for financial trading.

## License

MIT License - see [LICENSE](LICENSE).

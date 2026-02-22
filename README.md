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
*   **Efficient Data**: Local caching (`.cache/`) and retry logic with Polygon-first + Yahoo Finance fallback.
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

### Market data source (manual provider mode)

Default behavior is now manual provider selection:
- `polygon`: use Polygon/Massive chain only (WS -> snapshot -> minute aggregate)
- `yfinance`/`yahoo`: use Yahoo/yfinance only

Create `.env` from `.env.example` and set:

```bash
POLYGON_API_KEY=your_polygon_api_key_here
DIFFSTOCK_DATA_PROVIDER=polygon
DIFFSTOCK_WS_PRIORITY_RTH_ONLY=true
```

`DIFFSTOCK_DATA_PROVIDER` options:
- `polygon`: use Polygon/Massive only (no cross-provider fallback)
- `yahoo` or `yfinance`: use Yahoo/yfinance only

`DIFFSTOCK_WS_PRIORITY_RTH_ONLY` options:
- `true` (default): prioritize WebSocket only during US regular session (09:30-16:00 ET), otherwise use freshest source
- `false`: prioritize WebSocket all day (fallback to snapshot/minute when WS unavailable)

`auto` mode is disabled to avoid mixed-latency data alignment issues.

Training data default range is `5y`.

### Backend selection logic (important)

Current backend resolution is **explicit**, not a full auto-chain:

- `--compute-backend auto`
	- Selects `cuda` **only if** binary was built with `--features cuda`
	- Otherwise selects `cpu`
- `--compute-backend directml`
	- Uses DirectML path (requires build with `--features directml`)
	- If DirectML runtime/model unavailable at inference time, request falls back to CPU
- `--compute-backend cuda`
	- Requires build with `--features cuda`, otherwise falls back to CPU
- `--compute-backend cpu`
	- Always CPU

So the current behavior is **not** `cuda -> directml -> cpu` automatic cascading under `auto`.

### Automatic dual-artifact checkpointing (CUDA training)

During training, when a new best checkpoint is found:
- `model_weights.safetensors` is always saved.
- If training is running with CUDA, the trainer also attempts to export `model_weights.onnx` to the same directory.

Default ONNX export hook:
```bash
python tools/export_to_onnx.py --input model_weights.safetensors --output model_weights.onnx
```

Exporter dependencies (Python):
```bash
pip install torch onnx safetensors numpy
```

Current exporter behavior:
- Converts `model_weights.safetensors` into executable ONNX subgraphs:
	- Denoiser: `model_weights.onnx`
	- Encoder: `model_weights.encoder.onnx`
- The denoiser graph has runtime inputs (`x_t`, `time_steps`, `asset_ids`, `cond`) and can be consumed by ORT.

### If you only have `model_weights.safetensors`

You can convert it manually at any time:

```bash
# From project root (recommended)
py tools/export_to_onnx.py --input model_weights.safetensors --output model_weights.onnx
```

Equivalent absolute-path example:

```bash
py d:\DiffStock\diffstock-tui\tools\export_to_onnx.py --input d:\DiffStock\diffstock-tui\model_weights.safetensors --output d:\DiffStock\diffstock-tui\model_weights.onnx
```

Expected outputs:
- `model_weights.onnx`
- `model_weights.encoder.onnx`

Recommended DirectML run command (release binary):

```bash
set ORT_DYLIB_PATH=d:\DiffStock\diffstock-tui\.runtime\ort_dml_1_24_1\onnxruntime\capi\onnxruntime.dll
set DIFFSTOCK_ORT_MODEL=d:\DiffStock\diffstock-tui\model_weights.onnx
cargo run --release --features directml -- --webui --webui-port 8099 --compute-backend directml
```

You can override exporter and script via env vars:
```bash
set DIFFSTOCK_ONNX_EXPORTER=python
set DIFFSTOCK_ONNX_EXPORT_SCRIPT=tools/export_to_onnx.py
```
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

## Portfolio Weights & Holdings Logic

### Portfolio optimizer (target weights)

`portfolio` mode outputs **target weights**, then paper trading executes those targets.

High-level optimizer flow:
1. Build per-asset MC return paths from diffusion forecasts
2. Compute mean return vector + covariance matrix
3. Sample many random weight vectors (`OPTIMIZER_SAMPLES`)
4. Keep candidates that satisfy constraints:
	- max single weight = `0.40`
	- min non-zero weight = `0.02`
5. Choose highest Sharpe candidate
6. Local refinement around best candidate using score:
	- `score = sharpe - 0.5 * cvar`
7. Apply vol targeting (`0.5x` to `2.0x` leverage cap)

Output is `PortfolioAllocation.weights` + risk/return stats.

### Paper trading holdings execution

Paper trading converts target weights into holdings at scheduled analysis times:

1. Compute current portfolio value: `cash + sum(shares * price)`
2. For each symbol:
	- target dollar = `portfolio_value * target_weight`
	- target shares = `floor(target_dollar / price)`
	- delta = `target_shares - current_shares`
3. Trade only if `|delta| >= 1` share
4. Buy side:
	- capped by cash affordability with fee (`TRADING_FEE_RATE = 0.0005`)
5. Sell side:
	- capped by available shares
6. Update `cash_usd`, `holdings_shares`, append trade records and snapshots

Default analysis schedule is local `02:30` and `23:30` (configurable from WebUI / API).

## Technical Architecture

*   **ML Framework**: `candle-core` & `candle-nn` (Rust-native).
*   **Model**: Conditional Gaussian Diffusion with LSTM context and Asset Embeddings.
*   **Backend**: `tokio` async runtime with `tracing` for structured logging.
*   **Frontend**: `ratatui` (TUI) and `egui` (GUI).

## Disclaimer

**Educational use only.** Do not use these forecasts for financial trading.

## License

MIT License - see [LICENSE](LICENSE).

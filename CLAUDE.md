# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Build (always use release for ML workloads)
cargo build --release

# Build with CUDA GPU support
cargo build --release --features cuda

# Run TUI (default mode)
cargo run --release

# Run GUI
cargo run --release -- --gui

# Train model (fetches 5y data for 18 symbols, saves best weights to model_weights.safetensors)
cargo run --release -- --train
cargo run --release -- --train --epochs 100 --batch-size 32 --learning-rate 0.0005 --patience 30

# Train/run on GPU (requires --features cuda at compile time)
cargo run --release --features cuda -- --train --cuda
cargo run --release --features cuda -- --cuda

# Backtest on SPY
cargo run --release -- --backtest

# Run portfolio optimizer (comma-separated symbols)
cargo run --release -- --portfolio NVDA,MSFT,AAPL,GOOGL,AMZN,META,QQQ,SPY

# Run all tests (must use --release due to candle integer overflow in debug)
cargo test --release

# Run a single test module
cargo test --release data::tests
cargo test --release diffusion::tests
cargo test --release train::tests
cargo test --release inference::tests
cargo test --release portfolio::tests
```

## Architecture

Probabilistic stock forecasting app using a TimeGrad-inspired conditional diffusion model, built with Hugging Face's `candle` framework (Rust-native ML).

### ML Pipeline

**Data** (`data.rs`) — Fetches OHLCV from Yahoo Finance with retry logic and local JSON caching (`.cache/`, 24h TTL). Produces sliding-window datasets (lookback=50, forecast=10) with z-score normalization. Supports train/val split (80/20).

**Model** (`models/time_grad.rs`) — Two-part architecture:
- `RNNEncoder`: LSTM that encodes historical context into a scalar conditioning signal
- `EpsilonTheta`: WaveNet-style denoiser with 4 residual blocks (dilated convolutions), diffusion step embeddings, and asset ID embeddings (supports 18 tickers)

**Diffusion** (`diffusion.rs`) — Gaussian diffusion with linear beta schedule (100 steps). Forward process adds noise; reverse process iteratively denoises via the model.

**Training** (`train.rs`) — Trains on multi-asset data. Samples random timesteps, adds noise, predicts noise (MSE loss). AdamW optimizer with LR decay (0.5x every 50 epochs). Checkpoints best model by validation loss to `model_weights.safetensors`. Early stopping halts training when validation loss plateaus (configurable via `--patience`, default 20 epochs).

**Inference** (`inference.rs`) — Loads saved weights, runs autoregressive diffusion sampling over 500+ Monte Carlo paths, outputs P10/P30/P50/P70/P90 percentile price cones.

### Portfolio Optimization

**Portfolio** (`portfolio.rs`) — Multi-asset portfolio optimizer powered by diffusion model forecasts. Pipeline:
1. Generates per-asset Monte Carlo return distributions via the trained diffusion model
2. Estimates cross-asset covariance matrix from MC paths
3. Optimizes weights via Monte Carlo random sampling + local refinement (Phase 1: max Sharpe, Phase 2: Sharpe–CVaR combined objective)
4. Applies vol-targeting overlay (scales leverage so annualized vol ≈ 16%)
5. Outputs weight allocation, share counts, risk metrics (Sharpe, CVaR), and price targets

Key constants in `portfolio.rs`: `PORTFOLIO_MC_PATHS=500`, `PORTFOLIO_HORIZON=10`, `RISK_FREE_RATE=0.05`, `MAX_SINGLE_WEIGHT=0.40`, `TARGET_ANNUAL_VOL=0.16`, `OPTIMIZER_SAMPLES=100000`.

### App Layer

**State machine** (`app.rs`) — `AppState` enum: `Input` → `Loading` → `Forecasting` → `Dashboard`. Uses tokio MPSC channels for async progress reporting.

**TUI** (`ui.rs`) — ratatui-based: line chart with historical prices, technical levels (support/resistance/pivot), and forecast cone. Controls: Enter (submit), r (reset), q (quit).

**GUI** (`gui.rs`) — egui-based: interactive plot with the same data. Buttons for Predict/Back.

### Device Selection (`config.rs`)

`get_device(use_cuda: bool)` — centralized device helper. Tries `Device::new_cuda(0)` when `--cuda` is passed and the `cuda` cargo feature is enabled; falls back to CPU with a warning otherwise. All modules (train, inference, app) call this instead of hardcoding `Device::Cpu`.

### Key Constants (`config.rs`)

`LOOKBACK=50`, `FORECAST=10`, `BATCH_SIZE=64`, `EPOCHS=200`, `LEARNING_RATE=1e-3`, `INPUT_DIM=2`, `HIDDEN_DIM=128`, `NUM_LAYERS=4`, `DIFF_STEPS=100`, `PATIENCE=20`, `TRAINING_SYMBOLS` (18 ETFs/stocks).

## Key Patterns

- All async code runs on tokio; data fetching and inference are async
- Neural network parameters managed via candle's `VarMap`/`VarBuilder`
- Model weights serialized as safetensors format
- Tests are inline (`#[cfg(test)]` modules within source files)
- Tests must run in release mode (`cargo test --release`) due to candle integer overflow in debug builds
- CUDA support is behind a cargo feature flag (`cuda`); the `--cuda` CLI flag selects GPU at runtime
- Rust edition 2024

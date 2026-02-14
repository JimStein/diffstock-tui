# Improvement Plan for DiffStock-TUI

## 1. Configuration Management
**Goal**: Remove hardcoded values and allow flexible configuration.
- [x] Create a `config.toml` or `src/config.rs` to store:
    - Training tickers (currently hardcoded in `src/train.rs`).
    - Hyperparameters: `LOOKBACK`, `FORECAST`, `BATCH_SIZE`, `EPOCHS`, `LEARNING_RATE`.
    - API endpoints/timeouts.
- [x] Update `src/train.rs` and `src/main.rs` to use this configuration.
- [x] Add CLI arguments to override config values (e.g., `--epochs 100`).

## 2. Error Handling & Logging
**Goal**: Improve observability and robustness.
- [x] Replace `println!` / `eprintln!` with `tracing` or `log` crate.
- [x] Implement retry logic for `fetch_range` in `src/data.rs` to handle transient network failures.
- [x] Add more context to errors (e.g., which ticker failed and why).

## 3. Data Caching
**Goal**: Reduce API usage and speed up training/development.
- [x] Implement a caching layer in `src/data.rs`.
- [x] Save fetched CSV/JSON to a `.cache/` directory.
- [x] Check cache before making network requests.

## 4. Testing
**Goal**: Ensure correctness of data processing and model logic.
- [x] Add unit tests for `StockData::prepare_training_data` in `src/data.rs`.
    - Verify shape of output tensors.
    - Verify normalization (mean ~0, std ~1).
- [x] Add unit tests for `GaussianDiffusion` logic.
- [x] Add integration test with mocked network calls.

## 5. Code Quality & Documentation
**Goal**: Maintainability.
- [x] Add doc comments (`///`) to public structs and functions.
- [x] Run `cargo clippy` and fix warnings.
- [x] Refactor `train_model` to be more modular (separate data loading, model init, training loop).

## 6. Feature Enhancements
- [x] **Model Checkpointing**: Save best model based on validation loss, not just the last epoch.
- [x] **Validation Set**: Split data into Train/Validation to monitor overfitting.
- [x] **Multi-Asset Training**: Train on multiple assets simultaneously with ID embeddings.

## 7. Centralized Model Hyperparameters
**Goal**: Eliminate silent weight shape mismatches between training and inference.
- [x] Move `INPUT_DIM`, `HIDDEN_DIM`, `NUM_LAYERS`, `DIFF_STEPS` to `config.rs` as constants.
- [x] Update `train.rs` and `inference.rs` to import from `config.rs` (removed duplicated local bindings).
- [x] Remove dead `_input_dim` variable in `train.rs`.

## 8. Training Robustness
**Goal**: Prevent rare OOB panics and reduce wasted compute.
- [x] Clamp timestep indices after `floor()` to `[0, DIFF_STEPS-1]` in both training and validation loops.
- [x] Fix validation loop `to_vec1` roundtrip — replaced with direct `to_dtype(DType::U32)`.
- [x] Add early stopping with configurable `--patience` CLI arg (default: 20 epochs).

## 9. Inference Improvements
**Goal**: Better UX and test coverage for the user-facing path.
- [x] Add `tracing::warn!` when `model_weights.safetensors` is missing (zeros fallback).
- [x] Add `test_inference_with_mock_data` — trains 1 epoch, runs inference, verifies output structure and percentile ordering.
- [x] Add `test_inference_without_weights` — verifies zeros fallback doesn't panic.

## 10. CUDA / GPU Support
**Goal**: Enable GPU-accelerated training and inference.
- [x] Add `cuda` cargo feature flag (`candle-core/cuda`, `candle-nn/cuda`).
- [x] Add `get_device(use_cuda)` helper in `config.rs` with automatic CPU fallback.
- [x] Add `--cuda` CLI flag threaded through train, inference, app, and GUI.
- [x] Fix GPU-hostile `to_vec1` roundtrip in training loop (replaced with `to_dtype(DType::U32)`).

## 11. Portfolio Optimization & Position Sizing
**Goal**: Automatically adjust portfolio weights across multiple assets using diffusion model forecasts.
- [x] Create `portfolio.rs` module with full pipeline (forecast → optimize → allocate).
- [x] Monte Carlo covariance estimation from diffusion model paths.
- [x] Mean-variance optimization with Sharpe maximization + CVaR risk control.
- [x] Constraint enforcement (max/min single-asset weight, sum-to-1).
- [x] Vol-targeting overlay (leverage scaling to target annualized vol).
- [x] `--portfolio NVDA,MSFT,...` CLI flag for running portfolio optimizer.
- [x] Pretty-printed allocation report with share counts for given capital.
- [x] Per-asset price targets (P10/P50/P90) from diffusion forecasts.
- [x] Unit tests for covariance symmetry, weight generation, optimizer correctness.
- [ ] Backtest portfolio strategy on historical data with rolling windows.
- [ ] TUI/GUI screen for portfolio dashboard.
- [ ] Transaction cost modeling (commissions, spread, slippage).
- [ ] Black-Litterman overlay for incorporating analyst views.

# DiffStock-TUI

**DiffStock-TUI** is a cutting-edge Terminal User Interface (TUI) application built in Rust for probabilistic stock price forecasting. It leverages **Generative AI** techniques—specifically a **TimeGrad-inspired Conditional Diffusion Model**—to generate high-fidelity future price paths.

Unlike traditional Monte Carlo simulations that rely on simple statistical properties (like GBM), DiffStock-TUI implements a full neural network architecture using Hugging Face's `candle` framework to learn and sample from the conditional distribution of future prices.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-2024-orange.svg)
![AI](https://img.shields.io/badge/AI-Diffusion%20Model-purple.svg)

## Features

*   **Generative AI Forecasting**: Implements a conditional diffusion probabilistic model (DDPM) for time-series forecasting.
*   **Deep Learning Architecture**:
    *   **Encoder**: LSTM (Long Short-Term Memory) network to process historical price context.
    *   **Denoiser**: WaveNet-style 1D Dilated Convolutional Network with Residual Blocks and Gated Activations.
    *   **Features**: Trains on Log Returns and Overnight Returns for robust pattern recognition.
*   **Training Pipeline**: Built-in training mode to learn from 5 years of market history (currently configured for ARKK).
*   **Probabilistic Inference**: Generates "Cones of Uncertainty" (P10, P30, P50, P70, P90) by sampling 500 distinct future paths.
*   **Backtesting Utility**: Verify model performance against historical data.
*   **Real-Time Data**: Fetches live OHLCV data from Yahoo Finance.
*   **Rust-Native ML**: Powered by `candle-core` and `candle-nn` for efficient CPU-based inference.
*   **Interactive TUI & GUI**: Choose between a keyboard-driven terminal interface (`ratatui`) or a modern graphical user interface (`egui`) with interactive charts.

## Installation

### Prerequisites
*   Rust and Cargo (latest stable version)
*   A terminal with TrueColor support (e.g., Alacritty, iTerm2, Windows Terminal, VS Code Integrated Terminal)

### Build from Source

```bash
git clone https://github.com/sudorambo/diffstock-tui.git
cd diffstock-tui
cargo build --release
```

The binary will be located in `target/release/diffstock-tui`.

## Usage

### 1. Training the Model
Before running inference, you should train the model to learn market patterns. This downloads 5 years of data and trains the diffusion model. The list of tickers used in training can be edited inside the train.rs source file. Currently: QQQ, DIA, SPY, and XL* sectors are in the list. Experiment and have fun. REMINDER: DO NOT USE FOR TRADING OR MAKING FINANCIAL DECISIONS. EDUCATIONAL USE ONLY.

```bash
cargo run --release -- --train
```
*   **Output**: Saves trained weights to `model_weights.safetensors`.
*   **Duration**: Depends on CPU, typically a few minutes for 100 epochs.

### 2. Running the Application

#### Terminal UI (Default)
Run the application in your terminal to visualize forecasts with a keyboard-driven interface.

```bash
cargo run --release
```

1.  **Input**: Type a valid stock ticker symbol (e.g., `NVDA`, `BTC-USD`, `SPY`) and press `Enter`.
2.  **Inference**: Watch the progress bar as the Diffusion Model iteratively denoises random signals into price forecasts.
3.  **Controls**: `Enter` to fetch, `r` to reset, `q` or `Esc` to quit.

#### Graphical UI (GUI)
Launch the modern windowed interface for interactive charts and mouse support.

```bash
cargo run --release -- --gui
```

*   **Interactive Charts**: Zoom, pan, and hover over data points to see exact dates and prices.
*   **Visual Forecasts**: Clear visualization of P10, P30, P50 (Median), P70, and P90 confidence intervals.

### 3. Backtesting
Validate the model's performance on historical SPY data.

```bash
cargo run --release -- --backtest
```

## Technical Architecture

The application implements a sophisticated Model-View-Update (MVU) architecture:

*   **Frontend**:
    *   **TUI**: `ratatui` for rendering terminal charts, gauges, and text.
    *   **GUI**: `egui` and `eframe` for immediate-mode graphical rendering and interactive plotting.
*   **Backend**: `tokio` for asynchronous runtime and non-blocking UI.
*   **Machine Learning**:
    *   **Framework**: `candle` (Rust-native tensor library).
    *   **Model**: A conditional diffusion model that learns the gradient of the data distribution.
    *   **Sampling**: 50-step Gaussian Diffusion reverse process.
    *   **Persistence**: `safetensors` for secure and fast weight loading.

## Disclaimer

This project is for **educational and research purposes only**. The neural network architecture is implemented faithfully, but in this demo version, it may run with unoptimized or random weights depending on the configuration. Do not use these forecasts for real financial trading.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

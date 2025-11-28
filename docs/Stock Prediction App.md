Coding Specification for `DiffStock-TUI`: A Rust-based Diffusion Forecasting Application

This document specifies the required components, dependencies, and architectural patterns for developing the `DiffStock-TUI` application in Rust. The application is a Terminal User Interface (TUI) tool for probabilistic stock price forecasting using Denoising Diffusion Probabilistic Models (DDPMs).1. Core Project Details

| Field | Value |
| ----- | ----- |
| **Project Name** | `DiffStock-TUI` |
| **Primary Language** | Rust |
| **Architecture** | Model-View-Update (MVU / The Elm Architecture) |
| **Runtime** | `tokio` (Asynchronous Runtime) |
| **Target Platforms** | Linux, macOS, Windows (CLI/Terminal) |
| **Core Function** | Generate a **Cone of Uncertainty** forecast (P10, P50, P90 percentiles) using a diffusion model. |

2\. Key Dependencies (Crates)

The coding agent MUST use the following Rust crates:

| Category | Crate(s) | Purpose |
| ----- | ----- | ----- |
| **TUI** | `ratatui`, `crossterm` | Rendering the user interface and handling terminal events. |
| **Asynchronicity** | `tokio` (with `full` or equivalent features), `tokio::sync::mpsc` | Managing the non-blocking event loop and inter-thread communication. |
| **ML Inference** | `candle-core`, `candle-nn` (or `candle-onnx`) | Loading model weights (`.safetensors` or `.onnx`) and executing the diffusion inference process on CPU/GPU. |
| **Data Fetching** | `reqwest`, `yfinance-rs` (or similar stable financial API client) | Fetching OHLCV (Open, High, Low, Close, Volume) data. |
| **Math/Stats** | `ndarray`, appropriate stats crates | Data manipulation, log-return calculation, Z-score normalization, and percentile calculation. |

3\. Architecture Specification (MVU Pattern)

The application MUST be structured around the MVU pattern using `tokio` channels for message passing.3.1. `Model` (Application State)

Define a main `struct AppState` (the Model) to hold all dynamic application data.

| Field | Type | Description |
| ----- | ----- | ----- |
| `mode` | `enum UIMode` | `Input`, `LoadingData`, `LoadingModel`, `Forecasting`, `Results` |
| `input_ticker` | `String` | The stock symbol currently being queried (e.g., "AAPL"). |
| `historical_data` | `Vec<f64>` | Log-Returns or Prices for the input window (e.g., last 64 days). |
| `forecast_paths` | `ndarray::Array2<f64>` | Matrix of Monte Carlo paths, shape `[N, ForecastHorizon]`. |
| `percentiles` | `struct Percentiles` | Calculated P10, P50 (median), P90 price paths for visualization. |
| `norm_stats` | `struct NormalizationStats` | Rolling mean ($\\mu$) and standard deviation ($\\sigma$) used for Z-Score normalization. CRITICAL for denormalization. |
| `status_message` | `String` | Message displayed in the bottom status bar (e.g., "Fetching data...", "Inference complete in 3.5s"). |

3.2. `AppEvent` (Message Passing)

Define an `enum AppEvent` to represent all possible state-changing actions.

| Event | Data | Trigger/Source |
| ----- | ----- | ----- |
| `Input(KeyEvent)` | `crossterm::event::KeyEvent` | User key press (e.g., 'q', 'Enter'). |
| `Tick` | `()` | Sent periodically by a `tokio::time::interval` for TUI redraw (e.g., 60 FPS). |
| `DataFetched(Result<TickerData, Error>)` | Ticker data with metadata. | Background data fetching task completes. |
| `InferenceReady(Result<ForecastData, Error>)` | Forecast paths and percentiles. | Background diffusion inference task completes. |

3.3. `Update` Loop (Main Asynchronous Logic)

The main `tokio::main` loop MUST:

1. Listen for events via a `tokio::sync::mpsc::UnboundedReceiver<AppEvent>`.  
2. Call a function `fn update(&mut state, event: AppEvent)` to mutate the `state`.  
3. If `AppEvent::Input` is **Enter**, it MUST spawn a new `tokio::task` for **Data Ingestion**.  
4. If `AppEvent::DataFetched` is received, it MUST spawn a new `tokio::task` for **Inference Engine**.

3.4. `View` (TUI Rendering)

A function `fn ui(f: &mut Frame, state: &AppState)` (or equivalent) MUST use `ratatui` to render the UI based **only** on the current `state`.4. Component Specification4.1. Data Ingestion Layer

**Objective:** Fetch raw data and transform it into the required tensor format.

1. **Fetching:** Use `yfinance-rs` to asynchronously fetch a window of historical OHLCV data (e.g., 64 days) for the ticker.  
2. **Transformation (Critical Logic):** The application MUST operate in **Log-Return Space** to handle non-stationarity.  
   * Calculate log-returns: $r\_t \= \\ln(P\_t) \- \\ln(P\_{t-1})$.  
   * **Normalization:** Compute the rolling Z-Score standardization on the log-returns: $z\_t \= \\frac{r\_t \- \\mu}{\\sigma}$. Store $\\mu$ and $\\sigma$.  
   * **Tensor Creation:** Convert the normalized returns (`Vec<f64>`) into a `candle::Tensor` of shape `[1, sequence_length, features]`.

4.2. Diffusion Inference Engine

**Objective:** Execute the TimeGrad-like reverse diffusion process.

1. **Model Loading:** Implement a function to load the pre-trained diffusion model weights (e.g., LSTM encoder \+ Denoiser network) from a `.safetensors` or `.onnx` file using `candle`.  
2. **Sampling Function:** Implement the core reverse DDPM or DDIM sampling loop (preferably **DDIM** for speed optimization \- see Section 5.1).  
3. **Autoregressive Loop:** The model MUST follow the **TimeGrad** logic: After predicting $x\_t$, the new value MUST be fed back into the LSTM encoder to update the hidden state $h\_t$ for the next prediction step.  
4. **Monte Carlo Simulation:** Run the sampling function $N \\geq 100$ times (batch processing if possible) to generate 100 distinct price paths.

4.3. Post-Inference Processing

1. **Denormalization:** The forecast (in Z-Score Log-Returns) MUST be converted back to absolute prices.  
   * Reverse Z-Score: $r\_t' \= z\_t \\cdot \\sigma \+ \\mu$.  
   * Reverse Log-Returns: Prices are generated by integrating the log-returns forward from the last known historical price $P\_{t-1}$: $P\_t \= P\_{t-1} \\cdot e^{r\_t'}$.  
2. **Percentile Calculation:** Calculate the 10th, 50th (median), and 90th percentiles across the $N$ Monte Carlo paths at each time step in the forecast horizon.

4.4. TUI Chart Visualization

The Main Window MUST contain a `ratatui::widgets::Chart` that displays:

1. **Historical Price:** A solid line (e.g., White/Green).  
2. **Median Forecast (P50):** A distinct line (e.g., Yellow) starting from the last historical point.  
3. **Uncertainty Cone:** The area between the **P10** and **P90** paths MUST be filled or shaded (using appropriate ASCII block characters or color) to visually represent the probability density.  
4. **Interactivity:** The chart MUST be zoomable/pannable using keyboard inputs.

5\. Optimization and Reliability Requirements5.1. Performance (Latency)

* The agent MUST prioritize using **DDIM Sampling** over DDPM to reduce the number of required inference steps (e.g., from 1000 to 10-100).  
* The agent SHOULD use `candle`'s **model quantization** features (e.g., GGUF/GGML if supported, or other quantization methods) to optimize CPU inference speed.

5.2. Concurrency and Safety

* The TUI rendering thread MUST **NEVER** be blocked by the data fetching or inference tasks. All I/O and heavy computation must occur in `tokio::spawn` tasks.  
* The application MUST use Rust's `Result<T, E>` for all I/O and ML operations. **Explicit error handling** for API failures, file loading errors, and tensor dimension mismatches is mandatory. No panics (e.g., `unwrap()`, `expect()`) are permitted in core logic.

5.3. TUI Stability

* The TUI MUST be **flicker-free**. This is achieved automatically by `ratatui`'s immediate mode/diff-rendering, but the draw loop MUST be tied to the `AppEvent::Tick` event.


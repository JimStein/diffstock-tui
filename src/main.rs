mod app;
mod config;
mod data;
mod diffusion;
mod inference;
mod models;
mod portfolio;
mod paper_trading;
mod train;
mod tui;
mod ui;
mod gui;
mod webui;

use app::App;
use clap::{Parser, ValueEnum};
use std::io;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Debug, ValueEnum)]
enum GuiRendererChoice {
    Auto,
    Wgpu,
    Glow,
}

#[derive(Parser, Debug)]
#[command(
    author, 
    version, 
    about = "DiffStock-TUI: Probabilistic stock price forecasting with Diffusion Models",
    after_help = "EXAMPLES:
    # Train with default settings
    cargo run --release -- --train

    # Train with custom hyperparameters
    cargo run --release -- --train --epochs 100 --batch-size 32 --learning-rate 0.0005

    # Run backtest
    cargo run --release -- --backtest

    # Launch GUI
    cargo run --release -- --gui"
)]
struct Args {
    /// Train the model on historical data
    #[arg(long)]
    train: bool,

    /// Run backtest on SPY data
    #[arg(long)]
    backtest: bool,

    /// Number of rolling windows for backtest (default: 1 = single-window backtest)
    #[arg(long, default_value_t = 1)]
    backtest_windows: usize,

    /// Rolling step size in days between windows (default: 10)
    #[arg(long, default_value_t = 10)]
    backtest_step_days: usize,

    /// Number of hidden days from the end for the first backtest window (default: 50)
    #[arg(long, default_value_t = 50)]
    backtest_hidden_days: usize,

    /// Launch in GUI mode
    #[arg(long)]
    gui: bool,

    /// Launch in WebUI mode
    #[arg(long)]
    webui: bool,

    /// WebUI server port
    #[arg(long, default_value_t = 8080)]
    webui_port: u16,

    /// GUI renderer backend (auto|wgpu|glow). Useful for RDP compatibility.
    #[arg(long, value_enum, default_value_t = GuiRendererChoice::Wgpu)]
    gui_renderer: GuiRendererChoice,

    /// Enable GUI safe mode for remote desktop (disables vsync/MSAA and hardware acceleration).
    #[arg(long)]
    gui_safe_mode: bool,

    /// Number of epochs for training (default: 200). Ignored if --train is not set.
    #[arg(long)]
    epochs: Option<usize>,

    /// Batch size for training (default: 64). Ignored if --train is not set.
    #[arg(long)]
    batch_size: Option<usize>,

    /// Learning rate for training (default: 0.001). Ignored if --train is not set.
    #[arg(long)]
    learning_rate: Option<f64>,

    /// Early stopping patience — stop training after this many epochs without improvement (default: 20). Ignored if --train is not set.
    #[arg(long)]
    patience: Option<usize>,

    /// Run portfolio optimizer — provide comma-separated symbols (e.g., NVDA,MSFT,AAPL)
    #[arg(long)]
    portfolio: Option<String>,

    /// Use CUDA GPU acceleration (requires --features cuda at compile time)
    #[arg(long)]
    cuda: bool,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    config::init_cpu_parallelism();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("diffstock_tui=info,wgpu_core=error,wgpu_hal=error")
    });
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();
    let args = Args::parse();

    if args.cuda && !cfg!(feature = "cuda") {
        error!(
            "--cuda was requested, but this binary was compiled without CUDA support. Re-run with: cargo run --release --features cuda -- --train --cuda"
        );
        return Ok(());
    }

    if args.train {
        match train::train_model(args.epochs, args.batch_size, args.learning_rate, args.patience, args.cuda).await {
            Ok(_) => info!("Training completed successfully."),
            Err(e) => error!("Training failed: {}", e),
        }
        return Ok(());
    }

    if let Some(ref symbols_str) = args.portfolio {
        let symbols: Vec<String> = symbols_str
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect();
        if symbols.len() < 2 {
            error!("Portfolio optimization requires at least 2 symbols. Example: --portfolio NVDA,MSFT,AAPL,QQQ");
            return Ok(());
        }
        match portfolio::run_portfolio_optimization(&symbols, args.cuda).await {
            Ok(_alloc) => info!("Portfolio optimization completed."),
            Err(e) => error!("Portfolio optimization failed: {}", e),
        }
        return Ok(());
    }

    if args.backtest {
        info!("Fetching SPY data for backtesting...");
        match data::fetch_range("SPY", "5y").await {
            Ok(data) => {
                let data = std::sync::Arc::new(data);
                let result = if args.backtest_windows <= 1 {
                    inference::run_backtest_with_params(
                        data,
                        args.cuda,
                        args.backtest_hidden_days,
                    )
                    .await
                } else {
                    inference::run_backtest_rolling(
                        data,
                        args.cuda,
                        args.backtest_windows,
                        args.backtest_step_days,
                        args.backtest_hidden_days,
                    )
                    .await
                };
                match result {
                    Ok(_) => info!("Backtest completed."),
                    Err(e) => error!("Backtest failed: {}", e),
                }
            }
            Err(e) => error!("Failed to fetch data: {}", e),
        }
        return Ok(());
    }

    if args.gui {
        let mut options = eframe::NativeOptions::default();
        options.renderer = match args.gui_renderer {
            GuiRendererChoice::Auto => eframe::Renderer::Wgpu,
            GuiRendererChoice::Wgpu => eframe::Renderer::Wgpu,
            GuiRendererChoice::Glow => eframe::Renderer::Glow,
        };

        if args.gui_safe_mode {
            options.vsync = false;
            options.multisampling = 0;
            options.depth_buffer = 0;
            options.stencil_buffer = 0;
            options.hardware_acceleration = eframe::HardwareAcceleration::Off;
        }

        info!(
            "Launching GUI with renderer: {:?}, safe_mode={}",
            args.gui_renderer,
            args.gui_safe_mode
        );
        eframe::run_native(
            "DiffStock",
            options,
            Box::new(|_cc| Ok(Box::new(gui::GuiApp::new(App::new(args.cuda))))),
        ).map_err(|e| io::Error::other(e.to_string()))?;
        return Ok(());
    }

    if args.webui {
        match webui::run_webui_server(args.webui_port, args.cuda).await {
            Ok(_) => info!("WebUI exited."),
            Err(e) => error!("WebUI failed: {}", e),
        }
        return Ok(());
    }

    let mut terminal = tui::init()?;
    let mut app = App::new(args.cuda);
    let res = app.run(&mut terminal).await;
    
    tui::restore()?;

    if let Err(e) = res {
        error!("Error: {:?}", e);
    }

    Ok(())
}

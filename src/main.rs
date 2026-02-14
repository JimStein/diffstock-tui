mod app;
mod config;
mod data;
mod diffusion;
mod inference;
mod models;
mod portfolio;
mod train;
mod tui;
mod ui;
mod gui;

use app::App;
use clap::Parser;
use std::io;
use tracing::{info, error};

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

    /// Launch in GUI mode
    #[arg(long)]
    gui: bool,

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
    tracing_subscriber::fmt::init();
    let args = Args::parse();

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
                match inference::run_backtest(data, args.cuda).await {
                    Ok(_) => info!("Backtest completed."),
                    Err(e) => error!("Backtest failed: {}", e),
                }
            }
            Err(e) => error!("Failed to fetch data: {}", e),
        }
        return Ok(());
    }

    if args.gui {
        let options = eframe::NativeOptions::default();
        eframe::run_native(
            "DiffStock",
            options,
            Box::new(|_cc| Ok(Box::new(gui::GuiApp::new(App::new(args.cuda))))),
        ).map_err(|e| io::Error::other(e.to_string()))?;
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

use crate::data::StockData;
use crate::inference::{self, ForecastData};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use std::io;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver};

pub enum AppState {
    Input,
    Loading,
    Forecasting,
    Dashboard,
}

pub struct App {
    pub should_quit: bool,
    pub state: AppState,
    pub input: String,
    pub stock_data: Option<Arc<StockData>>,
    pub forecast: Option<ForecastData>,
    pub error_msg: Option<String>,
    pub progress: f64,
    pub progress_rx: Option<Receiver<f64>>,
    pub result_rx: Option<Receiver<anyhow::Result<ForecastData>>>,
}

impl App {
    pub fn new() -> Self {
        Self {
            should_quit: false,
            state: AppState::Input,
            input: String::new(),
            stock_data: None,
            forecast: None,
            error_msg: None,
            progress: 0.0,
            progress_rx: None,
            result_rx: None,
        }
    }

    pub async fn run(&mut self, terminal: &mut crate::tui::Tui) -> io::Result<()> {
        while !self.should_quit {
            terminal.draw(|f| crate::ui::render(f, self))?;

            // Check for progress updates
            if let Some(rx) = &mut self.progress_rx {
                while let Ok(p) = rx.try_recv() {
                    self.progress = p;
                }
            }

            // Check for result
            if let Some(rx) = &mut self.result_rx {
                if let Ok(res) = rx.try_recv() {
                    match res {
                        Ok(forecast) => {
                            self.forecast = Some(forecast);
                            self.state = AppState::Dashboard;
                        }
                        Err(e) => {
                            self.error_msg = Some(format!("Inference failed: {}", e));
                            self.state = AppState::Dashboard;
                        }
                    }
                    // Cleanup channels
                    self.progress_rx = None;
                    self.result_rx = None;
                }
            }

            if event::poll(std::time::Duration::from_millis(16))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match self.state {
                            AppState::Input => match key.code {
                                KeyCode::Char(c) => self.input.push(c),
                                KeyCode::Backspace => { self.input.pop(); },
                                KeyCode::Enter => {
                                    if !self.input.is_empty() {
                                        self.state = AppState::Loading;
                                        // Trigger fetch
                                        match StockData::fetch(&self.input).await {
                                            Ok(data) => {
                                                let data = Arc::new(data);
                                                self.stock_data = Some(data.clone());
                                                self.state = AppState::Forecasting;
                                                self.error_msg = None;
                                                self.progress = 0.0;
                                                
                                                // Setup channels
                                                let (prog_tx, prog_rx) = mpsc::channel(100);
                                                let (res_tx, res_rx) = mpsc::channel(1);
                                                
                                                self.progress_rx = Some(prog_rx);
                                                self.result_rx = Some(res_rx);

                                                let data_clone = data.clone();
                                                
                                                // Spawn Inference Task
                                                tokio::spawn(async move {
                                                    let res = inference::run_inference(data_clone, 50, 100, Some(prog_tx)).await;
                                                    let _ = res_tx.send(res).await;
                                                });
                                            }
                                            Err(e) => {
                                                self.error_msg = Some(e.to_string());
                                                self.state = AppState::Input;
                                            }
                                        }
                                    }
                                }
                                KeyCode::Esc => self.should_quit = true,
                                _ => {}
                            },
                            AppState::Forecasting => {
                                // Allow cancelling? For now just ignore input or allow quit
                                if key.code == KeyCode::Esc {
                                    self.should_quit = true;
                                }
                            }
                            _ => match key.code {
                                KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
                                KeyCode::Char('r') => {
                                    self.state = AppState::Input;
                                    self.input.clear();
                                    self.stock_data = None;
                                    self.forecast = None;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

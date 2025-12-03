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
    pub data_rx: Option<Receiver<anyhow::Result<StockData>>>,
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
            data_rx: None,
            progress_rx: None,
            result_rx: None,
        }
    }

    pub fn tick(&mut self) {
        // Check for data fetch results
        if let Some(rx) = &mut self.data_rx {
            if let Ok(res) = rx.try_recv() {
                match res {
                    Ok(data) => {
                        let data = Arc::new(data);
                        self.stock_data = Some(data.clone());
                        self.state = AppState::Forecasting;
                        self.error_msg = None;
                        self.progress = 0.0;
                        
                        // Setup channels for inference
                        let (prog_tx, prog_rx) = mpsc::channel(100);
                        let (res_tx, res_rx) = mpsc::channel(1);
                        
                        self.progress_rx = Some(prog_rx);
                        self.result_rx = Some(res_rx);

                        let data_clone = data.clone();
                        
                        // Spawn Inference Task
                        tokio::spawn(async move {
                            let res = inference::run_inference(data_clone, 50, 500, Some(prog_tx)).await;
                            let _ = res_tx.send(res).await;
                        });
                    }
                    Err(e) => {
                        self.error_msg = Some(e.to_string());
                        self.state = AppState::Input;
                    }
                }
                self.data_rx = None;
            }
        }

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
    }

    pub fn trigger_fetch(&mut self) {
        if !self.input.is_empty() {
            self.state = AppState::Loading;
            let symbol = self.input.clone();
            let (tx, rx) = mpsc::channel(1);
            self.data_rx = Some(rx);
            
            tokio::spawn(async move {
                let res = StockData::fetch(&symbol).await;
                let _ = tx.send(res).await;
            });
        }
    }

    pub async fn run(&mut self, terminal: &mut crate::tui::Tui) -> io::Result<()> {
        while !self.should_quit {
            terminal.draw(|f| crate::ui::render(f, self))?;

            self.tick();

            if event::poll(std::time::Duration::from_millis(16))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match self.state {
                            AppState::Input => match key.code {
                                KeyCode::Char(c) => self.input.push(c),
                                KeyCode::Backspace => { self.input.pop(); },
                                KeyCode::Enter => {
                                    self.trigger_fetch();
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

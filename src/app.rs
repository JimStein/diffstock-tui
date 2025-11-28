use crate::data::StockData;
use crate::inference::{self, ForecastData};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use std::io;
use std::sync::Arc;

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
        }
    }

    pub async fn run(&mut self, terminal: &mut crate::tui::Tui) -> io::Result<()> {
        while !self.should_quit {
            terminal.draw(|f| crate::ui::render(f, self))?;

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
                                                
                                                // Run Inference
                                                match inference::run_inference(data, 50, 100).await {
                                                    Ok(forecast) => {
                                                        self.forecast = Some(forecast);
                                                        self.state = AppState::Dashboard;
                                                    }
                                                    Err(e) => {
                                                        self.error_msg = Some(format!("Inference failed: {}", e));
                                                        self.state = AppState::Dashboard; // Show data anyway
                                                    }
                                                }
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

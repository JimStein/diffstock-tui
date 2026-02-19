use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints, PlotUi};
use crate::app::{App, AppState};
use crate::paper_trading::{self, AnalysisRecord, MinutePortfolioSnapshot, PaperCommand, PaperEvent};
use crate::portfolio::{self, PortfolioAllocation};
use crate::train;
use chrono::{DateTime, Duration as ChronoDuration, Local, TimeZone};
use std::path::Path;
use tokio::sync::mpsc;
use std::time::{Duration as StdDuration, Instant};

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// Color Palette 鈥?Professional dark financial terminal
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

const ACCENT_BLUE: egui::Color32 = egui::Color32::from_rgb(59, 130, 246);
const ACCENT_GREEN: egui::Color32 = egui::Color32::from_rgb(34, 197, 94);
const ACCENT_RED: egui::Color32 = egui::Color32::from_rgb(239, 68, 68);
const ACCENT_YELLOW: egui::Color32 = egui::Color32::from_rgb(250, 204, 21);
const ACCENT_ORANGE: egui::Color32 = egui::Color32::from_rgb(251, 146, 60);
const ACCENT_CYAN: egui::Color32 = egui::Color32::from_rgb(34, 211, 238);
const ACCENT_PURPLE: egui::Color32 = egui::Color32::from_rgb(168, 85, 247);

const BG_DARK: egui::Color32 = egui::Color32::from_rgb(15, 15, 20);
const BG_CARD: egui::Color32 = egui::Color32::from_rgb(24, 24, 32);
const BG_ELEVATED: egui::Color32 = egui::Color32::from_rgb(32, 32, 44);
const TEXT_PRIMARY: egui::Color32 = egui::Color32::from_rgb(226, 232, 240);
const TEXT_SECONDARY: egui::Color32 = egui::Color32::from_rgb(148, 163, 184);
const BORDER_SUBTLE: egui::Color32 = egui::Color32::from_rgb(51, 51, 68);

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// GUI Tab State
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[derive(PartialEq, Clone)]
enum GuiTab {
    Forecast,
    Portfolio,
    Train,
}

#[derive(PartialEq)]
enum PortfolioState {
    Idle,
    Running,
    Done,
    Error(String),
}

#[derive(PartialEq)]
enum TrainState {
    Idle,
    Running,
    Finished,
    Error(String),
}

#[derive(PartialEq)]
enum PaperState {
    Idle,
    Running,
    Paused,
    Error(String),
}

#[derive(PartialEq, Clone, Copy)]
enum CurveRangePreset {
    D1,
    D7,
    M1,
    M3,
    Y1,
    Y2,
    Manual,
}

/// A single training epoch log entry.
#[derive(Clone)]
struct TrainLogEntry {
    epoch: usize,
    train_loss: f64,
    val_loss: f64,
}

/// Messages sent from the training thread back to the GUI.
pub enum TrainMessage {
    Epoch { epoch: usize, train_loss: f64, val_loss: f64 },
    Log(String),
    Finished,
    Error(String),
}

pub struct GuiApp {
    app: App,
    active_tab: GuiTab,
    // Portfolio
    portfolio_input: String,
    portfolio_state: PortfolioState,
    portfolio_result: Option<PortfolioAllocation>,
    portfolio_rx: Option<mpsc::Receiver<Result<PortfolioAllocation, String>>>,
    paper_state: PaperState,
    paper_rx: Option<mpsc::Receiver<PaperEvent>>,
    paper_cmd_tx: Option<mpsc::Sender<PaperCommand>>,
    paper_snapshots: Vec<MinutePortfolioSnapshot>,
    paper_last_analysis: Option<AnalysisRecord>,
    paper_log_messages: Vec<String>,
    paper_strategy_file: Option<String>,
    paper_runtime_file: Option<String>,
    paper_target_weights: Option<Vec<(String, f64)>>,
    paper_start_time: Option<Instant>,
    paper_initial_capital_input: String,
    paper_time1_input: String,
    paper_time2_input: String,
    paper_history_path_input: String,
    paper_curve_preset: CurveRangePreset,
    paper_curve_manual_days_input: String,
    paper_force_repaint: bool,
    // Train
    train_state: TrainState,
    train_epochs: String,
    train_batch_size: String,
    train_lr: String,
    train_patience: String,
    train_log: Vec<TrainLogEntry>,
    train_log_messages: Vec<String>,
    train_rx: Option<mpsc::Receiver<TrainMessage>>,
    // Timing / ETA
    train_start_time: Option<Instant>,
    forecast_start_time: Option<Instant>,
    portfolio_start_time: Option<Instant>,
}

impl GuiApp {
    pub fn new(app: App) -> Self {
        Self {
            app,
            active_tab: GuiTab::Forecast,
            portfolio_input: String::from("NVDA,MSFT,URA,IAU,COPX,ETN,TLT"),
            portfolio_state: PortfolioState::Idle,
            portfolio_result: None,
            portfolio_rx: None,
            paper_state: PaperState::Idle,
            paper_rx: None,
            paper_cmd_tx: None,
            paper_snapshots: Vec::new(),
            paper_last_analysis: None,
            paper_log_messages: Vec::new(),
            paper_strategy_file: None,
            paper_runtime_file: None,
            paper_target_weights: None,
            paper_start_time: None,
            paper_initial_capital_input: String::from("80000"),
            paper_time1_input: String::from("09:30"),
            paper_time2_input: String::from("15:00"),
            paper_history_path_input: String::new(),
            paper_curve_preset: CurveRangePreset::D7,
            paper_curve_manual_days_input: String::from("30"),
            paper_force_repaint: false,
            train_state: TrainState::Idle,
            train_epochs: String::from("200"),
            train_batch_size: String::from("64"),
            train_lr: String::from("0.001"),
            train_patience: String::from("20"),
            train_log: Vec::new(),
            train_log_messages: Vec::new(),
            train_rx: None,
            train_start_time: None,
            forecast_start_time: None,
            portfolio_start_time: None,
        }
    }

    fn apply_theme(ctx: &egui::Context) {
        let mut style = (*ctx.style()).clone();

        // Rounded, modern feel
        style.visuals.window_rounding = egui::Rounding::same(8.0);
        style.visuals.widgets.noninteractive.rounding = egui::Rounding::same(6.0);
        style.visuals.widgets.inactive.rounding = egui::Rounding::same(6.0);
        style.visuals.widgets.active.rounding = egui::Rounding::same(6.0);
        style.visuals.widgets.hovered.rounding = egui::Rounding::same(6.0);

        // Dark background
        style.visuals.dark_mode = true;
        style.visuals.panel_fill = BG_DARK;
        style.visuals.window_fill = BG_CARD;
        style.visuals.faint_bg_color = BG_ELEVATED;

        // Widget styling
        style.visuals.widgets.noninteractive.bg_fill = BG_CARD;
        style.visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, TEXT_SECONDARY);
        style.visuals.widgets.inactive.bg_fill = BG_ELEVATED;
        style.visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, TEXT_PRIMARY);
        style.visuals.widgets.hovered.bg_fill = egui::Color32::from_rgb(45, 45, 60);
        style.visuals.widgets.hovered.fg_stroke = egui::Stroke::new(1.0, egui::Color32::WHITE);
        style.visuals.widgets.active.bg_fill = ACCENT_BLUE;
        style.visuals.widgets.active.fg_stroke = egui::Stroke::new(1.0, egui::Color32::WHITE);

        style.visuals.selection.bg_fill = ACCENT_BLUE.linear_multiply(0.4);
        style.visuals.selection.stroke = egui::Stroke::new(1.0, ACCENT_BLUE);

        style.spacing.item_spacing = egui::vec2(8.0, 6.0);

        ctx.set_style(style);
    }
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        Self::apply_theme(ctx);
        self.app.tick();

        // Check portfolio channel
        if let Some(rx) = &mut self.portfolio_rx {
            if let Ok(res) = rx.try_recv() {
                match res {
                    Ok(alloc) => {
                        self.portfolio_result = Some(alloc);
                        self.portfolio_state = PortfolioState::Done;
                    }
                    Err(e) => {
                        self.portfolio_state = PortfolioState::Error(e);
                    }
                }
                self.portfolio_rx = None;
            }
        }

        // Check training channel
        if let Some(rx) = &mut self.train_rx {
            loop {
                match rx.try_recv() {
                    Ok(TrainMessage::Epoch { epoch, train_loss, val_loss }) => {
                        self.train_log.push(TrainLogEntry { epoch, train_loss, val_loss });
                    }
                    Ok(TrainMessage::Log(msg)) => {
                        self.train_log_messages.push(msg);
                    }
                    Ok(TrainMessage::Finished) => {
                        self.train_state = TrainState::Finished;
                        self.train_rx = None;
                        break;
                    }
                    Ok(TrainMessage::Error(e)) => {
                        self.train_state = TrainState::Error(e);
                        self.train_rx = None;
                        break;
                    }
                    Err(_) => break,
                }
            }
        }

        // Check paper trading channel
        if let Some(rx) = &mut self.paper_rx {
            loop {
                match rx.try_recv() {
                    Ok(PaperEvent::Started {
                        strategy_file,
                        runtime_file,
                    }) => {
                        self.paper_strategy_file = Some(strategy_file.clone());
                        self.paper_runtime_file = Some(runtime_file.clone());
                        self.paper_log_messages
                            .push(format!("Paper trading started. strategy={}, runtime={}", strategy_file, runtime_file));
                    }
                    Ok(PaperEvent::Info(message)) => {
                        self.paper_log_messages.push(message);
                    }
                    Ok(PaperEvent::Warning(message)) => {
                        self.paper_log_messages.push(format!("Warning: {}", message));
                    }
                    Ok(PaperEvent::Analysis(analysis)) => {
                        self.paper_last_analysis = Some(analysis.clone());
                        self.paper_log_messages.push(format!(
                            "Analysis {} trades={} value_after=${:.2}",
                            analysis.timestamp,
                            analysis.trades.len(),
                            analysis.portfolio_value_after
                        ));
                    }
                    Ok(PaperEvent::Minute(snapshot)) => {
                        self.paper_snapshots.push(snapshot);
                        if self.paper_snapshots.len() > 5000 {
                            self.paper_snapshots.remove(0);
                        }
                    }
                    Ok(PaperEvent::Error(message)) => {
                        self.paper_state = PaperState::Error(message.clone());
                        self.paper_log_messages.push(format!("Error: {}", message));
                    }
                    Err(_) => break,
                }
            }
        }

        if self.paper_state == PaperState::Running || self.paper_state == PaperState::Paused {
            ctx.request_repaint();
        }

        if self.paper_force_repaint {
            ctx.request_repaint();
            self.paper_force_repaint = false;
        }

        ctx.request_repaint_after(StdDuration::from_secs(1));

        // 鈹€鈹€ Top Bar 鈹€鈹€
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.label(egui::RichText::new("DiffStock")
                    .size(18.0)
                    .strong()
                    .color(ACCENT_BLUE));
                ui.add_space(4.0);
                ui.label(egui::RichText::new("Probabilistic Forecasting Engine")
                    .size(11.0)
                    .color(TEXT_SECONDARY));

                let now_text = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                let (paper_status_text, paper_status_color) = match &self.paper_state {
                    PaperState::Running => ("System: RUNNING", ACCENT_GREEN),
                    PaperState::Paused => ("System: PAUSED", ACCENT_ORANGE),
                    PaperState::Error(_) => ("System: ERROR", ACCENT_RED),
                    PaperState::Idle => ("System: IDLE", TEXT_SECONDARY),
                };

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    small_chip(ui, &format!("Time {}", now_text), ACCENT_CYAN);
                    small_chip(ui, paper_status_text, paper_status_color);
                    ui.add_space(8.0);

                    let mode_label = match self.active_tab {
                        GuiTab::Forecast => "Forecast",
                        GuiTab::Portfolio => "Portfolio",
                        GuiTab::Train => "Train",
                    };
                    small_chip(ui, mode_label, ACCENT_PURPLE);
                    small_chip(ui, if self.app.use_cuda { "CUDA" } else { "CPU" }, ACCENT_CYAN);

                    ui.add_space(8.0);
                    let train_btn = ui.selectable_label(
                        self.active_tab == GuiTab::Train,
                        egui::RichText::new("Train").size(13.0),
                    );
                    let portfolio_btn = ui.selectable_label(
                        self.active_tab == GuiTab::Portfolio,
                        egui::RichText::new("Portfolio").size(13.0),
                    );
                    let forecast_btn = ui.selectable_label(
                        self.active_tab == GuiTab::Forecast,
                        egui::RichText::new("Forecast").size(13.0),
                    );
                    if forecast_btn.clicked() {
                        self.active_tab = GuiTab::Forecast;
                    }
                    if portfolio_btn.clicked() {
                        self.active_tab = GuiTab::Portfolio;
                    }
                    if train_btn.clicked() {
                        self.active_tab = GuiTab::Train;
                    }
                });
            });
            ui.add_space(4.0);
        });

        // 鈹€鈹€ Main Content 鈹€鈹€
        egui::CentralPanel::default().show(ctx, |ui| {
            match self.active_tab {
                GuiTab::Forecast => self.render_forecast_tab(ui, ctx),
                GuiTab::Portfolio => self.render_portfolio_tab(ui, ctx),
                GuiTab::Train => self.render_train_tab(ui, ctx),
            }
        });
    }
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// Forecast Tab
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

impl GuiApp {
    fn render_forecast_tab(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        match self.app.state {
            AppState::Input => {
                self.forecast_start_time = None;
                self.render_input_screen(ui);
            }
            AppState::Loading => {
                self.render_centered_status(ui, "Fetching Market Data...", None, None);
                ctx.request_repaint();
            }
            AppState::Forecasting => {
                if self.forecast_start_time.is_none() {
                    self.forecast_start_time = Some(Instant::now());
                }
                let eta_text = self.compute_eta_text(self.forecast_start_time, self.app.progress);
                self.render_centered_status(
                    ui,
                    "Running Diffusion Inference...",
                    Some(self.app.progress as f32),
                    eta_text.as_deref(),
                );
                ctx.request_repaint();
            }
            AppState::Dashboard => self.render_dashboard(ui),
        }
    }

    fn render_input_screen(&mut self, ui: &mut egui::Ui) {
        let available = ui.available_size();
        ui.vertical_centered(|ui| {
            ui.add_space(available.y * 0.2);

            ui.add_space(8.0);
            ui.label(egui::RichText::new("DiffStock")
                .size(32.0)
                .strong()
                .color(ACCENT_BLUE));
            ui.label(egui::RichText::new("AI-Powered Stock Forecasting")
                .size(14.0)
                .color(TEXT_SECONDARY));
            ui.add_space(30.0);

            // Input card
            egui::Frame::none()
                .fill(BG_CARD)
                .rounding(egui::Rounding::same(12.0))
                .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
                .inner_margin(egui::Margin::same(24.0))
                .show(ui, |ui| {
                    ui.set_width(340.0);

                    ui.label(egui::RichText::new("Enter Stock Symbol")
                        .size(14.0)
                        .color(TEXT_SECONDARY));
                    ui.add_space(8.0);

                    let input_response = ui.add(
                        egui::TextEdit::singleline(&mut self.app.input)
                            .desired_width(300.0)
                            .font(egui::TextStyle::Heading)
                            .hint_text("e.g. NVDA, AAPL, SPY...")
                    );

                    if input_response.lost_focus()
                        && ui.input(|i| i.key_pressed(egui::Key::Enter))
                    {
                        self.app.trigger_fetch();
                    }

                    ui.add_space(12.0);

                    let btn = ui.add_sized(
                        [300.0, 40.0],
                        egui::Button::new(
                            egui::RichText::new("Predict")
                                .size(15.0)
                                .strong()
                                .color(egui::Color32::WHITE),
                        )
                        .fill(ACCENT_BLUE)
                        .rounding(egui::Rounding::same(8.0)),
                    );
                    if btn.clicked() {
                        self.app.trigger_fetch();
                    }

                    if let Some(err) = &self.app.error_msg {
                        ui.add_space(12.0);
                        egui::Frame::none()
                            .fill(egui::Color32::from_rgba_premultiplied(239, 68, 68, 25))
                            .rounding(egui::Rounding::same(6.0))
                            .inner_margin(egui::Margin::same(8.0))
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new(format!("Error: {}", err))
                                    .color(ACCENT_RED)
                                    .size(12.0));
                            });
                    }
                });

            ui.add_space(20.0);
            ui.label(egui::RichText::new("Powered by TimeGrad Diffusion Model  |  500 Monte Carlo Paths")
                .size(11.0)
                .color(TEXT_SECONDARY));
        });
    }

    fn render_centered_status(
        &self,
        ui: &mut egui::Ui,
        message: &str,
        progress: Option<f32>,
        time_info: Option<&str>,
    ) {
        let available = ui.available_size();
        ui.vertical_centered(|ui| {
            ui.add_space(available.y * 0.3);

            ui.add_space(12.0);
            ui.label(egui::RichText::new(message).size(16.0).color(TEXT_PRIMARY));

            if let Some(p) = progress {
                ui.add_space(16.0);
                ui.add_sized(
                    [300.0, 8.0],
                    egui::ProgressBar::new(p)
                        .animate(true)
                        .fill(ACCENT_BLUE),
                );
                ui.add_space(6.0);
                ui.label(
                    egui::RichText::new(format!("{:.0}%", p * 100.0))
                        .size(12.0)
                        .color(TEXT_SECONDARY),
                );
            } else {
                ui.add_space(12.0);
                ui.spinner();
            }

            if let Some(info) = time_info {
                ui.add_space(8.0);
                ui.label(
                    egui::RichText::new(info)
                        .size(12.0)
                        .color(ACCENT_YELLOW),
                );
            }
        });
    }

    fn compute_eta_text(&self, start_time: Option<Instant>, progress: f64) -> Option<String> {
        let start = start_time?;
        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs();
        let elapsed_str = format_duration(elapsed_secs);
        if progress > 0.01 && progress < 1.0 {
            let total_estimated = elapsed_secs as f64 / progress;
            let remaining = (total_estimated - elapsed_secs as f64).max(0.0) as u64;
            let eta_str = format_duration(remaining);
            Some(format!("Elapsed: {}  |  ETA: ~{}", elapsed_str, eta_str))
        } else {
            Some(format!("Elapsed: {}", elapsed_str))
        }
    }

    fn render_dashboard(&mut self, ui: &mut egui::Ui) {
        // 鈹€鈹€ Header Bar 鈹€鈹€
        egui::Frame::none()
            .fill(BG_CARD)
            .inner_margin(egui::Margin::symmetric(12.0, 8.0))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    let back_btn = ui.add(
                        egui::Button::new(egui::RichText::new("<- Back").size(12.0))
                            .rounding(egui::Rounding::same(6.0)),
                    );
                    if back_btn.clicked() {
                        self.app.state = AppState::Input;
                        self.app.input.clear();
                        self.app.stock_data = None;
                        self.app.forecast = None;
                    }

                    if let Some(data) = &self.app.stock_data {
                        ui.add_space(12.0);
                        ui.label(egui::RichText::new(&data.symbol)
                            .size(20.0)
                            .strong()
                            .color(ACCENT_CYAN));

                        if let Some(last) = data.history.last() {
                            ui.add_space(8.0);
                            ui.label(egui::RichText::new(format!("${:.2}", last.close))
                                .size(20.0)
                                .strong()
                                .color(TEXT_PRIMARY));

                            // Calculate change
                            if data.history.len() >= 2 {
                                let prev = data.history[data.history.len() - 2].close;
                                let change = last.close - prev;
                                let pct = change / prev * 100.0;
                                let (color, arrow) = if change >= 0.0 {
                                    (ACCENT_GREEN, "+")
                                } else {
                                    (ACCENT_RED, "")
                                };
                                ui.label(egui::RichText::new(
                                    format!("{}{:.2} ({:+.2}%)", arrow, change, pct))
                                    .size(14.0)
                                    .color(color));
                            }

                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                ui.label(egui::RichText::new(
                                    format!("{}  |  Vol: {:.0}", last.date.format("%Y-%m-%d"), last.volume))
                                    .size(11.0)
                                    .color(TEXT_SECONDARY));
                            });
                        }
                    }
                });
            });

        ui.add_space(4.0);

        // 鈹€鈹€ Main area: Chart + Side Panel 鈹€鈹€
        let available = ui.available_size();
        let side_panel_width = 260.0_f32.min(available.x * 0.25);

        ui.horizontal(|ui| {
            // Chart
            ui.vertical(|ui| {
                ui.set_width(available.x - side_panel_width - 16.0);
                self.render_chart(ui);
            });

            // Side Panel
            ui.vertical(|ui| {
                ui.set_width(side_panel_width);
                self.render_side_panel(ui);
            });
        });
    }

    fn render_chart(&self, ui: &mut egui::Ui) {
        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(8.0))
            .show(ui, |ui| {
                let plot = Plot::new("stock_chart")
                    .legend(egui_plot::Legend::default().position(egui_plot::Corner::LeftTop))
                    .x_axis_formatter(|x, _range| {
                        chrono::Utc.timestamp_opt(x.value as i64, 0)
                            .map(|dt| dt.format("%b %d").to_string())
                            .single()
                            .unwrap_or_default()
                    })
                    .label_formatter(|name, value| {
                        let date = chrono::Utc.timestamp_opt(value.x as i64, 0)
                            .map(|dt| dt.format("%Y-%m-%d").to_string())
                            .single()
                            .unwrap_or_default();
                        format!("{}\nDate: {}\nPrice: ${:.2}", name, date, value.y)
                    })
                    .coordinates_formatter(
                        egui_plot::Corner::LeftBottom,
                        egui_plot::CoordinatesFormatter::new(
                            |point: &egui_plot::PlotPoint, _bounds: &egui_plot::PlotBounds| {
                                let date = chrono::Utc.timestamp_opt(point.x as i64, 0)
                                    .map(|dt| dt.format("%Y-%m-%d").to_string())
                                    .single()
                                    .unwrap_or_default();
                                format!("{}  ${:.2}", date, point.y)
                            },
                        ),
                    )
                    .view_aspect(2.5)
                    .allow_drag(true)
                    .allow_zoom(true);

                plot.show(ui, |plot_ui| {
                    self.draw_chart_data(plot_ui);
                });
            });
    }

    fn draw_chart_data(&self, plot_ui: &mut PlotUi) {
        if let Some(data) = &self.app.stock_data {
            let points: PlotPoints = data.history.iter()
                .map(|c| [c.date.timestamp() as f64, c.close])
                .collect();
            plot_ui.line(
                Line::new(points)
                    .name("Price")
                    .color(ACCENT_CYAN)
                    .width(1.8),
            );
        }

        if let Some(forecast) = &self.app.forecast {
            // P10-P90 outer cone
            let p10: PlotPoints = forecast.p10.iter().map(|&(x, y)| [x, y]).collect();
            let p90: PlotPoints = forecast.p90.iter().map(|&(x, y)| [x, y]).collect();
            plot_ui.line(Line::new(p90).name("P90 (Bull)")
                .color(egui::Color32::from_rgba_premultiplied(34, 197, 94, 100))
                .style(egui_plot::LineStyle::Dashed { length: 8.0 })
                .width(1.2));
            plot_ui.line(Line::new(p10).name("P10 (Bear)")
                .color(egui::Color32::from_rgba_premultiplied(239, 68, 68, 100))
                .style(egui_plot::LineStyle::Dashed { length: 8.0 })
                .width(1.2));

            // P30-P70 inner cone
            let p30: PlotPoints = forecast.p30.iter().map(|&(x, y)| [x, y]).collect();
            let p70: PlotPoints = forecast.p70.iter().map(|&(x, y)| [x, y]).collect();
            plot_ui.line(Line::new(p70).name("P70")
                .color(egui::Color32::from_rgba_premultiplied(59, 130, 246, 130))
                .style(egui_plot::LineStyle::Dashed { length: 5.0 })
                .width(1.0));
            plot_ui.line(Line::new(p30).name("P30")
                .color(egui::Color32::from_rgba_premultiplied(251, 146, 60, 130))
                .style(egui_plot::LineStyle::Dashed { length: 5.0 })
                .width(1.0));

            // P50 Median 鈥?bold
            let p50: PlotPoints = forecast.p50.iter().map(|&(x, y)| [x, y]).collect();
            plot_ui.line(Line::new(p50).name("Median (P50)")
                .color(ACCENT_YELLOW)
                .width(2.5));

            // Connect last price to first forecast point
            if let (Some(data), Some(first_p50)) = (&self.app.stock_data, forecast.p50.first()) {
                if let Some(last) = data.history.last() {
                    let bridge: PlotPoints = PlotPoints::new(vec![
                        [last.date.timestamp() as f64, last.close],
                        [first_p50.0, first_p50.1],
                    ]);
                    plot_ui.line(Line::new(bridge)
                        .color(egui::Color32::from_rgba_premultiplied(250, 204, 21, 80))
                        .style(egui_plot::LineStyle::Dotted { spacing: 4.0 })
                        .width(1.5));
                }
            }
        }
    }

    fn render_side_panel(&mut self, ui: &mut egui::Ui) {
        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(12.0))
            .show(ui, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    // 鈹€鈹€ Analysis Section 鈹€鈹€
                    if let Some(data) = &self.app.stock_data {
                        let analysis = data.analyze();

                        section_header(ui, "Technical Levels");
                        stat_row(ui, "Current", &format!("${:.2}", analysis.current_price), TEXT_PRIMARY);
                        stat_row(ui, "Resistance", &format!("${:.2}", analysis.resistance), ACCENT_RED);
                        stat_row(ui, "Support", &format!("${:.2}", analysis.support), ACCENT_GREEN);
                        stat_row(ui, "Pivot", &format!("${:.2}", analysis.pivot), ACCENT_YELLOW);

                        ui.add_space(8.0);
                        ui.add(egui::Separator::default().spacing(4.0));
                        ui.add_space(8.0);
                    }

                    // 鈹€鈹€ Forecast Section 鈹€鈹€
                    if let Some(forecast) = &self.app.forecast {
                        section_header(ui, "Forecast Targets");

                        if let Some(data) = &self.app.stock_data {
                            let current = data.history.last().map(|c| c.close).unwrap_or(0.0);

                            let targets = [
                                ("P90 (Bull)", forecast.p90.last(), ACCENT_GREEN),
                                ("P70", forecast.p70.last(), egui::Color32::from_rgb(134, 239, 172)),
                                ("P50 (Med)", forecast.p50.last(), ACCENT_YELLOW),
                                ("P30", forecast.p30.last(), ACCENT_ORANGE),
                                ("P10 (Bear)", forecast.p10.last(), ACCENT_RED),
                            ];

                            for (label, val, color) in &targets {
                                if let Some((_, price)) = val {
                                    let pct = (price / current - 1.0) * 100.0;
                                    let arrow = if pct >= 0.0 { "+" } else { "" };
                                    ui.horizontal(|ui| {
                                        ui.label(egui::RichText::new(*label)
                                            .size(11.0)
                                            .color(TEXT_SECONDARY));
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            ui.label(egui::RichText::new(
                                                format!("{}{:.1}%", arrow, pct))
                                                .size(11.0)
                                                .color(*color));
                                            ui.label(egui::RichText::new(format!("${:.2}", price))
                                                .size(12.0)
                                                .strong()
                                                .color(*color));
                                        });
                                    });
                                    ui.add_space(2.0);
                                }
                            }

                            // Forecast date
                            if let Some((ts, _)) = forecast.p50.last() {
                                ui.add_space(6.0);
                                let date = chrono::Utc.timestamp_opt(*ts as i64, 0)
                                    .map(|dt| dt.format("%Y-%m-%d").to_string())
                                    .single()
                                    .unwrap_or_default();
                                ui.label(egui::RichText::new(format!("Target Date: {}", date))
                                    .size(10.0)
                                    .color(TEXT_SECONDARY));
                            }
                        }

                        ui.add_space(8.0);
                        ui.add(egui::Separator::default().spacing(4.0));
                        ui.add_space(8.0);

                        // 鈹€鈹€ Confidence Meter 鈹€鈹€
                        section_header(ui, "Forecast Spread");
                        if let (Some((_, p10)), Some((_, p90))) = (forecast.p10.last(), forecast.p90.last()) {
                            let spread = ((p90 / p10) - 1.0) * 100.0;
                            let confidence = if spread < 10.0 {
                                ("High Confidence", ACCENT_GREEN)
                            } else if spread < 25.0 {
                                ("Moderate", ACCENT_YELLOW)
                            } else {
                                ("Wide Dispersion", ACCENT_RED)
                            };
                            ui.horizontal(|ui| {
                                ui.label(egui::RichText::new(format!("P90/P10 Spread: {:.1}%", spread))
                                    .size(11.0)
                                    .color(TEXT_SECONDARY));
                            });
                            ui.label(egui::RichText::new(confidence.0)
                                .size(12.0)
                                .strong()
                                .color(confidence.1));
                        }
                    }

                    if let Some(err) = &self.app.error_msg {
                        ui.add_space(12.0);
                        ui.label(egui::RichText::new(format!("Error: {}", err))
                            .color(ACCENT_RED)
                            .size(11.0));
                    }

                    if let Some(data) = &self.app.stock_data {
                        ui.add_space(10.0);
                        ui.add(egui::Separator::default().spacing(4.0));
                        ui.add_space(8.0);
                        section_header(ui, "Single-Stock Simulation");
                        ui.label(egui::RichText::new("Run paper trading directly from this stock forecast (100% allocation).")
                            .size(10.0)
                            .color(TEXT_SECONDARY));

                        ui.horizontal(|ui| {
                            ui.label(egui::RichText::new("Capital").size(10.0).color(TEXT_SECONDARY));
                            ui.add(egui::TextEdit::singleline(&mut self.paper_initial_capital_input).desired_width(80.0));
                            ui.label(egui::RichText::new("T1").size(10.0).color(TEXT_SECONDARY));
                            ui.add(egui::TextEdit::singleline(&mut self.paper_time1_input).desired_width(56.0));
                            ui.label(egui::RichText::new("T2").size(10.0).color(TEXT_SECONDARY));
                            ui.add(egui::TextEdit::singleline(&mut self.paper_time2_input).desired_width(56.0));
                        });

                        let run_btn = ui.add_enabled(
                            self.paper_state != PaperState::Running,
                            egui::Button::new(
                                egui::RichText::new("Start Single-Stock Simulation")
                                    .size(11.0)
                                    .strong()
                                    .color(egui::Color32::WHITE),
                            )
                            .fill(if self.paper_state == PaperState::Running {
                                BG_ELEVATED
                            } else {
                                ACCENT_GREEN
                            })
                            .rounding(egui::Rounding::same(6.0)),
                        );

                        if run_btn.clicked() {
                            self.start_single_stock_paper_trading(data.symbol.clone());
                        }
                    }
                });
            });
    }
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// Portfolio Tab
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

impl GuiApp {
    fn render_portfolio_tab(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        if self.portfolio_state == PortfolioState::Running {
            let elapsed_text = self.portfolio_start_time.map(|t| {
                format!("Elapsed: {}", format_duration(t.elapsed().as_secs()))
            });
            self.render_centered_status(
                ui,
                "Optimizing Portfolio...\nForecasting all assets via Diffusion Model",
                None,
                elapsed_text.as_deref(),
            );
            ctx.request_repaint();
            return;
        }

        // Split: top input bar + results below
        ui.add_space(8.0);

        // 鈹€鈹€ Input Section 鈹€鈹€
        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(16.0))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Portfolio Symbols")
                        .size(14.0)
                        .strong()
                        .color(TEXT_PRIMARY));
                    ui.add_space(8.0);
                    ui.add(
                        egui::TextEdit::singleline(&mut self.portfolio_input)
                            .desired_width(400.0)
                            .hint_text("NVDA,MSFT,AAPL,GOOGL...")
                    );
                    ui.add_space(8.0);

                    let run_btn = ui.add(
                        egui::Button::new(
                            egui::RichText::new("Optimize")
                                .size(13.0)
                                .strong()
                                .color(egui::Color32::WHITE),
                        )
                        .fill(ACCENT_PURPLE)
                        .rounding(egui::Rounding::same(6.0)),
                    );

                    if run_btn.clicked() && self.portfolio_state != PortfolioState::Running {
                        let symbols: Vec<String> = self.portfolio_input
                            .split(',')
                            .map(|s| s.trim().to_uppercase())
                            .filter(|s| !s.is_empty())
                            .collect();

                        if symbols.len() < 2 {
                            self.portfolio_state = PortfolioState::Error(
                                "Need at least 2 symbols".to_string(),
                            );
                        } else {
                            self.portfolio_state = PortfolioState::Running;
                            self.portfolio_start_time = Some(Instant::now());
                            let (tx, rx) = mpsc::channel(1);
                            self.portfolio_rx = Some(rx);
                            let use_cuda = self.app.use_cuda;

                            tokio::spawn(async move {
                                let result = portfolio::run_portfolio_optimization(&symbols, use_cuda).await;
                                let _ = tx.send(result.map_err(|e| e.to_string())).await;
                            });
                        }
                    }
                });

                if let PortfolioState::Error(e) = &self.portfolio_state {
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new(format!("Error: {}", e))
                        .color(ACCENT_RED)
                        .size(12.0));
                }
            });

        ui.add_space(8.0);

        // 鈹€鈹€ Results 鈹€鈹€
        if let Some(alloc) = self.portfolio_result.clone() {
            self.render_portfolio_results(ui, &alloc);
        } else if self.portfolio_state == PortfolioState::Idle {
            // Empty state
            let available = ui.available_size();
            ui.vertical_centered(|ui| {
                ui.add_space(available.y * 0.2);
                ui.add_space(8.0);
                ui.label(egui::RichText::new("Portfolio Optimizer")
                    .size(22.0)
                    .strong()
                    .color(TEXT_PRIMARY));
                ui.add_space(8.0);
                ui.label(egui::RichText::new(
                    "Enter comma-separated symbols above and click Optimize.\n\
                     The diffusion model will forecast each asset, estimate covariance,\n\
                     and find optimal weights via Mean-Variance + CVaR optimization.")
                    .size(13.0)
                    .color(TEXT_SECONDARY));
            });
        }
    }

    fn render_portfolio_results(&mut self, ui: &mut egui::Ui, alloc: &PortfolioAllocation) {
        egui::ScrollArea::vertical().show(ui, |ui| {
            // 鈹€鈹€ Summary Cards Row 鈹€鈹€
            ui.horizontal(|ui| {
                summary_card(ui, "Annual Return",
                    &format!("{:+.1}%", alloc.expected_annual_return * 100.0),
                    if alloc.expected_annual_return > 0.0 { ACCENT_GREEN } else { ACCENT_RED });
                summary_card(ui, "Annual Vol",
                    &format!("{:.1}%", alloc.expected_annual_vol * 100.0),
                    ACCENT_YELLOW);
                summary_card(ui, "Sharpe Ratio",
                    &format!("{:.2}", alloc.sharpe_ratio),
                    if alloc.sharpe_ratio > 1.0 { ACCENT_GREEN } else { ACCENT_ORANGE });
                summary_card(ui, "CVaR (95%)",
                    &format!("{:.1}%", alloc.cvar_95 * 100.0),
                    ACCENT_RED);
                summary_card(ui, "Leverage",
                    &format!("{:.2}x", alloc.leverage),
                    ACCENT_CYAN);
            });

            ui.add_space(8.0);

            egui::Frame::none()
                .fill(BG_CARD)
                .rounding(egui::Rounding::same(8.0))
                .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
                .inner_margin(egui::Margin::same(12.0))
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        section_header(ui, "Paper Trading Simulator");
                        ui.label(egui::RichText::new("Initial Capital: $80,000 | Fee: 0.05% per trade | Analysis: start immediately + every 12h")
                            .size(10.0)
                            .color(TEXT_SECONDARY));
                    });

                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("Capital").size(10.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.paper_initial_capital_input).desired_width(90.0));
                        ui.label(egui::RichText::new("Time1").size(10.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.paper_time1_input).desired_width(64.0));
                        ui.label(egui::RichText::new("Time2").size(10.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.paper_time2_input).desired_width(64.0));

                        let start_btn = ui.add_enabled(
                            self.paper_state != PaperState::Running,
                            egui::Button::new(
                                egui::RichText::new(if self.paper_state == PaperState::Running {
                                    "Simulation Running"
                                } else {
                                    "Start Simulation"
                                })
                                .size(12.0)
                                .strong()
                                .color(egui::Color32::WHITE),
                            )
                            .fill(if self.paper_state == PaperState::Running {
                                BG_ELEVATED
                            } else {
                                ACCENT_GREEN
                            })
                            .rounding(egui::Rounding::same(6.0)),
                        );

                        if start_btn.clicked() {
                            self.start_paper_trading_from_weights(alloc.weights.clone());
                        }

                        let stop_btn = ui.add_enabled(
                            self.paper_state == PaperState::Running,
                            egui::Button::new(
                                egui::RichText::new("Stop")
                                    .size(11.0)
                                    .strong()
                                    .color(egui::Color32::WHITE),
                            )
                            .fill(ACCENT_RED)
                            .rounding(egui::Rounding::same(6.0)),
                        );
                        if stop_btn.clicked() {
                            self.pause_paper_trading();
                        }

                        let resume_btn = ui.add_enabled(
                            self.paper_state == PaperState::Paused,
                            egui::Button::new(
                                egui::RichText::new("Resume")
                                    .size(11.0)
                                    .strong()
                                    .color(egui::Color32::WHITE),
                            )
                            .fill(ACCENT_GREEN)
                            .rounding(egui::Rounding::same(6.0)),
                        );
                        if resume_btn.clicked() {
                            self.resume_paper_trading();
                        }

                        let status_text = match &self.paper_state {
                            PaperState::Idle => "Idle",
                            PaperState::Running => "Running",
                            PaperState::Paused => "Paused",
                            PaperState::Error(_) => "Error",
                        };
                        let status_color = match &self.paper_state {
                            PaperState::Idle => TEXT_SECONDARY,
                            PaperState::Running => ACCENT_YELLOW,
                            PaperState::Paused => ACCENT_ORANGE,
                            PaperState::Error(_) => ACCENT_RED,
                        };
                        ui.label(egui::RichText::new(format!("Status: {}", status_text))
                            .size(11.0)
                            .color(status_color));
                    });

                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("History JSONL").size(10.0).color(TEXT_SECONDARY));
                        ui.add(
                            egui::TextEdit::singleline(&mut self.paper_history_path_input)
                                .desired_width(280.0)
                                .hint_text("empty = auto latest log/paper_runtime_*.jsonl"),
                        );
                        let load_btn = ui.button("Load History");
                        if load_btn.clicked() {
                            self.load_paper_history();
                        }
                    });

                    ui.label(egui::RichText::new("Times are local machine time in HH:MM format (daily schedule).")
                        .size(10.0)
                        .color(TEXT_SECONDARY));

                    if let Some(path) = &self.paper_strategy_file {
                        ui.label(egui::RichText::new(format!("Strategy JSON: {}", path))
                            .size(10.0)
                            .color(TEXT_SECONDARY));
                    }
                    if let Some(path) = &self.paper_runtime_file {
                        ui.label(egui::RichText::new(format!("Runtime JSONL: {}", path))
                            .size(10.0)
                            .color(TEXT_SECONDARY));
                    }

                    if let PaperState::Error(message) = &self.paper_state {
                        ui.label(egui::RichText::new(format!("Error: {}", message))
                            .size(10.0)
                            .color(ACCENT_RED));
                    }
                });

            ui.add_space(8.0);

            // 鈹€鈹€ Two columns: Weights + Asset Details 鈹€鈹€
            ui.horizontal(|ui| {
                // Left: Allocation Weights
                ui.vertical(|ui| {
                    ui.set_width(ui.available_width() * 0.45);
                    egui::Frame::none()
                        .fill(BG_CARD)
                        .rounding(egui::Rounding::same(8.0))
                        .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
                        .inner_margin(egui::Margin::same(12.0))
                        .show(ui, |ui| {
                            section_header(ui, "Allocation Weights");
                            ui.add_space(4.0);

                            let mut sorted_weights = alloc.weights.clone();
                            sorted_weights.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

                            let colors = [ACCENT_BLUE, ACCENT_GREEN, ACCENT_PURPLE, ACCENT_CYAN, ACCENT_YELLOW, ACCENT_ORANGE, ACCENT_RED,
                                egui::Color32::from_rgb(236, 72, 153)];

                            for (i, (sym, w)) in sorted_weights.iter().enumerate() {
                                let color = colors[i % colors.len()];
                                ui.horizontal(|ui| {
                                    // Color indicator dot
                                    let (rect, _) = ui.allocate_exact_size(
                                        egui::vec2(8.0, 8.0),
                                        egui::Sense::hover(),
                                    );
                                    ui.painter().circle_filled(rect.center(), 4.0, color);

                                    ui.label(egui::RichText::new(sym)
                                        .size(13.0)
                                        .strong()
                                        .color(TEXT_PRIMARY));

                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        ui.label(egui::RichText::new(format!("{:.1}%", w * 100.0))
                                            .size(13.0)
                                            .strong()
                                            .color(color));

                                        // Weight bar
                                        let bar_width = 80.0;
                                        let (bar_rect, _) = ui.allocate_exact_size(
                                            egui::vec2(bar_width, 6.0),
                                            egui::Sense::hover(),
                                        );
                                        ui.painter().rect_filled(
                                            bar_rect,
                                            egui::Rounding::same(3.0),
                                            BG_ELEVATED,
                                        );
                                        let fill_w = (w.min(1.0) * bar_width as f64) as f32;
                                        let fill_rect = egui::Rect::from_min_size(
                                            bar_rect.min,
                                            egui::vec2(fill_w, 6.0),
                                        );
                                        ui.painter().rect_filled(
                                            fill_rect,
                                            egui::Rounding::same(3.0),
                                            color,
                                        );
                                    });
                                });
                                ui.add_space(3.0);
                            }

                            ui.add_space(8.0);
                            let total_w: f64 = sorted_weights.iter().map(|(_, w)| w).sum();
                            let cash = (1.0 - total_w.min(1.0)) * 100.0;
                            if cash > 0.1 {
                                stat_row(ui, "Cash", &format!("{:.1}%", cash), TEXT_SECONDARY);
                            }
                        });
                });

                // Right: Per-Asset Details
                ui.vertical(|ui| {
                    egui::Frame::none()
                        .fill(BG_CARD)
                        .rounding(egui::Rounding::same(8.0))
                        .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
                        .inner_margin(egui::Margin::same(12.0))
                        .show(ui, |ui| {
                            section_header(ui, "Asset Forecasts");
                            ui.add_space(4.0);

                            // Table header
                            egui::Frame::none()
                                .fill(BG_ELEVATED)
                                .rounding(egui::Rounding::same(6.0))
                                .inner_margin(egui::Margin::symmetric(6.0, 3.0))
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.set_width(ui.available_width());
                                        let col_w = ui.available_width() / 8.0;
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("Symbol").size(10.0).color(TEXT_SECONDARY));
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("Model Price").size(10.0).color(TEXT_SECONDARY));
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("Current Price").size(10.0).color(TEXT_SECONDARY));
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("Deviation").size(10.0).color(TEXT_SECONDARY));
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("E[Ret]").size(10.0).color(TEXT_SECONDARY));
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("Vol").size(10.0).color(TEXT_SECONDARY));
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("Sharpe").size(10.0).color(TEXT_SECONDARY));
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                            ui.label(egui::RichText::new("P50 Target").size(10.0).color(TEXT_SECONDARY));
                                        });
                                    });
                                });

                            ui.add(egui::Separator::default().spacing(2.0));

                            for (row_idx, f) in alloc.asset_forecasts.iter().enumerate() {
                                let ret_color = if f.annual_return > 0.0 { ACCENT_GREEN } else { ACCENT_RED };
                                let sharpe_color = if f.sharpe > 1.0 { ACCENT_GREEN }
                                    else if f.sharpe > 0.0 { ACCENT_YELLOW }
                                    else { ACCENT_RED };
                                let latest_snapshot_price = self
                                    .paper_snapshots
                                    .last()
                                    .and_then(|snapshot| {
                                        snapshot
                                            .symbols
                                            .iter()
                                            .find(|symbol_snapshot| symbol_snapshot.symbol == f.symbol)
                                            .map(|symbol_snapshot| symbol_snapshot.price)
                                    });

                                let (deviation_text, deviation_color) = if let Some(current_price) = latest_snapshot_price {
                                    let delta = current_price - f.current_price;
                                    let delta_pct = if f.current_price.abs() > 1e-9 {
                                        delta / f.current_price * 100.0
                                    } else {
                                        0.0
                                    };
                                    (
                                        format!("{:+.2} ({:+.2}%)", delta, delta_pct),
                                        if delta >= 0.0 { ACCENT_GREEN } else { ACCENT_RED },
                                    )
                                } else {
                                    ("--".to_string(), TEXT_SECONDARY)
                                };

                                let row_fill = if row_idx % 2 == 0 {
                                    BG_CARD
                                } else {
                                    BG_ELEVATED.linear_multiply(0.6)
                                };

                                egui::Frame::none()
                                    .fill(row_fill)
                                    .rounding(egui::Rounding::same(4.0))
                                    .inner_margin(egui::Margin::symmetric(6.0, 2.0))
                                    .show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            let col_w = ui.available_width() / 8.0;
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                ui.label(egui::RichText::new(&f.symbol)
                                                    .size(12.0).strong().color(TEXT_PRIMARY));
                                            });
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                ui.label(egui::RichText::new(format!("${:.2}", f.current_price))
                                                    .size(12.0).color(TEXT_PRIMARY));
                                            });
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                let current_price_text = latest_snapshot_price
                                                    .map(|price| format!("${:.2}", price))
                                                    .unwrap_or_else(|| "--".to_string());
                                                ui.label(
                                                    egui::RichText::new(current_price_text)
                                                        .size(12.0)
                                                        .color(ACCENT_CYAN),
                                                );
                                            });
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                ui.label(egui::RichText::new(deviation_text.clone())
                                                    .size(11.0).color(deviation_color));
                                            });
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                ui.label(egui::RichText::new(format!("{:+.1}%", f.annual_return * 100.0))
                                                    .size(12.0).color(ret_color));
                                            });
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                ui.label(egui::RichText::new(format!("{:.1}%", f.annual_vol * 100.0))
                                                    .size(12.0).color(TEXT_SECONDARY));
                                            });
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                ui.label(egui::RichText::new(format!("{:.2}", f.sharpe))
                                                    .size(12.0).color(sharpe_color));
                                            });
                                            ui.allocate_ui(egui::vec2(col_w, 20.0), |ui| {
                                                let pct = (f.p50_price / f.current_price - 1.0) * 100.0;
                                                let color = if pct >= 0.0 { ACCENT_GREEN } else { ACCENT_RED };
                                                ui.label(egui::RichText::new(
                                                    format!("${:.2} ({:+.1}%)", f.p50_price, pct))
                                                    .size(12.0).color(color));
                                            });
                                        });
                                    });
                                ui.add_space(2.0);
                            }

                            ui.add_space(8.0);

                            // $100K allocation
                            ui.add(egui::Separator::default().spacing(4.0));
                            ui.add_space(4.0);
                            section_header(ui, "$100,000 Allocation");
                            ui.add_space(4.0);

                            let capital = 100_000.0_f64;
                            let mut sorted = alloc.weights.clone();
                            sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

                            for (sym, w) in &sorted {
                                if let Some(f) = alloc.asset_forecasts.iter().find(|f| &f.symbol == sym) {
                                    let dollars = capital * w;
                                    let shares = (dollars / f.current_price).floor();
                                    ui.horizontal(|ui| {
                                        ui.label(egui::RichText::new(sym)
                                            .size(12.0).strong().color(TEXT_PRIMARY));
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            ui.label(egui::RichText::new(format!("~{:.0} shares", shares))
                                                .size(11.0).color(TEXT_SECONDARY));
                                            ui.label(egui::RichText::new(format!("${:.0}", dollars))
                                                .size(12.0).strong().color(ACCENT_CYAN));
                                        });
                                    });
                                }
                            }
                        });
                });
            });

            ui.add_space(8.0);
            self.render_paper_monitor_panel(ui, alloc);

            ui.add_space(12.0);
            ui.label(egui::RichText::new("Educational use only. Not financial advice.")
                .size(10.0)
                .color(TEXT_SECONDARY));
        });
    }

    fn start_paper_trading_from_weights(&mut self, target_weights: Vec<(String, f64)>) {
        if let Some(cmd_tx) = &self.paper_cmd_tx {
            let _ = cmd_tx.try_send(PaperCommand::Stop);
        }

        let config = match paper_trading::build_config(
            Some(&self.paper_initial_capital_input),
            &self.paper_time1_input,
            &self.paper_time2_input,
        ) {
            Ok(cfg) => cfg,
            Err(error) => {
                self.paper_state = PaperState::Error(error.to_string());
                self.paper_log_messages
                    .push(format!("Invalid paper trading config: {}", error));
                return;
            }
        };

        self.paper_state = PaperState::Running;
        self.paper_snapshots.clear();
        self.paper_last_analysis = None;
        self.paper_log_messages.clear();
        self.paper_strategy_file = None;
        self.paper_runtime_file = None;
        self.paper_start_time = Some(Instant::now());
        self.paper_target_weights = Some(target_weights.clone());

        let (tx, rx) = mpsc::channel(1024);
        self.paper_rx = Some(rx);
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        self.paper_cmd_tx = Some(cmd_tx);

        tokio::spawn(async move {
            let tx_clone = tx.clone();
            if let Err(error) = paper_trading::run_paper_trading(target_weights, config, tx_clone, cmd_rx).await {
                let _ = tx
                    .send(PaperEvent::Error(format!("Paper trading stopped: {}", error)))
                    .await;
            }
        });
    }

    fn start_single_stock_paper_trading(&mut self, symbol: String) {
        self.start_paper_trading_from_weights(vec![(symbol, 1.0)]);
    }

    fn pause_paper_trading(&mut self) {
        if let Some(cmd_tx) = &self.paper_cmd_tx {
            let _ = cmd_tx.try_send(PaperCommand::Pause);
            self.paper_state = PaperState::Paused;
        }
    }

    fn resume_paper_trading(&mut self) {
        if let Some(cmd_tx) = &self.paper_cmd_tx {
            let _ = cmd_tx.try_send(PaperCommand::Resume);
            self.paper_state = PaperState::Running;
            return;
        }

        if let Some(weights) = &self.paper_target_weights {
            self.start_paper_trading_from_weights(weights.clone());
        }
    }

    fn load_paper_history(&mut self) {
        match self.resolve_history_path() {
            Ok(path) => match std::fs::read_to_string(&path) {
                Ok(content) => {
                    let mut loaded = Vec::new();
                    for line in content.lines().filter(|line| !line.trim().is_empty()) {
                        if let Ok(snapshot) = serde_json::from_str::<MinutePortfolioSnapshot>(line) {
                            loaded.push(snapshot);
                        }
                    }
                    if loaded.is_empty() {
                        self.paper_state = PaperState::Error("No valid minute snapshots in history file".to_string());
                    } else {
                        self.paper_snapshots = loaded;
                        self.paper_runtime_file = Some(path.display().to_string());
                        self.paper_log_messages.push(format!(
                            "Loaded {} historical snapshots from {}",
                            self.paper_snapshots.len(),
                            path.display()
                        ));
                        if self.paper_state != PaperState::Running && self.paper_state != PaperState::Paused {
                            self.paper_state = PaperState::Idle;
                        }
                        self.paper_force_repaint = true;
                    }
                }
                Err(error) => {
                    self.paper_state = PaperState::Error(format!("Failed to read history file: {}", error));
                }
            },
            Err(error) => {
                self.paper_state = PaperState::Error(error.to_string());
            }
        }
    }

    fn resolve_history_path(&self) -> anyhow::Result<std::path::PathBuf> {
        let raw = self.paper_history_path_input.trim();
        if !raw.is_empty() {
            let candidate = std::path::PathBuf::from(raw);
            if candidate.exists() {
                return Ok(candidate);
            }
            return Err(anyhow::anyhow!("History path does not exist: {}", raw));
        }

        let log_dir = Path::new("log");
        let mut latest: Option<(std::path::PathBuf, std::time::SystemTime)> = None;
        for entry in std::fs::read_dir(log_dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string();
            if !name.starts_with("paper_runtime_") || !name.ends_with(".jsonl") {
                continue;
            }
            let modified = entry.metadata()?.modified()?;
            if latest
                .as_ref()
                .map(|(_, ts)| modified > *ts)
                .unwrap_or(true)
            {
                latest = Some((path.clone(), modified));
            }
        }

        latest
            .map(|(path, _)| path)
            .ok_or(anyhow::anyhow!("No historical paper_runtime_*.jsonl file found in log/"))
    }

    fn render_paper_monitor_panel(&mut self, ui: &mut egui::Ui, alloc: &PortfolioAllocation) {
        if self.paper_snapshots.is_empty()
            && self.paper_state != PaperState::Running
            && self.paper_state != PaperState::Paused
        {
            return;
        }

        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(12.0))
            .show(ui, |ui| {
                section_header(ui, "Live Portfolio Monitor (1m)");

                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Curve Range").size(10.0).color(TEXT_SECONDARY));
                    egui::ComboBox::from_id_salt("paper_curve_range")
                        .selected_text(self.curve_range_label())
                        .show_ui(ui, |ui| {
                            ui.selectable_value(&mut self.paper_curve_preset, CurveRangePreset::D1, "1D");
                            ui.selectable_value(&mut self.paper_curve_preset, CurveRangePreset::D7, "7D");
                            ui.selectable_value(&mut self.paper_curve_preset, CurveRangePreset::M1, "1M");
                            ui.selectable_value(&mut self.paper_curve_preset, CurveRangePreset::M3, "3M");
                            ui.selectable_value(&mut self.paper_curve_preset, CurveRangePreset::Y1, "1Y");
                            ui.selectable_value(&mut self.paper_curve_preset, CurveRangePreset::Y2, "2Y");
                            ui.selectable_value(&mut self.paper_curve_preset, CurveRangePreset::Manual, "Manual");
                        });

                    if self.paper_curve_preset == CurveRangePreset::Manual {
                        ui.label(egui::RichText::new("Days").size(10.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.paper_curve_manual_days_input).desired_width(70.0));
                        ui.label(egui::RichText::new("(max 3650)").size(10.0).color(TEXT_SECONDARY));
                    }
                });

                if let Some(snapshot) = self.paper_snapshots.last() {
                    let filtered = self.filtered_paper_snapshots();
                    ui.horizontal(|ui| {
                        summary_card(ui, "Total Value", &format!("${:.2}", snapshot.total_value), ACCENT_CYAN);
                        summary_card(ui, "PnL", &format!("${:+.2}", snapshot.pnl_usd), if snapshot.pnl_usd >= 0.0 { ACCENT_GREEN } else { ACCENT_RED });
                        summary_card(ui, "PnL %", &format!("{:+.2}%", snapshot.pnl_pct), if snapshot.pnl_pct >= 0.0 { ACCENT_GREEN } else { ACCENT_RED });
                        summary_card(ui, "QQQ Ref", &format!("{:+.2}%", snapshot.benchmark_return_pct), ACCENT_YELLOW);
                    });

                    if let Some(start_time) = self.paper_start_time {
                        ui.label(egui::RichText::new(format!(
                            "Runtime: {} | Last update: {}",
                            format_duration(start_time.elapsed().as_secs()),
                            snapshot.timestamp
                        ))
                        .size(10.0)
                        .color(TEXT_SECONDARY));
                    }

                    let asset_curve: PlotPoints = filtered
                        .iter()
                        .enumerate()
                        .map(|(index, point)| [index as f64, point.total_value])
                        .collect();
                    let benchmark_curve: PlotPoints = filtered
                        .iter()
                        .enumerate()
                        .map(|(index, point)| {
                            [
                                index as f64,
                                filtered
                                    .first()
                                    .map(|point| point.total_value - point.pnl_usd)
                                    .unwrap_or(paper_trading::DEFAULT_INITIAL_CAPITAL_USD)
                                    * (1.0 + point.benchmark_return_pct / 100.0),
                            ]
                        })
                        .collect();

                    Plot::new("paper_portfolio_curve")
                        .height(220.0)
                        .x_axis_label("Point")
                        .y_axis_label("USD")
                        .legend(egui_plot::Legend::default().position(egui_plot::Corner::RightTop))
                        .show(ui, |plot_ui| {
                            plot_ui.line(Line::new(asset_curve).name("Portfolio Value").color(ACCENT_CYAN).width(2.2));
                            plot_ui.line(Line::new(benchmark_curve).name("QQQ Benchmark Value").color(ACCENT_YELLOW).width(1.8));
                        });

                    ui.add_space(4.0);
                    section_header(ui, "Input + Holding Symbols Realtime Price");
                    let mut symbols_to_show: Vec<String> = Vec::new();
                    for asset in &alloc.asset_forecasts {
                        if !symbols_to_show.iter().any(|existing| existing == &asset.symbol) {
                            symbols_to_show.push(asset.symbol.clone());
                        }
                    }
                    if let Some(target_weights) = &self.paper_target_weights {
                        for (symbol, _) in target_weights {
                            if !symbols_to_show.iter().any(|existing| existing == symbol) {
                                symbols_to_show.push(symbol.clone());
                            }
                        }
                    }
                    for symbol in &snapshot.holdings_symbols {
                        if !symbols_to_show.iter().any(|existing| existing == symbol) {
                            symbols_to_show.push(symbol.clone());
                        }
                    }

                    if symbols_to_show.is_empty() {
                        ui.label(
                            egui::RichText::new("No input/holding symbols available yet")
                                .size(10.0)
                                .color(TEXT_SECONDARY),
                        );
                    } else {
                        egui::Frame::none()
                            .fill(BG_ELEVATED)
                            .rounding(egui::Rounding::same(6.0))
                            .inner_margin(egui::Margin::symmetric(6.0, 3.0))
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    let col_w = ui.available_width() / 3.0;
                                    ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                        ui.label(egui::RichText::new("Symbol").size(10.0).color(TEXT_SECONDARY));
                                    });
                                    ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                        ui.label(egui::RichText::new("Current Price").size(10.0).color(TEXT_SECONDARY));
                                    });
                                    ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                        ui.label(egui::RichText::new("Tag").size(10.0).color(TEXT_SECONDARY));
                                    });
                                });
                            });
                        ui.add(egui::Separator::default().spacing(2.0));

                        for symbol in symbols_to_show {
                            let symbol_price = snapshot
                                .symbols
                                .iter()
                                .find(|symbol_snapshot| symbol_snapshot.symbol == symbol)
                                .map(|symbol_snapshot| symbol_snapshot.price);
                            let model_price = alloc
                                .asset_forecasts
                                .iter()
                                .find(|forecast| forecast.symbol == symbol)
                                .map(|forecast| forecast.current_price);

                            let is_holding = snapshot.holdings_symbols.iter().any(|held| held == &symbol);
                            let price_text = symbol_price
                                .map(|price| format!("${:.2}", price))
                                .or_else(|| model_price.map(|price| format!("${:.2}", price)))
                                .unwrap_or_else(|| "--".to_string());

                            egui::Frame::none()
                                .fill(BG_ELEVATED.linear_multiply(0.35))
                                .rounding(egui::Rounding::same(4.0))
                                .inner_margin(egui::Margin::symmetric(6.0, 2.0))
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        let col_w = ui.available_width() / 3.0;
                                let symbol_label = if is_holding {
                                            symbol.clone()
                                } else {
                                            symbol.clone()
                                };
                                        ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                            ui.label(
                                                egui::RichText::new(symbol_label)
                                                    .size(11.0)
                                                    .strong()
                                                    .color(TEXT_PRIMARY),
                                            );
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                            ui.label(
                                                egui::RichText::new(price_text)
                                                    .size(11.0)
                                                    .color(ACCENT_CYAN),
                                            );
                                        });
                                        ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                            let tag_text = if is_holding { "Holding" } else { "Input" };
                                            let tag_color = if is_holding { ACCENT_GREEN } else { TEXT_SECONDARY };
                                            ui.label(
                                                egui::RichText::new(tag_text)
                                                    .size(10.0)
                                                    .color(tag_color),
                                            );
                                        });
                                    });
                                });
                            ui.add_space(2.0);
                        }
                    }

                    ui.add_space(4.0);
                    section_header(ui, "Per-Symbol 1m Change");
                    ui.horizontal(|ui| {
                        let col_w = ui.available_width() / 4.0;
                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                            ui.label(egui::RichText::new("Symbol").size(10.0).color(TEXT_SECONDARY));
                        });
                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                            ui.label(egui::RichText::new("Current Price").size(10.0).color(TEXT_SECONDARY));
                        });
                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                            ui.label(egui::RichText::new("1m Δ").size(10.0).color(TEXT_SECONDARY));
                        });
                        ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                            ui.label(egui::RichText::new("1m Δ%").size(10.0).color(TEXT_SECONDARY));
                        });
                    });
                    ui.add(egui::Separator::default().spacing(2.0));

                    for symbol_snapshot in &snapshot.symbols {
                        let change_color = if symbol_snapshot.change_1m >= 0.0 {
                            ACCENT_GREEN
                        } else {
                            ACCENT_RED
                        };
                        ui.horizontal(|ui| {
                            let col_w = ui.available_width() / 4.0;
                            ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                ui.label(
                                    egui::RichText::new(&symbol_snapshot.symbol)
                                        .size(11.0)
                                        .strong()
                                        .color(TEXT_PRIMARY),
                                );
                            });
                            ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                ui.label(
                                    egui::RichText::new(format!("${:.2}", symbol_snapshot.price))
                                        .size(11.0)
                                        .color(TEXT_SECONDARY),
                                );
                            });
                            ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                ui.label(
                                    egui::RichText::new(format!("{:+.3}", symbol_snapshot.change_1m))
                                        .size(11.0)
                                        .color(change_color),
                                );
                            });
                            ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                ui.label(
                                    egui::RichText::new(format!("{:+.3}%", symbol_snapshot.change_1m_pct))
                                        .size(11.0)
                                        .color(change_color),
                                );
                            });
                        });
                    }
                }

                if let Some(analysis) = &self.paper_last_analysis {
                    ui.add_space(6.0);
                    section_header(ui, "Latest Rebalance");
                    ui.label(
                        egui::RichText::new(format!(
                            "{} | trades={} | value_before=${:.2} | value_after=${:.2}",
                            analysis.timestamp,
                            analysis.trades.len(),
                            analysis.portfolio_value_before,
                            analysis.portfolio_value_after
                        ))
                        .size(10.0)
                        .color(TEXT_SECONDARY),
                    );
                }

                if !self.paper_log_messages.is_empty() {
                    ui.add_space(6.0);
                    section_header(ui, "Simulation Log");
                    egui::ScrollArea::vertical().max_height(80.0).show(ui, |ui| {
                        for message in self.paper_log_messages.iter().rev().take(8).rev() {
                            ui.label(egui::RichText::new(message).size(10.0).color(TEXT_SECONDARY));
                        }
                    });
                }
            });
    }

    fn curve_range_label(&self) -> &'static str {
        match self.paper_curve_preset {
            CurveRangePreset::D1 => "1D",
            CurveRangePreset::D7 => "7D",
            CurveRangePreset::M1 => "1M",
            CurveRangePreset::M3 => "3M",
            CurveRangePreset::Y1 => "1Y",
            CurveRangePreset::Y2 => "2Y",
            CurveRangePreset::Manual => "Manual",
        }
    }

    fn filtered_paper_snapshots(&self) -> Vec<&MinutePortfolioSnapshot> {
        if self.paper_snapshots.is_empty() {
            return Vec::new();
        }

        let max_days = match self.paper_curve_preset {
            CurveRangePreset::D1 => 1,
            CurveRangePreset::D7 => 7,
            CurveRangePreset::M1 => 30,
            CurveRangePreset::M3 => 90,
            CurveRangePreset::Y1 => 365,
            CurveRangePreset::Y2 => 730,
            CurveRangePreset::Manual => self
                .paper_curve_manual_days_input
                .trim()
                .parse::<i64>()
                .ok()
                .unwrap_or(30)
                .clamp(1, 3650),
        };

        let reference_time = self
            .paper_snapshots
            .last()
            .and_then(|snapshot| DateTime::parse_from_rfc3339(&snapshot.timestamp).ok());

        let Some(reference_time) = reference_time else {
            return self.paper_snapshots.iter().collect();
        };

        let cutoff = reference_time - ChronoDuration::days(max_days);

        let mut filtered: Vec<&MinutePortfolioSnapshot> = self
            .paper_snapshots
            .iter()
            .filter(|snapshot| {
                DateTime::parse_from_rfc3339(&snapshot.timestamp)
                    .map(|timestamp| timestamp >= cutoff)
                    .unwrap_or(false)
            })
            .collect();

        if filtered.is_empty() {
            filtered = self.paper_snapshots.iter().collect();
        }

        filtered
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Train Tab
// ──────────────────────────────────────────────────────────────────────────────

impl GuiApp {
    fn render_train_tab(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        if self.train_state == TrainState::Running {
            ctx.request_repaint();
        }

        ui.add_space(8.0);

        // ── Config Card ──
        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(16.0))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Model Training")
                        .size(16.0)
                        .strong()
                        .color(TEXT_PRIMARY));

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        // Status badge
                        let (status_text, status_color) = match &self.train_state {
                            TrainState::Idle => ("Ready", TEXT_SECONDARY),
                            TrainState::Running => ("Training...", ACCENT_YELLOW),
                            TrainState::Finished => ("Completed", ACCENT_GREEN),
                            TrainState::Error(_) => ("Error", ACCENT_RED),
                        };
                        ui.label(egui::RichText::new(status_text)
                            .size(12.0)
                            .strong()
                            .color(status_color));
                    });
                });

                ui.add_space(12.0);

                // Config row
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 16.0;

                    ui.vertical(|ui| {
                        ui.label(egui::RichText::new("Epochs").size(11.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.train_epochs)
                            .desired_width(70.0));
                    });
                    ui.vertical(|ui| {
                        ui.label(egui::RichText::new("Batch Size").size(11.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.train_batch_size)
                            .desired_width(70.0));
                    });
                    ui.vertical(|ui| {
                        ui.label(egui::RichText::new("Learning Rate").size(11.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.train_lr)
                            .desired_width(80.0));
                    });
                    ui.vertical(|ui| {
                        ui.label(egui::RichText::new("Patience").size(11.0).color(TEXT_SECONDARY));
                        ui.add(egui::TextEdit::singleline(&mut self.train_patience)
                            .desired_width(70.0));
                    });

                    ui.add_space(16.0);

                    let is_running = self.train_state == TrainState::Running;

                    let start_btn = ui.add_enabled(
                        !is_running,
                        egui::Button::new(
                            egui::RichText::new(if is_running { "Training..." } else { "Start Training" })
                                .size(13.0)
                                .strong()
                                .color(egui::Color32::WHITE),
                        )
                        .fill(if is_running { BG_ELEVATED } else { ACCENT_GREEN })
                        .rounding(egui::Rounding::same(6.0)),
                    );

                    if start_btn.clicked() && !is_running {
                        self.start_training();
                    }
                });

                if let TrainState::Error(e) = &self.train_state {
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new(format!("Error: {}", e))
                        .color(ACCENT_RED)
                        .size(12.0));
                }
            });

        ui.add_space(8.0);

        // Show content based on state
        if self.train_log.is_empty() && self.train_state == TrainState::Idle {
            // Empty state
            let available = ui.available_size();
            ui.vertical_centered(|ui| {
                ui.add_space(available.y * 0.15);
                ui.label(egui::RichText::new("TimeGrad Diffusion Model Trainer")
                    .size(22.0)
                    .strong()
                    .color(TEXT_PRIMARY));
                ui.add_space(8.0);
                ui.label(egui::RichText::new(
                    "Configure hyperparameters above and click Start Training.\n\
                     The model will train on historical data from 18 diversified assets,\n\
                     learning to reverse-diffuse noise into accurate price forecasts.")
                    .size(13.0)
                    .color(TEXT_SECONDARY));
                ui.add_space(20.0);

                // Training symbols display
                egui::Frame::none()
                    .fill(BG_CARD)
                    .rounding(egui::Rounding::same(8.0))
                    .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
                    .inner_margin(egui::Margin::same(12.0))
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("Training Universe")
                            .size(12.0)
                            .strong()
                            .color(ACCENT_CYAN));
                        ui.add_space(4.0);
                        let symbols = crate::config::TRAINING_SYMBOLS.join(", ");
                        ui.label(egui::RichText::new(symbols)
                            .size(11.0)
                            .color(TEXT_SECONDARY));
                    });
            });
        } else {
            // Results area: loss chart + log
            let available = ui.available_size();
            let chart_height = (available.y * 0.55).max(200.0);

            ui.horizontal(|ui| {
                // Left: Loss Chart
                ui.vertical(|ui| {
                    ui.set_width(available.x * 0.6);
                    self.render_loss_chart(ui, chart_height);
                });

                // Right: Training Log + Stats
                ui.vertical(|ui| {
                    self.render_train_stats_and_log(ui, chart_height);
                });
            });
        }
    }

    fn start_training(&mut self) {
        let epochs = self.train_epochs.parse::<usize>().unwrap_or(200);
        let batch_size = self.train_batch_size.parse::<usize>().unwrap_or(64);
        let lr = self.train_lr.parse::<f64>().unwrap_or(1e-3);
        let patience = self.train_patience.parse::<usize>().unwrap_or(20);
        let use_cuda = self.app.use_cuda;

        self.train_state = TrainState::Running;
        self.train_log.clear();
        self.train_log_messages.clear();
        self.train_start_time = Some(Instant::now());

        let (tx, rx) = mpsc::channel(256);
        self.train_rx = Some(rx);

        tokio::spawn(async move {
            let tx_clone = tx.clone();
            let _ = tx.send(TrainMessage::Log(format!(
                "Starting training: epochs={}, batch={}, lr={}, patience={}",
                epochs, batch_size, lr, patience
            ))).await;

            match train::train_model_with_progress(
                Some(epochs),
                Some(batch_size),
                Some(lr),
                Some(patience),
                use_cuda,
                tx_clone,
            ).await {
                Ok(()) => {
                    let _ = tx.send(TrainMessage::Finished).await;
                }
                Err(e) => {
                    let _ = tx.send(TrainMessage::Error(e.to_string())).await;
                }
            }
        });
    }

    fn render_loss_chart(&self, ui: &mut egui::Ui, height: f32) {
        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(8.0))
            .show(ui, |ui| {
                section_header(ui, "Training Loss Curve");

                let plot = Plot::new("loss_chart")
                    .legend(egui_plot::Legend::default().position(egui_plot::Corner::RightTop))
                    .x_axis_label("Epoch")
                    .y_axis_label("Loss")
                    .height(height - 40.0)
                    .allow_drag(true)
                    .allow_zoom(true);

                plot.show(ui, |plot_ui| {
                    if !self.train_log.is_empty() {
                        let train_points: PlotPoints = self.train_log.iter()
                            .map(|e| [e.epoch as f64, e.train_loss])
                            .collect();
                        plot_ui.line(
                            Line::new(train_points)
                                .name("Train Loss")
                                .color(ACCENT_BLUE)
                                .width(2.0),
                        );

                        let val_points: PlotPoints = self.train_log.iter()
                            .filter(|e| e.val_loss > 0.0)
                            .map(|e| [e.epoch as f64, e.val_loss])
                            .collect();
                        plot_ui.line(
                            Line::new(val_points)
                                .name("Val Loss")
                                .color(ACCENT_ORANGE)
                                .width(2.0),
                        );
                    }
                });
            });
    }

    fn render_train_stats_and_log(&self, ui: &mut egui::Ui, height: f32) {
        // Stats cards
        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(12.0))
            .show(ui, |ui| {
                section_header(ui, "Training Statistics");

                if let Some(last) = self.train_log.last() {
                    stat_row(ui, "Current Epoch", &format!("{}", last.epoch), ACCENT_CYAN);
                    stat_row(ui, "Train Loss", &format!("{:.6}", last.train_loss), ACCENT_BLUE);
                    stat_row(ui, "Val Loss", &format!("{:.6}", last.val_loss), ACCENT_ORANGE);

                    // Best val loss
                    let best_val = self.train_log.iter()
                        .filter(|e| e.val_loss > 0.0)
                        .map(|e| e.val_loss)
                        .fold(f64::INFINITY, f64::min);
                    if best_val < f64::INFINITY {
                        stat_row(ui, "Best Val Loss", &format!("{:.6}", best_val), ACCENT_GREEN);
                    }

                    // Improvement tracking
                    if self.train_log.len() >= 2 {
                        let prev = &self.train_log[self.train_log.len() - 2];
                        let delta = last.train_loss - prev.train_loss;
                        let color = if delta < 0.0 { ACCENT_GREEN } else { ACCENT_RED };
                        stat_row(ui, "Loss Delta", &format!("{:+.6}", delta), color);
                    }

                    // Progress bar
                    let total_epochs = self.train_epochs.parse::<usize>().unwrap_or(200);
                    let progress = last.epoch as f32 / total_epochs as f32;
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new(format!("Progress: {}/{}", last.epoch, total_epochs))
                        .size(11.0)
                        .color(TEXT_SECONDARY));
                    ui.add(egui::ProgressBar::new(progress.min(1.0))
                        .fill(ACCENT_BLUE)
                        .animate(self.train_state == TrainState::Running));

                    // ETA display
                    if let Some(start) = self.train_start_time {
                        let elapsed = start.elapsed();
                        let elapsed_secs = elapsed.as_secs();
                        let elapsed_str = format_duration(elapsed_secs);
                        if last.epoch > 0 && self.train_state == TrainState::Running {
                            let secs_per_epoch = elapsed_secs as f64 / last.epoch as f64;
                            let remaining = total_epochs.saturating_sub(last.epoch);
                            let eta_secs = (secs_per_epoch * remaining as f64) as u64;
                            let eta_str = format_duration(eta_secs);
                            stat_row(ui, "Elapsed", &elapsed_str, TEXT_SECONDARY);
                            stat_row(ui, "ETA", &eta_str, ACCENT_YELLOW);
                        } else {
                            stat_row(ui, "Elapsed", &elapsed_str, TEXT_SECONDARY);
                        }
                    }
                } else {
                    ui.label(egui::RichText::new("Waiting for first epoch...")
                        .size(12.0)
                        .color(TEXT_SECONDARY));
                    ui.spinner();
                }
            });

        ui.add_space(8.0);

        // Log panel
        let log_height = (height - 200.0).max(100.0);
        egui::Frame::none()
            .fill(BG_CARD)
            .rounding(egui::Rounding::same(8.0))
            .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
            .inner_margin(egui::Margin::same(12.0))
            .show(ui, |ui| {
                section_header(ui, "Log");
                egui::ScrollArea::vertical()
                    .max_height(log_height)
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for msg in &self.train_log_messages {
                            ui.label(egui::RichText::new(msg)
                                .size(10.0)
                                .color(TEXT_SECONDARY)
                                .family(egui::FontFamily::Monospace));
                        }
                        // Show epoch summaries
                        for entry in &self.train_log {
                            ui.label(egui::RichText::new(
                                format!("Epoch {:>3}: train={:.6}  val={:.6}",
                                    entry.epoch, entry.train_loss, entry.val_loss))
                                .size(10.0)
                                .color(TEXT_SECONDARY)
                                .family(egui::FontFamily::Monospace));
                        }
                    });
            });
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn section_header(ui: &mut egui::Ui, text: &str) {
    ui.label(egui::RichText::new(text)
        .size(13.0)
        .strong()
        .color(TEXT_PRIMARY));
    ui.add_space(4.0);
}

fn stat_row(ui: &mut egui::Ui, label: &str, value: &str, color: egui::Color32) {
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new(label).size(11.0).color(TEXT_SECONDARY));
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.label(egui::RichText::new(value).size(12.0).strong().color(color));
        });
    });
}

fn summary_card(ui: &mut egui::Ui, label: &str, value: &str, color: egui::Color32) {
    egui::Frame::none()
        .fill(BG_CARD)
        .rounding(egui::Rounding::same(8.0))
        .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
        .inner_margin(egui::Margin::same(12.0))
        .show(ui, |ui| {
            ui.set_min_width(100.0);
            ui.vertical_centered(|ui| {
                ui.label(egui::RichText::new(label)
                    .size(10.0)
                    .color(TEXT_SECONDARY));
                ui.label(egui::RichText::new(value)
                    .size(20.0)
                    .strong()
                    .color(color));
            });
        });
}

fn small_chip(ui: &mut egui::Ui, text: &str, color: egui::Color32) {
    egui::Frame::none()
        .fill(color.linear_multiply(0.18))
        .rounding(egui::Rounding::same(6.0))
        .stroke(egui::Stroke::new(1.0, color.linear_multiply(0.7)))
        .inner_margin(egui::Margin::symmetric(6.0, 2.0))
        .show(ui, |ui| {
            ui.label(
                egui::RichText::new(text)
                    .size(10.0)
                    .strong()
                    .color(color),
            );
        });
}

fn format_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{}h {:02}m {:02}s", h, m, s)
    } else if m > 0 {
        format!("{}m {:02}s", m, s)
    } else {
        format!("{}s", s)
    }
}

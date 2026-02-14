use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints, PlotUi};
use crate::app::{App, AppState};
use crate::portfolio::{self, PortfolioAllocation};
use crate::train;
use chrono::TimeZone;
use tokio::sync::mpsc;

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
    // Train
    train_state: TrainState,
    train_epochs: String,
    train_batch_size: String,
    train_lr: String,
    train_patience: String,
    train_log: Vec<TrainLogEntry>,
    train_log_messages: Vec<String>,
    train_rx: Option<mpsc::Receiver<TrainMessage>>,
}

impl GuiApp {
    pub fn new(app: App) -> Self {
        Self {
            app,
            active_tab: GuiTab::Forecast,
            portfolio_input: String::from("NVDA,MSFT,AAPL,GOOGL,AMZN,META,QQQ,SPY"),
            portfolio_state: PortfolioState::Idle,
            portfolio_result: None,
            portfolio_rx: None,
            train_state: TrainState::Idle,
            train_epochs: String::from("200"),
            train_batch_size: String::from("64"),
            train_lr: String::from("0.001"),
            train_patience: String::from("20"),
            train_log: Vec::new(),
            train_log_messages: Vec::new(),
            train_rx: None,
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

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
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
            AppState::Input => self.render_input_screen(ui),
            AppState::Loading => {
                self.render_centered_status(ui, "Fetching Market Data...", None);
                ctx.request_repaint();
            }
            AppState::Forecasting => {
                self.render_centered_status(
                    ui,
                    "Running Diffusion Inference...",
                    Some(self.app.progress as f32),
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
        });
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

    fn render_side_panel(&self, ui: &mut egui::Ui) {
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
            self.render_centered_status(ui, "Optimizing Portfolio...\nForecasting all assets via Diffusion Model", None);
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
        if let Some(alloc) = &self.portfolio_result.clone() {
            self.render_portfolio_results(ui, alloc);
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

    fn render_portfolio_results(&self, ui: &mut egui::Ui, alloc: &PortfolioAllocation) {
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
                            ui.horizontal(|ui| {
                                ui.set_width(ui.available_width());
                                let col_w = ui.available_width() / 6.0;
                                ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                    ui.label(egui::RichText::new("Symbol").size(10.0).color(TEXT_SECONDARY));
                                });
                                ui.allocate_ui(egui::vec2(col_w, 16.0), |ui| {
                                    ui.label(egui::RichText::new("Price").size(10.0).color(TEXT_SECONDARY));
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

                            ui.add(egui::Separator::default().spacing(2.0));

                            for f in &alloc.asset_forecasts {
                                let ret_color = if f.annual_return > 0.0 { ACCENT_GREEN } else { ACCENT_RED };
                                let sharpe_color = if f.sharpe > 1.0 { ACCENT_GREEN }
                                    else if f.sharpe > 0.0 { ACCENT_YELLOW }
                                    else { ACCENT_RED };

                                ui.horizontal(|ui| {
                                    let col_w = ui.available_width() / 6.0;
                                    ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                        ui.label(egui::RichText::new(&f.symbol)
                                            .size(12.0).strong().color(TEXT_PRIMARY));
                                    });
                                    ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                        ui.label(egui::RichText::new(format!("${:.2}", f.current_price))
                                            .size(12.0).color(TEXT_PRIMARY));
                                    });
                                    ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                        ui.label(egui::RichText::new(format!("{:+.1}%", f.annual_return * 100.0))
                                            .size(12.0).color(ret_color));
                                    });
                                    ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                        ui.label(egui::RichText::new(format!("{:.1}%", f.annual_vol * 100.0))
                                            .size(12.0).color(TEXT_SECONDARY));
                                    });
                                    ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                        ui.label(egui::RichText::new(format!("{:.2}", f.sharpe))
                                            .size(12.0).color(sharpe_color));
                                    });
                                    ui.allocate_ui(egui::vec2(col_w, 18.0), |ui| {
                                        let pct = (f.p50_price / f.current_price - 1.0) * 100.0;
                                        let color = if pct >= 0.0 { ACCENT_GREEN } else { ACCENT_RED };
                                        ui.label(egui::RichText::new(
                                            format!("${:.2} ({:+.1}%)", f.p50_price, pct))
                                            .size(12.0).color(color));
                                    });
                                });
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

            ui.add_space(12.0);
            ui.label(egui::RichText::new("Educational use only. Not financial advice.")
                .size(10.0)
                .color(TEXT_SECONDARY));
        });
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

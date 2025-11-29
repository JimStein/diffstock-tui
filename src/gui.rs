use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints};
use crate::app::{App, AppState};
use chrono::TimeZone;

pub struct GuiApp {
    app: App,
}

impl GuiApp {
    pub fn new(app: App) -> Self {
        Self { app }
    }
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.app.tick();

        egui::CentralPanel::default().show(ctx, |ui| {
            match self.app.state {
                AppState::Input => {
                    ui.vertical_centered(|ui| {
                        ui.add_space(50.0);
                        ui.heading("DiffStock");
                        ui.add_space(20.0);
                        ui.label("Enter stock symbol:");
                        
                        let response = ui.text_edit_singleline(&mut self.app.input);
                        if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                             self.app.trigger_fetch();
                        }
                        
                        ui.add_space(10.0);
                        if ui.button("Predict").clicked() {
                            self.app.trigger_fetch();
                        }
                        
                        if let Some(err) = &self.app.error_msg {
                            ui.add_space(10.0);
                            ui.colored_label(egui::Color32::RED, err);
                        }
                    });
                }
                AppState::Loading => {
                    ui.vertical_centered(|ui| {
                        ui.add_space(100.0);
                        ui.spinner();
                        ui.label("Fetching data...");
                    });
                    ctx.request_repaint();
                }
                AppState::Forecasting => {
                    ui.vertical_centered(|ui| {
                        ui.add_space(100.0);
                        ui.label("Running Inference...");
                        ui.add(egui::ProgressBar::new(self.app.progress as f32));
                    });
                    ctx.request_repaint();
                }
                AppState::Dashboard => {
                    ui.horizontal(|ui| {
                        if ui.button("Back").clicked() {
                            self.app.state = AppState::Input;
                            self.app.input.clear();
                            self.app.stock_data = None;
                            self.app.forecast = None;
                        }
                        if let Some(data) = &self.app.stock_data {
                            ui.label(format!("Symbol: {}", data.symbol));
                        }
                    });
                    
                    let plot = Plot::new("stock_plot")
                        .legend(egui_plot::Legend::default().position(egui_plot::Corner::LeftTop))
                        .x_axis_formatter(|x, _range| {
                            let dt = chrono::Utc.timestamp_opt(x.value as i64, 0).unwrap();
                            dt.format("%Y-%m-%d").to_string()
                        })
                        .label_formatter(|name, value| {
                            let dt = chrono::Utc.timestamp_opt(value.x as i64, 0)
                                .map(|dt| dt.format("%Y-%m-%d").to_string())
                                .single()
                                .unwrap_or_default();
                            format!("{}\nDate: {}\nPrice: {:.2}", name, dt, value.y)
                        })
                        .coordinates_formatter(egui_plot::Corner::LeftBottom, egui_plot::CoordinatesFormatter::new(|point: &egui_plot::PlotPoint, _bounds: &egui_plot::PlotBounds| {
                            let dt = chrono::Utc.timestamp_opt(point.x as i64, 0)
                                .map(|dt| dt.format("%Y-%m-%d").to_string())
                                .single()
                                .unwrap_or_default();
                            format!("Date: {}, Price: {:.2}", dt, point.y)
                        }))
                        .view_aspect(2.0)
                        .link_axis("stock_link", true, false);
                        
                    plot.show(ui, |plot_ui| {
                        if let Some(data) = &self.app.stock_data {
                            let points: PlotPoints = data.history.iter()
                                .map(|c| [c.date.timestamp() as f64, c.close])
                                .collect();
                            plot_ui.line(Line::new(points).name("History"));
                        }
                        
                        if let Some(forecast) = &self.app.forecast {
                            // Assuming forecast times are also timestamps
                            let p50: PlotPoints = forecast.p50.iter().map(|&(x, y)| [x, y]).collect();
                            let p30: PlotPoints = forecast.p30.iter().map(|&(x, y)| [x, y]).collect();
                            let p70: PlotPoints = forecast.p70.iter().map(|&(x, y)| [x, y]).collect();
                            let p10: PlotPoints = forecast.p10.iter().map(|&(x, y)| [x, y]).collect();
                            let p90: PlotPoints = forecast.p90.iter().map(|&(x, y)| [x, y]).collect();

                            plot_ui.line(Line::new(p50).name("Median").color(egui::Color32::GREEN));
                            plot_ui.line(Line::new(p30).name("P30").style(egui_plot::LineStyle::Dashed { length: 5.0 }).color(egui::Color32::LIGHT_GREEN));
                            plot_ui.line(Line::new(p70).name("P70").style(egui_plot::LineStyle::Dashed { length: 5.0 }).color(egui::Color32::LIGHT_GREEN));
                            plot_ui.line(Line::new(p10).name("P10").style(egui_plot::LineStyle::Dashed { length: 10.0 }).color(egui::Color32::from_rgb(100, 200, 100)));
                            plot_ui.line(Line::new(p90).name("P90").style(egui_plot::LineStyle::Dashed { length: 10.0 }).color(egui::Color32::from_rgb(100, 200, 100)));
                        }
                    });

                    ui.add_space(10.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        ui.vertical(|ui| {
                            ui.heading("Market Data");
                            if let Some(data) = &self.app.stock_data {
                                if let Some(last) = data.history.last() {
                                    ui.label(format!("Last Price: {:.2}", last.close));
                                    ui.label(format!("Date: {}", last.date.format("%Y-%m-%d")));
                                    ui.label(format!("Volume: {:.0}", last.volume));
                                }
                            }
                        });
                        ui.separator();
                        ui.vertical(|ui| {
                            ui.heading("Forecast");
                            if let Some(forecast) = &self.app.forecast {
                                if let Some((timestamp, p50_last)) = forecast.p50.last() {
                                     let dt = chrono::Utc.timestamp_opt(*timestamp as i64, 0)
                                         .map(|dt| dt.format("%Y-%m-%d").to_string())
                                         .single()
                                         .unwrap_or_default();
                                     ui.label(egui::RichText::new(format!("Target (Median) (50day - {}): {:.2}", dt, p50_last)).strong().color(egui::Color32::GREEN));
                                }
                                if let Some((_, p30_last)) = forecast.p30.last() {
                                     if let Some((_, p70_last)) = forecast.p70.last() {
                                         ui.label(egui::RichText::new(format!("Range (P30-P70): {:.2} - {:.2}", p30_last, p70_last)).strong().color(egui::Color32::YELLOW));
                                     }
                                }
                                 if let Some((_, p10_last)) = forecast.p10.last() {
                                     if let Some((_, p90_last)) = forecast.p90.last() {
                                         ui.label(egui::RichText::new(format!("Range (P10-P90): {:.2} - {:.2}", p10_last, p90_last)).strong().color(egui::Color32::LIGHT_RED));
                                         ui.label("P10: Bearish / Conservative");
                                         ui.label("P90: Bullish / Optimistic");
                                     }
                                }
                            }
                        });
                    });
                }
            }
        });
    }
}

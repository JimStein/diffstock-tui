use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style, Modifier},
    symbols,
    text::{Span, Line},
    widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, GraphType, Paragraph},
    Frame,
};
use crate::app::{App, AppState};

pub fn render(f: &mut Frame, app: &App) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(2),
        ])
        .split(f.area());

    render_header(f, app, layout[0]);

    match app.state {
        AppState::Input => render_input(f, app, layout[1]),
        AppState::Loading => render_loading(f, "Fetching market data...", layout[1]),
        AppState::Forecasting => render_progress(f, app, layout[1]),
        AppState::Dashboard => render_dashboard(f, app, layout[1]),
    }

    render_footer(f, app, layout[2]);
}

fn render_header(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let mut spans = vec![
        Span::styled(" DiffStock TUI ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(" | "),
        Span::styled(
            match app.state {
                AppState::Input => "Input",
                AppState::Loading => "Loading",
                AppState::Forecasting => "Forecasting",
                AppState::Dashboard => "Dashboard",
            },
            Style::default().fg(Color::Yellow),
        ),
    ];

    if let Some(data) = &app.stock_data {
        if let Some(last) = data.history.last() {
            spans.push(Span::raw(" | "));
            spans.push(Span::styled(
                format!("{} ${:.2}", data.symbol, last.close),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ));

            if data.history.len() >= 2 {
                let prev = data.history[data.history.len() - 2].close;
                let delta = last.close - prev;
                let pct = delta / prev * 100.0;
                let color = if delta >= 0.0 { Color::Green } else { Color::Red };
                spans.push(Span::raw(" "));
                spans.push(Span::styled(
                    format!("({:+.2}, {:+.2}%)", delta, pct),
                    Style::default().fg(color),
                ));
            }
        }
    }

    let header = Paragraph::new(Line::from(spans)).block(Block::default().borders(Borders::ALL));
    f.render_widget(header, area);
}

fn render_footer(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let hint = match app.state {
        AppState::Input => "Enter: predict | Esc: quit",
        AppState::Loading => "Loading...",
        AppState::Forecasting => "Esc: quit",
        AppState::Dashboard => "r: reset | q/Esc: quit",
    };

    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" Controls: ", Style::default().fg(Color::Gray)),
        Span::styled(hint, Style::default().fg(Color::White)),
    ]))
    .block(Block::default().borders(Borders::ALL));

    f.render_widget(footer, area);
}

fn render_progress(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(35),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Percentage(40),
        ])
        .split(area);

    let gauge = Gauge::default()
        .block(Block::default().title(" Diffusion Inference Progress ").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Cyan))
        .percent((app.progress * 100.0) as u16);

    f.render_widget(gauge, chunks[1]);

    let progress_text = Paragraph::new(Line::from(vec![
        Span::styled("Progress: ", Style::default().fg(Color::Gray)),
        Span::styled(format!("{:.1}%", app.progress * 100.0), Style::default().fg(Color::Yellow)),
    ]))
    .alignment(Alignment::Center)
    .block(Block::default().borders(Borders::ALL));

    f.render_widget(progress_text, chunks[2]);
}

fn render_input(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(30),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(4),
            Constraint::Percentage(40),
        ])
        .split(area);

    let title = Paragraph::new(Line::from(vec![
        Span::styled("Probabilistic Forecasting", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw("  |  "),
        Span::styled("Input ticker like NVDA / SPY / QQQ", Style::default().fg(Color::Gray)),
    ]))
    .alignment(Alignment::Center)
    .block(Block::default().borders(Borders::ALL).title(" DiffStock "));

    f.render_widget(title, chunks[1]);

    let input = Paragraph::new(app.input.as_str())
        .style(Style::default().fg(Color::Yellow))
        .block(Block::default().borders(Borders::ALL).title(" Enter Stock Symbol "));
    
    f.render_widget(input, chunks[2]);

    if let Some(err) = &app.error_msg {
        let error = Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::ALL).title(" Error "));
        f.render_widget(error, chunks[3]);
    }
}

fn render_loading(f: &mut Frame, msg: &str, area: ratatui::layout::Rect) {
    let block = Block::default().borders(Borders::ALL);
    let text = Paragraph::new(msg)
        .alignment(Alignment::Center)
        .block(block);
    f.render_widget(text, area);
}

fn render_dashboard(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let constraints = if app.error_msg.is_some() {
        vec![Constraint::Min(0), Constraint::Length(3)]
    } else {
        vec![Constraint::Percentage(100)]
    };

    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(constraints)
        .split(area);

    if let Some(data) = &app.stock_data {
        // Split into Chart (Left) and Info (Right)
        let dashboard_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(75), Constraint::Percentage(25)])
            .split(main_chunks[0]);

        let points: Vec<(f64, f64)> = data
            .history
            .iter()
            .enumerate()
            .map(|(i, candle)| (i as f64, candle.close))
            .collect();

        let analysis = data.analyze();
        let x_len = points.len() as f64;
        let forecast_horizon = app.forecast.as_ref().map(|f| f.p50.len()).unwrap_or(0) as f64;
        let x_max = x_len + forecast_horizon + 1.0;

        let mut datasets = vec![Dataset::default()
            .name(data.symbol.as_str())
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Cyan))
            .data(&points)];

        let mut p50_points: Vec<(f64, f64)> = Vec::new();
        let mut p90_points: Vec<(f64, f64)> = Vec::new();
        let mut p70_points: Vec<(f64, f64)> = Vec::new();
        let mut p30_points: Vec<(f64, f64)> = Vec::new();
        let mut p10_points: Vec<(f64, f64)> = Vec::new();

        // Add Technical Levels
        let support_line = vec![(0.0, analysis.support), (x_max, analysis.support)];
        let resistance_line = vec![(0.0, analysis.resistance), (x_max, analysis.resistance)];
        let current_line = vec![(0.0, analysis.current_price), (x_max, analysis.current_price)];

        datasets.push(Dataset::default()
            .name("Support")
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Green))
            .data(&support_line));

        datasets.push(Dataset::default()
            .name("Resistance")
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Red))
            .data(&resistance_line));

        datasets.push(Dataset::default()
            .name("Current")
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::White))
            .data(&current_line));

        // Add Forecast Lines if available
        if let Some(forecast) = &app.forecast {
            let forecast_start = x_len;
            p50_points.extend(
                forecast
                    .p50
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, price))| (forecast_start + idx as f64 + 1.0, *price)),
            );
            p90_points.extend(
                forecast
                    .p90
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, price))| (forecast_start + idx as f64 + 1.0, *price)),
            );
            p70_points.extend(
                forecast
                    .p70
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, price))| (forecast_start + idx as f64 + 1.0, *price)),
            );
            p30_points.extend(
                forecast
                    .p30
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, price))| (forecast_start + idx as f64 + 1.0, *price)),
            );
            p10_points.extend(
                forecast
                    .p10
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, price))| (forecast_start + idx as f64 + 1.0, *price)),
            );

            datasets.push(Dataset::default()
                .name("P50 Forecast")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Yellow))
                .data(&p50_points));
            
            datasets.push(Dataset::default()
                .name("P90 Upper")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::DarkGray))
                .data(&p90_points));

            datasets.push(Dataset::default()
                .name("P70 Upper")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Gray))
                .data(&p70_points));

            datasets.push(Dataset::default()
                .name("P30 Lower")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Gray))
                .data(&p30_points));

            datasets.push(Dataset::default()
                .name("P10 Lower")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::DarkGray))
                .data(&p10_points));
        }

        let min_price = data.history.iter().map(|c| c.close).fold(f64::INFINITY, |a, b| a.min(b));
        let max_price = data.history.iter().map(|c| c.close).fold(f64::NEG_INFINITY, |a, b| a.max(b));
        
        // Adjust bounds for forecast
        let (min_price, max_price) = if let Some(forecast) = &app.forecast {
            let f_min = forecast.p10.iter().map(|(_, p)| *p).fold(f64::INFINITY, |a, b| a.min(b));
            let f_max = forecast.p90.iter().map(|(_, p)| *p).fold(f64::NEG_INFINITY, |a, b| a.max(b));
            (min_price.min(f_min), max_price.max(f_max))
        } else {
            (min_price, max_price)
        };

        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title(Span::styled(
                        format!("{} - Daily Close + Forecast", data.symbol),
                        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                    ))
                    .borders(Borders::ALL),
            )
            .x_axis(
                Axis::default()
                    .title("Days")
                    .style(Style::default().fg(Color::Gray))
                    .bounds([0.0, x_max])
            )
            .y_axis(
                Axis::default()
                    .title("Price")
                    .style(Style::default().fg(Color::Gray))
                    .bounds([min_price * 0.95, max_price * 1.05])
                    .labels(vec![
                        Span::styled(format!("{:.1}", min_price), Style::default().fg(Color::Gray)),
                        Span::styled(format!("{:.1}", max_price), Style::default().fg(Color::Gray)),
                    ]),
            );

        f.render_widget(chart, dashboard_chunks[0]);

        // Render Info Panel
        let mut info_text = vec![
            Line::from(Span::styled("Technical Levels", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
            Line::from(format!("Current: {:.2}", analysis.current_price)),
            Line::from(Span::styled(format!("Resist:  {:.2}", analysis.resistance), Style::default().fg(Color::Red))),
            Line::from(Span::styled(format!("Support: {:.2}", analysis.support), Style::default().fg(Color::Green))),
            Line::from(format!("Pivot:   {:.2}", analysis.pivot)),
            Line::from(""),
        ];

        if let Some(forecast) = &app.forecast {
            let last_p10 = forecast.p10.last().map(|x| x.1).unwrap_or(0.0);
            let last_p30 = forecast.p30.last().map(|x| x.1).unwrap_or(0.0);
            let last_p50 = forecast.p50.last().map(|x| x.1).unwrap_or(0.0);
            let last_p70 = forecast.p70.last().map(|x| x.1).unwrap_or(0.0);
            let last_p90 = forecast.p90.last().map(|x| x.1).unwrap_or(0.0);
            let spread = if last_p10 > 0.0 {
                (last_p90 / last_p10 - 1.0) * 100.0
            } else {
                0.0
            };
            let confidence = if spread < 10.0 {
                ("High", Color::Green)
            } else if spread < 25.0 {
                ("Moderate", Color::Yellow)
            } else {
                ("Wide", Color::Red)
            };

            info_text.push(Line::from(Span::styled("Forecast Targets", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))));
            info_text.push(Line::from(Span::styled(format!("P90: {:.2}", last_p90), Style::default().fg(Color::Green))));
            info_text.push(Line::from(Span::styled(format!("P70: {:.2}", last_p70), Style::default().fg(Color::LightGreen))));
            info_text.push(Line::from(Span::styled(format!("P50: {:.2}", last_p50), Style::default().fg(Color::Yellow))));
            info_text.push(Line::from(Span::styled(format!("P30: {:.2}", last_p30), Style::default().fg(Color::LightRed))));
            info_text.push(Line::from(Span::styled(format!("P10: {:.2}", last_p10), Style::default().fg(Color::Red))));
            info_text.push(Line::from(""));
            info_text.push(Line::from(Span::styled("Forecast Spread", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))));
            info_text.push(Line::from(format!("P90/P10: {:.1}%", spread)));
            info_text.push(Line::from(Span::styled(format!("Confidence: {}", confidence.0), Style::default().fg(confidence.1))));
        }

        let info_block = Paragraph::new(info_text)
            .block(Block::default().borders(Borders::ALL).title("Details"))
            .style(Style::default().fg(Color::White));
        
        f.render_widget(info_block, dashboard_chunks[1]);
    }

    if let Some(err) = &app.error_msg {
        let error = Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::ALL).title("Error"));
        f.render_widget(error, main_chunks[1]);
    }
}

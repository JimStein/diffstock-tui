use ratatui::{
    layout::{Constraint, Direction, Layout, Alignment},
    style::{Color, Style, Modifier},
    symbols,
    text::{Span, Line},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
    Frame,
};
use crate::app::{App, AppState};

pub fn render(f: &mut Frame, app: &App) {
    match app.state {
        AppState::Input => render_input(f, app),
        AppState::Loading => render_loading(f, "Fetching Data..."),
        AppState::Forecasting => render_loading(f, "Running Inference..."),
        AppState::Dashboard => render_dashboard(f, app),
    }
}

fn render_input(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(40),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Percentage(40),
        ])
        .split(f.area());

    let input = Paragraph::new(app.input.as_str())
        .style(Style::default().fg(Color::Yellow))
        .block(Block::default().borders(Borders::ALL).title("Enter Stock Symbol"));
    
    f.render_widget(input, chunks[1]);

    if let Some(err) = &app.error_msg {
        let error = Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::ALL).title("Error"));
        f.render_widget(error, chunks[2]);
    }
}

fn render_loading(f: &mut Frame, msg: &str) {
    let block = Block::default().borders(Borders::ALL);
    let text = Paragraph::new(msg)
        .alignment(Alignment::Center)
        .block(block);
    f.render_widget(text, f.area());
}

fn render_dashboard(f: &mut Frame, app: &App) {
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([Constraint::Percentage(100)].as_ref())
        .split(f.area());

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
        let x_max = x_len + if app.forecast.is_some() { 50.0 } else { 0.0 };

        let mut datasets = vec![Dataset::default()
            .name(data.symbol.as_str())
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Cyan))
            .data(&points)];

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
            datasets.push(Dataset::default()
                .name("P50 Forecast")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Yellow))
                .data(&forecast.p50));
            
            datasets.push(Dataset::default()
                .name("P90 Upper")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::DarkGray))
                .data(&forecast.p90));

            datasets.push(Dataset::default()
                .name("P70 Upper")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Gray))
                .data(&forecast.p70));

            datasets.push(Dataset::default()
                .name("P30 Lower")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Gray))
                .data(&forecast.p30));

            datasets.push(Dataset::default()
                .name("P10 Lower")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::DarkGray))
                .data(&forecast.p10));
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
            Line::from(Span::styled("Analysis", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
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

            info_text.push(Line::from(Span::styled("Forecast (50d)", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))));
            info_text.push(Line::from(Span::styled(format!("P90: {:.2}", last_p90), Style::default().fg(Color::Green))));
            info_text.push(Line::from(Span::styled(format!("P70: {:.2}", last_p70), Style::default().fg(Color::LightGreen))));
            info_text.push(Line::from(Span::styled(format!("P50: {:.2}", last_p50), Style::default().fg(Color::Yellow))));
            info_text.push(Line::from(Span::styled(format!("P30: {:.2}", last_p30), Style::default().fg(Color::LightRed))));
            info_text.push(Line::from(Span::styled(format!("P10: {:.2}", last_p10), Style::default().fg(Color::Red))));
        }

        let info_block = Paragraph::new(info_text)
            .block(Block::default().borders(Borders::ALL).title("Details"))
            .style(Style::default().fg(Color::White));
        
        f.render_widget(info_block, dashboard_chunks[1]);
    }
}

use chrono::{DateTime, Duration, Utc, TimeZone};
use rand::prelude::*;
use serde::Deserialize;
use anyhow::Result;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct Candle {
    pub date: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Debug)]
pub struct StockData {
    pub symbol: String,
    pub history: Vec<Candle>,
}

#[derive(Deserialize, Debug)]
struct YahooChartResponse {
    chart: YahooChart,
}

#[derive(Deserialize, Debug)]
struct YahooChart {
    result: Vec<YahooResult>,
}

#[derive(Deserialize, Debug)]
struct YahooResult {
    timestamp: Vec<i64>,
    indicators: YahooIndicators,
}

#[derive(Deserialize, Debug)]
struct YahooIndicators {
    quote: Vec<YahooQuote>,
}

#[derive(Deserialize, Debug)]
struct YahooQuote {
    open: Vec<Option<f64>>,
    high: Vec<Option<f64>>,
    low: Vec<Option<f64>>,
    close: Vec<Option<f64>>,
    volume: Vec<Option<f64>>,
}

impl StockData {
    pub async fn fetch(symbol: &str) -> Result<Self> {
        let url = format!(
            "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&range=1y",
            symbol
        );

        let resp = reqwest::Client::new()
            .get(&url)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await?
            .json::<YahooChartResponse>()
            .await?;

        let result = resp.chart.result.first().ok_or(anyhow::anyhow!("No data found"))?;
        let quote = result.indicators.quote.first().ok_or(anyhow::anyhow!("No quotes found"))?;
        
        let mut history = Vec::new();
        
        for (i, &timestamp) in result.timestamp.iter().enumerate() {
            if let (Some(open), Some(high), Some(low), Some(close), Some(volume)) = (
                quote.open.get(i).and_then(|v| *v),
                quote.high.get(i).and_then(|v| *v),
                quote.low.get(i).and_then(|v| *v),
                quote.close.get(i).and_then(|v| *v),
                quote.volume.get(i).and_then(|v| *v),
            ) {
                history.push(Candle {
                    date: Utc.timestamp_opt(timestamp, 0).unwrap(),
                    open,
                    high,
                    low,
                    close,
                    volume,
                });
            }
        }

        Ok(Self {
            symbol: symbol.to_string(),
            history,
        })
    }

    pub fn log_returns(&self) -> Vec<f64> {
        self.history
            .windows(2)
            .map(|w| (w[1].close / w[0].close).ln())
            .collect()
    }

    pub fn stats(&self) -> (f64, f64) {
        let returns = self.log_returns();
        let n = returns.len() as f64;
        if n == 0.0 { return (0.0, 0.0); }

        let mean = returns.iter().sum::<f64>() / n;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        
        (mean, variance.sqrt())
    }

    pub fn analyze(&self) -> Analysis {
        let last = self.history.last().unwrap();
        let current_price = last.close;
        let pivot = (last.high + last.low + last.close) / 3.0;
        
        let support = self.history.iter().map(|c| c.low).fold(f64::INFINITY, |a, b| a.min(b));
        let resistance = self.history.iter().map(|c| c.high).fold(f64::NEG_INFINITY, |a, b| a.max(b));

        Analysis { current_price, support, resistance, pivot }
    }

    #[allow(dead_code)]
    pub fn new_mock(symbol: &str, days: usize) -> Self {
        let mut rng = rand::thread_rng();
        let mut history = Vec::with_capacity(days);
        let mut current_price: f64 = 100.0;
        let mut current_date = Utc::now() - Duration::days(days as i64);

        for _ in 0..days {
            let volatility = 0.02; // 2% daily volatility
            let change_pct: f64 = rng.gen_range(-volatility..volatility);
            let open = current_price;
            let close = open * (1.0 + change_pct);
            let high = open.max(close) * (1.0 + rng.gen_range(0.0..0.01));
            let low = open.min(close) * (1.0 - rng.gen_range(0.0..0.01));
            let volume = rng.gen_range(1000.0..10000.0);

            history.push(Candle {
                date: current_date,
                open,
                high,
                low,
                close,
                volume,
            });

            current_price = close;
            current_date = current_date + Duration::days(1);
        }

        Self {
            symbol: symbol.to_string(),
            history,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Analysis {
    pub current_price: f64,
    pub support: f64,
    pub resistance: f64,
    pub pivot: f64,
}

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

pub async fn fetch_range(symbol: &str, range: &str) -> Result<StockData> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?range={}&interval=1d",
        symbol, range
    );
    
    let resp = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await?
        .json::<YahooChartResponse>()
        .await?;
    let result = resp.chart.result.first().ok_or(anyhow::anyhow!("No data found"))?;
    
    let mut history = Vec::new();
    let quotes = &result.indicators.quote[0];
    
    for (i, &timestamp) in result.timestamp.iter().enumerate() {
        if let (Some(open), Some(high), Some(low), Some(close), Some(volume)) = (
            quotes.open[i],
            quotes.high[i],
            quotes.low[i],
            quotes.close[i],
            quotes.volume[i],
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
    
    Ok(StockData {
        symbol: symbol.to_string(),
        history,
    })
}

impl StockData {
    pub async fn fetch(symbol: &str) -> Result<Self> {
        Self::fetch_range(symbol, "1y").await
    }

    pub async fn fetch_range(symbol: &str, range: &str) -> Result<Self> {
        let url = format!(
            "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&range={}",
            symbol, range
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

    #[allow(dead_code)]
    pub fn log_returns(&self) -> Vec<f64> {
        self.history
            .windows(2)
            .map(|w| (w[1].close / w[0].close).ln())
            .collect()
    }

    #[allow(dead_code)]
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

pub struct TrainingDataset {
    pub features: Vec<Vec<f64>>, // [seq_len, 2] (Close Return, Overnight Return)
    pub targets: Vec<Vec<f64>>,  // [forecast_len, 1] (Close Return)
}

impl StockData {
    pub fn prepare_training_data(&self, lookback: usize, forecast: usize) -> TrainingDataset {
        let mut features = Vec::new();
        let mut targets = Vec::new();

        // Calculate returns
        // We need at least lookback + forecast + 1 data points
        if self.history.len() < lookback + forecast + 1 {
            return TrainingDataset { features, targets };
        }
        
        let mut all_close_returns = Vec::with_capacity(self.history.len());
        let mut all_overnight_returns = Vec::with_capacity(self.history.len());

        for i in 1..self.history.len() {
            let close_ret = (self.history[i].close / self.history[i-1].close).ln();
            let overnight_ret = (self.history[i].open / self.history[i-1].close).ln();
            
            all_close_returns.push(close_ret);
            all_overnight_returns.push(overnight_ret);
        }

        // Create sliding windows
        let total_returns = all_close_returns.len();
        if total_returns < lookback + forecast {
             return TrainingDataset { features, targets };
        }

        for j in 0..total_returns - lookback - forecast {
            let mut window_features = Vec::with_capacity(lookback);
            for k in 0..lookback {
                window_features.push(vec![
                    all_close_returns[j+k],
                    all_overnight_returns[j+k]
                ]);
            }

            let mut window_targets = Vec::with_capacity(forecast);
            for k in 0..forecast {
                window_targets.push(all_close_returns[j+lookback+k]);
            }
            
            // Z-Score Normalization per window
            let close_vals: Vec<f64> = window_features.iter().map(|f| f[0]).collect();
            let mean = close_vals.iter().sum::<f64>() / lookback as f64;
            let variance = close_vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (lookback as f64 - 1.0);
            let std = variance.sqrt() + 1e-6;

            let normalized_features: Vec<f64> = window_features.iter().flat_map(|f| {
                vec![
                    (f[0] - mean) / std,
                    (f[1] - mean) / std
                ]
            }).collect();

            let normalized_targets: Vec<f64> = window_targets.iter().map(|t| (t - mean) / std).collect();

            features.push(normalized_features);
            targets.push(normalized_targets);
        }

        TrainingDataset { features, targets }
    }
}

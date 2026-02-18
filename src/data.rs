use chrono::{DateTime, Duration, Utc, TimeZone};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use tracing::{info, warn};

/// Represents a single candlestick data point (OHLCV).
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

/// Holds historical stock data for a specific symbol.
#[derive(Clone, Debug)]
pub struct StockData {
    pub symbol: String,
    pub history: Vec<Candle>,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooChartResponse {
    chart: YahooChart,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooChart {
    result: Vec<YahooResult>,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooResult {
    timestamp: Vec<i64>,
    indicators: YahooIndicators,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooIndicators {
    quote: Vec<YahooQuote>,
}

#[derive(Deserialize, Serialize, Debug)]
struct YahooQuote {
    open: Vec<Option<f64>>,
    high: Vec<Option<f64>>,
    low: Vec<Option<f64>>,
    close: Vec<Option<f64>>,
    volume: Vec<Option<f64>>,
}

/// Fetches historical stock data from Yahoo Finance.
///
/// # Arguments
/// * `symbol` - The stock ticker symbol (e.g., "AAPL").
/// * `range` - The time range to fetch (e.g., "1y", "5y").
pub async fn fetch_range(symbol: &str, range: &str) -> Result<StockData> {
    let cache_dir = std::path::Path::new(".cache");
    if !cache_dir.exists() {
        std::fs::create_dir(cache_dir)?;
    }
    
    let cache_file = cache_dir.join(format!("{}_{}.json", symbol, range));
    
    let response: YahooChartResponse = if cache_file.exists() {
        // Check if cache is fresh (e.g. < 24 hours)
        let metadata = std::fs::metadata(&cache_file)?;
        let modified = metadata.modified()?;
        let age = std::time::SystemTime::now().duration_since(modified)?;
        
        if age.as_secs() < 86400 {
            info!("Loading {} from cache...", symbol);
            let file = std::fs::File::open(&cache_file)?;
            let reader = std::io::BufReader::new(file);
            serde_json::from_reader(reader)?
        } else {
            info!("Cache expired for {}, fetching...", symbol);
            fetch_from_api(symbol, range, &cache_file).await?
        }
    } else {
        info!("Cache miss for {}, fetching...", symbol);
        fetch_from_api(symbol, range, &cache_file).await?
    };

    let result = response.chart.result.first().ok_or(anyhow::anyhow!("No data found"))?;
    
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
        symbol: symbol.to_uppercase(),
        history,
    })
}

async fn fetch_from_api(symbol: &str, range: &str, cache_path: &std::path::Path) -> Result<YahooChartResponse> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?range={}&interval=1d",
        symbol, range
    );
    
    let mut attempts = 0;
    let max_attempts = 3;
    
    loop {
        attempts += 1;
        match reqwest::Client::new()
            .get(&url)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await 
        {
            Ok(resp) => {
                match resp.json::<YahooChartResponse>().await {
                    Ok(resp_json) => {
                        // Save to cache
                        let file = std::fs::File::create(cache_path)?;
                        let writer = std::io::BufWriter::new(file);
                        serde_json::to_writer(writer, &resp_json)?;
                        
                        return Ok(resp_json);
                    }
                    Err(e) => {
                        if attempts >= max_attempts {
                            return Err(e.into());
                        }
                        warn!("Failed to parse JSON for {} (attempt {}/{}): {}", symbol, attempts, max_attempts, e);
                    }
                }
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(e.into());
                }
                warn!("Failed to fetch data for {} (attempt {}/{}): {}", symbol, attempts, max_attempts, e);
            }
        }
        
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Fetches the latest 1-minute close price from Yahoo Finance.
///
/// Uses `interval=1m` and `range=1d`, then returns the most recent non-null close.
pub async fn fetch_latest_price_1m(symbol: &str) -> Result<f64> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1m&range=1d",
        symbol
    );

    let response = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await?
        .json::<YahooChartResponse>()
        .await?;

    let result = response
        .chart
        .result
        .first()
        .ok_or(anyhow::anyhow!("No chart result for {}", symbol))?;
    let quote = result
        .indicators
        .quote
        .first()
        .ok_or(anyhow::anyhow!("No quote result for {}", symbol))?;

    let latest = quote
        .close
        .iter()
        .rev()
        .find_map(|value| *value)
        .ok_or(anyhow::anyhow!("No valid 1m close for {}", symbol))?;

    Ok(latest)
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
            symbol: symbol.to_uppercase(),
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
            current_date += Duration::days(1);
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
    pub asset_ids: Vec<usize>,   // [1] Asset ID for each sample
}

impl TrainingDataset {
    pub fn split(self, train_ratio: f64) -> (Self, Self) {
        let n = self.features.len();
        let train_size = (n as f64 * train_ratio) as usize;
        
        let (train_features, val_features) = self.features.split_at(train_size);
        let (train_targets, val_targets) = self.targets.split_at(train_size);
        let (train_ids, val_ids) = self.asset_ids.split_at(train_size);
        
        (
            Self {
                features: train_features.to_vec(),
                targets: train_targets.to_vec(),
                asset_ids: train_ids.to_vec(),
            },
            Self {
                features: val_features.to_vec(),
                targets: val_targets.to_vec(),
                asset_ids: val_ids.to_vec(),
            }
        )
    }
}

impl StockData {
    /// Prepares sliding window datasets for training the diffusion model.
    ///
    /// # Arguments
    /// * `lookback` - Number of past days to use as input context.
    /// * `forecast` - Number of future days to predict.
    /// * `asset_id` - Unique identifier for the asset.
    pub fn prepare_training_data(&self, lookback: usize, forecast: usize, asset_id: usize) -> TrainingDataset {
        let mut features = Vec::new();
        let mut targets = Vec::new();
        let mut asset_ids = Vec::new();

        // Calculate returns
        // We need at least lookback + forecast + 1 data points
        if self.history.len() < lookback + forecast + 1 {
            return TrainingDataset { features, targets, asset_ids };
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
             return TrainingDataset { features, targets, asset_ids };
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
            asset_ids.push(asset_id);
        }

        TrainingDataset { features, targets, asset_ids }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_training_data() {
        let mock_data = StockData::new_mock("TEST", 100);
        let lookback = 10;
        let forecast = 5;
        let asset_id = 0;
        
        let dataset = mock_data.prepare_training_data(lookback, forecast, asset_id);
        
        // Check if we have data
        assert!(!dataset.features.is_empty());
        assert!(!dataset.targets.is_empty());
        assert!(!dataset.asset_ids.is_empty());
        assert_eq!(dataset.features.len(), dataset.targets.len());
        assert_eq!(dataset.features.len(), dataset.asset_ids.len());
        assert_eq!(dataset.asset_ids[0], asset_id);
        
        // Check dimensions
        let first_feature = &dataset.features[0];
        assert_eq!(first_feature.len(), lookback * 2); // 2 features per step
        
        let first_target = &dataset.targets[0];
        assert_eq!(first_target.len(), forecast);
        
        // Check normalization (mean should be close to 0, std close to 1)
        // This is per-window normalization, so we check one window
        let close_vals: Vec<f64> = first_feature.iter().step_by(2).cloned().collect();
        let _mean = close_vals.iter().sum::<f64>() / close_vals.len() as f64;
        // Since we normalized, the mean of the *original* window was subtracted.
        // The values in `first_feature` are already normalized.
        // So their mean should be ~0 and std ~1.
        
        let _feat_mean = first_feature.iter().sum::<f64>() / first_feature.len() as f64;
        // Note: we normalize close and overnight returns together? 
        // In prepare_training_data:
        // let normalized_features: Vec<f64> = window_features.iter().flat_map(|f| {
        //     vec![
        //         (f[0] - mean) / std,
        //         (f[1] - mean) / std
        //     ]
        // }).collect();
        // We use the same mean/std (calculated from close returns) for both features.
        // So the mean of the normalized close returns should be 0.
        
        let norm_close_vals: Vec<f64> = first_feature.iter().step_by(2).cloned().collect();
        let norm_mean = norm_close_vals.iter().sum::<f64>() / norm_close_vals.len() as f64;
        
        assert!(norm_mean.abs() < 1e-5);
    }
}

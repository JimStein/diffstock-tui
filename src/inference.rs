use crate::data::StockData;
use anyhow::Result;
use rand_distr::{Distribution, Normal};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ForecastData {
    pub p10: Vec<(f64, f64)>, // (time, price)
    pub p30: Vec<(f64, f64)>,
    pub p50: Vec<(f64, f64)>,
    pub p70: Vec<(f64, f64)>,
    pub p90: Vec<(f64, f64)>,
    pub _paths: Vec<Vec<f64>>, // Raw paths for potential detailed inspection
}

pub async fn run_inference(data: Arc<StockData>, horizon: usize, num_simulations: usize) -> Result<ForecastData> {
    // In a real implementation, this would load a Candle/Torch model.
    // Here we simulate the "Diffusion" process using Geometric Brownian Motion (GBM)
    // which is the theoretical limit of a diffusion process for stock prices.
    
    let (mu, sigma) = data.stats();
    let last_price = data.history.last().map(|c| c.close).unwrap_or(100.0);
    let start_idx = data.history.len() as f64;

    let mut rng = rand::thread_rng();
    let normal = Normal::new(0.0, 1.0).unwrap();

    let mut all_paths = Vec::with_capacity(num_simulations);

    for _ in 0..num_simulations {
        let mut path = Vec::with_capacity(horizon);
        let mut current_price = last_price;

        for _ in 0..horizon {
            // GBM: P_t = P_{t-1} * exp((mu - 0.5*sigma^2) + sigma * Z)
            // We use the calculated log-return stats directly
            let drift = mu; // mu of log returns includes the drift component
            let shock = normal.sample(&mut rng) * sigma;
            
            let log_ret = drift + shock;
            current_price *= log_ret.exp();
            path.push(current_price);
        }
        all_paths.push(path);
    }

    // Calculate Percentiles
    let mut p10 = Vec::with_capacity(horizon);
    let mut p30 = Vec::with_capacity(horizon);
    let mut p50 = Vec::with_capacity(horizon);
    let mut p70 = Vec::with_capacity(horizon);
    let mut p90 = Vec::with_capacity(horizon);

    for t in 0..horizon {
        let mut time_slice: Vec<f64> = all_paths.iter().map(|p| p[t]).collect();
        time_slice.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let idx_10 = (num_simulations as f64 * 0.1) as usize;
        let idx_30 = (num_simulations as f64 * 0.3) as usize;
        let idx_50 = (num_simulations as f64 * 0.5) as usize;
        let idx_70 = (num_simulations as f64 * 0.7) as usize;
        let idx_90 = (num_simulations as f64 * 0.9) as usize;

        let time_point = start_idx + (t as f64);
        p10.push((time_point, time_slice[idx_10]));
        p30.push((time_point, time_slice[idx_30]));
        p50.push((time_point, time_slice[idx_50]));
        p70.push((time_point, time_slice[idx_70]));
        p90.push((time_point, time_slice[idx_90]));
    }

    Ok(ForecastData {
        p10,
        p30,
        p50,
        p70,
        p90,
        _paths: all_paths,
    })
}

#![allow(dead_code)]

use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::data::{Candle, StockData, TrainingDataset};

const TRADING_DAYS_PER_YEAR: f64 = 252.0;
const EPSILON: f64 = 1e-9;
pub const FEATURE_MANIFEST_PATH: &str = "model_features.json";

#[derive(Clone, Debug)]
pub struct FeatureEngineConfig {
    pub return_windows: Vec<usize>,
    pub ma_windows: Vec<usize>,
    pub volatility_windows: Vec<usize>,
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub atr_window: usize,
    pub rsi_window: usize,
    pub stochastic_window: usize,
    pub stochastic_smoothing: usize,
    pub cci_window: usize,
    pub williams_window: usize,
    pub bollinger_window: usize,
    pub bollinger_std_mult: f64,
    pub volume_window: usize,
    pub parkinson_window: usize,
    pub slope_lookback: usize,
    pub include_vwap_features: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeatureSelectionProfile {
    Full,
    Core,
    Trend,
    Custom,
}

impl FeatureSelectionProfile {
    fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "full" => Self::Full,
            "core" => Self::Core,
            "trend" => Self::Trend,
            "custom" => Self::Custom,
            _ => Self::Full,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FeatureSelectionConfig {
    pub profile: FeatureSelectionProfile,
    pub groups: Vec<String>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub target_feature: String,
}

pub fn recommended_training_feature_names() -> Vec<String> {
    [
        "log_return_1d",
        "ret_5d",
        "ret_20d",
        "excess_ret_5d_vs_benchmark",
        "excess_ret_20d_vs_benchmark",
        "price_to_ema_20",
        "macd_histogram",
        "rolling_std_20",
        "volume_ratio_20",
        "close_location_value",
    ]
    .into_iter()
    .map(|name| name.to_string())
    .collect()
}

impl Default for FeatureSelectionConfig {
    fn default() -> Self {
        Self {
            profile: FeatureSelectionProfile::Custom,
            groups: Vec::new(),
            include: recommended_training_feature_names(),
            exclude: Vec::new(),
            target_feature: "log_return_1d".to_string(),
        }
    }
}

impl FeatureSelectionConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        let profile = std::env::var("DIFFSTOCK_FEATURE_PROFILE")
            .map(|value| FeatureSelectionProfile::from_env_value(&value))
            .unwrap_or(default.profile);
        let groups = parse_env_csv("DIFFSTOCK_FEATURE_GROUPS");
        let include = match std::env::var("DIFFSTOCK_FEATURE_INCLUDE") {
            Ok(value) => parse_csv_list(&value),
            Err(_) if profile == FeatureSelectionProfile::Custom => default.include,
            Err(_) => Vec::new(),
        };
        let exclude = parse_env_csv("DIFFSTOCK_FEATURE_EXCLUDE");
        let target_feature = std::env::var("DIFFSTOCK_TARGET_FEATURE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or(default.target_feature);

        Self {
            profile,
            groups,
            include,
            exclude,
            target_feature,
        }
    }
}

impl Default for FeatureEngineConfig {
    fn default() -> Self {
        Self {
            return_windows: vec![1, 3, 5, 10, 20],
            ma_windows: vec![5, 10, 20],
            volatility_windows: vec![5, 10, 20],
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            atr_window: 14,
            rsi_window: 14,
            stochastic_window: 14,
            stochastic_smoothing: 3,
            cci_window: 20,
            williams_window: 14,
            bollinger_window: 20,
            bollinger_std_mult: 2.0,
            volume_window: 20,
            parkinson_window: 20,
            slope_lookback: 5,
            include_vwap_features: false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SymbolSupplementalData {
    pub daily_vwap: HashMap<NaiveDate, f64>,
}

#[derive(Clone, Debug)]
pub struct FeatureRow {
    pub date: DateTime<Utc>,
    pub values: Vec<f64>,
}

#[derive(Clone, Debug)]
pub struct FeatureMatrix {
    pub symbol: String,
    pub feature_names: Vec<String>,
    pub rows: Vec<FeatureRow>,
}

impl FeatureMatrix {
    pub fn feature_index(&self, name: &str) -> Option<usize> {
        self.feature_names.iter().position(|candidate| candidate == name)
    }
}

#[derive(Clone, Debug)]
pub struct MultiAssetFeatureStore {
    pub feature_names: Vec<String>,
    pub benchmark_symbol: Option<String>,
    pub by_symbol: HashMap<String, FeatureMatrix>,
}

impl MultiAssetFeatureStore {
    pub fn feature_index(&self, name: &str) -> Option<usize> {
        self.feature_names.iter().position(|candidate| candidate == name)
    }

    pub fn select_features(&self, selection: &FeatureSelectionConfig) -> Result<Self> {
        let selected_names = select_feature_names(&self.feature_names, selection)?;
        self.select_exact_features(&selected_names)
    }

    pub fn select_exact_features(&self, selected_names: &[String]) -> Result<Self> {
        let selected_indices = selected_names
            .iter()
            .map(|name| {
                self.feature_index(name)
                    .ok_or_else(|| anyhow!("selected feature {} not found in feature store", name))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut by_symbol = HashMap::with_capacity(self.by_symbol.len());
        for (symbol, matrix) in &self.by_symbol {
            let rows = matrix
                .rows
                .iter()
                .map(|row| FeatureRow {
                    date: row.date,
                    values: selected_indices.iter().map(|&idx| row.values[idx]).collect(),
                })
                .collect::<Vec<_>>();

            by_symbol.insert(
                symbol.clone(),
                FeatureMatrix {
                    symbol: matrix.symbol.clone(),
                    feature_names: selected_names.to_vec(),
                    rows,
                },
            );
        }

        Ok(Self {
            feature_names: selected_names.to_vec(),
            benchmark_symbol: self.benchmark_symbol.clone(),
            by_symbol,
        })
    }
}

impl FeatureManifest {
    pub fn from_store(
        store: &MultiAssetFeatureStore,
        selection: &FeatureSelectionConfig,
        lookback: usize,
        forecast: usize,
    ) -> Self {
        Self {
            version: 1,
            feature_names: store.feature_names.clone(),
            target_feature: selection.target_feature.clone(),
            benchmark_symbol: store.benchmark_symbol.clone(),
            lookback,
            forecast,
            input_dim: store.feature_names.len(),
        }
    }
}

pub fn save_feature_manifest(manifest: &FeatureManifest) -> Result<std::path::PathBuf> {
    let path = crate::config::project_file_path(FEATURE_MANIFEST_PATH);
    let file = std::fs::File::create(&path)?;
    let writer = std::io::BufWriter::new(file);
    serde_json::to_writer_pretty(writer, manifest)?;
    Ok(path)
}

pub fn load_feature_manifest() -> Result<FeatureManifest> {
    let path = crate::config::project_file_path(FEATURE_MANIFEST_PATH);
    let file = std::fs::File::open(&path)?;
    let reader = std::io::BufReader::new(file);
    Ok(serde_json::from_reader(reader)?)
}

pub fn load_feature_manifest_if_exists() -> Option<FeatureManifest> {
    let path = crate::config::project_file_path(FEATURE_MANIFEST_PATH);
    if !path.exists() {
        return None;
    }
    let file = std::fs::File::open(path).ok()?;
    let reader = std::io::BufReader::new(file);
    serde_json::from_reader(reader).ok()
}

pub fn configured_feature_benchmark_symbol() -> Option<String> {
    std::env::var("DIFFSTOCK_FEATURE_BENCHMARK")
        .ok()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .or_else(|| Some("QQQ".to_string()))
}

pub fn select_feature_names(
    available_feature_names: &[String],
    selection: &FeatureSelectionConfig,
) -> Result<Vec<String>> {
    let available_set = available_feature_names.iter().cloned().collect::<HashSet<_>>();
    let group_map = build_feature_group_map(available_feature_names);

    let mut selected = match selection.profile {
        FeatureSelectionProfile::Full => available_feature_names.to_vec(),
        FeatureSelectionProfile::Core => feature_names_for_groups(
            available_feature_names,
            &group_map,
            &["returns", "benchmark", "trend", "volatility", "volume", "candles"],
        ),
        FeatureSelectionProfile::Trend => feature_names_for_groups(
            available_feature_names,
            &group_map,
            &["returns", "benchmark", "trend", "volatility"],
        ),
        FeatureSelectionProfile::Custom => Vec::new(),
    };

    if !selection.groups.is_empty() {
        let group_refs = selection
            .groups
            .iter()
            .map(|group| group.as_str())
            .collect::<Vec<_>>();
        let mut extra = feature_names_for_groups(available_feature_names, &group_map, &group_refs);
        selected.append(&mut extra);
    }

    for name in &selection.include {
        if name.eq_ignore_ascii_case("full") {
            selected.extend(available_feature_names.iter().cloned());
            continue;
        }
        if !available_set.contains(name) {
            return Err(anyhow!("feature include {} not found", name));
        }
        selected.push(name.clone());
    }

    let exclude_set = selection.exclude.iter().cloned().collect::<HashSet<_>>();
    for name in &exclude_set {
        if !available_set.contains(name) {
            return Err(anyhow!("feature exclude {} not found", name));
        }
    }

    let mut seen = HashSet::new();
    let selected = selected
        .into_iter()
        .filter(|name| !exclude_set.contains(name))
        .filter(|name| seen.insert(name.clone()))
        .collect::<Vec<_>>();

    if selected.is_empty() {
        return Err(anyhow!(
            "feature selection resolved to zero features; check DIFFSTOCK_FEATURE_PROFILE / GROUPS / INCLUDE / EXCLUDE"
        ));
    }

    if !selected.iter().any(|name| name == &selection.target_feature) {
        return Err(anyhow!(
            "target feature {} is not present in selected feature set",
            selection.target_feature
        ));
    }

    Ok(selected)
}

pub fn build_selected_feature_store(
    assets: &HashMap<String, StockData>,
    selection: &FeatureSelectionConfig,
    config: &FeatureEngineConfig,
    benchmark_symbol: Option<&str>,
) -> Result<MultiAssetFeatureStore> {
    let full_store = build_multi_asset_feature_store(assets, benchmark_symbol, None, config)?;
    if selection.profile == FeatureSelectionProfile::Full
        && selection.groups.is_empty()
        && selection.include.is_empty()
        && selection.exclude.is_empty()
    {
        return Ok(full_store);
    }
    full_store.select_features(selection)
}

pub fn build_training_dataset_bundle(
    assets: &HashMap<String, StockData>,
    selection: &FeatureSelectionConfig,
    config: &FeatureEngineConfig,
    benchmark_symbol: Option<&str>,
    lookback: usize,
    forecast: usize,
    asset_ids: &HashMap<String, usize>,
) -> Result<(TrainingDataset, FeatureManifest)> {
    let selected_store = build_selected_feature_store(assets, selection, config, benchmark_symbol)?;
    let manifest = FeatureManifest::from_store(&selected_store, selection, lookback, forecast);
    let dataset = build_training_dataset_from_features(
        &selected_store,
        lookback,
        forecast,
        &manifest.target_feature,
        asset_ids,
    )?;
    Ok((dataset, manifest))
}

pub fn prepare_latest_feature_context(
    assets: &HashMap<String, StockData>,
    symbol: &str,
    manifest: &FeatureManifest,
) -> Result<PreparedFeatureContext> {
    let store = build_multi_asset_feature_store(
        assets,
        manifest.benchmark_symbol.as_deref(),
        None,
        &FeatureEngineConfig::default(),
    )?
    .select_exact_features(&manifest.feature_names)?;

    let matrix = store
        .by_symbol
        .get(&symbol.trim().to_ascii_uppercase())
        .ok_or_else(|| anyhow!("feature context missing symbol {}", symbol))?;

    if matrix.rows.len() < manifest.lookback {
        return Err(anyhow!(
            "not enough feature rows for {} (need {}, got {})",
            symbol,
            manifest.lookback,
            matrix.rows.len()
        ));
    }

    let target_idx = matrix
        .feature_index(&manifest.target_feature)
        .ok_or_else(|| anyhow!("target feature {} not found", manifest.target_feature))?;
    let window = &matrix.rows[matrix.rows.len() - manifest.lookback..];
    let input_dim = matrix.feature_names.len();
    let mut column_means = vec![0.0; input_dim];
    let mut column_stds = vec![0.0; input_dim];

    for col in 0..input_dim {
        let column_values = window.iter().map(|row| row.values[col]).collect::<Vec<_>>();
        column_means[col] = mean(&column_values);
        column_stds[col] = sample_std(&column_values).max(1e-6);
    }

    let mut normalized_features = Vec::with_capacity(manifest.lookback * input_dim);
    for row in window {
        for col in 0..input_dim {
            normalized_features.push(((row.values[col] - column_means[col]) / column_stds[col]) as f32);
        }
    }

    Ok(PreparedFeatureContext {
        normalized_features,
        input_dim,
        target_mean: column_means[target_idx],
        target_std: column_stds[target_idx],
    })
}

pub fn build_multi_asset_feature_store(
    assets: &HashMap<String, StockData>,
    benchmark_symbol: Option<&str>,
    supplemental: Option<&HashMap<String, SymbolSupplementalData>>,
    config: &FeatureEngineConfig,
) -> Result<MultiAssetFeatureStore> {
    if assets.is_empty() {
        return Err(anyhow!("feature engine requires at least one asset"));
    }

    let benchmark_upper = benchmark_symbol.map(|symbol| symbol.trim().to_uppercase());
    let benchmark_data = benchmark_upper
        .as_ref()
        .map(|symbol| {
            assets
                .get(symbol)
                .ok_or_else(|| anyhow!("benchmark symbol {} not found in asset map", symbol))
        })
        .transpose()?;

    let include_vwap_features = config.include_vwap_features
        && supplemental
            .map(|map| map.values().any(|entry| !entry.daily_vwap.is_empty()))
            .unwrap_or(false);

    let feature_names = build_feature_names(config, benchmark_data.is_some(), include_vwap_features);
    let benchmark_lookup = benchmark_data.map(build_benchmark_lookup);
    let mut by_symbol = HashMap::with_capacity(assets.len());

    for (symbol, data) in assets {
        let symbol_upper = symbol.trim().to_uppercase();
        let supplemental_data = supplemental.and_then(|map| map.get(&symbol_upper));
        let matrix = build_feature_matrix_for_symbol(
            &symbol_upper,
            data,
            benchmark_data,
            benchmark_lookup.as_ref(),
            supplemental_data,
            config,
            &feature_names,
            include_vwap_features,
        )?;
        by_symbol.insert(symbol_upper, matrix);
    }

    Ok(MultiAssetFeatureStore {
        feature_names,
        benchmark_symbol: benchmark_upper,
        by_symbol,
    })
}

pub fn build_training_dataset_from_features(
    store: &MultiAssetFeatureStore,
    lookback: usize,
    forecast: usize,
    target_feature: &str,
    asset_ids: &HashMap<String, usize>,
) -> Result<TrainingDataset> {
    if lookback == 0 || forecast == 0 {
        return Err(anyhow!("lookback and forecast must both be > 0"));
    }

    let target_idx = store
        .feature_index(target_feature)
        .ok_or_else(|| anyhow!("target feature {} not found", target_feature))?;

    let input_dim = store.feature_names.len();
    let mut features = Vec::new();
    let mut targets = Vec::new();
    let mut resolved_asset_ids = Vec::new();

    for (symbol, matrix) in &store.by_symbol {
        let Some(&asset_id) = asset_ids.get(symbol) else {
            continue;
        };
        if matrix.rows.len() < lookback + forecast {
            continue;
        }

        for start in 0..=(matrix.rows.len() - lookback - forecast) {
            let feature_window = &matrix.rows[start..start + lookback];
            let target_window = &matrix.rows[start + lookback..start + lookback + forecast];

            let mut column_means = vec![0.0; input_dim];
            let mut column_stds = vec![0.0; input_dim];

            for col in 0..input_dim {
                let column_values: Vec<f64> = feature_window.iter().map(|row| row.values[col]).collect();
                column_means[col] = mean(&column_values);
                column_stds[col] = sample_std(&column_values).max(1e-6);
            }

            let mut normalized_features = Vec::with_capacity(lookback * input_dim);
            for row in feature_window {
                for col in 0..input_dim {
                    normalized_features.push((row.values[col] - column_means[col]) / column_stds[col]);
                }
            }

            let target_mean = column_means[target_idx];
            let target_std = column_stds[target_idx].max(1e-6);
            let normalized_targets = target_window
                .iter()
                .map(|row| (row.values[target_idx] - target_mean) / target_std)
                .collect::<Vec<_>>();

            features.push(normalized_features);
            targets.push(normalized_targets);
            resolved_asset_ids.push(asset_id);
        }
    }

    Ok(TrainingDataset {
        features,
        targets,
        asset_ids: resolved_asset_ids,
    })
}

fn build_benchmark_lookup(data: &StockData) -> HashMap<NaiveDate, usize> {
    let mut lookup = HashMap::with_capacity(data.history.len());
    for (idx, candle) in data.history.iter().enumerate() {
        lookup.insert(candle.date.date_naive(), idx);
    }
    lookup
}

fn build_feature_names(
    config: &FeatureEngineConfig,
    has_benchmark: bool,
    include_vwap_features: bool,
) -> Vec<String> {
    let mut names = Vec::new();

    for &window in &config.return_windows {
        names.push(format!("ret_{}d", window));
    }
    names.push("log_return_1d".to_string());
    names.push("cumulative_return".to_string());
    if has_benchmark {
        for &window in &config.return_windows {
            names.push(format!("excess_ret_{}d_vs_benchmark", window));
        }
    }

    for &window in &config.ma_windows {
        names.push(format!("sma_{}", window));
        names.push(format!("ema_{}", window));
        names.push(format!("sma_slope_{}", window));
        names.push(format!("ema_slope_{}", window));
        names.push(format!("price_to_sma_{}", window));
        names.push(format!("price_to_ema_{}", window));
    }
    if config.ma_windows.len() >= 2 {
        let fast = config.ma_windows[0];
        let slow = *config.ma_windows.last().unwrap_or(&config.ma_windows[0]);
        names.push(format!("sma_spread_{}_{}", fast, slow));
    }
    names.push(format!("ema_spread_{}_{}", config.macd_fast, config.macd_slow));
    names.push("macd_line".to_string());
    names.push("macd_signal".to_string());
    names.push("macd_histogram".to_string());

    for &window in &config.volatility_windows {
        names.push(format!("rolling_std_{}", window));
        names.push(format!("realized_vol_{}", window));
    }
    names.push(format!("atr_{}", config.atr_window));
    names.push("true_range".to_string());
    names.push(format!("parkinson_vol_{}", config.parkinson_window));
    names.push("high_low_range_pct".to_string());

    names.push(format!("rsi_{}", config.rsi_window));
    names.push(format!("stochastic_k_{}", config.stochastic_window));
    names.push(format!("stochastic_d_{}", config.stochastic_smoothing));
    names.push(format!("cci_{}", config.cci_window));
    names.push(format!("williams_r_{}", config.williams_window));

    names.push(format!("bollinger_upper_{}", config.bollinger_window));
    names.push(format!("bollinger_lower_{}", config.bollinger_window));
    names.push(format!("bollinger_bandwidth_{}", config.bollinger_window));
    names.push(format!("bollinger_percent_b_{}", config.bollinger_window));

    names.push(format!("volume_ma_{}", config.volume_window));
    names.push(format!("volume_ratio_{}", config.volume_window));
    names.push(format!("volume_zscore_{}", config.volume_window));
    names.push("dollar_volume".to_string());
    names.push("obv".to_string());
    if include_vwap_features {
        names.push("close_to_vwap_pct".to_string());
        names.push("typical_price_to_vwap_pct".to_string());
    }

    names.push("open_to_close_return".to_string());
    names.push("gap_return".to_string());
    names.push("upper_shadow_ratio".to_string());
    names.push("lower_shadow_ratio".to_string());
    names.push("close_location_value".to_string());

    names
}

fn build_feature_group_map(feature_names: &[String]) -> HashMap<&'static str, Vec<String>> {
    let mut groups: HashMap<&'static str, Vec<String>> = HashMap::new();

    for name in feature_names {
        let key = if name.starts_with("ret_") || name == "log_return_1d" || name == "cumulative_return" {
            "returns"
        } else if name.starts_with("excess_ret_") {
            "benchmark"
        } else if name.starts_with("sma_")
            || name.starts_with("ema_")
            || name.starts_with("price_to_")
            || name.starts_with("macd_")
        {
            "trend"
        } else if name.starts_with("rolling_std_")
            || name.starts_with("realized_vol_")
            || name.starts_with("atr_")
            || name == "true_range"
            || name.starts_with("parkinson_vol_")
            || name == "high_low_range_pct"
        {
            "volatility"
        } else if name.starts_with("rsi_")
            || name.starts_with("stochastic_")
            || name.starts_with("cci_")
            || name.starts_with("williams_r_")
        {
            "oscillators"
        } else if name.starts_with("bollinger_") {
            "bollinger"
        } else if name.starts_with("volume_")
            || name == "dollar_volume"
            || name == "obv"
        {
            "volume"
        } else if name.contains("vwap") {
            "vwap"
        } else if name == "open_to_close_return"
            || name == "gap_return"
            || name == "upper_shadow_ratio"
            || name == "lower_shadow_ratio"
            || name == "close_location_value"
        {
            "candles"
        } else {
            "other"
        };

        groups.entry(key).or_default().push(name.clone());
    }

    groups
}

fn feature_names_for_groups(
    available_feature_names: &[String],
    group_map: &HashMap<&'static str, Vec<String>>,
    groups: &[&str],
) -> Vec<String> {
    let requested = groups
        .iter()
        .map(|group| group.trim().to_ascii_lowercase())
        .collect::<HashSet<_>>();

    available_feature_names
        .iter()
        .filter(|name| {
            group_map.iter().any(|(group, names)| {
                requested.contains(*group) && names.iter().any(|candidate| candidate == *name)
            })
        })
        .cloned()
        .collect()
}

fn parse_env_csv(name: &str) -> Vec<String> {
    std::env::var(name)
        .ok()
        .map(|value| parse_csv_list(&value))
        .unwrap_or_default()
}

fn parse_csv_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|token| token.trim())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_string())
        .collect::<Vec<_>>()
}

fn build_feature_matrix_for_symbol(
    symbol: &str,
    data: &StockData,
    benchmark_data: Option<&StockData>,
    benchmark_lookup: Option<&HashMap<NaiveDate, usize>>,
    supplemental: Option<&SymbolSupplementalData>,
    config: &FeatureEngineConfig,
    feature_names: &[String],
    include_vwap_features: bool,
) -> Result<FeatureMatrix> {
    let candles = &data.history;
    if candles.len() < required_history(config) + 1 {
        return Ok(FeatureMatrix {
            symbol: symbol.to_string(),
            feature_names: feature_names.to_vec(),
            rows: Vec::new(),
        });
    }

    let close_values = candles.iter().map(|c| c.close).collect::<Vec<_>>();
    let simple_returns = compute_simple_returns(&close_values, &config.return_windows);
    let open_values = candles.iter().map(|c| c.open).collect::<Vec<_>>();
    let high_values = candles.iter().map(|c| c.high).collect::<Vec<_>>();
    let low_values = candles.iter().map(|c| c.low).collect::<Vec<_>>();
    let volume_values = candles.iter().map(|c| c.volume).collect::<Vec<_>>();
    let typical_prices = candles
        .iter()
        .map(|c| (c.high + c.low + c.close) / 3.0)
        .collect::<Vec<_>>();

    let log_returns = compute_log_returns(&close_values);
    let true_ranges = compute_true_ranges(candles);
    let obv_values = compute_obv(candles);

    let mut ema_map = HashMap::new();
    for &window in &config.ma_windows {
        ema_map.insert(window, ema_series(&close_values, window));
    }
    let macd_fast_ema = ema_series(&close_values, config.macd_fast);
    let macd_slow_ema = ema_series(&close_values, config.macd_slow);
    let macd_line = macd_fast_ema
        .iter()
        .zip(macd_slow_ema.iter())
        .map(|(fast, slow)| fast - slow)
        .collect::<Vec<_>>();
    let macd_signal = ema_series(&macd_line, config.macd_signal);
    let macd_histogram = macd_line
        .iter()
        .zip(macd_signal.iter())
        .map(|(line, signal)| line - signal)
        .collect::<Vec<_>>();
    let stochastic_k = compute_stochastic_k(candles, config.stochastic_window);
    let stochastic_d = sma_series(&stochastic_k, config.stochastic_smoothing);

    let start_idx = required_history(config);
    let mut rows = Vec::with_capacity(candles.len().saturating_sub(start_idx));

    for idx in start_idx..candles.len() {
        let candle = &candles[idx];
        let benchmark_idx = benchmark_lookup.and_then(|lookup| lookup.get(&candle.date.date_naive()).copied());
        if benchmark_lookup.is_some() && benchmark_idx.is_none() {
            continue;
        }

        let mut values = Vec::with_capacity(feature_names.len());

        for &window in &config.return_windows {
            values.push(*simple_returns.get(&window).and_then(|series| series.get(idx)).unwrap_or(&0.0));
        }
        values.push(log_returns[idx]);
        values.push(close_values[idx] / close_values[0] - 1.0);
        if let (Some(bench_idx), Some(benchmark)) = (benchmark_idx, benchmark_data) {
            for &window in &config.return_windows {
                let bench_ret = if bench_idx >= window {
                    benchmark_return_for_date(benchmark, candle.date.date_naive(), benchmark_lookup, window)
                        .unwrap_or(0.0)
                } else {
                    0.0
                };
                let asset_ret = *simple_returns.get(&window).and_then(|series| series.get(idx)).unwrap_or(&0.0);
                values.push(asset_ret - bench_ret);
            }
        }

        for &window in &config.ma_windows {
            let sma = mean(&close_values[idx + 1 - window..=idx]);
            let ema = *ema_map
                .get(&window)
                .and_then(|series| series.get(idx))
                .unwrap_or(&close_values[idx]);
            let prev_sma = mean(&close_values[idx + 1 - window - config.slope_lookback..=idx - config.slope_lookback]);
            let prev_ema = *ema_map
                .get(&window)
                .and_then(|series| series.get(idx - config.slope_lookback))
                .unwrap_or(&ema);
            values.push(sma);
            values.push(ema);
            values.push(safe_ratio(sma, prev_sma) - 1.0);
            values.push(safe_ratio(ema, prev_ema) - 1.0);
            values.push(safe_ratio(close_values[idx], sma) - 1.0);
            values.push(safe_ratio(close_values[idx], ema) - 1.0);
        }
        if config.ma_windows.len() >= 2 {
            let fast = config.ma_windows[0];
            let slow = *config.ma_windows.last().unwrap_or(&fast);
            let sma_fast = mean(&close_values[idx + 1 - fast..=idx]);
            let sma_slow = mean(&close_values[idx + 1 - slow..=idx]);
            values.push(safe_ratio(sma_fast, sma_slow) - 1.0);
        }
        values.push(safe_ratio(macd_fast_ema[idx], macd_slow_ema[idx]) - 1.0);
        values.push(macd_line[idx]);
        values.push(macd_signal[idx]);
        values.push(macd_histogram[idx]);

        for &window in &config.volatility_windows {
            let slice = &log_returns[idx + 1 - window..=idx];
            let std = sample_std(slice);
            values.push(std);
            values.push(std * TRADING_DAYS_PER_YEAR.sqrt());
        }
        values.push(mean(&true_ranges[idx + 1 - config.atr_window..=idx]));
        values.push(true_ranges[idx]);
        values.push(parkinson_volatility(&high_values[idx + 1 - config.parkinson_window..=idx], &low_values[idx + 1 - config.parkinson_window..=idx]));
        values.push((high_values[idx] - low_values[idx]) / close_values[idx].max(EPSILON));

        values.push(compute_rsi(&log_returns[idx + 1 - config.rsi_window..=idx]));
        values.push(stochastic_k[idx]);
        values.push(stochastic_d[idx]);
        values.push(compute_cci(&typical_prices[idx + 1 - config.cci_window..=idx]));
        values.push(compute_williams_r(&high_values[idx + 1 - config.williams_window..=idx], &low_values[idx + 1 - config.williams_window..=idx], close_values[idx]));

        let boll_mean = mean(&close_values[idx + 1 - config.bollinger_window..=idx]);
        let boll_std = sample_std(&close_values[idx + 1 - config.bollinger_window..=idx]);
        let boll_upper = boll_mean + config.bollinger_std_mult * boll_std;
        let boll_lower = boll_mean - config.bollinger_std_mult * boll_std;
        let boll_width = (boll_upper - boll_lower) / boll_mean.max(EPSILON);
        let percent_b = (close_values[idx] - boll_lower) / (boll_upper - boll_lower).max(EPSILON);
        values.push(boll_upper);
        values.push(boll_lower);
        values.push(boll_width);
        values.push(percent_b);

        let volume_slice = &volume_values[idx + 1 - config.volume_window..=idx];
        let volume_mean = mean(volume_slice);
        let volume_std = sample_std(volume_slice).max(EPSILON);
        values.push(volume_mean);
        values.push(volume_values[idx] / volume_mean.max(EPSILON));
        values.push((volume_values[idx] - volume_mean) / volume_std);
        values.push(close_values[idx] * volume_values[idx]);
        values.push(obv_values[idx]);
        if include_vwap_features {
            let vwap = supplemental
                .and_then(|entry| entry.daily_vwap.get(&candle.date.date_naive()).copied())
                .unwrap_or(close_values[idx]);
            values.push(safe_ratio(close_values[idx], vwap) - 1.0);
            values.push(safe_ratio(typical_prices[idx], vwap) - 1.0);
        }

        let intraday_range = (high_values[idx] - low_values[idx]).max(EPSILON);
        let upper_shadow = high_values[idx] - open_values[idx].max(close_values[idx]);
        let lower_shadow = open_values[idx].min(close_values[idx]) - low_values[idx];
        values.push(safe_ratio(close_values[idx], open_values[idx]) - 1.0);
        values.push(safe_ratio(open_values[idx], close_values[idx - 1]) - 1.0);
        values.push(upper_shadow.max(0.0) / intraday_range);
        values.push(lower_shadow.max(0.0) / intraday_range);
        values.push(((close_values[idx] - low_values[idx]) - (high_values[idx] - close_values[idx])) / intraday_range);

        rows.push(FeatureRow {
            date: candle.date,
            values,
        });
    }

    Ok(FeatureMatrix {
        symbol: symbol.to_string(),
        feature_names: feature_names.to_vec(),
        rows,
    })
}

fn required_history(config: &FeatureEngineConfig) -> usize {
    let max_return = config.return_windows.iter().copied().max().unwrap_or(1);
    let max_ma = config.ma_windows.iter().copied().max().unwrap_or(1);
    [
        max_return,
        max_ma + config.slope_lookback,
        config.volatility_windows.iter().copied().max().unwrap_or(1),
        config.macd_slow + config.macd_signal - 1,
        config.atr_window,
        config.rsi_window,
        config.stochastic_window + config.stochastic_smoothing - 1,
        config.cci_window,
        config.williams_window,
        config.bollinger_window,
        config.volume_window,
        config.parkinson_window,
    ]
    .into_iter()
    .max()
    .unwrap_or(1)
}

fn compute_simple_returns(close_values: &[f64], windows: &[usize]) -> HashMap<usize, Vec<f64>> {
    let mut out = HashMap::with_capacity(windows.len());
    for &window in windows {
        let mut series = vec![0.0; close_values.len()];
        for idx in window..close_values.len() {
            series[idx] = safe_ratio(close_values[idx], close_values[idx - window]) - 1.0;
        }
        out.insert(window, series);
    }
    out
}

fn benchmark_return_for_date(
    benchmark: &StockData,
    date: NaiveDate,
    benchmark_lookup: Option<&HashMap<NaiveDate, usize>>,
    window: usize,
) -> Option<f64> {
    let idx = benchmark_lookup?.get(&date).copied()?;
    if idx < window {
        None
    } else {
        Some(safe_ratio(benchmark.history[idx].close, benchmark.history[idx - window].close) - 1.0)
    }
}

fn compute_log_returns(close_values: &[f64]) -> Vec<f64> {
    let mut out = vec![0.0; close_values.len()];
    for idx in 1..close_values.len() {
        out[idx] = (close_values[idx] / close_values[idx - 1].max(EPSILON)).ln();
    }
    out
}

fn compute_true_ranges(candles: &[Candle]) -> Vec<f64> {
    let mut out = vec![0.0; candles.len()];
    for idx in 1..candles.len() {
        let high_low = candles[idx].high - candles[idx].low;
        let high_prev_close = (candles[idx].high - candles[idx - 1].close).abs();
        let low_prev_close = (candles[idx].low - candles[idx - 1].close).abs();
        out[idx] = high_low.max(high_prev_close).max(low_prev_close);
    }
    out
}

fn compute_obv(candles: &[Candle]) -> Vec<f64> {
    let mut out = vec![0.0; candles.len()];
    for idx in 1..candles.len() {
        out[idx] = out[idx - 1]
            + if candles[idx].close > candles[idx - 1].close {
                candles[idx].volume
            } else if candles[idx].close < candles[idx - 1].close {
                -candles[idx].volume
            } else {
                0.0
            }
    }
    out
}

fn compute_stochastic_k(candles: &[Candle], window: usize) -> Vec<f64> {
    let mut out = vec![50.0; candles.len()];
    for idx in 0..candles.len() {
        if idx + 1 < window {
            continue;
        }
        let highs = candles[idx + 1 - window..=idx].iter().map(|c| c.high);
        let lows = candles[idx + 1 - window..=idx].iter().map(|c| c.low);
        let highest = highs.fold(f64::NEG_INFINITY, f64::max);
        let lowest = lows.fold(f64::INFINITY, f64::min);
        out[idx] = 100.0 * (candles[idx].close - lowest) / (highest - lowest).max(EPSILON);
    }
    out
}

fn compute_rsi(log_returns: &[f64]) -> f64 {
    let mut gains = 0.0;
    let mut losses = 0.0;
    for &ret in log_returns {
        if ret >= 0.0 {
            gains += ret;
        } else {
            losses += -ret;
        }
    }
    let avg_gain = gains / log_returns.len().max(1) as f64;
    let avg_loss = losses / log_returns.len().max(1) as f64;
    if avg_loss <= EPSILON {
        100.0
    } else {
        let rs = avg_gain / avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }
}

fn compute_cci(typical_prices: &[f64]) -> f64 {
    let mean_tp = mean(typical_prices);
    let mean_dev = typical_prices
        .iter()
        .map(|price| (price - mean_tp).abs())
        .sum::<f64>()
        / typical_prices.len().max(1) as f64;
    (typical_prices[typical_prices.len() - 1] - mean_tp) / (0.015 * mean_dev.max(EPSILON))
}

fn compute_williams_r(highs: &[f64], lows: &[f64], close: f64) -> f64 {
    let highest = highs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let lowest = lows.iter().copied().fold(f64::INFINITY, f64::min);
    -100.0 * (highest - close) / (highest - lowest).max(EPSILON)
}

fn parkinson_volatility(highs: &[f64], lows: &[f64]) -> f64 {
    let n = highs.len().max(1) as f64;
    let variance = highs
        .iter()
        .zip(lows.iter())
        .map(|(high, low)| (high / low.max(EPSILON)).ln().powi(2))
        .sum::<f64>()
        / (4.0 * n * std::f64::consts::LN_2);
    variance.sqrt() * TRADING_DAYS_PER_YEAR.sqrt()
}

fn sma_series(values: &[f64], window: usize) -> Vec<f64> {
    let mut out = vec![0.0; values.len()];
    for idx in 0..values.len() {
        let start = idx.saturating_add(1).saturating_sub(window);
        out[idx] = mean(&values[start..=idx]);
    }
    out
}

fn ema_series(values: &[f64], window: usize) -> Vec<f64> {
    if values.is_empty() {
        return Vec::new();
    }
    let alpha = 2.0 / (window as f64 + 1.0);
    let mut out = vec![values[0]; values.len()];
    for idx in 1..values.len() {
        out[idx] = alpha * values[idx] + (1.0 - alpha) * out[idx - 1];
    }
    out
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn sample_std(values: &[f64]) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }
    let mu = mean(values);
    let variance = values.iter().map(|value| (value - mu).powi(2)).sum::<f64>() / (values.len() as f64 - 1.0);
    variance.sqrt()
}

fn safe_ratio(numerator: f64, denominator: f64) -> f64 {
    numerator / denominator.max(EPSILON)
}


#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_stock(symbol: &str, seed: f64) -> StockData {
        let start = Utc::now() - Duration::days(120);
        let mut history = Vec::new();
        for idx in 0..100 {
            let close = seed + idx as f64 * 0.8 + (idx % 5) as f64 * 0.2;
            let open = close * 0.995;
            let high = close * 1.01;
            let low = close * 0.99;
            let volume = 1_000_000.0 + idx as f64 * 10_000.0;
            history.push(Candle {
                date: start + Duration::days(idx as i64),
                open,
                high,
                low,
                close,
                volume,
            });
        }
        StockData {
            symbol: symbol.to_string(),
            history,
        }
    }

    #[test]
    fn builds_feature_store_for_multiple_assets() {
        let mut assets = HashMap::new();
        assets.insert("AAPL".to_string(), make_stock("AAPL", 100.0));
        assets.insert("QQQ".to_string(), make_stock("QQQ", 200.0));

        let store = build_multi_asset_feature_store(&assets, Some("QQQ"), None, &FeatureEngineConfig::default()).unwrap();
        let aapl = store.by_symbol.get("AAPL").unwrap();

        assert!(!aapl.rows.is_empty());
        assert!(store.feature_names.iter().any(|name| name == "volume_ma_20"));
        assert!(store.feature_names.iter().any(|name| name == "macd_line"));
        assert!(store.feature_names.iter().any(|name| name == "close_location_value"));
        assert!(aapl.rows.iter().all(|row| row.values.iter().all(|value| value.is_finite())));
    }

    #[test]
    fn builds_training_windows_from_feature_store() {
        let mut assets = HashMap::new();
        assets.insert("AAPL".to_string(), make_stock("AAPL", 100.0));
        assets.insert("QQQ".to_string(), make_stock("QQQ", 200.0));
        let store = build_multi_asset_feature_store(&assets, Some("QQQ"), None, &FeatureEngineConfig::default()).unwrap();

        let mut asset_ids = HashMap::new();
        asset_ids.insert("AAPL".to_string(), 1usize);
        asset_ids.insert("QQQ".to_string(), 2usize);

        let dataset = build_training_dataset_from_features(&store, 10, 3, "log_return_1d", &asset_ids).unwrap();
        assert!(!dataset.features.is_empty());
        assert_eq!(dataset.features[0].len(), 10 * store.feature_names.len());
        assert_eq!(dataset.targets[0].len(), 3);
    }

    #[test]
    fn selects_core_feature_profile() {
        let mut assets = HashMap::new();
        assets.insert("AAPL".to_string(), make_stock("AAPL", 100.0));
        assets.insert("QQQ".to_string(), make_stock("QQQ", 200.0));
        let store = build_multi_asset_feature_store(&assets, Some("QQQ"), None, &FeatureEngineConfig::default()).unwrap();

        let selection = FeatureSelectionConfig {
            profile: FeatureSelectionProfile::Core,
            groups: Vec::new(),
            include: vec!["macd_line".to_string()],
            exclude: vec!["obv".to_string()],
            target_feature: "log_return_1d".to_string(),
        };

        let selected = store.select_features(&selection).unwrap();
        assert!(selected.feature_names.iter().any(|name| name == "macd_line"));
        assert!(!selected.feature_names.iter().any(|name| name == "obv"));
        assert!(selected.feature_names.iter().any(|name| name == "log_return_1d"));
    }

    #[test]
    fn default_selection_uses_recommended_feature_set() {
        let selection = FeatureSelectionConfig::default();
        assert_eq!(selection.profile, FeatureSelectionProfile::Custom);
        assert_eq!(selection.include.len(), 10);
        assert!(selection.include.iter().any(|name| name == "log_return_1d"));
        assert!(selection.include.iter().any(|name| name == "excess_ret_20d_vs_benchmark"));
    }

    #[test]
    fn include_full_expands_to_all_features() {
        let available = vec![
            "log_return_1d".to_string(),
            "ret_5d".to_string(),
            "macd_histogram".to_string(),
        ];
        let selection = FeatureSelectionConfig {
            profile: FeatureSelectionProfile::Custom,
            groups: Vec::new(),
            include: vec!["full".to_string()],
            exclude: Vec::new(),
            target_feature: "log_return_1d".to_string(),
        };

        let selected = select_feature_names(&available, &selection).unwrap();
        assert_eq!(selected, available);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FeatureManifest {
    pub version: u32,
    pub feature_names: Vec<String>,
    pub target_feature: String,
    pub benchmark_symbol: Option<String>,
    pub lookback: usize,
    pub forecast: usize,
    pub input_dim: usize,
}

#[derive(Clone, Debug)]
pub struct PreparedFeatureContext {
    pub normalized_features: Vec<f32>,
    pub input_dim: usize,
    pub target_mean: f64,
    pub target_std: f64,
}
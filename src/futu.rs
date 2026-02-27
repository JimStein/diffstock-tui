#![allow(dead_code)]

use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FutuApiMode {
    Http,
    Python,
}

#[derive(Clone, Debug)]
pub struct FutuApiConfig {
    pub mode: FutuApiMode,
    pub base_url: String,
    pub token: Option<String>,
    pub token_header: String,
    pub token_prefix: String,
    pub account_list_path: String,
    pub unlock_trading_path: String,
    pub account_financial_path: String,
    pub positions_path: String,
    pub max_tradable_qty_path: String,
    pub place_order_path: String,
    pub modify_order_path: String,
    pub order_list_path: String,
    pub history_order_list_path: String,
    pub today_trades_path: String,
    pub history_trades_path: String,
    pub account_id: Option<String>,
    pub trd_env: Option<String>,
    pub market: Option<String>,
    pub password_md5: Option<String>,
    pub python_bin: String,
    pub py_host: String,
    pub py_port: u16,
    pub py_filter_trdmarket: String,
    pub py_security_firm: String,
}

impl FutuApiConfig {
    pub fn from_env() -> Result<Self> {
        let mode_env = std::env::var("FUTU_API_MODE").ok();

        let base_url = std::env::var("FUTU_API_BASE_URL")
            .unwrap_or_default()
            .trim()
            .trim_end_matches('/')
            .to_string();

        let mode = if let Some(mode_text) = mode_env {
            match mode_text.trim().to_lowercase().as_str() {
                "python" | "py" => FutuApiMode::Python,
                _ => FutuApiMode::Http,
            }
        } else if base_url.is_empty() {
            FutuApiMode::Python
        } else {
            FutuApiMode::Http
        };
        if matches!(mode, FutuApiMode::Http) && base_url.is_empty() {
            return Err(anyhow!("FUTU_API_BASE_URL is empty"));
        }

        let py_port = std::env::var("FUTU_PY_PORT")
            .ok()
            .and_then(|v| v.trim().parse::<u16>().ok())
            .unwrap_or(11111);

        Ok(Self {
            mode,
            base_url,
            token: std::env::var("FUTU_API_TOKEN").ok().filter(|v| !v.trim().is_empty()),
            token_header: std::env::var("FUTU_API_TOKEN_HEADER")
                .unwrap_or_else(|_| "Authorization".to_string()),
            token_prefix: std::env::var("FUTU_API_TOKEN_PREFIX")
                .unwrap_or_else(|_| "Bearer".to_string()),
            account_list_path: std::env::var("FUTU_API_ACCOUNT_LIST_PATH")
                .unwrap_or_else(|_| "/account/list".to_string()),
            unlock_trading_path: std::env::var("FUTU_API_UNLOCK_PATH")
                .unwrap_or_else(|_| "/trade/unlock".to_string()),
            account_financial_path: std::env::var("FUTU_API_ACCOUNT_PATH")
                .unwrap_or_else(|_| "/account".to_string()),
            positions_path: std::env::var("FUTU_API_POSITIONS_PATH")
                .unwrap_or_else(|_| "/positions".to_string()),
            max_tradable_qty_path: std::env::var("FUTU_API_MAX_TRADABLE_QTY_PATH")
                .unwrap_or_else(|_| "/trade/max-tradable-qty".to_string()),
            place_order_path: std::env::var("FUTU_API_PLACE_ORDER_PATH")
                .unwrap_or_else(|_| "/order/place".to_string()),
            modify_order_path: std::env::var("FUTU_API_MODIFY_ORDER_PATH")
                .unwrap_or_else(|_| "/order/modify".to_string()),
            order_list_path: std::env::var("FUTU_API_ORDER_LIST_PATH")
                .unwrap_or_else(|_| "/order/list".to_string()),
            history_order_list_path: std::env::var("FUTU_API_HISTORY_ORDER_LIST_PATH")
                .unwrap_or_else(|_| "/order/list/history".to_string()),
            today_trades_path: std::env::var("FUTU_API_TODAY_TRADES_PATH")
                .unwrap_or_else(|_| "/trade/list/today".to_string()),
            history_trades_path: std::env::var("FUTU_API_HISTORY_TRADES_PATH")
                .unwrap_or_else(|_| "/trade/list/history".to_string()),
            account_id: std::env::var("FUTU_API_ACC_ID").ok().filter(|v| !v.trim().is_empty()),
            trd_env: std::env::var("FUTU_API_TRD_ENV").ok().filter(|v| !v.trim().is_empty()),
            market: std::env::var("FUTU_API_MARKET").ok().filter(|v| !v.trim().is_empty()),
            password_md5: std::env::var("FUTU_API_PASSWORD_MD5").ok().filter(|v| !v.trim().is_empty()),
            python_bin: std::env::var("FUTU_PYTHON_BIN").unwrap_or_else(|_| "python".to_string()),
            py_host: std::env::var("FUTU_PY_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            py_port,
            py_filter_trdmarket: std::env::var("FUTU_PY_FILTER_TRDMARKET")
                .unwrap_or_else(|_| "HK".to_string()),
            py_security_firm: std::env::var("FUTU_PY_SECURITY_FIRM")
                .unwrap_or_else(|_| "FUTUSECURITIES".to_string()),
        })
    }

    fn endpoint(&self, path: &str) -> String {
        let normalized = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        format!("{}{}", self.base_url, normalized)
    }
}

#[derive(Clone, Debug)]
pub struct FutuApiClient {
    client: Client,
    config: FutuApiConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FutuPosition {
    pub symbol: String,
    pub quantity: f64,
    pub avg_cost: f64,
    pub market_price: f64,
    pub updated_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FutuExecutionSnapshot {
    pub cash_usd: f64,
    pub buying_power_usd: f64,
    pub positions: Vec<FutuPosition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FutuPlaceOrderRequest {
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub price: Option<f64>,
    pub order_type: Option<String>,
    pub market: Option<String>,
    pub trd_env: Option<String>,
    pub acc_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FutuModifyOrderRequest {
    pub order_id: String,
    pub action: String,
    pub quantity: Option<f64>,
    pub price: Option<f64>,
    pub trd_env: Option<String>,
    pub acc_id: Option<String>,
}

impl FutuApiClient {
    pub fn from_env() -> Result<Self> {
        let config = FutuApiConfig::from_env()?;
        Ok(Self {
            client: Client::new(),
            config,
        })
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value> {
        let url = self.config.endpoint(path);
        let mut req = self.client.get(url);
        if let Some(token) = &self.config.token {
            let header_value = format!("{} {}", self.config.token_prefix, token);
            req = req.header(self.config.token_header.as_str(), header_value);
        }
        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(anyhow!("GET {} failed: HTTP {}", path, res.status()));
        }
        Ok(res.json::<serde_json::Value>().await?)
    }

    async fn post_json<T: Serialize>(&self, path: &str, payload: &T) -> Result<serde_json::Value> {
        let url = self.config.endpoint(path);
        let mut req = self.client.post(url).json(payload);
        if let Some(token) = &self.config.token {
            let header_value = format!("{} {}", self.config.token_prefix, token);
            req = req.header(self.config.token_header.as_str(), header_value);
        }
        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(anyhow!("POST {} failed: HTTP {}", path, res.status()));
        }
        Ok(res.json::<serde_json::Value>().await?)
    }

    pub async fn get_account_list(&self) -> Result<serde_json::Value> {
        if matches!(self.config.mode, FutuApiMode::Python) {
            return self.get_account_list_via_python();
        }
        self.get_json(&self.config.account_list_path).await
    }

    pub async fn unlock_trading(&self, password_md5: Option<&str>) -> Result<serde_json::Value> {
        let payload = serde_json::json!({
            "password_md5": password_md5
                .map(|v| v.to_string())
                .or_else(|| self.config.password_md5.clone()),
            "acc_id": self.config.account_id.clone(),
            "trd_env": self.config.trd_env.clone(),
        });
        self.post_json(&self.config.unlock_trading_path, &payload).await
    }

    pub async fn get_account_financial_information(&self) -> Result<serde_json::Value> {
        self.get_json(&self.config.account_financial_path).await
    }

    pub async fn get_positions_list(&self) -> Result<serde_json::Value> {
        self.get_json(&self.config.positions_path).await
    }

    pub async fn get_maximum_tradable_quantity(
        &self,
        symbol: &str,
        side: &str,
    ) -> Result<serde_json::Value> {
        let payload = serde_json::json!({
            "symbol": symbol,
            "side": side,
            "acc_id": self.config.account_id.clone(),
            "trd_env": self.config.trd_env.clone(),
            "market": self.config.market.clone(),
        });
        self.post_json(&self.config.max_tradable_qty_path, &payload).await
    }

    pub async fn place_order(&self, req: &FutuPlaceOrderRequest) -> Result<serde_json::Value> {
        let payload = serde_json::json!({
            "symbol": req.symbol,
            "side": req.side,
            "quantity": req.quantity,
            "price": req.price,
            "order_type": req.order_type,
            "market": req.market.clone().or_else(|| self.config.market.clone()),
            "trd_env": req.trd_env.clone().or_else(|| self.config.trd_env.clone()),
            "acc_id": req.acc_id.clone().or_else(|| self.config.account_id.clone()),
        });
        self.post_json(&self.config.place_order_path, &payload).await
    }

    pub async fn modify_or_cancel_order(
        &self,
        req: &FutuModifyOrderRequest,
    ) -> Result<serde_json::Value> {
        let payload = serde_json::json!({
            "order_id": req.order_id,
            "action": req.action,
            "quantity": req.quantity,
            "price": req.price,
            "trd_env": req.trd_env.clone().or_else(|| self.config.trd_env.clone()),
            "acc_id": req.acc_id.clone().or_else(|| self.config.account_id.clone()),
        });
        self.post_json(&self.config.modify_order_path, &payload).await
    }

    pub async fn get_order_list(&self) -> Result<serde_json::Value> {
        self.get_json(&self.config.order_list_path).await
    }

    pub async fn get_historical_order_list(&self) -> Result<serde_json::Value> {
        self.get_json(&self.config.history_order_list_path).await
    }

    pub async fn get_today_executed_trades(&self) -> Result<serde_json::Value> {
        self.get_json(&self.config.today_trades_path).await
    }

    pub async fn get_historical_executed_trades(&self) -> Result<serde_json::Value> {
        self.get_json(&self.config.history_trades_path).await
    }

    pub async fn poll_execution_snapshot(&self) -> Result<FutuExecutionSnapshot> {
        let account_json = self.get_account_financial_information().await?;
        let positions_json = self.get_positions_list().await?;

        let cash_usd = json_get_number(
            &account_json,
            &["cash_usd", "cash", "available_cash", "available_funds"],
        )
        .or_else(|| {
            account_json
                .get("data")
                .and_then(|v| {
                    json_get_number(v, &["cash_usd", "cash", "available_cash", "available_funds"])
                })
        })
        .unwrap_or(0.0);

        let buying_power_usd = json_get_number(
            &account_json,
            &["buying_power", "buying_power_usd", "max_power", "available_funds"],
        )
        .or_else(|| {
            account_json
                .get("data")
                .and_then(|v| {
                    json_get_number(v, &["buying_power", "buying_power_usd", "max_power", "available_funds"])
                })
        })
        .unwrap_or(cash_usd);

        let positions = extract_positions(&positions_json);

        Ok(FutuExecutionSnapshot {
            cash_usd,
            buying_power_usd,
            positions,
        })
    }
}

impl FutuApiClient {
    fn get_account_list_via_python(&self) -> Result<serde_json::Value> {
        let script = r#"
import json
from futu import *

host = __HOST__
port = __PORT__
filter_market = TrdMarket.__MARKET__
security_firm = SecurityFirm.__SEC_FIRM__

ctx = OpenSecTradeContext(
    filter_trdmarket=filter_market,
    host=host,
    port=port,
    security_firm=security_firm,
)
try:
    ret, data = ctx.get_acc_list()
    if ret == RET_OK:
        payload = {
            'ok': True,
            'ret': ret,
            'data': data.to_dict(orient='records') if hasattr(data, 'to_dict') else data,
            'acc_id_first': (data['acc_id'][0] if hasattr(data, '__getitem__') and 'acc_id' in data and len(data['acc_id']) > 0 else None),
            'acc_id_list': (data['acc_id'].values.tolist() if hasattr(data, '__getitem__') and 'acc_id' in data else []),
        }
    else:
        payload = {
            'ok': False,
            'ret': ret,
            'error': str(data),
        }
    print(json.dumps(payload, ensure_ascii=False))
finally:
    ctx.close()
"#;

        let script = script
            .replace("__HOST__", &format!("{:?}", self.config.py_host))
            .replace("__PORT__", &self.config.py_port.to_string())
            .replace("__MARKET__", &self.config.py_filter_trdmarket)
            .replace("__SEC_FIRM__", &self.config.py_security_firm);

        let output = Command::new(&self.config.python_bin)
            .arg("-c")
            .arg(script)
            .output()
            .map_err(|e| anyhow!("failed to run python futu account script: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(anyhow!("python futu account script failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Err(anyhow!("python futu account script returned empty output"));
        }

        let v = serde_json::from_str::<serde_json::Value>(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu account output: {}", e))?;

        Ok(v)
    }
}

fn json_get_number(value: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(raw) = value.get(*key) {
            if let Some(v) = raw.as_f64() {
                return Some(v);
            }
            if let Some(v) = raw.as_i64() {
                return Some(v as f64);
            }
            if let Some(v) = raw.as_u64() {
                return Some(v as f64);
            }
            if let Some(text) = raw.as_str() {
                if let Ok(v) = text.trim().parse::<f64>() {
                    return Some(v);
                }
            }
        }
    }
    None
}

fn json_get_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(raw) = value.get(*key) {
            if let Some(v) = raw.as_str() {
                let text = v.trim();
                if !text.is_empty() {
                    return Some(text.to_string());
                }
            }
        }
    }
    None
}

fn extract_positions(value: &serde_json::Value) -> Vec<FutuPosition> {
    let candidates = [
        value.get("positions"),
        value.get("data").and_then(|v| v.get("positions")),
        value.get("data").and_then(|v| v.get("items")),
        value.get("items"),
    ];

    let rows_opt = candidates
        .into_iter()
        .flatten()
        .find_map(|v| v.as_array())
        .cloned()
        .or_else(|| value.as_array().cloned());

    let mut out = Vec::new();
    let Some(rows) = rows_opt else {
        return out;
    };

    for row in rows {
        let symbol = json_get_string(&row, &["symbol", "code", "ticker"])
            .unwrap_or_default()
            .to_uppercase();
        if symbol.is_empty() {
            continue;
        }

        let quantity = json_get_number(&row, &["quantity", "qty", "position", "holding_qty"])
            .unwrap_or(0.0);
        let avg_cost =
            json_get_number(&row, &["avg_cost", "avg_price", "cost_price", "cost"]).unwrap_or(0.0);
        let market_price =
            json_get_number(&row, &["market_price", "last_price", "price", "current_price"])
                .unwrap_or(0.0);
        let updated_at = json_get_string(&row, &["updated_at", "update_time", "time"]);

        out.push(FutuPosition {
            symbol,
            quantity,
            avg_cost,
            market_price,
            updated_at,
        });
    }

    out.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    out
}

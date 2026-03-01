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
                .unwrap_or_else(|_| "US".to_string()),
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
    pub selected_acc_id: Option<String>,
    pub selected_trd_env: Option<String>,
    pub selected_market: Option<String>,
    pub opend_account_list: Option<serde_json::Value>,
    pub opend_selected_account: Option<serde_json::Value>,
    pub opend_account_info_raw: Option<serde_json::Value>,
    pub opend_positions_raw: Option<serde_json::Value>,
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
    pub adjust_limit: Option<f64>,
    pub remark: Option<String>,
    pub time_in_force: Option<String>,
    pub fill_outside_rth: Option<bool>,
    pub session: Option<String>,
    pub aux_price: Option<f64>,
    pub trail_type: Option<String>,
    pub trail_value: Option<f64>,
    pub trail_spread: Option<f64>,
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

    pub fn connection_info(&self) -> (String, u16, String, String) {
        (
            self.config.py_host.clone(),
            self.config.py_port,
            self.config.py_filter_trdmarket.clone(),
            self.config.py_security_firm.clone(),
        )
    }

    pub fn set_account_id_override(&mut self, account_id: Option<String>) {
        self.config.account_id = account_id.filter(|v| !v.trim().is_empty());
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
        if matches!(self.config.mode, FutuApiMode::Python) {
            return self.place_order_via_python(req);
        }

        let effective_env = req
            .trd_env
            .clone()
            .or_else(|| self.config.trd_env.clone())
            .unwrap_or_else(|| "SIMULATE".to_string());

        if effective_env.eq_ignore_ascii_case("REAL") {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let order_id = format!("REAL_PLACEHOLDER_{}", ts);
            return Ok(serde_json::json!({
                "ok": true,
                "placeholder": true,
                "order_id": order_id,
                "status": "SKIPPED_REAL_ORDER",
                "message": "REAL trading is disabled for safety; SIMULATE account remains active.",
                "symbol": req.symbol,
                "side": req.side,
                "quantity": req.quantity,
                "price": req.price,
                "order_type": req.order_type,
                "trd_env": effective_env,
                "acc_id": req.acc_id.clone().or_else(|| self.config.account_id.clone()),
            }));
        }

        let payload = serde_json::json!({
            "symbol": req.symbol,
            "side": req.side,
            "quantity": req.quantity,
            "price": req.price,
            "order_type": req.order_type,
            "market": req.market.clone().or_else(|| self.config.market.clone()),
            "trd_env": req.trd_env.clone().or_else(|| self.config.trd_env.clone()),
            "acc_id": req.acc_id.clone().or_else(|| self.config.account_id.clone()),
            "adjust_limit": req.adjust_limit,
            "remark": req.remark,
            "time_in_force": req.time_in_force,
            "fill_outside_rth": req.fill_outside_rth,
            "session": req.session,
            "aux_price": req.aux_price,
            "trail_type": req.trail_type,
            "trail_value": req.trail_value,
            "trail_spread": req.trail_spread,
        });
        self.post_json(&self.config.place_order_path, &payload).await
    }

    fn place_order_via_python(&self, req: &FutuPlaceOrderRequest) -> Result<serde_json::Value> {
        let symbol = req.symbol.trim().to_uppercase();
        if symbol.is_empty() {
            return Err(anyhow!("symbol is empty"));
        }

        let side_text = req.side.trim().to_uppercase();
        if side_text != "BUY" && side_text != "SELL" {
            return Err(anyhow!("unsupported side: {}", req.side));
        }

        let quantity = req.quantity.max(0.0);
        if quantity < 1.0 {
            return Err(anyhow!("quantity must be >= 1"));
        }

        let order_type = req
            .order_type
            .clone()
            .unwrap_or_else(|| "MARKET".to_string())
            .trim()
            .to_uppercase();
        let market_text = req
            .market
            .clone()
            .or_else(|| self.config.market.clone())
            .unwrap_or_else(|| self.config.py_filter_trdmarket.clone())
            .trim()
            .to_uppercase();
        let trd_env_text = req
            .trd_env
            .clone()
            .or_else(|| self.config.trd_env.clone())
            .unwrap_or_else(|| "SIMULATE".to_string())
            .trim()
            .to_uppercase();
        let acc_id_text = req
            .acc_id
            .clone()
            .or_else(|| self.config.account_id.clone())
            .unwrap_or_default();
        let price = req.price.unwrap_or(0.0);
        let adjust_limit = req.adjust_limit.unwrap_or(0.0);
        let remark = req.remark.clone().unwrap_or_default();
        let time_in_force_text = req
            .time_in_force
            .clone()
            .unwrap_or_else(|| "DAY".to_string())
            .trim()
            .to_uppercase();
        let fill_outside_rth = req.fill_outside_rth.unwrap_or(false);
        let session_text = req
            .session
            .clone()
            .unwrap_or_else(|| "NONE".to_string())
            .trim()
            .to_uppercase();
        let aux_price = req.aux_price.unwrap_or(0.0);
        let trail_type_text = req
            .trail_type
            .clone()
            .unwrap_or_else(|| "NONE".to_string())
            .trim()
            .to_uppercase();
        let trail_value = req.trail_value.unwrap_or(0.0);
        let trail_spread = req.trail_spread.unwrap_or(0.0);

        let script = r#"
import json
from futu import *

host = __HOST__
port = __PORT__
security_firm = SecurityFirm.__SEC_FIRM__
market_text = __MARKET__.strip().upper()
symbol = __SYMBOL__.strip().upper()
side_text = __SIDE__.strip().upper()
qty = float(__QTY__)
price = float(__PRICE__)
order_type_text = __ORDER_TYPE__.strip().upper()
trd_env_text = __TRD_ENV__.strip().upper()
acc_id_text = __ACC_ID__.strip()
adjust_limit = float(__ADJUST_LIMIT__)
remark = __REMARK__
time_in_force_text = __TIME_IN_FORCE__.strip().upper()
fill_outside_rth = __FILL_OUTSIDE_RTH__
session_text = __SESSION__.strip().upper()
aux_price = float(__AUX_PRICE__)
trail_type_text = __TRAIL_TYPE__.strip().upper()
trail_value = float(__TRAIL_VALUE__)
trail_spread = float(__TRAIL_SPREAD__)

def _to_builtin(value):
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(v) for v in value]
    return value

market_enum = getattr(TrdMarket, market_text, TrdMarket.US)
trd_env = TrdEnv.SIMULATE if trd_env_text == 'SIMULATE' else TrdEnv.REAL
trd_side = TrdSide.BUY if side_text == 'BUY' else TrdSide.SELL
order_type = OrderType.MARKET if order_type_text == 'MARKET' else OrderType.NORMAL
time_in_force = getattr(TimeInForce, time_in_force_text, TimeInForce.DAY)
session = getattr(Session, session_text, Session.NONE)

trail_type = None
if trail_type_text and trail_type_text != 'NONE':
    trail_type = getattr(TrailType, trail_type_text, None)

acc_id = 0
if acc_id_text:
    try:
        acc_id = int(acc_id_text)
    except Exception:
        acc_id = 0

ctx = OpenSecTradeContext(filter_trdmarket=market_enum, host=host, port=port, security_firm=security_firm)
try:
    ret, data = ctx.place_order(
        price=price,
        qty=qty,
        code=symbol,
        trd_side=trd_side,
        order_type=order_type,
        adjust_limit=adjust_limit,
        trd_env=trd_env,
        acc_id=acc_id,
        remark=remark if remark else None,
        time_in_force=time_in_force,
        fill_outside_rth=bool(fill_outside_rth),
        aux_price=aux_price if aux_price > 0 else None,
        trail_type=trail_type,
        trail_value=trail_value if trail_value > 0 else None,
        trail_spread=trail_spread if trail_spread > 0 else None,
        session=session,
    )
    if ret != RET_OK:
        print(json.dumps({'ok': False, 'ret': ret, 'error': str(data)}, ensure_ascii=False))
    else:
        rows = data.to_dict(orient='records') if hasattr(data, 'to_dict') else []
        first = rows[0] if len(rows) > 0 else {}
        order_id = first.get('order_id') or first.get('id')
        print(json.dumps(_to_builtin({'ok': True, 'ret': RET_OK, 'order_id': order_id, 'data': rows}), ensure_ascii=False))
finally:
    ctx.close()
"#;

        let script = script
            .replace("__HOST__", &format!("{:?}", self.config.py_host))
            .replace("__PORT__", &self.config.py_port.to_string())
            .replace("__SEC_FIRM__", &self.config.py_security_firm)
            .replace("__MARKET__", &format!("{:?}", market_text))
            .replace("__SYMBOL__", &format!("{:?}", symbol))
            .replace("__SIDE__", &format!("{:?}", side_text))
            .replace("__QTY__", &quantity.to_string())
            .replace("__PRICE__", &price.to_string())
            .replace("__ORDER_TYPE__", &format!("{:?}", order_type))
            .replace("__TRD_ENV__", &format!("{:?}", trd_env_text))
            .replace("__ACC_ID__", &format!("{:?}", acc_id_text))
            .replace("__ADJUST_LIMIT__", &adjust_limit.to_string())
            .replace("__REMARK__", &format!("{:?}", remark))
            .replace("__TIME_IN_FORCE__", &format!("{:?}", time_in_force_text))
            .replace("__FILL_OUTSIDE_RTH__", if fill_outside_rth { "True" } else { "False" })
            .replace("__SESSION__", &format!("{:?}", session_text))
            .replace("__AUX_PRICE__", &aux_price.to_string())
            .replace("__TRAIL_TYPE__", &format!("{:?}", trail_type_text))
            .replace("__TRAIL_VALUE__", &trail_value.to_string())
            .replace("__TRAIL_SPREAD__", &trail_spread.to_string());

        let output = Command::new(&self.config.python_bin)
            .arg("-c")
            .arg(script)
            .output()
            .map_err(|e| anyhow!("failed to run python futu place_order script: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(anyhow!("python futu place_order script failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Err(anyhow!("python futu place_order script returned empty output"));
        }

        let value = parse_json_from_stdout(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu place_order output: {}", e))?;

        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            let err_text = value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown futu python order error");
            return Err(anyhow!("{}", err_text));
        }

        Ok(value)
    }

    pub async fn modify_or_cancel_order(
        &self,
        req: &FutuModifyOrderRequest,
    ) -> Result<serde_json::Value> {
        if matches!(self.config.mode, FutuApiMode::Python) {
            return self.modify_or_cancel_order_via_python(req);
        }

        let effective_env = req
            .trd_env
            .clone()
            .or_else(|| self.config.trd_env.clone())
            .unwrap_or_else(|| "SIMULATE".to_string());

        if effective_env.eq_ignore_ascii_case("REAL") {
            return Ok(serde_json::json!({
                "ok": true,
                "placeholder": true,
                "order_id": req.order_id,
                "action": req.action,
                "status": "SKIPPED_REAL_ORDER_MODIFICATION",
                "message": "REAL trading is disabled for safety; no live order was modified/canceled.",
                "trd_env": effective_env,
                "acc_id": req.acc_id.clone().or_else(|| self.config.account_id.clone()),
            }));
        }

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
        if matches!(self.config.mode, FutuApiMode::Python) {
            return self.get_order_list_via_python();
        }
        self.get_json(&self.config.order_list_path).await
    }

    pub async fn get_historical_order_list(&self) -> Result<serde_json::Value> {
        self.get_historical_order_list_in_range(None, None).await
    }

    pub async fn get_historical_order_list_in_range(
        &self,
        start: Option<String>,
        end: Option<String>,
    ) -> Result<serde_json::Value> {
        if matches!(self.config.mode, FutuApiMode::Python) {
            return self.get_historical_order_list_via_python(start, end);
        }
        self.get_json(&self.config.history_order_list_path).await
    }

    pub async fn get_today_executed_trades(&self) -> Result<serde_json::Value> {
        if matches!(self.config.mode, FutuApiMode::Python) {
            return self.get_today_executed_trades_via_python();
        }
        self.get_json(&self.config.today_trades_path).await
    }

    pub async fn get_historical_executed_trades(&self) -> Result<serde_json::Value> {
        self.get_json(&self.config.history_trades_path).await
    }

    pub async fn poll_execution_snapshot(&self) -> Result<FutuExecutionSnapshot> {
        if matches!(self.config.mode, FutuApiMode::Python) {
            return self.poll_execution_snapshot_via_python();
        }

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
            selected_acc_id: None,
            selected_trd_env: None,
            selected_market: None,
            opend_account_list: None,
            opend_selected_account: None,
            opend_account_info_raw: None,
            opend_positions_raw: None,
        })
    }
}

impl FutuApiClient {
    fn get_historical_order_list_via_python(
        &self,
        start: Option<String>,
        end: Option<String>,
    ) -> Result<serde_json::Value> {
        let desired_acc_id = self.config.account_id.clone().unwrap_or_default();
        let desired_env = self
            .config
            .trd_env
            .clone()
            .unwrap_or_else(|| "SIMULATE".to_string())
            .trim()
            .to_uppercase();
        let desired_market = self
            .config
            .market
            .clone()
            .unwrap_or_else(|| self.config.py_filter_trdmarket.clone())
            .trim()
            .to_uppercase();
        let start_text = start.unwrap_or_default();
        let end_text = end.unwrap_or_default();

        let script = r#"
import json
from futu import *

def _to_builtin(value):
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(v) for v in value]
    return value

host = __HOST__
port = __PORT__
security_firm = SecurityFirm.__SEC_FIRM__
market_text = __MARKET__.strip().upper()
trd_env_text = __TRD_ENV__.strip().upper()
acc_id_text = __ACC_ID__.strip()
start_text = __START__.strip()
end_text = __END__.strip()

market_enum = getattr(TrdMarket, market_text, TrdMarket.US)
trd_env = TrdEnv.SIMULATE if trd_env_text == 'SIMULATE' else TrdEnv.REAL

acc_id = 0
if acc_id_text:
    try:
        acc_id = int(acc_id_text)
    except Exception:
        acc_id = 0

ctx = OpenSecTradeContext(filter_trdmarket=market_enum, host=host, port=port, security_firm=security_firm)
try:
    start_arg = start_text if start_text else None
    end_arg = end_text if end_text else None
    ret, data = ctx.history_order_list_query(
        trd_env=trd_env,
        acc_id=acc_id,
        order_market=market_enum,
        start=start_arg,
        end=end_arg,
    )
    if ret != RET_OK:
        print(json.dumps({'ok': False, 'ret': ret, 'error': str(data)}, ensure_ascii=False))
    else:
        rows = data.to_dict(orient='records') if hasattr(data, 'to_dict') else []
        print(json.dumps(_to_builtin({'ok': True, 'ret': RET_OK, 'data': rows}), ensure_ascii=False))
finally:
    ctx.close()
"#;

        let script = script
            .replace("__HOST__", &format!("{:?}", self.config.py_host))
            .replace("__PORT__", &self.config.py_port.to_string())
            .replace("__SEC_FIRM__", &self.config.py_security_firm)
            .replace("__MARKET__", &format!("{:?}", desired_market))
            .replace("__TRD_ENV__", &format!("{:?}", desired_env))
            .replace("__ACC_ID__", &format!("{:?}", desired_acc_id))
            .replace("__START__", &format!("{:?}", start_text))
            .replace("__END__", &format!("{:?}", end_text));

        let output = Command::new(&self.config.python_bin)
            .arg("-c")
            .arg(script)
            .output()
            .map_err(|e| anyhow!("failed to run python futu history_order_list script: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(anyhow!("python futu history_order_list script failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Err(anyhow!("python futu history_order_list script returned empty output"));
        }

        let value = parse_json_from_stdout(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu history_order_list output: {}", e))?;

        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            let err_text = value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown futu python history_order_list error");
            return Err(anyhow!("{}", err_text));
        }

        Ok(value)
    }

    fn get_today_executed_trades_via_python(&self) -> Result<serde_json::Value> {
        let desired_acc_id = self.config.account_id.clone().unwrap_or_default();
        let desired_env = self
            .config
            .trd_env
            .clone()
            .unwrap_or_else(|| "SIMULATE".to_string())
            .trim()
            .to_uppercase();
        let desired_market = self
            .config
            .market
            .clone()
            .unwrap_or_else(|| self.config.py_filter_trdmarket.clone())
            .trim()
            .to_uppercase();

        let script = r#"
import json
from futu import *

def _to_builtin(value):
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(v) for v in value]
    return value

host = __HOST__
port = __PORT__
security_firm = SecurityFirm.__SEC_FIRM__
market_text = __MARKET__.strip().upper()
trd_env_text = __TRD_ENV__.strip().upper()
acc_id_text = __ACC_ID__.strip()

market_enum = getattr(TrdMarket, market_text, TrdMarket.US)
trd_env = TrdEnv.SIMULATE if trd_env_text == 'SIMULATE' else TrdEnv.REAL

acc_id = 0
if acc_id_text:
    try:
        acc_id = int(acc_id_text)
    except Exception:
        acc_id = 0

ctx = OpenSecTradeContext(filter_trdmarket=market_enum, host=host, port=port, security_firm=security_firm)
try:
    ret = RET_ERROR
    data = None

    try:
        ret, data = ctx.deal_list_query(trd_env=trd_env, acc_id=acc_id, refresh_cache=True)
    except TypeError:
        ret, data = ctx.deal_list_query(trd_env=trd_env, acc_id=acc_id)
    except Exception:
        try:
            ret, data = ctx.history_deal_list_query(trd_env=trd_env, acc_id=acc_id)
        except Exception as e:
            ret = RET_ERROR
            data = str(e)

    if ret != RET_OK:
        print(json.dumps({'ok': False, 'ret': ret, 'error': str(data)}, ensure_ascii=False))
    else:
        rows = data.to_dict(orient='records') if hasattr(data, 'to_dict') else []
        print(json.dumps(_to_builtin({'ok': True, 'ret': RET_OK, 'data': rows}), ensure_ascii=False))
finally:
    ctx.close()
"#;

        let script = script
            .replace("__HOST__", &format!("{:?}", self.config.py_host))
            .replace("__PORT__", &self.config.py_port.to_string())
            .replace("__SEC_FIRM__", &self.config.py_security_firm)
            .replace("__MARKET__", &format!("{:?}", desired_market))
            .replace("__TRD_ENV__", &format!("{:?}", desired_env))
            .replace("__ACC_ID__", &format!("{:?}", desired_acc_id));

        let output = Command::new(&self.config.python_bin)
            .arg("-c")
            .arg(script)
            .output()
            .map_err(|e| anyhow!("failed to run python futu deal_list script: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(anyhow!("python futu deal_list script failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Err(anyhow!("python futu deal_list script returned empty output"));
        }

        let value = parse_json_from_stdout(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu deal_list output: {}", e))?;

        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            let err_text = value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown futu python deal_list error");
            return Err(anyhow!("{}", err_text));
        }

        Ok(value)
    }

    fn get_order_list_via_python(&self) -> Result<serde_json::Value> {
        let desired_acc_id = self.config.account_id.clone().unwrap_or_default();
        let desired_env = self
            .config
            .trd_env
            .clone()
            .unwrap_or_else(|| "SIMULATE".to_string())
            .trim()
            .to_uppercase();
        let desired_market = self
            .config
            .market
            .clone()
            .unwrap_or_else(|| self.config.py_filter_trdmarket.clone())
            .trim()
            .to_uppercase();

        let script = r#"
import json
from futu import *

def _to_builtin(value):
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(v) for v in value]
    return value

host = __HOST__
port = __PORT__
security_firm = SecurityFirm.__SEC_FIRM__
market_text = __MARKET__.strip().upper()
trd_env_text = __TRD_ENV__.strip().upper()
acc_id_text = __ACC_ID__.strip()

market_enum = getattr(TrdMarket, market_text, TrdMarket.US)
trd_env = TrdEnv.SIMULATE if trd_env_text == 'SIMULATE' else TrdEnv.REAL

acc_id = 0
if acc_id_text:
    try:
        acc_id = int(acc_id_text)
    except Exception:
        acc_id = 0

ctx = OpenSecTradeContext(filter_trdmarket=market_enum, host=host, port=port, security_firm=security_firm)
try:
    try:
        ret, data = ctx.order_list_query(trd_env=trd_env, acc_id=acc_id, refresh_cache=True)
    except TypeError:
        ret, data = ctx.order_list_query(trd_env=trd_env, acc_id=acc_id)

    if ret != RET_OK:
        print(json.dumps({'ok': False, 'ret': ret, 'error': str(data)}, ensure_ascii=False))
    else:
        rows = data.to_dict(orient='records') if hasattr(data, 'to_dict') else []
        print(json.dumps(_to_builtin({'ok': True, 'ret': RET_OK, 'data': rows}), ensure_ascii=False))
finally:
    ctx.close()
"#;

        let script = script
            .replace("__HOST__", &format!("{:?}", self.config.py_host))
            .replace("__PORT__", &self.config.py_port.to_string())
            .replace("__SEC_FIRM__", &self.config.py_security_firm)
            .replace("__MARKET__", &format!("{:?}", desired_market))
            .replace("__TRD_ENV__", &format!("{:?}", desired_env))
            .replace("__ACC_ID__", &format!("{:?}", desired_acc_id));

        let output = Command::new(&self.config.python_bin)
            .arg("-c")
            .arg(script)
            .output()
            .map_err(|e| anyhow!("failed to run python futu order_list script: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(anyhow!("python futu order_list script failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Err(anyhow!("python futu order_list script returned empty output"));
        }

        let value = parse_json_from_stdout(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu order_list output: {}", e))?;

        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            let err_text = value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown futu python order_list error");
            return Err(anyhow!("{}", err_text));
        }

        Ok(value)
    }

    fn modify_or_cancel_order_via_python(
        &self,
        req: &FutuModifyOrderRequest,
    ) -> Result<serde_json::Value> {
        let order_id = req.order_id.trim().to_string();
        if order_id.is_empty() {
            return Err(anyhow!("order_id is empty"));
        }

        let action = req.action.trim().to_uppercase();
        let op_text = if action.contains("CANCEL") || action.contains("DELETE") {
            "CANCEL"
        } else {
            "NORMAL"
        };

        let desired_env = req
            .trd_env
            .clone()
            .or_else(|| self.config.trd_env.clone())
            .unwrap_or_else(|| "SIMULATE".to_string())
            .trim()
            .to_uppercase();
        let desired_market = self
            .config
            .market
            .clone()
            .unwrap_or_else(|| self.config.py_filter_trdmarket.clone())
            .trim()
            .to_uppercase();
        let acc_id_text = req
            .acc_id
            .clone()
            .or_else(|| self.config.account_id.clone())
            .unwrap_or_default();
        let qty = req.quantity.unwrap_or(0.0);
        let price = req.price.unwrap_or(0.0);

        let script = r#"
import json
from futu import *

def _to_builtin(value):
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(v) for v in value]
    return value

host = __HOST__
port = __PORT__
security_firm = SecurityFirm.__SEC_FIRM__
market_text = __MARKET__.strip().upper()
trd_env_text = __TRD_ENV__.strip().upper()
acc_id_text = __ACC_ID__.strip()
order_id = __ORDER_ID__.strip()
op_text = __OP__.strip().upper()
qty = float(__QTY__)
price = float(__PRICE__)

market_enum = getattr(TrdMarket, market_text, TrdMarket.US)
trd_env = TrdEnv.SIMULATE if trd_env_text == 'SIMULATE' else TrdEnv.REAL
modify_op = ModifyOrderOp.CANCEL if op_text == 'CANCEL' else ModifyOrderOp.NORMAL

acc_id = 0
if acc_id_text:
    try:
        acc_id = int(acc_id_text)
    except Exception:
        acc_id = 0

ctx = OpenSecTradeContext(filter_trdmarket=market_enum, host=host, port=port, security_firm=security_firm)
try:
    ret, data = ctx.modify_order(
        modify_op,
        order_id,
        qty,
        price,
        trd_env=trd_env,
        acc_id=acc_id,
    )
    if ret != RET_OK:
        print(json.dumps({'ok': False, 'ret': ret, 'error': str(data)}, ensure_ascii=False))
    else:
        rows = data.to_dict(orient='records') if hasattr(data, 'to_dict') else []
        print(json.dumps(_to_builtin({'ok': True, 'ret': RET_OK, 'order_id': order_id, 'data': rows}), ensure_ascii=False))
finally:
    ctx.close()
"#;

        let script = script
            .replace("__HOST__", &format!("{:?}", self.config.py_host))
            .replace("__PORT__", &self.config.py_port.to_string())
            .replace("__SEC_FIRM__", &self.config.py_security_firm)
            .replace("__MARKET__", &format!("{:?}", desired_market))
            .replace("__TRD_ENV__", &format!("{:?}", desired_env))
            .replace("__ACC_ID__", &format!("{:?}", acc_id_text))
            .replace("__ORDER_ID__", &format!("{:?}", order_id))
            .replace("__OP__", &format!("{:?}", op_text))
            .replace("__QTY__", &qty.to_string())
            .replace("__PRICE__", &price.to_string());

        let output = Command::new(&self.config.python_bin)
            .arg("-c")
            .arg(script)
            .output()
            .map_err(|e| anyhow!("failed to run python futu modify_order script: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(anyhow!("python futu modify_order script failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Err(anyhow!("python futu modify_order script returned empty output"));
        }

        let value = parse_json_from_stdout(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu modify_order output: {}", e))?;

        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            let err_text = value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown futu python modify_order error");
            return Err(anyhow!("{}", err_text));
        }

        Ok(value)
    }

    fn get_account_list_via_python(&self) -> Result<serde_json::Value> {
        let script = r#"
import json
from futu import *

def _to_builtin(value):
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(v) for v in value]
    return value

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
    has_acc_id = hasattr(data, 'columns') and ('acc_id' in list(data.columns))
    records = data.to_dict(orient='records') if hasattr(data, 'to_dict') else data
    acc_id_first = None
    acc_id_list = []
    if has_acc_id:
        acc_ids = data['acc_id'].values.tolist()
        acc_id_list = [_to_builtin(v) for v in acc_ids]
        if len(acc_id_list) > 0:
            acc_id_first = acc_id_list[0]

    if ret == RET_OK:
        payload = {
            'ok': True,
            'ret': ret,
            'data': _to_builtin(records),
            'acc_id_first': _to_builtin(acc_id_first),
            'acc_id_list': _to_builtin(acc_id_list),
        }
    else:
        payload = {
            'ok': False,
            'ret': ret,
            'error': str(data),
        }
    print(json.dumps(_to_builtin(payload), ensure_ascii=False))
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

        let v = parse_json_from_stdout(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu account output: {}", e))?;

        Ok(v)
    }

    fn poll_execution_snapshot_via_python(&self) -> Result<FutuExecutionSnapshot> {
        let script = r#"
import json
from futu import *

def _to_builtin(value):
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(v) for v in value]
    return value

host = __HOST__
port = __PORT__
filter_market = TrdMarket.__MARKET__
security_firm = SecurityFirm.__SEC_FIRM__
desired_acc_id = __ACC_ID__
desired_env = __TRD_ENV__.strip().upper()
desired_market = __MARKET_STR__.strip().upper()
market_enum = getattr(TrdMarket, desired_market, TrdMarket.NONE)

def _auth_contains_market(auth, market_text):
    if auth is None:
        return False
    if isinstance(auth, (list, tuple)):
        vals = [str(v).upper() for v in auth]
        return market_text in vals
    text = str(auth).upper()
    return market_text in text

def _is_margin_like(row):
    acc_type = str(row.get('acc_type', '')).upper()
    sim_type = str(row.get('sim_acc_type', '')).upper()
    return ('MARGIN' in acc_type) or ('MARGIN' in sim_type)

ctx = OpenSecTradeContext(
    filter_trdmarket=filter_market,
    host=host,
    port=port,
    security_firm=security_firm,
)
try:
    ret, acc_df = ctx.get_acc_list()
    if ret != RET_OK:
        print(json.dumps({'ok': False, 'ret': ret, 'error': str(acc_df)}, ensure_ascii=False))
    else:
        acc_rows = acc_df.to_dict(orient='records') if hasattr(acc_df, 'to_dict') else []

        selected = None
        if desired_acc_id:
            for row in acc_rows:
                if str(row.get('acc_id', '')) == str(desired_acc_id):
                    selected = row
                    break

        if selected is None:
            target_env = desired_env if desired_env else 'SIMULATE'
            # Preferred: US margin-like simulated account
            for row in acc_rows:
                env_text = str(row.get('trd_env', '')).upper()
                auth = row.get('trdmarket_auth')
                if env_text == target_env and _auth_contains_market(auth, desired_market) and _is_margin_like(row):
                    selected = row
                    break

        if selected is None:
            target_env = desired_env if desired_env else 'SIMULATE'
            # Fallback 1: market-matched simulated account
            for row in acc_rows:
                env_text = str(row.get('trd_env', '')).upper()
                auth = row.get('trdmarket_auth')
                if env_text == target_env and _auth_contains_market(auth, desired_market):
                    selected = row
                    break

        if selected is None:
            target_env = desired_env if desired_env else 'SIMULATE'
            # Fallback 2: any simulated account
            for row in acc_rows:
                env_text = str(row.get('trd_env', '')).upper()
                if env_text == target_env:
                    selected = row
                    break

        if selected is None and len(acc_rows) > 0:
            selected = acc_rows[0]

        if selected is None:
            print(json.dumps({'ok': False, 'ret': -1, 'error': 'No FUTU account available'}, ensure_ascii=False))
        else:
            selected_acc_id = selected.get('acc_id')
            selected_env_text = str(selected.get('trd_env', 'SIMULATE')).upper()
            trd_env = TrdEnv.SIMULATE if selected_env_text == 'SIMULATE' else TrdEnv.REAL

            ret_acc, acc_info = ctx.accinfo_query(trd_env=trd_env, acc_id=selected_acc_id)
            if ret_acc != RET_OK:
                print(json.dumps({'ok': False, 'ret': ret_acc, 'error': str(acc_info)}, ensure_ascii=False))
            else:
                try:
                    ret_pos, pos_info = ctx.position_list_query(
                        trd_env=trd_env,
                        acc_id=selected_acc_id,
                        position_market=market_enum,
                        refresh_cache=True,
                    )
                except TypeError:
                    ret_pos, pos_info = ctx.position_list_query(
                        trd_env=trd_env,
                        acc_id=selected_acc_id,
                        position_market=market_enum,
                    )
                if ret_pos != RET_OK:
                    print(json.dumps({'ok': False, 'ret': ret_pos, 'error': str(pos_info)}, ensure_ascii=False))
                else:
                    acc_rows_info = acc_info.to_dict(orient='records') if hasattr(acc_info, 'to_dict') else []
                    pos_rows = pos_info.to_dict(orient='records') if hasattr(pos_info, 'to_dict') else []
                    acc0 = acc_rows_info[0] if len(acc_rows_info) > 0 else {}
                    payload = {
                        'ok': True,
                        'ret': RET_OK,
                        'selected_acc_id': selected_acc_id,
                        'selected_trd_env': selected_env_text,
                        'selected_market': desired_market,
                        'selected_account': selected,
                        'account_list': acc_rows,
                        'account_info_raw': acc_rows_info,
                        'positions_raw': pos_rows,
                        'cash': acc0.get('cash', 0),
                        'available_funds': acc0.get('avl_withdrawal_cash', acc0.get('cash', 0)),
                        'buying_power': acc0.get('power', acc0.get('max_power', acc0.get('cash', 0))),
                        'positions': pos_rows,
                    }
                    print(json.dumps(_to_builtin(payload), ensure_ascii=False))
finally:
    ctx.close()
"#;

        let desired_acc_id = self.config.account_id.clone().unwrap_or_default();
        let desired_env = self
            .config
            .trd_env
            .clone()
            .unwrap_or_else(|| "SIMULATE".to_string());

        let script = script
            .replace("__HOST__", &format!("{:?}", self.config.py_host))
            .replace("__PORT__", &self.config.py_port.to_string())
            .replace("__MARKET__", &self.config.py_filter_trdmarket)
            .replace("__SEC_FIRM__", &self.config.py_security_firm)
            .replace("__ACC_ID__", &format!("{:?}", desired_acc_id))
            .replace("__TRD_ENV__", &format!("{:?}", desired_env))
            .replace("__MARKET_STR__", &format!("{:?}", self.config.py_filter_trdmarket));

        let output = Command::new(&self.config.python_bin)
            .arg("-c")
            .arg(script)
            .output()
            .map_err(|e| anyhow!("failed to run python futu snapshot script: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(anyhow!("python futu snapshot script failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Err(anyhow!("python futu snapshot script returned empty output"));
        }

        let value = parse_json_from_stdout(&stdout)
            .map_err(|e| anyhow!("failed to parse python futu snapshot output: {}", e))?;

        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
        if !ok {
            let err_text = value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown futu python error");
            return Err(anyhow!("{}", err_text));
        }

        let cash_usd = json_get_number(&value, &["cash_usd", "cash", "available_cash", "available_funds"])
            .unwrap_or(0.0);
        let buying_power_usd = json_get_number(
            &value,
            &["buying_power", "buying_power_usd", "max_power", "available_funds"],
        )
        .unwrap_or(cash_usd);
        let positions = extract_positions(&value);
        let selected_acc_id = value
            .get("selected_acc_id")
            .and_then(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.as_i64().map(|n| n.to_string()))
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            });
        let selected_trd_env = value
            .get("selected_trd_env")
            .and_then(|v| v.as_str().map(|s| s.to_string()));
        let selected_market = value
            .get("selected_market")
            .and_then(|v| v.as_str().map(|s| s.to_string()));
        let opend_account_list = value.get("account_list").cloned();
        let opend_selected_account = value.get("selected_account").cloned();
        let opend_account_info_raw = value.get("account_info_raw").cloned();
        let opend_positions_raw = value.get("positions_raw").cloned();

        Ok(FutuExecutionSnapshot {
            cash_usd,
            buying_power_usd,
            positions,
            selected_acc_id,
            selected_trd_env,
            selected_market,
            opend_account_list,
            opend_selected_account,
            opend_account_info_raw,
            opend_positions_raw,
        })
    }
}

fn parse_json_from_stdout(stdout: &str) -> Result<serde_json::Value> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(stdout) {
        return Ok(v);
    }

    let start = stdout
        .find('{')
        .ok_or_else(|| anyhow!("no json object start found in stdout"))?;
    let end = stdout
        .rfind('}')
        .ok_or_else(|| anyhow!("no json object end found in stdout"))?;
    if end <= start {
        return Err(anyhow!("invalid json object range in stdout"));
    }

    serde_json::from_str::<serde_json::Value>(&stdout[start..=end])
        .map_err(|e| anyhow!("{}", e))
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

fn json_get_bool(value: &serde_json::Value, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(raw) = value.get(*key) {
            if let Some(v) = raw.as_bool() {
                return Some(v);
            }
            if let Some(text) = raw.as_str() {
                let normalized = text.trim().to_ascii_lowercase();
                if normalized == "true" || normalized == "1" || normalized == "yes" {
                    return Some(true);
                }
                if normalized == "false" || normalized == "0" || normalized == "no" {
                    return Some(false);
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
        let avg_cost = json_get_number(
            &row,
            &[
                "average_cost",
                "avg_cost",
                "avg_price",
                "position_cost_price",
                "holding_cost",
            ],
        )
        .or_else(|| {
            if json_get_bool(&row, &["cost_price_valid"]).unwrap_or(true) {
                json_get_number(&row, &["cost_price", "diluted_cost", "cost"])
            } else {
                None
            }
        })
        .unwrap_or(0.0);
        let market_price = json_get_number(
            &row,
            &[
                "market_price",
                "nominal_price",
                "last_price",
                "price",
                "current_price",
            ],
        )
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

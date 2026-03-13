const api = async (url, options = {}) => {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
};

const tabs = document.querySelectorAll('.tab-btn');
const panels = document.querySelectorAll('.tab-panel');

const applyRuntimeChips = (state) => {
  const backendChip = document.getElementById('backendChip');
  const backendDot = document.getElementById('backendDot');
  const precisionChip = document.getElementById('precisionChip');
  const precisionDot = document.getElementById('precisionDot');
  const precisionStatusChip = document.getElementById('precisionStatusChip');

  const backend = String(state?.compute_backend || state?.forecast?.last_request?.compute_backend || '').trim();
  if (backendChip) {
    if (backend) {
      const lower = backend.toLowerCase();
      const label = lower === 'directml'
        ? 'DirectML'
        : lower === 'cuda'
          ? 'CUDA'
          : lower === 'cpu'
            ? 'CPU'
            : (backend.charAt(0).toUpperCase() + backend.slice(1));
      backendChip.textContent = `Backend: ${label}`;
      if (backendDot) backendDot.style.background = lower === 'cpu' ? 'var(--muted)' : 'var(--accent)';
    } else {
      backendChip.textContent = 'Backend: --';
      if (backendDot) backendDot.style.background = 'var(--muted-2)';
    }
  }

  if (!precisionChip) return;
  const requested = String(state?.directml_requested_precision || '').trim().toLowerCase();
  const active = String(state?.directml_active_precision || '').trim().toLowerCase();
  const modelPath = String(state?.directml_model_path || '').trim();

  if (String(backend || '').toLowerCase() !== 'directml') {
    precisionChip.textContent = 'Precision: --';
    if (precisionDot) precisionDot.style.background = 'var(--muted-2)';
    if (precisionStatusChip) {
      precisionStatusChip.title = 'Precision status is shown for DirectML ONNX inference.';
    }
    return;
  }

  precisionChip.textContent = `Precision: ${active ? active.toUpperCase() : '--'}`;
  if (precisionDot) {
    precisionDot.style.background = active === 'fp16'
      ? 'var(--up)'
      : active === 'fp32'
        ? 'var(--gold)'
        : 'var(--muted-2)';
  }
  if (precisionStatusChip) {
    const modelName = modelPath ? modelPath.split(/[\\/]/).pop() : '--';
    const fallbackHint = requested && active && requested !== active
      ? ` | requested ${requested.toUpperCase()} but loaded ${active.toUpperCase()}`
      : '';
    precisionStatusChip.title = `DirectML model: ${modelName}${fallbackHint}${modelPath ? ` | path: ${modelPath}` : ''}`;
  }
};

const switchTabByName = (tabName) => {
  const content = document.querySelector('.content');
  for (const b of tabs) b.classList.remove('active');
  const targetBtn = Array.from(tabs).find(b => b.dataset.tab === tabName);
  if (targetBtn) targetBtn.classList.add('active');
  for (const p of panels) p.classList.add('hidden');
  document.getElementById(`tab-${tabName}`)?.classList.remove('hidden');
  content?.classList.toggle('futu-scroll-layout', tabName === 'futu');
  if (content) {
    content.scrollTop = 0;
    content.scrollLeft = 0;
  }
  window.scrollTo({ top: 0, left: 0, behavior: 'auto' });

  if (tabName === 'forecast') {
    fChart.resize();
    fChart.fit();
  }
  if (tabName === 'paper') {
    paperChart.resize();
    paperChart.fit();
  }
  if (tabName === 'futu') {
    futuChart.resize();
    futuChart.fit();
  }
  if (tabName === 'backtest') {
    backtestChart.resize();
    backtestChart.fit();
  }
};
let lastPortfolio = null;
let lastPaperSeries = [];
let latestQuoteMap = new Map();
let latestQuoteExchangeTsMap = new Map();
let latestQuoteDeltaMap = new Map();
let lastQuoteRequestSymbols = [];
let lastQuoteMissingSymbols = [];
let lastQuoteMissingUpdatedAtMs = 0;
let lastQuotesAt = 0;
let lastQuotesStampText = '--';
let lastExchangeStampText = '--';
const normalizeQuoteRequestSymbol = (symbol) => {
  const upper = String(symbol || '').trim().toUpperCase();
  if (!upper) return '';
  const usMatch = upper.match(/^US\.(.+)$/);
  if (usMatch && usMatch[1]) return usMatch[1];
  return upper;
};
const getLatestQuotePrice = (symbol) => {
  const upper = String(symbol || '').trim().toUpperCase();
  if (!upper) return null;
  const direct = Number(latestQuoteMap.get(upper));
  if (Number.isFinite(direct)) return direct;
  const normalized = normalizeQuoteRequestSymbol(upper);
  const normalizedPx = Number(latestQuoteMap.get(normalized));
  return Number.isFinite(normalizedPx) ? normalizedPx : null;
};
const getLatestQuoteExchangeTs = (symbol) => {
  const upper = String(symbol || '').trim().toUpperCase();
  if (!upper) return null;
  const direct = Number(latestQuoteExchangeTsMap.get(upper));
  if (Number.isFinite(direct) && direct > 0) return direct;
  const normalized = normalizeQuoteRequestSymbol(upper);
  const normalizedTs = Number(latestQuoteExchangeTsMap.get(normalized));
  return Number.isFinite(normalizedTs) && normalizedTs > 0 ? normalizedTs : null;
};
const getLatestQuoteDelta = (symbol) => {
  const upper = String(symbol || '').trim().toUpperCase();
  if (!upper) return null;
  return latestQuoteDeltaMap.get(upper)
    || latestQuoteDeltaMap.get(normalizeQuoteRequestSymbol(upper))
    || null;
};
const actionStatus = document.getElementById('actionStatus');
const quotesAsOf = document.getElementById('quotesAsOf');
const backtestStartBtn = document.getElementById('backtestStart');
const backtestStopBtn = document.getElementById('backtestStop');
const backtestSymbolsInput = document.getElementById('backtestSymbols');
const backtestCapitalInput = document.getElementById('backtestCapital');
const backtestDaysInput = document.getElementById('backtestDays');
const backtestRebalanceDaysInput = document.getElementById('backtestRebalanceDays');
const backtestSummaryMeta = document.getElementById('backtestSummaryMeta');
const paperStartBtn = document.getElementById('paperStart');
const paperLoadBtn = document.getElementById('paperLoad');
const paperFilePicker = document.getElementById('paperFilePicker');
const paperPauseBtn = document.getElementById('paperPause');
const paperResumeBtn = document.getElementById('paperResume');
const paperStopBtn = document.getElementById('paperStop');
const paperTargetInput = document.getElementById('paperTargetInput');
const paperTargetAddBtn = document.getElementById('paperTargetAdd');
const paperTargetApplyBtn = document.getElementById('paperTargetApply');
const paperApplyNowCheckbox = document.getElementById('paperApplyNow');
const paperOptTimeInput = document.getElementById('paperOptTime');
const paperOptWeekdayChecks = Array.from(document.querySelectorAll('[data-paper-opt-weekday]'));
const paperTargetChips = document.getElementById('paperTargetChips');
const paperTargetCount = document.getElementById('paperTargetCount');
const paperTargetDirtyBadge = document.getElementById('paperTargetDirtyBadge');
const futuStartBtn = document.getElementById('futuStart');
const futuLoadBtn = document.getElementById('futuLoad');
const futuStopBtn = document.getElementById('futuStop');
const futuLoadPathInput = document.getElementById('futuLoadPath');
const futuFilePicker = document.getElementById('futuFilePicker');
const futuRecentLoadsBox = document.getElementById('futuRecentLoads');
const futuAccountSelect = document.getElementById('futuAccountSelect');
const futuAccountApplyBtn = document.getElementById('futuAccountApply');
const futuAccountApplyHint = document.getElementById('futuAccountApplyHint');
const strategyPayloadPreview = document.getElementById('strategyPayloadPreview');
const strategyCopyPayloadBtn = document.getElementById('strategyCopyPayload');
const strategyPayloadHint = document.getElementById('strategyPayloadHint');
const paperRangePadBand = document.getElementById('paperRangePadBand');
const paperLegend = document.getElementById('paperLegend');
const legendPortfolio = document.getElementById('legendPortfolio');
const legendPortfolioPnl = document.getElementById('legendPortfolioPnl');
const legendBenchmark = document.getElementById('legendBenchmark');
const legendBenchmarkPnl = document.getElementById('legendBenchmarkPnl');
const legendSpread = document.getElementById('legendSpread');
const legendUpdated = document.getElementById('legendUpdated');
let legendForecastCurrentDate = document.getElementById('legendForecastCurrentDate');
let legendForecastCurrentPrice = document.getElementById('legendForecastCurrentPrice');
let legendForecastTargetDate = document.getElementById('legendForecastTargetDate');
let legendForecastP50 = document.getElementById('legendForecastP50');
let legendForecastP10 = document.getElementById('legendForecastP10');
let legendForecastP90 = document.getElementById('legendForecastP90');
let paperMetricsByTime = new Map();
let futuMetricsByTime = new Map();
let backtestMetricsByTime = new Map();
let lastForecastContext = null;
let forecastBatchResults = new Map(); // symbol → {data, selectedSymbol}
let forecastSelectedSymbol = null;
let latestBacktestStatus = null;
let paperTradeHistory = [];
let paperTradeSeenKeys = new Set();
let paperCostBasis = new Map();
let selectedTradeFilter = 'all';
let tradeSearchText = '';
let futuActivityFilterText = '';
let futuActivityRangeDays = 30;
let futuOpenOrderEditState = {
  orderId: '',
  qty: '',
  price: '',
};
let selectedPaperRangeDays = 0.5;
let selectedFutuRangeDays = 0.5;
let selectedBacktestRangeDays = 365;
let selectedFutuCurveMode = 'account';
let paperSessionStartMs = null;
let manualPaperTargets = [];
let paperTargetsDirty = false;
let paperStartOptimizing = false;
let paperApplyManualOptimizing = false;
let paperApplyAutoOptimizing = false;
let paperOptSaveTimer = null;
let paperOptSaveInFlight = false;
let paperOptSavePending = false;
let dataSourceLastValue = '';
let dataSourceSwitchLog = [];
let lastWsDiagnostics = null;
let lastLiveFetchDiagnostics = null;
let wsTimeoutCount = 0;
let wsTimeoutActive = false;
let wsLastTimeoutAtMs = 0;
let wsLastTimeoutError = '--';
let wsNoTimeoutStreak = 0;
const FORECAST_BATCH_CACHE_KEY = 'diffstock:forecast-batch:v2';
const FORECAST_META_CACHE_KEY = 'diffstock:forecast-meta:v1';
const FUTU_RECENT_LOADS_KEY = 'diffstock:futu-recent-loads:v1';
const FUTU_RECENT_LOADS_MAX = 8;
const FUTU_DEFAULT_ACC_ID = '';
let futuRecentLoads = [];
let futuAccountListCache = [];
let futuAccountListInFlight = false;
let paperFullContext = {
  portfolioSeries: [],
  benchmarkSeries: [],
  metricsByTime: new Map(),
  latest: null,
};
let lastPaperChartDataSignature = '';
let futuFullContext = {
  portfolioSeries: [],
  strategyFtSeries: [],
  benchmarkSeries: [],
  metricsByTime: new Map(),
  latest: null,
};
let futuRenderContext = {
  portfolioSeries: [],
  strategyFtSeries: [],
  benchmarkSeries: [],
  metricsByTime: new Map(),
  latest: null,
};
let backtestFullContext = {
  portfolioSeries: [],
  benchmarkSeries: [],
  metricsByTime: new Map(),
  latest: null,
};
let latestPaperStatus = null;
let latestFutuStatus = null;
const backtestRangeButtons = Array.from(document.querySelectorAll('[data-backtest-range-days]'));
const paperRangeButtons = Array.from(document.querySelectorAll('[data-paper-range-days]'));
const futuRangeButtons = Array.from(document.querySelectorAll('[data-futu-range-days]'));
const futuCurveModeButtons = Array.from(document.querySelectorAll('[data-futu-curve-mode]'));

const futuLegend = document.getElementById('futuLegend');
const futuLegendTitle = document.getElementById('futuLegendTitle');
const futuLegendPortfolioKey = document.getElementById('futuLegendPortfolioKey');
const futuLegendPortfolioPnlKey = document.getElementById('futuLegendPortfolioPnlKey');
const futuLegendPortfolio = document.getElementById('futuLegendPortfolio');
const futuLegendPortfolioPnl = document.getElementById('futuLegendPortfolioPnl');
const futuLegendBenchmark = document.getElementById('futuLegendBenchmark');
const futuLegendBenchmarkPnl = document.getElementById('futuLegendBenchmarkPnl');
const futuLegendSpread = document.getElementById('futuLegendSpread');
const futuLegendUpdated = document.getElementById('futuLegendUpdated');

const backtestLegend = document.getElementById('backtestLegend');
const backtestLegendPortfolio = document.getElementById('backtestLegendPortfolio');
const backtestLegendPortfolioPnl = document.getElementById('backtestLegendPortfolioPnl');
const backtestLegendBenchmark = document.getElementById('backtestLegendBenchmark');
const backtestLegendBenchmarkPnl = document.getElementById('backtestLegendBenchmarkPnl');
const backtestLegendSpread = document.getElementById('backtestLegendSpread');
const backtestLegendUpdated = document.getElementById('backtestLegendUpdated');

const tradeFilterButtons = Array.from(document.querySelectorAll('[data-trade-filter]'));
const tradeSearchInput = document.getElementById('tradeSearchInput');
const futuActivityFilterInput = document.getElementById('futuActivityFilterInput');
const futuActivityRangeSelect = document.getElementById('futuActivityRangeSelect');
const futuCapitalCapInput = document.getElementById('futuCapitalCapInput');
const futuCapitalCapApplyBtn = document.getElementById('futuCapitalCapApply');
const futuCapitalCapHint = document.getElementById('futuCapitalCapHint');
const futuStrategyCapitalInput = document.getElementById('futuStrategyCapitalInput');
const futuStrategyCapitalApplyBtn = document.getElementById('futuStrategyCapitalApply');
const futuStrategyCapitalHint = document.getElementById('futuStrategyCapitalHint');
const futuManualSymbolInput = document.getElementById('futuManualSymbol');
const futuManualSideSelect = document.getElementById('futuManualSide');
const futuManualQtyInput = document.getElementById('futuManualQty');
const futuManualPriceInput = document.getElementById('futuManualPrice');
const futuManualTifSelect = document.getElementById('futuManualTif');
const futuManualSessionSelect = document.getElementById('futuManualSession');
const futuManualRemarkInput = document.getElementById('futuManualRemark');
const futuManualSubmitBtn = document.getElementById('futuManualSubmit');
const futuManualOrderHint = document.getElementById('futuManualOrderHint');

const saveForecastBatchCache = () => {
  try {
    const symbolsInput = document.getElementById('fSymbol')?.value || '';
    const horizon = Number(document.getElementById('fHorizon')?.value || 10);
    const simulations = Number(document.getElementById('fSims')?.value || 500);
    const meta = {
      savedAt: Date.now(),
      symbolsInput,
      horizon,
      simulations,
      selectedSymbol: forecastSelectedSymbol,
    };
    localStorage.setItem(FORECAST_META_CACHE_KEY, JSON.stringify(meta));

    if (forecastBatchResults.size === 0) {
      localStorage.removeItem(FORECAST_BATCH_CACHE_KEY);
      return;
    }
    const payload = {
      savedAt: Date.now(),
      symbolsInput,
      horizon,
      simulations,
      selectedSymbol: forecastSelectedSymbol,
      results: Array.from(forecastBatchResults.entries()).map(([symbol, data]) => ({ symbol, data })),
    };
    localStorage.setItem(FORECAST_BATCH_CACHE_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('saveForecastBatchCache failed', err);
  }
};

const loadForecastMetaCache = () => {
  try {
    const raw = localStorage.getItem(FORECAST_META_CACHE_KEY);
    if (!raw) return null;
    const meta = JSON.parse(raw);
    if (!meta || typeof meta !== 'object') return null;
    return meta;
  } catch {
    return null;
  }
};

const getForecastTimestampMs = (result) => {
  if (!result || typeof result !== 'object') return 0;
  const raw = String(result.forecasted_at || '').trim();
  if (!raw) return 0;
  const ms = new Date(raw).getTime();
  return Number.isFinite(ms) ? ms : 0;
};

const loadLocalForecastBatchPayload = () => {
  try {
    const raw = localStorage.getItem(FORECAST_BATCH_CACHE_KEY);
    if (!raw) return null;
    const payload = JSON.parse(raw);
    if (!payload || typeof payload !== 'object') return null;
    const rows = Array.isArray(payload.results) ? payload.results : [];
    if (rows.length === 0) return null;
    return payload;
  } catch {
    return null;
  }
};

const mergeForecastBatchFromLocalByTimestamp = (requestState = null) => {
  const payload = loadLocalForecastBatchPayload();
  if (!payload) return false;

  const localMap = new Map();
  for (const row of payload.results || []) {
    if (!row || !row.symbol || !row.data) continue;
    localMap.set(String(row.symbol).toUpperCase(), row.data);
  }
  if (localMap.size === 0) return false;

  let changed = false;

  if (forecastBatchResults.size === 0) {
    forecastBatchResults = new Map(localMap);
    changed = true;
  } else {
    for (const [symbol, localData] of localMap.entries()) {
      const existing = forecastBatchResults.get(symbol);
      if (!existing) {
        forecastBatchResults.set(symbol, localData);
        changed = true;
        continue;
      }
      const existingTs = getForecastTimestampMs(existing);
      const localTs = getForecastTimestampMs(localData);
      if (localTs > existingTs) {
        forecastBatchResults.set(symbol, localData);
        changed = true;
      }
    }
  }

  if (forecastBatchResults.size === 0) return false;

  const reqSymbol = requestState?.symbol ? String(requestState.symbol).toUpperCase() : null;
  const preferredLocal = payload.selectedSymbol ? String(payload.selectedSymbol).toUpperCase() : null;
  const first = forecastBatchResults.keys().next().value;
  if (reqSymbol && forecastBatchResults.has(reqSymbol)) {
    forecastSelectedSymbol = reqSymbol;
  } else if (forecastSelectedSymbol && forecastBatchResults.has(forecastSelectedSymbol)) {
    // keep existing selection
  } else if (preferredLocal && forecastBatchResults.has(preferredLocal)) {
    forecastSelectedSymbol = preferredLocal;
  } else {
    forecastSelectedSymbol = first;
  }

  const allSymbols = Array.from(forecastBatchResults.keys());
  const inputEl = document.getElementById('fSymbol');
  const horizonEl = document.getElementById('fHorizon');
  const simsEl = document.getElementById('fSims');
  if (inputEl) inputEl.value = allSymbols.join(',');
  if (horizonEl && Number.isFinite(Number(requestState?.horizon || payload.horizon))) {
    horizonEl.value = Number(requestState?.horizon || payload.horizon);
  }
  if (simsEl && Number.isFinite(Number(requestState?.simulations || payload.simulations))) {
    simsEl.value = Number(requestState?.simulations || payload.simulations);
  }
  syncQuickChips();

  const selectedData = forecastBatchResults.get(forecastSelectedSymbol);
  if (selectedData) {
    applyForecastDataToChart(selectedData);
    renderFcKpiCards(selectedData);
  }
  renderBatchGrid();
  saveForecastBatchCache();

  return changed;
};

const restoreForecastBatchCache = () => {
  try {
    const raw = localStorage.getItem(FORECAST_BATCH_CACHE_KEY);
    if (!raw) return false;
    const payload = JSON.parse(raw);
    const rows = Array.isArray(payload?.results) ? payload.results : [];
    if (rows.length === 0) return false;

    forecastBatchResults.clear();
    for (const row of rows) {
      if (!row || !row.symbol || !row.data) continue;
      forecastBatchResults.set(String(row.symbol).toUpperCase(), row.data);
    }
    if (forecastBatchResults.size === 0) return false;

    const inputEl = document.getElementById('fSymbol');
    const horizonEl = document.getElementById('fHorizon');
    const simsEl = document.getElementById('fSims');
    if (inputEl && payload.symbolsInput) inputEl.value = payload.symbolsInput;
    if (horizonEl && Number.isFinite(Number(payload.horizon))) horizonEl.value = Number(payload.horizon);
    if (simsEl && Number.isFinite(Number(payload.simulations))) simsEl.value = Number(payload.simulations);
    syncQuickChips();

    const preferred = payload.selectedSymbol ? String(payload.selectedSymbol).toUpperCase() : null;
    const first = forecastBatchResults.keys().next().value;
    forecastSelectedSymbol = preferred && forecastBatchResults.has(preferred) ? preferred : first;

    const selectedData = forecastBatchResults.get(forecastSelectedSymbol);
    if (selectedData) {
      applyForecastDataToChart(selectedData);
      renderFcKpiCards(selectedData);
    }
    renderBatchGrid();
    return true;
  } catch (err) {
    console.warn('restoreForecastBatchCache failed', err);
    return false;
  }
};

const ensureForecastLegendElements = () => {
  const chartHost = document.getElementById('forecastChart');
  if (!chartHost) return false;

  let wrap = chartHost.parentElement;
  if (!wrap || !wrap.classList.contains('chart-wrap')) {
    const newWrap = document.createElement('div');
    newWrap.className = 'chart-wrap';
    chartHost.parentNode.insertBefore(newWrap, chartHost);
    newWrap.appendChild(chartHost);
    wrap = newWrap;
  }

  let legend = document.getElementById('forecastLegend');
  if (!legend) {
    legend = document.createElement('div');
    legend.id = 'forecastLegend';
    legend.className = 'chart-legend';
    legend.innerHTML = `
      <div class="legend-title">Forecast Metrics</div>
      <div class="legend-row"><span class="legend-key">Current Date</span><span class="legend-val" id="legendForecastCurrentDate">--</span></div>
      <div class="legend-row"><span class="legend-key">Current Price</span><span class="legend-val" id="legendForecastCurrentPrice">--</span></div>
      <div class="legend-row"><span class="legend-key">Target Date</span><span class="legend-val" id="legendForecastTargetDate">--</span></div>
      <div class="legend-row"><span class="legend-key"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#5b9cf6;margin-right:5px;vertical-align:middle"></span>P50 Median</span><span class="legend-val" id="legendForecastP50">--</span></div>
      <div class="legend-row"><span class="legend-key"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#ff6b7a;margin-right:5px;vertical-align:middle"></span>P10 Bear</span><span class="legend-val" id="legendForecastP10">--</span></div>
      <div class="legend-row"><span class="legend-key"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#2ee8b0;margin-right:5px;vertical-align:middle"></span>P90 Bull</span><span class="legend-val" id="legendForecastP90">--</span></div>
    `;
    wrap.appendChild(legend);
  }

  legend.style.zIndex = '60';
  legend.style.pointerEvents = 'none';

  legendForecastCurrentDate = document.getElementById('legendForecastCurrentDate');
  legendForecastCurrentPrice = document.getElementById('legendForecastCurrentPrice');
  legendForecastTargetDate = document.getElementById('legendForecastTargetDate');
  legendForecastP50 = document.getElementById('legendForecastP50');
  legendForecastP10 = document.getElementById('legendForecastP10');
  legendForecastP90 = document.getElementById('legendForecastP90');

  return !!(
    legendForecastCurrentDate && legendForecastCurrentPrice && legendForecastTargetDate &&
    legendForecastP50 && legendForecastP10 && legendForecastP90
  );
};

ensureForecastLegendElements();

const showForecastLegend = () => {
  const legend = document.getElementById('forecastLegend');
  if (!legend) return;
  legend.classList.add('visible');
};

const hideForecastLegend = () => {
  const legend = document.getElementById('forecastLegend');
  if (!legend) return;
  legend.classList.remove('visible');
};

const setStatus = (text, type = '') => {
  if (!actionStatus) return;
  actionStatus.textContent = text;
  actionStatus.classList.remove('ok', 'err');
  if (type) actionStatus.classList.add(type);
};

const renderQuotesAsOf = () => {
  if (!quotesAsOf) return;
  const missingCount = Array.isArray(lastQuoteMissingSymbols) ? lastQuoteMissingSymbols.length : 0;
  const missingText = missingCount > 0 ? ` | Missing: ${missingCount}` : '';
  quotesAsOf.textContent = `Updated: ${lastQuotesStampText} | Exchange: ${lastExchangeStampText}${missingText}`;
};

const formatDiagTs = (ms) => {
  if (!Number.isFinite(ms)) return '--';
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
};

const WS_EVENT_TIMEOUT_MS = 120000;
const WS_RECOVERY_FETCH_WINDOW_MS = 120000;

const getWsTimeoutSeconds = (diag) => {
  if (!diag || typeof diag !== 'object') return null;
  const lastText = Number(diag.last_text_at_ms || 0);
  const lastData = Number(diag.last_data_event_at_ms || 0);
  const lastSeen = Number.isFinite(lastText) && lastText > 0
    ? lastText
    : (Number.isFinite(lastData) && lastData > 0 ? lastData : 0);
  if (!Number.isFinite(lastSeen) || lastSeen <= 0) return null;
  return Math.max(0, Math.floor((Date.now() - lastSeen) / 1000));
};

const classifyWsDiagnostics = (diag) => {
  if (!diag || typeof diag !== 'object') return '无事件';
  const lastText = Number(diag.last_text_at_ms || 0);
  const hasText = Number.isFinite(lastText) && lastText > 0;
  const lastData = Number(diag.last_data_event_at_ms || 0);
  const lastParse = Number(diag.last_parse_failure_at_ms || 0);
  const parseFailures = Number(diag.parse_failures_total || 0);
  const timeoutSec = getWsTimeoutSeconds(diag);
  if (parseFailures > 0 && (!hasText || (lastParse && lastParse >= lastText))) {
    return '解析失败';
  }
  if (!hasText) {
    return '无事件';
  }
  if (diag.connected && Number.isFinite(timeoutSec) && timeoutSec * 1000 >= WS_EVENT_TIMEOUT_MS) {
    return '超时(120s)';
  }
  return '正常';
};

const updateWsTimeoutState = (diag, wsStatus) => {
  const timeoutSec = getWsTimeoutSeconds(diag);
  const isTimeout = wsStatus === '超时(120s)';
  if (isTimeout && !wsTimeoutActive) {
    wsTimeoutCount += 1;
    wsLastTimeoutAtMs = Date.now();
    wsLastTimeoutError = Number.isFinite(timeoutSec)
      ? `WS事件超时(${timeoutSec}s >= 120s)`
      : 'WS事件超时(无有效数据时间戳)';
  }
  if (isTimeout) {
    wsNoTimeoutStreak = 0;
    wsTimeoutActive = true;
    return;
  }
  if (wsTimeoutCount > 0) {
    wsNoTimeoutStreak = Math.min(3, wsNoTimeoutStreak + 1);
  }
  wsTimeoutActive = isTimeout;
};

const hasRecentLiveFetchRecovery = (fetchDiag) => {
  if (!fetchDiag || typeof fetchDiag !== 'object') return false;
  const atMs = Number(fetchDiag.last_prefetch_at_ms || 0);
  const success = Number(fetchDiag.last_prefetch_success_count || 0);
  if (!Number.isFinite(atMs) || atMs <= 0 || success <= 0) return false;
  return (Date.now() - atMs) <= WS_RECOVERY_FETCH_WINDOW_MS;
};

const classifyEffectiveWsStatus = (rawWsStatus, fetchDiag) => {
  if (rawWsStatus !== '超时(120s)') return rawWsStatus;
  if (hasRecentLiveFetchRecovery(fetchDiag)) return '正常(回补)';
  return rawWsStatus;
};

const getWsRecoveryPhase = (wsStatus) => {
  if (wsStatus === '超时(120s)') return 'timeout';
  if (wsTimeoutCount > 0 && wsNoTimeoutStreak >= 3) return 'stable';
  if (wsTimeoutCount > 0) return 'recovering';
  return 'normal';
};

const setDataSourceChip = (sourceRaw, wsConnected = false, wsDiagnostics = null, liveFetchDiagnostics = null) => {
  const dataSourceStatusChip = document.getElementById('dataSourceStatusChip');
  const dataSourceChip = document.getElementById('dataSourceChip');
  const dataSourceDot = document.getElementById('dataSourceDot');
  if (!dataSourceChip) return;
  if (wsDiagnostics && typeof wsDiagnostics === 'object') {
    lastWsDiagnostics = wsDiagnostics;
  }
  if (liveFetchDiagnostics && typeof liveFetchDiagnostics === 'object') {
    lastLiveFetchDiagnostics = liveFetchDiagnostics;
  }

  const source = String(sourceRaw || '').trim();
  const wsStatusRaw = classifyWsDiagnostics(lastWsDiagnostics);
  const wsStatus = classifyEffectiveWsStatus(wsStatusRaw, lastLiveFetchDiagnostics);
  updateWsTimeoutState(lastWsDiagnostics, wsStatus);
  if (!source || source.toLowerCase() === 'unknown') {
    dataSourceChip.textContent = 'Data: --';
    if (dataSourceDot) dataSourceDot.style.background = 'var(--muted-2)';
    if (dataSourceStatusChip) {
      const wsHint = wsConnected ? ' | WS connected (not freshest)' : '';
      dataSourceStatusChip.title = `Data source status${wsHint} | WS: ${wsStatus} | WS原始: ${wsStatusRaw} (click for details)`;
    }
    return;
  }

  const label = source;
  const lower = label.toLowerCase();
  dataSourceChip.textContent = `Data: ${label}`;

  if (label !== dataSourceLastValue) {
    dataSourceLastValue = label;
    const now = new Date();
    const ts = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
    dataSourceSwitchLog.unshift(`${ts} ${label}`);
    dataSourceSwitchLog = dataSourceSwitchLog.slice(0, 5);
  }

  if (dataSourceStatusChip) {
    const latest = dataSourceSwitchLog.length ? dataSourceSwitchLog[0] : '--';
    const wsHint = wsConnected && !lower.includes('polygon-ws') ? ' | WS connected (not freshest)' : '';
    dataSourceStatusChip.title = `Data source status: ${label}${wsHint} | WS: ${wsStatus} | WS原始: ${wsStatusRaw} | WS恢复计数: ${wsNoTimeoutStreak}/3 | latest switch: ${latest} | click for details`;
  }

  if (dataSourceDot) {
    const phase = getWsRecoveryPhase(wsStatus);
    if (phase === 'timeout') {
      dataSourceDot.style.background = 'var(--down)';
    } else if (phase === 'recovering') {
      dataSourceDot.style.background = 'var(--gold)';
    } else if (phase === 'stable') {
      dataSourceDot.style.background = 'var(--up)';
    } else if (lower.includes('polygon-ws')) {
      dataSourceDot.style.background = 'var(--up)';
    } else if (lower.includes('polygon')) {
      dataSourceDot.style.background = 'var(--accent)';
    } else if (lower.includes('yfinance')) {
      dataSourceDot.style.background = 'var(--gold)';
    } else {
      dataSourceDot.style.background = 'var(--muted-2)';
    }
  }
};

const showDataSourceLogPopup = () => {
  const old = document.getElementById('dataDiagOverlay');
  if (old) old.remove();

  const lines = dataSourceSwitchLog.length
    ? dataSourceSwitchLog.map((line, idx) => `${idx + 1}. ${line}`).join('\n')
    : 'No source switch history yet.';

  const diag = lastWsDiagnostics || {};
  const fetchDiag = lastLiveFetchDiagnostics || {};
  const wsStatusRaw = classifyWsDiagnostics(diag);
  const wsStatus = classifyEffectiveWsStatus(wsStatusRaw, fetchDiag);
  const wsPhase = getWsRecoveryPhase(wsStatus);
  const timeoutSec = getWsTimeoutSeconds(diag);
  const timeoutText = !Number.isFinite(timeoutSec)
    ? '无数据事件'
    : (timeoutSec * 1000 >= WS_EVENT_TIMEOUT_MS ? `已超时 (${timeoutSec}s)` : `未超时 (${timeoutSec}s)`);
  const missingSymbolsText = Array.isArray(lastQuoteMissingSymbols) && lastQuoteMissingSymbols.length
    ? lastQuoteMissingSymbols.join(', ')
    : '--';
  const historySourceLogText = Array.isArray(fetchDiag.history_source_log) && fetchDiag.history_source_log.length
    ? fetchDiag.history_source_log.join('\n')
    : '--';
  const esc = (v) => String(v ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
  const overlay = document.createElement('div');
  overlay.id = 'dataDiagOverlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.22);backdrop-filter:blur(3px);-webkit-backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;z-index:9999;';

  const panel = document.createElement('div');
  panel.style.cssText = 'width:min(880px,92vw);max-height:82vh;overflow:auto;background:color-mix(in srgb, var(--card) 78%, transparent);border:1px solid color-mix(in srgb, var(--line) 85%, transparent);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);box-shadow:0 8px 28px rgba(0,0,0,.24);border-radius:12px;padding:14px;';
  panel.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
      <strong>Data Source Diagnostics</strong>
      <button id="dataDiagCloseBtn" style="background:var(--card-2);color:var(--fg);border:1px solid var(--line);border-radius:8px;padding:4px 10px;cursor:pointer;">Close</button>
    </div>
    <div style="margin-bottom:10px;color:var(--muted);white-space:pre-wrap;">${esc(lines)}</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead>
        <tr>
          <th style="text-align:left;border-bottom:1px solid var(--line);padding:6px;">Item</th>
          <th style="text-align:left;border-bottom:1px solid var(--line);padding:6px;">Value</th>
        </tr>
      </thead>
      <tbody>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">WS状态</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(wsStatus)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">WS原始状态</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(wsStatusRaw)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">WS连接</td><td style="padding:6px;border-bottom:1px solid var(--line);">${diag.connected ? '已连接' : '未连接'}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">后端超时累计</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(diag.timeout_strikes_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">后端Failover状态</td><td style="padding:6px;border-bottom:1px solid var(--line);">${diag.failover_active ? '已切到REST/Snapshot' : 'WS优先'}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">文本消息总数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(diag.text_messages_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">状态消息总数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(diag.status_messages_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">数据事件总数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(diag.data_events_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">有效价格事件</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(diag.accepted_price_events_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">无效价格事件</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(diag.dropped_invalid_price_events_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">解析失败次数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(diag.parse_failures_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">超时判定窗口</td><td style="padding:6px;border-bottom:1px solid var(--line);">120s</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">WS超时次数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(wsTimeoutCount || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">WS恢复阶段</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(wsPhase === 'timeout' ? '超时' : wsPhase === 'recovering' ? '恢复中(黄点)' : wsPhase === 'stable' ? '稳定(绿点)' : '正常')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">连续无超时计数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(wsNoTimeoutStreak || 0)} / 3</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">超时状态</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(timeoutText)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近超时发生</td><td style="padding:6px;border-bottom:1px solid var(--line);">${formatDiagTs(Number(wsLastTimeoutAtMs || 0))}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近超时错误</td><td style="padding:6px;border-bottom:1px solid var(--line);white-space:pre-wrap;">${esc(wsLastTimeoutError || '--')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">批量拉取总次数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(fetchDiag.prefetch_calls_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">批量失败退化次数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(fetchDiag.prefetch_fallback_total || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近批量symbol数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(fetchDiag.last_prefetch_symbol_count || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近成功symbol数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(fetchDiag.last_prefetch_success_count || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近缺失symbol数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(fetchDiag.last_prefetch_missing_count || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近拉取模式</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(fetchDiag.last_prefetch_mode || '--')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近批量耗时</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(fetchDiag.last_prefetch_duration_ms || 0)} ms</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近批量时间</td><td style="padding:6px;border-bottom:1px solid var(--line);">${formatDiagTs(Number(fetchDiag.last_prefetch_at_ms || 0))}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近批量错误</td><td style="padding:6px;border-bottom:1px solid var(--line);white-space:pre-wrap;">${esc(fetchDiag.last_prefetch_error || '--')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近历史symbol</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(fetchDiag.last_history_symbol || '--')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近历史range</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(fetchDiag.last_history_range || '--')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近历史来源</td><td style="padding:6px;border-bottom:1px solid var(--line);">${esc(fetchDiag.last_history_source || '--')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近历史时间</td><td style="padding:6px;border-bottom:1px solid var(--line);">${formatDiagTs(Number(fetchDiag.last_history_at_ms || 0))}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近历史错误</td><td style="padding:6px;border-bottom:1px solid var(--line);white-space:pre-wrap;">${esc(fetchDiag.last_history_error || '--')}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">历史来源日志(最近10条)</td><td style="padding:6px;border-bottom:1px solid var(--line);white-space:pre-wrap;">${esc(historySourceLogText)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近请求symbol数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(lastQuoteRequestSymbols.length || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近缺失symbol数</td><td style="padding:6px;border-bottom:1px solid var(--line);">${Number(lastQuoteMissingSymbols.length || 0)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近缺失symbols</td><td style="padding:6px;border-bottom:1px solid var(--line);white-space:pre-wrap;">${esc(missingSymbolsText)}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">缺失统计更新时间</td><td style="padding:6px;border-bottom:1px solid var(--line);">${formatDiagTs(Number(lastQuoteMissingUpdatedAtMs || 0))}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近文本消息</td><td style="padding:6px;border-bottom:1px solid var(--line);">${formatDiagTs(Number(diag.last_text_at_ms || 0))}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近数据事件</td><td style="padding:6px;border-bottom:1px solid var(--line);">${formatDiagTs(Number(diag.last_data_event_at_ms || 0))}</td></tr>
        <tr><td style="padding:6px;border-bottom:1px solid var(--line);">最近解析失败</td><td style="padding:6px;border-bottom:1px solid var(--line);">${formatDiagTs(Number(diag.last_parse_failure_at_ms || 0))}</td></tr>
        <tr><td style="padding:6px;">最近解析错误</td><td style="padding:6px;white-space:pre-wrap;">${esc(diag.last_parse_failure || '--')}</td></tr>
      </tbody>
    </table>
  `;

  overlay.appendChild(panel);
  document.body.appendChild(overlay);

  panel.querySelector('#dataDiagCloseBtn')?.addEventListener('click', () => overlay.remove());
  overlay.addEventListener('click', (ev) => {
    if (ev.target === overlay) overlay.remove();
  });
};

document.getElementById('dataSourceStatusChip')?.addEventListener('click', showDataSourceLogPopup);

const withBusy = async (buttonId, startText, doneText, fn) => {
  const btn = document.getElementById(buttonId);
  const old = btn?.textContent;
  if (btn) {
    btn.disabled = true;
    btn.textContent = startText;
  }
  setStatus(startText);
  try {
    const result = await fn();
    setStatus(doneText, 'ok');
    return result;
  } catch (e) {
    setStatus(e.message || String(e), 'err');
    throw e;
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = old;
    }
  }
};

const createNoopSeries = () => ({ setData: () => {} });

const addLineSeriesCompat = (chart, options) => {
  if (!chart) return createNoopSeries();
  if (typeof chart.addLineSeries === 'function') {
    return chart.addLineSeries(options);
  }
  if (window.LightweightCharts?.LineSeries && typeof chart.addSeries === 'function') {
    return chart.addSeries(window.LightweightCharts.LineSeries, options);
  }
  return createNoopSeries();
};

const addAreaSeriesCompat = (chart, options) => {
  if (!chart) return createNoopSeries();
  if (typeof chart.addAreaSeries === 'function') {
    return chart.addAreaSeries(options);
  }
  if (window.LightweightCharts?.AreaSeries && typeof chart.addSeries === 'function') {
    return chart.addSeries(window.LightweightCharts.AreaSeries, options);
  }
  // Fallback to line series with fill
  return addLineSeriesCompat(chart, { ...options, lineWidth: 0 });
};

const createChartCompat = (containerId) => {
  const container = document.getElementById(containerId);
  const lib = window.LightweightCharts;
  if (!container || !lib || typeof lib.createChart !== 'function') {
    return {
      chart: null,
      addLineSeries: () => createNoopSeries(),
      fit: () => {},
    };
  }
  const chart = lib.createChart(container, {
    layout: { background: { color: '#0f1420' }, textColor: '#c5c9d6' },
    grid: { vertLines: { color: 'rgba(255,255,255,.04)' }, horzLines: { color: 'rgba(255,255,255,.04)' } },
    timeScale: {
      timeVisible: true,
      secondsVisible: false,
      minBarSpacing: 0.02,
      rightOffset: 3,
    },
  });
  return {
    chart,
    container,
    addLineSeries: (options) => addLineSeriesCompat(chart, options),
    fit: () => chart.timeScale().fitContent(),
    resize: () => {
      if (!chart || !container) return;
      const rect = container.getBoundingClientRect();
      const w = Math.max(10, Math.floor(rect.width));
      const h = Math.max(10, Math.floor(rect.height));
      if (typeof chart.applyOptions === 'function') {
        chart.applyOptions({ width: w, height: h });
      }
    },
  };
};

const attachChartAutoResize = (chartRef) => {
  if (!chartRef?.container || typeof ResizeObserver === 'undefined') return;
  const observer = new ResizeObserver(() => {
    chartRef.resize();
    chartRef.fit();
  });
  observer.observe(chartRef.container);
};

for (const btn of tabs) {
  btn.addEventListener('click', () => {
    const t = btn.dataset.tab;
    switchTabByName(t);
  });
}

const clockChip = document.getElementById('clockChip');
setInterval(() => {
  const now = new Date();
  clockChip.textContent = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')} ${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}:${String(now.getSeconds()).padStart(2,'0')}`;
}, 1000);

const fChart = createChartCompat('forecastChart');
// Confidence band FIRST (renders behind everything)
const p70AreaSeries = addAreaSeriesCompat(fChart.chart, {
  topColor: 'rgba(91,156,246,0.30)',
  bottomColor: 'rgba(91,156,246,0.08)',
  lineColor: 'rgba(91,156,246,0.35)',
  lineWidth: 1,
  crosshairMarkerVisible: false,
});
const p30AreaSeries = addAreaSeriesCompat(fChart.chart, {
  topColor: '#0f1420',
  bottomColor: '#0f1420',
  lineColor: 'rgba(91,156,246,0.35)',
  lineWidth: 1,
  crosshairMarkerVisible: false,
});
// History line - bright enough to see clearly
const historySeries = fChart.addLineSeries({ color: '#b0b8d0', lineWidth: 2 });
// Percentile lines ON TOP of area bands
const p10Series = fChart.addLineSeries({ color: '#ff6b7a', lineWidth: 2, lineStyle: 2 });
const p90Series = fChart.addLineSeries({ color: '#2ee8b0', lineWidth: 2, lineStyle: 2 });
// P50 (median) last — most prominent
const p50Series = fChart.addLineSeries({ color: '#5b9cf6', lineWidth: 3 });
if (!fChart.chart) {
  setStatus('Chart library unavailable, but APIs and tables still work.', 'err');
}
attachChartAutoResize(fChart);

if (fChart?.container) {
  fChart.container.addEventListener('mouseenter', () => {
    if (!lastForecastContext) return;
    showForecastLegend();
  });
  fChart.container.addEventListener('mouseleave', () => {
    if (!lastForecastContext) return;
    hideForecastLegend();
  });
}

if (fChart?.chart && typeof fChart.chart.subscribeCrosshairMove === 'function') {
  fChart.chart.subscribeCrosshairMove((param) => {
    if (!param || !param.time || !lastForecastContext) return;
    const t = typeof param.time === 'number'
      ? param.time
      : (typeof param.time?.timestamp === 'number' ? param.time.timestamp : null);
    if (t == null) return;

    showForecastLegend();
    setForecastLegendAtTime(Number(t));
  });
}

const toSeries = (points) => points.map(p => ({ time: p.time, value: p.value }));

const toPaperSeriesFromSnapshots = (snapshots = []) => {
  const out = [];
  let lastTime = 0;
  for (const s of snapshots) {
    const ts = Math.floor(new Date(s.timestamp).getTime() / 1000);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    const time = ts <= lastTime ? lastTime + 1 : ts;
    lastTime = time;
    out.push({ time, value: s.total_value });
  }
  return out;
};

const toPaperBenchmarkSeries = (snapshots = []) => {
  if (!snapshots || snapshots.length === 0) return [];
  const initialCapital = Math.max(
    1,
    (snapshots[0]?.total_value ?? 0) - (snapshots[0]?.pnl_usd ?? 0),
  );

  const out = [];
  let lastTime = 0;
  for (const s of snapshots) {
    const ts = Math.floor(new Date(s.timestamp).getTime() / 1000);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    const time = ts <= lastTime ? lastTime + 1 : ts;
    lastTime = time;
    const value = initialCapital * (1 + (s.benchmark_return_pct || 0) / 100);
    out.push({ time, value });
  }
  return out;
};

const buildPaperSeriesContext = (snapshots = []) => {
  if (!snapshots || snapshots.length === 0) {
    return {
      portfolioSeries: [],
      benchmarkSeries: [],
      metricsByTime: new Map(),
      latest: null,
    };
  }

  const initialCapital = Math.max(
    1,
    (snapshots[0]?.total_value ?? 0) - (snapshots[0]?.pnl_usd ?? 0),
  );

  const portfolioSeries = [];
  const benchmarkSeries = [];
  const metricsByTime = new Map();
  let lastTime = 0;
  let latest = null;

  for (const s of snapshots) {
    const ts = Math.floor(new Date(s.timestamp).getTime() / 1000);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    const time = ts <= lastTime ? lastTime + 1 : ts;
    lastTime = time;

    const benchmarkValue = initialCapital * (1 + (s.benchmark_return_pct || 0) / 100);
    const benchmarkPnlUsd = benchmarkValue - initialCapital;
    const spreadUsd = (s.total_value || 0) - benchmarkValue;
    const spreadPct = benchmarkValue !== 0 ? (spreadUsd / benchmarkValue) * 100 : 0;

    portfolioSeries.push({ time, value: s.total_value });
    benchmarkSeries.push({ time, value: benchmarkValue });

    const metrics = {
      time,
      updatedText: s.timestamp,
      portfolioValue: s.total_value,
      portfolioPnlUsd: s.pnl_usd,
      portfolioPnlPct: s.pnl_pct,
      benchmarkValue,
      benchmarkPnlUsd,
      benchmarkPnlPct: s.benchmark_return_pct || 0,
      spreadUsd,
      spreadPct,
    };
    metricsByTime.set(time, metrics);
    latest = metrics;
  }

  return { portfolioSeries, benchmarkSeries, metricsByTime, latest };
};

const buildFallbackPaperContext = (snapshot) => {
  if (!snapshot) {
    return {
      portfolioSeries: [],
      benchmarkSeries: [],
      metricsByTime: new Map(),
      latest: null,
    };
  }

  const t = Math.floor(new Date(snapshot.timestamp).getTime() / 1000);
  if (!Number.isFinite(t) || t <= 0) {
    return {
      portfolioSeries: [],
      benchmarkSeries: [],
      metricsByTime: new Map(),
      latest: null,
    };
  }

  const baseline = Math.max(1, snapshot.total_value - (snapshot.pnl_usd || 0));
  const benchValue = baseline * (1 + (snapshot.benchmark_return_pct || 0) / 100);
  const metrics = {
    time: t,
    updatedText: snapshot.timestamp,
    portfolioValue: snapshot.total_value,
    portfolioPnlUsd: snapshot.pnl_usd || 0,
    portfolioPnlPct: snapshot.pnl_pct || 0,
    benchmarkValue: benchValue,
    benchmarkPnlUsd: benchValue - baseline,
    benchmarkPnlPct: snapshot.benchmark_return_pct || 0,
    spreadUsd: snapshot.total_value - benchValue,
    spreadPct: benchValue !== 0 ? ((snapshot.total_value - benchValue) / benchValue) * 100 : 0,
  };
  const metricsByTime = new Map();
  metricsByTime.set(t, metrics);

  return {
    portfolioSeries: [{ time: t, value: snapshot.total_value }],
    benchmarkSeries: [{ time: t, value: benchValue }],
    metricsByTime,
    latest: metrics,
  };
};

const getFutuModeLabel = () => (selectedFutuCurveMode === 'strategy' ? 'Strategy Sleeve' : 'Account');

const buildFutuCandidateSymbolSet = (futuStatus) => {
  const set = new Set();
  const marketPrefix = String(futuStatus?.selected_market || futuStatus?.conn_market || 'US').trim().toUpperCase();

  const addSymbol = (rawSymbol) => {
    const upper = String(rawSymbol || '').trim().toUpperCase();
    if (!upper) return;
    set.add(upper);
    if (upper.includes('.')) {
      const stripped = upper.split('.').slice(1).join('.').trim();
      if (stripped) set.add(stripped);
    } else if (marketPrefix) {
      set.add(`${marketPrefix}.${upper}`);
    }
  };

  for (const symbol of (Array.isArray(futuStatus?.candidate_symbols) ? futuStatus.candidate_symbols : [])) {
    addSymbol(symbol);
  }

  if (set.size === 0) {
    for (const item of (latestPaperStatus?.target_weights || [])) {
      addSymbol(item?.symbol);
    }
  }

  return set;
};

const isFutuCandidateSymbol = (symbol, candidateSet, futuStatus) => {
  const upper = String(symbol || '').trim().toUpperCase();
  if (!upper || !(candidateSet instanceof Set) || candidateSet.size === 0) return false;
  if (candidateSet.has(upper)) return true;
  const stripped = upper.includes('.') ? upper.split('.').slice(1).join('.').trim() : upper;
  if (stripped && candidateSet.has(stripped)) return true;
  const marketPrefix = String(futuStatus?.selected_market || futuStatus?.conn_market || 'US').trim().toUpperCase();
  return !!marketPrefix && candidateSet.has(`${marketPrefix}.${stripped}`);
};

const syncFutuLegendModeLabels = () => {
  const modeLabel = getFutuModeLabel();
  if (futuLegendTitle) futuLegendTitle.textContent = `${modeLabel} Metrics`;
  if (futuLegendPortfolioKey) futuLegendPortfolioKey.textContent = `${modeLabel} NAV`;
  if (futuLegendPortfolioPnlKey) futuLegendPortfolioPnlKey.textContent = `${modeLabel} PnL`;
};

const computeFutuStrategyRealtimeMetrics = (futuStatus) => {
  const strategySnapshot = futuStatus?.latest_strategy_snapshot;
  const candidateSet = buildFutuCandidateSymbolSet(futuStatus);
  if (candidateSet.size === 0 && strategySnapshot) {
    return {
      totalValue: Number(strategySnapshot?.total_value),
      cashUsd: Number(strategySnapshot?.cash_usd),
      investedValue: Number(strategySnapshot?.invested_value_usd),
      cashWeightPct: Number(strategySnapshot?.cash_weight_pct),
      pnlUsd: Number(strategySnapshot?.pnl_usd),
      pnlPct: Number(strategySnapshot?.pnl_pct),
      baseCapital: Number(futuStatus?.strategy_start_capital_usd),
    };
  }
  const strategyHoldings = Array.isArray(futuStatus?.latest_snapshot?.holdings)
    ? futuStatus.latest_snapshot.holdings.filter((holding) => isFutuCandidateSymbol(holding?.symbol, candidateSet, futuStatus))
    : [];

  let investedValue = strategyHoldings.reduce((sum, holding) => {
    const symbol = String(holding?.symbol || '').toUpperCase();
    const symbolNoPrefix = symbol.includes('.') ? symbol.split('.').slice(1).join('.') : symbol;
    const livePrice = getLatestQuotePrice(symbol) ?? getLatestQuotePrice(symbolNoPrefix);
    const currentPrice = Number.isFinite(livePrice) ? livePrice : Number(holding?.price);
    const quantity = Number(holding?.quantity);
    if (!Number.isFinite(currentPrice) || !Number.isFinite(quantity) || quantity <= 0) return sum;
    return sum + currentPrice * quantity;
  }, 0);

  let pnlUsd = strategyHoldings.reduce((sum, holding) => {
    const symbol = String(holding?.symbol || '').toUpperCase();
    const symbolNoPrefix = symbol.includes('.') ? symbol.split('.').slice(1).join('.') : symbol;
    const livePrice = getLatestQuotePrice(symbol) ?? getLatestQuotePrice(symbolNoPrefix);
    const currentPrice = Number.isFinite(livePrice) ? livePrice : Number(holding?.price);
    const quantity = Number(holding?.quantity);
    const avgCost = Number(holding?.avg_cost);
    if (!Number.isFinite(currentPrice) || !Number.isFinite(quantity) || !Number.isFinite(avgCost) || quantity <= 0) return sum;
    return sum + (currentPrice - avgCost) * quantity;
  }, 0);

  const strategyBase = Number(futuStatus?.strategy_start_capital_usd);
  if (Number.isFinite(strategyBase) && strategyBase > 0 && Number.isFinite(investedValue) && investedValue > strategyBase) {
    const scale = Math.max(0, Math.min(1, strategyBase / investedValue));
    investedValue *= scale;
    pnlUsd *= scale;
  }
  const fallbackCash = Number(strategySnapshot?.cash_usd);
  const totalValue = Number.isFinite(strategyBase) && strategyBase > 0
    ? strategyBase + pnlUsd
    : (Number.isFinite(fallbackCash) ? fallbackCash + investedValue : NaN);
  const cashUsd = Number.isFinite(totalValue) ? (totalValue - investedValue) : NaN;
  const cashWeightPct = Number.isFinite(totalValue) && totalValue > 0 ? (cashUsd / totalValue) * 100 : NaN;
  const pnlPct = Number.isFinite(strategyBase) && strategyBase > 0 ? (pnlUsd / strategyBase) * 100 : NaN;

  return {
    totalValue,
    cashUsd,
    investedValue,
    cashWeightPct,
    pnlUsd,
    pnlPct,
    baseCapital: strategyBase,
  };
};

const buildFutuSeriesContext = (futuStatus) => {
  if (selectedFutuCurveMode === 'strategy') {
    const strategySnapshots = Array.isArray(futuStatus?.strategy_snapshots) ? futuStatus.strategy_snapshots : [];
    const strategyLatest = futuStatus?.latest_strategy_snapshot;
    const ftCtx = (() => {
      const ctx = buildPaperSeriesContext(strategySnapshots);
      if (ctx.portfolioSeries.length > 0) return ctx;
      return buildFallbackPaperContext(strategyLatest);
    })();

    const rtMetrics = computeFutuStrategyRealtimeMetrics(futuStatus);
    const rtValue = Number(rtMetrics?.totalValue);
    const strategyFtSeries = ftCtx.portfolioSeries || [];

    let portfolioSeries = strategyFtSeries;
    let latest = ftCtx.latest;
    let metricsByTime = ftCtx.metricsByTime;

    if (Number.isFinite(rtValue) && rtValue > 0 && strategyFtSeries.length > 0) {
      const lastIdx = strategyFtSeries.length - 1;
      portfolioSeries = strategyFtSeries.map((p, i) => (i === lastIdx ? { time: p.time, value: rtValue } : p));
      const latestTime = portfolioSeries[lastIdx].time;
      const latestMetrics = { ...(ftCtx.latest || {}) };
      const benchmarkValue = Number(latestMetrics.benchmarkValue);
      const baseCapital = Number(rtMetrics.baseCapital);
      const portfolioPnlUsd = Number(rtMetrics.pnlUsd);
      const portfolioPnlPct = Number(rtMetrics.pnlPct);
      const spreadUsd = Number.isFinite(benchmarkValue) ? rtValue - benchmarkValue : NaN;
      const spreadPct = Number.isFinite(benchmarkValue) && benchmarkValue !== 0
        ? (spreadUsd / benchmarkValue) * 100
        : NaN;

      latest = {
        ...latestMetrics,
        time: latestTime,
        portfolioValue: rtValue,
        portfolioPnlUsd: Number.isFinite(portfolioPnlUsd)
          ? portfolioPnlUsd
          : (Number.isFinite(baseCapital) ? rtValue - baseCapital : Number(latestMetrics.portfolioPnlUsd)),
        portfolioPnlPct: Number.isFinite(portfolioPnlPct)
          ? portfolioPnlPct
          : (Number.isFinite(baseCapital) && baseCapital > 0
            ? ((rtValue - baseCapital) / baseCapital) * 100
            : Number(latestMetrics.portfolioPnlPct)),
        spreadUsd,
        spreadPct,
      };

      metricsByTime = new Map(ftCtx.metricsByTime || []);
      metricsByTime.set(latestTime, latest);
    }

    return {
      portfolioSeries,
      strategyFtSeries,
      benchmarkSeries: ftCtx.benchmarkSeries || [],
      metricsByTime,
      latest,
    };
  }
  const accountSnapshots = Array.isArray(futuStatus?.snapshots) ? futuStatus.snapshots : [];
  const ctx = buildPaperSeriesContext(accountSnapshots);
  if (ctx.portfolioSeries.length > 0) return { ...ctx, strategyFtSeries: [] };
  return { ...buildFallbackPaperContext(futuStatus?.latest_snapshot), strategyFtSeries: [] };
};

const filterPaperContextByRangeDays = (ctx, rangeDays) => {
  const days = Number(rangeDays);
  if (!ctx || !Array.isArray(ctx.portfolioSeries) || ctx.portfolioSeries.length === 0) {
    return {
      portfolioSeries: [],
      strategyFtSeries: [],
      benchmarkSeries: [],
      metricsByTime: new Map(),
      latest: null,
    };
  }

  if (!Number.isFinite(days) || days <= 0) {
    return ctx;
  }

  const latestTime = ctx.portfolioSeries[ctx.portfolioSeries.length - 1].time;
  const cutoff = latestTime - Math.floor(days * 86400);

  const portfolioSeries = ctx.portfolioSeries.filter((p) => p.time >= cutoff);
  const strategyFtSeries = Array.isArray(ctx.strategyFtSeries)
    ? ctx.strategyFtSeries.filter((p) => p.time >= cutoff)
    : [];
  const benchmarkSeries = ctx.benchmarkSeries.filter((p) => p.time >= cutoff);
  const metricsByTime = new Map();
  for (const [time, metrics] of ctx.metricsByTime.entries()) {
    if (time >= cutoff) {
      metricsByTime.set(time, metrics);
    }
  }

  let latest = null;
  if (portfolioSeries.length > 0) {
    const t = portfolioSeries[portfolioSeries.length - 1].time;
    latest = metricsByTime.get(t) || ctx.latest;
  }

  return {
    portfolioSeries,
    strategyFtSeries,
    benchmarkSeries,
    metricsByTime,
    latest,
  };
};

const renderPaperChartFromCurrentContext = () => {
  const filtered = filterPaperContextByRangeDays(paperFullContext, selectedPaperRangeDays);
  paperMetricsByTime = filtered.metricsByTime;

  if (filtered.portfolioSeries.length > 0) {
    lastPaperSeries = filtered.portfolioSeries;
    const latestTime = filtered.portfolioSeries[filtered.portfolioSeries.length - 1].time;
    const selectedDays = Number(selectedPaperRangeDays);
    const from = latestTime - Math.floor(selectedDays * 86400);
    const paddedPortfolioSeries = padPaperSeriesToRange(filtered.portfolioSeries, from);
    const paddedBenchmarkSeries = filtered.benchmarkSeries.length > 0
      ? padPaperSeriesToRange(filtered.benchmarkSeries, from)
      : [];

    portfolioLine.setData(ensureVisibleSeries(paddedPortfolioSeries));
    benchmarkLine.setData(paddedBenchmarkSeries.length > 0 ? ensureVisibleSeries(paddedBenchmarkSeries) : []);

    const timeScale = paperChart?.chart?.timeScale?.();
    if (timeScale && Number.isFinite(selectedDays) && selectedDays > 0) {
      let applied = false;
      if (typeof timeScale.setVisibleLogicalRange === 'function' && filtered.portfolioSeries.length >= 2) {
        const deltas = [];
        for (let i = 1; i < filtered.portfolioSeries.length; i += 1) {
          const d = Number(filtered.portfolioSeries[i].time) - Number(filtered.portfolioSeries[i - 1].time);
          if (Number.isFinite(d) && d > 0) deltas.push(d);
        }
        if (deltas.length > 0) {
          deltas.sort((a, b) => a - b);
          const medianDelta = deltas[Math.floor(deltas.length / 2)] || 60;
          const barsPerDay = Math.max(1, Math.floor(86400 / Math.max(1, medianDelta)));
          const rangeBars = Math.max(1, Math.floor(selectedDays * barsPerDay));
          if (typeof timeScale.applyOptions === 'function') {
            const chartWidth = Math.max(300, Math.floor(paperChart?.container?.clientWidth || 1000));
            const targetSpacing = chartWidth / Math.max(1, rangeBars);
            timeScale.applyOptions({
              minBarSpacing: 0.02,
              barSpacing: Math.max(0.02, Math.min(8, targetSpacing)),
              rightOffset: 3,
            });
          }
          const toLogical = filtered.portfolioSeries.length - 1;
          const fromLogical = toLogical - rangeBars;
          timeScale.setVisibleLogicalRange({ from: fromLogical, to: toLogical });
          applied = true;
        }
      }
      if (!applied && typeof timeScale.setVisibleRange === 'function') {
        timeScale.setVisibleRange({ from, to: latestTime });
        applied = true;
      }
      if (!applied) {
        paperChart.fit();
      }
    } else {
      paperChart.fit();
    }
    setLegendText(filtered.latest || paperFullContext.latest || null);
  } else {
    lastPaperSeries = [];
    portfolioLine.setData([]);
    benchmarkLine.setData([]);
    setLegendText(null);
  }

  for (const btn of paperRangeButtons) {
    const days = Number(btn.dataset.paperRangeDays);
    btn.classList.toggle('active', days === selectedPaperRangeDays);
  }

  updatePaperRangePadBand();
};

const renderBacktestChartFromCurrentContext = () => {
  const filtered = filterPaperContextByRangeDays(backtestFullContext, selectedBacktestRangeDays);
  backtestMetricsByTime = filtered.metricsByTime;

  if (filtered.portfolioSeries.length > 0) {
    const latestTime = filtered.portfolioSeries[filtered.portfolioSeries.length - 1].time;
    const selectedDays = Number(selectedBacktestRangeDays);
    const from = Number.isFinite(selectedDays) && selectedDays > 0
      ? latestTime - Math.floor(selectedDays * 86400)
      : filtered.portfolioSeries[0].time;
    const paddedPortfolioSeries = Number.isFinite(selectedDays) && selectedDays > 0
      ? padPaperSeriesToRange(filtered.portfolioSeries, from)
      : filtered.portfolioSeries;
    const paddedBenchmarkSeries = filtered.benchmarkSeries.length > 0
      ? (Number.isFinite(selectedDays) && selectedDays > 0
        ? padPaperSeriesToRange(filtered.benchmarkSeries, from)
        : filtered.benchmarkSeries)
      : [];

    backtestPortfolioLine.setData(ensureVisibleSeries(paddedPortfolioSeries));
    backtestBenchmarkLine.setData(paddedBenchmarkSeries.length > 0 ? ensureVisibleSeries(paddedBenchmarkSeries) : []);

    const timeScale = backtestChart?.chart?.timeScale?.();
    if (timeScale && Number.isFinite(selectedDays) && selectedDays > 0 && typeof timeScale.setVisibleRange === 'function') {
      timeScale.setVisibleRange({ from, to: latestTime });
    } else {
      backtestChart.fit();
    }
    setBacktestLegendText(filtered.latest || backtestFullContext.latest || null);
  } else {
    backtestPortfolioLine.setData([]);
    backtestBenchmarkLine.setData([]);
    setBacktestLegendText(null);
  }

  for (const btn of backtestRangeButtons) {
    const days = Number(btn.dataset.backtestRangeDays);
    btn.classList.toggle('active', days === selectedBacktestRangeDays);
  }
};

const buildSnapshotDataSignature = (status) => {
  const snapshots = Array.isArray(status?.snapshots) ? status.snapshots : [];
  const latest = status?.latest_snapshot || null;
  const latestTs = latest?.timestamp || '';
  const latestNav = Number(latest?.total_value);
  const latestNavText = Number.isFinite(latestNav) ? latestNav.toFixed(6) : '--';
  return `${snapshots.length}|${latestTs}|${latestNavText}`;
};

const ensureVisibleSeries = (series = []) => {
  if (!series || series.length === 0) return [];
  if (series.length === 1) {
    const p = series[0];
    return [
      { time: p.time - 1, value: p.value },
      p,
    ];
  }
  return series;
};

const padPaperSeriesToRange = (series = [], cutoffTime) => {
  if (!Array.isArray(series) || series.length === 0) return [];
  if (!Number.isFinite(cutoffTime)) return series;
  const first = series[0];
  if (!first || !Number.isFinite(first.time) || first.time <= cutoffTime) return series;
  return [{ time: cutoffTime }, ...series];
};

const updatePaperRangePadBand = () => {
  if (!paperRangePadBand) return;
  const selectedDays = Number(selectedPaperRangeDays);
  const series = paperFullContext?.portfolioSeries || [];
  if (!Number.isFinite(selectedDays) || selectedDays <= 0 || series.length < 2) {
    paperRangePadBand.style.display = 'none';
    paperRangePadBand.style.width = '0';
    return;
  }

  const firstTime = Number(series[0].time);
  const lastTime = Number(series[series.length - 1].time);
  if (!Number.isFinite(firstTime) || !Number.isFinite(lastTime) || lastTime <= firstTime) {
    paperRangePadBand.style.display = 'none';
    paperRangePadBand.style.width = '0';
    return;
  }

  const availableDays = (lastTime - firstTime) / 86400;
  if (!Number.isFinite(availableDays) || availableDays >= selectedDays) {
    paperRangePadBand.style.display = 'none';
    paperRangePadBand.style.width = '0';
    return;
  }

  const missingRatio = Math.max(0, Math.min(1, (selectedDays - availableDays) / selectedDays));
  if (missingRatio <= 0.002) {
    paperRangePadBand.style.display = 'none';
    paperRangePadBand.style.width = '0';
    return;
  }

  paperRangePadBand.style.display = '';
  paperRangePadBand.style.width = `${(missingRatio * 100).toFixed(2)}%`;
};

const formatMoney = (v) => `$${v.toFixed(2)}`;

const formatChartDate = (unixSec) => {
  if (!Number.isFinite(unixSec)) return '--';
  return new Date(unixSec * 1000).toLocaleDateString();
};

const formatDateTime = (iso) => {
  if (!iso) return '--';
  const dt = new Date(iso);
  if (!Number.isFinite(dt.getTime())) return '--';
  return dt.toLocaleString();
};

const formatPrice = (v) => {
  if (!Number.isFinite(v)) return '--';
  return `$${v.toFixed(2)}`;
};

const formatPct = (v) => {
  if (!Number.isFinite(v)) return '--';
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
};

const formatSignedMoney = (v) => {
  if (!Number.isFinite(v)) return '--';
  return `${v >= 0 ? '+' : '-'}$${Math.abs(v).toFixed(2)}`;
};

const formatCompactCurrency = (v, { signed = false } = {}) => {
  if (!Number.isFinite(v)) return '--';
  const abs = Math.abs(v);
  const formatter = new Intl.NumberFormat(undefined, {
    notation: abs >= 1000 ? 'compact' : 'standard',
    maximumFractionDigits: abs >= 1000 ? 1 : 2,
    minimumFractionDigits: 0,
  });
  const prefix = signed ? (v >= 0 ? '+' : '-') : '';
  return `${prefix}$${formatter.format(abs)}`;
};

const escapeHtmlText = (value) => String(value)
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&#39;');

const clampUnit = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Math.max(0, Math.min(1, num));
};

const formatWeightPct = (value, digits = 1) => {
  if (!Number.isFinite(value)) return '--';
  return `${(value * 100).toFixed(digits)}%`;
};

const formatRegimeLabel = (state) => {
  const normalized = String(state || 'risk_on').trim().toLowerCase();
  if (normalized === 'risk_off') return '风险关闭';
  if (normalized === 'defensive') return '防守';
  return '风险开启';
};

const getRegimeMeta = (state) => {
  const normalized = String(state || 'risk_on').trim().toLowerCase();
  if (normalized === 'risk_off') {
    return {
      stateClass: 'risk-off',
      badge: 'Risk Off',
      title: '优先保本，允许全现金',
      subtitle: '广度或尾部风险已经越线，gross 上限会被压到最低，必要时直接切到现金。',
    };
  }
  if (normalized === 'defensive') {
    return {
      stateClass: 'defensive',
      badge: 'Defensive',
      title: '进入防守仓位控制',
      subtitle: '信号分化，overlay 正在限制总敞口，避免波动目标把资金一次性打满。',
    };
  }
  return {
    stateClass: 'risk-on',
    badge: 'Risk On',
    title: '允许按模型正常部署',
    subtitle: '当前广度与尾部风险没有触发额外收缩，目标权重可以跟随模型输出。',
  };
};

const PORTFOLIO_WEIGHT_COLORS = ['#3b82f6', '#8b5cf6', '#00d4aa', '#f59e0b', '#ff4757', '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#a78bfa'];
const REGIME_NEGATIVE_BREADTH_THRESHOLD = 0.75;
const REGIME_CVAR_RISK_OFF_THRESHOLD = 0.04;
const REGIME_DEFENSIVE_BREADTH_ENTER = 0.55;
const REGIME_DEFENSIVE_BREADTH_EXIT = 0.45;
const REGIME_RISK_OFF_BREADTH_EXIT = 0.60;
const REGIME_DEFENSIVE_CVAR_ENTER = 0.025;
const REGIME_DEFENSIVE_CVAR_EXIT = 0.018;
const REGIME_RISK_OFF_CVAR_EXIT = 0.03;
const REGIME_DEFENSIVE_RETURN_ENTER = 0.08;
const REGIME_DEFENSIVE_RETURN_EXIT = 0.12;
const REGIME_RISK_OFF_RETURN_EXIT = 0.03;
const REGIME_RISK_ON_MIN_GROSS = 0.85;
const REGIME_DEFENSIVE_MAX_GROSS = 0.75;
const REGIME_DEFENSIVE_MIN_GROSS = 0.35;
const REGIME_RISK_OFF_MAX_GROSS = 0.20;
const REGIME_OVERLAY_SCOPES = {
  portfolio: {
    titlePrefix: '组合优化',
    emptyText: '运行组合优化后，这里会显示 regime、现金权重和风控触发原因。',
  },
  paper: {
    titlePrefix: 'Paper',
    emptyText: '执行一次 paper 候选池优化后，这里会显示当前使用中的 regime 叠加层。',
  },
  futu: {
    titlePrefix: 'FUTU',
    emptyText: '完成一次候选池优化后，这里会同步 FUTU 策略袖口沿用的 regime 约束。',
  },
  backtest: {
    titlePrefix: 'Backtest',
    emptyText: '回测出现首次 rebalance 后，这里会显示最近一次再平衡的 regime 约束。',
  },
};

const formatAsOfText = (value) => {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return `最近快照 ${value}`;
  return `最近快照 ${date.toLocaleString()}`;
};

const getRegimeBand = (state) => {
  const normalized = String(state || 'risk_on').trim().toLowerCase();
  if (normalized === 'risk_off') {
    return {
      label: 'RiskOff 档',
      grossRange: `${formatWeightPct(0)} - ${formatWeightPct(REGIME_RISK_OFF_MAX_GROSS)}`,
      entry: `进入: 广度 >= ${formatWeightPct(REGIME_NEGATIVE_BREADTH_THRESHOLD, 0)} 或 CVaR >= ${formatWeightPct(REGIME_CVAR_RISK_OFF_THRESHOLD, 1)}`,
      exit: `退出: 广度 < ${formatWeightPct(REGIME_RISK_OFF_BREADTH_EXIT, 0)} 且 CVaR < ${formatWeightPct(REGIME_RISK_OFF_CVAR_EXIT, 1)} 且年化收益 > ${formatWeightPct(REGIME_RISK_OFF_RETURN_EXIT, 0)}`,
    };
  }
  if (normalized === 'defensive') {
    return {
      label: 'Defensive 档',
      grossRange: `${formatWeightPct(REGIME_DEFENSIVE_MIN_GROSS)} - ${formatWeightPct(REGIME_DEFENSIVE_MAX_GROSS)}`,
      entry: `进入: 广度 >= ${formatWeightPct(REGIME_DEFENSIVE_BREADTH_ENTER, 0)} 或 CVaR >= ${formatWeightPct(REGIME_DEFENSIVE_CVAR_ENTER, 1)} 或年化收益 <= ${formatWeightPct(REGIME_DEFENSIVE_RETURN_ENTER, 0)}`,
      exit: `退出: 广度 < ${formatWeightPct(REGIME_DEFENSIVE_BREADTH_EXIT, 0)} 且 CVaR < ${formatWeightPct(REGIME_DEFENSIVE_CVAR_EXIT, 1)} 且年化收益 > ${formatWeightPct(REGIME_DEFENSIVE_RETURN_EXIT, 0)}`,
    };
  }
  return {
    label: 'RiskOn 档',
    grossRange: `${formatWeightPct(REGIME_RISK_ON_MIN_GROSS)} - ${formatWeightPct(1)}`,
    entry: `进入: 信号回暖后恢复部署`,
    exit: `退出: 广度 >= ${formatWeightPct(REGIME_DEFENSIVE_BREADTH_ENTER, 0)} 或 CVaR >= ${formatWeightPct(REGIME_DEFENSIVE_CVAR_ENTER, 1)} 或年化收益 <= ${formatWeightPct(REGIME_DEFENSIVE_RETURN_ENTER, 0)}`,
  };
};

const formatHysteresisStatus = (reasonList) => {
  const held = Array.isArray(reasonList) && reasonList.some((reason) => /hysteresis hold/i.test(String(reason || '')));
  return held ? '迟滞锁定中' : '未锁定';
};

const normalizedAbove = (value, start, end) => {
  if (end <= start) return 0;
  return clampUnit((Number(value || 0) - start) / (end - start));
};

const normalizedBelow = (value, start, end) => {
  if (start <= end) return 0;
  return clampUnit((start - Number(value || 0)) / (start - end));
};

const getThresholdGap = (value, threshold, direction) => {
  if (!Number.isFinite(value) || !Number.isFinite(threshold)) return null;
  if (direction === 'below') return value - threshold;
  return threshold - value;
};

const formatThresholdGap = (gap, direction, digits = 1) => {
  if (!Number.isFinite(gap)) return '无';
  if (Math.abs(gap) < 0.0005) return '刚好到线';
  if (gap > 0) return `还差 ${formatWeightPct(gap, digits)}`;
  return direction === 'below'
    ? `已低于 ${formatWeightPct(Math.abs(gap), digits)}`
    : `已高于 ${formatWeightPct(Math.abs(gap), digits)}`;
};

const getRegimeThresholdRows = (state, regime, expectedAnnualReturn, cvar95) => {
  const normalized = String(state || 'risk_on').trim().toLowerCase();
  const rows = [
    {
      label: '看空广度',
      current: clampUnit(regime.bearish_breadth),
      direction: 'above',
    },
    {
      label: '负收益广度',
      current: clampUnit(regime.negative_return_breadth),
      direction: 'above',
    },
    {
      label: '负 Sharpe 广度',
      current: clampUnit(regime.negative_sharpe_breadth),
      direction: 'above',
    },
    {
      label: '年化预期收益',
      current: Number(expectedAnnualReturn || 0),
      direction: 'below',
    },
    {
      label: 'CVaR 95',
      current: Number(cvar95 || 0),
      direction: 'above',
    },
  ];

  const thresholdsByState = {
    risk_on: {
      enterLabel: '进入 Defensive',
      exitLabel: '退出 RiskOn',
      enter: [
        REGIME_DEFENSIVE_BREADTH_ENTER,
        REGIME_DEFENSIVE_BREADTH_ENTER,
        REGIME_DEFENSIVE_BREADTH_ENTER,
        REGIME_DEFENSIVE_RETURN_ENTER,
        REGIME_DEFENSIVE_CVAR_ENTER,
      ],
      exit: [
        REGIME_DEFENSIVE_BREADTH_ENTER,
        REGIME_DEFENSIVE_BREADTH_ENTER,
        REGIME_DEFENSIVE_BREADTH_ENTER,
        REGIME_DEFENSIVE_RETURN_ENTER,
        REGIME_DEFENSIVE_CVAR_ENTER,
      ],
    },
    defensive: {
      enterLabel: '进入 RiskOff',
      exitLabel: '退出 Defensive',
      enter: [
        REGIME_NEGATIVE_BREADTH_THRESHOLD,
        REGIME_NEGATIVE_BREADTH_THRESHOLD,
        REGIME_NEGATIVE_BREADTH_THRESHOLD,
        0,
        REGIME_CVAR_RISK_OFF_THRESHOLD,
      ],
      exit: [
        REGIME_DEFENSIVE_BREADTH_EXIT,
        REGIME_DEFENSIVE_BREADTH_EXIT,
        REGIME_DEFENSIVE_BREADTH_EXIT,
        REGIME_DEFENSIVE_RETURN_EXIT,
        REGIME_DEFENSIVE_CVAR_EXIT,
      ],
    },
    risk_off: {
      enterLabel: '进入更紧档',
      exitLabel: '退出 RiskOff',
      enter: [null, null, null, null, null],
      exit: [
        REGIME_RISK_OFF_BREADTH_EXIT,
        REGIME_RISK_OFF_BREADTH_EXIT,
        REGIME_RISK_OFF_BREADTH_EXIT,
        REGIME_RISK_OFF_RETURN_EXIT,
        REGIME_RISK_OFF_CVAR_EXIT,
      ],
    },
  };

  const selected = thresholdsByState[normalized] || thresholdsByState.risk_on;
  return {
    enterLabel: selected.enterLabel,
    exitLabel: selected.exitLabel,
    rows: rows.map((row, index) => {
      const enterThreshold = selected.enter[index];
      const exitThreshold = selected.exit[index];
      return {
        ...row,
        enterThreshold,
        exitThreshold,
        enterGap: getThresholdGap(row.current, enterThreshold, row.direction),
        exitGap: getThresholdGap(row.current, exitThreshold, row.direction),
      };
    }),
  };
};

const summarizeThresholdGap = (rows, kind, fallbackText) => {
  const filtered = rows.filter((row) => Number.isFinite(row[`${kind}Gap`]));
  if (filtered.length === 0) {
    return {
      value: '--',
      sub: fallbackText,
    };
  }

  const crossed = filtered
    .filter((row) => row[`${kind}Gap`] <= 0)
    .sort((a, b) => a[`${kind}Gap`] - b[`${kind}Gap`]);
  if (crossed.length > 0) {
    const row = crossed[0];
    return {
      value: '已达到',
      sub: `${row.label} ${formatThresholdGap(row[`${kind}Gap`], row.direction)}`,
    };
  }

  const nearest = [...filtered].sort((a, b) => a[`${kind}Gap`] - b[`${kind}Gap`])[0];
  return {
    value: formatThresholdGap(nearest[`${kind}Gap`], nearest.direction),
    sub: `最近: ${nearest.label}`,
  };
};

const getDominantGrossDriver = (state, regime, expectedAnnualReturn, cvar95) => {
  const candidates = [
    {
      key: 'bearish_breadth',
      label: '看空广度',
      defensivePressure: normalizedAbove(regime.bearish_breadth, REGIME_DEFENSIVE_BREADTH_EXIT, REGIME_NEGATIVE_BREADTH_THRESHOLD),
      riskOffPressure: normalizedAbove(regime.bearish_breadth, REGIME_RISK_OFF_BREADTH_EXIT, 0.95),
    },
    {
      key: 'negative_return_breadth',
      label: '负收益广度',
      defensivePressure: normalizedAbove(regime.negative_return_breadth, REGIME_DEFENSIVE_BREADTH_EXIT, REGIME_NEGATIVE_BREADTH_THRESHOLD),
      riskOffPressure: normalizedAbove(regime.negative_return_breadth, REGIME_RISK_OFF_BREADTH_EXIT, 0.95),
    },
    {
      key: 'negative_sharpe_breadth',
      label: '负 Sharpe 广度',
      defensivePressure: normalizedAbove(regime.negative_sharpe_breadth, REGIME_DEFENSIVE_BREADTH_EXIT, REGIME_NEGATIVE_BREADTH_THRESHOLD),
      riskOffPressure: normalizedAbove(regime.negative_sharpe_breadth, REGIME_RISK_OFF_BREADTH_EXIT, 0.95),
    },
    {
      key: 'expected_return',
      label: '年化预期收益',
      defensivePressure: normalizedBelow(expectedAnnualReturn, REGIME_DEFENSIVE_RETURN_EXIT, 0),
      riskOffPressure: normalizedBelow(expectedAnnualReturn, REGIME_RISK_OFF_RETURN_EXIT, -0.10),
    },
    {
      key: 'cvar_95',
      label: 'CVaR 95',
      defensivePressure: normalizedAbove(cvar95, REGIME_DEFENSIVE_CVAR_EXIT, REGIME_CVAR_RISK_OFF_THRESHOLD),
      riskOffPressure: normalizedAbove(cvar95, REGIME_RISK_OFF_CVAR_EXIT, 0.08),
    },
  ];

  const normalized = String(state || 'risk_on').trim().toLowerCase();
  const ranked = candidates.map((candidate) => {
    let effectivePressure = candidate.defensivePressure;
    let pressureSource = 'defensive';
    if (normalized === 'risk_off') {
      effectivePressure = candidate.riskOffPressure;
      pressureSource = 'risk_off';
    } else if (normalized === 'defensive') {
      if (candidate.riskOffPressure >= candidate.defensivePressure) {
        effectivePressure = candidate.riskOffPressure;
        pressureSource = 'risk_off';
      }
    } else if (candidate.riskOffPressure * 0.6 > candidate.defensivePressure) {
      effectivePressure = candidate.riskOffPressure * 0.6;
      pressureSource = 'risk_off_scaled';
    }

    return {
      ...candidate,
      effectivePressure,
      pressureSource,
    };
  }).sort((a, b) => b.effectivePressure - a.effectivePressure);

  const dominant = ranked[0] || null;
  if (!dominant) {
    return {
      key: null,
      label: '无',
      detail: '当前没有显著的 gross 压缩驱动项。',
    };
  }

  const sourceLabel = dominant.pressureSource === 'risk_off'
    ? 'RiskOff pressure'
    : dominant.pressureSource === 'risk_off_scaled'
      ? 'RiskOff pressure x 0.6'
      : 'Defensive pressure';
  return {
    key: dominant.key,
    label: dominant.label,
    detail: `${sourceLabel} ${formatWeightPct(dominant.effectivePressure)}`,
  };
};

const translateRegimeReason = (reason) => {
  const text = String(reason || '').trim();
  if (!text) return '无';

  let match = text.match(/bearish breadth ([\d.]+)% >= ([\d.]+)%/i);
  if (match) {
    return `看空广度 ${match[1]}%，已经高于风险阈值 ${match[2]}%。`;
  }

  match = text.match(/negative return breadth ([\d.]+)% >= ([\d.]+)%/i);
  if (match) {
    return `负收益广度 ${match[1]}%，已经高于风险阈值 ${match[2]}%。`;
  }

  match = text.match(/negative sharpe breadth ([\d.]+)% >= ([\d.]+)%/i);
  if (match) {
    return `负 Sharpe 广度 ${match[1]}%，已经高于风险阈值 ${match[2]}%。`;
  }

  match = text.match(/portfolio expected annual return ([\-\d.]+)% <= 0%/i);
  if (match) {
    return `组合年化预期收益 ${match[1]}%，已经跌到 0% 以下。`;
  }

  match = text.match(/portfolio expected annual return ([\-\d.]+)% <= ([\d.]+)%/i);
  if (match) {
    return `组合年化预期收益 ${match[1]}%，已经低于防守阈值 ${match[2]}%。`;
  }

  match = text.match(/portfolio cvar\(95\) ([\-\d.]+)% >= ([\d.]+)%/i);
  if (match) {
    return `组合 CVaR95 ${match[1]}%，已经高于风险阈值 ${match[2]}%。`;
  }

  match = text.match(/hysteresis hold: remain (.+) until signals improve further/i);
  if (match) {
    const stateText = formatRegimeLabel(match[1]);
    return `迟滞保护仍然生效，当前继续保持 ${stateText} 状态，直到信号进一步改善。`;
  }

  return text;
};

const getOverlayElements = (scope) => {
  const prefix = String(scope || '').trim();
  return {
    panel: document.getElementById(`${prefix}OverlayPanel`),
    badge: document.getElementById(`${prefix}RegimeBadge`),
    title: document.getElementById(`${prefix}RegimeTitle`),
    subtitle: document.getElementById(`${prefix}RegimeSubtitle`),
    metrics: document.getElementById(`${prefix}OverlayMetrics`),
    exposureWrap: document.getElementById(`${prefix}ExposureWrap`),
    diagnostics: document.getElementById(`${prefix}Diagnostics`),
    reasons: document.getElementById(`${prefix}Reasons`),
    thresholds: document.getElementById(`${prefix}Thresholds`),
  };
};

const renderRegimeOverlay = (scope, alloc, asOf) => {
  const elements = getOverlayElements(scope);
  const { panel, badge, title, subtitle, metrics, exposureWrap, diagnostics, reasons, thresholds } = elements;
  if (!panel || !badge || !title || !subtitle || !metrics || !exposureWrap || !diagnostics || !reasons || !thresholds) return;

  const scopeMeta = REGIME_OVERLAY_SCOPES[scope] || REGIME_OVERLAY_SCOPES.portfolio;
  if (!alloc || typeof alloc !== 'object') {
    panel.classList.add('is-visible');
    badge.className = 'regime-badge';
    badge.textContent = 'Pending';
    title.textContent = `${scopeMeta.titlePrefix} Regime Overlay`;
    subtitle.textContent = scopeMeta.emptyText;
    metrics.innerHTML = `
      <div class='overlay-metric-card'>
        <div class='overlay-metric-label'>最大总仓位</div>
        <div class='overlay-metric-value'>--</div>
        <div class='overlay-metric-sub'>等待最新 allocation</div>
      </div>
      <div class='overlay-metric-card'>
        <div class='overlay-metric-label'>目标现金</div>
        <div class='overlay-metric-value'>--</div>
        <div class='overlay-metric-sub'>等待最新 allocation</div>
      </div>
      <div class='overlay-metric-card'>
        <div class='overlay-metric-label'>已部署仓位</div>
        <div class='overlay-metric-value'>--</div>
        <div class='overlay-metric-sub'>等待最新 allocation</div>
      </div>
      <div class='overlay-metric-card'>
        <div class='overlay-metric-label'>触发原因</div>
        <div class='overlay-metric-value'>0</div>
        <div class='overlay-metric-sub'>当前无新快照</div>
      </div>
    `;
    exposureWrap.innerHTML = `
      <div class='overlay-exposure-header'>
        <span>目标分配结构</span>
        <span>等待候选池/回测产生最新 allocation</span>
      </div>
      <div class='overlay-exposure-bar'><div class='overlay-exposure-seg cash' style='flex-basis:100%;'>等待数据</div></div>
      <div class='overlay-exposure-legend'><span class='overlay-exposure-legend-item'><span class='overlay-exposure-legend-dot' style='background:#6b7a99'></span>暂无可视化数据</span></div>
    `;
    diagnostics.innerHTML = `
      <div class='overlay-section-label'>广度诊断</div>
      <div class='overlay-reasons-empty'>当前还没有最新的 regime 快照，所以阈值条和组合检查暂时不显示。</div>
    `;
    reasons.innerHTML = `
      <div class='overlay-section-label'>触发原因</div>
      <div class='overlay-reasons-empty'>先执行一次 portfolio optimize、paper targets update，或让 backtest 进入首次 rebalance，这里就会出现实际内容。</div>
    `;
    thresholds.innerHTML = `
      <div class='overlay-section-label'>阈值距离与主导项</div>
      <div class='overlay-reasons-empty'>暂无阈值快照。保留这个空面板，方便确认当前 scope 已正确挂载调试节点。</div>
    `;
    return;
  }

  const regime = alloc.market_regime || {};
  const stateMeta = getRegimeMeta(regime.state);
  const weights = Array.isArray(alloc.weights) ? alloc.weights.filter((entry) => Array.isArray(entry) && Number(entry[1]) > 0) : [];
  const deployedWeight = clampUnit(weights.reduce((sum, [, weight]) => sum + Number(weight || 0), 0));
  const maxGross = clampUnit(alloc.max_gross_exposure);
  const targetCash = clampUnit(Math.max(Number(alloc.target_cash_weight || 0), 1 - deployedWeight));
  const reasonList = Array.isArray(regime.reasons) ? regime.reasons : [];
  const reasonCount = reasonList.length;
  const expectedAnnualReturn = Number(alloc.expected_annual_return || 0);
  const cvar95 = Math.abs(Number(alloc.cvar_95 || 0));
  const asOfText = formatAsOfText(asOf);
  const band = getRegimeBand(regime.state);
  const hysteresisStatus = formatHysteresisStatus(reasonList);
  const thresholdInfo = getRegimeThresholdRows(regime.state, regime, expectedAnnualReturn, cvar95);
  const nearestEnter = summarizeThresholdGap(thresholdInfo.rows, 'enter', '当前档位没有更紧的进入阈值');
  const nearestExit = summarizeThresholdGap(thresholdInfo.rows, 'exit', '当前档位没有更松的退出阈值');
  const dominantDriver = getDominantGrossDriver(regime.state, regime, expectedAnnualReturn, cvar95);

  panel.classList.add('is-visible');
  badge.className = `regime-badge ${stateMeta.stateClass}`;
  badge.textContent = stateMeta.badge;
  title.textContent = `${scopeMeta.titlePrefix} · ${stateMeta.title}`;
  subtitle.textContent = `${stateMeta.subtitle}${asOfText ? ` ${asOfText}。` : ` 当前状态：${formatRegimeLabel(regime.state)}。`}`;

  metrics.innerHTML = `
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>最大总仓位</div>
      <div class='overlay-metric-value'>${formatWeightPct(maxGross)}</div>
      <div class='overlay-metric-sub'>overlay 上限</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>目标现金</div>
      <div class='overlay-metric-value'>${formatWeightPct(targetCash)}</div>
      <div class='overlay-metric-sub'>保留缓冲</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>已部署仓位</div>
      <div class='overlay-metric-value'>${formatWeightPct(deployedWeight)}</div>
      <div class='overlay-metric-sub'>目标权重合计</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>触发原因</div>
      <div class='overlay-metric-value'>${reasonCount}</div>
      <div class='overlay-metric-sub'>当前生效</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>当前档位</div>
      <div class='overlay-metric-value'>${escapeHtmlText(band.label)}</div>
      <div class='overlay-metric-sub'>gross 区间 ${escapeHtmlText(band.grossRange)}</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>Hysteresis</div>
      <div class='overlay-metric-value'>${escapeHtmlText(hysteresisStatus)}</div>
      <div class='overlay-metric-sub'>防止阈值附近反复切档</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>进入阈值</div>
      <div class='overlay-metric-value'>${escapeHtmlText(nearestEnter.value)}</div>
      <div class='overlay-metric-sub'>${escapeHtmlText(nearestEnter.sub)}</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>退出阈值</div>
      <div class='overlay-metric-value'>${escapeHtmlText(nearestExit.value)}</div>
      <div class='overlay-metric-sub'>${escapeHtmlText(nearestExit.sub)}</div>
    </div>
    <div class='overlay-metric-card'>
      <div class='overlay-metric-label'>主导指标</div>
      <div class='overlay-metric-value'>${escapeHtmlText(dominantDriver.label)}</div>
      <div class='overlay-metric-sub'>${escapeHtmlText(dominantDriver.detail)}</div>
    </div>
  `;

  const exposureSegments = [...weights]
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .map(([symbol, weight], index) => ({
      label: String(symbol || '').toUpperCase(),
      weight: clampUnit(weight),
      color: PORTFOLIO_WEIGHT_COLORS[index % PORTFOLIO_WEIGHT_COLORS.length],
      cash: false,
    }));
  if (targetCash > 0.0001) {
    exposureSegments.push({
      label: '现金',
      weight: targetCash,
      color: '#6b7a99',
      cash: true,
    });
  }
  const exposureBar = exposureSegments.length > 0
    ? exposureSegments.map((segment) => {
      const pct = formatWeightPct(segment.weight);
      const text = segment.weight >= 0.12 ? `${segment.label} ${pct}` : segment.weight >= 0.06 ? segment.label : '';
      return `<div class='overlay-exposure-seg${segment.cash ? ' cash' : ''}' style='flex-basis:${(segment.weight * 100).toFixed(2)}%;${segment.cash ? '' : `background:${segment.color};`}' title='${escapeHtmlText(`${segment.label}: ${pct}`)}'>${escapeHtmlText(text)}</div>`;
    }).join('')
    : `<div class='overlay-exposure-seg cash' style='flex-basis:100%;'>现金 100.0%</div>`;
  const exposureLegend = exposureSegments.length > 0
    ? exposureSegments.map((segment) => `<span class='overlay-exposure-legend-item'><span class='overlay-exposure-legend-dot' style='background:${segment.color}'></span>${escapeHtmlText(segment.label)} ${formatWeightPct(segment.weight)}</span>`).join('')
    : `<span class='overlay-exposure-legend-item'><span class='overlay-exposure-legend-dot' style='background:#6b7a99'></span>现金 100.0%</span>`;
  exposureWrap.innerHTML = `
    <div class='overlay-exposure-header'>
      <span>目标分配结构</span>
      <span>Gross ${formatWeightPct(deployedWeight)} / 现金 ${formatWeightPct(targetCash)}</span>
    </div>
    <div class='overlay-exposure-bar'>${exposureBar}</div>
    <div class='overlay-exposure-legend'>${exposureLegend}</div>
  `;

  const breadthRows = [
    { key: 'bearish_breadth', label: '看空广度', hint: `风险关闭阈值 ${formatWeightPct(REGIME_NEGATIVE_BREADTH_THRESHOLD, 0)}`, value: clampUnit(regime.bearish_breadth), warn: 0.5, danger: REGIME_NEGATIVE_BREADTH_THRESHOLD },
    { key: 'negative_return_breadth', label: '负收益广度', hint: `风险关闭阈值 ${formatWeightPct(REGIME_NEGATIVE_BREADTH_THRESHOLD, 0)}`, value: clampUnit(regime.negative_return_breadth), warn: 0.5, danger: REGIME_NEGATIVE_BREADTH_THRESHOLD },
    { key: 'negative_sharpe_breadth', label: '负 Sharpe 广度', hint: `风险关闭阈值 ${formatWeightPct(REGIME_NEGATIVE_BREADTH_THRESHOLD, 0)}`, value: clampUnit(regime.negative_sharpe_breadth), warn: 0.5, danger: REGIME_NEGATIVE_BREADTH_THRESHOLD },
  ];
  diagnostics.innerHTML = `
    <div class='overlay-section-label'>广度诊断</div>
    ${breadthRows.map((row) => {
      const fillClass = row.value >= row.danger ? 'danger' : row.value >= row.warn ? 'warn' : '';
      const dominantClass = dominantDriver.key === row.key ? 'is-dominant' : '';
      return `
        <div class='overlay-diagnostic-row ${dominantClass}'>
          <div class='overlay-diagnostic-name'>${row.label}<div class='overlay-diagnostic-meta'>${row.hint}</div></div>
          <div class='overlay-diagnostic-track'><div class='overlay-diagnostic-fill ${fillClass}' style='width:${(row.value * 100).toFixed(1)}%;'></div></div>
          <div class='overlay-diagnostic-value'>${formatWeightPct(row.value, 0)}${dominantDriver.key === row.key ? '<div class="overlay-diagnostic-badge">主导</div>' : ''}</div>
        </div>`;
    }).join('')}
    <div class='overlay-section-label'>组合检查</div>
    <div class='overlay-diagnostic-row ${dominantDriver.key === 'expected_return' ? 'is-dominant' : ''}'>
      <div class='overlay-diagnostic-name'>年化预期收益<div class='overlay-diagnostic-meta'>低于 0% 直接偏防守</div></div>
      <div class='overlay-diagnostic-track'><div class='overlay-diagnostic-fill ${expectedAnnualReturn < 0 ? 'danger' : expectedAnnualReturn < 0.08 ? 'warn' : ''}' style='width:${(clampUnit(Math.abs(expectedAnnualReturn) / 0.25) * 100).toFixed(1)}%;'></div></div>
      <div class='overlay-diagnostic-value'>${formatWeightPct(expectedAnnualReturn, 1)}${dominantDriver.key === 'expected_return' ? '<div class="overlay-diagnostic-badge">主导</div>' : ''}</div>
    </div>
    <div class='overlay-diagnostic-row ${dominantDriver.key === 'cvar_95' ? 'is-dominant' : ''}'>
      <div class='overlay-diagnostic-name'>CVaR 95<div class='overlay-diagnostic-meta'>风险关闭阈值 ${formatWeightPct(REGIME_CVAR_RISK_OFF_THRESHOLD, 1)}</div></div>
      <div class='overlay-diagnostic-track'><div class='overlay-diagnostic-fill ${cvar95 >= REGIME_CVAR_RISK_OFF_THRESHOLD ? 'danger' : cvar95 >= REGIME_CVAR_RISK_OFF_THRESHOLD * 0.6 ? 'warn' : ''}' style='width:${(clampUnit(cvar95 / 0.25) * 100).toFixed(1)}%;'></div></div>
      <div class='overlay-diagnostic-value'>${formatWeightPct(Number(alloc.cvar_95 || 0), 1)}${dominantDriver.key === 'cvar_95' ? '<div class="overlay-diagnostic-badge">主导</div>' : ''}</div>
    </div>
    <div class='overlay-section-label'>档位与迟滞</div>
    <div class='overlay-reason-item'>
      <strong>${escapeHtmlText(band.label)}</strong><br>
      gross 区间: ${escapeHtmlText(band.grossRange)}<br>
      ${escapeHtmlText(band.entry)}<br>
      ${escapeHtmlText(band.exit)}<br>
      当前迟滞状态: ${escapeHtmlText(hysteresisStatus)}
    </div>
  `;

  thresholds.innerHTML = thresholdInfo.rows.length > 0 ? `
    <div class='overlay-section-label'>阈值距离与主导项</div>
    <div class='overlay-threshold-grid'>
      ${thresholdInfo.rows.map((row) => `
        <div class='overlay-reason-item'>
          <div class='overlay-threshold-title'>${escapeHtmlText(row.label)}</div>
          <div class='overlay-threshold-line'><span>当前值</span><span>${formatWeightPct(row.current, 1)}</span></div>
          <div class='overlay-threshold-line'><span>${escapeHtmlText(thresholdInfo.enterLabel)}</span><span>${row.enterThreshold == null ? '最紧档' : formatThresholdGap(row.enterGap, row.direction)}</span></div>
          <div class='overlay-threshold-line'><span>${escapeHtmlText(thresholdInfo.exitLabel)}</span><span>${row.exitThreshold == null ? '最宽档' : formatThresholdGap(row.exitGap, row.direction)}</span></div>
        </div>`).join('')}
      <div class='overlay-reason-item'>
        <div class='overlay-threshold-title'>主导 max_gross_exposure</div>
        <div class='overlay-threshold-line'><span>当前主导项</span><span class='overlay-threshold-emphasis'>${escapeHtmlText(dominantDriver.label)}</span></div>
        <div class='overlay-threshold-line'><span>有效压力</span><span>${escapeHtmlText(dominantDriver.detail)}</span></div>
        <div class='overlay-threshold-line'><span>解释</span><span>当前 gross 压缩主要由这一项决定</span></div>
      </div>
    </div>
  ` : `
    <div class='overlay-section-label'>阈值距离与主导项</div>
    <div class='overlay-reasons-empty'>当前 allocation 没有返回阈值距离明细，保留空面板以便继续排查数据链路。</div>
  `;

  if (reasonCount === 0) {
    reasons.innerHTML = `
      <div class='overlay-section-label'>触发原因</div>
      <div class='overlay-reasons-empty'>当前没有额外防守节流，组合按常规 overlay 约束运行。</div>
    `;
    return;
  }

  reasons.innerHTML = `
    <div class='overlay-section-label'>触发原因</div>
    <div class='overlay-reason-list'>
      ${reasonList.map((reason) => `<div class='overlay-reason-item'>${escapeHtmlText(translateRegimeReason(reason))}</div>`).join('')}
    </div>
  `;
};

const renderPortfolioOverlay = (alloc) => renderRegimeOverlay('portfolio', alloc, null);
const renderPaperOverlay = (paperStatus) => renderRegimeOverlay('paper', paperStatus?.latest_allocation, paperStatus?.latest_allocation_as_of);
const renderFutuOverlay = (futuStatus) => renderRegimeOverlay('futu', futuStatus?.latest_allocation, futuStatus?.latest_allocation_as_of);
const renderBacktestOverlay = (backtestStatus) => renderRegimeOverlay('backtest', backtestStatus?.latest_allocation, backtestStatus?.latest_allocation_as_of);

const normalizeTradeSide = (side) => {
  const s = String(side || '').toUpperCase();
  if (s === 'BUY' || s === 'B') return 'BUY';
  if (s === 'SELL' || s === 'S') return 'SELL';
  return s || 'UNKNOWN';
};

const normalizeSymbols = (text) => {
  return [...new Set(String(text || '')
    .toUpperCase()
    .split(',')
    .map(s => s.trim())
    .filter(Boolean))];
};

const addPaperTargetSymbols = (symbols) => {
  const merged = new Set(manualPaperTargets);
  for (const s of symbols) merged.add(s);
  manualPaperTargets = Array.from(merged).sort();
  paperTargetsDirty = true;
};

const removePaperTargetSymbol = (symbol) => {
  manualPaperTargets = manualPaperTargets.filter(s => s !== symbol);
  paperTargetsDirty = true;
};

const renderPaperTargetChips = () => {
  if (!paperTargetChips) return;
  const n = manualPaperTargets.length;
  if (paperTargetCount) {
    const poolText = n === 1 ? '1 symbol' : `${n} symbols`;
    paperTargetCount.textContent = poolText;
    const ctrlPool = document.getElementById('paperCtrlPool');
    if (ctrlPool) ctrlPool.textContent = poolText;
  }
  if (paperTargetDirtyBadge) {
    paperTargetDirtyBadge.style.display = paperTargetsDirty ? '' : 'none';
  }
  if (!n) {
    paperTargetChips.innerHTML = `<span class="paper-target-chips-empty">No symbols — Start will fall back to Portfolio weights</span>`;
    return;
  }
  paperTargetChips.innerHTML = manualPaperTargets
    .map(sym => `<span class='paper-target-chip'>${sym}<button class='paper-target-remove' data-paper-target-remove='${sym}' title='Remove ${sym}'>×</button></span>`)
    .join('');
  paperTargetChips.querySelectorAll('[data-paper-target-remove]').forEach(btn => {
    btn.addEventListener('click', () => {
      removePaperTargetSymbol(btn.dataset.paperTargetRemove);
      renderPaperTargetChips();
    });
  });
  renderStrategyDispatchPreview();
};

const hydratePaperTargetsFromStatus = (st) => {
  if (paperTargetsDirty) return;
  const fromCandidatePool = (st?.candidate_symbols || [])
    .map(x => String(x || '').toUpperCase())
    .filter(Boolean);
  const fromStatus = fromCandidatePool.length
    ? fromCandidatePool
    : (st?.target_weights || [])
      .map(x => String(x?.symbol || '').toUpperCase())
      .filter(Boolean);
  if (fromStatus.length) {
    manualPaperTargets = [...new Set(fromStatus)].sort();
    renderPaperTargetChips();
  }
};

const hydratePaperOptimizationFromStatus = (st) => {
  const timeText = String(st?.optimization_time_local || '').trim();
  if (timeText && paperOptTimeInput && paperOptTimeInput.value !== timeText) {
    paperOptTimeInput.value = timeText;
  }

  const days = Array.isArray(st?.optimization_weekdays)
    ? st.optimization_weekdays.map(x => Number(x)).filter(x => x >= 1 && x <= 7)
    : [];
  if (days.length > 0 && paperOptWeekdayChecks.length > 0) {
    const selected = new Set(days);
    for (const el of paperOptWeekdayChecks) {
      const day = Number(el.getAttribute('data-paper-opt-weekday') || 0);
      el.checked = selected.has(day);
    }
  }

  syncNextOptimizationBadge();
  renderStrategyDispatchPreview();
};

const forceSyncPaperTargetsFromStatus = async () => {
  try {
    const st = await api('/api/paper/status');
    const fromCandidatePool = (st?.candidate_symbols || [])
      .map(x => String(x || '').toUpperCase())
      .filter(Boolean);
    const fromStatus = fromCandidatePool.length
      ? fromCandidatePool
      : (st?.target_weights || [])
        .map(x => String(x?.symbol || '').toUpperCase())
        .filter(Boolean);
    if (fromStatus.length) {
      manualPaperTargets = [...new Set(fromStatus)].sort();
      paperTargetsDirty = false;
      renderPaperTargetChips();
    }
  } catch {
    // ignore sync failures
  }
};

const applyTradeToCostBasis = (trade) => {
  const symbol = trade.symbol;
  const side = trade.side;
  const quantity = Math.max(0, Number(trade.quantity) || 0);
  const price = Number(trade.price) || 0;

  const current = paperCostBasis.get(symbol) || { quantity: 0, avgCost: 0 };
  let nextQty = current.quantity;
  let nextAvg = current.avgCost;
  let realizedUsd = 0;

  if (side === 'BUY') {
    nextQty = current.quantity + quantity;
    nextAvg = nextQty > 0
      ? ((current.avgCost * current.quantity) + (price * quantity)) / nextQty
      : 0;
  } else if (side === 'SELL') {
    const matchedQty = Math.min(quantity, Math.max(0, current.quantity));
    if (matchedQty > 0) {
      realizedUsd = (price - current.avgCost) * matchedQty;
      nextQty = current.quantity - matchedQty;
      if (nextQty <= 1e-8) {
        nextQty = 0;
        nextAvg = 0;
      }
    }
  }

  paperCostBasis.set(symbol, { quantity: nextQty, avgCost: nextAvg });
  return realizedUsd;
};

const ingestPaperTrades = (paperStatus) => {
  const fullTradeHistory = Array.isArray(paperStatus?.trade_history) ? paperStatus.trade_history : null;
  if (fullTradeHistory && fullTradeHistory.length > 0) {
    paperTradeHistory = [];
    paperTradeSeenKeys = new Set();
    paperCostBasis = new Map();

    for (const tr of fullTradeHistory) {
      const symbol = String(tr?.symbol || '').toUpperCase();
      const side = normalizeTradeSide(tr?.side);
      const quantity = Number(tr?.quantity);
      const price = Number(tr?.price);
      const fee = Number(tr?.fee || 0);
      const notional = Number(tr?.notional || (quantity * price));
      const timestamp = tr?.timestamp || new Date().toISOString();

      if (!symbol || !Number.isFinite(quantity) || quantity <= 0 || !Number.isFinite(price) || price <= 0) {
        continue;
      }

      const key = [timestamp, symbol, side, quantity.toFixed(8), price.toFixed(8), fee.toFixed(8)].join('|');
      if (paperTradeSeenKeys.has(key)) continue;

      const realizedUsd = applyTradeToCostBasis({ symbol, side, quantity, price });
      const parsedTs = Date.parse(timestamp);
      const timestampMs = Number.isFinite(parsedTs) ? parsedTs : Date.now();

      paperTradeSeenKeys.add(key);
      paperTradeHistory.push({
        key,
        timestamp,
        timestampMs,
        symbol,
        side,
        quantity,
        price,
        notional: Number.isFinite(notional) ? notional : quantity * price,
        fee: Number.isFinite(fee) ? fee : 0,
        realizedUsd,
      });
    }

    paperTradeHistory.sort((a, b) => b.timestampMs - a.timestampMs);
    if (paperTradeHistory.length > 300) {
      paperTradeHistory = paperTradeHistory.slice(0, 300);
    }
    return;
  }

  const analysis = paperStatus?.last_analysis;
  const trades = Array.isArray(analysis?.trades) ? analysis.trades : [];
  if (trades.length === 0) return;

  for (const tr of trades) {
    const symbol = String(tr?.symbol || '').toUpperCase();
    const side = normalizeTradeSide(tr?.side);
    const quantity = Number(tr?.quantity);
    const price = Number(tr?.price);
    const fee = Number(tr?.fee || 0);
    const notional = Number(tr?.notional || (quantity * price));
    const timestamp = tr?.timestamp || analysis?.timestamp || new Date().toISOString();

    if (!symbol || !Number.isFinite(quantity) || quantity <= 0 || !Number.isFinite(price) || price <= 0) {
      continue;
    }

    const key = [timestamp, symbol, side, quantity.toFixed(8), price.toFixed(8), fee.toFixed(8)].join('|');
    if (paperTradeSeenKeys.has(key)) continue;

    const realizedUsd = applyTradeToCostBasis({ symbol, side, quantity, price });
    const parsedTs = Date.parse(timestamp);
    const timestampMs = Number.isFinite(parsedTs) ? parsedTs : Date.now();

    paperTradeSeenKeys.add(key);
    paperTradeHistory.push({
      key,
      timestamp,
      timestampMs,
      symbol,
      side,
      quantity,
      price,
      notional: Number.isFinite(notional) ? notional : quantity * price,
      fee: Number.isFinite(fee) ? fee : 0,
      realizedUsd,
    });
  }

  paperTradeHistory.sort((a, b) => b.timestampMs - a.timestampMs);
  if (paperTradeHistory.length > 300) {
    paperTradeHistory = paperTradeHistory.slice(0, 300);
  }
};

const renderTradeHistory = () => {
  const box = document.getElementById('tradeHistory');
  if (!box) return;

  const normalizedSearch = tradeSearchText.trim().toUpperCase();
  const filteredTrades = paperTradeHistory.filter((tr) => {
    if (selectedTradeFilter === 'buy' && tr.side !== 'BUY') return false;
    if (selectedTradeFilter === 'sell' && tr.side !== 'SELL') return false;
    if (selectedTradeFilter === 'profit' && !(tr.side === 'SELL' && tr.realizedUsd > 0)) return false;
    if (selectedTradeFilter === 'loss' && !(tr.side === 'SELL' && tr.realizedUsd < 0)) return false;
    if (normalizedSearch && !String(tr.symbol || '').toUpperCase().includes(normalizedSearch)) return false;
    return true;
  });

  if (!filteredTrades.length) {
    box.innerHTML = `<div class='empty-state'><div class='empty-state-icon'>📝</div>No trades yet.<br>Start paper trading to see execution history.</div>`;
    return;
  }

  box.innerHTML = filteredTrades.map((tr) => {
    const sideClass = tr.side === 'BUY' ? 'buy' : (tr.side === 'SELL' ? 'sell' : '');
    const sideText = tr.side === 'BUY' ? 'BUY' : (tr.side === 'SELL' ? 'SELL' : tr.side);
    const feeText = Number.isFinite(tr.fee) ? `$${tr.fee.toFixed(2)}` : '--';
    const realizedClass = tr.realizedUsd > 0 ? 'up' : (tr.realizedUsd < 0 ? 'down' : 'flat');
    const realizedText = tr.side === 'SELL' ? formatSignedMoney(tr.realizedUsd) : '--';
    const ts = Number.isFinite(tr.timestampMs) ? new Date(tr.timestampMs).toLocaleString() : tr.timestamp;

    return `
      <div class='trade-item'>
        <div class='trade-time'>${ts}</div>
        <div><span class='trade-side ${sideClass}'>${sideText}</span></div>
        <div class='trade-main'>
          <span class='trade-symbol'>${tr.symbol}</span>
          <span>${tr.quantity.toFixed(2)} @ $${tr.price.toFixed(2)}</span>
          <span class='trade-meta'>Notional $${tr.notional.toFixed(2)} · Fee ${feeText}</span>
        </div>
        <div class='trade-pnl ${realizedClass}'>${realizedText}</div>
      </div>
    `;
  }).join('');
};

const resetPaperTradeState = () => {
  paperTradeHistory = [];
  paperTradeSeenKeys = new Set();
  paperCostBasis = new Map();
  renderTradeHistory();
  if (typeof portfolioLine?.setMarkers === 'function') {
    portfolioLine.setMarkers([]);
  }
};

const setPaperTradeMarkers = () => {
  if (typeof portfolioLine?.setMarkers !== 'function') return;

  const markers = [];
  const seen = new Set();
  for (const tr of paperTradeHistory) {
    const t = Math.floor(Number(tr.timestampMs) / 1000);
    if (!Number.isFinite(t) || t <= 0) continue;
    const markerKey = `${t}-${tr.side}-${tr.symbol}`;
    if (seen.has(markerKey)) continue;
    seen.add(markerKey);

    const isBuy = tr.side === 'BUY';
    markers.push({
      time: t,
      position: isBuy ? 'belowBar' : 'aboveBar',
      color: isBuy ? '#00d4aa' : '#ff4757',
      shape: isBuy ? 'arrowUp' : 'arrowDown',
      text: `${tr.side} ${tr.symbol}`,
    });
  }

  markers.sort((a, b) => a.time - b.time);
  portfolioLine.setMarkers(markers);
};

const computeMaxDrawdownPct = (series = []) => {
  if (!Array.isArray(series) || series.length === 0) return null;
  let peak = -Infinity;
  let maxDrawdown = 0;
  for (const point of series) {
    const value = Number(point?.value);
    if (!Number.isFinite(value) || value <= 0) continue;
    peak = Math.max(peak, value);
    if (peak > 0) {
      const drawdown = ((value - peak) / peak) * 100;
      maxDrawdown = Math.min(maxDrawdown, drawdown);
    }
  }
  return maxDrawdown;
};

const BACKTEST_TRADING_DAYS_PER_YEAR = 252;
const BACKTEST_RISK_FREE_RATE = 0.05;

const computeMean = (values = []) => {
  if (!Array.isArray(values) || values.length === 0) return NaN;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
};

const computeSampleStd = (values = []) => {
  if (!Array.isArray(values) || values.length < 2) return NaN;
  const mean = computeMean(values);
  const variance = values.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / (values.length - 1);
  return Math.sqrt(Math.max(variance, 0));
};

const computeCovariance = (lhs = [], rhs = []) => {
  if (!Array.isArray(lhs) || !Array.isArray(rhs) || lhs.length < 2 || lhs.length !== rhs.length) return NaN;
  const meanLhs = computeMean(lhs);
  const meanRhs = computeMean(rhs);
  let sum = 0;
  for (let idx = 0; idx < lhs.length; idx += 1) {
    sum += (lhs[idx] - meanLhs) * (rhs[idx] - meanRhs);
  }
  return sum / (lhs.length - 1);
};

const buildReturnMapFromSeries = (series = []) => {
  const out = new Map();
  if (!Array.isArray(series) || series.length < 2) return out;
  let prev = null;
  for (const point of series) {
    const time = Number(point?.time);
    const value = Number(point?.value);
    if (!Number.isFinite(time) || !Number.isFinite(value) || value <= 0) continue;
    if (prev && prev.value > 0) {
      out.set(time, (value / prev.value) - 1);
    }
    prev = { time, value };
  }
  return out;
};

const computeBacktestPerformance = (ctx) => {
  const portfolioSeries = Array.isArray(ctx?.portfolioSeries) ? ctx.portfolioSeries : [];
  const benchmarkSeries = Array.isArray(ctx?.benchmarkSeries) ? ctx.benchmarkSeries : [];
  if (portfolioSeries.length < 2) return null;

  const startValue = Number(portfolioSeries[0]?.value);
  const finalValue = Number(portfolioSeries[portfolioSeries.length - 1]?.value);
  if (!Number.isFinite(startValue) || !Number.isFinite(finalValue) || startValue <= 0 || finalValue <= 0) {
    return null;
  }

  const benchmarkStartValue = Number(benchmarkSeries[0]?.value);
  const benchmarkFinalValue = Number(benchmarkSeries[benchmarkSeries.length - 1]?.value);
  const portfolioReturnMap = buildReturnMapFromSeries(portfolioSeries);
  const benchmarkReturnMap = buildReturnMapFromSeries(benchmarkSeries);
  const portfolioReturns = Array.from(portfolioReturnMap.values()).filter((value) => Number.isFinite(value));
  const benchmarkReturns = Array.from(benchmarkReturnMap.values()).filter((value) => Number.isFinite(value));

  const alignedPortfolioReturns = [];
  const alignedBenchmarkReturns = [];
  for (const [time, portRet] of portfolioReturnMap.entries()) {
    const benchRet = benchmarkReturnMap.get(time);
    if (Number.isFinite(portRet) && Number.isFinite(benchRet)) {
      alignedPortfolioReturns.push(portRet);
      alignedBenchmarkReturns.push(benchRet);
    }
  }

  const totalReturn = (finalValue / startValue) - 1;
  const benchmarkTotalReturn = Number.isFinite(benchmarkStartValue) && Number.isFinite(benchmarkFinalValue) && benchmarkStartValue > 0
    ? (benchmarkFinalValue / benchmarkStartValue) - 1
    : NaN;
  const tradingDays = portfolioReturns.length;
  const years = tradingDays > 0 ? tradingDays / BACKTEST_TRADING_DAYS_PER_YEAR : NaN;
  const cagr = Number.isFinite(years) && years > 0
    ? Math.pow(finalValue / startValue, 1 / years) - 1
    : NaN;
  const benchmarkCagr = Number.isFinite(benchmarkTotalReturn) && Number.isFinite(years) && years > 0
    ? Math.pow(1 + benchmarkTotalReturn, 1 / years) - 1
    : NaN;

  const dailyRf = Math.pow(1 + BACKTEST_RISK_FREE_RATE, 1 / BACKTEST_TRADING_DAYS_PER_YEAR) - 1;
  const portfolioStd = computeSampleStd(portfolioReturns);
  const benchmarkStd = computeSampleStd(alignedBenchmarkReturns.length > 1 ? alignedBenchmarkReturns : benchmarkReturns);
  const annualVol = Number.isFinite(portfolioStd)
    ? portfolioStd * Math.sqrt(BACKTEST_TRADING_DAYS_PER_YEAR)
    : NaN;
  const sharpe = Number.isFinite(portfolioStd) && portfolioStd > 1e-12
    ? ((computeMean(portfolioReturns) - dailyRf) / portfolioStd) * Math.sqrt(BACKTEST_TRADING_DAYS_PER_YEAR)
    : NaN;
  const benchmarkSharpe = Number.isFinite(benchmarkStd) && benchmarkStd > 1e-12
    ? ((computeMean(alignedBenchmarkReturns.length > 0 ? alignedBenchmarkReturns : benchmarkReturns) - dailyRf) / benchmarkStd) * Math.sqrt(BACKTEST_TRADING_DAYS_PER_YEAR)
    : NaN;

  const downsideReturns = portfolioReturns
    .map((value) => value - dailyRf)
    .filter((value) => Number.isFinite(value) && value < 0);
  const benchmarkDownsideReturns = (alignedBenchmarkReturns.length > 0 ? alignedBenchmarkReturns : benchmarkReturns)
    .map((value) => value - dailyRf)
    .filter((value) => Number.isFinite(value) && value < 0);
  const downsideStd = computeSampleStd(downsideReturns);
  const benchmarkDownsideStd = computeSampleStd(benchmarkDownsideReturns);
  const sortino = Number.isFinite(downsideStd) && downsideStd > 1e-12
    ? ((computeMean(portfolioReturns) - dailyRf) / downsideStd) * Math.sqrt(BACKTEST_TRADING_DAYS_PER_YEAR)
    : NaN;
  const benchmarkSortino = Number.isFinite(benchmarkDownsideStd) && benchmarkDownsideStd > 1e-12
    ? ((computeMean(alignedBenchmarkReturns.length > 0 ? alignedBenchmarkReturns : benchmarkReturns) - dailyRf) / benchmarkDownsideStd) * Math.sqrt(BACKTEST_TRADING_DAYS_PER_YEAR)
    : NaN;

  const maxDrawdownPct = computeMaxDrawdownPct(portfolioSeries);
  const benchmarkMaxDrawdownPct = computeMaxDrawdownPct(benchmarkSeries);
  const calmar = Number.isFinite(cagr) && Number.isFinite(maxDrawdownPct) && maxDrawdownPct < 0
    ? cagr / Math.abs(maxDrawdownPct / 100)
    : NaN;
  const benchmarkCalmar = Number.isFinite(benchmarkCagr) && Number.isFinite(benchmarkMaxDrawdownPct) && benchmarkMaxDrawdownPct < 0
    ? benchmarkCagr / Math.abs(benchmarkMaxDrawdownPct / 100)
    : NaN;

  const activeReturns = alignedPortfolioReturns.map((value, idx) => value - alignedBenchmarkReturns[idx]);
  const trackingError = computeSampleStd(activeReturns);
  const informationRatio = Number.isFinite(trackingError) && trackingError > 1e-12
    ? (computeMean(activeReturns) / trackingError) * Math.sqrt(BACKTEST_TRADING_DAYS_PER_YEAR)
    : NaN;

  const benchmarkVariance = computeCovariance(alignedBenchmarkReturns, alignedBenchmarkReturns);
  const covariance = computeCovariance(alignedPortfolioReturns, alignedBenchmarkReturns);
  const beta = Number.isFinite(benchmarkVariance) && benchmarkVariance > 1e-12 && Number.isFinite(covariance)
    ? covariance / benchmarkVariance
    : NaN;
  const alphaAnnual = Number.isFinite(beta)
    ? (((computeMean(alignedPortfolioReturns) - dailyRf) - beta * (computeMean(alignedBenchmarkReturns) - dailyRf)) * BACKTEST_TRADING_DAYS_PER_YEAR)
    : NaN;

  const positiveDays = portfolioReturns.filter((value) => Number.isFinite(value) && value > 0).length;
  const winRate = tradingDays > 0 ? (positiveDays / tradingDays) * 100 : NaN;

  return {
    startValue,
    finalValue,
    totalReturnPct: totalReturn * 100,
    totalPnlUsd: finalValue - startValue,
    benchmarkFinalValue,
    benchmarkTotalReturnPct: benchmarkTotalReturn * 100,
    benchmarkPnlUsd: Number.isFinite(benchmarkFinalValue) ? benchmarkFinalValue - benchmarkStartValue : NaN,
    alphaPct: (totalReturn - benchmarkTotalReturn) * 100,
    alphaUsd: Number.isFinite(benchmarkFinalValue) ? finalValue - benchmarkFinalValue : NaN,
    cagrPct: cagr * 100,
    benchmarkCagrPct: benchmarkCagr * 100,
    annualVolPct: annualVol * 100,
    benchmarkAnnualVolPct: (benchmarkStd * Math.sqrt(BACKTEST_TRADING_DAYS_PER_YEAR)) * 100,
    maxDrawdownPct,
    benchmarkMaxDrawdownPct,
    sharpe,
    benchmarkSharpe,
    sortino,
    benchmarkSortino,
    calmar,
    benchmarkCalmar,
    beta,
    alphaAnnualPct: alphaAnnual * 100,
    informationRatio,
    winRate,
    tradingDays,
  };
};

const normalizeBacktestSummary = (summary) => {
  if (!summary || typeof summary !== 'object') return null;
  return {
    totalReturnPct: Number(summary.strategy_total_return_pct),
    totalPnlUsd: Number(summary.strategy_total_pnl_usd),
    benchmarkTotalReturnPct: Number(summary.benchmark_total_return_pct),
    benchmarkPnlUsd: Number(summary.benchmark_total_pnl_usd),
    alphaPct: Number(summary.alpha_pct),
    alphaUsd: Number(summary.alpha_usd),
    cagrPct: Number(summary.strategy_cagr_pct),
    benchmarkCagrPct: Number(summary.benchmark_cagr_pct),
    annualVolPct: Number(summary.strategy_annual_vol_pct),
    benchmarkAnnualVolPct: Number(summary.benchmark_annual_vol_pct),
    maxDrawdownPct: Number(summary.strategy_max_drawdown_pct),
    benchmarkMaxDrawdownPct: Number(summary.benchmark_max_drawdown_pct),
    sharpe: Number(summary.strategy_sharpe),
    benchmarkSharpe: Number(summary.benchmark_sharpe),
    sortino: Number(summary.strategy_sortino),
    benchmarkSortino: Number(summary.benchmark_sortino),
    calmar: Number(summary.strategy_calmar),
    benchmarkCalmar: Number(summary.benchmark_calmar),
    beta: Number(summary.beta_vs_benchmark),
    alphaAnnualPct: Number(summary.annualized_alpha_pct),
    informationRatio: Number(summary.information_ratio),
    winRate: Number(summary.strategy_win_rate_pct),
    tradingDays: Number(summary.trading_days),
  };
};

const renderPaperKpis = (paperStatus) => {
  const grid = document.getElementById('paperKpiGrid');
  if (!grid) return;
  renderPaperOverlay(paperStatus);

  const snapshot = paperStatus?.latest_snapshot;
  if (!snapshot) {
    grid.innerHTML = `<div class='empty-state'><div class='empty-state-icon'>📉</div>No live paper snapshot yet.</div>`;
    return;
  }

  const totalAssets = Number(snapshot.total_value);
  const pnlUsd = Number(snapshot.pnl_usd);
  const pnlPct = Number(snapshot.pnl_pct);
  const cashUsd = Number(snapshot.cash_usd);
  const investedPct = Number.isFinite(totalAssets) && totalAssets > 0 && Number.isFinite(cashUsd)
    ? ((totalAssets - cashUsd) / totalAssets) * 100
    : null;

  const maxDrawdownPct = computeMaxDrawdownPct(paperFullContext?.portfolioSeries || []);
  const sellTrades = paperTradeHistory.filter((x) => x.side === 'SELL');
  const winners = sellTrades.filter((x) => Number(x.realizedUsd) > 0).length;
  const winRate = sellTrades.length > 0 ? (winners / sellTrades.length) * 100 : null;
  const spreadPct = Number(paperFullContext?.latest?.spreadPct);

  const investedClass = Number.isFinite(investedPct) && investedPct > 90 ? 'down' : 'up';
  const spreadClass = Number.isFinite(spreadPct) && spreadPct < 0 ? 'down' : 'up';

  // Determine card mood classes for glassmorphism
  const pnlMood = Number.isFinite(pnlUsd) ? (pnlUsd >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const investedMood = Number.isFinite(investedPct) && investedPct > 90 ? 'kpi-warn' : 'kpi-neutral';
  const ddMood = Number.isFinite(maxDrawdownPct) && maxDrawdownPct < -5 ? 'kpi-negative' : (Number.isFinite(maxDrawdownPct) ? 'kpi-neutral' : 'kpi-neutral');
  const winMood = Number.isFinite(winRate) ? (winRate >= 50 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const spreadMood = Number.isFinite(spreadPct) ? (spreadPct >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';

  grid.innerHTML = `
    <div class='paper-kpi-card kpi-neutral'>
      <div class='paper-kpi-label'>Account NAV</div>
      <div class='paper-kpi-value'>${Number.isFinite(totalAssets) ? `$${totalAssets.toFixed(2)}` : '--'}</div>
      <div class='futu-kpi-sub'>USD</div>
    </div>
    <div class='paper-kpi-card ${pnlMood}'>
      <div class='paper-kpi-label'>Account PnL</div>
      <div class='paper-kpi-value ${Number.isFinite(pnlUsd) && pnlUsd < 0 ? 'down' : 'up'}'>${Number.isFinite(pnlUsd) ? `${formatSignedMoney(pnlUsd)} (${formatPct(pnlPct)})` : '--'}</div>
      <div class='futu-kpi-sub'>USD</div>
    </div>
    <div class='paper-kpi-card ${investedMood}'>
      <div class='paper-kpi-label'>Open Risk</div>
      <div class='paper-kpi-value ${investedClass}'>${Number.isFinite(investedPct) ? `${investedPct.toFixed(1)}%` : '--'}</div>
      <div class='futu-kpi-sub'>%</div>
    </div>
    <div class='paper-kpi-card ${ddMood}'>
      <div class='paper-kpi-label'>Max Drawdown</div>
      <div class='paper-kpi-value ${Number.isFinite(maxDrawdownPct) && maxDrawdownPct < 0 ? 'down' : ''}'>${Number.isFinite(maxDrawdownPct) ? `${maxDrawdownPct.toFixed(2)}%` : '--'}</div>
      <div class='futu-kpi-sub'>%</div>
    </div>
    <div class='paper-kpi-card ${winMood}'>
      <div class='paper-kpi-label'>Win Rate (SELL)</div>
      <div class='paper-kpi-value'>${Number.isFinite(winRate) ? `${winRate.toFixed(1)}%` : '--'}</div>
      <div class='futu-kpi-sub'>%</div>
    </div>
    <div class='paper-kpi-card ${spreadMood}'>
      <div class='paper-kpi-label'>vs Benchmark</div>
      <div class='paper-kpi-value ${spreadClass}'>${Number.isFinite(spreadPct) ? `${spreadPct >= 0 ? '+' : ''}${spreadPct.toFixed(2)}%` : '--'}</div>
      <div class='futu-kpi-sub'>%</div>
    </div>
  `;

  // Session duration badge
  const badge = document.getElementById('kpiDurationBadge');
  if (badge) {
    if (paperSessionStartMs && paperStatus?.running) {
      const elapsed = Date.now() - paperSessionStartMs;
      const h = Math.floor(elapsed / 3600000);
      const m = Math.floor((elapsed % 3600000) / 60000);
      badge.textContent = `⏱ ${h > 0 ? h + 'h ' : ''}${m}m`;
      badge.style.display = '';
    } else {
      badge.style.display = 'none';
    }
  }
};

const computeFutuSellWinRate = (futuStatus) => {
  const rows = Array.isArray(futuStatus?.trade_history) ? futuStatus.trade_history : [];
  if (!rows.length) return null;

  const sellRows = rows.filter((row) => String(futuReadField(row, ['trd_side', 'side'], '')).toUpperCase().includes('SELL'));
  if (!sellRows.length) return null;

  let validCount = 0;
  let winners = 0;
  for (const row of sellRows) {
    const pnl = futuReadNumber(row, ['realized_pnl', 'realized_pl', 'pl_val', 'profit', 'pnl_usd']);
    if (!Number.isFinite(pnl)) continue;
    validCount += 1;
    if (pnl > 0) winners += 1;
  }
  if (validCount === 0) return null;
  return (winners / validCount) * 100;
};

const renderFutuConnectionKpis = (futuStatus, futuContext) => {
  const grid = document.getElementById('futuConnKpiGrid');
  const badge = document.getElementById('futuKpiDurationBadge');
  if (!grid) return;
  renderFutuOverlay(futuStatus);
  renderFutuAccountSectionPreview(futuStatus);

  const snapshot = futuStatus?.latest_snapshot;
  const strategySnapshot = futuStatus?.latest_strategy_snapshot;
  if (!snapshot) {
    grid.innerHTML = `<div class='empty-state'><div class='empty-state-icon'>📉</div>No live FUTU snapshot yet.</div>`;
    if (badge) badge.style.display = 'none';
    return;
  }

  const totalAssets = Number(snapshot.total_value);
  const pnlUsd = Number(snapshot.pnl_usd);
  const pnlPct = Number(snapshot.pnl_pct);
  const cashUsd = Number.isFinite(Number(futuStatus?.account_cash_usd))
    ? Number(futuStatus?.account_cash_usd)
    : Number(snapshot.cash_usd);
  const investedPct = Number.isFinite(totalAssets) && totalAssets > 0 && Number.isFinite(cashUsd)
    ? ((totalAssets - cashUsd) / totalAssets) * 100
    : null;

  const maxDrawdownPct = computeMaxDrawdownPct(futuContext?.portfolioSeries || []);
  const winRate = computeFutuSellWinRate(futuStatus);
  const spreadPct = Number(futuContext?.latest?.spreadPct);
  const strategyNav = Number(strategySnapshot?.total_value);
  const strategyPnlUsd = Number(strategySnapshot?.pnl_usd);
  const strategyPnlPct = Number(strategySnapshot?.pnl_pct);
  const capitalLimitUsd = Number(futuStatus?.rebalance_capital_limit_usd);
  const strategyBaseCapitalUsd = Number(futuStatus?.strategy_start_capital_usd);
  const hasCapitalLimit = Number.isFinite(capitalLimitUsd) && capitalLimitUsd > 0;
  const strategyPnlMood = Number.isFinite(strategyPnlUsd)
    ? (strategyPnlUsd >= 0 ? 'kpi-positive' : 'kpi-negative')
    : 'kpi-neutral';

  const pnlMood = Number.isFinite(pnlUsd) ? (pnlUsd >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const investedMood = Number.isFinite(investedPct) && investedPct > 90 ? 'kpi-warn' : 'kpi-neutral';
  const ddMood = Number.isFinite(maxDrawdownPct) && maxDrawdownPct < -5 ? 'kpi-negative' : 'kpi-neutral';
  const winMood = Number.isFinite(winRate) ? (winRate >= 50 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const spreadMood = Number.isFinite(spreadPct) ? (spreadPct >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const investedClass = Number.isFinite(investedPct) && investedPct > 90 ? 'down' : 'up';
  const spreadClass = Number.isFinite(spreadPct) && spreadPct < 0 ? 'down' : 'up';

  grid.innerHTML = `
    <div class='futu-kpi-tail'>
      <div class='futu-kpi-summary-item ${ddMood}'>
        <div class='futu-kpi-summary-label'>Max Drawdown</div>
        <div class='futu-kpi-summary-value ${Number.isFinite(maxDrawdownPct) && maxDrawdownPct < 0 ? 'down' : ''}'>${Number.isFinite(maxDrawdownPct) ? `${maxDrawdownPct.toFixed(2)}%` : '--'}</div>
      </div>
      <div class='futu-kpi-summary-item ${winMood}'>
        <div class='futu-kpi-summary-label'>Win Rate</div>
        <div class='futu-kpi-summary-value'>${Number.isFinite(winRate) ? `${winRate.toFixed(1)}%` : '--'}</div>
      </div>
      <div class='futu-kpi-summary-item ${spreadMood}'>
        <div class='futu-kpi-summary-label'>vs Benchmark</div>
        <div class='futu-kpi-summary-value ${spreadClass}'>${Number.isFinite(spreadPct) ? `${spreadPct >= 0 ? '+' : ''}${spreadPct.toFixed(2)}%` : '--'}</div>
      </div>
    </div>
    <div class='futu-kpi-split'>
      <section class='futu-kpi-section account'>
        <div class='futu-kpi-section-head'>
          <div class='futu-kpi-section-title'>Account</div>
          <div class='futu-kpi-section-note'>Broker snapshot</div>
        </div>
        <div class='futu-kpi-section-grid'>
          <div class='paper-kpi-card ${pnlMood} account-pnl-highlight'>
            <div class='paper-kpi-label'>Account PnL</div>
            <div class='paper-kpi-value ${Number.isFinite(pnlUsd) && pnlUsd < 0 ? 'down' : 'up'}'>${Number.isFinite(pnlUsd) ? `${formatSignedMoney(pnlUsd)} (${formatPct(pnlPct)})` : '--'}</div>
            <div class='futu-kpi-sub'>Broker mark-to-market</div>
          </div>
          <div class='paper-kpi-card kpi-neutral'>
            <div class='paper-kpi-label'>Account NAV</div>
            <div class='paper-kpi-value'>${Number.isFinite(totalAssets) ? `$${totalAssets.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '--'}</div>
            <div class='futu-kpi-sub'>Broker equity</div>
          </div>
          <div class='paper-kpi-card kpi-neutral'>
            <div class='paper-kpi-label'>Account Cash</div>
            <div class='paper-kpi-value' id='futuAccountCash'>${Number.isFinite(cashUsd) ? `$${cashUsd.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '--'}</div>
            <div class='futu-kpi-sub'>Available USD cash</div>
          </div>
          <div class='paper-kpi-card ${investedMood}'>
            <div class='paper-kpi-label'>Open Risk</div>
            <div class='paper-kpi-value ${investedClass}'>${Number.isFinite(investedPct) ? `${investedPct.toFixed(1)}%` : '--'}</div>
            <div class='futu-kpi-sub'>Capital deployed</div>
          </div>
        </div>
      </section>
      <section class='futu-kpi-section sleeve'>
        <div class='futu-kpi-section-head'>
          <div class='futu-kpi-section-title'>Sleeve</div>
          <div class='futu-kpi-section-note'>Strategy snapshot</div>
        </div>
        <div class='futu-kpi-section-grid'>
          <div class='paper-kpi-card kpi-neutral'>
            <div class='paper-kpi-label'>Sleeve NAV</div>
            <div class='paper-kpi-value'>${Number.isFinite(strategyNav) ? `$${strategyNav.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '--'}</div>
            <div class='futu-kpi-sub'>Strategy view</div>
          </div>
          <div class='paper-kpi-card ${strategyPnlMood}'>
            <div class='paper-kpi-label'>Sleeve PnL</div>
            <div class='paper-kpi-value ${Number.isFinite(strategyPnlUsd) && strategyPnlUsd < 0 ? 'down' : 'up'}'>${Number.isFinite(strategyPnlUsd) ? `${formatSignedMoney(strategyPnlUsd)} (${formatPct(strategyPnlPct)})` : '--'}</div>
            <div class='futu-kpi-sub'>Strategy mark-to-market</div>
          </div>
          <div class='paper-kpi-card kpi-neutral'>
            <div class='paper-kpi-label'>Cap Limit</div>
            <div class='paper-kpi-value'>${hasCapitalLimit ? `$${capitalLimitUsd.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : 'Unlimited'}</div>
            <div class='futu-kpi-sub'>Base ${Number.isFinite(strategyBaseCapitalUsd) && strategyBaseCapitalUsd > 0 ? `$${strategyBaseCapitalUsd.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '--'}</div>
          </div>
        </div>
      </section>
    </div>
  `;

  if (badge) {
    if (futuStatus?.running && futuStatus?.started_at) {
      const startedMs = new Date(futuStatus.started_at).getTime();
      if (Number.isFinite(startedMs) && startedMs > 0) {
        const elapsed = Math.max(0, Date.now() - startedMs);
        const h = Math.floor(elapsed / 3600000);
        const m = Math.floor((elapsed % 3600000) / 60000);
        badge.textContent = `⏱ ${h > 0 ? h + 'h ' : ''}${m}m`;
        badge.style.display = '';
      } else {
        badge.style.display = 'none';
      }
    } else {
      badge.style.display = 'none';
    }
  }
};

const renderFutuAccountSectionPreview = (futuStatus) => {
  const gatewayCard = document.getElementById('futuAccountPreviewGatewayCard');
  const gatewayValue = document.getElementById('futuAccountPreviewGateway');
  const navCard = document.getElementById('futuAccountPreviewNavCard');
  const navValue = document.getElementById('futuAccountPreviewNav');
  const pnlCard = document.getElementById('futuAccountPreviewPnlCard');
  const pnlValue = document.getElementById('futuAccountPreviewPnl');
  const sleeveCard = document.getElementById('futuAccountPreviewSleeveCard');
  const sleeveValue = document.getElementById('futuAccountPreviewSleeve');
  const sleevePnlCard = document.getElementById('futuAccountPreviewSleevePnlCard');
  const sleevePnlValue = document.getElementById('futuAccountPreviewSleevePnl');
  if (!gatewayCard || !gatewayValue || !navCard || !navValue || !pnlCard || !pnlValue || !sleeveCard || !sleeveValue || !sleevePnlCard || !sleevePnlValue) return;

  const snapshot = futuStatus?.latest_snapshot;
  const strategySnapshot = futuStatus?.latest_strategy_snapshot;
  const totalAssets = Number(snapshot?.total_value);
  const pnlUsd = Number(snapshot?.pnl_usd);
  const pnlPct = Number(snapshot?.pnl_pct);
  const strategyNav = Number(strategySnapshot?.total_value);
  const strategyPnlUsd = Number(strategySnapshot?.pnl_usd);
  const strategyPnlPct = Number(strategySnapshot?.pnl_pct);
  const connected = !!futuStatus?.connected;

  gatewayValue.textContent = connected ? 'Connected' : 'Offline';
  gatewayCard.classList.toggle('is-live', connected);
  gatewayCard.classList.toggle('is-warn', !connected);

  navValue.textContent = Number.isFinite(totalAssets) ? formatCompactCurrency(totalAssets) : '--';

  pnlValue.textContent = Number.isFinite(pnlUsd)
    ? `${formatCompactCurrency(pnlUsd, { signed: true })}${Number.isFinite(pnlPct) ? ` · ${formatPct(pnlPct)}` : ''}`
    : '--';
  pnlCard.classList.toggle('is-positive', Number.isFinite(pnlUsd) && pnlUsd >= 0);
  pnlCard.classList.toggle('is-negative', Number.isFinite(pnlUsd) && pnlUsd < 0);

  sleeveValue.textContent = Number.isFinite(strategyNav) ? formatCompactCurrency(strategyNav) : '--';
  sleevePnlValue.textContent = Number.isFinite(strategyPnlUsd)
    ? `${formatCompactCurrency(strategyPnlUsd, { signed: true })}${Number.isFinite(strategyPnlPct) ? ` · ${formatPct(strategyPnlPct)}` : ''}`
    : '--';
  sleevePnlCard.classList.toggle('is-positive', Number.isFinite(strategyPnlUsd) && strategyPnlUsd >= 0);
  sleevePnlCard.classList.toggle('is-negative', Number.isFinite(strategyPnlUsd) && strategyPnlUsd < 0);
};

const renderPaperRiskAlerts = (paperStatus) => {
  const panel = document.getElementById('paperRiskPanel');
  const list = document.getElementById('paperRiskList');
  if (!panel || !list) return;

  const alerts = [];
  const snapshot = paperStatus?.latest_snapshot;
  if (snapshot) {
    const totalAssets = Number(snapshot.total_value);
    const cashUsd = Number(snapshot.cash_usd);
    const cashPct = Number.isFinite(totalAssets) && totalAssets > 0 && Number.isFinite(cashUsd)
      ? (cashUsd / totalAssets) * 100
      : null;

    if (Number.isFinite(cashPct) && cashPct < 5) {
      alerts.push({ level: 'warn', label: 'Low cash buffer', value: `${cashPct.toFixed(2)}% cash remaining` });
    }

    const holdings = Array.isArray(snapshot.holdings) ? snapshot.holdings : [];
    let maxSinglePos = 0;
    let maxSingleSym = '--';
    for (const holding of holdings) {
      const assetValue = Number(holding?.asset_value);
      if (!Number.isFinite(assetValue) || !Number.isFinite(totalAssets) || totalAssets <= 0) continue;
      const weight = (assetValue / totalAssets) * 100;
      if (weight > maxSinglePos) {
        maxSinglePos = weight;
        maxSingleSym = String(holding?.symbol || '--');
      }
    }
    if (maxSinglePos > 45) {
      alerts.push({ level: 'warn', label: 'Concentration risk', value: `${maxSingleSym} at ${maxSinglePos.toFixed(1)}%` });
    }
  }

  const maxDd = computeMaxDrawdownPct(paperFullContext?.portfolioSeries || []);
  if (Number.isFinite(maxDd) && maxDd <= -8) {
    alerts.push({ level: 'danger', label: 'Drawdown pressure', value: `Peak-to-trough ${maxDd.toFixed(2)}%` });
  }

  const spreadPct = Number(paperFullContext?.latest?.spreadPct);
  if (Number.isFinite(spreadPct) && spreadPct < -5) {
    alerts.push({ level: 'danger', label: 'Benchmark underperformance', value: `${spreadPct.toFixed(2)}% vs QQQ` });
  }

  if (!alerts.length) {
    panel.style.display = 'none';
    list.innerHTML = '';
    return;
  }

  panel.style.display = '';
  list.innerHTML = alerts.map((alert) => `
    <div class='paper-risk-item ${alert.level === 'danger' ? 'danger' : ''}'>
      <strong>${alert.label}</strong>
      <span>${alert.value}</span>
    </div>
  `).join('');
};

const setForecastDeltaText = (el, targetValue, currentValue) => {
  if (!el) return;
  el.classList.remove('legend-up', 'legend-down');

  if (!Number.isFinite(targetValue) || !Number.isFinite(currentValue) || currentValue === 0) {
    el.textContent = '--';
    return;
  }

  const changePct = ((targetValue - currentValue) / currentValue) * 100;
  el.textContent = `${formatPrice(targetValue)} (${formatPct(changePct)})`;
  el.classList.add(changePct >= 0 ? 'legend-up' : 'legend-down');
};

const setForecastLegend = (data) => {
  if (!ensureForecastLegendElements()) return;
  if (!legendForecastCurrentDate || !legendForecastCurrentPrice || !legendForecastTargetDate || !legendForecastP50 || !legendForecastP10 || !legendForecastP90) {
    return;
  }

  const history = Array.isArray(data?.history) ? data.history : [];
  const p50 = Array.isArray(data?.p50) ? data.p50 : [];
  const p10 = Array.isArray(data?.p10) ? data.p10 : [];
  const p90 = Array.isArray(data?.p90) ? data.p90 : [];

  const current = history.length > 0 ? history[history.length - 1] : null;
  const p50Last = p50.length > 0 ? p50[p50.length - 1] : null;
  const p10Last = p10.length > 0 ? p10[p10.length - 1] : null;
  const p90Last = p90.length > 0 ? p90[p90.length - 1] : null;

  const currentPrice = Number(current?.value);

  const p10ByTime = new Map(p10.map((pt) => [Number(pt.time), Number(pt.value)]));
  const p50ByTime = new Map(p50.map((pt) => [Number(pt.time), Number(pt.value)]));
  const p90ByTime = new Map(p90.map((pt) => [Number(pt.time), Number(pt.value)]));
  lastForecastContext = {
    currentTime: Number(current?.time),
    currentPrice,
    p10ByTime,
    p50ByTime,
    p90ByTime,
    defaultTargetTime: Number(p50Last?.time),
  };

  legendForecastCurrentDate.textContent = formatChartDate(Number(current?.time));
  legendForecastCurrentPrice.textContent = formatPrice(currentPrice);
  legendForecastTargetDate.textContent = formatChartDate(Number(p50Last?.time));

  setForecastDeltaText(legendForecastP50, Number(p50Last?.value), currentPrice);
  setForecastDeltaText(legendForecastP10, Number(p10Last?.value), currentPrice);
  setForecastDeltaText(legendForecastP90, Number(p90Last?.value), currentPrice);
};

const setForecastLegendAtTime = (time) => {
  if (!ensureForecastLegendElements()) return;
  if (!lastForecastContext) return;
  const {
    currentTime,
    currentPrice,
    p10ByTime,
    p50ByTime,
    p90ByTime,
    defaultTargetTime,
  } = lastForecastContext;

  const targetTime = p50ByTime.has(time) ? time : defaultTargetTime;
  const p50Value = p50ByTime.get(targetTime);
  const p10Value = p10ByTime.get(targetTime);
  const p90Value = p90ByTime.get(targetTime);

  legendForecastCurrentDate.textContent = formatChartDate(currentTime);
  legendForecastCurrentPrice.textContent = formatPrice(currentPrice);
  legendForecastTargetDate.textContent = formatChartDate(targetTime);

  setForecastDeltaText(legendForecastP50, p50Value, currentPrice);
  setForecastDeltaText(legendForecastP10, p10Value, currentPrice);
  setForecastDeltaText(legendForecastP90, p90Value, currentPrice);
};

const setLegendText = (metrics) => {
  if (!legendPortfolio || !legendPortfolioPnl || !legendBenchmark || !legendBenchmarkPnl || !legendSpread || !legendUpdated) return;

  if (!metrics) {
    legendPortfolio.textContent = '--';
    legendPortfolioPnl.textContent = '--';
    legendBenchmark.textContent = '--';
    legendBenchmarkPnl.textContent = '--';
    legendSpread.textContent = '--';
    legendUpdated.textContent = '--';
    legendPortfolioPnl.classList.remove('legend-up', 'legend-down');
    legendBenchmarkPnl.classList.remove('legend-up', 'legend-down');
    legendSpread.classList.remove('legend-up', 'legend-down');
    return;
  }

  legendPortfolio.textContent = formatMoney(metrics.portfolioValue);
  legendPortfolioPnl.textContent = `${metrics.portfolioPnlUsd >= 0 ? '+' : ''}${formatMoney(metrics.portfolioPnlUsd)} (${metrics.portfolioPnlPct >= 0 ? '+' : ''}${metrics.portfolioPnlPct.toFixed(2)}%)`;
  legendBenchmark.textContent = formatMoney(metrics.benchmarkValue);
  legendBenchmarkPnl.textContent = `${metrics.benchmarkPnlUsd >= 0 ? '+' : ''}${formatMoney(metrics.benchmarkPnlUsd)} (${metrics.benchmarkPnlPct >= 0 ? '+' : ''}${metrics.benchmarkPnlPct.toFixed(2)}%)`;
  legendSpread.textContent = `${metrics.spreadUsd >= 0 ? '+' : ''}${formatMoney(metrics.spreadUsd)} (${metrics.spreadPct >= 0 ? '+' : ''}${metrics.spreadPct.toFixed(2)}%)`;
  legendUpdated.textContent = metrics.updatedText || '--';

  legendPortfolioPnl.classList.remove('legend-up', 'legend-down');
  legendPortfolioPnl.classList.add(metrics.portfolioPnlUsd >= 0 ? 'legend-up' : 'legend-down');
  legendBenchmarkPnl.classList.remove('legend-up', 'legend-down');
  legendBenchmarkPnl.classList.add(metrics.benchmarkPnlUsd >= 0 ? 'legend-up' : 'legend-down');
  legendSpread.classList.remove('legend-up', 'legend-down');
  legendSpread.classList.add(metrics.spreadUsd >= 0 ? 'legend-up' : 'legend-down');
};

const setFutuLegendText = (metrics) => {
  if (!futuLegendPortfolio || !futuLegendPortfolioPnl || !futuLegendBenchmark || !futuLegendBenchmarkPnl || !futuLegendSpread || !futuLegendUpdated) return;
  syncFutuLegendModeLabels();

  if (!metrics) {
    futuLegendPortfolio.textContent = '--';
    futuLegendPortfolioPnl.textContent = '--';
    futuLegendBenchmark.textContent = '--';
    futuLegendBenchmarkPnl.textContent = '--';
    futuLegendSpread.textContent = '--';
    futuLegendUpdated.textContent = '--';
    futuLegendPortfolioPnl.classList.remove('legend-up', 'legend-down');
    futuLegendBenchmarkPnl.classList.remove('legend-up', 'legend-down');
    futuLegendSpread.classList.remove('legend-up', 'legend-down');
    return;
  }

  futuLegendPortfolio.textContent = formatMoney(metrics.portfolioValue);
  futuLegendPortfolioPnl.textContent = `${metrics.portfolioPnlUsd >= 0 ? '+' : ''}${formatMoney(metrics.portfolioPnlUsd)} (${metrics.portfolioPnlPct >= 0 ? '+' : ''}${metrics.portfolioPnlPct.toFixed(2)}%)`;
  futuLegendBenchmark.textContent = formatMoney(metrics.benchmarkValue);
  futuLegendBenchmarkPnl.textContent = `${metrics.benchmarkPnlUsd >= 0 ? '+' : ''}${formatMoney(metrics.benchmarkPnlUsd)} (${metrics.benchmarkPnlPct >= 0 ? '+' : ''}${metrics.benchmarkPnlPct.toFixed(2)}%)`;
  futuLegendSpread.textContent = `${metrics.spreadUsd >= 0 ? '+' : ''}${formatMoney(metrics.spreadUsd)} (${metrics.spreadPct >= 0 ? '+' : ''}${metrics.spreadPct.toFixed(2)}%)`;
  futuLegendUpdated.textContent = metrics.updatedText || '--';

  futuLegendPortfolioPnl.classList.remove('legend-up', 'legend-down');
  futuLegendPortfolioPnl.classList.add(metrics.portfolioPnlUsd >= 0 ? 'legend-up' : 'legend-down');
  futuLegendBenchmarkPnl.classList.remove('legend-up', 'legend-down');
  futuLegendBenchmarkPnl.classList.add(metrics.benchmarkPnlUsd >= 0 ? 'legend-up' : 'legend-down');
  futuLegendSpread.classList.remove('legend-up', 'legend-down');
  futuLegendSpread.classList.add(metrics.spreadUsd >= 0 ? 'legend-up' : 'legend-down');
};

const setBacktestLegendText = (metrics) => {
  if (!backtestLegendPortfolio || !backtestLegendPortfolioPnl || !backtestLegendBenchmark || !backtestLegendBenchmarkPnl || !backtestLegendSpread || !backtestLegendUpdated) return;

  if (!metrics) {
    backtestLegendPortfolio.textContent = '--';
    backtestLegendPortfolioPnl.textContent = '--';
    backtestLegendBenchmark.textContent = '--';
    backtestLegendBenchmarkPnl.textContent = '--';
    backtestLegendSpread.textContent = '--';
    backtestLegendUpdated.textContent = '--';
    backtestLegendPortfolioPnl.classList.remove('legend-up', 'legend-down');
    backtestLegendBenchmarkPnl.classList.remove('legend-up', 'legend-down');
    backtestLegendSpread.classList.remove('legend-up', 'legend-down');
    return;
  }

  backtestLegendPortfolio.textContent = formatMoney(metrics.portfolioValue);
  backtestLegendPortfolioPnl.textContent = `${metrics.portfolioPnlUsd >= 0 ? '+' : ''}${formatMoney(metrics.portfolioPnlUsd)} (${metrics.portfolioPnlPct >= 0 ? '+' : ''}${metrics.portfolioPnlPct.toFixed(2)}%)`;
  backtestLegendBenchmark.textContent = formatMoney(metrics.benchmarkValue);
  backtestLegendBenchmarkPnl.textContent = `${metrics.benchmarkPnlUsd >= 0 ? '+' : ''}${formatMoney(metrics.benchmarkPnlUsd)} (${metrics.benchmarkPnlPct >= 0 ? '+' : ''}${metrics.benchmarkPnlPct.toFixed(2)}%)`;
  backtestLegendSpread.textContent = `${metrics.spreadUsd >= 0 ? '+' : ''}${formatMoney(metrics.spreadUsd)} (${metrics.spreadPct >= 0 ? '+' : ''}${metrics.spreadPct.toFixed(2)}%)`;
  backtestLegendUpdated.textContent = metrics.updatedText || '--';

  backtestLegendPortfolioPnl.classList.remove('legend-up', 'legend-down');
  backtestLegendPortfolioPnl.classList.add(metrics.portfolioPnlUsd >= 0 ? 'legend-up' : 'legend-down');
  backtestLegendBenchmarkPnl.classList.remove('legend-up', 'legend-down');
  backtestLegendBenchmarkPnl.classList.add(metrics.benchmarkPnlUsd >= 0 ? 'legend-up' : 'legend-down');
  backtestLegendSpread.classList.remove('legend-up', 'legend-down');
  backtestLegendSpread.classList.add(metrics.spreadUsd >= 0 ? 'legend-up' : 'legend-down');
};

const showPaperLegend = () => {
  if (!paperLegend) return;
  paperLegend.classList.add('visible');
};

const showFutuLegend = () => {
  if (!futuLegend) return;
  futuLegend.classList.add('visible');
};

const showBacktestLegend = () => {
  if (!backtestLegend) return;
  backtestLegend.classList.add('visible');
};

const hideFutuLegend = () => {
  if (!futuLegend) return;
  futuLegend.classList.remove('visible');
};

const hideBacktestLegend = () => {
  if (!backtestLegend) return;
  backtestLegend.classList.remove('visible');
};

const hidePaperLegend = () => {
  if (!paperLegend) return;
  paperLegend.classList.remove('visible');
};

const syncPaperButtons = (status) => {
  const running = !!status?.running;
  const paused = !!status?.paused;

  if (!paperStartBtn || !paperPauseBtn || !paperResumeBtn || !paperStopBtn) return;

  if (paperStartOptimizing) {
    paperStartBtn.textContent = 'Optimizing...';
    paperStartBtn.disabled = true;
    paperPauseBtn.disabled = true;
    paperResumeBtn.disabled = true;
    paperStopBtn.disabled = true;
    if (paperLoadBtn) paperLoadBtn.disabled = true;
    return;
  }

  if (paperLoadBtn) paperLoadBtn.disabled = false;

  if (running && !paused) {
    paperStartBtn.textContent = 'Running';
    paperStartBtn.disabled = true;
    paperPauseBtn.disabled = false;
    paperResumeBtn.disabled = true;
    paperStopBtn.disabled = false;
    return;
  }

  if (running && paused) {
    paperStartBtn.textContent = 'Paused';
    paperStartBtn.disabled = true;
    paperPauseBtn.disabled = true;
    paperResumeBtn.disabled = false;
    paperStopBtn.disabled = false;
    return;
  }

  paperStartBtn.textContent = 'Start';
  paperStartBtn.disabled = false;
  paperPauseBtn.disabled = true;
  paperResumeBtn.disabled = true;
  paperStopBtn.disabled = true;
};

const setPaperStartOptimizing = (optimizing) => {
  paperStartOptimizing = !!optimizing;
  syncPaperButtons(null);
};

const syncBacktestButtons = (status) => {
  if (!backtestStartBtn || !backtestStopBtn) return;
  const running = !!status?.running;
  const stopping = !!status?.stop_requested;

  if (running && stopping) {
    backtestStartBtn.textContent = 'Running';
    backtestStartBtn.disabled = true;
    backtestStopBtn.textContent = 'Stopping...';
    backtestStopBtn.disabled = true;
    return;
  }

  if (running) {
    backtestStartBtn.textContent = 'Running';
    backtestStartBtn.disabled = true;
    backtestStopBtn.textContent = 'Stop Backtest';
    backtestStopBtn.disabled = false;
    return;
  }

  backtestStartBtn.textContent = '▶ Run Backtest';
  backtestStartBtn.disabled = false;
  backtestStopBtn.textContent = 'Stop Backtest';
  backtestStopBtn.disabled = true;
};

const renderBacktestSummaryMeta = (st) => {
  if (!backtestSummaryMeta) return;
  const summaryStatus = st?.completion_status ? String(st.completion_status).toUpperCase() : null;
  const finishedAt = st?.finished_at || '--';
  const runtimeFile = st?.runtime_file || '--';
  const summaryFile = st?.summary_file || '--';
  if (st?.running) {
    backtestSummaryMeta.textContent = `Status: RUNNING · Runtime ${runtimeFile}`;
    return;
  }
  if (summaryStatus) {
    backtestSummaryMeta.textContent = `Status: ${summaryStatus} · Finished ${finishedAt} · Summary ${summaryFile} · Runtime ${runtimeFile}`;
    return;
  }
  backtestSummaryMeta.textContent = 'No completed backtest summary yet.';
};

const syncFutuButtons = (status) => {
  if (!futuStartBtn || !futuLoadBtn || !futuStopBtn) return;
  const running = !!status?.running;

  if (running) {
    futuStartBtn.textContent = 'Running';
    futuStartBtn.disabled = true;
    futuLoadBtn.disabled = false;
    futuStopBtn.disabled = false;
    return;
  }

  futuStartBtn.textContent = '▶ Start FUTU';
  futuStartBtn.disabled = false;
  futuLoadBtn.disabled = false;
  futuStopBtn.disabled = true;
};

const normalizeFutuAccountRows = (payload) => {
  if (Array.isArray(payload?.data)) return payload.data.filter((row) => row && typeof row === 'object');
  if (Array.isArray(payload?.account_list)) return payload.account_list.filter((row) => row && typeof row === 'object');
  if (Array.isArray(payload)) return payload.filter((row) => row && typeof row === 'object');
  return [];
};

const getFutuAccountId = (row) => {
  const value = row?.acc_id ?? row?.accId ?? '';
  return String(value || '').trim();
};

const buildFutuAccountLabel = (row, fallbackId) => {
  const accId = getFutuAccountId(row) || String(fallbackId || '').trim() || '--';
  const env = String(row?.trd_env ?? '--');
  const marketAuth = Array.isArray(row?.trdmarket_auth)
    ? row.trdmarket_auth.map((v) => String(v)).filter(Boolean).join('/')
    : String(row?.trdmarket_auth ?? row?.trd_market ?? '--');
  return `#${accId} · ${env} · ${marketAuth}`;
};

const getFutuAccountIdSet = () => {
  const set = new Set();
  for (const row of futuAccountListCache || []) {
    const accId = getFutuAccountId(row);
    if (accId) set.add(accId);
  }
  return set;
};

const applyFutuAccountOptions = (rows) => {
  if (!futuAccountSelect) return;
  const currentValue = String(futuAccountSelect.value || '').trim();
  const optionRows = Array.isArray(rows) ? rows : [];
  const seen = new Set();
  const options = ['<option value="">Select FUTU account</option>'];

  for (const row of optionRows) {
    const accId = getFutuAccountId(row);
    if (!accId || seen.has(accId)) continue;
    seen.add(accId);
    options.push(`<option value="${accId}">${buildFutuAccountLabel(row, accId)}</option>`);
  }

  if (seen.size === 0) {
    futuAccountSelect.innerHTML = options.join('');
    futuAccountSelect.value = '';
    return;
  }

  futuAccountSelect.innerHTML = options.join('');
  futuAccountSelect.value = seen.has(currentValue) ? currentValue : '';
};

const refreshFutuAccountList = async ({ force = false } = {}) => {
  if (!futuAccountSelect) return;
  if (futuAccountListInFlight) return;
  const hasCache = Array.isArray(futuAccountListCache) && futuAccountListCache.length > 0;
  if (!force && hasCache) {
    applyFutuAccountOptions(futuAccountListCache);
    return;
  }

  futuAccountListInFlight = true;
  try {
    const payload = await api('/api/futu/account-list');
    futuAccountListCache = normalizeFutuAccountRows(payload);
    applyFutuAccountOptions(futuAccountListCache);
  } catch (e) {
    console.warn(e);
    applyFutuAccountOptions([]);
  } finally {
    futuAccountListInFlight = false;
  }
};

const loadFutuRecentLoads = () => {
  try {
    const raw = localStorage.getItem(FUTU_RECENT_LOADS_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => ({
        runtime_file: String(item?.runtime_file || '').trim(),
        loaded_at: String(item?.loaded_at || '').trim(),
        snapshots: Number(item?.snapshots || 0),
      }))
      .filter((item) => item.runtime_file);
  } catch {
    return [];
  }
};

const saveFutuRecentLoads = () => {
  try {
    localStorage.setItem(FUTU_RECENT_LOADS_KEY, JSON.stringify(futuRecentLoads.slice(0, FUTU_RECENT_LOADS_MAX)));
  } catch {
    // ignore storage errors
  }
};

const renderFutuRecentLoads = () => {
  if (!futuRecentLoadsBox) return;
  if (!Array.isArray(futuRecentLoads) || futuRecentLoads.length === 0) {
    futuRecentLoadsBox.innerHTML = `<div class='futu-recent-load-empty'>No load history yet.</div>`;
    return;
  }

  futuRecentLoadsBox.innerHTML = futuRecentLoads
    .slice(0, FUTU_RECENT_LOADS_MAX)
    .map((item, idx) => {
      const file = String(item.runtime_file || '');
      const base = file.split(/[\\/]/).pop() || file;
      const ts = item.loaded_at ? new Date(item.loaded_at) : null;
      const label = ts && Number.isFinite(ts.getTime()) ? ts.toLocaleString() : '--';
      const snap = Number.isFinite(item.snapshots) && item.snapshots > 0 ? `${item.snapshots} snapshots` : 'snapshots: --';
      return `
        <button class='futu-recent-load-item' data-futu-recent-idx='${idx}' title='${file}'>
          <div class='futu-recent-load-main'>${base}</div>
          <div class='futu-recent-load-sub'>${label} · ${snap}</div>
        </button>
      `;
    })
    .join('');

  futuRecentLoadsBox.querySelectorAll('[data-futu-recent-idx]').forEach((el) => {
    el.addEventListener('click', async () => {
      const idx = Number(el.getAttribute('data-futu-recent-idx'));
      if (!Number.isFinite(idx) || idx < 0 || idx >= futuRecentLoads.length) return;
      const selected = futuRecentLoads[idx];
      if (!selected?.runtime_file) return;
      if (futuLoadPathInput) futuLoadPathInput.value = selected.runtime_file;
      await futuControl('/api/futu/load', { runtime_file: selected.runtime_file });
    });
  });
};

const rememberFutuLoad = (runtimeFile, snapshots, preserveTimestamp = false) => {
  const runtime_file = String(runtimeFile || '').trim();
  if (!runtime_file) return;

  const existing = futuRecentLoads.find((x) => String(x?.runtime_file || '').trim() === runtime_file);
  const nowIso = new Date().toISOString();
  const snapshotCount = Number.isFinite(Number(snapshots)) ? Number(snapshots) : 0;
  const loadedAt = preserveTimestamp && existing?.loaded_at ? existing.loaded_at : nowIso;
  futuRecentLoads = [
    { runtime_file, loaded_at: loadedAt, snapshots: snapshotCount || Number(existing?.snapshots || 0) },
    ...futuRecentLoads.filter((x) => String(x?.runtime_file || '').trim() !== runtime_file),
  ].slice(0, FUTU_RECENT_LOADS_MAX);

  saveFutuRecentLoads();
  renderFutuRecentLoads();
};

const syncPaperApplyButtonState = () => {
  const busy = !!(paperApplyManualOptimizing || paperApplyAutoOptimizing);
  if (!paperTargetApplyBtn) return;
  paperTargetApplyBtn.disabled = busy;
  paperTargetApplyBtn.textContent = busy ? 'Optimizing...' : '⚡ Apply Candidate Universe';
  if (paperApplyNowCheckbox) paperApplyNowCheckbox.disabled = busy;
};

const setPaperApplyOptimizing = (optimizing) => {
  paperApplyManualOptimizing = !!optimizing;
  syncPaperApplyButtonState();
};

const setPaperApplyAutoOptimizing = (optimizing) => {
  paperApplyAutoOptimizing = !!optimizing;
  syncPaperApplyButtonState();
};

/* ─── Forecast: helpers ─── */
const fcEmptyState = document.getElementById('fcEmptyState');
const fcChartWrap = document.getElementById('fcChartWrap');
const fcKpiGrid = document.getElementById('fcKpiGrid');
const fcKpiPanel = document.getElementById('fcKpiPanel');
const fcKpiSymbolTag = document.getElementById('fcKpiSymbolTag');
const fcKpiSignalBadge = document.getElementById('fcKpiSignalBadge');
const fcBatchGrid = document.getElementById('fcBatchGrid');
const fcChartHeader = document.getElementById('fcChartHeader');
const fcChartSubtitle = document.getElementById('fcChartSubtitle');

const showFcChart = (symbol) => {
  if (fcEmptyState) fcEmptyState.style.display = 'none';
  if (fcChartWrap) fcChartWrap.style.display = '';
  if (fcChartHeader) fcChartHeader.style.display = '';
  if (fcChartSubtitle && symbol) {
    const hEl = document.getElementById('fHorizon');
    const h = hEl ? hEl.value : '5';
    fcChartSubtitle.textContent = symbol + ' · ' + h + '-day horizon · Monte Carlo';
  }
};
const showFcEmpty = () => {
  if (fcEmptyState) fcEmptyState.style.display = '';
  if (fcChartWrap) fcChartWrap.style.display = 'none';
  if (fcChartHeader) fcChartHeader.style.display = 'none';
  if (fcKpiPanel) { fcKpiPanel.style.display = 'none'; }
  if (fcKpiGrid) { fcKpiGrid.innerHTML = ''; }
  if (fcBatchGrid) { fcBatchGrid.style.display = 'none'; fcBatchGrid.innerHTML = ''; }
};

const computeSignal = (currentPrice, p50Target) => {
  if (!Number.isFinite(currentPrice) || !Number.isFinite(p50Target) || currentPrice === 0) return 'neutral';
  const pct = ((p50Target - currentPrice) / currentPrice) * 100;
  if (pct > 2) return 'bull';
  if (pct < -2) return 'bear';
  return 'neutral';
};

const signalLabel = (sig) => sig === 'bull' ? '▲ Bullish' : sig === 'bear' ? '▼ Bearish' : '● Neutral';

const renderFcKpiCards = (data) => {
  if (!fcKpiGrid) return;
  const history = Array.isArray(data?.history) ? data.history : [];
  const p50 = Array.isArray(data?.p50) ? data.p50 : [];
  const p10 = Array.isArray(data?.p10) ? data.p10 : [];
  const p90 = Array.isArray(data?.p90) ? data.p90 : [];
  const current = history.length > 0 ? history[history.length - 1] : null;
  const p50Last = p50.length > 0 ? p50[p50.length - 1] : null;
  const p10Last = p10.length > 0 ? p10[p10.length - 1] : null;
  const p90Last = p90.length > 0 ? p90[p90.length - 1] : null;
  const curP = Number(current?.value);
  const p50V = Number(p50Last?.value);
  const p10V = Number(p10Last?.value);
  const p90V = Number(p90Last?.value);

  const pctDelta = (target) => {
    if (!Number.isFinite(target) || !Number.isFinite(curP) || curP === 0) return '--';
    const d = ((target - curP) / curP) * 100;
    return `${d >= 0 ? '+' : ''}${d.toFixed(2)}%`;
  };

  const signal = computeSignal(curP, p50V);
  const rangeSpread = (Number.isFinite(p90V) && Number.isFinite(p10V))
    ? `$${(p90V - p10V).toFixed(2)}`
    : '--';

  // Update panel header
  if (fcKpiPanel) fcKpiPanel.style.display = '';
  if (fcKpiSymbolTag) fcKpiSymbolTag.textContent = data.symbol || '--';
  if (fcKpiSignalBadge) {
    fcKpiSignalBadge.className = `fc-kpi-panel-signal ${signal}`;
    fcKpiSignalBadge.textContent = signalLabel(signal);
  }

  fcKpiGrid.innerHTML = `
    <div class="fc-kpi-card kpi-neutral">
      <div class="fc-kpi-label">Current Price</div>
      <div class="fc-kpi-value">${formatPrice(curP)}</div>
      <div class="fc-kpi-sub" style="color:var(--muted)">${data.symbol || '--'}</div>
    </div>
    <div class="fc-kpi-card ${p50V >= curP ? 'kpi-bull' : 'kpi-bear'}">
      <div class="fc-kpi-label">P50 Target (Median)</div>
      <div class="fc-kpi-value" style="color:${p50V >= curP ? 'var(--up)' : 'var(--down)'}">${formatPrice(p50V)}</div>
      <div class="fc-kpi-sub" style="color:${p50V >= curP ? 'var(--up)' : 'var(--down)'}">${pctDelta(p50V)}</div>
    </div>
    <div class="fc-kpi-card kpi-bear">
      <div class="fc-kpi-label">P10 Bear Case</div>
      <div class="fc-kpi-value" style="color:var(--down)">${formatPrice(p10V)}</div>
      <div class="fc-kpi-sub" style="color:var(--down)">${pctDelta(p10V)}</div>
    </div>
    <div class="fc-kpi-card kpi-bull">
      <div class="fc-kpi-label">P90 Bull Case</div>
      <div class="fc-kpi-value" style="color:var(--up)">${formatPrice(p90V)}</div>
      <div class="fc-kpi-sub" style="color:var(--up)">${pctDelta(p90V)}</div>
    </div>
    <div class="fc-kpi-card kpi-neutral">
      <div class="fc-kpi-label">Expected Range</div>
      <div class="fc-kpi-value">${rangeSpread}</div>
      <div class="fc-kpi-sub" style="color:var(--muted)">P10 – P90 spread</div>
    </div>
    <div class="fc-kpi-card kpi-neutral">
      <div class="fc-kpi-label">Horizon</div>
      <div class="fc-kpi-value" style="font-size:16px">${document.getElementById('fHorizon')?.value || '10'}d</div>
      <div class="fc-kpi-sub" style="color:var(--muted)">Trading days</div>
    </div>
  `;
};

const applyForecastDataToChart = (data) => {
  historySeries.setData(toSeries(data.history || []));
  p10Series.setData(toSeries(data.p10 || []));
  p50Series.setData(toSeries(data.p50 || []));
  p90Series.setData(toSeries(data.p90 || []));
  // Confidence band: P30-P70
  if (data.p70 && data.p70.length > 0) {
    p70AreaSeries.setData(toSeries(data.p70));
  } else {
    p70AreaSeries.setData([]);
  }
  if (data.p30 && data.p30.length > 0) {
    p30AreaSeries.setData(toSeries(data.p30));
  } else {
    p30AreaSeries.setData([]);
  }
  setForecastLegend(data);
  showFcChart(data.symbol || forecastSelectedSymbol || '');
  fChart.fit();
};

const renderBatchGrid = () => {
  if (!fcBatchGrid) return;
  if (forecastBatchResults.size <= 1) {
    fcBatchGrid.style.display = 'none';
    fcBatchGrid.innerHTML = '';
    return;
  }
  fcBatchGrid.style.display = '';
  let html = '';
  for (const [sym, data] of forecastBatchResults) {
    const history = data.history || [];
    const p50 = data.p50 || [];
    const p10 = data.p10 || [];
    const p90 = data.p90 || [];
    const curP = history.length > 0 ? Number(history[history.length - 1]?.value) : NaN;
    const p50V = p50.length > 0 ? Number(p50[p50.length - 1]?.value) : NaN;
    const p10V = p10.length > 0 ? Number(p10[p10.length - 1]?.value) : NaN;
    const p90V = p90.length > 0 ? Number(p90[p90.length - 1]?.value) : NaN;
    const signal = computeSignal(curP, p50V);
    const pctDelta = Number.isFinite(p50V) && Number.isFinite(curP) && curP !== 0
      ? ((p50V - curP) / curP * 100)
      : 0;
    const pctStr = Number.isFinite(pctDelta)
      ? `${pctDelta >= 0 ? '+' : ''}${pctDelta.toFixed(2)}%`
      : '--';
    const selected = forecastSelectedSymbol === sym;
    const forecastedAt = data?.forecasted_at || null;

    // Progress bar: normalize p50 delta between p10 and p90 range
    let progressPct = 50;
    if (Number.isFinite(p10V) && Number.isFinite(p90V) && Number.isFinite(curP) && (p90V - p10V) > 0) {
      progressPct = Math.max(0, Math.min(100, ((curP - p10V) / (p90V - p10V)) * 100));
    }
    const progressColor = signal === 'bull' ? 'var(--up)' : signal === 'bear' ? 'var(--down)' : 'var(--accent)';

    html += `
      <div class="fc-batch-card ${signal === 'bull' ? 'card-bull' : signal === 'bear' ? 'card-bear' : ''} ${selected ? 'selected' : ''}" data-batch-sym="${sym}">
        <div class="fc-batch-card-header">
          <span class="fc-batch-symbol">${sym}</span>
          <span class="fc-batch-signal ${signal}">${signalLabel(signal)}</span>
        </div>
        <div class="fc-batch-row"><span>Price</span><span class="val">${formatPrice(curP)}</span></div>
        <div class="fc-batch-row"><span>P50 Target</span><span class="val" style="color:${pctDelta >= 0 ? 'var(--up)' : 'var(--down)'}">${formatPrice(p50V)} (${pctStr})</span></div>
        <div class="fc-batch-row"><span>Bear / Bull</span><span class="val">${formatPrice(p10V)} / ${formatPrice(p90V)}</span></div>
        <div class="fc-batch-row"><span>Forecasted</span><span class="val">${formatDateTime(forecastedAt)}</span></div>
        <div class="fc-batch-progress"><div class="fc-batch-progress-fill" style="width:${progressPct}%;background:${progressColor}"></div></div>
      </div>
    `;
  }
  fcBatchGrid.innerHTML = html;

  // Click handler for selecting symbol
  fcBatchGrid.querySelectorAll('.fc-batch-card').forEach(card => {
    card.addEventListener('click', () => {
      const sym = card.dataset.batchSym;
      if (!sym || !forecastBatchResults.has(sym)) return;
      forecastSelectedSymbol = sym;
      const data = forecastBatchResults.get(sym);
      applyForecastDataToChart(data);
      renderFcKpiCards(data);
      saveForecastBatchCache();
      renderBatchGrid(); // re-render to update selected highlight
    });
  });
};

const syncQuickChips = () => {
  const input = document.getElementById('fSymbol');
  if (!input) return;
  const syms = input.value.toUpperCase().split(',').map(s => s.trim()).filter(Boolean);
  document.querySelectorAll('.fc-quick-chip').forEach(chip => {
    chip.classList.toggle('active', syms.includes(chip.dataset.sym));
  });
};

// Quick chip click handlers
document.querySelectorAll('.fc-quick-chip').forEach(chip => {
  chip.addEventListener('click', () => {
    const sym = chip.dataset.sym;
    const input = document.getElementById('fSymbol');
    if (!input) return;
    const current = input.value.toUpperCase().split(',').map(s => s.trim()).filter(Boolean);
    const idx = current.indexOf(sym);
    if (idx >= 0) {
      current.splice(idx, 1);
    } else {
      current.push(sym);
    }
    input.value = current.join(',');
    syncQuickChips();
  });
});

// Sync chips on input change
document.getElementById('fSymbol')?.addEventListener('input', syncQuickChips);
syncQuickChips();

const runForecastBatchForSymbols = async (symbols, horizon, simulations) => {
  forecastBatchResults.clear();
  const rows = await api('/api/forecast/batch', {
    method: 'POST',
    body: JSON.stringify({ symbols, horizon, simulations }),
  });

  let firstGoodSymbol = null;
  for (const row of (rows || [])) {
    if (!row || !row.symbol) continue;
    const sym = String(row.symbol).toUpperCase();
    forecastBatchResults.set(sym, row);
    if (!firstGoodSymbol) firstGoodSymbol = sym;
  }

  if (forecastBatchResults.size === 0) {
    throw new Error('Batch forecast returned no results');
  }

  document.getElementById('fSymbol').value = symbols.join(',');
  document.getElementById('fHorizon').value = horizon;
  document.getElementById('fSims').value = simulations;
  syncQuickChips();

  if (!(forecastSelectedSymbol && forecastBatchResults.has(forecastSelectedSymbol))) {
    forecastSelectedSymbol = firstGoodSymbol;
  }
  const selectedData = forecastBatchResults.get(forecastSelectedSymbol);
  if (selectedData) {
    applyForecastDataToChart(selectedData);
    renderFcKpiCards(selectedData);
  }
  renderBatchGrid();
  saveForecastBatchCache();
  refreshBackendChip();

  return { errors: [] };
};

document.getElementById('runForecast').addEventListener('click', async () => {
  try {
    await withBusy('runForecast', 'Running...', 'Forecast updated', async () => {
      const rawInput = document.getElementById('fSymbol').value.trim();
      const symbols = [...new Set(rawInput.toUpperCase().split(',').map(s => s.trim()).filter(Boolean))];
      if (symbols.length === 0) throw new Error('Please enter at least one symbol');

      const horizon = Number(document.getElementById('fHorizon').value);
      const simulations = Number(document.getElementById('fSims').value);
      const { errors } = await runForecastBatchForSymbols(symbols, horizon, simulations);

      if (errors.length > 0) {
        setStatus(`Forecast done (${errors.length} error${errors.length > 1 ? 's' : ''}): ${errors.join('; ')}`, 'err');
      }
    });
  } catch (e) { alert(e.message); }
});

const fillAssetTable = (alloc, paperStatus) => {
  const MODEL_PRICE_HORIZON_DAYS = 10;
  const paperMap = new Map((paperStatus?.latest_snapshot?.symbols || []).map(x => [x.symbol, x.price]));
  const weightMap = new Map((alloc.weights || []).map(([s, w]) => [s, w]));
  const tb = document.querySelector('#assetTable tbody');
  tb.innerHTML = '';
  for (const f of alloc.asset_forecasts) {
    const modelPrice = Number.isFinite(f.expected_return)
      ? f.current_price * Math.exp(f.expected_return * MODEL_PRICE_HORIZON_DAYS)
      : f.current_price;
    const current = getLatestQuotePrice(f.symbol);
    const dev = current == null ? null : (current - modelPrice);
    const devPct = current == null ? null : (dev / modelPrice * 100);
    const optWeight = weightMap.get(f.symbol);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${f.symbol}</td>
      <td class='num'>$${modelPrice.toFixed(2)}</td>
      <td class='num'>${current == null ? '--' : '$' + current.toFixed(2)}</td>
      <td class='num ${dev == null ? '' : (dev >= 0 ? 'up' : 'down')}'>${dev == null ? '--' : `${dev >=0 ? '+' : ''}${dev.toFixed(2)} (${devPct >=0 ? '+' : ''}${devPct.toFixed(2)}%)`}</td>
      <td class='num ${f.annual_return >=0 ? 'up' : 'down'}'>${(f.annual_return*100).toFixed(1)}%</td>
      <td class='num'>${(f.annual_vol*100).toFixed(1)}%</td>
      <td class='num ${f.sharpe >= 0 ? 'up':'down'}'>${f.sharpe.toFixed(2)}</td>
      <td class='num'>${optWeight == null ? '--' : `${(optWeight * 100).toFixed(1)}%`}</td>
      <td class='num ${f.p50_price >= f.current_price ? 'up':'down'}'>$${f.p50_price.toFixed(2)}</td>`;
    tb.appendChild(tr);
  }

  // Portfolio stat cards
  const statsPanel = document.getElementById('portfolioStatsPanel');
  if (statsPanel) {
    const retClass = alloc.expected_annual_return >= 0 ? 'up' : 'down';
    const sharpeClass = alloc.sharpe_ratio >= 0 ? 'up' : 'down';
    statsPanel.style.display = '';
    statsPanel.innerHTML = `
      <div class='stat-card'>
        <div class='stat-card-label'>Expected Return</div>
        <div class='stat-card-value ${retClass}'>${(alloc.expected_annual_return * 100).toFixed(1)}%</div>
        <div class='stat-card-sub'>Annualized</div>
      </div>
      <div class='stat-card'>
        <div class='stat-card-label'>Volatility</div>
        <div class='stat-card-value'>${(alloc.expected_annual_vol * 100).toFixed(1)}%</div>
        <div class='stat-card-sub'>Annualized</div>
      </div>
      <div class='stat-card'>
        <div class='stat-card-label'>Sharpe Ratio</div>
        <div class='stat-card-value ${sharpeClass}'>${alloc.sharpe_ratio.toFixed(2)}</div>
        <div class='stat-card-sub'>Risk-adjusted</div>
      </div>
      <div class='stat-card'>
        <div class='stat-card-label'>CVaR 95%</div>
        <div class='stat-card-value down'>${(alloc.cvar_95 * 100).toFixed(2)}%</div>
        <div class='stat-card-sub'>Tail risk</div>
      </div>
      <div class='stat-card'>
        <div class='stat-card-label'>Leverage</div>
        <div class='stat-card-value'>${alloc.leverage.toFixed(2)}x</div>
        <div class='stat-card-sub'>Vol-target</div>
      </div>
    `;
  }

  renderPortfolioOverlay(alloc);

  // Weight allocation bar
  const barWrap = document.getElementById('portfolioWeightBar');
  const cashWeight = clampUnit(Math.max(Number(alloc.target_cash_weight || 0), 1 - (alloc.weights || []).reduce((sum, [, w]) => sum + Number(w || 0), 0)));
  if (barWrap && ((alloc.weights && alloc.weights.length > 0) || cashWeight > 0.0001)) {
    barWrap.style.display = '';
    const sorted = [...alloc.weights].sort((a, b) => b[1] - a[1]);
    let segments = '';
    let legendItems = '';
    sorted.forEach(([sym, w], i) => {
      const color = PORTFOLIO_WEIGHT_COLORS[i % PORTFOLIO_WEIGHT_COLORS.length];
      const pct = (w * 100).toFixed(1);
      segments += `<div class='weight-bar-seg' style='flex-basis:${pct}%;background:${color};' title='${sym}: ${pct}%'>${w > 0.06 ? sym : ''}</div>`;
      legendItems += `<span class='weight-bar-legend-item'><span class='weight-bar-legend-dot' style='background:${color}'></span>${sym} ${pct}%</span>`;
    });
    if (cashWeight > 0.0001) {
      const cashPct = (cashWeight * 100).toFixed(1);
      segments += `<div class='weight-bar-seg' style='flex-basis:${cashPct}%;background:#6b7a99;color:var(--text-2);text-shadow:none;' title='Cash: ${cashPct}%'>${cashWeight > 0.08 ? 'CASH' : ''}</div>`;
      legendItems += `<span class='weight-bar-legend-item'><span class='weight-bar-legend-dot' style='background:#6b7a99'></span>Cash ${cashPct}%</span>`;
    }
    barWrap.innerHTML = `
      <div class='weight-bar-label'>Target Allocation Mix</div>
      <div class='weight-bar'>${segments}</div>
      <div class='weight-bar-legend'>${legendItems}</div>
    `;
  } else if (barWrap) {
    barWrap.style.display = 'none';
  }
};

const fillHoldingsTable = (paperStatus) => {
  const tb = document.querySelector('#holdingsTable tbody');
  if (!tb) return;
  const holdingsPoolBadge = document.getElementById('holdingsPoolBadge');

  tb.innerHTML = '';

  const snapshot = paperStatus?.latest_snapshot;
  const holdingsSet = new Set(snapshot?.holdings_symbols || []);
  const snapshotMap = new Map((snapshot?.symbols || []).map(x => [x.symbol, x]));
  const holdingsMap = new Map((snapshot?.holdings || []).map(x => [x.symbol, x]));
  const targetMap = new Map();
  for (const t of (paperStatus?.target_weights || [])) {
    const symbol = String(t?.symbol || '').toUpperCase();
    const weight = Number(t?.weight);
    if (!symbol || !Number.isFinite(weight)) continue;
    targetMap.set(symbol, weight);
  }

  if (holdingsPoolBadge) {
    const candidateSymbols = Array.from(new Set((paperStatus?.candidate_symbols || [])
      .map((s) => String(s || '').toUpperCase())
      .filter(Boolean)));
    const holdingCount = holdingsSet.size;
    const poolCount = candidateSymbols.length;
    const inPoolHeldCount = candidateSymbols.filter(symbol => holdingsSet.has(symbol)).length;
    const notHeldCount = Math.max(0, poolCount - inPoolHeldCount);
    holdingsPoolBadge.textContent = `Holding ${holdingCount} · In Pool ${poolCount} · Not Held ${notHeldCount}`;
  }

  const orderedSymbols = Array.from(holdingsSet).sort();
  if (orderedSymbols.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='8'><div class='empty-state'><div class='empty-state-icon'>📊</div>No holdings data yet.<br>Run portfolio optimization and start paper trading.</div></td>`;
    tb.appendChild(tr);
    return;
  }

  for (const sym of orderedSymbols) {
    const row = snapshotMap.get(sym);
    const holding = holdingsMap.get(sym);
    const targetWeight = targetMap.get(sym);
    const currentPrice = getLatestQuotePrice(sym);
    const quantity = holding?.quantity ?? (holdingsSet.has(sym) ? 0 : null);
    const assetValue = holding?.asset_value ?? (quantity != null && currentPrice != null ? quantity * currentPrice : null);
    const snapshotAvgCost = holding?.avg_cost;
    const basis = paperCostBasis.get(sym);
    const avgCostFromTrades = basis && Number.isFinite(basis.avgCost) && basis.avgCost > 0 ? basis.avgCost : null;
    const avgCost = Number.isFinite(snapshotAvgCost) && snapshotAvgCost > 0
      ? snapshotAvgCost
      : avgCostFromTrades;

    let priceCell = '--';
    if (currentPrice != null) {
      let indicator = '';
      if (avgCost != null) {
        const diff = currentPrice - avgCost;
        const cls = diff > 0 ? 'up' : (diff < 0 ? 'down' : 'flat');
        const arrow = diff > 0 ? '&#9650;' : (diff < 0 ? '&#9660;' : '&#9679;');
        indicator = ` <span class='price-indicator ${cls}' title='Avg buy: $${avgCost.toFixed(2)}'>${arrow}</span>`;
      }
      priceCell = `$${currentPrice.toFixed(2)}${indicator}`;
    }

    let unrealizedText = '--';
    let unrealizedClass = '';
    if (quantity != null && currentPrice != null && avgCost != null && quantity > 0) {
      const unrealizedUsd = (currentPrice - avgCost) * quantity;
      const unrealizedPct = avgCost !== 0 ? ((currentPrice - avgCost) / avgCost) * 100 : 0;
      unrealizedText = `${formatSignedMoney(unrealizedUsd)} (${unrealizedPct >= 0 ? '+' : ''}${unrealizedPct.toFixed(2)}%)`;
      unrealizedClass = unrealizedUsd >= 0 ? 'up' : 'down';
    }

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${sym}</td>
      <td class='up'>Holding</td>
      <td class='num'>${quantity == null ? '--' : quantity.toFixed(2)}</td>
      <td class='num'>${priceCell}</td>
      <td class='num'>${avgCost == null ? '--' : '$' + avgCost.toFixed(2)}</td>
      <td class='num'>${assetValue == null ? '--' : '$' + assetValue.toFixed(2)}</td>
      <td class='num'>${targetWeight == null ? '--' : `${(targetWeight * 100).toFixed(2)}%`}</td>
      <td class='num ${unrealizedClass}'>${(() => {
        if (unrealizedText === '--') return '--';
        const uPct = avgCost !== 0 && quantity > 0 ? Math.abs(((currentPrice - avgCost) / avgCost) * 100) : 0;
        const barW = Math.min(uPct * 2, 100);
        const barCls = unrealizedClass === 'up' ? 'bar-up' : 'bar-down';
        return `<div class='pnl-cell-wrap'><span>${unrealizedText}</span><div class='pnl-mini-bar ${barCls}' style='width:${barW}%'></div></div>`;
      })()}</td>
    `;
    tb.appendChild(tr);
  }

};

const fillCapitalSummaryTable = (paperStatus) => {
  const tb = document.querySelector('#capitalSummaryTable tbody');
  if (!tb) return;

  tb.innerHTML = '';

  const snapshot = paperStatus?.latest_snapshot;
  if (!snapshot) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='8'><div class='empty-state'><div class='empty-state-icon'>💰</div>No capital snapshot yet.<br>Start or load paper trading first.</div></td>`;
    tb.appendChild(tr);
    return;
  }

  const totalAssets = Number(snapshot?.total_value);
  const cashUsd = Number(snapshot?.cash_usd);
  const totalPnlUsd = Number(snapshot?.pnl_usd);
  const totalPnlPct = Number(snapshot?.pnl_pct);
  const hasHoldings = (snapshot?.holdings_symbols?.length || 0) > 0;

  const safeTotal = Number.isFinite(totalAssets) && totalAssets > 0 ? totalAssets : 0;
  const safeCash = Number.isFinite(cashUsd) ? cashUsd : 0;
  const investedValue = safeTotal - safeCash;
  const cashWeightPct = safeTotal > 0 ? (safeCash / safeTotal) * 100 : 0;
  const investedWeightPct = safeTotal > 0 ? (investedValue / safeTotal) * 100 : 0;

  const cashTr = document.createElement('tr');
  cashTr.innerHTML = `
    <td><strong>ACCOUNT CASH</strong></td>
    <td>Account</td>
    <td class='num'>${safeTotal > 0 ? `${cashWeightPct.toFixed(2)}%` : '--'}</td>
    <td class='num'>${Number.isFinite(cashUsd) ? '$' + cashUsd.toFixed(2) : '--'}</td>
    <td class='num'>${safeTotal > 0 ? `$${investedValue.toFixed(2)} (${investedWeightPct.toFixed(2)}%)` : '--'}</td>
    <td class='num'>${Number.isFinite(totalAssets) ? '$' + totalAssets.toFixed(2) : '--'}</td>
    <td class='num'>--</td>
    <td class='num'>--</td>
  `;
  tb.appendChild(cashTr);

  const totalPnlClass = Number.isFinite(totalPnlUsd)
    ? (totalPnlUsd >= 0 ? 'up' : 'down')
    : '';
  const returnBgClass = Number.isFinite(totalPnlUsd)
    ? (totalPnlUsd >= 0 ? 'pnl-bg-up' : 'pnl-bg-down')
    : '';
  const returnText = Number.isFinite(totalPnlPct)
    ? `${totalPnlPct >= 0 ? '+' : ''}${totalPnlPct.toFixed(2)}%`
    : '--';
  const pnlText = Number.isFinite(totalPnlUsd) && Number.isFinite(totalPnlPct)
    ? (!hasHoldings
      ? returnText
      : `${formatSignedMoney(totalPnlUsd)} (${returnText})`)
    : '--';
  const pnlTitle = !hasHoldings
    ? 'Realized return vs initial capital (all positions closed).'
    : 'Portfolio PnL vs initial capital (includes open positions).';

  const totalTr = document.createElement('tr');
  totalTr.innerHTML = `
    <td><strong>ACCOUNT NAV</strong></td>
    <td>Account</td>
    <td class='num'>${safeTotal > 0 ? `${cashWeightPct.toFixed(2)}% cash` : '--'}</td>
    <td class='num'>${Number.isFinite(cashUsd) ? '$' + cashUsd.toFixed(2) : '--'}</td>
    <td class='num'>${safeTotal > 0 ? `$${investedValue.toFixed(2)} (${investedWeightPct.toFixed(2)}%)` : '--'}</td>
    <td class='num'>${Number.isFinite(totalAssets) ? '$' + totalAssets.toFixed(2) : '--'}</td>
    <td class='num ${totalPnlClass} ${returnBgClass}'>${returnText}</td>
    <td class='num ${totalPnlClass} ${returnBgClass}' title='${pnlTitle}'>${pnlText}</td>
  `;
  tb.appendChild(totalTr);
};

const fillFutuCapitalSummaryTable = (futuStatus) => {
  const tb = document.querySelector('#futuCapitalSummaryTable tbody');
  if (!tb) return;

  tb.innerHTML = '';
  const snapshot = futuStatus?.latest_snapshot;
  if (!snapshot) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='8'><div class='empty-state'><div class='empty-state-icon'>💰</div>No FUTU capital snapshot yet.<br>Start or load futu simulation first.</div></td>`;
    tb.appendChild(tr);
    return;
  }

  const totalAssets = Number(snapshot?.total_value);
  const cashUsd = Number(snapshot?.cash_usd);
  const totalPnlUsd = Number(snapshot?.pnl_usd);
  const totalPnlPct = Number(snapshot?.pnl_pct);
  const hasHoldings = (snapshot?.holdings_symbols?.length || 0) > 0;

  const safeTotal = Number.isFinite(totalAssets) && totalAssets > 0 ? totalAssets : 0;
  const safeCash = Number.isFinite(cashUsd) ? cashUsd : 0;
  const investedValue = safeTotal - safeCash;
  const cashWeightPct = safeTotal > 0 ? (safeCash / safeTotal) * 100 : 0;
  const investedWeightPct = safeTotal > 0 ? (investedValue / safeTotal) * 100 : 0;

  const cashTr = document.createElement('tr');
  cashTr.innerHTML = `
    <td><strong>ACCOUNT CASH</strong></td>
    <td>Account</td>
    <td class='num'>${safeTotal > 0 ? `${cashWeightPct.toFixed(2)}%` : '--'}</td>
    <td class='num'>${Number.isFinite(cashUsd) ? '$' + cashUsd.toFixed(2) : '--'}</td>
    <td class='num'>${safeTotal > 0 ? `$${investedValue.toFixed(2)} (${investedWeightPct.toFixed(2)}%)` : '--'}</td>
    <td class='num'>${Number.isFinite(totalAssets) ? '$' + totalAssets.toFixed(2) : '--'}</td>
    <td class='num'>--</td>
    <td class='num'>--</td>
  `;
  tb.appendChild(cashTr);

  const totalPnlClass = Number.isFinite(totalPnlUsd)
    ? (totalPnlUsd >= 0 ? 'up' : 'down')
    : '';
  const returnBgClass = Number.isFinite(totalPnlUsd)
    ? (totalPnlUsd >= 0 ? 'pnl-bg-up' : 'pnl-bg-down')
    : '';
  const returnText = Number.isFinite(totalPnlPct)
    ? `${totalPnlPct >= 0 ? '+' : ''}${totalPnlPct.toFixed(2)}%`
    : '--';
  const pnlText = Number.isFinite(totalPnlUsd) && Number.isFinite(totalPnlPct)
    ? (!hasHoldings
      ? returnText
      : `${formatSignedMoney(totalPnlUsd)} (${returnText})`)
    : '--';

  const totalTr = document.createElement('tr');
  totalTr.innerHTML = `
    <td><strong>ACCOUNT NAV</strong></td>
    <td>Account</td>
    <td class='num'>${safeTotal > 0 ? `${cashWeightPct.toFixed(2)}% cash` : '--'}</td>
    <td class='num'>${Number.isFinite(cashUsd) ? '$' + cashUsd.toFixed(2) : '--'}</td>
    <td class='num'>${safeTotal > 0 ? `$${investedValue.toFixed(2)} (${investedWeightPct.toFixed(2)}%)` : '--'}</td>
    <td class='num'>${Number.isFinite(totalAssets) ? '$' + totalAssets.toFixed(2) : '--'}</td>
    <td class='num ${totalPnlClass} ${returnBgClass}'>${returnText}</td>
    <td class='num ${totalPnlClass} ${returnBgClass}'>${pnlText}</td>
  `;
  tb.appendChild(totalTr);

  const strategySnapshot = futuStatus?.latest_strategy_snapshot;
  const strategyTotal = Number(strategySnapshot?.total_value);
  const strategyCash = Number(strategySnapshot?.cash_usd);
  const strategyInvested = Number(strategySnapshot?.invested_value_usd);
  const strategyCashWeightPct = Number(strategySnapshot?.cash_weight_pct);
  const strategyPnlUsd = Number(strategySnapshot?.pnl_usd);
  const strategyPnlPct = Number(strategySnapshot?.pnl_pct);
  const strategyReturnText = Number.isFinite(strategyPnlPct)
    ? `${strategyPnlPct >= 0 ? '+' : ''}${strategyPnlPct.toFixed(2)}%`
    : '--';
  const strategyPnlText = Number.isFinite(strategyPnlUsd) && Number.isFinite(strategyPnlPct)
    ? `${formatSignedMoney(strategyPnlUsd)} (${strategyReturnText})`
    : '--';
  const strategyPnlClass = Number.isFinite(strategyPnlUsd)
    ? (strategyPnlUsd >= 0 ? 'up' : 'down')
    : '';
  const strategyBgClass = Number.isFinite(strategyPnlUsd)
    ? (strategyPnlUsd >= 0 ? 'pnl-bg-up' : 'pnl-bg-down')
    : '';

  const strategyTr = document.createElement('tr');
  strategyTr.innerHTML = `
    <td><strong>STRATEGY SLEEVE FT</strong></td>
    <td>Pool Snapshot</td>
    <td class='num'>${Number.isFinite(strategyCashWeightPct) ? `${strategyCashWeightPct.toFixed(2)}%` : '--'}</td>
    <td class='num'>${Number.isFinite(strategyCash) ? '$' + strategyCash.toFixed(2) : '--'}</td>
    <td class='num'>${Number.isFinite(strategyInvested) && Number.isFinite(strategyTotal) && strategyTotal > 0
      ? `$${strategyInvested.toFixed(2)} (${((strategyInvested / strategyTotal) * 100).toFixed(2)}%)`
      : (Number.isFinite(strategyInvested) ? '$' + strategyInvested.toFixed(2) : '--')}</td>
    <td class='num'>${Number.isFinite(strategyTotal) ? '$' + strategyTotal.toFixed(2) : '--'}</td>
    <td class='num ${strategyPnlClass} ${strategyBgClass}'>${strategyReturnText}</td>
    <td class='num ${strategyPnlClass} ${strategyBgClass}'>${strategyPnlText}</td>
  `;
  tb.appendChild(strategyTr);

  const rtMetrics = computeFutuStrategyRealtimeMetrics(futuStatus);
  const rtInvested = Number(rtMetrics.investedValue);
  const rtPnlUsd = Number(rtMetrics.pnlUsd);
  const rtTotal = Number(rtMetrics.totalValue);
  const rtCash = Number(rtMetrics.cashUsd);
  const rtCashWeightPct = Number(rtMetrics.cashWeightPct);
  const rtPnlPct = Number(rtMetrics.pnlPct);
  const rtReturnText = Number.isFinite(rtPnlPct)
    ? `${rtPnlPct >= 0 ? '+' : ''}${rtPnlPct.toFixed(2)}%`
    : '--';
  const rtPnlText = Number.isFinite(rtPnlUsd)
    ? `${formatSignedMoney(rtPnlUsd)} (${rtReturnText})`
    : '--';
  const rtPnlClass = Number.isFinite(rtPnlUsd)
    ? (rtPnlUsd >= 0 ? 'up' : 'down')
    : '';
  const rtBgClass = Number.isFinite(rtPnlUsd)
    ? (rtPnlUsd >= 0 ? 'pnl-bg-up' : 'pnl-bg-down')
    : '';

  const strategyRtTr = document.createElement('tr');
  strategyRtTr.innerHTML = `
    <td><strong>STRATEGY SLEEVE RT</strong></td>
    <td>Pool Realtime</td>
    <td class='num'>${Number.isFinite(rtCashWeightPct) ? `${rtCashWeightPct.toFixed(2)}%` : '--'}</td>
    <td class='num'>${Number.isFinite(rtCash) ? '$' + rtCash.toFixed(2) : '--'}</td>
    <td class='num'>${Number.isFinite(rtInvested) && Number.isFinite(rtTotal) && rtTotal > 0
      ? `$${rtInvested.toFixed(2)} (${((rtInvested / rtTotal) * 100).toFixed(2)}%)`
      : (Number.isFinite(rtInvested) ? '$' + rtInvested.toFixed(2) : '--')}</td>
    <td class='num'>${Number.isFinite(rtTotal) ? '$' + rtTotal.toFixed(2) : '--'}</td>
    <td class='num ${rtPnlClass} ${rtBgClass}'>${rtReturnText}</td>
    <td class='num ${rtPnlClass} ${rtBgClass}'>${rtPnlText}</td>
  `;
  tb.appendChild(strategyRtTr);

  const metaEl = document.getElementById('futuRuntimeMeta');
  if (metaEl) {
    const runtimeFile = futuStatus?.runtime_file ? String(futuStatus.runtime_file) : '--';
    metaEl.textContent = `Runtime file: ${runtimeFile.split(/[\\/]/).pop() || runtimeFile}`;
  }
};

const collectTrackedSymbols = () => {
  const typedSymbols = (document.getElementById('pSymbols')?.value || '')
    .split(',')
    .map((x) => String(x || '').trim().toUpperCase())
    .filter(Boolean);
  const portfolioSymbols = (lastPortfolio?.asset_forecasts || [])
    .map((x) => String(x.symbol || '').toUpperCase())
    .filter(Boolean);
  const paperSnapshotSymbols = (latestPaperStatus?.latest_snapshot?.symbols || [])
    .map((x) => String(x.symbol || '').toUpperCase())
    .filter(Boolean);
  const futuSnapshotSymbols = (latestFutuStatus?.latest_snapshot?.symbols || [])
    .map((x) => String(x.symbol || '').toUpperCase())
    .filter(Boolean);

  return Array.from(new Set([
    ...portfolioSymbols,
    ...typedSymbols,
    ...paperSnapshotSymbols,
    ...futuSnapshotSymbols,
  ]));
};

const renderRealtimeMarketGrid = (gridId, snapshot, fallbackSymbols = []) => {
  const rtGrid = document.getElementById(gridId);
  if (!rtGrid) return;

  rtGrid.innerHTML = '';
  const holdings = new Set(snapshot?.holdings_symbols || []);
  const snapshotMap = new Map((snapshot?.symbols || []).map(x => [x.symbol, x]));
  const allInputs = new Set([...(fallbackSymbols || [])]);
  for (const s of (snapshot?.symbols || [])) allInputs.add(s.symbol);

  if (allInputs.size === 0) {
    rtGrid.innerHTML = `<div class='empty-state'><div class='empty-state-icon'>📡</div>No symbols tracked yet.<br>Waiting for execution snapshot.</div>`;
    return;
  }

  for (const sym of allInputs) {
    const price = getLatestQuotePrice(sym);
    const source = price != null ? 'rt' : 'none';
    const isHolding = holdings.has(sym);

    const delta = getLatestQuoteDelta(sym);
    const ch = Number(delta?.delta);
    const chPct = Number(delta?.pct);
    const hasCh = Number.isFinite(ch);
    const chSign = hasCh ? (ch >= 0 ? '+' : '') : '';
    const chClass = hasCh ? (ch >= 0 ? 'up' : 'down') : '';
    const cardMood = hasCh ? (ch >= 0 ? 'card-up' : 'card-down') : 'card-neutral';

    const sourceBadge = source === 'rt'
      ? `<span class='source-badge rt'>RT</span>`
      : `<span class='source-badge forecast'>--</span>`;
    const updatedText = source === 'rt' ? lastQuotesStampText : '--';
    const exchangeRawMs = Number(getLatestQuoteExchangeTs(sym));
    const exchangeText = source === 'rt' && Number.isFinite(exchangeRawMs) && exchangeRawMs > 0
      ? (() => {
        const d = new Date(exchangeRawMs);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;
      })()
      : '--';

    const card = document.createElement('div');
    card.className = `rt-card ${cardMood}`;
    card.innerHTML = `
      <div class='rt-card-header'>
        <div class='rt-card-symbol'>${isHolding ? "<span class='rt-card-holding-dot' title='Holding'></span>" : ''}${sym}</div>
        ${sourceBadge}
      </div>
      <div class='rt-card-price'>${price == null ? '--' : '$' + price.toFixed(2)}</div>
      <div class='rt-card-change ${chClass}'>
        ${hasCh ? `<span>${chSign}${ch.toFixed(3)}</span><span>(${chSign}${chPct.toFixed(3)}%)</span>` : '<span style="color:var(--muted-2)">1m: --</span>'}
      </div>
      <div class='rt-card-footer'>
        <span class='rt-card-updated'>Upd ${updatedText} · Exch ${exchangeText}</span>
      </div>
    `;
    rtGrid.appendChild(card);
  }
};

const renderFutuChartSummaryStrip = () => {
  const strip = document.getElementById('futuChartSummaryStrip');
  if (!strip) return;

  const latest = futuRenderContext?.latest;
  const series = futuRenderContext?.portfolioSeries || [];
  if (!latest || series.length === 0) {
    strip.style.display = 'none';
    return;
  }

  const nav = Number(latest.portfolioValue);
  const pnlPct = Number(latest.portfolioPnlPct);
  const spreadPct = Number(latest.spreadPct);

  let dailyChange = null;
  let dailyChangePct = null;
  if (series.length >= 2) {
    const cur = Number(series[series.length - 1]?.value);
    const prev = Number(series[series.length - 2]?.value);
    if (Number.isFinite(cur) && Number.isFinite(prev) && prev > 0) {
      dailyChange = cur - prev;
      dailyChangePct = ((cur - prev) / prev) * 100;
    }
  }

  const upDown = (v) => Number.isFinite(v) ? (v >= 0 ? 'up' : 'down') : '';
  const modeLabel = getFutuModeLabel();
  strip.style.display = '';
  strip.innerHTML = `
    <div class='strip-item'>
      <span class='strip-label'>${modeLabel} NAV</span>
      <span class='strip-val'>${Number.isFinite(nav) ? '$' + nav.toFixed(2) : '--'}</span>
    </div>
    <div class='strip-item'>
      <span class='strip-label'>PnL</span>
      <span class='strip-val ${upDown(pnlPct)}'>${Number.isFinite(pnlPct) ? `${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(2)}%` : '--'}</span>
    </div>
    <div class='strip-item'>
      <span class='strip-label'>Last Δ</span>
      <span class='strip-val ${upDown(dailyChange)}'>${Number.isFinite(dailyChange) ? `${dailyChange >= 0 ? '+' : ''}$${dailyChange.toFixed(2)} (${dailyChangePct.toFixed(2)}%)` : '--'}</span>
    </div>
    <div class='strip-item'>
      <span class='strip-label'>vs Bench</span>
      <span class='strip-val ${upDown(spreadPct)}'>${Number.isFinite(spreadPct) ? `${spreadPct >= 0 ? '+' : ''}${spreadPct.toFixed(2)}%` : '--'}</span>
    </div>
  `;
};

const renderFutuChartFromCurrentContext = () => {
  const filtered = filterPaperContextByRangeDays(futuFullContext, selectedFutuRangeDays);
  futuRenderContext = filtered;
  futuMetricsByTime = filtered.metricsByTime;
  if (filtered.portfolioSeries?.length > 0) {
    futuPortfolioLine.setData(ensureVisibleSeries(filtered.portfolioSeries));
    if (selectedFutuCurveMode === 'strategy' && filtered.strategyFtSeries?.length > 0) {
      futuStrategyFtLine.setData(ensureVisibleSeries(filtered.strategyFtSeries));
    } else {
      futuStrategyFtLine.setData([]);
    }
    futuBenchmarkLine.setData(filtered.benchmarkSeries?.length > 0 ? ensureVisibleSeries(filtered.benchmarkSeries) : []);

    const timeScale = futuChart?.chart?.timeScale?.();
    const latestTime = filtered.portfolioSeries[filtered.portfolioSeries.length - 1].time;
    const selectedDays = Number(selectedFutuRangeDays);
    if (timeScale && Number.isFinite(selectedDays) && selectedDays > 0 && typeof timeScale.setVisibleRange === 'function') {
      const from = latestTime - Math.floor(selectedDays * 86400);
      timeScale.setVisibleRange({ from, to: latestTime });
    } else {
      futuChart.fit();
    }
    setFutuLegendText(filtered.latest || null);
  } else {
    futuPortfolioLine.setData([]);
    futuStrategyFtLine.setData([]);
    futuBenchmarkLine.setData([]);
    setFutuLegendText(null);
  }

  for (const btn of futuRangeButtons) {
    const days = Number(btn.dataset.futuRangeDays);
    btn.classList.toggle('active', days === selectedFutuRangeDays);
  }
};

const fillFutuHoldingsTable = (futuStatus) => {
  const tb = document.querySelector('#futuHoldingsTable tbody');
  const nonPoolTb = document.querySelector('#futuNonPoolHoldingsTable tbody');
  if (!tb) return;
  const holdingsPoolBadge = document.getElementById('futuHoldingsPoolBadge');
  const nonPoolBadge = document.getElementById('futuNonPoolHoldingsBadge');
  tb.innerHTML = '';
  if (nonPoolTb) nonPoolTb.innerHTML = '';

  const snapshot = futuStatus?.latest_snapshot;
  const toNum = (value) => {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  };
  const candidateSet = buildFutuCandidateSymbolSet(futuStatus);
  const holdingsSet = new Set(snapshot?.holdings_symbols || []);
  const snapshotMap = new Map((snapshot?.symbols || []).map(x => [x.symbol, x]));
  const holdingsMap = new Map((snapshot?.holdings || []).map(x => [x.symbol, x]));
  const targetWeightMap = new Map();
  for (const t of (latestPaperStatus?.target_weights || [])) {
    const symbol = String(t?.symbol || '').trim().toUpperCase();
    const weight = Number(t?.weight);
    if (!symbol || !Number.isFinite(weight)) continue;
    targetWeightMap.set(symbol, weight);
    const parts = symbol.split('.');
    if (parts.length === 2 && parts[1]) {
      targetWeightMap.set(parts[1], weight);
    } else {
      const marketPrefix = String(futuStatus?.selected_market || 'US').trim().toUpperCase();
      if (marketPrefix) targetWeightMap.set(`${marketPrefix}.${symbol}`, weight);
    }
  }
  const rawPositionsRows = (() => {
    const raw = futuStatus?.opend_positions_raw;
    if (Array.isArray(raw)) return raw;
    if (!raw || typeof raw !== 'object') return [];
    if (Array.isArray(raw?.data)) return raw.data;
    if (Array.isArray(raw?.positions)) return raw.positions;
    if (Array.isArray(raw?.rows)) return raw.rows;
    return [];
  })();
  const rawPositionsMap = new Map(
    rawPositionsRows
      .map((row) => [String(row?.code || '').toUpperCase(), row])
      .filter(([code]) => !!code)
  );

  if (holdingsPoolBadge) {
    const symbols = Array.from(candidateSet).filter((symbol) => symbol.includes('.'));
    const holdingCount = holdingsSet.size;
    const poolCount = symbols.length;
    const inPoolHeldCount = Array.from(holdingsSet).filter((symbol) => isFutuCandidateSymbol(symbol, candidateSet, futuStatus)).length;
    const notHeldCount = Math.max(0, poolCount - inPoolHeldCount);
    holdingsPoolBadge.textContent = `Holding ${holdingCount} · Pool ${poolCount} · In Pool ${inPoolHeldCount} · Not Held ${notHeldCount}`;
  }

  const orderedSymbols = Array.from(holdingsSet).sort();
  if (orderedSymbols.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='9'><div class='empty-state'><div class='empty-state-icon'>📊</div>No Futu holdings yet.<br>Check Futu API connection and position stream.</div></td>`;
    tb.appendChild(tr);
    if (nonPoolTb) {
      const nonPoolTr = document.createElement('tr');
      nonPoolTr.innerHTML = `<td colspan='7'><div class='empty-state'><div class='empty-state-icon'>📦</div>No non-pool holdings.</div></td>`;
      nonPoolTb.appendChild(nonPoolTr);
    }
    return;
  }

  const poolRows = [];
  const nonPoolRows = [];

  for (const sym of orderedSymbols) {
    const row = snapshotMap.get(sym);
    const holding = holdingsMap.get(sym);
    const rawRow = rawPositionsMap.get(sym) || rawPositionsMap.get(`US.${sym}`) || null;
    const rawNominalPrice = [
      toNum(rawRow?.nominal_price),
      toNum(rawRow?.market_price),
      toNum(rawRow?.price),
      toNum(rawRow?.last_price),
    ].find((v) => Number.isFinite(v) && v > 0) ?? null;
    const livePrice = getLatestQuotePrice(sym);
    const currentPrice = livePrice ?? rawNominalPrice;
    const quantity = holding?.quantity ?? null;
    const rawMarketValue = [
      toNum(rawRow?.market_val),
      toNum(rawRow?.market_value),
      toNum(rawRow?.asset_value),
    ].find((v) => Number.isFinite(v) && v >= 0) ?? null;
    const assetValue = (quantity != null && currentPrice != null)
      ? quantity * currentPrice
      : (rawMarketValue ?? holding?.asset_value ?? null);
    const avgCostFromRaw = [
      toNum(rawRow?.average_cost),
      toNum(rawRow?.avg_cost),
      toNum(rawRow?.cost_price),
      toNum(rawRow?.diluted_cost),
      toNum(rawRow?.cost),
    ].find((v) => Number.isFinite(v) && v > 0) ?? null;
    const avgCostFromHolding = toNum(holding?.avg_cost);
    const avgCost = (avgCostFromRaw != null && avgCostFromRaw > 0) ? avgCostFromRaw : avgCostFromHolding;
    const validAvgCost = Number.isFinite(avgCost) && avgCost > 0 ? avgCost : null;
    const symUpper = String(sym || '').toUpperCase();
    const symStripped = symUpper.includes('.') ? symUpper.split('.').slice(1).join('.') : symUpper;
    const targetWeight = targetWeightMap.get(symUpper) ?? targetWeightMap.get(symStripped);

    let unrealizedText = '--';
    let unrealizedClass = '';
    if (quantity != null && currentPrice != null && validAvgCost != null && quantity > 0) {
      const unrealizedUsd = (currentPrice - validAvgCost) * quantity;
      const unrealizedPct = validAvgCost !== 0 ? ((currentPrice - validAvgCost) / validAvgCost) * 100 : 0;
      unrealizedText = `${formatSignedMoney(unrealizedUsd)} (${unrealizedPct >= 0 ? '+' : ''}${unrealizedPct.toFixed(2)}%)`;
      unrealizedClass = unrealizedUsd >= 0 ? 'up' : 'down';
    }
    if (unrealizedText === '--' && rawRow) {
      const ratioAvgCost = toNum(rawRow?.pl_ratio_avg_cost);
      const ratioAny = ratioAvgCost ?? toNum(rawRow?.pl_ratio);
      const plValValid = String(rawRow?.pl_val_valid).toLowerCase() !== 'false';
      const plVal = plValValid ? toNum(rawRow?.pl_val) : null;
      if (ratioAny != null) {
        const ratioText = `${ratioAny >= 0 ? '+' : ''}${ratioAny.toFixed(2)}%`;
        if (plVal != null) {
          unrealizedText = `${formatSignedMoney(plVal)} (${ratioText})`;
          unrealizedClass = plVal >= 0 ? 'up' : 'down';
        } else {
          unrealizedText = `-- (${ratioText})`;
          unrealizedClass = ratioAny >= 0 ? 'up' : 'down';
        }
      } else if (plVal != null) {
        unrealizedText = formatSignedMoney(plVal);
        unrealizedClass = plVal >= 0 ? 'up' : 'down';
      }
    }

    const rowData = {
      symbol: sym,
      quantity,
      currentPrice,
      validAvgCost,
      assetValue,
      targetWeight,
      unrealizedText,
      unrealizedClass,
      isCandidate: isFutuCandidateSymbol(sym, candidateSet, futuStatus),
    };

    if (rowData.isCandidate) {
      poolRows.push(rowData);
    } else {
      nonPoolRows.push(rowData);
    }
  }

  if (nonPoolBadge) {
    const nonPoolAssetValue = nonPoolRows.reduce((sum, row) => sum + (Number.isFinite(row.assetValue) ? row.assetValue : 0), 0);
    nonPoolBadge.textContent = nonPoolRows.length > 0
      ? `Excluded from FUTU sim metrics · ${nonPoolRows.length} symbols · $${nonPoolAssetValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
      : 'Excluded from FUTU sim metrics · 0 symbols';
  }

  if (poolRows.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='9'><div class='empty-state'><div class='empty-state-icon'>🎯</div>No in-pool FUTU holdings.<br>Pool-only strategy metrics will ignore the positions listed below.</div></td>`;
    tb.appendChild(tr);
  }

  for (const row of poolRows) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style='font-weight:600;letter-spacing:.2px;'>${row.symbol}</td>
      <td class='up' style='font-weight:600;font-size:11px;font-family:var(--mono);'>Pool</td>
      <td class='num'>${row.quantity == null ? '--' : row.quantity.toFixed(2)}</td>
      <td class='num'>${row.currentPrice == null ? '--' : '$' + row.currentPrice.toFixed(2)}</td>
      <td class='num'>${row.validAvgCost == null ? '--' : '$' + row.validAvgCost.toFixed(2)}</td>
      <td class='num'>${row.assetValue == null ? '--' : '$' + row.assetValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
      <td class='num'>${Number.isFinite(row.targetWeight) ? `${(row.targetWeight * 100).toFixed(2)}%` : '--'}</td>
      <td class='num ${row.unrealizedClass}' style='font-weight:600;'>${row.unrealizedText}</td>
      <td><button class='ghost futu-manual-fill-btn' data-symbol='${row.symbol}' data-price='${row.currentPrice == null ? '' : row.currentPrice.toFixed(4)}'>Prefill</button></td>
    `;
    tb.appendChild(tr);
  }

  if (nonPoolTb) {
    if (nonPoolRows.length === 0) {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td colspan='7'><div class='empty-state'><div class='empty-state-icon'>📦</div>No non-pool holdings.</div></td>`;
      nonPoolTb.appendChild(tr);
    } else {
      for (const row of nonPoolRows) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td style='font-weight:600;letter-spacing:.2px;'>${row.symbol}</td>
          <td class='flat' style='font-weight:600;font-size:11px;font-family:var(--mono);'>Excluded</td>
          <td class='num'>${row.quantity == null ? '--' : row.quantity.toFixed(2)}</td>
          <td class='num'>${row.currentPrice == null ? '--' : '$' + row.currentPrice.toFixed(2)}</td>
          <td class='num'>${row.validAvgCost == null ? '--' : '$' + row.validAvgCost.toFixed(2)}</td>
          <td class='num'>${row.assetValue == null ? '--' : '$' + row.assetValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
          <td class='num ${row.unrealizedClass}' style='font-weight:600;'>${row.unrealizedText}</td>
        `;
        nonPoolTb.appendChild(tr);
      }
    }
  }
};

const setFutuManualOrderHint = (text, mode = '') => {
  if (!futuManualOrderHint) return;
  futuManualOrderHint.textContent = text;
  futuManualOrderHint.classList.remove('up', 'down');
  if (mode === 'ok') futuManualOrderHint.classList.add('up');
  if (mode === 'err') futuManualOrderHint.classList.add('down');
};

const renderFutuTradingChecklist = (context) => {
  const grid = document.getElementById('futuChecklistGrid');
  if (!grid) return;
  const summaryCard = document.getElementById('futuChecklistSummaryCard');
  const summaryStatus = document.getElementById('futuChecklistSummaryStatus');
  const summaryMeta = document.getElementById('futuChecklistSummaryMeta');
  const summaryText = document.getElementById('futuChecklistSummaryText');

  const items = [
    {
      label: 'Account Selected',
      status: context.hasAccount ? 'READY' : 'MISSING',
      tone: context.hasAccount ? 'ok' : 'warn',
      text: context.hasAccount
        ? `Active account #${context.selectedAccId} is selected for ${context.selectedMarket} routing.`
        : 'Choose and apply a FUTU account before considering live rollout.',
    },
    {
      label: 'Trading Environment',
      status: context.isRealMode ? 'REAL' : 'SIM',
      tone: context.isRealMode ? 'warn' : 'ok',
      text: context.isRealMode
        ? 'REAL account context is visible. This is useful for inspection, but not enough to arm live execution.'
        : 'Simulation environment is selected, which is the correct rehearsal mode before going live.',
    },
    {
      label: 'Gateway Link',
      status: context.connected ? 'ONLINE' : 'OFFLINE',
      tone: context.connected ? 'ok' : 'warn',
      text: context.connected
        ? `OpenD gateway is connected at ${context.connHostPort}.`
        : 'Gateway is not connected. Live readiness cannot be assessed while broker connectivity is offline.',
    },
    {
      label: 'Executor State',
      status: context.running ? 'RUNNING' : 'IDLE',
      tone: context.running ? 'ok' : 'warn',
      text: context.running
        ? 'FUTU executor loop is active and can react to signals.'
        : 'Start the FUTU executor to validate the end-to-end operational path.',
    },
    {
      label: 'Market Data',
      status: context.dataFeedReady ? 'READY' : 'DEGRADED',
      tone: context.dataFeedReady ? 'ok' : 'warn',
      text: context.dataFeedReady
        ? `Realtime data path is healthy via ${context.dataLiveSource}.`
        : `Realtime data path is not fully healthy. Current source: ${context.dataLiveSource}.`,
    },
    {
      label: 'Risk Gate',
      status: context.hasRiskGate ? 'READY' : 'PENDING',
      tone: context.hasRiskGate ? 'ok' : 'warn',
      text: context.hasRiskGate
        ? 'Latest regime overlay exists, so cash cap and gross exposure controls are available.'
        : 'No latest allocation overlay yet. Run candidate optimization before trusting execution limits.',
    },
    {
      label: 'Broker Unlock',
      status: 'UNKNOWN',
      tone: 'unknown',
      text: 'Unlock status is not surfaced to the WebUI yet. Backend/API wiring is still needed before this can become a hard readiness check.',
    },
    {
      label: 'Order Gate',
      status: context.isRealMode ? 'DISABLED' : 'SIM ONLY',
      tone: context.isRealMode ? 'fail' : 'ok',
      text: context.isRealMode
        ? 'Live order placement remains intentionally blocked by the backend safety gate in this build.'
        : 'Orders can be submitted only to the simulation environment. Real order routing is still disarmed.',
    },
  ];

  const readyCount = items.filter((item) => item.tone === 'ok').length;
  const attentionCount = items.length - readyCount;

  summaryCard?.classList.toggle('mode-real', !!context.isRealMode);
  summaryCard?.classList.toggle('mode-sim', !context.isRealMode);
  if (summaryStatus) summaryStatus.textContent = `${readyCount}/${items.length} Ready`;
  if (summaryMeta) {
    summaryMeta.textContent = `${attentionCount} attention · ${context.isRealMode ? 'REAL context visible' : 'SIM context active'}`;
  }
  if (summaryText) {
    summaryText.textContent = context.isRealMode
      ? 'Real account context detected. Expand to inspect safety and readiness gates.'
      : 'SIM rehearsal active. Expand to review the live-readiness checklist.';
  }

  const checklistGroups = [
    {
      title: 'Access Gate',
      subtitle: 'Broker identity and routing context.',
      itemIndexes: [0, 1, 6],
    },
    {
      title: 'Connectivity',
      subtitle: 'Gateway and data-plane readiness.',
      itemIndexes: [2, 4],
    },
    {
      title: 'Execution Path',
      subtitle: 'Executor and order submission safeguards.',
      itemIndexes: [3, 7],
    },
    {
      title: 'Risk Controls',
      subtitle: 'Allocation overlay and trade guardrails.',
      itemIndexes: [5],
    },
  ];

  const renderChecklistItem = (item) => {
    const pillToneStyles = {
      ok: 'border-color:rgba(0,212,170,.26);background:rgba(0,212,170,.12);color:#8ff2d6;',
      warn: 'border-color:rgba(245,158,11,.26);background:rgba(245,158,11,.12);color:#ffd28a;',
      fail: 'border-color:rgba(255,71,87,.28);background:rgba(255,71,87,.12);color:#ffb2ba;',
      unknown: 'border-color:rgba(96,165,250,.24);background:rgba(96,165,250,.10);color:#add2ff;',
    };
    return `
      <article class='futu-checklist-item ${item.tone}' style='display:grid;align-content:start;gap:8px;min-height:96px;padding:12px;border-radius:12px;border:1px solid rgba(255,255,255,.06);background:linear-gradient(180deg, rgba(10,16,24,.92) 0%, rgba(8,12,18,.98) 100%);box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
        <div class='futu-checklist-item-top' style='display:flex;align-items:flex-start;justify-content:space-between;gap:10px;'>
          <div class='futu-checklist-label' style='font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#8ea2bf;'>${escapeHtmlText(item.label)}</div>
          <span class='futu-checklist-pill ${item.tone}' style='display:inline-flex;align-items:center;justify-content:center;padding:4px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.10);font-family:var(--mono);font-size:9.5px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;white-space:nowrap;${pillToneStyles[item.tone] || ''}'>${escapeHtmlText(item.status)}</span>
        </div>
        <div class='futu-checklist-text' style='font-size:10.5px;line-height:1.45;color:#d3dceb;'>${escapeHtmlText(item.text)}</div>
      </article>
    `;
  };

  grid.innerHTML = checklistGroups.map((group) => {
    const groupItems = group.itemIndexes
      .map((index) => items[index])
      .filter(Boolean);
    const okCount = groupItems.filter((item) => item.tone === 'ok').length;
    return `
      <section class='futu-checklist-cluster' style='display:grid;gap:10px;padding:12px;border-radius:14px;border:1px solid rgba(255,255,255,.06);background:radial-gradient(circle at top right, rgba(59,130,246,.07), transparent 28%), linear-gradient(180deg, rgba(10,16,24,.92) 0%, rgba(8,12,18,.98) 100%);box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
        <div class='futu-checklist-cluster-head' style='display:flex;align-items:flex-start;justify-content:space-between;gap:10px;padding-bottom:8px;border-bottom:1px solid rgba(255,255,255,.05);'>
          <div>
            <div class='futu-checklist-cluster-title' style='font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:#dbe7f8;margin-bottom:3px;'>${escapeHtmlText(group.title)}</div>
            <div class='futu-checklist-cluster-subtitle' style='font-size:10px;line-height:1.45;color:#8ea2bf;max-width:28ch;'>${escapeHtmlText(group.subtitle)}</div>
          </div>
          <span class='futu-checklist-cluster-badge' style='display:inline-flex;align-items:center;justify-content:center;padding:5px 9px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.05);color:#eff5ff;font-family:var(--mono);font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;white-space:nowrap;'>${okCount}/${groupItems.length} Ready</span>
        </div>
        <div class='futu-checklist-cluster-grid' style='display:grid;grid-template-columns:repeat(auto-fit, minmax(180px, 1fr));gap:10px;'>
          ${groupItems.map(renderChecklistItem).join('')}
        </div>
      </section>
    `;
  }).join('');
};

const submitFutuManualOrder = async () => {
  const selectedEnv = String(latestFutuStatus?.selected_trd_env || '').toUpperCase();
  if (selectedEnv && selectedEnv !== 'SIMULATE') {
    throw new Error('Manual order is allowed only in SIMULATE mode.');
  }

  const symbol = String(futuManualSymbolInput?.value || '').trim().toUpperCase();
  const side = String(futuManualSideSelect?.value || 'BUY').trim().toUpperCase();
  const quantity = Number(futuManualQtyInput?.value);
  const priceRaw = String(futuManualPriceInput?.value || '').trim();
  const price = priceRaw ? Number(priceRaw) : NaN;
  const timeInForce = String(futuManualTifSelect?.value || 'DAY').trim().toUpperCase();
  const session = String(futuManualSessionSelect?.value || '').trim().toUpperCase();
  const remark = String(futuManualRemarkInput?.value || '').trim();

  if (!symbol) throw new Error('Symbol is required.');
  if (!Number.isFinite(quantity) || quantity <= 0) throw new Error('Quantity must be > 0.');
  if (!Number.isFinite(price) || price <= 0) throw new Error('Limit price must be > 0.');
  if (side !== 'BUY' && side !== 'SELL') throw new Error('Side must be BUY or SELL.');

  const payload = {
    symbol,
    side,
    quantity,
    price,
    order_type: 'NORMAL',
    time_in_force: timeInForce,
    fill_outside_rth: false,
    session: session || null,
    remark: remark || null,
  };

  const response = await api('/api/futu/manual-order', {
    method: 'POST',
    body: JSON.stringify(payload),
  });

  return response;
};

const submitFutuModifyOrder = async ({ orderId, modifyOrderOp, qty = null, price = null }) => {
  const selectedEnv = String(latestFutuStatus?.selected_trd_env || '').toUpperCase();
  if (selectedEnv && selectedEnv !== 'SIMULATE') {
    throw new Error('Modify/cancel is allowed only in SIMULATE mode.');
  }

  const payload = {
    order_id: String(orderId || '').trim(),
    modify_order_op: String(modifyOrderOp || '').trim().toUpperCase(),
    qty: qty == null ? null : Number(qty),
    price: price == null ? null : Number(price),
    trd_env: selectedEnv || null,
    acc_id: latestFutuStatus?.selected_acc_id || null,
    adjust_limit: 0,
  };

  if (!payload.order_id) throw new Error('order_id is required.');
  if (payload.modify_order_op !== 'CANCEL' && payload.modify_order_op !== 'NORMAL') {
    throw new Error('modify_order_op must be CANCEL or NORMAL.');
  }
  if (payload.modify_order_op === 'NORMAL') {
    if (!Number.isFinite(payload.qty) || payload.qty <= 0) throw new Error('qty must be > 0 for change.');
    if (!Number.isFinite(payload.price) || payload.price <= 0) throw new Error('price must be > 0 for change.');
  }

  const response = await api('/api/futu/modify-order', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
  return response;
};

const futuReadField = (row, keys, fallback = '--') => {
  if (!row || typeof row !== 'object') return fallback;
  for (const key of keys) {
    const value = row?.[key];
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (!text || text.toLowerCase() === 'n/a') continue;
    return text;
  }
  return fallback;
};

const futuReadNumber = (row, keys) => {
  if (!row || typeof row !== 'object') return null;
  for (const key of keys) {
    const value = Number(row?.[key]);
    if (Number.isFinite(value)) return value;
  }
  return null;
};

const futuReadBool = (row, keys) => {
  if (!row || typeof row !== 'object') return null;
  for (const key of keys) {
    const raw = row?.[key];
    if (raw === null || raw === undefined) continue;
    if (typeof raw === 'boolean') return raw;
    const text = String(raw).trim().toLowerCase();
    if (!text) continue;
    if (text === 'true' || text === '1' || text === 'yes' || text === 'y') return true;
    if (text === 'false' || text === '0' || text === 'no' || text === 'n') return false;
  }
  return null;
};

const futuEscapeHtml = (value) => String(value)
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')
  .replaceAll("'", '&#39;');

const futuParseTimeMs = (raw) => {
  const text = String(raw || '').trim();
  if (!text) return NaN;

  let parsed = Date.parse(text);
  if (Number.isFinite(parsed)) return parsed;

  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$/.test(text)) {
    parsed = Date.parse(text.replace(' ', 'T'));
  }
  return parsed;
};

const futuRenderDualTime = (row, keys) => {
  const rawTime = futuReadField(row, keys, '--');
  if (rawTime === '--') return '--';

  const parsedMs = futuParseTimeMs(rawTime);
  const localTime = Number.isFinite(parsedMs)
    ? new Date(parsedMs).toLocaleString()
    : '--';

  return `<span style='white-space:nowrap;'>${futuEscapeHtml(localTime)}</span>`;
};

const futuOrderLooksOpen = (row) => {
  const canCancel = futuReadBool(row, ['can_cancel', 'is_can_cancel', 'can_cancelled']);
  if (canCancel === true) return true;

  const status = futuReadField(row, ['order_status', 'status', 'orderStatus'], '').toUpperCase();
  if (!status) return true;

  const terminalStatuses = [
    'FILLED_ALL',
    'FILLED',
    'CANCELLED_ALL',
    'CANCELLED_PART',
    'CANCELLED',
    'FAILED',
    'DELETED',
    'DISABLED',
    'WITHDRAWN',
    'REJECTED',
    'EXPIRED',
  ];

  return !terminalStatuses.some((terminal) => status.includes(terminal));
};

const futuSymbolMatchesFilter = (symbol) => {
  const keyword = String(futuActivityFilterText || '').trim().toUpperCase();
  if (!keyword) return true;
  const text = String(symbol || '').trim().toUpperCase();
  if (!text) return false;
  return text.includes(keyword);
};

const futuActivityTimeMatchesFilter = (row, keys = ['timestamp', 'create_time', 'updated_time', 'time']) => {
  const days = Number(futuActivityRangeDays || 0);
  if (!Number.isFinite(days) || days <= 0) return true;

  let tsMs = NaN;
  for (const key of keys) {
    const raw = row?.[key];
    if (raw === null || raw === undefined) continue;
    const parsed = new Date(String(raw)).getTime();
    if (Number.isFinite(parsed) && parsed > 0) {
      tsMs = parsed;
      break;
    }
  }
  if (!Number.isFinite(tsMs)) return true;

  const cutoff = Date.now() - days * 86400 * 1000;
  return tsMs >= cutoff;
};

const futuExtractTimestampMs = (row, keys = ['timestamp', 'create_time', 'updated_time', 'time']) => {
  for (const key of keys) {
    const raw = row?.[key];
    if (raw === null || raw === undefined) continue;
    const parsed = new Date(String(raw)).getTime();
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return 0;
};

const futuSortRowsByLatest = (rows, keys) => rows.slice().sort((left, right) => {
  const leftTs = futuExtractTimestampMs(left, keys);
  const rightTs = futuExtractTimestampMs(right, keys);
  return rightTs - leftTs;
});

const futuResetScrollViewport = (element) => {
  if (!element) return;
  element.scrollTop = 0;
  element.scrollLeft = 0;
};

const renderFutuOpenOrdersTable = (futuStatus) => {
  const tb = document.querySelector('#futuOpenOrdersTable tbody');
  if (!tb) return;
  tb.innerHTML = '';

  const rows = futuSortRowsByLatest(
    (Array.isArray(futuStatus?.open_orders) ? futuStatus.open_orders : [])
    .filter((row) => futuOrderLooksOpen(row))
    .filter((row) => futuSymbolMatchesFilter(futuReadField(row, ['code', 'symbol', 'ticker'], '')))
    .filter((row) => futuActivityTimeMatchesFilter(row, ['create_time', 'updated_time', 'time'])),
    ['create_time', 'updated_time', 'time']
  );
  const scrollShell = tb.closest('.futu-ops-table-shell');
  if (!rows.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='12'><div class='empty-state'><div class='empty-state-icon'>🧾</div>No open orders.<br>Pending / unfilled orders will appear here.</div></td>`;
    tb.appendChild(tr);
    futuResetScrollViewport(scrollShell);
    return;
  }

  for (const row of rows) {
    const created = futuRenderDualTime(row, ['create_time', 'created_at', 'time']);
    const orderId = futuReadField(row, ['order_id', 'id']);
    const symbol = futuReadField(row, ['code', 'symbol', 'ticker']);
    const side = futuReadField(row, ['trd_side', 'side']);
    const qty = futuReadNumber(row, ['qty', 'quantity']);
    const price = futuReadNumber(row, ['price']);
    const dealtQty = futuReadNumber(row, ['dealt_qty', 'filled_qty']);
    const dealtAvg = futuReadNumber(row, ['dealt_avg_price', 'filled_avg_price']);
    const orderType = futuReadField(row, ['order_type']);
    const orderStatus = futuReadField(row, ['order_status', 'status']);
    const orderTypeUpper = String(orderType).toUpperCase();
    const priceDisplay = (orderTypeUpper.includes('MARKET') && Number.isFinite(price) && price <= 0)
      ? 'MKT'
      : (price == null ? '--' : '$' + price.toFixed(4));

    const paramsParts = [
      `market=${futuReadField(row, ['order_market'], '--')}`,
      `env=${futuReadField(row, ['trd_env', 'trade_env'], '--')}`,
      `currency=${futuReadField(row, ['currency'], '--')}`,
      `tif=${futuReadField(row, ['time_in_force'], '--')}`,
      `session=${futuReadField(row, ['session'], '--')}`,
      `outside_rth=${futuReadField(row, ['fill_outside_rth'], '--')}`,
      `remark=${futuReadField(row, ['remark'], '--')}`,
      `aux=${futuReadField(row, ['aux_price'], '--')}`,
      `trail_type=${futuReadField(row, ['trail_type'], '--')}`,
      `trail_value=${futuReadField(row, ['trail_value'], '--')}`,
      `trail_spread=${futuReadField(row, ['trail_spread'], '--')}`,
      `err=${futuReadField(row, ['last_err_msg'], '--')}`,
    ];
    const paramsText = paramsParts.join(' · ');
    const actionDisabled = !orderId || String(latestFutuStatus?.selected_trd_env || '').toUpperCase() !== 'SIMULATE';
    const actionDisabledAttr = actionDisabled ? 'disabled' : '';
    const editingThisRow = futuOpenOrderEditState.orderId === orderId;
    const defaultQty = qty == null || !Number.isFinite(qty) ? '' : String(qty);
    const defaultPrice = price == null || !Number.isFinite(price) ? '' : String(price);

    const statusUpper = String(orderStatus).toUpperCase();
    const statusCls = statusUpper.includes('FILL') ? 'up'
      : (statusUpper.includes('CANCEL') || statusUpper.includes('FAIL') || statusUpper.includes('REJECT') || statusUpper.includes('EXPIRED') ? 'down'
      : (statusUpper.includes('SUBMIT') || statusUpper.includes('WAITING') ? 'flat' : ''));

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${created}</td>
      <td title='${orderId}'><span style='font-family:var(--mono);font-size:11px;color:var(--muted);'>${orderId}</span></td>
      <td style='font-weight:600;letter-spacing:.2px;'>${symbol}</td>
      <td class='${String(side).toUpperCase().includes('BUY') ? 'up' : (String(side).toUpperCase().includes('SELL') ? 'down' : '')}' style='font-weight:700;letter-spacing:.3px;'>${side}</td>
      <td class='num'>${qty == null ? '--' : qty.toFixed(2)}</td>
      <td class='num'>${priceDisplay}</td>
      <td class='num'>${dealtQty == null ? '--' : dealtQty.toFixed(2)}</td>
      <td class='num'>${dealtAvg == null ? '--' : '$' + dealtAvg.toFixed(4)}</td>
      <td><span style='font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.3px;'>${orderType}</span></td>
      <td class='${statusCls}'><span style='font-weight:600;font-size:11px;font-family:var(--mono);'>${orderStatus}</span></td>
      <td title='${paramsText.replaceAll("'", '&apos;')}' style='max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:10px;color:var(--muted);'>${paramsText}</td>
      <td>
        <div style='display:flex;gap:6px;justify-content:flex-end;align-items:center;flex-wrap:wrap;'>
          ${editingThisRow ? `
            <input class='futu-order-edit-qty' type='number' step='0.01' min='0.01' value='${futuOpenOrderEditState.qty || defaultQty}' style='width:90px;' />
            <input class='futu-order-edit-price' type='number' step='0.0001' min='0.0001' value='${futuOpenOrderEditState.price || defaultPrice}' style='width:100px;' />
            <button class='ghost futu-order-edit-save-btn' data-order-id='${orderId}' ${actionDisabledAttr}>Save</button>
            <button class='ghost futu-order-edit-cancel-btn' data-order-id='${orderId}'>Abort</button>
          ` : `
            <button class='ghost futu-order-change-btn' data-order-id='${orderId}' data-qty='${qty == null ? '' : qty}' data-price='${price == null ? '' : price}' ${actionDisabledAttr}>Change</button>
          `}
          <button class='ghost futu-order-cancel-btn' data-order-id='${orderId}' ${actionDisabledAttr}>Cancel</button>
        </div>
      </td>
    `;
    tb.appendChild(tr);
  }

  futuResetScrollViewport(scrollShell);
};

const renderFutuCancelHistoryTable = (futuStatus) => {
  const tb = document.querySelector('#futuCancelHistoryTable tbody');
  if (!tb) return;
  tb.innerHTML = '';

  const localCancelRows = Array.isArray(futuStatus?.cancel_history) ? futuStatus.cancel_history.slice() : [];
  const fallbackCancelRows = (Array.isArray(futuStatus?.history_orders) ? futuStatus.history_orders : [])
    .filter((row) => {
      const status = futuReadField(row, ['order_status', 'status', 'orderStatus'], '').toUpperCase();
      if (!status) return false;
      return status.includes('CANCEL') || status.includes('WITHDRAW') || status.includes('DELETE');
    })
    .map((row) => ({
      timestamp: futuReadField(row, ['updated_time', 'create_time', 'time'], '--'),
      order_id: futuReadField(row, ['order_id', 'id'], '--'),
      symbol: futuReadField(row, ['code', 'symbol', 'ticker'], '--'),
      trd_side: futuReadField(row, ['trd_side', 'side'], '--'),
      qty: futuReadNumber(row, ['qty', 'quantity']),
      price: futuReadNumber(row, ['price']),
      order_status: futuReadField(row, ['order_status', 'status', 'orderStatus'], '--'),
      reason: futuReadField(row, ['last_err_msg', 'remark'], 'Cancelled (history)'),
      signal_id: futuReadField(row, ['signal_id'], '--'),
    }));

  const dedup = new Map();
  for (const row of [...localCancelRows, ...fallbackCancelRows]) {
    const orderId = futuReadField(row, ['order_id', 'id'], '--');
    const ts = futuReadField(row, ['timestamp', 'updated_time', 'create_time', 'time'], '--');
    dedup.set(`${orderId}::${ts}`, row);
  }

  const rows = futuSortRowsByLatest(
    Array.from(dedup.values())
      .filter((row) => futuSymbolMatchesFilter(futuReadField(row, ['symbol', 'code'], '')))
      .filter((row) => futuActivityTimeMatchesFilter(row, ['timestamp', 'updated_time', 'create_time', 'time'])),
    ['timestamp', 'updated_time', 'create_time', 'time']
  );
  const scrollShell = tb.closest('.futu-ops-table-shell');
  if (!rows.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='9'><div class='empty-state'><div class='empty-state-icon'>🚫</div>No cancel records yet.<br>Auto-cancel events before rebalance will appear here.</div></td>`;
    tb.appendChild(tr);
    futuResetScrollViewport(scrollShell);
    return;
  }

  for (const row of rows) {
    const ts = futuRenderDualTime(row, ['timestamp', 'updated_time', 'create_time', 'time']);
    const orderId = futuReadField(row, ['order_id', 'id']);
    const symbol = futuReadField(row, ['symbol', 'code']);
    const side = futuReadField(row, ['trd_side', 'side']);
    const qty = futuReadNumber(row, ['qty', 'quantity']);
    const price = futuReadNumber(row, ['price']);
    const status = futuReadField(row, ['order_status', 'status']);
    const reason = futuReadField(row, ['reason']);
    const signalId = futuReadField(row, ['signal_id']);

    const statusCancelUpper = String(status).toUpperCase();
    const statusCancelCls = statusCancelUpper.includes('CANCEL') ? 'down'
      : (statusCancelUpper.includes('FILL') ? 'up' : 'flat');

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${ts}</td>
      <td><span style='font-family:var(--mono);font-size:11px;color:var(--muted);'>${orderId}</span></td>
      <td style='font-weight:600;letter-spacing:.2px;'>${symbol}</td>
      <td class='${String(side).toUpperCase().includes('BUY') ? 'up' : (String(side).toUpperCase().includes('SELL') ? 'down' : '')}' style='font-weight:700;letter-spacing:.3px;'>${side}</td>
      <td class='num'>${qty == null ? '--' : qty.toFixed(2)}</td>
      <td class='num'>${price == null ? '--' : '$' + price.toFixed(4)}</td>
      <td class='${statusCancelCls}'><span style='font-weight:600;font-size:11px;font-family:var(--mono);'>${status}</span></td>
      <td style='max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;' title='${String(reason).replaceAll("'", '&apos;')}'>${reason}</td>
      <td><span style='font-family:var(--mono);font-size:10px;color:var(--muted);'>${signalId}</span></td>
    `;
    tb.appendChild(tr);
  }

  futuResetScrollViewport(scrollShell);
};

const renderFutuTradeHistory = (futuStatus) => {
  const box = document.getElementById('futuTradeHistory');
  if (!box) return;

  const executedRows = Array.isArray(futuStatus?.trade_history) ? futuStatus.trade_history : [];
  const historyOrders = Array.isArray(futuStatus?.history_orders) ? futuStatus.history_orders : [];
  const rowsRaw = executedRows.length
    ? executedRows
    : historyOrders.filter((row) => {
        const dealtQty = futuReadNumber(row, ['dealt_qty', 'filled_qty', 'exec_qty']);
        if (dealtQty != null && dealtQty > 0) return true;
        const status = String(futuReadField(row, ['order_status', 'status'], '') || '').toUpperCase();
        return status.includes('FILLED');
      });
  const rows = futuSortRowsByLatest(
    rowsRaw
      .filter((row) => futuSymbolMatchesFilter(futuReadField(row, ['code', 'symbol', 'ticker'], ''))),
    ['create_time', 'updated_time', 'time', 'timestamp']
  );
  if (!rows.length) {
    box.innerHTML = `<div class='empty-state'><div class='empty-state-icon'>📒</div>No executed FUTU trades yet.<br>Filled executions will appear here.</div>`;
    futuResetScrollViewport(box);
    return;
  }

  box.innerHTML = rows.map((tr) => {
    const ts = futuRenderDualTime(tr, ['create_time', 'updated_time', 'time']);
    const side = futuReadField(tr, ['trd_side', 'side'], '--').toUpperCase();
    const sideClass = side.includes('BUY') ? 'buy' : (side.includes('SELL') ? 'sell' : '');
    const symbol = futuReadField(tr, ['code', 'symbol'], '--');
    const qty = futuReadNumber(tr, ['qty', 'quantity']);
    const price = futuReadNumber(tr, ['price', 'dealt_avg_price']);
    const amount = qty != null && price != null ? qty * price : null;
    const orderId = futuReadField(tr, ['order_id', 'id'], '--');
    const remark = futuReadField(tr, ['remark'], '--');
    const currency = futuReadField(tr, ['currency'], '--');

    const statusRow = futuReadField(tr, ['order_status', 'status'], '--');
    const statusRowUpper = String(statusRow).toUpperCase();
    const statusTrCls = statusRowUpper.includes('FILL') ? 'up'
      : (statusRowUpper.includes('CANCEL') || statusRowUpper.includes('FAIL') ? 'down' : 'flat');

    return `
      <div class='trade-item'>
        <div class='trade-time'>${ts}</div>
        <div><span class='trade-side ${sideClass}'>${side || '--'}</span></div>
        <div class='trade-main'>
          <span class='trade-symbol'>${symbol}</span>
          <span style='font-family:var(--mono);font-variant-numeric:tabular-nums;'>${qty == null ? '--' : qty.toFixed(2)} @ ${price == null ? '--' : '$' + price.toFixed(4)}</span>
          <span class='trade-meta'>Notional ${amount == null ? '--' : '$' + amount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} · ${currency} · <span class='${statusTrCls}' style='font-weight:600;'>${statusRow}</span> · #${orderId}</span>
        </div>
        <div class='trade-pnl flat'>${remark === '--' ? '' : remark}</div>
      </div>
    `;
  }).join('');

  futuResetScrollViewport(box);
};

const refreshRealtimeQuotes = async () => {
  const trackedSymbols = collectTrackedSymbols();
  if (!trackedSymbols.length) return;
  const quoteToTrackedMap = new Map();
  for (const symbol of trackedSymbols) {
    const tracked = String(symbol || '').trim().toUpperCase();
    if (!tracked) continue;
    const requestSymbol = normalizeQuoteRequestSymbol(tracked);
    if (!requestSymbol) continue;
    if (!quoteToTrackedMap.has(requestSymbol)) quoteToTrackedMap.set(requestSymbol, new Set());
    quoteToTrackedMap.get(requestSymbol).add(tracked);
    quoteToTrackedMap.get(requestSymbol).add(requestSymbol);
  }
  const symbols = Array.from(quoteToTrackedMap.keys());
  if (!symbols.length) return;
  const now = Date.now();
  if (now - lastQuotesAt < 25000) return;
  lastQuotesAt = now;
  try {
    const q = await api('/api/quotes', {
      method: 'POST',
      body: JSON.stringify({ symbols }),
    });
    const incomingPrices = Object.entries(q.prices || {});
    const returnedSymbols = new Set(incomingPrices.map(([symbol]) => String(symbol || '').toUpperCase()));
    lastQuoteRequestSymbols = symbols.slice();
    lastQuoteMissingSymbols = symbols.filter((symbol) => !returnedSymbols.has(symbol));
    lastQuoteMissingUpdatedAtMs = Date.now();
    const previousQuoteMap = new Map(latestQuoteMap);
    let hasNewerQuote = false;
    for (const [symbolRaw, priceRaw] of incomingPrices) {
      const symbol = String(symbolRaw || '').toUpperCase();
      const price = Number(priceRaw);
      if (!Number.isFinite(price)) continue;
      const newTs = Number((q.exchange_ts_ms || {})[symbol]);
      const targets = quoteToTrackedMap.get(symbol) || new Set([symbol]);

      for (const targetSymbol of targets) {
        const prevTs = Number(latestQuoteExchangeTsMap.get(targetSymbol));
        if (!Number.isFinite(prevTs) || (Number.isFinite(newTs) && newTs > prevTs)) {
          hasNewerQuote = true;
        }
        latestQuoteMap.set(targetSymbol, price);

        const prevPrice = Number(previousQuoteMap.get(targetSymbol));
        if (Number.isFinite(prevPrice) && prevPrice > 0) {
          const delta = price - prevPrice;
          latestQuoteDeltaMap.set(targetSymbol, {
            delta,
            pct: (delta / prevPrice) * 100,
          });
        }

        if (Number.isFinite(newTs) && newTs > 0) {
          latestQuoteExchangeTsMap.set(targetSymbol, newTs);
        }
      }
    }

    if (hasNewerQuote) {
      const now = new Date();
      lastQuotesStampText = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
      const exchangeMsValues = Array.from(latestQuoteExchangeTsMap.values())
        .map((v) => Number(v))
        .filter((v) => Number.isFinite(v) && v > 0);
      if (exchangeMsValues.length > 0) {
        const maxExchangeTs = Math.max(...exchangeMsValues);
        const exchangeDate = new Date(maxExchangeTs);
        lastExchangeStampText = `${String(exchangeDate.getHours()).padStart(2, '0')}:${String(exchangeDate.getMinutes()).padStart(2, '0')}:${String(exchangeDate.getSeconds()).padStart(2, '0')}`;
      }
    }
    renderQuotesAsOf();
  } catch {
    // Keep previous quote map silently
  }
};

document.getElementById('runPortfolio').addEventListener('click', async () => {
  try {
    await withBusy('runPortfolio', 'Optimizing...', 'Portfolio updated', async () => {
      const symbols = document.getElementById('pSymbols').value.split(',').map(s => s.trim()).filter(Boolean);
      const alloc = await api('/api/portfolio', { method: 'POST', body: JSON.stringify({ symbols }) });
      lastPortfolio = alloc;
      document.getElementById('pSymbols').value = symbols.join(',');
      await refreshRealtimeQuotes();
      const paper = await api('/api/paper/status');
      fillAssetTable(alloc, paper);
    });
  } catch (e) { alert(e.message); }
});

const paperChart = createChartCompat('paperChart');
const portfolioLine = paperChart.addLineSeries({ color: '#00d4aa', lineWidth: 2 });
const benchmarkLine = paperChart.addLineSeries({ color: '#f59e0b', lineWidth: 2 });
attachChartAutoResize(paperChart);

const futuChart = createChartCompat('futuChart');
const futuPortfolioLine = futuChart.addLineSeries({ color: '#00d4aa', lineWidth: 2 });
const futuStrategyFtLine = futuChart.addLineSeries({ color: '#5b9cf6', lineWidth: 2, lineStyle: 2 });
const futuBenchmarkLine = futuChart.addLineSeries({ color: '#f59e0b', lineWidth: 2 });
attachChartAutoResize(futuChart);

const backtestChart = createChartCompat('backtestChart');
const backtestPortfolioLine = backtestChart.addLineSeries({ color: '#00d4aa', lineWidth: 2 });
const backtestBenchmarkLine = backtestChart.addLineSeries({ color: '#f59e0b', lineWidth: 2 });
attachChartAutoResize(backtestChart);

for (const btn of tradeFilterButtons) {
  btn.addEventListener('click', () => {
    for (const b of tradeFilterButtons) b.classList.remove('active');
    btn.classList.add('active');
    selectedTradeFilter = String(btn.dataset.tradeFilter || 'all');
    renderTradeHistory();
  });
}

if (tradeSearchInput) {
  tradeSearchInput.addEventListener('input', () => {
    tradeSearchText = tradeSearchInput.value || '';
    renderTradeHistory();
  });
}

if (futuActivityFilterInput) {
  futuActivityFilterInput.addEventListener('input', () => {
    futuActivityFilterText = futuActivityFilterInput.value || '';
    renderFutuOpenOrdersTable(latestFutuStatus || {});
    renderFutuCancelHistoryTable(latestFutuStatus || {});
    renderFutuTradeHistory(latestFutuStatus || {});
  });
}

if (futuActivityRangeSelect) {
  futuActivityRangeSelect.addEventListener('change', async () => {
    futuActivityRangeDays = Number(futuActivityRangeSelect.value || 30);
    const capRaw = Number(futuCapitalCapInput?.value);
    const cap = Number.isFinite(capRaw) && capRaw > 0 ? capRaw : null;
    try {
      await api('/api/futu/activity-config', {
        method: 'POST',
        body: JSON.stringify({
          history_order_range_days: futuActivityRangeDays,
          rebalance_capital_limit_usd: cap,
        }),
      });
    } catch (error) {
      setStatus(`FUTU activity range update failed: ${error.message}`, 'err');
    }
    renderFutuOpenOrdersTable(latestFutuStatus || {});
    renderFutuCancelHistoryTable(latestFutuStatus || {});
    renderFutuTradeHistory(latestFutuStatus || {});
  });
}

if (futuCapitalCapApplyBtn) {
  futuCapitalCapApplyBtn.addEventListener('click', async () => {
    const capRaw = Number(futuCapitalCapInput?.value);
    const cap = Number.isFinite(capRaw) && capRaw > 0 ? capRaw : null;
    futuCapitalCapApplyBtn.disabled = true;
    const oldText = futuCapitalCapApplyBtn.textContent;
    futuCapitalCapApplyBtn.textContent = 'Applying...';
    try {
      await api('/api/futu/activity-config', {
        method: 'POST',
        body: JSON.stringify({
          history_order_range_days: futuActivityRangeDays,
          rebalance_capital_limit_usd: cap,
        }),
      });
      if (futuCapitalCapHint) {
        futuCapitalCapHint.textContent = cap == null
          ? 'Current rebalance cap: Unlimited'
          : `Current rebalance cap: $${cap.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
      }
      setStatus('FUTU rebalance capital limit updated', 'ok');
      await refreshFutu();
    } catch (error) {
      setStatus(`FUTU rebalance cap update failed: ${error.message}`, 'err');
      alert(error.message);
    } finally {
      futuCapitalCapApplyBtn.disabled = false;
      futuCapitalCapApplyBtn.textContent = oldText || '💰 Apply Cap';
    }
  });
}

if (futuStrategyCapitalApplyBtn) {
  futuStrategyCapitalApplyBtn.addEventListener('click', async () => {
    const capRaw = Number(futuStrategyCapitalInput?.value);
    if (!Number.isFinite(capRaw) || capRaw <= 0) {
      setStatus('FUTU strategy startup capital must be > 0', 'err');
      return;
    }
    futuStrategyCapitalApplyBtn.disabled = true;
    const oldText = futuStrategyCapitalApplyBtn.textContent;
    futuStrategyCapitalApplyBtn.textContent = 'Applying...';
    try {
      await api('/api/futu/strategy-capital', {
        method: 'POST',
        body: JSON.stringify({
          strategy_start_capital_usd: capRaw,
        }),
      });
      if (futuStrategyCapitalHint) {
        futuStrategyCapitalHint.textContent = `Strategy startup capital: $${capRaw.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
      }
      setStatus('FUTU strategy startup capital updated', 'ok');
      await refreshFutu();
    } catch (error) {
      setStatus(`FUTU strategy startup capital update failed: ${error.message}`, 'err');
      alert(error.message);
    } finally {
      futuStrategyCapitalApplyBtn.disabled = false;
      futuStrategyCapitalApplyBtn.textContent = oldText || '🧮 Apply Strategy Capital';
    }
  });
}

if (futuManualSubmitBtn) {
  futuManualSubmitBtn.addEventListener('click', async () => {
    if (futuManualSubmitBtn.disabled) return;
    const selectedEnv = String(latestFutuStatus?.selected_trd_env || '').toUpperCase();
    if (selectedEnv === 'REAL') {
      const confirmed = window.confirm('REAL account context is selected. Live orders are still blocked by the safety gate in this build. Continue only to validate the form and see the safety warning?');
      if (!confirmed) return;
      setFutuManualOrderHint('REAL account selected. Order form validated, but live order submission remains blocked by the safety gate.', 'err');
      setStatus('REAL trading is still blocked by the safety gate', 'err');
      return;
    }
    futuManualSubmitBtn.disabled = true;
    const prev = futuManualSubmitBtn.textContent;
    futuManualSubmitBtn.textContent = 'Submitting...';
    try {
      const result = await submitFutuManualOrder();
      setFutuManualOrderHint(
        `Submitted ${result.side || '--'} ${result.symbol || '--'} x ${Number(result.quantity || 0).toFixed(2)} @ ${result.price == null ? '--' : Number(result.price).toFixed(4)} · order ${result.order_id || '--'}`,
        'ok',
      );
      setStatus('FUTU manual order submitted', 'ok');
      await refreshFutu();
    } catch (e) {
      const msg = e?.message || String(e);
      setFutuManualOrderHint(`Manual order failed: ${msg}`, 'err');
      setStatus(msg, 'err');
      alert(msg);
    } finally {
      futuManualSubmitBtn.disabled = false;
      futuManualSubmitBtn.textContent = prev;
    }
  });
}

document.addEventListener('click', (event) => {
  const btn = event?.target?.closest?.('.futu-manual-fill-btn');
  if (!btn) return;
  const symbol = String(btn.getAttribute('data-symbol') || '').trim().toUpperCase();
  const price = String(btn.getAttribute('data-price') || '').trim();
  if (futuManualSymbolInput && symbol) futuManualSymbolInput.value = symbol;
  if (futuManualPriceInput && price) futuManualPriceInput.value = price;
  if (futuManualQtyInput && (!futuManualQtyInput.value || Number(futuManualQtyInput.value) <= 0)) {
    futuManualQtyInput.value = '1';
  }
  setFutuManualOrderHint(`Manual form prefilled from holding ${symbol || '--'}.`);
});

document.addEventListener('click', async (event) => {
  const cancelBtn = event?.target?.closest?.('.futu-order-cancel-btn');
  if (cancelBtn) {
    if (cancelBtn.disabled) return;
    const orderId = String(cancelBtn.getAttribute('data-order-id') || '').trim();
    if (!orderId) return;
    const confirmed = window.confirm(`Cancel order ${orderId}?`);
    if (!confirmed) return;
    cancelBtn.disabled = true;
    const prevText = cancelBtn.textContent;
    cancelBtn.textContent = 'Canceling...';
    try {
      await submitFutuModifyOrder({ orderId, modifyOrderOp: 'CANCEL' });
      setStatus(`FUTU order canceled: ${orderId}`, 'ok');
      await refreshFutu();
    } catch (e) {
      const msg = e?.message || String(e);
      setStatus(msg, 'err');
      alert(msg);
    } finally {
      cancelBtn.disabled = false;
      cancelBtn.textContent = prevText;
    }
    return;
  }

  const changeBtn = event?.target?.closest?.('.futu-order-change-btn');
  if (changeBtn) {
    if (changeBtn.disabled) return;
    const orderId = String(changeBtn.getAttribute('data-order-id') || '').trim();
    const currentQty = Number(changeBtn.getAttribute('data-qty'));
    const currentPrice = Number(changeBtn.getAttribute('data-price'));
    if (!orderId) return;

    futuOpenOrderEditState = {
      orderId,
      qty: Number.isFinite(currentQty) && currentQty > 0 ? String(currentQty) : '1',
      price: Number.isFinite(currentPrice) && currentPrice > 0 ? String(currentPrice) : '',
    };
    renderFutuOpenOrdersTable(latestFutuStatus || {});
    return;
  }

  const abortBtn = event?.target?.closest?.('.futu-order-edit-cancel-btn');
  if (abortBtn) {
    futuOpenOrderEditState = { orderId: '', qty: '', price: '' };
    renderFutuOpenOrdersTable(latestFutuStatus || {});
    return;
  }

  const saveBtn = event?.target?.closest?.('.futu-order-edit-save-btn');
  if (!saveBtn) return;
  if (saveBtn.disabled) return;

  const orderId = String(saveBtn.getAttribute('data-order-id') || '').trim();
  if (!orderId) return;

  const host = saveBtn.closest('tr');
  const qtyInputEl = host?.querySelector('.futu-order-edit-qty');
  const priceInputEl = host?.querySelector('.futu-order-edit-price');
  const qty = Number(String(qtyInputEl?.value || '').trim());
  const price = Number(String(priceInputEl?.value || '').trim());

  futuOpenOrderEditState.qty = String(qtyInputEl?.value || '').trim();
  futuOpenOrderEditState.price = String(priceInputEl?.value || '').trim();

  saveBtn.disabled = true;
  const prevText = saveBtn.textContent;
  saveBtn.textContent = 'Saving...';
  try {
    await submitFutuModifyOrder({ orderId, modifyOrderOp: 'NORMAL', qty, price });
    futuOpenOrderEditState = { orderId: '', qty: '', price: '' };
    setStatus(`FUTU order changed: ${orderId}`, 'ok');
    await refreshFutu();
  } catch (e) {
    const msg = e?.message || String(e);
    setStatus(msg, 'err');
    alert(msg);
  } finally {
    saveBtn.disabled = false;
    saveBtn.textContent = prevText;
  }
});

for (const btn of paperRangeButtons) {
  btn.addEventListener('click', () => {
    const days = Number(btn.dataset.paperRangeDays);
    if (!Number.isFinite(days) || days <= 0) return;
    selectedPaperRangeDays = days;
    renderPaperChartFromCurrentContext();
    renderChartSummaryStrip();

    const series = paperFullContext?.portfolioSeries || [];
    if (series.length >= 2) {
      const spanDays = (series[series.length - 1].time - series[0].time) / 86400;
      if (Number.isFinite(spanDays) && spanDays + 0.01 < days) {
        setStatus(`Selected ${days}d range; available history is ${spanDays.toFixed(1)}d`, '');
      }
    }
  });
}

for (const btn of backtestRangeButtons) {
  btn.addEventListener('click', () => {
    const days = Number(btn.dataset.backtestRangeDays);
    if (!Number.isFinite(days) || days < 0) return;
    selectedBacktestRangeDays = days;
    renderBacktestChartFromCurrentContext();
    renderBacktestKpis(latestBacktestStatus);
  });
}

for (const btn of futuRangeButtons) {
  btn.addEventListener('click', () => {
    const days = Number(btn.dataset.futuRangeDays);
    if (!Number.isFinite(days) || days <= 0) return;
    selectedFutuRangeDays = days;
    renderFutuChartFromCurrentContext();
    renderFutuChartSummaryStrip();
  });
}

for (const btn of futuCurveModeButtons) {
  btn.addEventListener('click', () => {
    const mode = String(btn.dataset.futuCurveMode || '').trim().toLowerCase();
    if (mode !== 'account' && mode !== 'strategy') return;
    selectedFutuCurveMode = mode;
    for (const b of futuCurveModeButtons) {
      b.classList.toggle('active', String(b.dataset.futuCurveMode || '').trim().toLowerCase() === mode);
    }
    syncFutuLegendModeLabels();
    futuFullContext = buildFutuSeriesContext(latestFutuStatus || {});
    renderFutuChartFromCurrentContext();
    renderFutuChartSummaryStrip();
    if (latestFutuStatus) {
      renderFutuConnectionKpis(latestFutuStatus, futuFullContext);
    }
  });
}

if (paperChart?.container) {
  paperChart.container.addEventListener('mouseenter', showPaperLegend);
  paperChart.container.addEventListener('mouseleave', hidePaperLegend);
}

if (paperChart?.chart && typeof paperChart.chart.subscribeCrosshairMove === 'function') {
  paperChart.chart.subscribeCrosshairMove((param) => {
    if (!param || !param.time) return;
    const t = typeof param.time === 'number'
      ? param.time
      : (typeof param.time?.timestamp === 'number' ? param.time.timestamp : null);
    if (t == null) return;

    const metrics = paperMetricsByTime.get(t);
    if (metrics) {
      setLegendText(metrics);
      showPaperLegend();
    }
  });
}

if (futuChart?.container) {
  futuChart.container.addEventListener('mouseenter', showFutuLegend);
  futuChart.container.addEventListener('mouseleave', hideFutuLegend);
}

if (futuChart?.chart && typeof futuChart.chart.subscribeCrosshairMove === 'function') {
  futuChart.chart.subscribeCrosshairMove((param) => {
    if (!param || !param.time) return;
    const t = typeof param.time === 'number'
      ? param.time
      : (typeof param.time?.timestamp === 'number' ? param.time.timestamp : null);
    if (t == null) return;

    const metrics = futuMetricsByTime.get(t);
    if (metrics) {
      setFutuLegendText(metrics);
      showFutuLegend();
    }
  });
}

if (backtestChart?.container) {
  backtestChart.container.addEventListener('mouseenter', showBacktestLegend);
  backtestChart.container.addEventListener('mouseleave', hideBacktestLegend);
}

if (backtestChart?.chart && typeof backtestChart.chart.subscribeCrosshairMove === 'function') {
  backtestChart.chart.subscribeCrosshairMove((param) => {
    if (!param || !param.time) return;
    const t = typeof param.time === 'number'
      ? param.time
      : (typeof param.time?.timestamp === 'number' ? param.time.timestamp : null);
    if (t == null) return;

    const metrics = backtestMetricsByTime.get(t);
    if (metrics) {
      setBacktestLegendText(metrics);
      showBacktestLegend();
    }
  });
}

window.addEventListener('resize', () => {
  fChart.resize();
  paperChart.resize();
  futuChart.resize();
  backtestChart.resize();
});

const setPaperStatusChip = (status) => {
  const chip = document.getElementById('paperStatusChip');
  const dot = document.getElementById('systemDot');
  const ctrlStatus = document.getElementById('paperCtrlStatus');
  const ctrlDot = document.getElementById('paperCtrlDot');
  const badge = document.getElementById('paperTabBadge');
  if (!status.running) {
    chip.textContent = 'IDLE';
    if (ctrlStatus) ctrlStatus.textContent = 'IDLE';
    if (dot) { dot.classList.remove('active', 'paused'); }
    if (ctrlDot) { ctrlDot.classList.remove('active', 'paused'); }
    if (badge) { badge.classList.remove('running', 'paused'); }
    paperSessionStartMs = null;
  } else if (status.paused) {
    chip.textContent = 'PAUSED';
    if (ctrlStatus) ctrlStatus.textContent = 'PAUSED';
    if (dot) { dot.classList.remove('active'); dot.classList.add('paused'); }
    if (ctrlDot) { ctrlDot.classList.remove('active'); ctrlDot.classList.add('paused'); }
    if (badge) { badge.classList.remove('running'); badge.classList.add('paused'); }
  } else {
    chip.textContent = 'RUNNING';
    if (ctrlStatus) ctrlStatus.textContent = 'RUNNING';
    if (dot) { dot.classList.remove('paused'); dot.classList.add('active'); }
    if (ctrlDot) { ctrlDot.classList.remove('paused'); ctrlDot.classList.add('active'); }
    if (badge) { badge.classList.remove('paused'); badge.classList.add('running'); }
    if (!paperSessionStartMs) paperSessionStartMs = Date.now();
  }

  // Next run chip
  const nextRunChip = document.getElementById('nextRunChip');
  if (nextRunChip) {
    const retryAtText = status?.auto_opt_retry_next_at;
    const retryCount = Number(status?.auto_opt_retry_count || 0);
    const retryMax = Number(status?.auto_opt_retry_max || 10);
    if (status.running && retryAtText) {
      const retryAt = new Date(retryAtText);
      const now = new Date();
      const remainSec = Math.max(0, Math.ceil((retryAt.getTime() - now.getTime()) / 1000));
      const mm = String(Math.floor(remainSec / 60)).padStart(2, '0');
      const ss = String(remainSec % 60).padStart(2, '0');
      nextRunChip.textContent = `Retry ${retryCount}/${retryMax} in ${mm}:${ss}`;
    } else if (status.running && !status.paused && status.next_run_at) {
      nextRunChip.textContent = `Next: ${status.next_run_at}`;
    } else if (status.running && !status.paused) {
      const t1 = document.getElementById('paperTime1')?.value || '--';
      const t2 = document.getElementById('paperTime2')?.value || '--';
      nextRunChip.textContent = `Sched: ${t1} / ${t2}`;
    } else {
      nextRunChip.textContent = 'Next: --';
    }
  }
};

const renderChartSummaryStrip = () => {
  const strip = document.getElementById('chartSummaryStrip');
  if (!strip) return;

  const latest = paperFullContext?.latest;
  const series = paperFullContext?.portfolioSeries || [];
  if (!latest || series.length === 0) {
    strip.style.display = 'none';
    return;
  }

  const nav = Number(latest.portfolioValue);
  const pnlPct = Number(latest.portfolioPnlPct);
  const spreadPct = Number(latest.spreadPct);

  // compute session daily change from last two points
  let dailyChange = null;
  let dailyChangePct = null;
  if (series.length >= 2) {
    const cur = Number(series[series.length - 1]?.value);
    const prev = Number(series[series.length - 2]?.value);
    if (Number.isFinite(cur) && Number.isFinite(prev) && prev > 0) {
      dailyChange = cur - prev;
      dailyChangePct = ((cur - prev) / prev) * 100;
    }
  }

  const upDown = (v) => Number.isFinite(v) ? (v >= 0 ? 'up' : 'down') : '';

  strip.style.display = '';
  strip.innerHTML = `
    <div class='strip-item'>
      <span class='strip-label'>NAV</span>
      <span class='strip-val'>${Number.isFinite(nav) ? '$' + nav.toFixed(2) : '--'}</span>
    </div>
    <div class='strip-item'>
      <span class='strip-label'>PnL</span>
      <span class='strip-val ${upDown(pnlPct)}'>${Number.isFinite(pnlPct) ? `${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(2)}%` : '--'}</span>
    </div>
    <div class='strip-item'>
      <span class='strip-label'>Last \u0394</span>
      <span class='strip-val ${upDown(dailyChange)}'>${Number.isFinite(dailyChange) ? `${dailyChange >= 0 ? '+' : ''}$${dailyChange.toFixed(2)} (${dailyChangePct.toFixed(2)}%)` : '--'}</span>
    </div>
    <div class='strip-item'>
      <span class='strip-label'>vs Bench</span>
      <span class='strip-val ${upDown(spreadPct)}'>${Number.isFinite(spreadPct) ? `${spreadPct >= 0 ? '+' : ''}${spreadPct.toFixed(2)}%` : '--'}</span>
    </div>
  `;
};

const refreshPaper = async () => {
  try {
    const st = await api('/api/paper/status');
    latestPaperStatus = st;
    setDataSourceChip(
      st?.data_live_source,
      !!st?.data_ws_connected,
      st?.data_ws_diagnostics,
      st?.data_live_fetch_diagnostics,
    );
    setPaperApplyAutoOptimizing(!!st?.auto_optimizing);
    hydratePaperOptimizationFromStatus(st);
    setPaperStatusChip(st);

    const paperExecDot = document.getElementById('paperExecDot');
    const paperExecStatus = document.getElementById('paperExecStatus');
    const paperExecCapital = document.getElementById('paperExecCapital');
    const paperExecSchedule = document.getElementById('paperExecSchedule');
    const paperExecNextOpt = document.getElementById('paperExecNextOpt');
    const paperExecPool = document.getElementById('paperExecPool');
    if (paperExecStatus) {
      if (st.running && st.paused) {
        paperExecStatus.textContent = 'PAUSED';
        paperExecDot?.classList.remove('active');
        paperExecDot?.classList.add('paused');
      } else if (st.running) {
        paperExecStatus.textContent = 'RUNNING';
        paperExecDot?.classList.remove('paused');
        paperExecDot?.classList.add('active');
      } else {
        paperExecStatus.textContent = 'IDLE';
        paperExecDot?.classList.remove('active');
        paperExecDot?.classList.remove('paused');
      }
    }
    if (paperExecCapital) {
      const capVal = Number(document.getElementById('paperCapital')?.value) || 0;
      paperExecCapital.textContent = capVal > 0 ? capVal.toLocaleString() : '--';
    }
    if (paperExecSchedule) {
      const t1 = document.getElementById('paperTime1')?.value || '--';
      const t2 = document.getElementById('paperTime2')?.value || '--';
      paperExecSchedule.textContent = `${t1} / ${t2}`;
    }
    if (paperExecNextOpt) {
      paperExecNextOpt.textContent = document.getElementById('paperCtrlNextOpt')?.textContent || '--';
    }
    if (paperExecPool) {
      paperExecPool.textContent = document.getElementById('paperCtrlPool')?.textContent || '0 symbols';
    }

    syncPaperButtons(st);
    hydratePaperTargetsFromStatus(st);
    renderStrategyDispatchPreview();

    const nextPaperSig = buildSnapshotDataSignature(st);
    if (nextPaperSig !== lastPaperChartDataSignature) {
      const ctx = buildPaperSeriesContext(st.snapshots || []);
      paperFullContext = ctx.portfolioSeries.length > 0 ? ctx : buildFallbackPaperContext(st.latest_snapshot);
      renderPaperChartFromCurrentContext();
      renderChartSummaryStrip();
      lastPaperChartDataSignature = nextPaperSig;
    }
    const fallbackSymbols = [
      ...(lastPortfolio?.asset_forecasts || []).map((x) => x.symbol),
      ...(document.getElementById('pSymbols')?.value || '')
        .split(',')
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean),
    ];
    renderRealtimeMarketGrid('rtMarketGrid', st.latest_snapshot, fallbackSymbols);

    const logBox = document.getElementById('logBox');
    if (logBox) {
      const escapeHtml = (value) => String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
      logBox.innerHTML = (st.logs || []).slice(-20).map((x) => {
        const text = String(x || '');
        const lower = text.toLowerCase();
        const cls = lower.includes('error:')
          ? 'log-error'
          : (lower.includes('warning:') || lower.includes('[warning]') || lower.includes('skip')
            ? 'log-warn'
            : '');
        return `<div class="${cls}">${escapeHtml(text)}</div>`;
      }).join('');
    }
    ingestPaperTrades(st);
    renderTradeHistory();
    setPaperTradeMarkers();
    fillHoldingsTable(st);
    fillCapitalSummaryTable(st);
    renderPaperKpis(st);
    renderPaperRiskAlerts(st);

    if (lastPortfolio) fillAssetTable(lastPortfolio, st);
  } catch (e) {
    console.warn(e);
  }
};

setInterval(refreshPaper, 4000);

const refreshFutu = async () => {
  try {
    normalizeFutuTerminalSections();
    const st = await api('/api/futu/status');
    if (!latestPaperStatus || !Array.isArray(latestPaperStatus?.target_weights) || latestPaperStatus.target_weights.length === 0) {
      try {
        latestPaperStatus = await api('/api/paper/status');
      } catch {
        // ignore; FUTU table will fallback to '--' when paper status unavailable
      }
    }
    latestFutuStatus = st;

    const futuExecDot = document.getElementById('futuExecDot');
    const futuExecStatus = document.getElementById('futuExecStatus');
    const futuExecCapital = document.getElementById('futuExecCapital');
    const futuRealModeBadge = document.getElementById('futuRealModeBadge');
    const futuRealSafetyHint = document.getElementById('futuRealSafetyHint');
    const futuModeCardSubtitle = document.getElementById('futuModeCardSubtitle');
    const futuModeHeroCard = document.getElementById('futuModeHeroCard');
    const futuModeHeroTitle = document.getElementById('futuModeHeroTitle');
    const futuModeHeroText = document.getElementById('futuModeHeroText');
    const futuModeSafetyCard = document.getElementById('futuModeSafetyCard');
    const futuModeSafetyTitle = document.getElementById('futuModeSafetyTitle');
    const futuModeSafetyText = document.getElementById('futuModeSafetyText');
    const futuModeRouteCard = document.getElementById('futuModeRouteCard');
    const futuModeRouteTitle = document.getElementById('futuModeRouteTitle');
    const futuModeRouteText = document.getElementById('futuModeRouteText');
    const futuLiveGateCard = document.getElementById('futuLiveGateCard');
    const futuLiveGateTitle = document.getElementById('futuLiveGateTitle');
    const futuLiveGateText = document.getElementById('futuLiveGateText');
    const futuManualOrderPanel = document.getElementById('futuManualOrderPanel');
    const futuManualGuard = document.getElementById('futuManualGuard');
    const futuExecPool = document.getElementById('futuExecPool');
    const futuConnStatus = document.getElementById('futuConnStatus');
    const futuConnStatusCaption = document.getElementById('futuConnStatusCaption');
    const futuConnSummary = document.getElementById('futuConnSummary');
    const futuConnMarket = document.getElementById('futuConnMarket');
    const futuConnFirm = document.getElementById('futuConnFirm');
    const futuConnHost = document.getElementById('futuConnHost');
    const futuAccountCash = document.getElementById('futuAccountCash');
    const futuBuyingPower = document.getElementById('futuBuyingPower');
    const futuLastSync = document.getElementById('futuLastSync');
    const futuConnDot = document.getElementById('futuConnDot');
    const futuConnCard = document.getElementById('futuConnCard');
    const futuCashCard = document.getElementById('futuCashCard');
    const futuBuyingPowerCard = document.getElementById('futuBuyingPowerCard');
    const futuAccPill = document.getElementById('futuAccPill');
    const futuAccIdBadge = document.getElementById('futuAccIdBadge');
    const futuAccountId = document.getElementById('futuAccountId');
    const futuAccountMeta = document.getElementById('futuAccountMeta');
    const futuAccType = document.getElementById('futuAccType');
    const futuAccEnvVal = document.getElementById('futuAccEnvVal');
    const futuAccStatusVal = document.getElementById('futuAccStatusVal');
    const futuAccRoleVal = document.getElementById('futuAccRoleVal');
    const futuAccSimTypeVal = document.getElementById('futuAccSimTypeVal');
    const futuAccMarketAuthVal = document.getElementById('futuAccMarketAuthVal');
    const futuAccFirmVal = document.getElementById('futuAccFirmVal');
    const futuAccCardVal = document.getElementById('futuAccCardVal');
    const futuAccUniCardVal = document.getElementById('futuAccUniCardVal');
    const futuStartedAt = document.getElementById('futuStartedAt');
    const futuCtrlDot = document.getElementById('futuCtrlDot');
    const futuTabBadge = document.getElementById('futuTabBadge');
    const futuCtrlStatus = document.getElementById('futuCtrlStatus');
    const futuCtrlCapital = document.getElementById('futuCtrlCapital');
    const futuCtrlAccount = document.getElementById('futuCtrlAccount');
    const futuCtrlRuntime = document.getElementById('futuCtrlRuntime');

    syncFutuButtons(st);

    if (st.connected) {
      futuExecStatus.textContent = 'RUNNING';
      futuExecDot?.classList.remove('paused');
      futuExecDot?.classList.add('active');
      futuConnDot?.classList.add('active');
      if (futuConnStatus) futuConnStatus.textContent = 'CONNECTED';
      if (futuConnStatusCaption) futuConnStatusCaption.textContent = 'Gateway linked. Account balances, holdings and execution telemetry are flowing in.';
      if (futuConnSummary) futuConnSummary.textContent = 'Futu API connected · holdings syncing';
      if (futuConnCard) { futuConnCard.classList.remove('kpi-warn'); futuConnCard.classList.add('kpi-positive'); }
    } else {
      futuExecStatus.textContent = 'DISCONNECTED';
      futuExecDot?.classList.remove('active');
      futuExecDot?.classList.remove('paused');
      futuConnDot?.classList.remove('active');
      if (futuConnStatus) futuConnStatus.textContent = 'DISCONNECTED';
      if (futuConnStatusCaption) futuConnStatusCaption.textContent = 'Gateway offline. Connect OpenD to unlock account context and live session metrics.';
      if (futuConnSummary) futuConnSummary.textContent = 'Waiting for Futu API or reconnecting';
      if (futuConnCard) { futuConnCard.classList.remove('kpi-positive'); futuConnCard.classList.add('kpi-warn'); }
    }

    if (futuCtrlStatus) futuCtrlStatus.textContent = st.running ? (st.connected ? 'RUNNING' : 'STARTED') : 'STOPPED';
    if (futuCtrlDot) {
      futuCtrlDot.classList.toggle('active', !!st.running && !!st.connected);
      futuCtrlDot.classList.toggle('paused', !!st.running && !st.connected);
    }
    if (futuTabBadge) {
      if (!st.running) {
        futuTabBadge.classList.remove('running', 'paused');
      } else if (!st.connected) {
        futuTabBadge.classList.remove('running');
        futuTabBadge.classList.add('paused');
      } else {
        futuTabBadge.classList.remove('paused');
        futuTabBadge.classList.add('running');
      }
    }
    if (futuStrategyCapitalHint) {
      const strategyCap = Number(st?.strategy_start_capital_usd);
      futuStrategyCapitalHint.textContent = Number.isFinite(strategyCap) && strategyCap > 0
        ? `Strategy startup capital: $${strategyCap.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
        : 'Strategy startup capital: --';
    }
    if (futuCtrlCapital) {
      const acctNav = Number(st?.latest_snapshot?.total_value);
      const stratNav = Number(st?.latest_strategy_snapshot?.total_value);
      if (Number.isFinite(acctNav) && Number.isFinite(stratNav)) {
        futuCtrlCapital.textContent = `Account NAV ${acctNav.toLocaleString(undefined, { maximumFractionDigits: 2 })} · Strategy NAV ${stratNav.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
      } else {
        futuCtrlCapital.textContent = Number.isFinite(acctNav) ? acctNav.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '--';
      }
    }

    const selectedAcc = (st?.selected_account && typeof st.selected_account === 'object') ? st.selected_account : null;
    const selectedAccId = st?.selected_acc_id ?? selectedAcc?.acc_id ?? '--';
    const preferredAccId = String(st?.preferred_acc_id || selectedAccId || '').trim();
    const selectedEnv = st?.selected_trd_env ?? selectedAcc?.trd_env ?? '--';
    const isRealMode = String(selectedEnv || '').toUpperCase() === 'REAL';
    const selectedMarket = st?.selected_market ?? st?.conn_market ?? '--';
    const connHost = String(st?.conn_host || '127.0.0.1');
    const connPort = Number(st?.conn_port);
    const connHostPort = Number.isFinite(connPort) && connPort > 0 ? `${connHost}:${connPort}` : connHost;
    const marketAuth = Array.isArray(selectedAcc?.trdmarket_auth)
      ? selectedAcc.trdmarket_auth.join(', ')
      : (selectedAcc?.trdmarket_auth ? String(selectedAcc.trdmarket_auth) : '--');
    if (futuAccountId) futuAccountId.textContent = String(selectedAccId);
    if (futuAccountMeta) futuAccountMeta.textContent = `${String(selectedEnv || '--').toUpperCase()} · ${selectedAcc?.acc_type ?? '--'}`;
    if (futuAccType) futuAccType.textContent = `${selectedEnv} · ${selectedAcc?.acc_type ?? '--'}`;
    if (futuAccEnvVal) futuAccEnvVal.textContent = `Env: ${selectedEnv}`;
    if (futuAccStatusVal) futuAccStatusVal.textContent = `Status: ${selectedAcc?.acc_status ?? '--'}`;
    if (futuAccRoleVal) futuAccRoleVal.textContent = `Role: ${selectedAcc?.acc_role ?? '--'}`;
    if (futuAccSimTypeVal) futuAccSimTypeVal.textContent = `Sim Type: ${selectedAcc?.sim_acc_type ?? '--'}`;
    if (futuAccMarketAuthVal) futuAccMarketAuthVal.textContent = `Market Auth: ${marketAuth}`;
    if (futuAccFirmVal) futuAccFirmVal.textContent = `Firm: ${selectedAcc?.security_firm ?? '--'}`;
    if (futuAccCardVal) futuAccCardVal.textContent = `Card: ${selectedAcc?.card_num ?? '--'}`;
    if (futuAccUniCardVal) futuAccUniCardVal.textContent = `Uni Card: ${selectedAcc?.uni_card_num ?? '--'}`;
    if (futuConnHost) futuConnHost.textContent = connHostPort;
    if (futuConnMarket) futuConnMarket.textContent = selectedMarket;
    if (futuConnFirm) futuConnFirm.textContent = String(st?.conn_security_firm || selectedAcc?.security_firm || 'FutuSecurities');
    if (futuAccPill) { futuAccPill.style.display = selectedAccId && selectedAccId !== '--' ? '' : 'none'; }
    if (futuAccIdBadge) futuAccIdBadge.textContent = `#${selectedAccId}`;
    if (futuCtrlAccount) futuCtrlAccount.textContent = `${selectedEnv}/${selectedMarket} · #${selectedAccId}`;
    if (futuTabBadge) {
      futuTabBadge.textContent = isRealMode ? 'REAL' : 'SIM';
      futuTabBadge.classList.toggle('mode-real', isRealMode);
      futuTabBadge.classList.toggle('mode-sim', !isRealMode);
    }
    if (futuRealModeBadge) {
      futuRealModeBadge.textContent = isRealMode ? 'REAL MODE' : 'SIM MODE';
      futuRealModeBadge.classList.toggle('sim', !isRealMode);
    }
    if (futuRealSafetyHint) {
      futuRealSafetyHint.textContent = isRealMode
        ? 'REAL ORDER DISABLED · PLACEHOLDER ONLY'
        : 'SIM ORDERS ACTIVE · SAFE SANDBOX';
      futuRealSafetyHint.classList.toggle('sim', !isRealMode);
    }
    if (futuModeHeroCard) {
      futuModeHeroCard.classList.toggle('mode-real', isRealMode);
      futuModeHeroCard.classList.toggle('mode-sim', !isRealMode);
    }
    if (futuModeSafetyCard) {
      futuModeSafetyCard.classList.toggle('mode-real', isRealMode);
      futuModeSafetyCard.classList.toggle('mode-sim', !isRealMode);
    }
    if (futuModeRouteCard) {
      futuModeRouteCard.classList.toggle('mode-real', isRealMode);
      futuModeRouteCard.classList.toggle('mode-sim', !isRealMode);
    }
    if (futuModeHeroTitle) {
      futuModeHeroTitle.textContent = isRealMode ? 'REAL ROUTE' : 'SIM SANDBOX';
    }
    if (futuModeHeroText) {
      futuModeHeroText.textContent = isRealMode
        ? 'Real account selected for inspection; order routing remains locked behind backend guardrails.'
        : 'Sandbox route selected for order rehearsal and workflow dry-runs.';
    }
    if (futuModeCardSubtitle) {
      futuModeCardSubtitle.textContent = isRealMode
        ? 'Real account visible. Live routing stays blocked.'
        : 'Sim route active. Live routing stays gated.';
    }
    if (futuModeSafetyTitle) {
      futuModeSafetyTitle.textContent = isRealMode ? 'Live blocked' : 'SIM allowed';
    }
    if (futuModeSafetyText) {
      futuModeSafetyText.textContent = isRealMode
        ? 'Inspection only until the live gate opens.'
        : 'Manual and rebalance orders stay in SIM.';
    }
    if (futuModeRouteTitle) {
      futuModeRouteTitle.textContent = `Account #${selectedAccId} · ${String(selectedEnv || '--').toUpperCase()}`;
    }
    if (futuModeRouteText) {
      const connState = st.connected ? 'gateway connected' : 'gateway waiting';
      futuModeRouteText.textContent = `${selectedMarket} · ${connHostPort} · ${connState}`;
    }
    if (futuLiveGateCard) {
      futuLiveGateCard.classList.toggle('sim', !isRealMode);
      futuLiveGateCard.classList.toggle('real-disabled', isRealMode);
    }
    if (futuLiveGateTitle) {
      futuLiveGateTitle.textContent = isRealMode ? 'LIVE TRADING DISABLED' : 'SIMULATION ONLY';
    }
    if (futuLiveGateText) {
      futuLiveGateText.textContent = isRealMode
        ? 'Backend lock active.'
        : 'Use SIM for rehearsal.';
    }
    if (futuAccountApplyHint) futuAccountApplyHint.textContent = `Preferred account: #${preferredAccId} · Active account: #${selectedAccId}`;
    if (futuAccountSelect) {
      if (!futuAccountListCache.length) {
        await refreshFutuAccountList();
      }
    }
    if (futuCtrlRuntime) futuCtrlRuntime.textContent = st.runtime_file ? String(st.runtime_file).split(/[\\/]/).pop() : '--';
    if (futuActivityRangeSelect) {
      const backendRangeDays = Number(st?.history_order_range_days);
      if (Number.isFinite(backendRangeDays) && backendRangeDays >= 0) {
        futuActivityRangeDays = backendRangeDays;
        const targetValue = String(backendRangeDays);
        const hasOption = Array.from(futuActivityRangeSelect.options).some((opt) => opt.value === targetValue);
        futuActivityRangeSelect.value = hasOption ? targetValue : '30';
      }
    }
    if (futuCapitalCapHint) {
      const cap = Number(st?.rebalance_capital_limit_usd);
      futuCapitalCapHint.textContent = Number.isFinite(cap) && cap > 0
        ? `Current rebalance cap: $${cap.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
        : 'Current rebalance cap: Unlimited';
    }
    if (futuManualOrderHint) {
      futuManualOrderHint.textContent = isRealMode
        ? 'REAL account selected. Manual order UI is visible for workflow validation, but live order placement is still blocked by the safety gate.'
        : 'Place manual BUY/SELL limit order for the FUTU simulation account.';
    }
    if (futuManualOrderPanel) {
      futuManualOrderPanel.classList.toggle('real-mode', isRealMode);
    }
    if (futuManualSubmitBtn) {
      futuManualSubmitBtn.classList.toggle('futu-manual-submit-real', isRealMode);
      futuManualSubmitBtn.textContent = isRealMode ? 'Validate Real Order Form' : 'Submit Manual Order';
      futuManualSubmitBtn.title = isRealMode
        ? 'REAL trading is blocked; use this button only to validate the order workflow.'
        : 'Submit a manual order to the FUTU simulation account.';
    }
    if (futuManualGuard) {
      futuManualGuard.style.display = isRealMode ? '' : 'none';
    }
    renderFutuTradingChecklist({
      hasAccount: !!selectedAccId && selectedAccId !== '--',
      selectedAccId,
      selectedMarket,
      isRealMode,
      connected: !!st.connected,
      running: !!st.running,
      connHostPort,
      dataFeedReady: !!st.data_ws_connected || String(st.data_live_source || '').toLowerCase().includes('polygon'),
      dataLiveSource: String(st.data_live_source || 'unknown'),
      hasRiskGate: !!st?.latest_allocation?.market_regime,
    });
    if (futuCtrlCapital) {
      const acctNav = Number(st?.latest_snapshot?.total_value);
      const stratNav = Number(st?.latest_strategy_snapshot?.total_value);
      if (Number.isFinite(acctNav) && Number.isFinite(stratNav)) {
        futuCtrlCapital.textContent = `Account NAV ${acctNav.toLocaleString(undefined, { maximumFractionDigits: 2 })} · Strategy NAV ${stratNav.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
      } else {
        futuCtrlCapital.textContent = Number.isFinite(acctNav) ? acctNav.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '--';
      }
    }
    if (futuStrategyCapitalHint) {
      const strategyCap = Number(st?.strategy_start_capital_usd);
      futuStrategyCapitalHint.textContent = Number.isFinite(strategyCap) && strategyCap > 0
        ? `Strategy startup capital: $${strategyCap.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
        : 'Strategy startup capital: --';
    }
    renderFutuAccountSectionPreview(st);
    if (futuLoadPathInput && st.runtime_file) {
      futuLoadPathInput.value = String(st.runtime_file);
      rememberFutuLoad(String(st.runtime_file), Array.isArray(st.snapshots) ? st.snapshots.length : 0, true);
    }

    // Started-at sub
    if (futuStartedAt && st.started_at) {
      try {
        const d = new Date(st.started_at);
        futuStartedAt.textContent = `Since ${d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
      } catch (_) {}
    }

    if (futuExecCapital) {
      const totalVal = Number(st?.latest_snapshot?.total_value);
      const strategyVal = Number(st?.latest_strategy_snapshot?.total_value);
      if (Number.isFinite(totalVal) && Number.isFinite(strategyVal)) {
        futuExecCapital.textContent = `Account ${totalVal.toLocaleString(undefined, { maximumFractionDigits: 2 })} | Strategy ${strategyVal.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
      } else {
        futuExecCapital.textContent = Number.isFinite(totalVal) ? totalVal.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '--';
      }
    }
    if (futuExecPool) {
      const count = Number(st?.latest_snapshot?.symbols?.length || 0);
      futuExecPool.textContent = `${count} symbols`;
    }

    futuFullContext = buildFutuSeriesContext(st);
    syncFutuLegendModeLabels();
    renderFutuConnectionKpis(st, futuFullContext);
    renderFutuChartFromCurrentContext();
    renderFutuChartSummaryStrip();

    fillFutuHoldingsTable(st);
    fillFutuCapitalSummaryTable(st);
    renderFutuOpenOrdersTable(st);
    renderFutuCancelHistoryTable(st);
    renderFutuTradeHistory(st);
    renderRealtimeMarketGrid('futuRtMarketGrid', st.latest_snapshot);

    const futuLogBox = document.getElementById('futuLogBox');
    if (futuLogBox) {
      const escapeHtml = (value) => String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
      const logEntries = (st.logs || []).slice(-20);
      futuLogBox.innerHTML = logEntries.map((x, i) => {
        const text = String(x || '');
        const lower = text.toLowerCase();
        const cls = lower.includes('error:')
          ? 'log-error'
          : (lower.includes('warning:') || lower.includes('[warning]') || lower.includes('disconnect')
            ? 'log-warn'
            : '');
        // Try to extract leading timestamp like [2025-02-19 09:04:20] or 2025-02-19T09:04:20
        const tsMatch = text.match(/^(\[?\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\]?)\s*/);
        const tspart = tsMatch ? `<span style='color:var(--muted);font-size:10px;font-family:var(--mono);margin-right:6px;'>${escapeHtml(tsMatch[1])}</span>` : '';
        const msgpart = tsMatch ? escapeHtml(text.slice(tsMatch[0].length)) : escapeHtml(text);
        return `<div class="${cls}">${tspart}${msgpart}</div>`;
      }).join('');
    }
  } catch (e) {
    console.warn(e);
  }
};

setInterval(refreshFutu, 4000);

const paperControl = async (path, body = {}, options = {}) => {
  try {
    await api(path, { method: 'POST', body: JSON.stringify(body) });
    const successText = path === '/api/paper/load'
      ? 'Paper history loaded · Restored holdings and running'
      : `Action success: ${path}`;
    setStatus(successText, 'ok');
    if (options.resetTradeState) {
      resetPaperTradeState();
    }
    await refreshPaper();
    if (options.syncTargetsOnce) {
      await forceSyncPaperTargetsFromStatus();
    }
  } catch (e) {
    setStatus(e.message || String(e), 'err');
    alert(e.message);
  }
};

const futuControl = async (path, body = {}) => {
  try {
    const response = await api(path, { method: 'POST', body: JSON.stringify(body) });
    const successText = path === '/api/futu/load'
      ? 'FUTU history loaded · restored snapshots and resume polling'
      : `Action success: ${path}`;
    if (path === '/api/futu/load') {
      const runtimeFile = response?.runtime_file || body?.runtime_file || futuLoadPathInput?.value;
      rememberFutuLoad(runtimeFile, response?.snapshots);
    }
    setStatus(successText, 'ok');
    await refreshFutu();
  } catch (e) {
    setStatus(e.message || String(e), 'err');
    alert(e.message);
  }
};

const buildPaperTargetsPayload = (symbols) => {
  const weight = symbols.length > 0 ? 1 / symbols.length : 0;
  return symbols.map(symbol => ({ symbol, weight }));
};

const buildStrategyStartPayloadPreview = () => {
  let symbols = [...manualPaperTargets];
  if (!symbols.length && lastPortfolio?.weights?.length) {
    symbols = lastPortfolio.weights
      .map(([symbol]) => String(symbol || '').toUpperCase())
      .filter(Boolean);
  }
  symbols = [...new Set(symbols)].sort();
  const targets = buildPaperTargetsPayload(symbols);

  return {
    targets,
    initial_capital: Number(document.getElementById('paperCapital')?.value),
    time1: document.getElementById('paperTime1')?.value || '23:30',
    time2: document.getElementById('paperTime2')?.value || '02:30',
    optimization_time: (paperOptTimeInput?.value || '22:00').trim() || '22:00',
    optimization_weekdays: getPaperOptimizationWeekdays(),
    apply_now_on_universe_update: paperApplyNowCheckbox ? !!paperApplyNowCheckbox.checked : true,
  };
};

const renderStrategyDispatchPreview = () => {
  if (!strategyPayloadPreview) return;
  strategyPayloadPreview.textContent = JSON.stringify(buildStrategyStartPayloadPreview(), null, 2);
};

const formatOptWeekday = (w) => ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][w - 1] || '--';

const computeNextOptimizationLabel = (optTimeText, weekdays) => {
  const m = String(optTimeText || '').trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return '--';
  const hour = Number(m[1]);
  const minute = Number(m[2]);
  if (!Number.isFinite(hour) || !Number.isFinite(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    return '--';
  }

  const now = new Date();
  const todayMidnight = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  for (let delta = 0; delta <= 7; delta += 1) {
    const day = new Date(todayMidnight);
    day.setDate(day.getDate() + delta);
    const weekdayMon = ((day.getDay() + 6) % 7) + 1;
    if (!weekdays.includes(weekdayMon)) continue;

    const candidate = new Date(day.getFullYear(), day.getMonth(), day.getDate(), hour, minute, 0, 0);
    if (candidate <= now) continue;

    const hoursDiff = Math.round((candidate.getTime() - now.getTime()) / 3600000);
    return `${formatOptWeekday(weekdayMon)} ${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')} (in ${hoursDiff}h)`;
  }
  return '--';
};

const getPaperOptimizationWeekdays = () => {
  const selected = paperOptWeekdayChecks
    .map(el => Number(el.getAttribute('data-paper-opt-weekday') || 0))
    .filter((day, idx) => day >= 1 && day <= 7 && paperOptWeekdayChecks[idx]?.checked);
  return selected.length ? selected : [1, 2, 3, 4, 5];
};

const syncNextOptimizationBadge = () => {
  const el = document.getElementById('paperCtrlNextOpt');
  if (!el) return;
  const weekdays = getPaperOptimizationWeekdays();
  el.textContent = computeNextOptimizationLabel(paperOptTimeInput?.value || '22:00', weekdays);
};

const persistPaperOptimizationSettings = (delayMs = 250) => {
  if (paperOptSaveTimer) {
    clearTimeout(paperOptSaveTimer);
  }

  paperOptSaveTimer = setTimeout(async () => {
    paperOptSaveTimer = null;

    if (paperOptSaveInFlight) {
      paperOptSavePending = true;
      return;
    }

    paperOptSaveInFlight = true;
    try {
      await api('/api/paper/optimization', {
        method: 'POST',
        body: JSON.stringify({
          optimization_time: (paperOptTimeInput?.value || '22:00').trim() || '22:00',
          optimization_weekdays: getPaperOptimizationWeekdays(),
        }),
      });
    } catch (e) {
      setStatus(`Optimization settings sync failed: ${e.message || String(e)}`, 'err');
    } finally {
      paperOptSaveInFlight = false;
      if (paperOptSavePending) {
        paperOptSavePending = false;
        persistPaperOptimizationSettings(0);
      }
    }
  }, Math.max(0, Number(delayMs) || 0));
};

if (paperTargetAddBtn) {
  paperTargetAddBtn.addEventListener('click', () => {
    const symbols = normalizeSymbols(paperTargetInput?.value || '');
    if (!symbols.length) return;
    addPaperTargetSymbols(symbols);
    if (paperTargetInput) paperTargetInput.value = '';
    renderPaperTargetChips();
  });
}

if (paperTargetInput) {
  paperTargetInput.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    paperTargetAddBtn?.click();
  });
}

if (paperTargetApplyBtn) {
  paperTargetApplyBtn.addEventListener('click', async () => {
    if (manualPaperTargets.length === 0) {
      alert('Candidate universe cannot be empty.');
      return;
    }
    setPaperApplyOptimizing(true);
    setStatus('Optimizing candidate universe...');
    try {
      const applyNow = paperApplyNowCheckbox ? !!paperApplyNowCheckbox.checked : true;
      await api('/api/paper/targets', {
        method: 'POST',
        body: JSON.stringify({ symbols: manualPaperTargets, apply_now: applyNow }),
      });
      paperTargetsDirty = false;
      renderPaperTargetChips();
      setStatus(applyNow
        ? 'Candidate universe optimized and rebalanced immediately'
        : 'Candidate universe optimized (rebalance will use latest optimized weights)', 'ok');
      await refreshPaper();
    } catch (e) {
      setStatus(e.message || String(e), 'err');
      alert(e.message);
    } finally {
      setPaperApplyOptimizing(false);
    }
  });
}

document.getElementById('paperStart').addEventListener('click', async () => {
  let symbols = [...manualPaperTargets];
  if (!symbols.length && lastPortfolio?.weights?.length) {
    symbols = lastPortfolio.weights.map(([symbol]) => String(symbol || '').toUpperCase()).filter(Boolean);
  }
  if (!symbols.length) {
    alert('Please set candidate universe first (manual symbols or run Portfolio optimization).');
    return;
  }
  symbols = [...new Set(symbols)].sort();
  manualPaperTargets = symbols;
  paperTargetsDirty = false;
  renderPaperTargetChips();
  const targets = buildPaperTargetsPayload(symbols);
  const optimizationTime = (paperOptTimeInput?.value || '22:00').trim() || '22:00';
  const optimizationWeekdays = getPaperOptimizationWeekdays();
  setPaperStartOptimizing(true);
  setStatus('Optimizing candidate universe before start...');
  try {
    await paperControl('/api/paper/start', {
      targets,
      initial_capital: Number(document.getElementById('paperCapital').value),
      time1: document.getElementById('paperTime1').value,
      time2: document.getElementById('paperTime2').value,
      optimization_time: optimizationTime,
      optimization_weekdays: optimizationWeekdays,
    }, { resetTradeState: true, syncTargetsOnce: true });
  } finally {
    setPaperStartOptimizing(false);
    await refreshPaper();
  }
});
document.getElementById('paperLoad').addEventListener('click', async () => {
  if (!paperFilePicker) {
    const strategyFile = (document.getElementById('paperLoadPath')?.value || '').trim();
    if (!strategyFile) {
      alert('Please enter strategy JSON path, e.g. log/paper_strategy_YYYYMMDD_HHMMSS.json');
      return;
    }
    await paperControl('/api/paper/load', {
      strategy_file: strategyFile,
    }, { resetTradeState: true, syncTargetsOnce: true });
    return;
  }

  paperFilePicker.value = '';
  paperFilePicker.click();
});

if (paperFilePicker) {
  paperFilePicker.addEventListener('change', async (event) => {
    const file = event?.target?.files?.[0];
    if (!file) return;

    const input = document.getElementById('paperLoadPath');
    const selectedName = String(file.name || '').trim();
    if (!selectedName) return;

    const suggestedPath = selectedName.toLowerCase().endsWith('.json')
      ? `log/${selectedName}`
      : `log/${selectedName}.json`;

    if (input) {
      input.value = suggestedPath;
    }

    if (paperLoadBtn) {
      paperLoadBtn.disabled = true;
      paperLoadBtn.textContent = 'Loading...';
    }

    try {
      await paperControl('/api/paper/load', {
        strategy_file: suggestedPath,
      }, { resetTradeState: true, syncTargetsOnce: true });
    } finally {
      if (paperLoadBtn) {
        paperLoadBtn.disabled = false;
        paperLoadBtn.textContent = '\uD83D\uDCC2 Load';
      }
    }
  });
}
document.getElementById('paperPause').addEventListener('click', () => paperControl('/api/paper/pause'));
document.getElementById('paperResume').addEventListener('click', () => paperControl('/api/paper/resume'));
document.getElementById('paperStop').addEventListener('click', () => paperControl('/api/paper/stop'));

document.getElementById('futuStart')?.addEventListener('click', () => futuControl('/api/futu/start'));
document.getElementById('futuStop')?.addEventListener('click', () => futuControl('/api/futu/stop'));
document.getElementById('futuAccountApply')?.addEventListener('click', async () => {
  const accId = String(futuAccountSelect?.value || '').trim();
  if (!accId) {
    alert('Please select a FUTU account ID first.');
    return;
  }

  const accountSet = getFutuAccountIdSet();
  if (accountSet.size > 0 && !accountSet.has(accId)) {
    const listText = Array.from(accountSet).join(', ');
    alert(`Selected account #${accId} is not in current OpenD account list.\nAvailable: ${listText}`);
    await refreshFutuAccountList({ force: true });
    return;
  }

  if (futuAccountApplyBtn) {
    futuAccountApplyBtn.disabled = true;
    futuAccountApplyBtn.textContent = 'Applying...';
  }

  try {
    await futuControl('/api/futu/account-apply', { acc_id: accId });
    if (futuAccountApplyHint) futuAccountApplyHint.textContent = `Preferred account applied: #${accId}`;
    await refreshFutuAccountList({ force: true });
  } finally {
    if (futuAccountApplyBtn) {
      futuAccountApplyBtn.disabled = false;
      futuAccountApplyBtn.textContent = '🪪 Account Apply';
    }
  }
});
document.getElementById('futuLoad')?.addEventListener('click', async () => {
  if (!futuFilePicker) {
    const runtimeFile = (futuLoadPathInput?.value || '').trim();
    await futuControl('/api/futu/load', {
      runtime_file: runtimeFile || null,
    });
    return;
  }

  futuFilePicker.value = '';
  futuFilePicker.click();
});

if (futuFilePicker) {
  futuFilePicker.addEventListener('change', async (event) => {
    const file = event?.target?.files?.[0];
    if (!file) return;
    const selectedName = String(file.name || '').trim();
    if (!selectedName) return;

    const suggestedPath = selectedName.toLowerCase().endsWith('.jsonl')
      ? `log/${selectedName}`
      : `log/${selectedName}.jsonl`;

    if (futuLoadPathInput) futuLoadPathInput.value = suggestedPath;

    if (futuLoadBtn) {
      futuLoadBtn.disabled = true;
      futuLoadBtn.textContent = 'Loading...';
    }

    try {
      await futuControl('/api/futu/load', {
        runtime_file: suggestedPath,
      });
    } finally {
      if (futuLoadBtn) {
        futuLoadBtn.disabled = false;
        futuLoadBtn.textContent = '📂 Load FUTU';
      }
    }
  });
}

const refreshTrain = async () => {
  try {
    const st = await api('/api/train/status');
    const rawBox = document.getElementById('trainRawJson');
    if (rawBox) rawBox.textContent = JSON.stringify(st, null, 2);

    const phaseLabel = document.getElementById('trainPhaseLabel');
    const progressWrap = document.getElementById('trainProgressWrap');
    const progressFill = document.getElementById('trainProgressFill');
    const progressLeft = document.getElementById('trainProgressLeft');
    const progressRight = document.getElementById('trainProgressRight');
    const cardsEl = document.getElementById('trainCards');
    const trainBadge = document.getElementById('trainTabBadge');

    const running = !!st.running;
    const epoch = st.epoch || st.current_epoch || 0;
    const totalEpochs = st.total_epochs || st.epochs || Number(document.getElementById('tEpochs')?.value) || 200;
    const loss = st.loss ?? st.train_loss ?? null;
    const bestLoss = st.best_loss ?? st.best_val_loss ?? null;
    const lr = st.learning_rate ?? st.lr ?? null;
    const elapsed = st.elapsed ?? st.elapsed_secs ?? null;

    if (phaseLabel) {
      phaseLabel.textContent = running ? `Epoch ${epoch} / ${totalEpochs}` : (epoch > 0 ? `Finished (${epoch} epochs)` : 'Idle');
    }

    if (trainBadge) {
      trainBadge.classList.toggle('running', running);
      if (!running) trainBadge.classList.remove('running');
    }

    if (progressWrap && progressFill) {
      if (running || epoch > 0) {
        progressWrap.style.display = '';
        const pct = totalEpochs > 0 ? Math.min(100, (epoch / totalEpochs) * 100) : 0;
        progressFill.style.width = `${pct.toFixed(1)}%`;
        if (progressLeft) progressLeft.textContent = `${pct.toFixed(0)}%`;
        if (progressRight) {
          if (running && elapsed && epoch > 0) {
            const secPerEpoch = elapsed / epoch;
            const remaining = (totalEpochs - epoch) * secPerEpoch;
            const mins = Math.floor(remaining / 60);
            const secs = Math.floor(remaining % 60);
            progressRight.textContent = `ETA ~${mins}m ${secs}s`;
          } else progressRight.textContent = running ? '...' : 'Done';
        }
      } else {
        progressWrap.style.display = 'none';
      }
    }

    if (cardsEl) {
      if (running || epoch > 0) {
        cardsEl.style.display = '';
        const fmtLoss = (v) => v != null && Number.isFinite(v) ? v.toFixed(6) : '--';
        const fmtLr = (v) => v != null && Number.isFinite(v) ? v.toExponential(2) : '--';
        const fmtTime = (secs) => {
          if (!Number.isFinite(secs)) return '--';
          if (secs < 60) return `${secs.toFixed(0)}s`;
          return `${Math.floor(secs/60)}m ${Math.floor(secs%60)}s`;
        };
        cardsEl.innerHTML = `
          <div class='stat-card'><div class='stat-card-label'>Epoch</div><div class='stat-card-value'>${epoch}<span style='font-size:12px;color:var(--muted);'> / ${totalEpochs}</span></div></div>
          <div class='stat-card'><div class='stat-card-label'>Loss</div><div class='stat-card-value'>${fmtLoss(loss)}</div></div>
          <div class='stat-card'><div class='stat-card-label'>Best Loss</div><div class='stat-card-value up'>${fmtLoss(bestLoss)}</div></div>
          <div class='stat-card'><div class='stat-card-label'>Learning Rate</div><div class='stat-card-value'>${fmtLr(lr)}</div></div>
          <div class='stat-card'><div class='stat-card-label'>Elapsed</div><div class='stat-card-value'>${fmtTime(elapsed)}</div></div>
        `;
      } else {
        cardsEl.style.display = 'none';
      }
    }
  } catch {}
};
setInterval(refreshTrain, 5000);

const renderBacktestWeightsTable = (st) => {
  const tbody = document.querySelector('#backtestWeightsTable tbody');
  const subtitle = document.getElementById('backtestWeightsSubtitle');
  if (!tbody) return;
  const rows = Array.isArray(st?.latest_weights) ? st.latest_weights : [];
  const asOfText = st?.latest_weights_as_of || st?.latest_snapshot?.timestamp || null;
  if (subtitle) {
    subtitle.textContent = asOfText
      ? `Weights currently used by the backtest engine as of ${asOfText}`
      : 'Weights currently used by the backtest engine';
  }
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="2" class="flat">No target weights yet. Start a backtest to populate daily allocations.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map((row) => `
    <tr>
      <td>${row.symbol || '--'}</td>
      <td class="num">${Number(row.weight || 0).toFixed(4)} (${(Number(row.weight || 0) * 100).toFixed(2)}%)</td>
    </tr>
  `).join('');
};

const renderIdleBacktestKpis = (st) => {
  const grid = document.getElementById('backtestKpiGrid');
  if (!grid) return;
  renderBacktestOverlay(st);
  const asOfText = st?.last_message || 'No backtest run yet';
  const placeholder = '--';
  grid.innerHTML = `
    <div class="kpi-card kpi-neutral">
      <div class="kpi-label">Final NAV</div>
      <div class="kpi-value">${placeholder}</div>
      <div class="kpi-sub">${asOfText}</div>
    </div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Range Return</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Awaiting backtest data</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">QQQ Return</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Awaiting benchmark data</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Alpha vs QQQ</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">No observations yet</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Sharpe Ratio</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">QQQ Sharpe</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Sortino Ratio</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">QQQ Sortino</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Max Drawdown</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">QQQ Max Drawdown</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Ann. Volatility</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">252d annualized</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">QQQ Ann. Vol</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">252d annualized</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Calmar Ratio</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">CAGR / MaxDD</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">QQQ Calmar</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">CAGR / MaxDD</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Annualized Alpha</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Jensen alpha</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Beta vs QQQ</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Market sensitivity</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Information Ratio</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Active return / TE</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Win Rate</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Positive days</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">CAGR</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">QQQ CAGR</div><div class="kpi-value">${placeholder}</div><div class="kpi-sub">Run backtest to compute</div></div>
    <div class="kpi-card kpi-neutral"><div class="kpi-label">Observed Days</div><div class="kpi-value">0</div><div class="kpi-sub">Return observations</div></div>
  `;
};

const renderBacktestKpis = (st) => {
  const grid = document.getElementById('backtestKpiGrid');
  if (!grid) return;
  renderBacktestOverlay(st);
  const filtered = filterPaperContextByRangeDays(backtestFullContext, selectedBacktestRangeDays);
  const latest = filtered?.latest || backtestFullContext?.latest;
  const perf = computeBacktestPerformance(filtered) || normalizeBacktestSummary(st?.summary) || null;
  if (!latest || !perf) {
    renderIdleBacktestKpis(st);
    return;
  }
  const asOfText = latest?.updatedText || latest?.timestamp || st?.last_message || '--';

  const returnMood = Number.isFinite(perf.totalReturnPct) ? (perf.totalReturnPct >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const benchmarkMood = Number.isFinite(perf.benchmarkTotalReturnPct) ? (perf.benchmarkTotalReturnPct >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const alphaMood = Number.isFinite(perf.alphaPct) ? (perf.alphaPct >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const ddMood = Number.isFinite(perf.maxDrawdownPct) ? (perf.maxDrawdownPct <= -10 ? 'kpi-negative' : 'kpi-neutral') : 'kpi-neutral';
  const sharpeMood = Number.isFinite(perf.sharpe) ? (perf.sharpe >= 1 ? 'kpi-positive' : (perf.sharpe < 0 ? 'kpi-negative' : 'kpi-neutral')) : 'kpi-neutral';
  const sortinoMood = Number.isFinite(perf.sortino) ? (perf.sortino >= 1 ? 'kpi-positive' : (perf.sortino < 0 ? 'kpi-negative' : 'kpi-neutral')) : 'kpi-neutral';
  const calmarMood = Number.isFinite(perf.calmar) ? (perf.calmar >= 1 ? 'kpi-positive' : (perf.calmar < 0 ? 'kpi-negative' : 'kpi-neutral')) : 'kpi-neutral';
  const infoMood = Number.isFinite(perf.informationRatio) ? (perf.informationRatio >= 0 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';
  const betaMood = Number.isFinite(perf.beta) ? (Math.abs(perf.beta - 1) < 0.25 ? 'kpi-neutral' : (perf.beta > 1.25 ? 'kpi-warn' : 'kpi-positive')) : 'kpi-neutral';
  const volMood = Number.isFinite(perf.annualVolPct) ? (perf.annualVolPct > 30 ? 'kpi-warn' : 'kpi-neutral') : 'kpi-neutral';
  const winMood = Number.isFinite(perf.winRate) ? (perf.winRate >= 50 ? 'kpi-positive' : 'kpi-negative') : 'kpi-neutral';

  grid.innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Final NAV</div>
      <div class="kpi-value">${formatMoney(latest.portfolioValue)}</div>
      <div class="kpi-sub">As Of ${asOfText}</div>
    </div>
    <div class="kpi-card ${returnMood}">
      <div class="kpi-label">Range Return</div>
      <div class="kpi-value">${Number.isFinite(perf.totalReturnPct) ? `${perf.totalReturnPct >= 0 ? '+' : ''}${perf.totalReturnPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">${Number.isFinite(perf.totalPnlUsd) ? `${perf.totalPnlUsd >= 0 ? '+' : ''}${formatMoney(perf.totalPnlUsd)}` : '--'}</div>
    </div>
    <div class="kpi-card ${benchmarkMood}">
      <div class="kpi-label">QQQ Return</div>
      <div class="kpi-value">${Number.isFinite(perf.benchmarkTotalReturnPct) ? `${perf.benchmarkTotalReturnPct >= 0 ? '+' : ''}${perf.benchmarkTotalReturnPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">${Number.isFinite(perf.benchmarkPnlUsd) ? `${perf.benchmarkPnlUsd >= 0 ? '+' : ''}${formatMoney(perf.benchmarkPnlUsd)}` : '--'}</div>
    </div>
    <div class="kpi-card ${alphaMood}">
      <div class="kpi-label">Alpha vs QQQ</div>
      <div class="kpi-value">${Number.isFinite(perf.alphaPct) ? `${perf.alphaPct >= 0 ? '+' : ''}${perf.alphaPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">${Number.isFinite(perf.alphaUsd) ? `${perf.alphaUsd >= 0 ? '+' : ''}${formatMoney(perf.alphaUsd)}` : '--'}</div>
    </div>
    <div class="kpi-card ${sharpeMood}">
      <div class="kpi-label">Sharpe Ratio</div>
      <div class="kpi-value">${Number.isFinite(perf.sharpe) ? perf.sharpe.toFixed(2) : '--'}</div>
      <div class="kpi-sub">rf ${(BACKTEST_RISK_FREE_RATE * 100).toFixed(1)}%</div>
    </div>
    <div class="kpi-card ${Number.isFinite(perf.benchmarkSharpe) ? (perf.benchmarkSharpe >= 1 ? 'kpi-positive' : (perf.benchmarkSharpe < 0 ? 'kpi-negative' : 'kpi-neutral')) : 'kpi-neutral'}">
      <div class="kpi-label">QQQ Sharpe</div>
      <div class="kpi-value">${Number.isFinite(perf.benchmarkSharpe) ? perf.benchmarkSharpe.toFixed(2) : '--'}</div>
      <div class="kpi-sub">benchmark</div>
    </div>
    <div class="kpi-card ${sortinoMood}">
      <div class="kpi-label">Sortino Ratio</div>
      <div class="kpi-value">${Number.isFinite(perf.sortino) ? perf.sortino.toFixed(2) : '--'}</div>
      <div class="kpi-sub">downside-adjusted</div>
    </div>
    <div class="kpi-card ${Number.isFinite(perf.benchmarkSortino) ? (perf.benchmarkSortino >= 1 ? 'kpi-positive' : (perf.benchmarkSortino < 0 ? 'kpi-negative' : 'kpi-neutral')) : 'kpi-neutral'}">
      <div class="kpi-label">QQQ Sortino</div>
      <div class="kpi-value">${Number.isFinite(perf.benchmarkSortino) ? perf.benchmarkSortino.toFixed(2) : '--'}</div>
      <div class="kpi-sub">benchmark</div>
    </div>
    <div class="kpi-card ${ddMood}">
      <div class="kpi-label">Max Drawdown</div>
      <div class="kpi-value ${Number.isFinite(perf.maxDrawdownPct) && perf.maxDrawdownPct < 0 ? 'down' : ''}">${Number.isFinite(perf.maxDrawdownPct) ? `${perf.maxDrawdownPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">QQQ ${Number.isFinite(perf.benchmarkMaxDrawdownPct) ? `${perf.benchmarkMaxDrawdownPct.toFixed(2)}%` : '--'}</div>
    </div>
    <div class="kpi-card ${Number.isFinite(perf.benchmarkMaxDrawdownPct) ? (perf.benchmarkMaxDrawdownPct <= -10 ? 'kpi-negative' : 'kpi-neutral') : 'kpi-neutral'}">
      <div class="kpi-label">QQQ Max Drawdown</div>
      <div class="kpi-value ${Number.isFinite(perf.benchmarkMaxDrawdownPct) && perf.benchmarkMaxDrawdownPct < 0 ? 'down' : ''}">${Number.isFinite(perf.benchmarkMaxDrawdownPct) ? `${perf.benchmarkMaxDrawdownPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">benchmark</div>
    </div>
    <div class="kpi-card ${volMood}">
      <div class="kpi-label">Ann. Volatility</div>
      <div class="kpi-value">${Number.isFinite(perf.annualVolPct) ? `${perf.annualVolPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">252d annualized</div>
    </div>
    <div class="kpi-card ${Number.isFinite(perf.benchmarkAnnualVolPct) ? (perf.benchmarkAnnualVolPct > 30 ? 'kpi-warn' : 'kpi-neutral') : 'kpi-neutral'}">
      <div class="kpi-label">QQQ Ann. Vol</div>
      <div class="kpi-value">${Number.isFinite(perf.benchmarkAnnualVolPct) ? `${perf.benchmarkAnnualVolPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">benchmark</div>
    </div>
    <div class="kpi-card ${calmarMood}">
      <div class="kpi-label">Calmar Ratio</div>
      <div class="kpi-value">${Number.isFinite(perf.calmar) ? perf.calmar.toFixed(2) : '--'}</div>
      <div class="kpi-sub">CAGR / MaxDD</div>
    </div>
    <div class="kpi-card ${Number.isFinite(perf.benchmarkCalmar) ? (perf.benchmarkCalmar >= 1 ? 'kpi-positive' : (perf.benchmarkCalmar < 0 ? 'kpi-negative' : 'kpi-neutral')) : 'kpi-neutral'}">
      <div class="kpi-label">QQQ Calmar</div>
      <div class="kpi-value">${Number.isFinite(perf.benchmarkCalmar) ? perf.benchmarkCalmar.toFixed(2) : '--'}</div>
      <div class="kpi-sub">benchmark</div>
    </div>
    <div class="kpi-card ${alphaMood}">
      <div class="kpi-label">Annualized Alpha</div>
      <div class="kpi-value">${Number.isFinite(perf.alphaAnnualPct) ? `${perf.alphaAnnualPct >= 0 ? '+' : ''}${perf.alphaAnnualPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">Jensen alpha</div>
    </div>
    <div class="kpi-card ${betaMood}">
      <div class="kpi-label">Beta vs QQQ</div>
      <div class="kpi-value">${Number.isFinite(perf.beta) ? perf.beta.toFixed(2) : '--'}</div>
      <div class="kpi-sub">market sensitivity</div>
    </div>
    <div class="kpi-card ${infoMood}">
      <div class="kpi-label">Information Ratio</div>
      <div class="kpi-value">${Number.isFinite(perf.informationRatio) ? perf.informationRatio.toFixed(2) : '--'}</div>
      <div class="kpi-sub">active return / TE</div>
    </div>
    <div class="kpi-card ${winMood}">
      <div class="kpi-label">Win Rate</div>
      <div class="kpi-value">${Number.isFinite(perf.winRate) ? `${perf.winRate.toFixed(1)}%` : '--'}</div>
      <div class="kpi-sub">positive days</div>
    </div>
    <div class="kpi-card ${Number.isFinite(perf.cagrPct) && perf.cagrPct >= 0 ? 'kpi-positive' : 'kpi-negative'}">
      <div class="kpi-label">CAGR</div>
      <div class="kpi-value">${Number.isFinite(perf.cagrPct) ? `${perf.cagrPct >= 0 ? '+' : ''}${perf.cagrPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">QQQ ${Number.isFinite(perf.benchmarkCagrPct) ? `${perf.benchmarkCagrPct >= 0 ? '+' : ''}${perf.benchmarkCagrPct.toFixed(2)}%` : '--'}</div>
    </div>
    <div class="kpi-card ${Number.isFinite(perf.benchmarkCagrPct) && perf.benchmarkCagrPct >= 0 ? 'kpi-positive' : 'kpi-negative'}">
      <div class="kpi-label">QQQ CAGR</div>
      <div class="kpi-value">${Number.isFinite(perf.benchmarkCagrPct) ? `${perf.benchmarkCagrPct >= 0 ? '+' : ''}${perf.benchmarkCagrPct.toFixed(2)}%` : '--'}</div>
      <div class="kpi-sub">benchmark</div>
    </div>
    <div class="kpi-card kpi-neutral">
      <div class="kpi-label">Observed Days</div>
      <div class="kpi-value">${Number.isFinite(perf.tradingDays) ? perf.tradingDays : '--'}</div>
      <div class="kpi-sub">return observations</div>
    </div>
  `;
};

const refreshBacktest = async () => {
  try {
    const st = await api('/api/backtest/status');
    latestBacktestStatus = st;
    syncBacktestButtons(st);
    renderBacktestSummaryMeta(st);

    const rawBox = document.getElementById('backtestRawJson');
    const phaseLabel = document.getElementById('backtestPhaseLabel');
    const progressWrap = document.getElementById('backtestProgressWrap');
    const progressFill = document.getElementById('backtestProgressFill');
    const progressLeft = document.getElementById('backtestProgressLeft');
    const progressRight = document.getElementById('backtestProgressRight');
    const badge = document.getElementById('backtestTabBadge');
    const logBox = document.getElementById('backtestLogBox');

    if (rawBox) rawBox.textContent = JSON.stringify(st, null, 2);
    if (badge) {
      badge.classList.toggle('running', !!st.running);
      if (!st.running) badge.classList.remove('running');
    }
    if (phaseLabel) {
      phaseLabel.textContent = st.running
        ? (st.last_message || `Running ${st.progress_current_day || 0}/${st.progress_total_days || 0}`)
        : (st.last_error ? `Failed: ${st.last_error}` : (st.last_message || 'Idle'));
    }

    if (progressWrap && progressFill) {
      const total = Number(st.progress_total_days || 0);
      const current = Number(st.progress_current_day || 0);
      if (st.running || current > 0 || total > 0) {
        progressWrap.style.display = '';
        const pct = total > 0 ? Math.min(100, (current / total) * 100) : 0;
        progressFill.style.width = `${pct.toFixed(1)}%`;
        if (progressLeft) progressLeft.textContent = `${pct.toFixed(0)}%`;
        if (progressRight) progressRight.textContent = `${current}/${total || '--'} trading days`;
      } else {
        progressWrap.style.display = 'none';
      }
    }

    const ctx = buildPaperSeriesContext(st.snapshots || []);
    backtestFullContext = ctx.portfolioSeries.length > 0 ? ctx : buildFallbackPaperContext(st.latest_snapshot);
    renderBacktestChartFromCurrentContext();
    renderBacktestKpis(st);
    renderBacktestWeightsTable(st);

    if (logBox) {
      const escapeHtml = (value) => String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
      logBox.innerHTML = (st.logs || []).slice(-80).map((x) => `<div>${escapeHtml(x)}</div>`).join('');
    }
  } catch (e) {
    console.warn(e);
  }
};
setInterval(refreshBacktest, 2500);

document.getElementById('trainStart').addEventListener('click', async () => {
  try {
    await withBusy('trainStart', 'Starting...', 'Training started', async () => {
      await api('/api/train/start', {
        method: 'POST',
        body: JSON.stringify({
          epochs: Number(document.getElementById('tEpochs').value),
          batch_size: Number(document.getElementById('tBatch').value),
          learning_rate: Number(document.getElementById('tLr').value),
          patience: Number(document.getElementById('tPatience').value),
        }),
      });
    });
  } catch (e) { alert(e.message); }
});

backtestStartBtn?.addEventListener('click', async () => {
  try {
    await withBusy('backtestStart', 'Starting...', 'Backtest started', async () => {
      const symbols = String(backtestSymbolsInput?.value || '')
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
      await api('/api/backtest/start', {
        method: 'POST',
        body: JSON.stringify({
          symbols,
          initial_capital: Number(backtestCapitalInput?.value || 80000),
          period_days: Number(backtestDaysInput?.value || 252),
          rebalance_every_days: Number(backtestRebalanceDaysInput?.value || 1),
        }),
      });
      await refreshBacktest();
    });
  } catch (e) {
    alert(e.message);
  }
});

backtestStopBtn?.addEventListener('click', async () => {
  try {
    await withBusy('backtestStop', 'Stopping...', 'Backtest stop requested', async () => {
      await api('/api/backtest/stop', { method: 'POST' });
      await refreshBacktest();
    });
  } catch (e) {
    alert(e.message);
  }
});

refreshPaper();
refreshFutu();
refreshTrain();
refreshBacktest();
renderPaperTargetChips();
refreshRealtimeQuotes();
setInterval(refreshRealtimeQuotes, 12000);

const restoreState = async () => {
  try {
    const state = await api('/api/state');
    let restoredFromBackendForecast = false;

    const backendCached = state?.forecast?.cached_results || {};
    const cachedSymbols = Object.keys(backendCached);

    if (cachedSymbols.length > 0) {
      forecastBatchResults.clear();
      for (const sym of cachedSymbols) {
        const entry = backendCached[sym];
        const result = entry?.result;
        if (!result) continue;
        if (!result.forecasted_at && entry?.forecasted_at) {
          result.forecasted_at = entry.forecasted_at;
        }
        forecastBatchResults.set(String(sym).toUpperCase(), result);
      }

      if (forecastBatchResults.size > 0) {
        const req = state.forecast?.last_request;
        const requestSymbol = req?.symbol ? String(req.symbol).toUpperCase() : null;
        const firstSym = forecastBatchResults.keys().next().value;
        forecastSelectedSymbol = requestSymbol && forecastBatchResults.has(requestSymbol) ? requestSymbol : firstSym;
        const allSymbols = Array.from(forecastBatchResults.keys());
        document.getElementById('fSymbol').value = allSymbols.join(',');
        if (req?.horizon) document.getElementById('fHorizon').value = req.horizon;
        if (req?.simulations) document.getElementById('fSims').value = req.simulations;
        syncQuickChips();

        const selectedData = forecastBatchResults.get(forecastSelectedSymbol);
        if (selectedData) {
          applyForecastDataToChart(selectedData);
          renderFcKpiCards(selectedData);
        }
        renderBatchGrid();
        saveForecastBatchCache();
        restoredFromBackendForecast = true;
      }
    } else if (state.forecast?.last_request) {
      document.getElementById('fSymbol').value = state.forecast.last_request.symbol || document.getElementById('fSymbol').value;
      document.getElementById('fHorizon').value = state.forecast.last_request.horizon || document.getElementById('fHorizon').value;
      document.getElementById('fSims').value = state.forecast.last_request.simulations || document.getElementById('fSims').value;
      syncQuickChips();
    }
    if (!restoredFromBackendForecast && state.forecast?.last_result) {
      const r = state.forecast.last_result;
      forecastBatchResults.clear();
      forecastBatchResults.set(r.symbol || document.getElementById('fSymbol').value.trim().toUpperCase(), r);
      forecastSelectedSymbol = r.symbol || document.getElementById('fSymbol').value.trim().toUpperCase();
      applyForecastDataToChart(r);
      renderFcKpiCards(r);
      renderBatchGrid();
      restoredFromBackendForecast = true;
    }

    const restoredFromLocalBatch = mergeForecastBatchFromLocalByTimestamp(state?.forecast?.last_request);
    let restoredByRefetch = false;
    if (forecastBatchResults.size === 0 && !restoredFromLocalBatch) {
      const meta = loadForecastMetaCache();
      const rawSymbols = String(meta?.symbolsInput || '').trim();
      const symbols = [...new Set(rawSymbols.toUpperCase().split(',').map(s => s.trim()).filter(Boolean))];
      const horizon = Number(meta?.horizon || document.getElementById('fHorizon').value || 10);
      const simulations = Number(meta?.simulations || document.getElementById('fSims').value || 500);
      if (symbols.length > 1) {
        const prevSelection = forecastSelectedSymbol;
        if (meta?.selectedSymbol) forecastSelectedSymbol = String(meta.selectedSymbol).toUpperCase();
        try {
          const { errors } = await runForecastBatchForSymbols(symbols, horizon, simulations);
          restoredByRefetch = true;
          if (errors.length > 0) {
            setStatus(`State restored (refetched, ${errors.length} symbol error${errors.length > 1 ? 's' : ''})`, 'err');
          }
        } catch (err) {
          forecastSelectedSymbol = prevSelection;
          console.warn('restore refetch failed', err);
        }
      }
    }

    if (state.portfolio?.last_symbols?.length) {
      document.getElementById('pSymbols').value = state.portfolio.last_symbols.join(',');
    }
    if (state.portfolio?.last_allocation) {
      lastPortfolio = state.portfolio.last_allocation;
      await refreshRealtimeQuotes();
      fillAssetTable(lastPortfolio, state.paper || {});
    }

    if (state.paper) {
      hydratePaperOptimizationFromStatus(state.paper);
      setPaperStatusChip(state.paper);
      syncPaperButtons(state.paper);
      if (state.paper.strategy_file && document.getElementById('paperLoadPath')) {
        document.getElementById('paperLoadPath').value = state.paper.strategy_file;
      }
      const ctx = buildPaperSeriesContext(state.paper.snapshots || []);
      paperFullContext = ctx.portfolioSeries.length > 0 ? ctx : buildFallbackPaperContext(state.paper.latest_snapshot);
      renderPaperChartFromCurrentContext();
      renderChartSummaryStrip();

      ingestPaperTrades(state.paper);
      renderTradeHistory();
      setPaperTradeMarkers();
      fillHoldingsTable(state.paper);
      fillCapitalSummaryTable(state.paper);
      renderPaperKpis(state.paper);
      renderPaperRiskAlerts(state.paper);
    }

    if (state.futu) {
      latestFutuStatus = state.futu;
      syncFutuButtons(state.futu);
      if (futuLoadPathInput && state.futu.runtime_file) {
        futuLoadPathInput.value = state.futu.runtime_file;
        rememberFutuLoad(state.futu.runtime_file, Array.isArray(state.futu.snapshots) ? state.futu.snapshots.length : 0, true);
      }
      const ctx = buildPaperSeriesContext(state.futu.snapshots || []);
      futuFullContext = ctx.portfolioSeries.length > 0 ? ctx : buildFallbackPaperContext(state.futu.latest_snapshot);
      renderFutuChartFromCurrentContext();
      renderFutuChartSummaryStrip();
      fillFutuHoldingsTable(state.futu);
      fillFutuCapitalSummaryTable(state.futu);
      renderRealtimeMarketGrid('futuRtMarketGrid', state.futu.latest_snapshot);
    }

    if (state.train) {
      const rawBox = document.getElementById('trainRawJson');
      if (rawBox) rawBox.textContent = JSON.stringify(state.train, null, 2);
    }

    if (state.backtest) {
      latestBacktestStatus = state.backtest;
      syncBacktestButtons(state.backtest);
      renderBacktestSummaryMeta(state.backtest);
      if (backtestSymbolsInput && Array.isArray(state.backtest.candidate_symbols) && state.backtest.candidate_symbols.length > 0) {
        backtestSymbolsInput.value = state.backtest.candidate_symbols.join(',');
      }
      if (backtestCapitalInput && Number.isFinite(Number(state.backtest.initial_capital_usd))) {
        backtestCapitalInput.value = Number(state.backtest.initial_capital_usd).toFixed(2);
      }
      if (backtestDaysInput && Number.isFinite(Number(state.backtest.period_days))) {
        backtestDaysInput.value = Number(state.backtest.period_days);
      }
      if (backtestRebalanceDaysInput && Number.isFinite(Number(state.backtest.rebalance_every_days))) {
        backtestRebalanceDaysInput.value = Number(state.backtest.rebalance_every_days);
      }
      const ctx = buildPaperSeriesContext(state.backtest.snapshots || []);
      backtestFullContext = ctx.portfolioSeries.length > 0 ? ctx : buildFallbackPaperContext(state.backtest.latest_snapshot);
      renderBacktestChartFromCurrentContext();
      renderBacktestKpis(state.backtest);
      renderBacktestWeightsTable(state.backtest);
      const rawBox = document.getElementById('backtestRawJson');
      if (rawBox) rawBox.textContent = JSON.stringify(state.backtest, null, 2);
    }

    applyRuntimeChips(state);
    if (restoredFromLocalBatch && forecastBatchResults.size > 1) {
      setStatus('State restored (backend + local multi-symbol cache)', 'ok');
    } else if (restoredByRefetch && forecastBatchResults.size > 1) {
      setStatus('State restored (multi-symbol refetched)', 'ok');
    } else if (restoredFromBackendForecast) {
      setStatus('State restored from backend', 'ok');
    } else {
      setStatus('State restored', 'ok');
    }
    refreshBackendChip();
  } catch (e) {
    setStatus(`State restore failed: ${e.message}`, 'err');
  }
};

restoreState();
renderQuotesAsOf();
renderTradeHistory();

// Backend chip: detect compute backend from /api/state
const refreshBackendChip = async () => {
  try {
    const st = await api('/api/state');
    setDataSourceChip(
      st?.data_live_source || st?.paper?.data_live_source,
      !!(st?.data_ws_connected ?? st?.paper?.data_ws_connected),
      st?.data_ws_diagnostics || st?.paper?.data_ws_diagnostics || null,
      st?.data_live_fetch_diagnostics || st?.paper?.data_live_fetch_diagnostics || null,
    );
    applyRuntimeChips(st);
  } catch {
    applyRuntimeChips({});
  }
};

refreshBackendChip();

// ─── Paper ctrl header: sync badges from inputs + collapsible toggle ───
const syncPaperCtrlBadges = () => {
  const capEl = document.getElementById('paperCtrlCapital');
  const schedEl = document.getElementById('paperCtrlSchedule');
  const capVal = Number(document.getElementById('paperCapital')?.value) || 0;
  const t1 = document.getElementById('paperTime1')?.value || '--';
  const t2 = document.getElementById('paperTime2')?.value || '--';
  if (capEl) capEl.textContent = capVal > 0 ? capVal.toLocaleString() : '--';
  if (schedEl) schedEl.textContent = `${t1} / ${t2}`;
  syncNextOptimizationBadge();
  renderStrategyDispatchPreview();
};
document.getElementById('paperCapital')?.addEventListener('input', syncPaperCtrlBadges);
document.getElementById('paperTime1')?.addEventListener('input', syncPaperCtrlBadges);
document.getElementById('paperTime2')?.addEventListener('input', syncPaperCtrlBadges);
paperApplyNowCheckbox?.addEventListener('change', renderStrategyDispatchPreview);
paperOptTimeInput?.addEventListener('input', () => {
  syncNextOptimizationBadge();
  persistPaperOptimizationSettings(250);
});
paperOptWeekdayChecks.forEach(el => el.addEventListener('change', () => {
  syncNextOptimizationBadge();
  persistPaperOptimizationSettings(120);
}));
syncPaperCtrlBadges();
renderStrategyDispatchPreview();
setInterval(syncNextOptimizationBadge, 60000);

document.getElementById('paperJumpStrategy')?.addEventListener('click', () => {
  switchTabByName('strategy');
});

document.getElementById('futuJumpStrategy')?.addEventListener('click', () => {
  switchTabByName('strategy');
});

if (strategyCopyPayloadBtn) {
  strategyCopyPayloadBtn.addEventListener('click', async () => {
    const text = JSON.stringify(buildStrategyStartPayloadPreview(), null, 2);
    try {
      await navigator.clipboard.writeText(text);
      if (strategyPayloadHint) strategyPayloadHint.textContent = 'Copied start payload to clipboard.';
      setStatus('Strategy start payload copied.', 'ok');
    } catch (e) {
      if (strategyPayloadHint) strategyPayloadHint.textContent = 'Copy failed. Please copy manually from preview.';
      setStatus(e?.message || 'Copy failed', 'err');
    }
  });
}

const paperCtrlBodyEl = document.getElementById('paperCtrlBody');
const paperCtrlChevron = document.getElementById('paperCtrlToggle');
const togglePaperCtrl = () => {
  paperCtrlBodyEl?.classList.toggle('open');
  paperCtrlChevron?.classList.toggle('open');
};
document.getElementById('paperCtrlSummary')?.addEventListener('click', togglePaperCtrl);
paperCtrlChevron?.addEventListener('click', togglePaperCtrl);

const futuCtrlBodyEl = document.getElementById('futuCtrlBody');
const futuCtrlChevron = document.getElementById('futuCtrlToggle');
const toggleFutuCtrl = () => {
  futuCtrlBodyEl?.classList.toggle('open');
  futuCtrlChevron?.classList.toggle('open');
};
document.getElementById('futuCtrlSummary')?.addEventListener('click', toggleFutuCtrl);
futuCtrlChevron?.addEventListener('click', toggleFutuCtrl);

const toggleFutuSection = (targetId, forceOpen = null, toggleEl = null) => {
  if (!targetId) return;
  const body = document.getElementById(targetId);
  if (!body) return;
  const container = body.closest('.futu-terminal-section, .futu-drawer-card');
  const toggle = toggleEl || container?.querySelector(`[data-futu-section-target="${targetId}"]`);
  const willOpen = forceOpen == null ? !body.classList.contains('open') : !!forceOpen;

  body.classList.toggle('open', willOpen);
  body.setAttribute('aria-hidden', willOpen ? 'false' : 'true');
  body.style.display = willOpen ? 'block' : 'none';
  container?.classList.toggle('is-open', willOpen);
  toggle?.setAttribute('aria-expanded', willOpen ? 'true' : 'false');

  if (willOpen && body.querySelector('#futuChart')) {
    window.setTimeout(() => {
      try {
        futuChart.resize();
        futuChart.fit();
        renderFutuChartFromCurrentContext();
        renderFutuChartSummaryStrip();
      } catch (_) {
        // UI-only enhancement; ignore layout timing issues while the section opens.
      }
    }, 260);
  }
};

const normalizeFutuTerminalSections = () => {
  const tab = document.getElementById('tab-futu');
  const stack = tab?.querySelector('.futu-terminal-stack');
  if (!tab || !stack) return;

  const orderedNames = ['account', 'market', 'portfolio', 'workflow', 'audit'];
  let previousSection = null;

  for (const sectionName of orderedNames) {
    const section = document.querySelector(`[data-futu-section="${sectionName}"]`);
    if (!(section instanceof HTMLElement)) continue;

    if (section.parentElement !== stack) {
      stack.appendChild(section);
    }

    if (!previousSection) {
      if (stack.firstElementChild !== section) {
        stack.insertBefore(section, stack.firstElementChild);
      }
    } else if (previousSection.nextElementSibling !== section) {
      stack.insertBefore(section, previousSection.nextElementSibling);
    }

    previousSection = section;
  }
};

window.__toggleFutuSection = toggleFutuSection;

const syncFutuNativeDrawerState = (drawer) => {
  if (!(drawer instanceof HTMLElement)) return;
  const isOpen = !!drawer.open;
  const body = drawer.querySelector('.futu-drawer-body');
  const toggle = drawer.querySelector('.futu-drawer-toggle');

  drawer.classList.toggle('is-open', isOpen);
  if (body instanceof HTMLElement) {
    body.classList.toggle('open', isOpen);
    body.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
    body.style.display = isOpen ? 'block' : 'none';
  }
  if (toggle instanceof HTMLElement) {
    toggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  }
};

const bindFutuNativeDrawers = () => {
  document.querySelectorAll('#tab-futu details.futu-native-drawer').forEach((drawer) => {
    if (!(drawer instanceof HTMLElement)) return;
    if (drawer.dataset.futuNativeDrawerBound === '1') {
      syncFutuNativeDrawerState(drawer);
      return;
    }

    drawer.dataset.futuNativeDrawerBound = '1';
    syncFutuNativeDrawerState(drawer);
    drawer.addEventListener('toggle', () => {
      syncFutuNativeDrawerState(drawer);
    });
  });
};

const bindFutuSectionToggles = () => {
  document.querySelectorAll('#tab-futu .futu-section-toggle').forEach((toggle) => {
    if (toggle.dataset.futuInlineToggle === '1') return;
    if (toggle.dataset.futuToggleBound === '1') return;
    toggle.dataset.futuToggleBound = '1';
    toggle.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      const targetId = String(toggle.getAttribute('data-futu-section-target') || '').trim();
      if (!targetId) return;
      toggleFutuSection(targetId, null, toggle);
    });
  });
};

document.addEventListener('click', (event) => {
  const toggle = event?.target?.closest?.('#tab-futu .futu-section-toggle');
  if (!toggle) return;
  if (toggle.dataset.futuInlineToggle === '1') return;
  if (toggle.dataset.futuToggleBound === '1') return;
  event.preventDefault();
  const targetId = String(toggle.getAttribute('data-futu-section-target') || '').trim();
  if (!targetId) return;
  toggleFutuSection(targetId, null, toggle);
});

normalizeFutuTerminalSections();
bindFutuSectionToggles();
bindFutuNativeDrawers();

futuRecentLoads = loadFutuRecentLoads();
renderFutuRecentLoads();
refreshFutuAccountList();
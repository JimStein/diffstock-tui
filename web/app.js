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
let lastPortfolio = null;
let lastPaperSeries = [];
let latestQuoteMap = new Map();
let lastQuotesAt = 0;
let lastQuotesStampText = '--';
const actionStatus = document.getElementById('actionStatus');
const quotesAsOf = document.getElementById('quotesAsOf');
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
let lastForecastContext = null;
let forecastBatchResults = new Map(); // symbol → {data, selectedSymbol}
let forecastSelectedSymbol = null;
let paperTradeHistory = [];
let paperTradeSeenKeys = new Set();
let paperCostBasis = new Map();
let selectedTradeFilter = 'all';
let tradeSearchText = '';
let selectedPaperRangeDays = 0.5;
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
const FORECAST_BATCH_CACHE_KEY = 'diffstock:forecast-batch:v2';
const FORECAST_META_CACHE_KEY = 'diffstock:forecast-meta:v1';
let paperFullContext = {
  portfolioSeries: [],
  benchmarkSeries: [],
  metricsByTime: new Map(),
  latest: null,
};
const paperRangeButtons = Array.from(document.querySelectorAll('[data-paper-range-days]'));

const tradeFilterButtons = Array.from(document.querySelectorAll('[data-trade-filter]'));
const tradeSearchInput = document.getElementById('tradeSearchInput');

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
  quotesAsOf.textContent = `Current Price as of ${lastQuotesStampText}`;
};

const setDataSourceChip = (sourceRaw) => {
  const dataSourceChip = document.getElementById('dataSourceChip');
  const dataSourceDot = document.getElementById('dataSourceDot');
  const dataSourceLogChip = document.getElementById('dataSourceLogChip');
  if (!dataSourceChip) return;

  const source = String(sourceRaw || '').trim();
  if (!source || source.toLowerCase() === 'unknown') {
    dataSourceChip.textContent = 'Data: --';
    if (dataSourceDot) dataSourceDot.style.background = 'var(--muted-2)';
    return;
  }

  const lower = source.toLowerCase();
  let label = source;
  if (lower.includes('yahoo')) {
    label = 'Yfinance';
  }
  dataSourceChip.textContent = `Data: ${label}`;

  if (label !== dataSourceLastValue) {
    dataSourceLastValue = label;
    const now = new Date();
    const ts = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
    dataSourceSwitchLog.unshift(`${ts} ${label}`);
    dataSourceSwitchLog = dataSourceSwitchLog.slice(0, 5);
  }

  if (dataSourceLogChip) {
    dataSourceLogChip.textContent = dataSourceSwitchLog.length
      ? `SrcLog: ${dataSourceSwitchLog.join(' · ')}`
      : 'SrcLog: --';
  }

  if (dataSourceDot) {
    if (lower.includes('polygon-ws')) {
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
  const lines = dataSourceSwitchLog.length
    ? dataSourceSwitchLog.map((line, idx) => `${idx + 1}. ${line}`).join('\n')
    : 'No source switch history yet.';
  alert(`Data Source Switch Log\n\n${lines}`);
};

document.getElementById('dataSourceLogChip')?.addEventListener('click', showDataSourceLogPopup);

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
    for (const b of tabs) b.classList.remove('active');
    btn.classList.add('active');
    const t = btn.dataset.tab;
    for (const p of panels) p.classList.add('hidden');
    document.getElementById(`tab-${t}`).classList.remove('hidden');

    // Hidden tabs initialize with 0 width. Force chart reflow when tab becomes visible.
    if (t === 'forecast') {
      fChart.resize();
      fChart.fit();
    }
    if (t === 'paper') {
      paperChart.resize();
      paperChart.fit();
    }
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

const filterPaperContextByRangeDays = (ctx, rangeDays) => {
  const days = Number(rangeDays);
  if (!ctx || !Array.isArray(ctx.portfolioSeries) || ctx.portfolioSeries.length === 0) {
    return {
      portfolioSeries: [],
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

const renderPaperKpis = (paperStatus) => {
  const grid = document.getElementById('paperKpiGrid');
  if (!grid) return;

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
      <div class='paper-kpi-label'>Total Assets</div>
      <div class='paper-kpi-value'>${Number.isFinite(totalAssets) ? `$${totalAssets.toFixed(2)}` : '--'}</div>
    </div>
    <div class='paper-kpi-card ${pnlMood}'>
      <div class='paper-kpi-label'>Session PnL</div>
      <div class='paper-kpi-value ${Number.isFinite(pnlUsd) && pnlUsd < 0 ? 'down' : 'up'}'>${Number.isFinite(pnlUsd) ? `${formatSignedMoney(pnlUsd)} (${formatPct(pnlPct)})` : '--'}</div>
    </div>
    <div class='paper-kpi-card ${investedMood}'>
      <div class='paper-kpi-label'>Open Risk</div>
      <div class='paper-kpi-value ${investedClass}'>${Number.isFinite(investedPct) ? `${investedPct.toFixed(1)}%` : '--'}</div>
    </div>
    <div class='paper-kpi-card ${ddMood}'>
      <div class='paper-kpi-label'>Max Drawdown</div>
      <div class='paper-kpi-value ${Number.isFinite(maxDrawdownPct) && maxDrawdownPct < 0 ? 'down' : ''}'>${Number.isFinite(maxDrawdownPct) ? `${maxDrawdownPct.toFixed(2)}%` : '--'}</div>
    </div>
    <div class='paper-kpi-card ${winMood}'>
      <div class='paper-kpi-label'>Win Rate (SELL)</div>
      <div class='paper-kpi-value'>${Number.isFinite(winRate) ? `${winRate.toFixed(1)}%` : '--'}</div>
    </div>
    <div class='paper-kpi-card ${spreadMood}'>
      <div class='paper-kpi-label'>vs Benchmark</div>
      <div class='paper-kpi-value ${spreadClass}'>${Number.isFinite(spreadPct) ? `${spreadPct >= 0 ? '+' : ''}${spreadPct.toFixed(2)}%` : '--'}</div>
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

const showPaperLegend = () => {
  if (!paperLegend) return;
  paperLegend.classList.add('visible');
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
    const current = latestQuoteMap.get(f.symbol) ?? paperMap.get(f.symbol);
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

  // Weight allocation bar
  const WEIGHT_COLORS = ['#3b82f6','#8b5cf6','#00d4aa','#f59e0b','#ff4757','#ec4899','#06b6d4','#84cc16','#f97316','#a78bfa'];
  const barWrap = document.getElementById('portfolioWeightBar');
  if (barWrap && alloc.weights && alloc.weights.length > 0) {
    barWrap.style.display = '';
    const sorted = [...alloc.weights].sort((a, b) => b[1] - a[1]);
    let segments = '';
    let legendItems = '';
    sorted.forEach(([sym, w], i) => {
      const color = WEIGHT_COLORS[i % WEIGHT_COLORS.length];
      const pct = (w * 100).toFixed(1);
      segments += `<div class='weight-bar-seg' style='flex-basis:${pct}%;background:${color};' title='${sym}: ${pct}%'>${w > 0.06 ? sym : ''}</div>`;
      legendItems += `<span class='weight-bar-legend-item'><span class='weight-bar-legend-dot' style='background:${color}'></span>${sym} ${pct}%</span>`;
    });
    barWrap.innerHTML = `
      <div class='weight-bar-label'>Optimal Weight Allocation</div>
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
    const candidateSymbols = Array.from(targetMap.keys());
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
    const currentPrice = row?.price ?? latestQuoteMap.get(sym);
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
    <td><strong>CASH</strong></td>
    <td>Balance</td>
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
    <td><strong>TOTAL ASSETS</strong></td>
    <td>Portfolio</td>
    <td class='num'>${safeTotal > 0 ? `${cashWeightPct.toFixed(2)}% cash` : '--'}</td>
    <td class='num'>${Number.isFinite(cashUsd) ? '$' + cashUsd.toFixed(2) : '--'}</td>
    <td class='num'>${safeTotal > 0 ? `$${investedValue.toFixed(2)} (${investedWeightPct.toFixed(2)}%)` : '--'}</td>
    <td class='num'>${Number.isFinite(totalAssets) ? '$' + totalAssets.toFixed(2) : '--'}</td>
    <td class='num ${totalPnlClass} ${returnBgClass}'>${returnText}</td>
    <td class='num ${totalPnlClass} ${returnBgClass}' title='${pnlTitle}'>${pnlText}</td>
  `;
  tb.appendChild(totalTr);
};

const refreshRealtimeQuotes = async () => {
  const typedSymbols = (document.getElementById('pSymbols')?.value || '')
    .split(',')
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
  const portfolioSymbols = (lastPortfolio?.asset_forecasts || [])
    .map((x) => String(x.symbol || '').toUpperCase())
    .filter(Boolean);
  const symbols = Array.from(new Set([...portfolioSymbols, ...typedSymbols]));
  if (!symbols.length) return;
  const now = Date.now();
  if (now - lastQuotesAt < 25000) return;
  lastQuotesAt = now;
  try {
    const q = await api('/api/quotes', {
      method: 'POST',
      body: JSON.stringify({ symbols }),
    });
    latestQuoteMap = new Map(Object.entries(q.prices || {}));
    const now = new Date();
    lastQuotesStampText = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
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

window.addEventListener('resize', () => {
  fChart.resize();
  paperChart.resize();
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
    setDataSourceChip(st?.data_live_source);
    setPaperApplyAutoOptimizing(!!st?.auto_optimizing);
    hydratePaperOptimizationFromStatus(st);
    setPaperStatusChip(st);
    syncPaperButtons(st);
    hydratePaperTargetsFromStatus(st);

    const ctx = buildPaperSeriesContext(st.snapshots || []);
    paperFullContext = ctx.portfolioSeries.length > 0 ? ctx : buildFallbackPaperContext(st.latest_snapshot);
    renderPaperChartFromCurrentContext();
    renderChartSummaryStrip();
    await refreshRealtimeQuotes();

    const rtGrid = document.getElementById('rtMarketGrid');
    if (rtGrid) rtGrid.innerHTML = '';

    const snapshot = st.latest_snapshot;
    const holdings = new Set(snapshot?.holdings_symbols || []);
    const forecastMap = new Map((lastPortfolio?.asset_forecasts || []).map(x => [x.symbol, x.current_price]));
    const snapshotMap = new Map((snapshot?.symbols || []).map(x => [x.symbol, x]));
    const typedInputs = (document.getElementById('pSymbols')?.value || '')
      .split(',')
      .map(s => s.trim().toUpperCase())
      .filter(Boolean);

    const allInputs = new Set([...(lastPortfolio?.asset_forecasts || []).map(x => x.symbol), ...typedInputs]);
    for (const s of (snapshot?.symbols || [])) allInputs.add(s.symbol);

    if (rtGrid) {
      if (allInputs.size === 0) {
        rtGrid.innerHTML = `<div class='empty-state'><div class='empty-state-icon'>📡</div>No symbols tracked yet.<br>Run portfolio optimization or start paper trading.</div>`;
      } else {
        for (const sym of allInputs) {
          const row = snapshotMap.get(sym);
          const forecastPx = Number(forecastMap.get(sym));
          const quotePx = Number(latestQuoteMap.get(sym));
          const fallback = Number.isFinite(forecastPx) ? forecastPx : null;
          const rowPrice = Number(row?.price);
          const rtPrice = Number.isFinite(rowPrice) ? rowPrice : (Number.isFinite(quotePx) ? quotePx : null);
          const source = rtPrice != null ? 'rt' : 'forecast';
          const price = rtPrice ?? fallback;
          const isHolding = holdings.has(sym);

          // 1m change from snapshot
          const ch = row?.change_1m;
          const chPct = row?.change_1m_pct;
          const hasCh = Number.isFinite(ch);
          const chSign = hasCh ? (ch >= 0 ? '+' : '') : '';
          const chClass = hasCh ? (ch >= 0 ? 'up' : 'down') : '';
          const cardMood = hasCh ? (ch >= 0 ? 'card-up' : 'card-down') : 'card-neutral';

          const sourceBadge = source === 'rt'
            ? `<span class='source-badge rt'>RT</span>`
            : `<span class='source-badge forecast'>FC</span>`;
          const updatedText = source === 'rt' ? lastQuotesStampText : '--';

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
              <span class='rt-card-updated'>${updatedText}</span>
            </div>
          `;
          rtGrid.appendChild(card);
        }
      }
    }

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

const buildPaperTargetsPayload = (symbols) => {
  const weight = symbols.length > 0 ? 1 / symbols.length : 0;
  return symbols.map(symbol => ({ symbol, weight }));
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

refreshPaper();
refreshTrain();
renderPaperTargetChips();

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

    const restoredFromLocalBatch = restoreForecastBatchCache();
    let restoredByRefetch = false;
    if (!restoredFromLocalBatch) {
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

    if (state.train) {
      const rawBox = document.getElementById('trainRawJson');
      if (rawBox) rawBox.textContent = JSON.stringify(state.train, null, 2);
    }

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
  const backendChip = document.getElementById('backendChip');
  const backendDot = document.getElementById('backendDot');
  if (!backendChip) return;
  try {
    const st = await api('/api/state');
    setDataSourceChip(st?.data_live_source || st?.paper?.data_live_source);
    const backend = st?.forecast?.last_request?.compute_backend || st?.compute_backend || null;
    if (backend) {
      const lower = String(backend).toLowerCase();
      const label = lower === 'directml'
        ? 'DirectML'
        : lower === 'cuda'
          ? 'CUDA'
          : lower === 'cpu'
            ? 'CPU'
            : (backend.charAt(0).toUpperCase() + backend.slice(1));
      backendChip.textContent = `Backend: ${label}`;
      if (backendDot) { backendDot.style.background = lower === 'cpu' ? 'var(--muted)' : 'var(--accent)'; }
    } else {
      backendChip.textContent = 'Backend: --';
      if (backendDot) { backendDot.style.background = 'var(--muted-2)'; }
    }
  } catch {
    backendChip.textContent = 'Backend: --';
    if (backendDot) { backendDot.style.background = 'var(--muted-2)'; }
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
};
document.getElementById('paperCapital')?.addEventListener('input', syncPaperCtrlBadges);
document.getElementById('paperTime1')?.addEventListener('input', syncPaperCtrlBadges);
document.getElementById('paperTime2')?.addEventListener('input', syncPaperCtrlBadges);
paperOptTimeInput?.addEventListener('input', () => {
  syncNextOptimizationBadge();
  persistPaperOptimizationSettings(250);
});
paperOptWeekdayChecks.forEach(el => el.addEventListener('change', () => {
  syncNextOptimizationBadge();
  persistPaperOptimizationSettings(120);
}));
syncPaperCtrlBadges();
setInterval(syncNextOptimizationBadge, 60000);

const paperCtrlBodyEl = document.getElementById('paperCtrlBody');
const paperCtrlChevron = document.getElementById('paperCtrlToggle');
const togglePaperCtrl = () => {
  paperCtrlBodyEl?.classList.toggle('open');
  paperCtrlChevron?.classList.toggle('open');
};
document.getElementById('paperCtrlSummary')?.addEventListener('click', togglePaperCtrl);
paperCtrlChevron?.addEventListener('click', togglePaperCtrl);
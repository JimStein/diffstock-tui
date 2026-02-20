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
let paperTradeHistory = [];
let paperTradeSeenKeys = new Set();
let paperCostBasis = new Map();
let selectedTradeFilter = 'all';
let tradeSearchText = '';
let selectedPaperRangeDays = 0.5;
let paperSessionStartMs = null;
let paperFullContext = {
  portfolioSeries: [],
  benchmarkSeries: [],
  metricsByTime: new Map(),
  latest: null,
};
const paperRangeButtons = Array.from(document.querySelectorAll('[data-paper-range-days]'));

const tradeFilterButtons = Array.from(document.querySelectorAll('[data-trade-filter]'));
const tradeSearchInput = document.getElementById('tradeSearchInput');

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
      <div class="legend-row"><span class="legend-key">P50 Target</span><span class="legend-val" id="legendForecastP50">--</span></div>
      <div class="legend-row"><span class="legend-key">P10 Target</span><span class="legend-val" id="legendForecastP10">--</span></div>
      <div class="legend-row"><span class="legend-key">P90 Target</span><span class="legend-val" id="legendForecastP90">--</span></div>
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
    timeScale: { timeVisible: true, secondsVisible: false },
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
const historySeries = fChart.addLineSeries({ color: '#7e8aa5', lineWidth: 1 });
const p50Series = fChart.addLineSeries({ color: '#3b82f6', lineWidth: 2 });
const p10Series = fChart.addLineSeries({ color: '#ff4757', lineWidth: 1 });
const p90Series = fChart.addLineSeries({ color: '#00d4aa', lineWidth: 1 });
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
    portfolioLine.setData(ensureVisibleSeries(filtered.portfolioSeries));
    benchmarkLine.setData(filtered.benchmarkSeries.length > 0 ? ensureVisibleSeries(filtered.benchmarkSeries) : []);
    paperChart.fit();
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

const formatMoney = (v) => `$${v.toFixed(2)}`;

const formatChartDate = (unixSec) => {
  if (!Number.isFinite(unixSec)) return '--';
  return new Date(unixSec * 1000).toLocaleDateString();
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

document.getElementById('runForecast').addEventListener('click', async () => {
  try {
    await withBusy('runForecast', 'Running...', 'Forecast updated', async () => {
      const payload = {
        symbol: document.getElementById('fSymbol').value.trim(),
        horizon: Number(document.getElementById('fHorizon').value),
        simulations: Number(document.getElementById('fSims').value),
      };
      const data = await api('/api/forecast', { method: 'POST', body: JSON.stringify(payload) });
      document.getElementById('fSymbol').value = payload.symbol;
      document.getElementById('fHorizon').value = payload.horizon;
      document.getElementById('fSims').value = payload.simulations;
      historySeries.setData(toSeries(data.history));
      p10Series.setData(toSeries(data.p10));
      p50Series.setData(toSeries(data.p50));
      p90Series.setData(toSeries(data.p90));
      setForecastLegend(data);
      fChart.fit();
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
  for (const [symbol, weight] of (lastPortfolio?.weights || [])) {
    const normalized = String(symbol || '').toUpperCase();
    const w = Number(weight);
    if (!normalized || !Number.isFinite(w)) continue;
    if (!targetMap.has(normalized)) {
      targetMap.set(normalized, w);
    }
  }

  const symbols = new Set();
  for (const symbol of holdingsSet) symbols.add(symbol);
  for (const symbol of targetMap.keys()) symbols.add(symbol);

  const orderedSymbols = Array.from(symbols).sort();
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
    const isHolding = holdingsSet.has(sym);
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
      <td class='${isHolding ? 'up' : ''}'>${isHolding ? 'Holding' : 'Target Only'}</td>
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
  const badge = document.getElementById('paperTabBadge');
  if (!status.running) {
    chip.textContent = 'IDLE';
    if (dot) { dot.classList.remove('active', 'paused'); }
    if (badge) { badge.classList.remove('running', 'paused'); }
    paperSessionStartMs = null;
  } else if (status.paused) {
    chip.textContent = 'PAUSED';
    if (dot) { dot.classList.remove('active'); dot.classList.add('paused'); }
    if (badge) { badge.classList.remove('running'); badge.classList.add('paused'); }
  } else {
    chip.textContent = 'RUNNING';
    if (dot) { dot.classList.remove('paused'); dot.classList.add('active'); }
    if (badge) { badge.classList.remove('paused'); badge.classList.add('running'); }
    if (!paperSessionStartMs) paperSessionStartMs = Date.now();
  }

  // Next run chip
  const nextRunChip = document.getElementById('nextRunChip');
  if (nextRunChip) {
    if (status.running && !status.paused && status.next_run_at) {
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
    setPaperStatusChip(st);
    syncPaperButtons(st);

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
    if (logBox) logBox.innerHTML = (st.logs || []).slice(-20).map(x => `<div>${x}</div>`).join('');
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
  } catch (e) {
    setStatus(e.message || String(e), 'err');
    alert(e.message);
  }
};

document.getElementById('paperStart').addEventListener('click', async () => {
  if (!lastPortfolio) {
    alert('Run Portfolio optimization first.');
    return;
  }
  const targets = lastPortfolio.weights.map(([symbol, weight]) => ({ symbol, weight }));
  await paperControl('/api/paper/start', {
    targets,
    initial_capital: Number(document.getElementById('paperCapital').value),
    time1: document.getElementById('paperTime1').value,
    time2: document.getElementById('paperTime2').value,
  }, { resetTradeState: true });
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
    }, { resetTradeState: true });
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
      }, { resetTradeState: true });
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

const restoreState = async () => {
  try {
    const state = await api('/api/state');

    if (state.forecast?.last_request) {
      document.getElementById('fSymbol').value = state.forecast.last_request.symbol || document.getElementById('fSymbol').value;
      document.getElementById('fHorizon').value = state.forecast.last_request.horizon || document.getElementById('fHorizon').value;
      document.getElementById('fSims').value = state.forecast.last_request.simulations || document.getElementById('fSims').value;
    }
    if (state.forecast?.last_result) {
      const r = state.forecast.last_result;
      historySeries.setData(toSeries(r.history || []));
      p10Series.setData(toSeries(r.p10 || []));
      p50Series.setData(toSeries(r.p50 || []));
      p90Series.setData(toSeries(r.p90 || []));
      fChart.fit();
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

    setStatus('State restored from backend', 'ok');
  } catch (e) {
    setStatus(`State restore failed: ${e.message}`, 'err');
  }
};

restoreState();
renderQuotesAsOf();
renderTradeHistory();

// Backend chip: detect compute backend from /api/state
(async () => {
  const backendChip = document.getElementById('backendChip');
  const backendDot = document.getElementById('backendDot');
  if (!backendChip) return;
  try {
    const st = await api('/api/state');
    const backend = st?.forecast?.last_request?.compute_backend || st?.compute_backend || null;
    if (backend) {
      const label = backend.charAt(0).toUpperCase() + backend.slice(1);
      backendChip.textContent = `Backend: ${label}`;
      if (backendDot) { backendDot.style.background = backend === 'cpu' ? 'var(--muted)' : 'var(--accent)'; }
    } else {
      backendChip.textContent = 'Backend: CPU';
    }
  } catch {
    backendChip.textContent = 'Backend: --';
  }
})();
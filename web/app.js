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
let paperMetricsByTime = new Map();

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
      fChart.fit();
    });
  } catch (e) { alert(e.message); }
});

const fillAssetTable = (alloc, paperStatus) => {
  const paperMap = new Map((paperStatus?.latest_snapshot?.symbols || []).map(x => [x.symbol, x.price]));
  const tb = document.querySelector('#assetTable tbody');
  tb.innerHTML = '';
  for (const f of alloc.asset_forecasts) {
    const current = latestQuoteMap.get(f.symbol) ?? paperMap.get(f.symbol);
    const dev = current == null ? null : (current - f.current_price);
    const devPct = current == null ? null : (dev / f.current_price * 100);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${f.symbol}</td>
      <td class='num'>$${f.current_price.toFixed(2)}</td>
      <td class='num'>${current == null ? '--' : '$' + current.toFixed(2)}</td>
      <td class='num ${dev == null ? '' : (dev >= 0 ? 'up' : 'down')}'>${dev == null ? '--' : `${dev >=0 ? '+' : ''}${dev.toFixed(2)} (${devPct >=0 ? '+' : ''}${devPct.toFixed(2)}%)`}</td>
      <td class='num ${f.annual_return >=0 ? 'up' : 'down'}'>${(f.annual_return*100).toFixed(1)}%</td>
      <td class='num'>${(f.annual_vol*100).toFixed(1)}%</td>
      <td class='num ${f.sharpe >= 0 ? 'up':'down'}'>${f.sharpe.toFixed(2)}</td>
      <td class='num ${f.p50_price >= f.current_price ? 'up':'down'}'>$${f.p50_price.toFixed(2)}</td>`;
    tb.appendChild(tr);
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
  const targetMap = new Map((lastPortfolio?.weights || []).map(([symbol, weight]) => [symbol, weight]));

  const symbols = new Set();
  for (const symbol of holdingsSet) symbols.add(symbol);
  for (const symbol of targetMap.keys()) symbols.add(symbol);

  const orderedSymbols = Array.from(symbols).sort();
  if (orderedSymbols.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan='7' class='small'>No holdings data yet. Run portfolio optimization and start paper trading.</td>`;
    tb.appendChild(tr);
    return;
  }

  for (const sym of orderedSymbols) {
    const row = snapshotMap.get(sym);
    const holding = holdingsMap.get(sym);
    const targetWeight = targetMap.get(sym);
    const currentPrice = row?.price ?? latestQuoteMap.get(sym);
    const change1mPct = row?.change_1m_pct;
    const quantity = holding?.quantity ?? (holdingsSet.has(sym) ? 0 : null);
    const assetValue = holding?.asset_value ?? (quantity != null && currentPrice != null ? quantity * currentPrice : null);
    const isHolding = holdingsSet.has(sym);

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${sym}</td>
      <td class='${isHolding ? 'up' : ''}'>${isHolding ? 'Holding' : 'Target Only'}</td>
      <td class='num'>${quantity == null ? '--' : quantity.toFixed(2)}</td>
      <td class='num'>${currentPrice == null ? '--' : '$' + currentPrice.toFixed(2)}</td>
      <td class='num'>${assetValue == null ? '--' : '$' + assetValue.toFixed(2)}</td>
      <td class='num'>${targetWeight == null ? '--' : `${(targetWeight * 100).toFixed(2)}%`}</td>
      <td class='num ${change1mPct == null ? '' : (change1mPct >= 0 ? 'up' : 'down')}'>${change1mPct == null ? '--' : `${change1mPct >= 0 ? '+' : ''}${change1mPct.toFixed(3)}%`}</td>
    `;
    tb.appendChild(tr);
  }

  const cashUsd = snapshot?.cash_usd;
  const totalAssets = snapshot?.total_value;

  const cashTr = document.createElement('tr');
  cashTr.innerHTML = `
    <td><strong>CASH</strong></td>
    <td>Balance</td>
    <td class='num'>--</td>
    <td class='num'>--</td>
    <td class='num'>${cashUsd == null ? '--' : '$' + cashUsd.toFixed(2)}</td>
    <td class='num'>--</td>
    <td class='num'>--</td>
  `;
  tb.appendChild(cashTr);

  const totalTr = document.createElement('tr');
  totalTr.innerHTML = `
    <td><strong>TOTAL ASSETS</strong></td>
    <td>Portfolio</td>
    <td class='num'>--</td>
    <td class='num'>--</td>
    <td class='num'>${totalAssets == null ? '--' : '$' + totalAssets.toFixed(2)}</td>
    <td class='num'>--</td>
    <td class='num'>--</td>
  `;
  tb.appendChild(totalTr);
};

const refreshRealtimeQuotes = async () => {
  if (!lastPortfolio?.asset_forecasts?.length) return;
  const now = Date.now();
  if (now - lastQuotesAt < 25000) return;
  lastQuotesAt = now;

  const symbols = lastPortfolio.asset_forecasts.map(x => x.symbol);
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
  if (!status.running) {
    chip.textContent = 'IDLE';
    if (dot) { dot.classList.remove('active', 'paused'); }
  } else if (status.paused) {
    chip.textContent = 'PAUSED';
    if (dot) { dot.classList.remove('active'); dot.classList.add('paused'); }
  } else {
    chip.textContent = 'RUNNING';
    if (dot) { dot.classList.remove('paused'); dot.classList.add('active'); }
  }
};

const refreshPaper = async () => {
  try {
    const st = await api('/api/paper/status');
    setPaperStatusChip(st);
    syncPaperButtons(st);

    const ctx = buildPaperSeriesContext(st.snapshots || []);
    paperMetricsByTime = ctx.metricsByTime;

    const series = ctx.portfolioSeries;
    const benchmarkSeries = ctx.benchmarkSeries;
    if (series.length > 0) {
      lastPaperSeries = series;
      portfolioLine.setData(ensureVisibleSeries(lastPaperSeries));
      if (benchmarkSeries.length > 0) {
        benchmarkLine.setData(ensureVisibleSeries(benchmarkSeries));
      }
      paperChart.fit();

      setLegendText(ctx.latest);
    } else if (st.latest_snapshot) {
      const t = Math.floor(new Date(st.latest_snapshot.timestamp).getTime() / 1000);
      if (Number.isFinite(t) && t > 0) {
        const single = [{ time: t, value: st.latest_snapshot.total_value }];
        const baseline = Math.max(1, st.latest_snapshot.total_value - (st.latest_snapshot.pnl_usd || 0));
        const benchValue = baseline * (1 + (st.latest_snapshot.benchmark_return_pct || 0) / 100);
        const singleBench = [{ time: t, value: benchValue }];
        lastPaperSeries = single;
        portfolioLine.setData(ensureVisibleSeries(single));
        benchmarkLine.setData(ensureVisibleSeries(singleBench));
        paperChart.fit();

        const fallbackMetrics = {
          time: t,
          updatedText: st.latest_snapshot.timestamp,
          portfolioValue: st.latest_snapshot.total_value,
          portfolioPnlUsd: st.latest_snapshot.pnl_usd || 0,
          portfolioPnlPct: st.latest_snapshot.pnl_pct || 0,
          benchmarkValue: benchValue,
          benchmarkPnlUsd: benchValue - baseline,
          benchmarkPnlPct: st.latest_snapshot.benchmark_return_pct || 0,
          spreadUsd: st.latest_snapshot.total_value - benchValue,
          spreadPct: benchValue !== 0 ? ((st.latest_snapshot.total_value - benchValue) / benchValue) * 100 : 0,
        };
        setLegendText(fallbackMetrics);
      }
    } else {
      setLegendText(null);
    }

    const rtTb = document.querySelector('#rtTable tbody');
    const chTb = document.querySelector('#chTable tbody');
    rtTb.innerHTML = '';
    chTb.innerHTML = '';

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

    for (const sym of allInputs) {
      const row = snapshotMap.get(sym);
      const fallback = forecastMap.get(sym);
      const price = row?.price ?? fallback;
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${sym}</td><td class='num'>${price == null ? '--' : '$'+price.toFixed(2)}</td><td>${holdings.has(sym)?'Holding':'Input'}</td>`;
      rtTb.appendChild(tr);
    }

    for (const sym of allInputs) {
      const s = snapshotMap.get(sym);
      if (!s) continue;
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${s.symbol}</td><td class='num'>$${s.price.toFixed(2)}</td><td class='num ${s.change_1m>=0?'up':'down'}'>${s.change_1m>=0?'+':''}${s.change_1m.toFixed(3)}</td><td class='num ${s.change_1m_pct>=0?'up':'down'}'>${s.change_1m_pct>=0?'+':''}${s.change_1m_pct.toFixed(3)}%</td>`;
      chTb.appendChild(tr);
    }

    const logBox = document.getElementById('logBox');
    logBox.innerHTML = (st.logs || []).slice(-20).map(x => `<div>${x}</div>`).join('');
    fillHoldingsTable(st);

    if (lastPortfolio) fillAssetTable(lastPortfolio, st);
    await refreshRealtimeQuotes();
    if (lastPortfolio) fillAssetTable(lastPortfolio, st);
  } catch (e) {
    console.warn(e);
  }
};

setInterval(refreshPaper, 4000);

const paperControl = async (path, body={}) => {
  try {
    await api(path, { method: 'POST', body: JSON.stringify(body) });
    setStatus(`Action success: ${path}`, 'ok');
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
  });
});
document.getElementById('paperPause').addEventListener('click', () => paperControl('/api/paper/pause'));
document.getElementById('paperResume').addEventListener('click', () => paperControl('/api/paper/resume'));
document.getElementById('paperStop').addEventListener('click', () => paperControl('/api/paper/stop'));

const refreshTrain = async () => {
  try {
    const st = await api('/api/train/status');
    document.getElementById('trainStatus').textContent = JSON.stringify(st, null, 2);
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
      const ctx = buildPaperSeriesContext(state.paper.snapshots || []);
      paperMetricsByTime = ctx.metricsByTime;

      const series = ctx.portfolioSeries;
      const benchmarkSeries = ctx.benchmarkSeries;
      if (series.length > 0) {
        lastPaperSeries = series;
        portfolioLine.setData(ensureVisibleSeries(lastPaperSeries));
        if (benchmarkSeries.length > 0) {
          benchmarkLine.setData(ensureVisibleSeries(benchmarkSeries));
        }
        paperChart.fit();

        setLegendText(ctx.latest);
      } else if (state.paper.latest_snapshot) {
        const baseline = Math.max(1, state.paper.latest_snapshot.total_value - (state.paper.latest_snapshot.pnl_usd || 0));
        const benchValue = baseline * (1 + (state.paper.latest_snapshot.benchmark_return_pct || 0) / 100);
        setLegendText({
          time: Math.floor(new Date(state.paper.latest_snapshot.timestamp).getTime() / 1000),
          updatedText: state.paper.latest_snapshot.timestamp,
          portfolioValue: state.paper.latest_snapshot.total_value,
          portfolioPnlUsd: state.paper.latest_snapshot.pnl_usd || 0,
          portfolioPnlPct: state.paper.latest_snapshot.pnl_pct || 0,
          benchmarkValue: benchValue,
          benchmarkPnlUsd: benchValue - baseline,
          benchmarkPnlPct: state.paper.latest_snapshot.benchmark_return_pct || 0,
          spreadUsd: state.paper.latest_snapshot.total_value - benchValue,
          spreadPct: benchValue !== 0 ? ((state.paper.latest_snapshot.total_value - benchValue) / benchValue) * 100 : 0,
        });
      } else {
        setLegendText(null);
      }

      fillHoldingsTable(state.paper);
    }

    if (state.train) {
      document.getElementById('trainStatus').textContent = JSON.stringify(state.train, null, 2);
    }

    setStatus('State restored from backend', 'ok');
  } catch (e) {
    setStatus(`State restore failed: ${e.message}`, 'err');
  }
};

restoreState();
renderQuotesAsOf();
// Review & Optimisation Engine v4
// GET  /api/review?action=weekly    — weekly performance review email
// GET  /api/review?action=optimize  — run walk-forward optimisation
// POST /api/review                  — trigger manually
const fetch = require('node-fetch');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || req.body?.action || 'weekly';
  if (action === 'optimize') return await runOptimization(req, res);
  if (action === 'backtest') return await runBacktest(req, res);
  if (action === 'pairs_backtest') return await runPairsBacktest(req, res);
  if (action === 'discover') return await runDiscovery(req, res);
  if (action === 'novel') return await runNovelDiscovery(req, res);
  if (action === 'aipatterns') return await runAIPatterns(req, res);
  if (action === 'deepanalysis') return await runDeepAnalysis(req, res);
  return await sendWeeklyReview(req, res);
};

// ─── BACKTEST ENGINE ──────────────────────────────────────────────────────────
async function runBacktest(req, res) {
  const { sql } = require('@vercel/postgres');
  const epic = req.query.epic || 'IX.D.FTSE.DAILY.IP';
  const days = parseInt(req.query.days || '500');
  // threshold param from UI doubles as rsiEntry override (1-7 maps to 28-40)
  const thresholdParam = parseInt(req.query.threshold || '0');
  const rsiEntry = req.query.rsiEntry ? parseFloat(req.query.rsiEntry)
    : thresholdParam >= 1 ? 28 + (thresholdParam - 1) * 2
    : 33;
  const resolution = req.query.resolution || 'DAY'; // DAY or HOUR
  const strategy = req.query.strategy || 'mr'; // mr, sma, momentum, breakout, all
  const stopMult = parseFloat(req.query.stopMult || '1.5'); // ATR stop multiplier
  const holdDays = parseInt(req.query.holdDays || '5');
  const log = [];
  const L = msg => log.push(msg);

  // Instrument name map
  const INSTR_MAP = {
    'IX.D.FTSE.DAILY.IP':'FTSE 100','IX.D.SPTRD.DAILY.IP':'S&P 500',
    'IX.D.DAX.DAILY.IP':'DAX 40','IX.D.DOW.DAILY.IP':'Dow Jones',
    'CC.D.LCO.USS.IP':'Brent Oil','CS.D.USCGC.TODAY.IP':'Gold',
    'CS.D.USCSI.TODAY.IP':'Silver','CS.D.GBPUSD.TODAY.IP':'GBP/USD',
    'CS.D.EURUSD.TODAY.IP':'EUR/USD','CS.D.EURGBP.TODAY.IP':'EUR/GBP',
    'CS.D.USDJPY.TODAY.IP':'USD/JPY','CS.D.COPPER.TODAY.IP':'Copper',
    'IX.D.CAC.DAILY.IP':'CAC 40','IX.D.NASDAQ.CASH.IP':'Nasdaq',
    'IX.D.NIKKEI.DAILY.IP':'Nikkei 225',
  };
  const instrName = INSTR_MAP[epic] || epic;

  try {
    // Fetch candles
    const rows = await sql`
      SELECT close_price, candle_time as dt
      FROM price_history
      WHERE (epic = ${epic} OR instrument = ${instrName}
             OR instrument ILIKE ${'%' + instrName + '%'})
      AND resolution = ${resolution} AND close_price > 0
      ORDER BY candle_time ASC
      LIMIT ${resolution === 'HOUR' ? days * 24 : days + 50}`;

    if (rows.rows.length < 10) {
      return res.status(200).json({ error: `Insufficient ${resolution} data — only ${rows.rows.length} candles. Run hourly backfill first.` });
    }

    const closes = rows.rows.map(r => parseFloat(r.close_price));
    const dates = rows.rows.map(r => {
      const d = new Date(r.dt);
      return resolution === 'HOUR'
        ? d.toISOString().substring(0,16).replace('T',' ')
        : d.toISOString().substring(0,10);
    });
    L(`Backtest: ${instrName} — ${closes.length} candles from ${dates[0]} to ${dates[dates.length-1]}`);

    // Helper functions
    function calcRSI(prices, period=14) {
      if(prices.length < period+1) return 50;
      let gains=0, losses=0;
      for(let i=1;i<=period;i++){
        const d=prices[i]-prices[i-1];
        if(d>0)gains+=d; else losses-=d;
      }
      let avgGain=gains/period, avgLoss=losses/period;
      for(let i=period+1;i<prices.length;i++){
        const d=prices[i]-prices[i-1];
        avgGain=(avgGain*(period-1)+(d>0?d:0))/period;
        avgLoss=(avgLoss*(period-1)+(d<0?-d:0))/period;
      }
      return avgLoss===0 ? 100 : 100-(100/(1+avgGain/avgLoss));
    }

    function calcSMA(prices, period) {
      if(prices.length < period) return null;
      return prices.slice(-period).reduce((a,b)=>a+b,0)/period;
    }

    function calcATR(prices, period=14) {
      if(prices.length < period+1) return 0;
      let sum=0;
      for(let i=prices.length-period;i<prices.length;i++){
        sum+=Math.abs(prices[i]-prices[i-1]);
      }
      return sum/period;
    }

    // Run simulation
    const trades = [];
    const minCandles = 30;

    for(let i=minCandles; i<closes.length-holdDays; i++) {
      const slice = closes.slice(0, i+1);
      const rsi = calcRSI(slice);
      const sma10 = calcSMA(slice, 10);
      const sma20 = calcSMA(slice, 20);
      const sma50 = calcSMA(slice, 50);
      const atr = calcATR(slice);
      const price = closes[i];

      // Score calculation (simplified)
      let score = 0;
      if(sma10 && sma20) score += sma10 > sma20 ? 1 : -1;
      const mom = i>5 ? (price - closes[i-5]) / closes[i-5] * 100 : 0;
      score += mom > 1 ? 1 : mom < -1 ? -1 : 0;

      // Trend filter
      const inDowntrend = sma10 && sma20 && sma10 < sma20 * 0.998;
      const inUptrend = sma10 && sma20 && sma10 > sma20 * 1.002;

      // Check for signals based on strategy
      let direction = null;
      let signalType = null;

      if(strategy === 'mr' || strategy === 'all') {
        // Mean reversion BUY — oversold
        if(rsi <= rsiEntry && !inDowntrend) {
          direction = 'BUY'; signalType = 'MR_BUY';
        } else if(rsi >= (100 - rsiEntry) && !inUptrend) {
          direction = 'SELL'; signalType = 'MR_SELL';
        }
      }

      if((strategy === 'sma' || strategy === 'all') && i >= 55) {
        // SMA crossover — only fires on crossover day
        const sma20now = closes.slice(i-19, i+1).reduce((a,b)=>a+b,0)/20;
        const sma50now = closes.slice(i-49, i+1).reduce((a,b)=>a+b,0)/50;
        const sma20prev = closes.slice(i-20, i).reduce((a,b)=>a+b,0)/20;
        const sma50prev = closes.slice(i-50, i).reduce((a,b)=>a+b,0)/50;
        if(sma20prev <= sma50prev && sma20now > sma50now) { direction='BUY'; signalType='SMA_CROSS'; }
        else if(sma20prev >= sma50prev && sma20now < sma50now) { direction='SELL'; signalType='SMA_CROSS'; }
      }

      if((strategy === 'momentum' || strategy === 'all') && !direction && i >= 5) {
        // 5-day momentum — only if no SMA signal
        const ret5 = (closes[i]-closes[i-5])/closes[i-5]*100;
        if(ret5 > 3.5) { direction='BUY'; signalType='MOMENTUM'; }
        else if(ret5 < -3.5) { direction='SELL'; signalType='MOMENTUM'; }
      }

      if((strategy === 'breakout' || strategy === 'all') && !direction && i >= 21) {
        // 20-day breakout — only if no other signal
        const high20 = Math.max(...closes.slice(i-20, i));
        const low20 = Math.min(...closes.slice(i-20, i));
        const price = closes[i], prev = closes[i-1];
        if(prev < high20 && price > high20) { direction='BUY'; signalType='BREAKOUT'; }
        else if(prev > low20 && price < low20) { direction='SELL'; signalType='BREAKOUT'; }
      }

      if(!direction) continue;

      // Score and trend filters
      // For trend strategies: trend filter CONFIRMS (same direction = good)
      // For mean reversion: trend filter BLOCKS (opposite direction = good)
      const isTrendStrategy = ['sma','momentum','breakout'].includes(strategy);
      
      const passesScore0 = true;
      const passesScore1 = isTrendStrategy ? Math.abs(score) >= 0 : Math.abs(score) >= 1;
      const passesScore2 = isTrendStrategy ? Math.abs(score) >= 0 : Math.abs(score) >= 2;

      // Calculate outcome — hold for holdDays or until opposite RSI extreme
      const entryPrice = closes[i];
      let exitPrice = closes[Math.min(i+holdDays, closes.length-1)];
      let exitDay = holdDays;
      let exitReason = 'time';

      // Check for early exit (RSI reversal)
      for(let j=1;j<=holdDays && i+j<closes.length;j++){
        const futureSlice = closes.slice(0, i+j+1);
        const futureRsi = calcRSI(futureSlice);
        if(direction==='BUY' && futureRsi >= 55) {
          exitPrice = closes[i+j]; exitDay=j; exitReason='rsi_exit'; break;
        }
        if(direction==='SELL' && futureRsi <= 45) {
          exitPrice = closes[i+j]; exitDay=j; exitReason='rsi_exit'; break;
        }
      }

      // ATR-based stop (configurable multiplier)
      const stopDist = atr * stopMult;
      let stopped = false;
      for(let j=1;j<=exitDay;j++){
        const p = closes[i+j];
        if(direction==='BUY' && p < entryPrice - stopDist) {
          exitPrice=p; exitDay=j; exitReason='stop'; stopped=true; break;
        }
        if(direction==='SELL' && p > entryPrice + stopDist) {
          exitPrice=p; exitDay=j; exitReason='stop'; stopped=true; break;
        }
      }

      // Track peak profit during hold period
      let peakPrice = entryPrice;
      let peakDay = 0;
      for(let j=1; j<=exitDay && i+j<closes.length; j++) {
        const p = closes[i+j];
        if(direction==='BUY' && p > peakPrice) { peakPrice=p; peakDay=j; }
        if(direction==='SELL' && p < peakPrice) { peakPrice=p; peakDay=j; }
      }
      const peakPnlPct = direction==='BUY'
        ? (peakPrice-entryPrice)/entryPrice*100
        : (entryPrice-peakPrice)/entryPrice*100;

      const pnlPct = direction==='BUY'
        ? (exitPrice-entryPrice)/entryPrice*100
        : (entryPrice-exitPrice)/entryPrice*100;
      const won = pnlPct > 0;
      // Was trade in profit at any point?
      const everProfitable = peakPnlPct > 0.1;
      // Did we leave money on table? (exited below peak)
      const leftOnTable = peakPnlPct - pnlPct;

      trades.push({
        date: dates[i], direction, signalType,
        entryPrice: entryPrice.toFixed(2),
        exitPrice: exitPrice.toFixed(2),
        peakPrice: peakPrice.toFixed(2),
        peakPnlPct: peakPnlPct.toFixed(2),
        peakDay, leftOnTable: leftOnTable.toFixed(2),
        everProfitable,
        rsi: rsi.toFixed(1), score,
        pnlPct: pnlPct.toFixed(2),
        exitDay, exitReason, won,
        passesScore0, passesScore1, passesScore2,
        trendOK: isTrendStrategy
          ? (direction==='BUY' ? !inDowntrend : !inUptrend)  // trend: need clear direction
          : (!inDowntrend && !inUptrend),  // mean rev: need neutral regime
      });

      // Skip ahead to avoid overlapping trades
      i += Math.max(2, exitDay);
    }

    // Calculate statistics
    const calcStats = (filtered) => {
      if(!filtered.length) return { trades:0, winRate:0, avgWin:0, avgLoss:0, expectancy:0, totalPnl:0 };
      const wins = filtered.filter(t=>t.won);
      const losses = filtered.filter(t=>!t.won);
      const avgWin = wins.length ? wins.reduce((s,t)=>s+parseFloat(t.pnlPct),0)/wins.length : 0;
      const avgLoss = losses.length ? losses.reduce((s,t)=>s+parseFloat(t.pnlPct),0)/losses.length : 0;
      const winRate = wins.length/filtered.length*100;
      const expectancy = (winRate/100)*avgWin + (1-winRate/100)*avgLoss;
      const totalPnl = filtered.reduce((s,t)=>s+parseFloat(t.pnlPct),0);
      return { trades:filtered.length, winRate:winRate.toFixed(1), avgWin:avgWin.toFixed(2),
               avgLoss:avgLoss.toFixed(2), expectancy:expectancy.toFixed(2), totalPnl:totalPnl.toFixed(2) };
    };

    // Stats at different filter levels
    const allTrades = trades;
    const withScore1 = trades.filter(t=>t.passesScore1);
    const withScore2 = trades.filter(t=>t.passesScore2);
    const withTrendFilter = trades.filter(t=>t.trendOK);
    const fullFilter = trades.filter(t=>t.passesScore2 && t.trendOK);

    const stats = {
      noFilter: calcStats(allTrades),
      scoreGte1: calcStats(withScore1),
      scoreGte2: calcStats(withScore2),
      trendFilter: calcStats(withTrendFilter),
      fullFilter: calcStats(fullFilter),
    };

    // Peak profit analysis
    const everProfTrades = allTrades.filter(t=>t.everProfitable);
    const avgLeftOnTable = allTrades.filter(t=>t.won||t.everProfitable)
      .reduce((s,t)=>s+parseFloat(t.leftOnTable),0) / Math.max(1, allTrades.filter(t=>t.won||t.everProfitable).length);
    const couldHaveWon = allTrades.filter(t=>!t.won && t.everProfitable).length;
    L(`Peak analysis: ${everProfTrades.length}/${allTrades.length} trades were profitable at some point`);
    L(`Trades that went positive but closed negative: ${couldHaveWon}`);
    L(`Avg profit left on table (winning+ever-profitable trades): ${avgLeftOnTable.toFixed(2)}%`);

    L(`Total signals: ${allTrades.length}`);
    L(`With score≥1: ${withScore1.length} (win rate: ${stats.scoreGte1.winRate}%)`);
    L(`With score≥2: ${withScore2.length} (win rate: ${stats.scoreGte2.winRate}%)`);
    L(`With trend filter: ${withTrendFilter.length} (win rate: ${stats.trendFilter.winRate}%)`);
    L(`Full filter (score≥2+trend): ${fullFilter.length} (win rate: ${stats.fullFilter.winRate}%)`);

    // Build summary in format expected by existing UI
    // For hourly, fall back to trendFilter if fullFilter has no trades
    const fs = stats.fullFilter.trades > 0 ? stats.fullFilter : stats.trendFilter;
    const summary = {
      totalTrades: fs.trades,
      winRate: parseFloat(fs.winRate),
      filterUsed: stats.fullFilter.trades > 0 ? 'score≥2+trend' : 'trend only',
      totalPnL: parseFloat(fs.totalPnl),
      profitFactor: fs.avgWin > 0 && Math.abs(parseFloat(fs.avgLoss)) > 0
        ? Math.abs(parseFloat(fs.avgWin) / parseFloat(fs.avgLoss))
        : parseFloat(fs.avgWin) > 0 ? 2 : 0,
      expectancy: parseFloat(fs.expectancy),
      avgWin: parseFloat(fs.avgWin),
      avgLoss: parseFloat(fs.avgLoss),
    };

    // Build byRegime from full filter trades
    const byRegime = {};
    fullFilter.forEach(t => {
      const r = 'ranging'; // simplified — all daily signals are ranging
      if(!byRegime[r]) byRegime[r] = { trades:0, pnl:0 };
      byRegime[r].trades++;
      byRegime[r].pnl += parseFloat(t.pnlPct);
    });

    // Recent trades for display
    const activeFilter = stats.fullFilter.trades > 0 ? fullFilter : withTrendFilter;
    const recentTrades = activeFilter.slice(-10).map(t => ({
      direction: t.direction,
      openPrice: t.entryPrice,
      closePrice: t.exitPrice,
      pnl: parseFloat(t.pnlPct),
      regime: 'ranging',
    }));

    // Test multiple stop multipliers automatically
    const stopTests = {};
    for(const sm of [0.5, 0.75, 1.0, 1.5, 2.0, 3.0]) {
      const filtered = allTrades.map(t => {
        // Re-simulate with different stop
        const entryP = parseFloat(t.entryPrice);
        const sd = parseFloat(t.entryPrice) * 0.01 * sm; // rough proxy
        return t;
      });
      stopTests[sm+'x'] = calcStats(allTrades.filter((t,i) => {
        // Can't re-simulate without price data here - use peak as proxy
        // If peak < stopMult×ATR from entry, trade would have been stopped out differently
        return true;
      }));
    }

    const peakAnalysis = {
      everProfitable: allTrades.filter(t=>t.everProfitable).length,
      totalTrades: allTrades.length,
      couldHaveWon: allTrades.filter(t=>!t.won && t.everProfitable).length,
      avgLeftOnTable: parseFloat((allTrades.filter(t=>t.won||t.everProfitable)
        .reduce((s,t)=>s+parseFloat(t.leftOnTable),0) / 
        Math.max(1,allTrades.filter(t=>t.won||t.everProfitable).length)).toFixed(2)),
      avgPeakPnl: parseFloat((allTrades.reduce((s,t)=>s+parseFloat(t.peakPnlPct),0)/Math.max(1,allTrades.length)).toFixed(2)),
      avgExitPnl: parseFloat((allTrades.reduce((s,t)=>s+parseFloat(t.pnlPct),0)/Math.max(1,allTrades.length)).toFixed(2)),
    };

    return res.status(200).json({
      instrument: instrName, epic, days: closes.length, resolution,
      rsiEntry, holdDays, stats, summary, byRegime, recentTrades,
      peakAnalysis,
      trades: allTrades.slice(-100),
      log
    });

  } catch(e) {
    return res.status(200).json({ error: e.message, log });
  }
}


// ─── POSITION SIZING ANALYSIS ────────────────────────────────────────────────
async function analysePositionSizing(trades, anthropicKey) {
  if (!trades || trades.length < 3) return null;

  // Only analyse closed trades with complete data
  const closed = trades.filter(t =>
    t.status === 'closed' &&
    t.profit_loss != null &&
    t.size != null &&
    t.open_level != null &&
    t.close_level != null
  );
  if (closed.length < 3) return null;

  // Calculate stop distance from actual trade data
  // stop_distance = |close_level - open_level| for stopped-out trades
  // or infer from size and max_loss if available
  const tradeStats = closed.map(t => {
    const pnl = parseFloat(t.profit_loss);
    const size = parseFloat(t.size);
    const openLevel = parseFloat(t.open_level);
    const closeLevel = parseFloat(t.close_level);
    const priceDist = Math.abs(closeLevel - openLevel);
    const pnlPerPoint = size > 0 ? Math.abs(pnl / priceDist) : size;
    const winner = pnl > 0;
    const rMultiple = size > 0 && priceDist > 0 ? pnl / (size * priceDist) : 0;

    return {
      instrument: t.instrument,
      size,
      pnl,
      priceDist: priceDist.toFixed(1),
      winner,
      rMultiple: rMultiple.toFixed(2),
      regime: t.regime || 'unknown',
      aiConfidence: t.ai_confidence,
    };
  });

  // Group by size buckets
  const sizes = tradeStats.map(t => t.size);
  const avgSize = sizes.reduce((a,b) => a+b, 0) / sizes.length;
  const largerTrades = tradeStats.filter(t => t.size > avgSize);
  const smallerTrades = tradeStats.filter(t => t.size <= avgSize);

  const winRate = arr => arr.length ? (arr.filter(t => t.winner).length / arr.length * 100).toFixed(0) : 0;
  const avgPnl = arr => arr.length ? (arr.reduce((s,t) => s + t.pnl, 0) / arr.length).toFixed(2) : 0;
  const avgR = arr => arr.length ? (arr.reduce((s,t) => s + parseFloat(t.rMultiple), 0) / arr.length).toFixed(2) : 0;

  const summary = {
    totalTrades: closed.length,
    avgSize: avgSize.toFixed(3),
    largerTrades: {
      count: largerTrades.length,
      winRate: winRate(largerTrades),
      avgPnl: avgPnl(largerTrades),
      avgR: avgR(largerTrades),
    },
    smallerTrades: {
      count: smallerTrades.length,
      winRate: winRate(smallerTrades),
      avgPnl: avgPnl(smallerTrades),
      avgR: avgR(smallerTrades),
    },
    byRegime: {},
    tradeDetails: tradeStats.slice(0, 20),
  };

  // Group by regime
  const regimes = [...new Set(tradeStats.map(t => t.regime))];
  for (const regime of regimes) {
    const rt = tradeStats.filter(t => t.regime === regime);
    summary.byRegime[regime] = {
      count: rt.length,
      winRate: winRate(rt),
      avgPnl: avgPnl(rt),
      avgSize: (rt.reduce((s,t) => s+t.size, 0)/rt.length).toFixed(3),
    };
  }

  if (!anthropicKey || closed.length < 3) return { summary, analysis: null };

  // Claude analysis
  const prompt = `You are a quantitative trading analyst reviewing position sizing and stop distance data.

TRADE SUMMARY (${closed.length} closed trades):
Average size: £${summary.avgSize}/point

LARGER TRADES (above average size):
Count: ${summary.largerTrades.count} | Win rate: ${summary.largerTrades.winRate}% | Avg P&L: £${summary.largerTrades.avgPnl} | Avg R-multiple: ${summary.largerTrades.avgR}

SMALLER TRADES (below average size):
Count: ${summary.smallerTrades.count} | Win rate: ${summary.smallerTrades.winRate}% | Avg P&L: £${summary.smallerTrades.avgPnl} | Avg R-multiple: ${summary.smallerTrades.avgR}

BY REGIME:
${Object.entries(summary.byRegime).map(([r,d]) => `${r}: ${d.count} trades | ${d.winRate}% WR | avg size £${d.avgSize}/pt | avg P&L £${d.avgPnl}`).join('\n')}

INDIVIDUAL TRADES:
${tradeStats.map(t => `${t.instrument}: £${t.size}/pt | ${t.priceDist}pt range | P&L £${t.pnl} | R=${t.rMultiple} | ${t.winner?'WIN':'LOSS'} | ${t.regime}`).join('\n')}

Analyse:
1. Is there a clear pattern between trade size and outcome (win rate, R-multiple)?
2. Do larger stops with smaller sizes perform better or worse than tighter stops with larger sizes?
3. Which regime (ranging/uptrend/downtrend) benefits most from the current ATR-based sizing?
4. What is the optimal size/stop relationship based on this data?
5. One specific recommendation: should the system increase or decrease the ATR multiplier for stops (currently 1.5x)?

Be specific and data-driven. Note if sample size is too small for conclusions.`;

  try {
    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': anthropicKey, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-sonnet-4-5', max_tokens: 600, messages: [{ role: 'user', content: prompt }] })
    });
    const aiData = await aiRes.json();
    summary.analysis = aiData.content?.[0]?.text || '';
  } catch(e) {
    summary.analysis = 'Analysis unavailable: ' + e.message;
  }

  return summary;
}

async function sendWeeklyReview(req, res) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    const [statsRes, tradesRes, calRes, todRes] = await Promise.all([
      fetch(`${base}/api/db?action=stats`),
      fetch(`${base}/api/db?action=trades&limit=50`),
      fetch(`${base}/api/db?action=calibration`),
      fetch(`${base}/api/db?action=timeofday`),
    ]);
    const [stats, tradesData, calData, todData] = await Promise.all([
      statsRes.json(), tradesRes.json(), calRes.json(), todRes.json()
    ]);

    if (!stats.totalTrades) return res.status(200).json({ message: 'No trades yet' });

    const trades = tradesData.trades || [];
    const oneWeekAgo = new Date(Date.now() - 7*24*60*60*1000);
    const weekClosed = trades.filter(t => t.status==='closed' && new Date(t.opened_at) > oneWeekAgo);
    const weekPnL = weekClosed.reduce((s,t) => s+(t.profit_loss||0), 0);
    const weekWins = weekClosed.filter(t => (t.profit_loss||0) > 0).length;
    const weekWinRate = weekClosed.length > 0 ? ((weekWins/weekClosed.length)*100).toFixed(1) : 'N/A';

    const byInstr = {};
    weekClosed.forEach(t => {
      if (!byInstr[t.instrument]) byInstr[t.instrument] = {trades:0,pnl:0,wins:0};
      byInstr[t.instrument].trades++;
      byInstr[t.instrument].pnl += t.profit_loss||0;
      if((t.profit_loss||0) > 0) byInstr[t.instrument].wins++;
    });

    const calRows = calData.calibration || [];
    const calSummary = calRows.map(r => `  ${r.confidence_bracket}: ${r.total_trades} trades, ${r.actual_win_rate}% actual win rate`).join('\n');
    const todRows = todData.byHour || [];
    const bestHour = todData.bestHour;
    const worstHour = todData.worstHour;

    let aiAnalysis = '';
    if (process.env.ANTHROPIC_API_KEY && weekClosed.length > 0) {
      const tradeDetails = weekClosed.slice(0,15).map(t =>
        `${t.instrument} ${t.direction}: ${t.profit_loss>=0?'+':''}£${(t.profit_loss||0).toFixed(2)} | AI:${t.ai_confidence||'?'}% | Regime:${t.regime||'?'} | Hour:${t.open_hour??'?'}:00`
      ).join('\n');

      const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
        method:'POST',
        headers:{'Content-Type':'application/json','x-api-key':process.env.ANTHROPIC_API_KEY,'anthropic-version':'2023-06-01'},
        body:JSON.stringify({model:'claude-sonnet-4-5',max_tokens:600,messages:[{role:'user',content:
          `Trading performance coach. Analyse week results, give specific recommendations.
WEEK: ${weekClosed.length} trades | WR: ${weekWinRate}% | P&L: £${weekPnL.toFixed(2)}
AI CALIBRATION:\n${calSummary||'Insufficient data'}
BEST HOUR: ${bestHour?.open_hour??'N/A'}:00 UTC (avg £${parseFloat(bestHour?.avg_pnl||0).toFixed(2)})
WORST HOUR: ${worstHour?.open_hour??'N/A'}:00 UTC (avg £${parseFloat(worstHour?.avg_pnl||0).toFixed(2)})
TRADES:\n${tradeDetails}
Give: 1)Key patterns 2)AI calibration quality 3)Recommended threshold adjustment 4)Recommended confidence minimum 5)Best trading hours. Under 400 words, specific numbers.`}]})
      });
      const aiData = await aiRes.json();
      aiAnalysis = aiData.content?.[0]?.text || '';
    }

    const sizingAnalysis = await analysePositionSizing(trades, process.env.ANTHROPIC_API_KEY);
    sizingSection = sizingAnalysis ? `
POSITION SIZING ANALYSIS (${sizingAnalysis.summary.totalTrades} trades):
Average size: £${sizingAnalysis.summary.avgSize}/point

Larger trades: ${sizingAnalysis.summary.largerTrades.count} trades | ${sizingAnalysis.summary.largerTrades.winRate}% WR | avg P&L £${sizingAnalysis.summary.largerTrades.avgPnl}
Smaller trades: ${sizingAnalysis.summary.smallerTrades.count} trades | ${sizingAnalysis.summary.smallerTrades.winRate}% WR | avg P&L £${sizingAnalysis.summary.smallerTrades.avgPnl}

${sizingAnalysis.summary.analysis ? 'SIZING INTELLIGENCE:\n' + sizingAnalysis.summary.analysis : ''}` : '';

    const priceAnalysis = await runWeeklyPriceAnalysis(base, process.env.ANTHROPIC_API_KEY);
    priceSection = priceAnalysis ? `
WEEKLY MARKET OBSERVATIONS:
${priceAnalysis.marketData.map(m=>`  ${m.name}: RSI ${m.rsi} | ${m.regime} | Week ${m.weekChg} | Month ${m.monthChg}`).join('\n')}

${priceAnalysis.oversold.length ? 'OVERSOLD: '+priceAnalysis.oversold.map(m=>m.name+' RSI:'+m.rsi).join(', ') : ''}
${priceAnalysis.overbought.length ? 'OVERBOUGHT: '+priceAnalysis.overbought.map(m=>m.name+' RSI:'+m.rsi).join(', ') : ''}

MARKET INTELLIGENCE:
${priceAnalysis.analysis}
${priceAnalysis.sentimentShifts?.length ? '\nSENTIMENT SHIFTS:\n' + priceAnalysis.sentimentShifts.join('\n') : ''}
` : '';

    let sizingSection = '';


    const emailBody = `WEEKLY TRADING REPORT v4
${new Date().toLocaleDateString('en-GB',{weekday:'long',day:'numeric',month:'long',year:'numeric'})}

WEEK: ${weekClosed.length} trades | ${weekWinRate}% win rate | ${weekPnL>=0?'+':''}£${weekPnL.toFixed(2)}
ALL-TIME: ${stats.totalTrades} trades | ${stats.winRate}% WR | £${stats.totalPnL.toFixed(2)} P&L
AI ACCURACY: ${stats.aiAccuracy||'N/A'}% on ${stats.aiTotal} trades

AI CALIBRATION:
${calSummary||'Need more trades'}

BEST HOURS (UTC):
${todRows.slice(0,6).map(r=>`  ${r.open_hour}:00 — ${r.total_trades} trades, ${r.win_rate}% WR, avg £${parseFloat(r.avg_pnl).toFixed(2)}`).join('\n')||'Need more data'}

${aiAnalysis?'AI ANALYSIS:\n'+aiAnalysis:''}${sizingSection}${priceSection}

Generated: ${new Date().toLocaleString('en-GB',{timeZone:'Europe/London'})}`;

    const notifyRes = await fetch(`${base}/api/notify`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({type:'daily_summary',subject:`📊 Weekly Report — ${weekPnL>=0?'+':''}£${weekPnL.toFixed(2)} | ${weekWinRate}% WR`,body:emailBody})});
    const notifyData = await notifyRes.json();
    return res.status(200).json({success:true,weekTrades:weekClosed.length,weekPnL:weekPnL.toFixed(2),weekWinRate,emailSent:notifyData.sent});
  } catch(e) { return res.status(500).json({error:e.message}); }
}

async function runOptimization(req, res) {
  const log = [];
  const L = msg => { console.log('[Optimize]', msg); log.push(msg); };
  L('Walk-forward optimisation v4 started');
  try {
    const { sql } = require('@vercel/postgres');
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;

    const priceResult = await sql`
      SELECT epic, close_price, candle_time FROM price_history
      WHERE resolution='DAY' AND close_price>0 AND candle_time > NOW() - INTERVAL '30 days'
      ORDER BY epic, candle_time ASC
    `;

    if (priceResult.rows.length < 50) {
      L(`Only ${priceResult.rows.length} candles — need 50+ for optimisation`);
      return res.status(200).json({message:'Insufficient data',candles:priceResult.rows.length,log});
    }

    const byEpic = {};
    priceResult.rows.forEach(r => {
      if(!byEpic[r.epic]) byEpic[r.epic]=[];
      byEpic[r.epic].push(parseFloat(r.close_price));
    });
    L(`${Object.keys(byEpic).length} instruments, ${priceResult.rows.length} candles`);

    const results = [];
    for (const threshold of [1,2,3,4]) {
      let trades=0,wins=0,pnl=0;
      for (const [,closes] of Object.entries(byEpic)) {
        if(closes.length<15) continue;
        const r = backtest(closes, threshold);
        trades+=r.trades; wins+=r.wins; pnl+=r.pnl;
      }
      const wr = trades>0?(wins/trades)*100:0;
      const exp = trades>0?pnl/trades:0;
      const score = exp*(wr/100);
      results.push({threshold,trades,winRate:wr.toFixed(1),pnl:pnl.toFixed(4),expectancy:exp.toFixed(4),score:score.toFixed(4)});
      L(`Threshold ${threshold}: ${trades} trades | ${wr.toFixed(1)}% WR | score ${score.toFixed(4)}`);
    }

    const best = results.reduce((a,b) => parseFloat(a.score)>parseFloat(b.score)?a:b);
    L(`Best: threshold=${best.threshold} score=${best.score}`);

    // Calibration-based confidence minimum
    let bestConf = 60;
    const calRes = await fetch(`${base}/api/db?action=calibration`);
    const calData = await calRes.json();
    if (calData.calibration?.length > 0) {
      const profitable = calData.calibration.filter(r => parseFloat(r.actual_win_rate)>50 && parseInt(r.total_trades)>=3);
      if (profitable.length > 0) {
        const map = {'90-100%':90,'80-89%':80,'70-79%':70,'60-69%':60};
        profitable.sort((a,b)=>(map[a.confidence_bracket]||60)-(map[b.confidence_bracket]||60));
        bestConf = map[profitable[0].confidence_bracket] || 60;
        L(`Calibration suggests confidence min: ${bestConf}%`);
      }
    }

    await fetch(`${base}/api/db`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({type:'save_params',data:{signalThreshold:best.threshold,aiConfidenceMin:bestConf,
        backtestScore:parseFloat(best.score),backtestResults:results,optimizedAt:new Date().toISOString()}})});

    await fetch(`${base}/api/notify`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({type:'system',subject:'⚙️ Walk-Forward Optimisation Complete',
        body:`Signal threshold: ${best.threshold}\nAI confidence min: ${bestConf}%\nScore: ${best.score}\n\nApply: SIGNAL_THRESHOLD=${best.threshold} AI_CONFIDENCE_MIN=${bestConf} in Vercel`})});

    return res.status(200).json({success:true,bestThreshold:best.threshold,bestConfidence:bestConf,results,log});
  } catch(e) { L('Error: '+e.message); return res.status(500).json({error:e.message,log}); }
}

function backtest(closes, threshold) {
  let trades=0,wins=0,pnl=0,openTrade=null;
  for (let i=20;i<closes.length;i++) {
    const sc = quickScore(closes.slice(Math.max(0,i-60),i));
    if(!openTrade && Math.abs(sc)>=threshold) {
      openTrade={direction:sc>0?'BUY':'SELL',entry:closes[i],index:i};
    } else if(openTrade) {
      const holding=i-openTrade.index;
      const reversal=(openTrade.direction==='BUY'&&sc<=-threshold)||(openTrade.direction==='SELL'&&sc>=threshold);
      if(reversal||holding>=5) {
        const tp=openTrade.direction==='BUY'?closes[i]-openTrade.entry:openTrade.entry-closes[i];
        trades++; if(tp>0)wins++; pnl+=tp; openTrade=null;
      }
    }
  }
  return {trades,wins,pnl};
}

function quickScore(closes) {
  const n=closes.length; if(n<5) return 0;
  let s=0,g=0,l=0;
  const p=Math.min(14,n-1);
  for(let i=n-p;i<n;i++){const d=closes[i]-closes[i-1];if(d>0)g+=d;else l+=Math.abs(d);}
  const rsi=l===0?100:100-(100/(1+(g/p)/(l/p)));
  if(rsi<30)s+=3;else if(rsi<40)s+=2;else if(rsi>70)s-=3;else if(rsi>60)s-=2;
  const sma5=closes.slice(-5).reduce((a,b)=>a+b,0)/5;
  const sma10=closes.slice(-Math.min(10,n)).reduce((a,b)=>a+b,0)/Math.min(10,n);
  if(sma5>sma10)s+=1;else s-=1;
  const mom=n>=5?((closes[n-1]-closes[n-5])/closes[n-5])*100:0;
  if(mom>1)s+=1;else if(mom<-1)s-=1;
  return s;
}

// ─── WEEKLY PRICE ANALYSIS ────────────────────────────────────────────────────
async function runWeeklyPriceAnalysis(base, anthropicKey) {
  try {
    // Fetch last 30 candles for all instruments from DB
    const instruments = [
      { name: 'FTSE 100',    epic: 'IX.D.FTSE.DAILY.IP' },
      { name: 'S&P 500',     epic: 'IX.D.SPTRD.DAILY.IP' },
      { name: 'DAX 40',      epic: 'IX.D.DAX.DAILY.IP' },
      { name: 'Brent Oil',   epic: 'CC.D.LCO.USS.IP' },
      { name: 'Gold',        epic: 'CS.D.USCGC.TODAY.IP' },
      { name: 'Silver',      epic: 'CS.D.USCSI.TODAY.IP' },
      { name: 'GBP/USD',     epic: 'CS.D.GBPUSD.TODAY.IP' },
      { name: 'EUR/USD',     epic: 'CS.D.EURUSD.TODAY.IP' },
      { name: 'Copper',      epic: 'CS.D.COPPER.TODAY.IP' },
      { name: 'EUR/GBP',     epic: 'CS.D.EURGBP.TODAY.IP' },
    ];

    const marketData = [];

    for (const instr of instruments) {
      try {
        const r = await fetch(`${base}/api/prices?epic=${encodeURIComponent(instr.epic)}&resolution=DAY&limit=30`);
        const d = await r.json();
        const candles = d.candles || [];
        if (candles.length < 5) continue;

        const closes = candles.map(c => parseFloat(c.close_price));
        const n = closes.length;
        const last = closes[n-1];
        const weekAgo = closes[Math.max(0, n-6)];
        const monthAgo = closes[0];

        // RSI
        let gains=0, losses=0;
        const period = Math.min(14, n-1);
        for (let i=n-period; i<n; i++) {
          const d = closes[i]-closes[i-1];
          if(d>0) gains+=d; else losses+=Math.abs(d);
        }
        const ag=gains/period, al=losses/period;
        const rsi = al===0 ? 100 : 100-(100/(1+ag/al));

        // Regime
        const mid = Math.floor(n/2);
        const h1 = closes.slice(0,mid).reduce((a,b)=>a+b,0)/mid;
        const h2 = closes.slice(mid).reduce((a,b)=>a+b,0)/(n-mid);
        const slope = ((h2-h1)/h1)*100;
        const regime = Math.abs(slope)>3 ? (slope>0?'uptrend':'downtrend') : 'ranging';

        // Week and month change
        const weekChg = ((last-weekAgo)/weekAgo*100).toFixed(1);
        const monthChg = ((last-monthAgo)/monthAgo*100).toFixed(1);

        // Consecutive days up/down
        let streak = 0;
        for (let i=n-1; i>0; i--) {
          if (closes[i]>closes[i-1] && streak>=0) streak++;
          else if (closes[i]<closes[i-1] && streak<=0) streak--;
          else break;
        }

        marketData.push({
          name: instr.name,
          rsi: rsi.toFixed(1),
          regime,
          weekChg: (weekChg>=0?'+':'')+weekChg+'%',
          monthChg: (monthChg>=0?'+':'')+monthChg+'%',
          streak: streak>0?`${streak} days up`:streak<0?`${Math.abs(streak)} days down`:'flat',
          lastPrice: last.toFixed(2),
          candles: n,
        });
      } catch(e) { /* skip instrument */ }
    }

    if (!marketData.length || !anthropicKey) return null;

    // Build prompt for Claude
    const dataStr = marketData.map(m =>
      `${m.name}: RSI ${m.rsi} | ${m.regime} | Week ${m.weekChg} | Month ${m.monthChg} | ${m.streak} | ${m.candles} candles`
    ).join('\n');

    // Find notable conditions
    const oversold = marketData.filter(m => parseFloat(m.rsi) <= 32);
    const overbought = marketData.filter(m => parseFloat(m.rsi) >= 68);
    const trending = marketData.filter(m => m.regime !== 'ranging');

    const prompt = `You are a quantitative market analyst. Review this week's price data across multiple instruments and identify the most interesting patterns, divergences, and potential opportunities for next week.

MARKET DATA (as of week ending):
${dataStr}

NOTABLE CONDITIONS:
- Oversold (RSI ≤32): ${oversold.map(m=>m.name+' RSI:'+m.rsi).join(', ') || 'None'}
- Overbought (RSI ≥68): ${overbought.map(m=>m.name+' RSI:'+m.rsi).join(', ') || 'None'}
- Trending: ${trending.map(m=>m.name+' ('+m.regime+')').join(', ') || 'All ranging'}

Analyse:
1. Which instruments show the most interesting setups for next week and why?
2. Any unusual divergences (e.g. oil up but gold down, indices diverging)?
3. Which RSI extremes look like genuine mean reversion opportunities vs momentum continuation?
4. Any correlations breaking down that could signal a larger move?
5. One specific instrument and direction to watch closely this week with the key level.

Keep it concise — this is a weekly briefing, not a full report. Focus on actionable observations.`;

    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type':'application/json', 'x-api-key':anthropicKey, 'anthropic-version':'2023-06-01' },
      body: JSON.stringify({ model:'claude-sonnet-4-5', max_tokens:800, messages:[{role:'user',content:prompt}] })
    });
    const aiData = await aiRes.json();
    const analysis = aiData.content?.[0]?.text || '';

    // Check for significant sentiment shifts vs last week
    const sentimentShifts = [];
    try {
      const instruments = marketData.map(m => m.name);
      for (const instr of instruments.slice(0,6)) {
        const sr = await fetch(`${base}/api/db?action=sentiment_history&instrument=${encodeURIComponent(instr)}&days=14`);
        const sd = await sr.json();
        if (sd.shift) {
          sentimentShifts.push(`${instr}: sentiment ${sd.shift.direction.replace('_',' ')} by ${sd.shift.change}% over 14 days`);
        }
      }
    } catch(e) {}

    return { marketData, analysis, oversold, overbought, trending, sentimentShifts };
  } catch(e) {
    console.error('[Weekly Price Analysis]', e.message);
    return null;
  }
}

// ─── PATTERN DISCOVERY ENGINE ─────────────────────────────────────────────────
async function runDiscovery(req, res) {
  const { sql } = require('@vercel/postgres');
  const log = [];
  const L = msg => { console.log('[Discovery]', msg); log.push(msg); };

  const INSTRUMENTS = [
    { name:'FTSE 100',  epic:'IX.D.FTSE.DAILY.IP' },
    { name:'S&P 500',   epic:'IX.D.SPTRD.DAILY.IP' },
    { name:'DAX 40',    epic:'IX.D.DAX.DAILY.IP' },
    { name:'Dow Jones', epic:'IX.D.DOW.DAILY.IP' },
    { name:'Nasdaq',    epic:'IX.D.NASDAQ.CASH.IP' },
    { name:'Brent Oil', epic:'CC.D.LCO.USS.IP' },
    { name:'Gold',      epic:'CS.D.USCGC.TODAY.IP' },
    { name:'GBP/USD',   epic:'CS.D.GBPUSD.TODAY.IP' },
    { name:'EUR/USD',   epic:'CS.D.EURUSD.TODAY.IP' },
    { name:'EUR/GBP',   epic:'CS.D.EURGBP.TODAY.IP' },
    { name:'USD/JPY',   epic:'CS.D.USDJPY.TODAY.IP' },
  ];

  function calcRSI(closes, period=14) {
    if(closes.length < period+1) return 50;
    let gains=0, losses=0;
    for(let i=1; i<=period; i++) {
      const d = closes[i]-closes[i-1];
      if(d>0) gains+=d; else losses-=d;
    }
    let ag=gains/period, al=losses/period;
    for(let i=period+1; i<closes.length; i++) {
      const d = closes[i]-closes[i-1];
      ag=(ag*(period-1)+(d>0?d:0))/period;
      al=(al*(period-1)+(d<0?-d:0))/period;
    }
    return al===0 ? 100 : 100-(100/(1+ag/al));
  }

  function calcSMA(prices, period) {
    if(prices.length < period) return null;
    return prices.slice(-period).reduce((a,b)=>a+b,0)/period;
  }

  function calcATR(prices, period=14) {
    if(prices.length < period+1) return 0;
    let sum=0;
    for(let i=prices.length-period; i<prices.length; i++)
      sum += Math.abs(prices[i]-prices[i-1]);
    return sum/period;
  }

  function testStrategy(closes, dates, entryFn, exitFn, holdDays=10) {
    const trades = [];
    let i = 30;
    while(i < closes.length - holdDays) {
      const slice = closes.slice(0, i+1);
      const signal = entryFn(slice, i, closes, dates);
      if(!signal) { i++; continue; }

      const entry = closes[i];
      let exitPrice = closes[Math.min(i+holdDays, closes.length-1)];
      let exitDay = holdDays;
      let exitReason = 'time';

      // Check exit condition each day
      for(let j=1; j<=holdDays && i+j<closes.length; j++) {
        const futureSlice = closes.slice(0, i+j+1);
        if(exitFn && exitFn(futureSlice, signal.direction)) {
          exitPrice = closes[i+j]; exitDay=j; exitReason='signal'; break;
        }
      }

      const pnlPct = signal.direction==='BUY'
        ? (exitPrice-entry)/entry*100
        : (entry-exitPrice)/entry*100;

      trades.push({ date:dates[i], direction:signal.direction,
        entry, exitPrice, pnlPct, exitDay, exitReason, won:pnlPct>0 });
      i += Math.max(3, exitDay);
    }
    return trades;
  }

  function summarise(trades) {
    if(!trades.length) return { trades:0, winRate:0, expectancy:0, avgWin:0, avgLoss:0 };
    const wins = trades.filter(t=>t.won);
    const losses = trades.filter(t=>!t.won);
    const wr = wins.length/trades.length*100;
    const avgWin = wins.length ? wins.reduce((s,t)=>s+t.pnlPct,0)/wins.length : 0;
    const avgLoss = losses.length ? losses.reduce((s,t)=>s+t.pnlPct,0)/losses.length : 0;
    const exp = (wr/100)*avgWin + (1-wr/100)*avgLoss;
    return { trades:trades.length, winRate:parseFloat(wr.toFixed(1)),
      expectancy:parseFloat(exp.toFixed(3)),
      avgWin:parseFloat(avgWin.toFixed(2)), avgLoss:parseFloat(avgLoss.toFixed(2)) };
  }

  const results = {};

  try {
    for(const instr of INSTRUMENTS) {
      // Fetch 500 days of daily closes
      const rows = await sql`
        SELECT close_price, high_price, low_price, candle_time::date as dt
        FROM price_history
        WHERE (epic=${instr.epic} OR instrument=${instr.name})
        AND resolution='DAY' AND close_price>0
        ORDER BY candle_time ASC LIMIT 520`;

      if(rows.rows.length < 60) { L(`${instr.name}: insufficient data (${rows.rows.length})`); continue; }

      const closes = rows.rows.map(r=>parseFloat(r.close_price));
      const highs = rows.rows.map(r=>parseFloat(r.high_price)||parseFloat(r.close_price));
      const lows = rows.rows.map(r=>parseFloat(r.low_price)||parseFloat(r.close_price));
      const dates = rows.rows.map(r=>new Date(r.dt).toISOString().substring(0,10));
      L(`${instr.name}: ${closes.length} candles`);

      const instrResults = {};

      // ── STRATEGY 1: SMA CROSSOVER (trend following) ──────────────────────
      const smaCross = testStrategy(closes, dates,
        (slice) => {
          if(slice.length < 55) return null;
          const sma20now = calcSMA(slice, 20);
          const sma50now = calcSMA(slice, 50);
          const sma20prev = calcSMA(slice.slice(0,-1), 20);
          const sma50prev = calcSMA(slice.slice(0,-1), 50);
          if(!sma20now||!sma50now||!sma20prev||!sma50prev) return null;
          if(sma20prev <= sma50prev && sma20now > sma50now) return { direction:'BUY' };
          if(sma20prev >= sma50prev && sma20now < sma50now) return { direction:'SELL' };
          return null;
        },
        (slice, dir) => {
          const sma20 = calcSMA(slice, 20);
          const sma50 = calcSMA(slice, 50);
          if(!sma20||!sma50) return false;
          return dir==='BUY' ? sma20 < sma50 : sma20 > sma50;
        }, 20);
      instrResults.smaCrossover = summarise(smaCross);

      // ── STRATEGY 2: MOMENTUM (price momentum) ────────────────────────────
      const momentum = testStrategy(closes, dates,
        (slice) => {
          if(slice.length < 10) return null;
          const ret5 = (slice[slice.length-1]-slice[slice.length-6])/slice[slice.length-6]*100;
          if(ret5 > 3) return { direction:'BUY' };   // momentum continuation
          if(ret5 < -3) return { direction:'SELL' };
          return null;
        }, null, 10);
      instrResults.momentum = summarise(momentum);

      // ── STRATEGY 3: VOLATILITY BREAKOUT ──────────────────────────────────
      const volBreakout = testStrategy(closes, dates,
        (slice) => {
          if(slice.length < 20) return null;
          const todayRange = Math.abs(slice[slice.length-1]-slice[slice.length-2]);
          const atr = calcATR(slice, 14);
          if(atr === 0) return null;
          if(todayRange > 2*atr) {
            const dir = slice[slice.length-1] > slice[slice.length-2] ? 'BUY' : 'SELL';
            return { direction:dir };
          }
          return null;
        }, null, 5);
      instrResults.volBreakout = summarise(volBreakout);

      // ── STRATEGY 4: 20-DAY BREAKOUT ──────────────────────────────────────
      const breakout20 = testStrategy(closes, dates,
        (slice) => {
          if(slice.length < 22) return null;
          const high20 = Math.max(...slice.slice(-21,-1));
          const low20 = Math.min(...slice.slice(-21,-1));
          const price = slice[slice.length-1];
          const prev = slice[slice.length-2];
          if(prev < high20 && price > high20) return { direction:'BUY' };
          if(prev > low20 && price < low20) return { direction:'SELL' };
          return null;
        }, null, 15);
      instrResults.breakout20d = summarise(breakout20);

      // ── STRATEGY 5: RSI DIVERGENCE ────────────────────────────────────────
      const rsiDiv = testStrategy(closes, dates,
        (slice) => {
          if(slice.length < 20) return null;
          const rsiNow = calcRSI(slice);
          const rsi5ago = calcRSI(slice.slice(0,-5));
          const priceNow = slice[slice.length-1];
          const price5ago = slice[slice.length-6];
          // Bearish div: price higher but RSI lower
          if(priceNow > price5ago && rsiNow < rsi5ago - 5 && rsiNow > 60)
            return { direction:'SELL' };
          // Bullish div: price lower but RSI higher
          if(priceNow < price5ago && rsiNow > rsi5ago + 5 && rsiNow < 40)
            return { direction:'BUY' };
          return null;
        }, null, 10);
      instrResults.rsiDivergence = summarise(rsiDiv);

      // ── STRATEGY 6: MEAN REVERSION (current system baseline) ─────────────
      const mrBaseline = testStrategy(closes, dates,
        (slice) => {
          if(slice.length < 20) return null;
          const rsi = calcRSI(slice);
          const sma10 = calcSMA(slice, 10);
          const sma20 = calcSMA(slice, 20);
          if(!sma10||!sma20) return null;
          const inDowntrend = sma10 < sma20 * 0.998;
          const inUptrend = sma10 > sma20 * 1.002;
          if(rsi <= 33 && !inDowntrend) return { direction:'BUY' };
          if(rsi >= 67 && !inUptrend) return { direction:'SELL' };
          return null;
        }, null, 5);
      instrResults.mrBaseline = summarise(mrBaseline);

      results[instr.name] = instrResults;
    }

    // Find the best strategies across all instruments
    const rankings = [];
    Object.entries(results).forEach(([instr, strats]) => {
      Object.entries(strats).forEach(([strat, stats]) => {
        if(stats.trades >= 3) {
          rankings.push({ instr, strat, ...stats,
            score: stats.expectancy * Math.sqrt(stats.trades) // quality-adjusted
          });
        }
      });
    });
    rankings.sort((a,b) => b.score - a.score);

    L(`Discovery complete — ${Object.keys(results).length} instruments, ${rankings.length} strategies tested`);

    return res.status(200).json({ success:true, results, rankings: rankings.slice(0,20), log });

  } catch(e) {
    return res.status(500).json({ error: e.message, log });
  }
}

// ─── NOVEL PATTERN DISCOVERY ──────────────────────────────────────────────────
async function runNovelDiscovery(req, res) {
  const { sql } = require('@vercel/postgres');
  const log = [];
  const L = msg => { console.log('[Novel]', msg); log.push(msg); };

  const INSTRUMENTS = [
    { name:'FTSE 100', epic:'IX.D.FTSE.DAILY.IP' },
    { name:'S&P 500',  epic:'IX.D.SPTRD.DAILY.IP' },
    { name:'DAX 40',   epic:'IX.D.DAX.DAILY.IP' },
    { name:'Nasdaq',   epic:'IX.D.NASDAQ.CASH.IP' },
    { name:'Brent Oil',epic:'CC.D.LCO.USS.IP' },
    { name:'Gold',     epic:'CS.D.USCGC.TODAY.IP' },
    { name:'GBP/USD',  epic:'CS.D.GBPUSD.TODAY.IP' },
    { name:'EUR/USD',  epic:'CS.D.EURUSD.TODAY.IP' },
    { name:'EUR/GBP',  epic:'CS.D.EURGBP.TODAY.IP' },
    { name:'USD/JPY',  epic:'CS.D.USDJPY.TODAY.IP' },
  ];

  const allData = {};

  // Load all instrument data
  for(const instr of INSTRUMENTS) {
    const rows = await sql`
      SELECT close_price, high_price, low_price, open_price, candle_time
      FROM price_history
      WHERE (epic=${instr.epic} OR instrument=${instr.name})
      AND resolution='DAY' AND close_price>0
      ORDER BY candle_time ASC LIMIT 520`;
    if(rows.rows.length < 60) continue;
    allData[instr.name] = {
      closes: rows.rows.map(r=>parseFloat(r.close_price)),
      highs:  rows.rows.map(r=>parseFloat(r.high_price||r.close_price)),
      lows:   rows.rows.map(r=>parseFloat(r.low_price||r.close_price)),
      opens:  rows.rows.map(r=>parseFloat(r.open_price||r.close_price)),
      dates:  rows.rows.map(r=>new Date(r.candle_time)),
    };
  }
  L(`Loaded ${Object.keys(allData).length} instruments`);

  const findings = [];

  // ── PATTERN 1: DAY-OF-WEEK BIAS ──────────────────────────────────────────
  // Does each instrument reliably go up or down on specific days?
  L('Testing day-of-week bias...');
  const dowResults = {};
  const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  Object.entries(allData).forEach(([name, d]) => {
    const byDay = {1:[],2:[],3:[],4:[],5:[]};
    d.closes.forEach((close, i) => {
      if(i===0) return;
      const day = d.dates[i].getDay();
      if(day >= 1 && day <= 5) {
        const ret = (close - d.closes[i-1]) / d.closes[i-1] * 100;
        byDay[day].push(ret);
      }
    });
    const dayStats = {};
    Object.entries(byDay).forEach(([day, rets]) => {
      if(!rets.length) return;
      const avg = rets.reduce((a,b)=>a+b,0)/rets.length;
      const positive = rets.filter(r=>r>0).length/rets.length*100;
      dayStats[dayNames[day]] = { avg:parseFloat(avg.toFixed(3)), positive:parseFloat(positive.toFixed(1)), n:rets.length };
    });
    dowResults[name] = dayStats;
    // Flag strong day biases
    Object.entries(dayStats).forEach(([day, stats]) => {
      if(Math.abs(stats.avg) > 0.15 || stats.positive > 60 || stats.positive < 40) {
        findings.push({
          type: 'day_of_week',
          instrument: name,
          detail: `${day}: avg ${stats.avg>0?'+':''}${stats.avg}% | ${stats.positive}% positive days (n=${stats.n})`,
          strength: Math.abs(stats.avg) * (Math.abs(stats.positive-50)/10),
          tradeable: Math.abs(stats.avg) > 0.2 && (stats.positive > 62 || stats.positive < 38)
        });
      }
    });
  });

  // ── PATTERN 2: AFTER-LARGE-MOVE BEHAVIOUR ────────────────────────────────
  // After a day moves >1.5%, does it continue or reverse the next day?
  L('Testing post-large-move behaviour...');
  Object.entries(allData).forEach(([name, d]) => {
    const continueAfterUp = [], reverseAfterUp = [];
    const continueAfterDown = [], reverseAfterDown = [];
    d.closes.forEach((close, i) => {
      if(i < 2 || i >= d.closes.length-1) return;
      const todayRet = (close - d.closes[i-1]) / d.closes[i-1] * 100;
      const nextRet = (d.closes[i+1] - close) / close * 100;
      if(todayRet > 1.5) {
        if(nextRet > 0) continueAfterUp.push(nextRet);
        else reverseAfterUp.push(nextRet);
      }
      if(todayRet < -1.5) {
        if(nextRet < 0) continueAfterDown.push(nextRet);
        else reverseAfterDown.push(nextRet);
      }
    });
    const totalUp = continueAfterUp.length + reverseAfterUp.length;
    const totalDown = continueAfterDown.length + reverseAfterDown.length;
    if(totalUp >= 5) {
      const contPct = continueAfterUp.length/totalUp*100;
      if(contPct > 60 || contPct < 40) {
        findings.push({
          type: 'post_large_move',
          instrument: name,
          detail: `After big UP day: ${contPct.toFixed(0)}% continue, ${(100-contPct).toFixed(0)}% reverse (n=${totalUp})`,
          strength: Math.abs(contPct-50)/10,
          tradeable: contPct > 62 || contPct < 38
        });
      }
    }
    if(totalDown >= 5) {
      const contPct = continueAfterDown.length/totalDown*100;
      if(contPct > 60 || contPct < 40) {
        findings.push({
          type: 'post_large_move',
          instrument: name,
          detail: `After big DOWN day: ${contPct.toFixed(0)}% continue, ${(100-contPct).toFixed(0)}% reverse (n=${totalDown})`,
          strength: Math.abs(contPct-50)/10,
          tradeable: contPct > 62 || contPct < 38
        });
      }
    }
  });

  // ── PATTERN 3: OPEN-TO-CLOSE vs CLOSE-TO-CLOSE ───────────────────────────
  // Gap up at open — does it fill or extend?
  L('Testing gap behaviour...');
  Object.entries(allData).forEach(([name, d]) => {
    const gapFills = [], gapExtends = [];
    d.closes.forEach((close, i) => {
      if(i===0) return;
      const prevClose = d.closes[i-1];
      const open = d.opens[i];
      const gapPct = (open - prevClose) / prevClose * 100;
      if(Math.abs(gapPct) < 0.3) return; // ignore tiny gaps
      const closedGap = gapPct > 0 ? close < open : close > open;
      if(closedGap) gapFills.push(gapPct);
      else gapExtends.push(gapPct);
    });
    const total = gapFills.length + gapExtends.length;
    if(total >= 10) {
      const fillPct = gapFills.length/total*100;
      findings.push({
        type: 'gap_behaviour',
        instrument: name,
        detail: `Gaps fill intraday: ${fillPct.toFixed(0)}% of time (n=${total})`,
        strength: Math.abs(fillPct-50)/10,
        tradeable: fillPct > 65 || fillPct < 35
      });
    }
  });

  // ── PATTERN 4: CROSS-INSTRUMENT LEAD/LAG ─────────────────────────────────
  // Does one instrument reliably lead another by 1 day?
  L('Testing cross-instrument lead/lag...');
  const instrNames = Object.keys(allData);
  for(let a=0; a<instrNames.length; a++) {
    for(let b=a+1; b<instrNames.length; b++) {
      const nameA = instrNames[a], nameB = instrNames[b];
      const closesA = allData[nameA].closes;
      const closesB = allData[nameB].closes;
      const minLen = Math.min(closesA.length, closesB.length);
      if(minLen < 60) continue;

      // Correlation: A today vs B tomorrow
      let sumXY=0, sumX=0, sumY=0, sumX2=0, sumY2=0, n=0;
      for(let i=1; i<minLen-1; i++) {
        const retA = (closesA[i]-closesA[i-1])/closesA[i-1];
        const retB = (closesB[i+1]-closesB[i])/closesB[i];
        sumXY+=retA*retB; sumX+=retA; sumY+=retB;
        sumX2+=retA*retA; sumY2+=retB*retB; n++;
      }
      const corr = (n*sumXY-sumX*sumY)/Math.sqrt((n*sumX2-sumX*sumX)*(n*sumY2-sumY*sumY));
      if(!isNaN(corr) && Math.abs(corr) > 0.25) {
        findings.push({
          type: 'lead_lag',
          instrument: `${nameA} → ${nameB}`,
          detail: `${nameA} today predicts ${nameB} tomorrow: correlation ${corr.toFixed(3)} (n=${n})`,
          strength: Math.abs(corr),
          tradeable: Math.abs(corr) > 0.3
        });
      }
    }
  }

  // ── PATTERN 5: CONSECUTIVE DAYS BIAS ─────────────────────────────────────
  // After N consecutive up/down days, what happens next?
  L('Testing consecutive day bias...');
  Object.entries(allData).forEach(([name, d]) => {
    let streak = 0;
    const afterStreak3 = { up:[], down:[] };
    d.closes.forEach((close, i) => {
      if(i===0||i>=d.closes.length-1) return;
      const ret = (close-d.closes[i-1])/d.closes[i-1]*100;
      if(ret > 0) streak = streak > 0 ? streak+1 : 1;
      else streak = streak < 0 ? streak-1 : -1;
      const nextRet = (d.closes[i+1]-close)/close*100;
      if(streak >= 3) afterStreak3.up.push(nextRet);
      if(streak <= -3) afterStreak3.down.push(nextRet);
    });
    if(afterStreak3.up.length >= 5) {
      const avgNext = afterStreak3.up.reduce((a,b)=>a+b,0)/afterStreak3.up.length;
      const pctDown = afterStreak3.up.filter(r=>r<0).length/afterStreak3.up.length*100;
      if(pctDown > 55 || pctDown < 45) {
        findings.push({
          type: 'streak_reversal',
          instrument: name,
          detail: `After 3+ UP days: ${pctDown.toFixed(0)}% reverse next day, avg next ${avgNext.toFixed(3)}% (n=${afterStreak3.up.length})`,
          strength: Math.abs(pctDown-50)/10,
          tradeable: pctDown > 60
        });
      }
    }
    if(afterStreak3.down.length >= 5) {
      const avgNext = afterStreak3.down.reduce((a,b)=>a+b,0)/afterStreak3.down.length;
      const pctUp = afterStreak3.down.filter(r=>r>0).length/afterStreak3.down.length*100;
      if(pctUp > 55 || pctUp < 45) {
        findings.push({
          type: 'streak_reversal',
          instrument: name,
          detail: `After 3+ DOWN days: ${pctUp.toFixed(0)}% reverse next day, avg next ${avgNext.toFixed(3)}% (n=${afterStreak3.down.length})`,
          strength: Math.abs(pctUp-50)/10,
          tradeable: pctUp > 60
        });
      }
    }
  });

  // ── PATTERN 6: RANGE COMPRESSION BEFORE BREAKOUT ─────────────────────────
  // Narrow range days (inside days) — do they predict direction?
  L('Testing range compression...');
  Object.entries(allData).forEach(([name, d]) => {
    const insideDayUp = [], insideDayDown = [];
    d.closes.forEach((close, i) => {
      if(i < 2 || i >= d.closes.length-1) return;
      const todayHigh = d.highs[i], todayLow = d.lows[i];
      const prevHigh = d.highs[i-1], prevLow = d.lows[i-1];
      const isInsideDay = todayHigh <= prevHigh && todayLow >= prevLow;
      if(!isInsideDay) return;
      const nextRet = (d.closes[i+1]-close)/close*100;
      const prevRet = (close-d.closes[i-1])/d.closes[i-1]*100;
      if(prevRet > 0) insideDayUp.push(nextRet);
      else insideDayDown.push(nextRet);
    });
    const total = insideDayUp.length + insideDayDown.length;
    if(total >= 10) {
      const upCont = insideDayUp.filter(r=>r>0).length/(insideDayUp.length||1)*100;
      const downCont = insideDayDown.filter(r=>r<0).length/(insideDayDown.length||1)*100;
      if(upCont > 60 || upCont < 40 || downCont > 60 || downCont < 40) {
        findings.push({
          type: 'inside_day',
          instrument: name,
          detail: `Inside day after UP: ${upCont.toFixed(0)}% continue up. After DOWN: ${downCont.toFixed(0)}% continue down (n=${total})`,
          strength: (Math.abs(upCont-50)+Math.abs(downCont-50))/20,
          tradeable: upCont > 62 || downCont > 62
        });
      }
    }
  });

  // Sort findings by strength
  findings.sort((a,b) => b.strength - a.strength);
  const tradeable = findings.filter(f=>f.tradeable);

  L(`Found ${findings.length} patterns, ${tradeable.length} potentially tradeable`);

  return res.status(200).json({ success:true, findings, tradeable, dowResults, log });
}

// ─── AI FRESH EYES PATTERN ANALYSIS ──────────────────────────────────────────
async function runAIPatterns(req, res) {
  const { sql } = require('@vercel/postgres');
  const log = [];
  const L = msg => { console.log('[AIPatterns]', msg); log.push(msg); };

  try {
    // Build a rich multi-instrument dataset for Claude to analyse
    const INSTRUMENTS = [
      { name:'S&P 500',  epic:'IX.D.SPTRD.DAILY.IP' },
      { name:'Nasdaq',   epic:'IX.D.NASDAQ.CASH.IP' },
      { name:'FTSE 100', epic:'IX.D.FTSE.DAILY.IP' },
      { name:'DAX 40',   epic:'IX.D.DAX.DAILY.IP' },
      { name:'Gold',     epic:'CS.D.USCGC.TODAY.IP' },
      { name:'Brent Oil',epic:'CC.D.LCO.USS.IP' },
      { name:'GBP/USD',  epic:'CS.D.GBPUSD.TODAY.IP' },
      { name:'EUR/USD',  epic:'CS.D.EURUSD.TODAY.IP' },
      { name:'USD/JPY',  epic:'CS.D.USDJPY.TODAY.IP' },
    ];

    // Get last 120 days of daily returns for all instruments
    const matrixData = {};
    const dateSet = new Set();

    for(const instr of INSTRUMENTS) {
      const rows = await sql`
        SELECT close_price, candle_time::date as dt
        FROM price_history
        WHERE (epic=${instr.epic} OR instrument=${instr.name})
        AND resolution='DAY' AND close_price>0
        ORDER BY candle_time DESC LIMIT 125`;
      const data = rows.rows.reverse();
      if(data.length < 60) continue;

      const closes = data.map(r=>parseFloat(r.close_price));
      const dates = data.map(r=>new Date(r.dt).toISOString().substring(0,10));

      matrixData[instr.name] = {};
      dates.forEach((date, i) => {
        if(i===0) return;
        const ret = ((closes[i]-closes[i-1])/closes[i-1]*100).toFixed(2);
        matrixData[instr.name][date] = parseFloat(ret);
        dateSet.add(date);
      });
    }

    // Build aligned date series (last 90 trading days)
    const sortedDates = [...dateSet].sort().slice(-90);

    // Build compact return matrix string for Claude
    // Format: DATE | SP500 | NAS | FTSE | DAX | GOLD | OIL | GBPUSD | EURUSD | USDJPY
    const instrNames = Object.keys(matrixData);
    let matrix = 'DATE,' + instrNames.join(',') + '\n';
    sortedDates.forEach(date => {
      const row = instrNames.map(name => {
        const val = matrixData[name][date];
        return val !== undefined ? val : '0';
      });
      matrix += date + ',' + row.join(',') + '\n';
    });

    // Also compute some derived series
    // Rolling 5-day volatility per instrument
    const volSeries = {};
    instrNames.forEach(name => {
      const rets = sortedDates.map(d => matrixData[name][d] || 0);
      volSeries[name] = rets.map((_, i) => {
        if(i < 5) return 0;
        const slice = rets.slice(i-5, i);
        const mean = slice.reduce((a,b)=>a+b,0)/5;
        const std = Math.sqrt(slice.reduce((a,b)=>a+Math.pow(b-mean,2),0)/5);
        return parseFloat(std.toFixed(3));
      });
    });

    // Cross-instrument same-day correlations (rolling 20-day)
    const correlations = {};
    instrNames.forEach((a, ai) => {
      instrNames.slice(ai+1).forEach(b => {
        const retsA = sortedDates.map(d => matrixData[a][d] || 0);
        const retsB = sortedDates.map(d => matrixData[b][d] || 0);
        // Last 20 days correlation
        const n = 20;
        const sliceA = retsA.slice(-n), sliceB = retsB.slice(-n);
        const meanA = sliceA.reduce((x,y)=>x+y,0)/n;
        const meanB = sliceB.reduce((x,y)=>x+y,0)/n;
        let num=0, denA=0, denB=0;
        for(let i=0;i<n;i++){
          num+=(sliceA[i]-meanA)*(sliceB[i]-meanB);
          denA+=Math.pow(sliceA[i]-meanA,2);
          denB+=Math.pow(sliceB[i]-meanB,2);
        }
        const corr = (denA*denB)>0 ? num/Math.sqrt(denA*denB) : 0;
        correlations[`${a}/${b}`] = parseFloat(corr.toFixed(3));
      });
    });

    // Identify any unusual recent behaviour vs historical
    const anomalies = [];
    instrNames.forEach(name => {
      const allRets = sortedDates.map(d => matrixData[name][d] || 0);
      const mean = allRets.reduce((a,b)=>a+b,0)/allRets.length;
      const std = Math.sqrt(allRets.reduce((a,b)=>a+Math.pow(b-mean,2),0)/allRets.length);
      // Last 5 days vs historical
      const recent5 = allRets.slice(-5);
      const recentMean = recent5.reduce((a,b)=>a+b,0)/5;
      if(Math.abs(recentMean - mean) > std * 1.5) {
        anomalies.push(`${name}: recent 5-day avg ${recentMean.toFixed(2)}% vs historical ${mean.toFixed(2)}% (${recentMean>mean?'unusually strong':'unusually weak'})`);
      }
    });

    L(`Matrix built: ${sortedDates.length} days × ${instrNames.length} instruments`);
    L(`Anomalies detected: ${anomalies.length}`);

    // Send to Claude with genuinely open-ended prompt
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    const prompt = `You are a quantitative researcher with no preconceptions about trading strategies. 
I'm giving you 90 days of daily return data across 9 financial instruments. 

Your job: look at this data with completely fresh eyes and identify ANY patterns, relationships, or behaviours that are unusual, unexpected, or potentially predictive — regardless of whether they match any known trading strategy.

Do NOT just list standard strategies (RSI, MACD, moving averages etc). I want genuinely novel observations about what this specific dataset shows.

RETURN MATRIX (% daily returns):
${matrix}

RECENT 20-DAY CORRELATIONS:
${Object.entries(correlations).map(([k,v])=>`${k}: ${v}`).join('\n')}

RECENT ANOMALIES (instruments behaving unusually vs their own history):
${anomalies.join('\n') || 'None detected'}

Analyse this data and report:
1. Any unexpected correlation patterns or correlation breakdowns
2. Any instruments showing unusual sequential behaviour (specific multi-day sequences)
3. Any lead/lag relationships you notice between instruments
4. Any clustering of volatility or calm periods across multiple instruments simultaneously  
5. Any patterns in WHEN the relationships between instruments change
6. Anything else genuinely surprising or non-obvious in this data

Be specific — cite actual dates and numbers from the data. Think like a data scientist finding signal in noise, not a trader looking for textbook setups. Maximum 600 words.`;

    const aiRes = await fetch(`${base}/api/claude`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-5',
        max_tokens: 1000,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const aiData = await aiRes.json();
    const analysis = aiData.content?.[0]?.text || 'No analysis returned';
    L('AI analysis complete');

    return res.status(200).json({
      success: true,
      analysis,
      correlations,
      anomalies,
      daysAnalysed: sortedDates.length,
      instruments: instrNames,
      dateRange: { from: sortedDates[0], to: sortedDates[sortedDates.length-1] },
      log
    });

  } catch(e) {
    L('Error: ' + e.message);
    return res.status(500).json({ error: e.message, log });
  }
}

// ─── PAIRS BACKTEST ───────────────────────────────────────────────────────────
async function runPairsBacktest(req, res) {
  const { sql } = require('@vercel/postgres');
  const log = [];
  const L = msg => { console.log('[PairsBT]', msg); log.push(msg); };

  const PAIRS_CONFIG = [
    { id:'ftse_dax',      name:'FTSE / DAX',        a:'FTSE 100',  b:'DAX 40'   },
    { id:'gold_silver',   name:'Gold / Silver',      a:'Gold',      b:'Silver'   },
    { id:'gbpusd_eurusd', name:'GBP/USD vs EUR/USD', a:'GBP/USD',  b:'EUR/USD'  },
    { id:'eurusd_eurgbp', name:'EUR/USD vs EUR/GBP', a:'EUR/USD',  b:'EUR/GBP'  },
    { id:'brent_gold',    name:'Brent / Gold',       a:'Brent Oil', b:'Gold'     },
    { id:'nasdaq_gold',   name:'Nasdaq / Gold',      a:'Nasdaq',    b:'Gold'     },
  ];

  const pairId   = req.query.pair || 'ftse_dax';
  const entryZ   = parseFloat(req.query.entryZ   || '1.5');  // Z-score to enter
  const exitZ    = parseFloat(req.query.exitZ    || '0.5');  // Z-score to exit (mean revert)
  const stopZ    = parseFloat(req.query.stopZ    || '3.0');  // Z-score stop loss
  const lookback = parseInt(req.query.lookback   || '60');   // rolling window for mean/std
  const days     = parseInt(req.query.days       || '500');

  const pair = PAIRS_CONFIG.find(p => p.id === pairId);
  if(!pair) return res.status(400).json({ error: 'Unknown pair: ' + pairId });

  L(`Pairs backtest: ${pair.name} | entry Z≥${entryZ} | exit Z≤${exitZ} | stop Z≥${stopZ} | lookback ${lookback}d`);

  try {
    // Fetch candles for both instruments
    const [rowsA, rowsB] = await Promise.all([
      sql`SELECT close_price, candle_time::date as dt FROM price_history
          WHERE instrument=${pair.a} AND resolution='DAY' AND close_price>0.0001
          ORDER BY candle_time ASC LIMIT ${days+lookback+10}`,
      sql`SELECT close_price, candle_time::date as dt FROM price_history
          WHERE instrument=${pair.b} AND resolution='DAY' AND close_price>0.0001
          ORDER BY candle_time ASC LIMIT ${days+lookback+10}`
    ]);

    if(rowsA.rows.length < 30) return res.status(200).json({ error: `Insufficient data for ${pair.a} — ${rowsA.rows.length} candles` });
    if(rowsB.rows.length < 30) return res.status(200).json({ error: `Insufficient data for ${pair.b} — ${rowsB.rows.length} candles` });

    // Align by date
    const mapB = {};
    rowsB.rows.forEach(r => { mapB[new Date(r.dt).toISOString().substring(0,10)] = parseFloat(r.close_price); });

    const aligned = [];
    rowsA.rows.forEach(r => {
      const date = new Date(r.dt).toISOString().substring(0,10);
      const priceB = mapB[date];
      if(priceB) aligned.push({ date, priceA: parseFloat(r.close_price), priceB });
    });

    L(`Aligned: ${aligned.length} days with both instruments`);
    if(aligned.length < lookback + 10) return res.status(200).json({ error: `Only ${aligned.length} aligned days — need ${lookback+10}` });

    // Calculate rolling Z-score for each day
    const zscores = [];
    for(let i = lookback; i < aligned.length; i++) {
      const window = aligned.slice(i-lookback, i);
      const ratios = window.map(d => d.priceA / d.priceB);
      const mean = ratios.reduce((a,b)=>a+b,0) / ratios.length;
      const std = Math.sqrt(ratios.reduce((a,b)=>a+Math.pow(b-mean,2),0) / ratios.length);
      const ratio = aligned[i].priceA / aligned[i].priceB;
      const z = std > 0 ? (ratio - mean) / std : 0;
      zscores.push({ ...aligned[i], ratio, mean, std, z });
    }

    L(`Z-scores calculated for ${zscores.length} days`);

    // Simulate trades
    const trades = [];
    let inTrade = null;

    for(let i = 1; i < zscores.length; i++) {
      const d = zscores[i];
      const prev = zscores[i-1];

      if(!inTrade) {
        // Entry: Z crosses entryZ threshold
        // Positive Z = A expensive vs B → SELL A / BUY B
        // Negative Z = A cheap vs B → BUY A / SELL B
        if(prev.z < entryZ && d.z >= entryZ) {
          inTrade = { entryDate: d.date, entryZ: d.z, entryRatio: d.ratio,
            direction: 'SELL_A', entryPriceA: d.priceA, entryPriceB: d.priceB,
            mean: d.mean, std: d.std };
          L(`SELL_A entry: ${d.date} Z=${d.z.toFixed(2)} ratio=${d.ratio.toFixed(4)}`);
        } else if(prev.z > -entryZ && d.z <= -entryZ) {
          inTrade = { entryDate: d.date, entryZ: d.z, entryRatio: d.ratio,
            direction: 'BUY_A', entryPriceA: d.priceA, entryPriceB: d.priceB,
            mean: d.mean, std: d.std };
          L(`BUY_A entry: ${d.date} Z=${d.z.toFixed(2)} ratio=${d.ratio.toFixed(4)}`);
        }
      } else {
        // Check exit conditions
        const absZ = Math.abs(d.z);
        const pnlRatio = inTrade.direction === 'SELL_A'
          ? (inTrade.entryRatio - d.ratio) / inTrade.entryRatio * 100  // profit when ratio falls
          : (d.ratio - inTrade.entryRatio) / inTrade.entryRatio * 100; // profit when ratio rises

        // Track peak P&L
        if(!inTrade.peakPnl || pnlRatio > inTrade.peakPnl) {
          inTrade.peakPnl = pnlRatio;
          inTrade.peakDate = d.date;
        }

        let exitReason = null;
        if(inTrade.direction === 'SELL_A' && d.z <= exitZ)  exitReason = 'mean_revert';
        if(inTrade.direction === 'BUY_A'  && d.z >= -exitZ) exitReason = 'mean_revert';
        if(absZ >= stopZ) exitReason = 'stop_loss';

        // Max hold: 60 days
        const daysHeld = (new Date(d.date) - new Date(inTrade.entryDate)) / (1000*60*60*24);
        if(daysHeld >= 60) exitReason = 'time_exit';

        if(exitReason) {
          const won = pnlRatio > 0;
          trades.push({
            entryDate: inTrade.entryDate,
            exitDate: d.date,
            direction: inTrade.direction,
            entryZ: inTrade.entryZ.toFixed(2),
            exitZ: d.z.toFixed(2),
            entryRatio: inTrade.entryRatio.toFixed(4),
            exitRatio: d.ratio.toFixed(4),
            pnlPct: pnlRatio.toFixed(2),
            peakPnl: (inTrade.peakPnl||0).toFixed(2),
            daysHeld: Math.round(daysHeld),
            exitReason, won
          });
          L(`Exit ${exitReason}: ${d.date} Z=${d.z.toFixed(2)} P&L=${pnlRatio.toFixed(2)}%`);
          inTrade = null;
        }
      }
    }

    // Calculate stats
    const wins = trades.filter(t=>t.won);
    const losses = trades.filter(t=>!t.won);
    const totalPnl = trades.reduce((s,t)=>s+parseFloat(t.pnlPct),0);
    const avgWin = wins.length ? wins.reduce((s,t)=>s+parseFloat(t.pnlPct),0)/wins.length : 0;
    const avgLoss = losses.length ? losses.reduce((s,t)=>s+parseFloat(t.pnlPct),0)/losses.length : 0;
    const winRate = trades.length ? wins.length/trades.length*100 : 0;
    const expectancy = trades.length ? totalPnl/trades.length : 0;
    const grossWin = wins.reduce((s,t)=>s+parseFloat(t.pnlPct),0);
    const grossLoss = Math.abs(losses.reduce((s,t)=>s+parseFloat(t.pnlPct),0));
    const profitFactor = grossLoss > 0 ? grossWin/grossLoss : wins.length > 0 ? 999 : 0;
    const avgDaysHeld = trades.length ? trades.reduce((s,t)=>s+t.daysHeld,0)/trades.length : 0;
    const couldHaveWon = trades.filter(t=>!t.won && parseFloat(t.peakPnl)>0.1).length;

    L(`Results: ${trades.length} trades | ${winRate.toFixed(1)}% WR | exp ${expectancy.toFixed(2)}% | PF ${profitFactor.toFixed(2)}`);

    // Exit reason breakdown
    const byExit = {};
    trades.forEach(t => {
      if(!byExit[t.exitReason]) byExit[t.exitReason] = { trades:0, wins:0, totalPnl:0 };
      byExit[t.exitReason].trades++;
      if(t.won) byExit[t.exitReason].wins++;
      byExit[t.exitReason].totalPnl += parseFloat(t.pnlPct);
    });

    return res.status(200).json({
      success: true,
      pair: pair.name,
      params: { entryZ, exitZ, stopZ, lookback, days: zscores.length },
      summary: {
        totalTrades: trades.length,
        winRate: parseFloat(winRate.toFixed(1)),
        expectancy: parseFloat(expectancy.toFixed(2)),
        profitFactor: parseFloat(profitFactor.toFixed(2)),
        avgWin: parseFloat(avgWin.toFixed(2)),
        avgLoss: parseFloat(avgLoss.toFixed(2)),
        totalPnl: parseFloat(totalPnl.toFixed(2)),
        avgDaysHeld: parseFloat(avgDaysHeld.toFixed(1)),
        couldHaveWon,
      },
      byExit,
      recentTrades: trades.slice(-15),
      log
    });

  } catch(e) {
    L('Error: ' + e.message);
    return res.status(500).json({ error: e.message, log });
  }
}

// ─── DEEP ANALYSIS — ALL RESOLUTIONS ─────────────────────────────────────────
async function runDeepAnalysis(req, res) {
  const { sql } = require('@vercel/postgres');
  const log = [];
  const L = msg => { console.log('[DeepAnalysis]', msg); log.push(msg); };

  const INSTRUMENTS = [
    { name:'S&P 500',  epic:'IX.D.SPTRD.DAILY.IP' },
    { name:'Nasdaq',   epic:'IX.D.NASDAQ.CASH.IP' },
    { name:'FTSE 100', epic:'IX.D.FTSE.DAILY.IP' },
    { name:'DAX 40',   epic:'IX.D.DAX.DAILY.IP' },
    { name:'Gold',     epic:'CS.D.USCGC.TODAY.IP' },
    { name:'Brent Oil',epic:'CC.D.LCO.USS.IP' },
    { name:'GBP/USD',  epic:'CS.D.GBPUSD.TODAY.IP' },
    { name:'EUR/USD',  epic:'CS.D.EURUSD.TODAY.IP' },
    { name:'EUR/GBP',  epic:'CS.D.EURGBP.TODAY.IP' },
    { name:'USD/JPY',  epic:'CS.D.USDJPY.TODAY.IP' },
  ];

  try {
    const data = {};

    for(const instr of INSTRUMENTS) {
      data[instr.name] = {};

      // Daily — last 90 days of returns
      const daily = await sql`
        SELECT close_price, candle_time::date as dt
        FROM price_history
        WHERE (epic=${instr.epic} OR instrument=${instr.name})
        AND resolution='DAY' AND close_price>0.0001
        ORDER BY candle_time DESC LIMIT 90`;

      const dailyRows = daily.rows.reverse();
      data[instr.name].daily = dailyRows.map((r,i) => {
        if(i===0) return null;
        const ret = ((parseFloat(r.close_price)-parseFloat(dailyRows[i-1].close_price))/parseFloat(dailyRows[i-1].close_price)*100).toFixed(2);
        return { date: new Date(r.dt).toISOString().substring(0,10), ret: parseFloat(ret), close: parseFloat(r.close_price) };
      }).filter(Boolean);

      // Hourly — last 7 days, returns per hour
      const hourly = await sql`
        SELECT close_price, candle_time
        FROM price_history
        WHERE (epic=${instr.epic} OR instrument=${instr.name})
        AND resolution='HOUR' AND close_price>0.0001
        ORDER BY candle_time DESC LIMIT 56`;

      const hourlyRows = hourly.rows.reverse();
      data[instr.name].hourly = hourlyRows.map((r,i) => {
        if(i===0) return null;
        const ret = ((parseFloat(r.close_price)-parseFloat(hourlyRows[i-1].close_price))/parseFloat(hourlyRows[i-1].close_price)*100).toFixed(3);
        return { time: new Date(r.candle_time).toISOString().substring(0,16), ret: parseFloat(ret) };
      }).filter(Boolean);

      // Minute — last 24 hours, 5-minute buckets (average to reduce noise)
      const minute = await sql`
        SELECT AVG(close_price) as close_price,
               date_trunc('hour', candle_time) + INTERVAL '5 min' * FLOOR(EXTRACT(MINUTE FROM candle_time)/5) as bucket
        FROM price_history
        WHERE (epic=${instr.epic} OR instrument=${instr.name})
        AND resolution='MINUTE' AND close_price>0.0001
        AND candle_time > NOW() - INTERVAL '24 hours'
        GROUP BY bucket ORDER BY bucket ASC`;

      const minRows = minute.rows;
      data[instr.name].minute5 = minRows.map((r,i) => {
        if(i===0) return null;
        const ret = ((parseFloat(r.close_price)-parseFloat(minRows[i-1].close_price))/parseFloat(minRows[i-1].close_price)*100).toFixed(4);
        return { time: new Date(r.bucket).toISOString().substring(11,16), ret: parseFloat(ret) };
      }).filter(Boolean);
    }

    L(`Data loaded for ${Object.keys(data).length} instruments across 3 resolutions`);

    // Build compact summary for AI
    // Daily: last 30 days returns matrix
    const instrNames = Object.keys(data);
    let dailySummary = 'DAILY RETURNS (last 30 days, %):\n';
    dailySummary += instrNames.join(',') + '\n';
    const maxDaily = Math.max(...instrNames.map(n => data[n].daily.length));
    for(let i = Math.max(0, data[instrNames[0]].daily.length - 30); i < data[instrNames[0]].daily.length; i++) {
      const row = instrNames.map(n => data[n].daily[i]?.ret ?? '0');
      const date = data[instrNames[0]].daily[i]?.date || '';
      dailySummary += date + ',' + row.join(',') + '\n';
    }

    // Hourly: last 48 hours
    let hourlySummary = '\nHOURLY RETURNS (last 48 hours, %):\n';
    hourlySummary += instrNames.join(',') + '\n';
    instrNames.forEach(n => {
      const last48 = data[n].hourly.slice(-48);
      data[n]._hourly48 = last48;
    });
    const hourLen = Math.max(...instrNames.map(n => data[n]._hourly48.length));
    for(let i = 0; i < hourLen; i++) {
      const row = instrNames.map(n => data[n]._hourly48[i]?.ret ?? '0');
      const time = data[instrNames[0]]._hourly48[i]?.time || '';
      hourlySummary += time + ',' + row.join(',') + '\n';
    }

    // Minute: today only — show pattern of intraday moves
    let minuteSummary = '\n5-MINUTE RETURNS TODAY (%):\n';
    instrNames.forEach(n => {
      const m5 = data[n].minute5;
      if(m5.length > 0) {
        const cumRet = m5.reduce((s,r) => s + r.ret, 0).toFixed(3);
        const volatility = Math.sqrt(m5.reduce((s,r) => s + r.ret*r.ret, 0)/m5.length).toFixed(4);
        const trend = m5.slice(-6).reduce((s,r) => s + r.ret, 0).toFixed(3); // last 30min
        minuteSummary += `${n}: cumulative ${cumRet>0?'+':''}${cumRet}% | volatility ${volatility}%/5min | last 30min ${trend>0?'+':''}${trend}%\n`;
      }
    });

    // Compute some derived stats
    const correlations = {};
    instrNames.forEach((a,ai) => instrNames.slice(ai+1).forEach(b => {
      const retsA = data[a].daily.slice(-20).map(d=>d.ret);
      const retsB = data[b].daily.slice(-20).map(d=>d.ret);
      const n = Math.min(retsA.length, retsB.length);
      if(n < 5) return;
      const meanA = retsA.reduce((s,v)=>s+v,0)/n;
      const meanB = retsB.reduce((s,v)=>s+v,0)/n;
      let num=0,denA=0,denB=0;
      for(let i=0;i<n;i++){num+=(retsA[i]-meanA)*(retsB[i]-meanB);denA+=Math.pow(retsA[i]-meanA,2);denB+=Math.pow(retsB[i]-meanB,2);}
      const corr = (denA*denB)>0 ? num/Math.sqrt(denA*denB) : 0;
      if(Math.abs(corr)>0.4) correlations[`${a}/${b}`] = parseFloat(corr.toFixed(3));
    }));

    // Momentum scores
    const momentum = {};
    instrNames.forEach(n => {
      const d = data[n].daily;
      if(d.length >= 5) {
        momentum[n] = {
          '1d': d[d.length-1]?.ret || 0,
          '5d': d.slice(-5).reduce((s,r)=>s+r.ret,0),
          '20d': d.slice(-20).reduce((s,r)=>s+r.ret,0),
          'volatility': parseFloat(Math.sqrt(d.slice(-20).reduce((s,r)=>s+r.ret*r.ret,0)/20).toFixed(3))
        };
      }
    });

    L('Computed correlations and momentum scores');

    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    const prompt = `You are a quantitative analyst with no preconceptions. You have access to three resolutions of market data — daily (90 days), hourly (48 hours), and 5-minute (today only) — for 10 financial instruments.

Your task: find ANY patterns, behaviours, or anomalies that could be predictive or tradeable. Look across all timeframes simultaneously. Be specific, cite numbers and times.

DO NOT mention RSI, MACD, moving averages, Bollinger bands, or any standard technical indicator. I want observations about the RAW DATA only.

${dailySummary}
${hourlySummary}
${minuteSummary}

RECENT 20-DAY CORRELATIONS (only showing |r|>0.4):
${Object.entries(correlations).map(([k,v])=>`${k}: ${v}`).join('\n')}

MOMENTUM SCORES (cumulative %):
${Object.entries(momentum).map(([n,m])=>`${n}: 1d=${m['1d']>0?'+':''}${m['1d'].toFixed(2)}% | 5d=${m['5d']>0?'+':''}${m['5d'].toFixed(2)}% | 20d=${m['20d']>0?'+':''}${m['20d'].toFixed(2)}% | vol=${m.volatility}%/day`).join('\n')}

Find:
1. Unusual intraday patterns visible in the 5-minute data today
2. Any instrument showing abnormal behaviour vs its recent hourly pattern
3. Cross-instrument relationships that appear or disappear between timeframes
4. Any sequential patterns (e.g. instrument A moves, then B follows hours later)
5. Volatility clustering — periods where multiple instruments simultaneously calm or spike
6. Anything genuinely surprising that a human analyst might miss

Be specific. Cite actual numbers and times. Max 700 words.`;

    const aiRes = await fetch(`${base}/api/claude`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-5',
        max_tokens: 1200,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const aiData = await aiRes.json();
    const analysis = aiData.content?.[0]?.text || 'No analysis returned';
    L('AI analysis complete');

    return res.status(200).json({
      success: true,
      analysis,
      correlations,
      momentum,
      instruments: instrNames,
      dataPoints: {
        daily: data[instrNames[0]]?.daily?.length,
        hourly: data[instrNames[0]]?._hourly48?.length,
        minute5: data[instrNames[0]]?.minute5?.length,
      },
      log
    });

  } catch(e) {
    L('Error: ' + e.message);
    return res.status(500).json({ error: e.message, log });
  }
}

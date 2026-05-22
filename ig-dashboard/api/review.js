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
    : thresholdParam >= 1 ? 28 + (thresholdParam - 1) * 2  // 1→28, 2→30, 3→32, 4→34, 5→36, 6→38, 7→40
    : 33;
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
      SELECT close_price, candle_time::date as dt
      FROM price_history
      WHERE (epic = ${epic} OR instrument = ${instrName})
      AND resolution = 'DAY' AND close_price > 0
      ORDER BY candle_time ASC
      LIMIT ${days + 50}`;

    if (rows.rows.length < 30) {
      return res.status(200).json({ error: 'Insufficient data', rows: rows.rows.length });
    }

    const closes = rows.rows.map(r => parseFloat(r.close_price));
    const dates = rows.rows.map(r => r.dt);
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

      // Check for signals
      let direction = null;
      let signalType = null;

      // Mean reversion BUY — oversold
      if(rsi <= rsiEntry && !inDowntrend) {
        direction = 'BUY';
        signalType = 'MR_BUY';
      }
      // Mean reversion SELL — overbought  
      else if(rsi >= (100 - rsiEntry) && !inUptrend) {
        direction = 'SELL';
        signalType = 'MR_SELL';
      }

      if(!direction) continue;

      // Score filter — require score ≥ 0 (relaxed for backtest analysis)
      // Run with different thresholds to show impact
      const passesScore0 = Math.abs(score) >= 0;
      const passesScore1 = Math.abs(score) >= 1;
      const passesScore2 = Math.abs(score) >= 2;

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

      // ATR-based stop (1.5× ATR)
      const stopDist = atr * 1.5;
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

      const pnlPct = direction==='BUY'
        ? (exitPrice-entryPrice)/entryPrice*100
        : (entryPrice-exitPrice)/entryPrice*100;
      const won = pnlPct > 0;

      trades.push({
        date: dates[i], direction, signalType,
        entryPrice: entryPrice.toFixed(2),
        exitPrice: exitPrice.toFixed(2),
        rsi: rsi.toFixed(1), score,
        pnlPct: pnlPct.toFixed(2),
        exitDay, exitReason, won,
        passesScore0, passesScore1, passesScore2,
        trendOK: !inDowntrend && !inUptrend,
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

    L(`Total signals: ${allTrades.length}`);
    L(`With score≥1: ${withScore1.length} (win rate: ${stats.scoreGte1.winRate}%)`);
    L(`With score≥2: ${withScore2.length} (win rate: ${stats.scoreGte2.winRate}%)`);
    L(`With trend filter: ${withTrendFilter.length} (win rate: ${stats.trendFilter.winRate}%)`);
    L(`Full filter (score≥2+trend): ${fullFilter.length} (win rate: ${stats.fullFilter.winRate}%)`);

    // Build summary in format expected by existing UI
    const fs = stats.fullFilter;
    const summary = {
      totalTrades: fs.trades,
      winRate: parseFloat(fs.winRate),
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
    const recentTrades = fullFilter.slice(-10).map(t => ({
      direction: t.direction,
      openPrice: t.entryPrice,
      closePrice: t.exitPrice,
      pnl: parseFloat(t.pnlPct),
      regime: 'ranging',
    }));

    return res.status(200).json({
      instrument: instrName, epic, days: closes.length,
      rsiEntry, holdDays, stats, summary, byRegime, recentTrades,
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

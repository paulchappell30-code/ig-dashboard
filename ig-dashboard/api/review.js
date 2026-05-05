// Weekly Review + Backtesting Engine v3
// GET  /api/review?action=weekly  — generate weekly performance review email
// GET  /api/review?action=backtest&epic=X&days=30  — backtest strategy on stored data
// POST /api/review  — trigger weekly review manually
const fetch = require('node-fetch');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || 'weekly';

  if (action === 'backtest') {
    return await runBacktest(req, res);
  }

  // Weekly review
  return await sendWeeklyReview(req, res);
};

async function sendWeeklyReview(req, res) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;

    // Fetch stats
    const statsRes = await fetch(`${base}/api/db?action=stats`);
    const stats = await statsRes.json();

    // Fetch recent trades
    const tradesRes = await fetch(`${base}/api/db?action=trades&limit=50`);
    const tradesData = await tradesRes.json();
    const trades = tradesData.trades || [];

    // Fetch equity snapshots
    const equityRes = await fetch(`${base}/api/db?action=equity&days=7`);
    const equityData = await equityRes.json();
    const snapshots = equityData.snapshots || [];

    if (stats.totalTrades === 0) {
      return res.status(200).json({ message: 'No trades to review yet' });
    }

    // Calculate week-specific stats
    const oneWeekAgo = new Date();
    oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

    const weekTrades = trades.filter(t => t.opened_at && new Date(t.opened_at) > oneWeekAgo);
    const weekClosed = weekTrades.filter(t => t.status === 'closed');
    const weekWins = weekClosed.filter(t => (t.profit_loss || 0) > 0);
    const weekPnL = weekClosed.reduce((sum, t) => sum + (t.profit_loss || 0), 0);
    const weekWinRate = weekClosed.length > 0 ? (weekWins.length / weekClosed.length * 100).toFixed(1) : 'N/A';

    // Instrument breakdown
    const byInstrument = {};
    weekClosed.forEach(t => {
      if (!byInstrument[t.instrument]) byInstrument[t.instrument] = { trades: 0, pnl: 0, wins: 0 };
      byInstrument[t.instrument].trades++;
      byInstrument[t.instrument].pnl += t.profit_loss || 0;
      if ((t.profit_loss || 0) > 0) byInstrument[t.instrument].wins++;
    });

    const instrBreakdown = Object.entries(byInstrument)
      .map(([instr, d]) => `${instr}: ${d.trades} trades, £${d.pnl.toFixed(2)} P&L, ${((d.wins/d.trades)*100).toFixed(0)}% win rate`)
      .join('\n');

    // Equity change this week
    const firstSnap = snapshots[0];
    const lastSnap = snapshots[snapshots.length - 1];
    const weekEquityChange = firstSnap && lastSnap
      ? lastSnap.balance - firstSnap.balance
      : 0;

    // Ask Claude for analysis
    let aiAnalysis = '';
    if (process.env.ANTHROPIC_API_KEY && weekClosed.length > 0) {
      const tradeDetails = weekClosed.slice(0, 10).map(t =>
        `${t.instrument} ${t.direction} → ${t.profit_loss >= 0 ? '+' : ''}£${(t.profit_loss || 0).toFixed(2)} (AI confidence: ${t.ai_confidence || '?'}%, score: ${t.signal_score || '?'})`
      ).join('\n');

      const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({
          model: 'claude-sonnet-4-5',
          max_tokens: 500,
          messages: [{
            role: 'user',
            content: `You are a trading performance coach. Analyse this week's automated trading results and provide actionable insights.

WEEK SUMMARY:
- Total trades: ${weekTrades.length} (${weekClosed.length} closed)
- Win rate: ${weekWinRate}%
- Total P&L: £${weekPnL.toFixed(2)}
- Account equity change: £${weekEquityChange.toFixed(2)}

INSTRUMENT BREAKDOWN:
${instrBreakdown || 'No closed trades this week'}

INDIVIDUAL TRADES:
${tradeDetails || 'No trades'}

Provide:
1. What went well this week (if anything)
2. What patterns you see in the winning vs losing trades
3. 3 specific actionable improvements for next week
4. One risk warning if applicable

Be direct and specific. Focus on what the data shows, not generic advice.`
          }]
        })
      });
      const aiData = await aiRes.json();
      aiAnalysis = aiData.content && aiData.content[0] && aiData.content[0].text || '';
    }

    // Build email body
    const emailBody = `WEEKLY TRADING REPORT
Week ending ${new Date().toLocaleDateString('en-GB', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' })}

═══════════════════════════════════
WEEK SUMMARY
═══════════════════════════════════
Trades this week:  ${weekTrades.length} opened, ${weekClosed.length} closed
Win rate:          ${weekWinRate}%
Weekly P&L:        ${weekPnL >= 0 ? '+' : ''}£${weekPnL.toFixed(2)}
Account change:    ${weekEquityChange >= 0 ? '+' : ''}£${weekEquityChange.toFixed(2)}

═══════════════════════════════════
ALL-TIME STATS
═══════════════════════════════════
Total trades:    ${stats.totalTrades}
Overall win rate: ${stats.winRate}%
Total P&L:       £${(stats.totalPnL || 0).toFixed(2)}
Best trade:      ${stats.bestTrade ? '+£' + stats.bestTrade.profit_loss + ' (' + stats.bestTrade.instrument + ')' : 'N/A'}
Worst trade:     ${stats.worstTrade ? '£' + stats.worstTrade.profit_loss + ' (' + stats.worstTrade.instrument + ')' : 'N/A'}

═══════════════════════════════════
INSTRUMENT BREAKDOWN
═══════════════════════════════════
${instrBreakdown || 'No closed trades this week'}

${aiAnalysis ? `═══════════════════════════════════
AI PERFORMANCE ANALYSIS
═══════════════════════════════════
${aiAnalysis}` : ''}

Generated: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`;

    // Send email
    const notifyRes = await fetch(`${base}/api/notify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'daily_summary',
        subject: `📊 Weekly Trading Report — ${weekPnL >= 0 ? '+' : ''}£${weekPnL.toFixed(2)} | ${weekWinRate}% win rate`,
        body: emailBody
      })
    });
    const notifyData = await notifyRes.json();

    return res.status(200).json({
      success: true,
      weekTrades: weekTrades.length,
      weekPnL: weekPnL.toFixed(2),
      weekWinRate,
      emailSent: notifyData.sent,
      preview: emailBody.substring(0, 300) + '...'
    });

  } catch(e) {
    console.error('[Review]', e.message);
    return res.status(500).json({ error: e.message });
  }
}

async function runBacktest(req, res) {
  const epic = req.query.epic || 'IX.D.FTSE.DAILY.IP';
  const days = parseInt(req.query.days) || 30;
  const signalThreshold = parseInt(req.query.threshold) || 2;

  try {
    const { sql } = require('@vercel/postgres');

    // Fetch stored price history
    const result = await sql`
      SELECT close_price, candle_time FROM price_history
      WHERE epic = ${epic} AND resolution = 'DAY' AND close_price > 0
      ORDER BY candle_time ASC
      LIMIT ${days + 60}
    `;

    if (result.rows.length < 10) {
      return res.status(200).json({
        error: 'Insufficient price history for backtesting',
        available: result.rows.length,
        needed: 10,
        hint: 'Run the price collector for at least a few days to build history'
      });
    }

    const closes = result.rows.map(r => parseFloat(r.close_price));
    const times = result.rows.map(r => r.candle_time);

    // Backtest: walk through data generating signals
    const trades = [];
    let openTrade = null;
    const lookback = 20; // Candles needed before first signal

    for (let i = lookback; i < closes.length; i++) {
      const window = closes.slice(Math.max(0, i - 60), i);
      const regime = detectRegime(window);
      const score = calcScore(window, regime);

      if (!openTrade && Math.abs(score) >= signalThreshold) {
        // Open trade
        openTrade = {
          direction: score > 0 ? 'BUY' : 'SELL',
          openPrice: closes[i],
          openTime: times[i],
          score, regime
        };
      } else if (openTrade) {
        // Check exit conditions
        const currentPrice = closes[i];
        const upl = openTrade.direction === 'BUY'
          ? currentPrice - openTrade.openPrice
          : openTrade.openPrice - currentPrice;
        const uplPct = (upl / openTrade.openPrice) * 100;

        // Exit if signal reverses or after 5 candles
        const holdingPeriod = i - closes.indexOf(openTrade.openPrice, Math.max(0, i - 20));
        const reversal = (openTrade.direction === 'BUY' && score <= -signalThreshold) ||
                        (openTrade.direction === 'SELL' && score >= signalThreshold);

        if (reversal || holdingPeriod >= 5) {
          trades.push({
            direction: openTrade.direction,
            openPrice: openTrade.openPrice,
            closePrice: currentPrice,
            openTime: openTrade.openTime,
            closeTime: times[i],
            pnl: upl,
            pnlPct: uplPct,
            holdingPeriod,
            score: openTrade.score,
            regime: openTrade.regime,
            exitReason: reversal ? 'signal_reversal' : 'time_exit'
          });
          openTrade = null;
        }
      }
    }

    // Calculate backtest stats
    const wins = trades.filter(t => t.pnl > 0);
    const losses = trades.filter(t => t.pnl <= 0);
    const totalPnL = trades.reduce((sum, t) => sum + t.pnl, 0);
    const maxDrawdown = calculateMaxDrawdown(trades);
    const winRate = trades.length > 0 ? (wins.length / trades.length * 100).toFixed(1) : 0;
    const avgWin = wins.length > 0 ? wins.reduce((s, t) => s + t.pnl, 0) / wins.length : 0;
    const avgLoss = losses.length > 0 ? losses.reduce((s, t) => s + t.pnl, 0) / losses.length : 0;
    const profitFactor = Math.abs(avgLoss) > 0 ? Math.abs(avgWin / avgLoss) : 0;

    // Regime breakdown
    const byRegime = {};
    trades.forEach(t => {
      if (!byRegime[t.regime]) byRegime[t.regime] = { trades: 0, pnl: 0, wins: 0 };
      byRegime[t.regime].trades++;
      byRegime[t.regime].pnl += t.pnl;
      if (t.pnl > 0) byRegime[t.regime].wins++;
    });

    return res.status(200).json({
      epic, days: result.rows.length, signalThreshold,
      summary: {
        totalTrades: trades.length,
        winRate: parseFloat(winRate),
        totalPnL: parseFloat(totalPnL.toFixed(4)),
        maxDrawdown: parseFloat(maxDrawdown.toFixed(4)),
        profitFactor: parseFloat(profitFactor.toFixed(2)),
        avgWin: parseFloat(avgWin.toFixed(4)),
        avgLoss: parseFloat(avgLoss.toFixed(4)),
        expectancy: parseFloat(((parseFloat(winRate)/100 * avgWin) + ((1-parseFloat(winRate)/100) * avgLoss)).toFixed(4))
      },
      byRegime,
      recentTrades: trades.slice(-10)
    });

  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
}

function calculateMaxDrawdown(trades) {
  let peak = 0, maxDrawdown = 0, cumPnL = 0;
  for (const t of trades) {
    cumPnL += t.pnl;
    if (cumPnL > peak) peak = cumPnL;
    const drawdown = peak - cumPnL;
    if (drawdown > maxDrawdown) maxDrawdown = drawdown;
  }
  return maxDrawdown;
}

function detectRegime(closes) {
  const n = closes.length;
  if (n < 10) return 'unknown';
  const recent = closes.slice(-5);
  const recentRange = Math.max(...recent) - Math.min(...recent);
  const totalRange = Math.max(...closes) - Math.min(...closes);
  const trendStr = recentRange / (totalRange || 1);
  const mid = Math.floor(n / 2);
  const h1 = closes.slice(0, mid).reduce((a, b) => a + b, 0) / mid;
  const h2 = closes.slice(mid).reduce((a, b) => a + b, 0) / (n - mid);
  const slope = ((h2 - h1) / h1) * 100;
  if (Math.abs(slope) > 3 && trendStr > 0.3) return slope > 0 ? 'uptrend' : 'downtrend';
  return 'ranging';
}

function calcScore(closes, regime) {
  const n = closes.length;
  if (n < 5) return 0;
  let s = 0;
  const rsi = calcRSI(closes);
  const sma20 = calcSMA(closes, Math.min(20, n));
  const sma50 = calcSMA(closes, Math.min(50, n));
  const macd = calcEMA(closes, Math.min(12, n)) - calcEMA(closes, Math.min(26, n));
  const mom = n >= 10 ? ((closes[n-1] - closes[n-10]) / closes[n-10]) * 100 : 0;
  const bb = calcBB(closes);

  if (regime === 'ranging') {
    if (rsi < 25) s += 4; else if (rsi < 35) s += 3;
    else if (rsi > 75) s -= 4; else if (rsi > 65) s -= 3;
    if (closes[n-1] < bb.lower) s += 3; else if (closes[n-1] > bb.upper) s -= 3;
    if (mom > 1) s += 1; else if (mom < -1) s -= 1;
    if (sma20 > sma50) s += 1; else s -= 1;
    if (macd > 0) s += 1; else s -= 1;
  } else if (regime === 'uptrend') {
    if (sma20 > sma50) s += 3; else s -= 3;
    if (mom > 2) s += 3; else if (mom > 1) s += 2; else if (mom < -1) s -= 2;
    if (macd > 0) s += 2; else s -= 1;
    if (rsi < 45) s += 2; else if (rsi > 75) s -= 2;
  } else if (regime === 'downtrend') {
    if (sma20 < sma50) s -= 3; else s += 3;
    if (mom < -2) s -= 3; else if (mom < -1) s -= 2; else if (mom > 1) s += 2;
    if (macd < 0) s -= 2; else s += 1;
    if (rsi > 55) s -= 2;
  } else {
    if (rsi < 30) s += 2; else if (rsi < 40) s += 1;
    else if (rsi > 70) s -= 2; else if (rsi > 60) s -= 1;
    if (sma20 > sma50) s += 1; else s -= 1;
    if (macd > 0) s += 1; else s -= 1;
    if (mom > 1) s += 1; else if (mom < -1) s -= 1;
    if (closes[n-1] < bb.lower) s += 1; else if (closes[n-1] > bb.upper) s -= 1;
  }
  return s;
}

function calcRSI(c, p = 14) {
  const period = Math.min(p, c.length - 1);
  if (period < 2) return 50;
  let g = 0, l = 0;
  for (let i = c.length - period; i < c.length; i++) {
    const d = c[i] - c[i-1];
    if (d > 0) g += d; else l += Math.abs(d);
  }
  const ag = g/period, al = l/period;
  if (al === 0) return 100;
  return 100 - (100 / (1 + ag/al));
}

function calcSMA(c, p) {
  const n = Math.min(p, c.length);
  return c.slice(-n).reduce((a, b) => a + b, 0) / n;
}

function calcEMA(c, p) {
  const n = Math.min(p, c.length);
  if (n < 2) return c[c.length-1];
  const k = 2 / (n + 1);
  let e = c.slice(0, n).reduce((a, b) => a + b, 0) / n;
  for (let i = n; i < c.length; i++) e = c[i] * k + e * (1 - k);
  return e;
}

function calcBB(c, p = 20) {
  const n = Math.min(p, c.length);
  const sma = c.slice(-n).reduce((a, b) => a + b, 0) / n;
  const std = Math.sqrt(c.slice(-n).reduce((s, v) => s + Math.pow(v - sma, 2), 0) / n);
  return { upper: sma + 2*std, middle: sma, lower: sma - 2*std };
}

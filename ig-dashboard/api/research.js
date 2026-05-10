// Research Engine — AI Pattern Discovery & Strategy Evolution
// GET  /api/research              — status and last findings
// POST /api/research              — run full research cycle
// POST /api/research?action=apply — apply approved suggestions
const fetch = require('node-fetch');
const { sql } = require('@vercel/postgres');

const MIN_TRADES_REQUIRED = 30; // Minimum trades before meaningful analysis

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || req.body?.action || 'research';

  if (req.method === 'GET') {
    // Return last research findings
    try {
      const result = await sql`
        SELECT * FROM research_findings
        ORDER BY created_at DESC LIMIT 1
      `;
      return res.status(200).json({
        status: 'Research engine ready',
        minTradesRequired: MIN_TRADES_REQUIRED,
        lastFindings: result.rows[0] || null,
      });
    } catch(e) {
      return res.status(200).json({ status: 'Research engine ready', minTradesRequired: MIN_TRADES_REQUIRED });
    }
  }

  // Apply approved suggestions
  if (action === 'apply') {
    const { suggestions } = req.body || {};
    if (!suggestions?.length) return res.status(400).json({ error: 'No suggestions provided' });

    const applied = [];
    for (const s of suggestions) {
      if (!s.approved) continue;
      // Save approved changes to optimized_params table
      try {
        await sql`
          INSERT INTO optimized_params (signal_threshold, ai_confidence_min, params_json, created_at, backtest_score)
          VALUES (
            ${s.signalThreshold || 2},
            ${s.aiConfidenceMin || 60},
            ${JSON.stringify(s)},
            NOW(),
            ${s.expectedImprovement || 0}
          )
        `;
        applied.push(s.title);
      } catch(e) { console.error('[Research] Apply error:', e.message); }
    }
    return res.status(200).json({ success: true, applied });
  }

  // Full research cycle
  const log = [];
  const L = msg => { console.log('[Research]', msg); log.push(msg); };

  L('=== Research Engine === ' + new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' }));

  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;

    // 1. Get all closed trades
    const tradesResult = await sql`
      SELECT * FROM trades
      WHERE status = 'closed' AND profit_loss IS NOT NULL
      ORDER BY closed_at DESC
      LIMIT 200
    `;
    const trades = tradesResult.rows;
    L(`Trades available: ${trades.length}`);

    if (trades.length < MIN_TRADES_REQUIRED) {
      L(`Insufficient data — need ${MIN_TRADES_REQUIRED} trades, have ${trades.length}`);
      return res.status(200).json({
        action: 'insufficient_data',
        tradesAvailable: trades.length,
        tradesRequired: MIN_TRADES_REQUIRED,
        estimatedReadyDate: estimateReadyDate(trades.length),
        log
      });
    }

    // 2. Calculate statistics for Claude to analyse
    const stats = calculateStats(trades);
    L(`Win rate: ${stats.winRate.toFixed(1)}% | Avg P&L: £${stats.avgPnL.toFixed(2)} | Total: £${stats.totalPnL.toFixed(2)}`);

    // 3. Build comprehensive data summary for Claude
    const dataSummary = buildDataSummary(trades, stats);

    // 4. Send to Claude Sonnet for deep analysis
    if (!process.env.ANTHROPIC_API_KEY) {
      return res.status(200).json({ error: 'ANTHROPIC_API_KEY not configured', log });
    }

    L('Sending to Claude Sonnet for pattern analysis...');

    const prompt = `You are a quantitative trading analyst reviewing the performance of an automated spread betting system. Analyse the data below and identify genuine patterns, then suggest specific, actionable parameter changes.

SYSTEM OVERVIEW:
- Instruments: FTSE 100, S&P 500, DAX, Dow, Brent Oil, Gold, GBP/USD, EUR/USD, USD/JPY, CAC 40, Nikkei 225, Nasdaq
- Signal scoring: RSI, SMA crossover, MACD, Bollinger Bands, news sentiment, IG client sentiment, Twelve Data
- Regimes: uptrend, downtrend, ranging
- Current threshold: 2 (signal must score ≥2 to be evaluated)
- Current AI confidence minimum: 60%

PERFORMANCE DATA:
${dataSummary}

ANALYSIS REQUIRED:
1. REGIME ANALYSIS: Which market regimes produce profitable trades? Should we disable trading in certain regimes?
2. INSTRUMENT ANALYSIS: Which instruments are profitable/unprofitable? Any to exclude or prioritise?
3. AI CALIBRATION: Are high-confidence AI scores (70%+) actually more profitable than low ones (60-70%)? Should we raise the minimum?
4. TIME OF DAY: Are certain hours significantly more profitable? Should we add time filters?
5. SIGNAL PATTERNS: Do certain signal combinations work better? (e.g. RSI + MACD agreement vs RSI alone)
6. DIRECTION BIAS: Are BUY or SELL signals more accurate? Is there a directional bias to exploit?
7. HOLDING TIME: Do short-held trades (< 2 hours) outperform long-held ones?
8. SPURIOUS PATTERNS: Flag any patterns with fewer than 8 trades as statistically unreliable.

RESPOND WITH JSON ONLY — no preamble, no markdown:
{
  "summary": "2-3 sentence overview of key findings",
  "confidence": "high/medium/low based on data quality and sample size",
  "patterns": [
    {
      "title": "Pattern name",
      "finding": "What the data shows",
      "sampleSize": 15,
      "winRateAffected": "62% vs 41% baseline",
      "reliable": true,
      "actionable": true
    }
  ],
  "suggestions": [
    {
      "title": "Suggestion title",
      "rationale": "Why this change based on data",
      "change": "Specific parameter to change",
      "currentValue": "current value",
      "suggestedValue": "new value",
      "expectedImprovement": 5.2,
      "riskLevel": "low/medium/high",
      "signalThreshold": 2,
      "aiConfidenceMin": 65,
      "approved": false
    }
  ],
  "dataQuality": "Assessment of whether we have enough data for reliable conclusions",
  "nextReviewRecommendation": "When to run next analysis"
}`;

    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-5',
        max_tokens: 2000,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const aiData = await aiRes.json();
    const aiText = aiData.content?.[0]?.text || '{}';

    let findings;
    try {
      findings = JSON.parse(aiText.replace(/```json|```/g, '').trim());
    } catch(e) {
      L('JSON parse error: ' + e.message);
      findings = { summary: aiText.substring(0, 500), patterns: [], suggestions: [] };
    }

    L(`Patterns found: ${findings.patterns?.length || 0}`);
    L(`Suggestions: ${findings.suggestions?.length || 0}`);
    L(`Summary: ${findings.summary}`);

    // 5. Save findings to DB
    try {
      await sql`
        CREATE TABLE IF NOT EXISTS research_findings (
          id SERIAL PRIMARY KEY,
          trade_count INTEGER,
          win_rate DECIMAL(5,2),
          total_pnl DECIMAL(10,2),
          findings JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `;
      await sql`
        INSERT INTO research_findings (trade_count, win_rate, total_pnl, findings, created_at)
        VALUES (${trades.length}, ${stats.winRate}, ${stats.totalPnL}, ${JSON.stringify(findings)}, NOW())
      `;
    } catch(e) { L('DB save error: ' + e.message); }

    // 6. Send email summary
    try {
      const emailBody = buildEmailSummary(findings, stats, trades.length);
      await fetch(`${base}/api/notify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          subject: `🔬 Research Report — ${findings.patterns?.length || 0} patterns found | ${stats.winRate.toFixed(0)}% win rate`,
          body: emailBody
        })
      });
      L('Research report emailed');
    } catch(e) { L('Email error: ' + e.message); }

    return res.status(200).json({
      success: true,
      tradesAnalysed: trades.length,
      stats,
      findings,
      log
    });

  } catch(e) {
    L('Research error: ' + e.message);
    return res.status(500).json({ error: e.message, log });
  }
};

function calculateStats(trades) {
  const closed = trades.filter(t => t.profit_loss !== null);
  const wins = closed.filter(t => parseFloat(t.profit_loss) > 0);
  const totalPnL = closed.reduce((s, t) => s + parseFloat(t.profit_loss || 0), 0);

  // By regime
  const byRegime = {};
  closed.forEach(t => {
    const r = t.regime || 'unknown';
    if (!byRegime[r]) byRegime[r] = { trades: 0, wins: 0, pnl: 0 };
    byRegime[r].trades++;
    if (parseFloat(t.profit_loss) > 0) byRegime[r].wins++;
    byRegime[r].pnl += parseFloat(t.profit_loss || 0);
  });

  // By instrument
  const byInstrument = {};
  closed.forEach(t => {
    const k = t.instrument || 'unknown';
    if (!byInstrument[k]) byInstrument[k] = { trades: 0, wins: 0, pnl: 0 };
    byInstrument[k].trades++;
    if (parseFloat(t.profit_loss) > 0) byInstrument[k].wins++;
    byInstrument[k].pnl += parseFloat(t.profit_loss || 0);
  });

  // By hour
  const byHour = {};
  closed.forEach(t => {
    const h = t.open_hour ?? 'unknown';
    if (!byHour[h]) byHour[h] = { trades: 0, wins: 0, pnl: 0 };
    byHour[h].trades++;
    if (parseFloat(t.profit_loss) > 0) byHour[h].wins++;
    byHour[h].pnl += parseFloat(t.profit_loss || 0);
  });

  // AI calibration
  const byConfidence = { high: { trades:0,wins:0,pnl:0 }, medium: { trades:0,wins:0,pnl:0 }, low: { trades:0,wins:0,pnl:0 } };
  closed.forEach(t => {
    const conf = parseInt(t.ai_confidence || 0);
    const bucket = conf >= 75 ? 'high' : conf >= 65 ? 'medium' : 'low';
    byConfidence[bucket].trades++;
    if (parseFloat(t.profit_loss) > 0) byConfidence[bucket].wins++;
    byConfidence[bucket].pnl += parseFloat(t.profit_loss || 0);
  });

  // By direction
  const byDirection = {};
  closed.forEach(t => {
    const d = t.direction || 'unknown';
    if (!byDirection[d]) byDirection[d] = { trades: 0, wins: 0, pnl: 0 };
    byDirection[d].trades++;
    if (parseFloat(t.profit_loss) > 0) byDirection[d].wins++;
    byDirection[d].pnl += parseFloat(t.profit_loss || 0);
  });

  // Holding time analysis
  const shortHeld = closed.filter(t => (t.holding_minutes || 0) < 120);
  const longHeld = closed.filter(t => (t.holding_minutes || 0) >= 120);

  return {
    total: closed.length,
    wins: wins.length,
    winRate: closed.length > 0 ? (wins.length / closed.length) * 100 : 0,
    totalPnL,
    avgPnL: closed.length > 0 ? totalPnL / closed.length : 0,
    byRegime,
    byInstrument,
    byHour,
    byConfidence,
    byDirection,
    shortHeld: {
      count: shortHeld.length,
      winRate: shortHeld.length > 0 ? (shortHeld.filter(t => parseFloat(t.profit_loss) > 0).length / shortHeld.length) * 100 : 0,
      avgPnL: shortHeld.length > 0 ? shortHeld.reduce((s,t) => s+parseFloat(t.profit_loss||0), 0) / shortHeld.length : 0
    },
    longHeld: {
      count: longHeld.length,
      winRate: longHeld.length > 0 ? (longHeld.filter(t => parseFloat(t.profit_loss) > 0).length / longHeld.length) * 100 : 0,
      avgPnL: longHeld.length > 0 ? longHeld.reduce((s,t) => s+parseFloat(t.profit_loss||0), 0) / longHeld.length : 0
    },
  };
}

function buildDataSummary(trades, stats) {
  const lines = [];

  lines.push(`OVERALL: ${stats.total} trades | ${stats.winRate.toFixed(1)}% win rate | £${stats.totalPnL.toFixed(2)} total P&L | avg £${stats.avgPnL.toFixed(2)}/trade`);

  lines.push('\nBY REGIME:');
  Object.entries(stats.byRegime).forEach(([r, d]) => {
    const wr = d.trades > 0 ? (d.wins/d.trades*100).toFixed(0) : 0;
    lines.push(`  ${r}: ${d.trades} trades | ${wr}% WR | £${d.pnl.toFixed(2)} P&L`);
  });

  lines.push('\nBY INSTRUMENT:');
  Object.entries(stats.byInstrument).sort((a,b) => b[1].pnl - a[1].pnl).forEach(([instr, d]) => {
    const wr = d.trades > 0 ? (d.wins/d.trades*100).toFixed(0) : 0;
    lines.push(`  ${instr}: ${d.trades} trades | ${wr}% WR | £${d.pnl.toFixed(2)} P&L`);
  });

  lines.push('\nAI CONFIDENCE CALIBRATION:');
  Object.entries(stats.byConfidence).forEach(([level, d]) => {
    const wr = d.trades > 0 ? (d.wins/d.trades*100).toFixed(0) : 0;
    lines.push(`  ${level} (${level==='high'?'75%+':level==='medium'?'65-74%':'<65%'}): ${d.trades} trades | ${wr}% WR | £${d.pnl.toFixed(2)} P&L`);
  });

  lines.push('\nBY DIRECTION:');
  Object.entries(stats.byDirection).forEach(([dir, d]) => {
    const wr = d.trades > 0 ? (d.wins/d.trades*100).toFixed(0) : 0;
    lines.push(`  ${dir}: ${d.trades} trades | ${wr}% WR | £${d.pnl.toFixed(2)} P&L`);
  });

  lines.push('\nHOLDING TIME:');
  lines.push(`  Short (<2h): ${stats.shortHeld.count} trades | ${stats.shortHeld.winRate.toFixed(0)}% WR | avg £${stats.shortHeld.avgPnL.toFixed(2)}`);
  lines.push(`  Long (2h+): ${stats.longHeld.count} trades | ${stats.longHeld.winRate.toFixed(0)}% WR | avg £${stats.longHeld.avgPnL.toFixed(2)}`);

  lines.push('\nBY HOUR (UTC):');
  Object.entries(stats.byHour).sort((a,b) => parseInt(a[0])-parseInt(b[0])).forEach(([h, d]) => {
    if (d.trades >= 3) {
      const wr = (d.wins/d.trades*100).toFixed(0);
      lines.push(`  ${h}:00 — ${d.trades} trades | ${wr}% WR | £${d.pnl.toFixed(2)} P&L`);
    }
  });

  // Recent 10 trades
  lines.push('\nRECENT 10 TRADES:');
  trades.slice(0, 10).forEach(t => {
    const pl = parseFloat(t.profit_loss || 0);
    lines.push(`  ${t.instrument} ${t.direction} | P&L: ${pl>=0?'+':''}£${pl.toFixed(2)} | AI: ${t.ai_confidence||'?'}% | Regime: ${t.regime||'?'} | Hour: ${t.open_hour??'?'}:00 | Held: ${t.holding_minutes||'?'}m`);
  });

  return lines.join('\n');
}

function buildEmailSummary(findings, stats, tradeCount) {
  const lines = [
    `RESEARCH REPORT — ${new Date().toLocaleDateString('en-GB', {weekday:'long',day:'numeric',month:'long'})}`,
    `Trades analysed: ${tradeCount} | Win rate: ${stats.winRate.toFixed(1)}% | Total P&L: £${stats.totalPnL.toFixed(2)}`,
    '',
    'SUMMARY:',
    findings.summary || 'No summary available',
    '',
    `CONFIDENCE: ${findings.confidence || 'unknown'}`,
    `DATA QUALITY: ${findings.dataQuality || 'unknown'}`,
    '',
    'PATTERNS FOUND:',
  ];

  (findings.patterns || []).forEach((p, i) => {
    lines.push(`${i+1}. ${p.title} (${p.reliable ? '✅ Reliable' : '⚠️ Tentative'} — ${p.sampleSize} trades)`);
    lines.push(`   ${p.finding}`);
    if (p.winRateAffected) lines.push(`   Win rate: ${p.winRateAffected}`);
    lines.push('');
  });

  lines.push('SUGGESTED CHANGES (review in dashboard):');
  (findings.suggestions || []).forEach((s, i) => {
    lines.push(`${i+1}. ${s.title} [${s.riskLevel} risk]`);
    lines.push(`   ${s.rationale}`);
    lines.push(`   Change: ${s.change} from ${s.currentValue} to ${s.suggestedValue}`);
    lines.push(`   Expected improvement: +${s.expectedImprovement}%`);
    lines.push('');
  });

  lines.push(`Next review: ${findings.nextReviewRecommendation || 'In 2-4 weeks'}`);
  lines.push(`Generated: ${new Date().toLocaleString('en-GB', {timeZone:'Europe/London'})}`);

  return lines.join('\n');
}

function estimateReadyDate(currentTrades) {
  // Rough estimate: 1-2 trades per day on average
  const tradesNeeded = MIN_TRADES_REQUIRED - currentTrades;
  const daysNeeded = Math.ceil(tradesNeeded / 1.5);
  const readyDate = new Date(Date.now() + daysNeeded * 24 * 60 * 60 * 1000);
  return readyDate.toLocaleDateString('en-GB', { weekday: 'long', day: 'numeric', month: 'long' });
}

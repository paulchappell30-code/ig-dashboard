// Automated Trading Engine
// Evaluates signals, checks daily limits, and places trades automatically
// Called by cron-job.org every 5 minutes during market hours
const fetch = require('node-fetch');

// Simple in-memory price cache (resets on each cold start)
const priceCache = {};
const CACHE_TTL = 15 * 60 * 1000; // 15 minutes

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

const EPIC_MAP = {
  'FTSE 100':  'IX.D.FTSE.DAILY.IP',
  'S&P 500':   'IX.D.SPTRD.DAILY.IP',
  'DAX 40':    'IX.D.DAX.DAILY.IP',
  'Dow Jones': 'IX.D.DOW.DAILY.IP',
  'Brent Oil': 'CC.D.LCO.USS.IP',
};

// Default config — overridden by env vars
const DEFAULT_CONFIG = {
  dailyProfitLock: 2.0,     // Stop opening new positions when up this %
  dailyLossLimit: 1.0,      // Close all and stop when down this %
  maxPositions: 3,          // Max concurrent open positions
  defaultSize: 1,           // Default deal size in units
  maxSizePerTrade: 5,       // Max units per trade
  requireAIConfirm: true,   // Use AI to confirm signals before trading
  aiConfidenceMin: 60,      // Minimum AI confidence % to place trade
  enabled: true,            // Master on/off switch
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Auth
  const cronSecret = process.env.CRON_SECRET || '';
  const authHeader = req.headers['authorization'] || '';
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorised' });
  }

  // Load config from env
  const config = {
    ...DEFAULT_CONFIG,
    dailyProfitLock:  parseFloat(process.env.DAILY_PROFIT_LOCK  || DEFAULT_CONFIG.dailyProfitLock),
    dailyLossLimit:   parseFloat(process.env.DAILY_LOSS_LIMIT   || DEFAULT_CONFIG.dailyLossLimit),
    maxPositions:     parseInt(process.env.MAX_POSITIONS        || DEFAULT_CONFIG.maxPositions),
    defaultSize:      parseInt(process.env.DEFAULT_SIZE         || DEFAULT_CONFIG.defaultSize),
    maxSizePerTrade:  parseInt(process.env.MAX_SIZE_PER_TRADE   || DEFAULT_CONFIG.maxSizePerTrade),
    requireAIConfirm: process.env.REQUIRE_AI_CONFIRM !== 'false',
    aiConfidenceMin:  parseInt(process.env.AI_CONFIDENCE_MIN    || DEFAULT_CONFIG.aiConfidenceMin),
    enabled:          process.env.AUTO_TRADING_ENABLED !== 'false',
  };

  if (req.method === 'GET') {
    return res.status(200).json({ status: 'Auto-trading engine ready', config, time: new Date().toISOString() });
  }

  if (!config.enabled) {
    return res.status(200).json({ message: 'Auto-trading disabled', config });
  }

  const igBase = IG_BASES[process.env.IG_ENV || 'demo'];
  let cst, xst;

  // Authenticate
  try {
    const authRes = await fetch(`${igBase}/session`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-IG-API-KEY': process.env.IG_API_KEY || '', 'Version': '2' },
      body: JSON.stringify({ identifier: process.env.IG_USERNAME, password: process.env.IG_PASSWORD })
    });
    if (!authRes.ok) return res.status(500).json({ error: 'IG auth failed' });
    cst = authRes.headers.get('CST');
    xst = authRes.headers.get('X-SECURITY-TOKEN');
    if (!cst) return res.status(500).json({ error: 'No CST token' });
  } catch(e) {
    return res.status(500).json({ error: 'Auth error: ' + e.message });
  }

  const igHeaders = {
    'Content-Type': 'application/json',
    'X-IG-API-KEY': process.env.IG_API_KEY || '',
    'CST': cst,
    'X-SECURITY-TOKEN': xst,
  };

  const log = [];
  const addLog = (msg) => { console.log('[AutoTrade]', msg); log.push(msg); };

  addLog('Engine started — ' + new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' }));

  // ── STEP 1: Get account state ─────────────────────────────────────────────
  let accountBalance, dailyPL, availableFunds;
  try {
    const acctRes = await fetch(`${igBase}/accounts`, { headers: { ...igHeaders, 'Version': '1' } });
    const acctData = await acctRes.json();
    const acct = acctData.accounts && acctData.accounts.find(a => a.accountType === 'SPREADBET');
    if (!acct) { addLog('No spreadbet account found'); return res.status(200).json({ log }); }
    accountBalance = acct.balance.balance;
    dailyPL = acct.balance.profitLoss;
    availableFunds = acct.balance.available;
    addLog(`Account: £${accountBalance} | P&L: £${dailyPL} | Available: £${availableFunds}`);
  } catch(e) {
    addLog('Error fetching account: ' + e.message);
    return res.status(200).json({ log });
  }

  // ── STEP 2: Check daily limits ────────────────────────────────────────────
  const dailyPLPct = accountBalance > 0 ? (dailyPL / accountBalance) * 100 : 0;
  addLog(`Daily P&L: ${dailyPLPct.toFixed(2)}% | Profit lock: +${config.dailyProfitLock}% | Loss limit: -${config.dailyLossLimit}%`);

  // Check loss limit — close all and stop
  if (dailyPLPct <= -config.dailyLossLimit) {
    addLog(`⚠️ DAILY LOSS LIMIT HIT (${dailyPLPct.toFixed(2)}%) — closing all positions`);
    const closed = await closeAllPositions(igBase, igHeaders);
    addLog(`Closed ${closed} positions`);
    await sendNotification('error',
      '🛑 Daily Loss Limit Hit — All Positions Closed',
      `Your automated trading engine has hit the daily loss limit.\n\nDaily P&L: ${dailyPLPct.toFixed(2)}%\nLimit: -${config.dailyLossLimit}%\nPositions closed: ${closed}\nAccount balance: £${accountBalance}\n\nAuto-trading has been paused for the rest of the day.`
    );
    return res.status(200).json({ action: 'loss_limit_hit', closed, dailyPLPct, log });
  }

  // Check profit lock — stop opening new positions
  if (dailyPLPct >= config.dailyProfitLock) {
    addLog(`✅ DAILY PROFIT LOCK HIT (${dailyPLPct.toFixed(2)}%) — no new positions today`);
    await sendNotification('dca',
      '✅ Daily Profit Target Locked In',
      `Your automated trading engine has hit the daily profit target.\n\nDaily P&L: +${dailyPLPct.toFixed(2)}%\nTarget: +${config.dailyProfitLock}%\nAccount balance: £${accountBalance}\n\nNo new positions will be opened today. Existing positions remain open.`
    );
    return res.status(200).json({ action: 'profit_lock_hit', dailyPLPct, log });
  }

  // ── STEP 3: Check open positions ──────────────────────────────────────────
  let openPositions = [];
  try {
    const posRes = await fetch(`${igBase}/positions`, { headers: { ...igHeaders, 'Version': '1' } });
    const posData = await posRes.json();
    openPositions = posData.positions || [];
    addLog(`Open positions: ${openPositions.length}/${config.maxPositions}`);
  } catch(e) {
    addLog('Error fetching positions: ' + e.message);
  }

  if (openPositions.length >= config.maxPositions) {
    addLog(`Max positions reached (${openPositions.length}/${config.maxPositions}) — skipping new trades`);
    return res.status(200).json({ action: 'max_positions', openPositions: openPositions.length, log });
  }

  // ── STEP 4: Evaluate signals for each instrument ──────────────────────────
  const instruments = Object.keys(EPIC_MAP);
  const signals = [];

  for (const instr of instruments) {
    const epic = EPIC_MAP[instr];

    // Skip if already have position in this instrument
    const hasPosition = openPositions.some(p => p.market.epic === epic);
    if (hasPosition) { addLog(`${instr}: already has open position — skip`); continue; }

    try {
      // Fetch price history
      // Check cache first to avoid hitting historical data allowance
      const cacheKey = epic + '_DAY';
      const cached = priceCache[cacheKey];
      if (cached && Date.now() - cached.ts < CACHE_TTL) {
        addLog(`${instr}: using cached price data (${cached.closes.length} closes)`);
        const closes = cached.closes;
        if (closes.length < 5) { addLog(`${instr}: insufficient cached data`); continue; }
        // Skip to signal calculation using cached closes
        const rsi = calcRSI(closes, 14);
        const sma20 = closes.slice(-Math.min(20,closes.length)).reduce((a,b)=>a+b,0)/Math.min(20,closes.length);
        const sma50 = closes.slice(-Math.min(50,closes.length)).reduce((a,b)=>a+b,0)/Math.min(50,closes.length);
        const ema12 = calcEMA(closes, 12);
        const ema26 = calcEMA(closes, 26);
        const macd = ema12 - ema26;
        const lastClose = closes[closes.length - 1];
        const momentum = ((lastClose - closes[Math.max(0,closes.length-10)]) / closes[Math.max(0,closes.length-10)]) * 100;
        let score = 0; const reasons = [];
        if (rsi < 35) { score += 2; reasons.push(`RSI oversold (${rsi.toFixed(1)})`); }
        else if (rsi > 65) { score -= 2; reasons.push(`RSI overbought (${rsi.toFixed(1)})`); }
        if (sma20 > sma50) { score += 1; reasons.push('SMA bullish'); } else { score -= 1; reasons.push('SMA bearish'); }
        if (macd > 0) { score += 1; reasons.push('MACD positive'); } else { score -= 1; reasons.push('MACD negative'); }
        if (momentum > 1) { score += 1; reasons.push(`Momentum +${momentum.toFixed(1)}%`); } else if (momentum < -1) { score -= 1; reasons.push(`Momentum ${momentum.toFixed(1)}%`); }
        const direction = score >= 2 ? 'BUY' : score <= -2 ? 'SELL' : null;
        if (!direction) { addLog(`${instr}: score ${score} — no clear signal`); continue; }
        addLog(`${instr}: score ${score} → ${direction} (cached data)`);
        signals.push({ instr, epic, direction, score, reasons, rsi, sma20, sma50, macd, momentum, lastClose });
        continue;
      }

      const priceRes = await fetch(`${igBase}/prices/${epic}?resolution=DAY&max=30&pageSize=0`, {
        headers: { ...igHeaders, 'Version': '3' }
      });
      const priceData = await priceRes.json();
      addLog(`${instr}: price fetch status ${priceRes.status}, got ${(priceData.prices||[]).length} candles`);
      
      const closes = (priceData.prices || [])
        .map(p => {
          if (p.closePrice && p.closePrice.bid) return p.closePrice.bid;
          if (p.closePrice && p.closePrice.mid) return p.closePrice.mid;
          if (p.closePrice && p.closePrice.ask) return p.closePrice.ask;
          return 0;
        })
        .filter(p => p > 0);

      if (closes.length < 5) { addLog(`${instr}: insufficient price data (${closes.length} closes)`); continue; }
      // Cache for next run
      priceCache[cacheKey] = { closes, ts: Date.now() };

      // Calculate indicators
      const rsi = calcRSI(closes, 14);
      const sma20 = closes.slice(-20).reduce((a,b)=>a+b,0) / Math.min(20, closes.length);
      const sma50 = closes.slice(-50).reduce((a,b)=>a+b,0) / Math.min(50, closes.length);
      const ema12 = calcEMA(closes, 12);
      const ema26 = calcEMA(closes, 26);
      const macd = ema12 - ema26;
      const lastClose = closes[closes.length - 1];
      const momentum = ((lastClose - closes[Math.max(0, closes.length-10)]) / closes[Math.max(0, closes.length-10)]) * 100;

      // Score signals
      let score = 0;
      const reasons = [];

      if (rsi < 35) { score += 2; reasons.push(`RSI oversold (${rsi.toFixed(1)})`); }
      else if (rsi > 65) { score -= 2; reasons.push(`RSI overbought (${rsi.toFixed(1)})`); }

      if (sma20 > sma50) { score += 1; reasons.push('SMA bullish'); }
      else { score -= 1; reasons.push('SMA bearish'); }

      if (macd > 0) { score += 1; reasons.push('MACD positive'); }
      else { score -= 1; reasons.push('MACD negative'); }

      if (momentum > 1) { score += 1; reasons.push(`Momentum +${momentum.toFixed(1)}%`); }
      else if (momentum < -1) { score -= 1; reasons.push(`Momentum ${momentum.toFixed(1)}%`); }

      const direction = score >= 2 ? 'BUY' : score <= -2 ? 'SELL' : null;
      if (!direction) { addLog(`${instr}: score ${score} — no clear signal`); continue; }

      addLog(`${instr}: score ${score} → ${direction} signal (${reasons.join(', ')})`);
      signals.push({ instr, epic, direction, score, reasons, rsi, sma20, sma50, macd, momentum, lastClose });

    } catch(e) {
      addLog(`${instr}: error — ${e.message}`);
    }
  }

  if (!signals.length) {
    addLog('No signals generated — no trades placed');
    return res.status(200).json({ action: 'no_signals', log });
  }

  // Sort by signal strength
  signals.sort((a, b) => Math.abs(b.score) - Math.abs(a.score));
  addLog(`${signals.length} signal(s) generated — processing top signal`);

  // ── STEP 5: AI confirmation ───────────────────────────────────────────────
  const topSignal = signals[0];
  let aiApproved = !config.requireAIConfirm;
  let aiConfidence = 0;
  let aiReasoning = '';

  if (config.requireAIConfirm) {
    try {
      addLog(`Requesting AI confirmation for ${topSignal.instr} ${topSignal.direction}...`);
      const aiPrompt = `You are a trading risk manager. Evaluate this trade signal and decide whether to approve it.

INSTRUMENT: ${topSignal.instr}
SIGNAL: ${topSignal.direction}
SIGNAL SCORE: ${topSignal.score}/5
INDICATORS:
- RSI: ${topSignal.rsi.toFixed(1)} (${topSignal.rsi < 30 ? 'oversold' : topSignal.rsi > 70 ? 'overbought' : 'neutral'})
- SMA 20/50: ${topSignal.sma20.toFixed(0)}/${topSignal.sma50.toFixed(0)} (${topSignal.sma20 > topSignal.sma50 ? 'bullish' : 'bearish'})
- MACD: ${topSignal.macd.toFixed(2)} (${topSignal.macd > 0 ? 'positive' : 'negative'})
- Momentum (10d): ${topSignal.momentum.toFixed(2)}%
- Current price: ${topSignal.lastClose}

ACCOUNT CONTEXT:
- Daily P&L so far: ${dailyPLPct.toFixed(2)}%
- Profit lock target: +${config.dailyProfitLock}%
- Loss limit: -${config.dailyLossLimit}%
- Open positions: ${openPositions.length}/${config.maxPositions}

Respond ONLY in this JSON format, no other text:
{"approved":true,"confidence":75,"reasoning":"Brief reason","risk":"Main risk"}`;

      const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': process.env.ANTHROPIC_API_KEY || '',
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({
          model: 'claude-haiku-4-5-20251001',
          max_tokens: 200,
          messages: [{ role: 'user', content: aiPrompt }]
        })
      });

      const aiData = await aiRes.json();
      const aiText = aiData.content && aiData.content[0] && aiData.content[0].text || '{}';
      const aiResult = JSON.parse(aiText.replace(/```json|```/g, '').trim());

      aiApproved = aiResult.approved && aiResult.confidence >= config.aiConfidenceMin;
      aiConfidence = aiResult.confidence || 0;
      aiReasoning = aiResult.reasoning || '';
      addLog(`AI: ${aiApproved ? '✅ APPROVED' : '❌ REJECTED'} (${aiConfidence}% confidence) — ${aiReasoning}`);

    } catch(e) {
      addLog('AI confirmation error — proceeding without AI: ' + e.message);
      aiApproved = true; // Fallback to rules-only if AI fails
    }
  }

  if (!aiApproved) {
    addLog('Trade rejected by AI — no order placed');
    return res.status(200).json({ action: 'ai_rejected', signal: topSignal, aiConfidence, log });
  }

  // ── STEP 6: Place trade ───────────────────────────────────────────────────
  const size = Math.min(config.defaultSize, config.maxSizePerTrade);
  addLog(`Placing ${topSignal.direction} ${size} unit(s) on ${topSignal.instr}...`);

  try {
    const orderRes = await fetch(`${igBase}/positions/otc`, {
      method: 'POST',
      headers: { ...igHeaders, 'Version': '1' },
      body: JSON.stringify({
        epic: topSignal.epic,
        direction: topSignal.direction,
        size,
        orderType: 'MARKET',
        expiry: 'DFB',
        guaranteedStop: false,
        forceOpen: true,
        currencyCode: 'GBP',
        dealType: 'SPREADBET'
      })
    });

    const orderData = await orderRes.json();

    if (!orderData.dealReference) {
      addLog('Order failed: ' + (orderData.errorCode || JSON.stringify(orderData)));
      return res.status(200).json({ action: 'order_failed', error: orderData.errorCode, log });
    }

    // Check confirmation
    await new Promise(r => setTimeout(r, 1000));
    const confirmRes = await fetch(`${igBase}/confirms/${orderData.dealReference}`, { headers: { ...igHeaders, 'Version': '1' } });
    const confirm = await confirmRes.json();

    if (confirm.dealStatus === 'ACCEPTED') {
      addLog(`✅ Order ACCEPTED — ref: ${orderData.dealReference} at ${confirm.level}`);
      await sendNotification('dca',
        `✅ Auto-Trade: ${topSignal.direction} ${topSignal.instr}`,
        `Automated trade placed successfully.\n\nInstrument: ${topSignal.instr}\nDirection: ${topSignal.direction}\nSize: ${size} unit(s)\nPrice: ${confirm.level}\nDeal ref: ${orderData.dealReference}\n\nSignal reasons: ${topSignal.reasons.join(', ')}\nAI confidence: ${aiConfidence}%\nAI reasoning: ${aiReasoning}\n\nDaily P&L: ${dailyPLPct.toFixed(2)}%\nTime: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`
      );
      return res.status(200).json({ action: 'trade_placed', direction: topSignal.direction, instr: topSignal.instr, size, dealReference: orderData.dealReference, confirm, aiConfidence, log });
    } else {
      addLog('Order REJECTED by IG: ' + (confirm.reason || confirm.dealStatus));
      return res.status(200).json({ action: 'order_rejected', reason: confirm.reason, log });
    }

  } catch(e) {
    addLog('Order error: ' + e.message);
    return res.status(200).json({ action: 'order_error', error: e.message, log });
  }
};

// ── HELPERS ──────────────────────────────────────────────────────────────────

async function closeAllPositions(igBase, igHeaders) {
  let closed = 0;
  try {
    const posRes = await fetch(`${igBase}/positions`, { headers: { ...igHeaders, 'Version': '1' } });
    const posData = await posRes.json();
    const positions = posData.positions || [];
    for (const p of positions) {
      try {
        await fetch(`${igBase}/positions/otc/${p.position.dealId}`, {
          method: 'DELETE',
          headers: { ...igHeaders, 'Version': '1' }
        });
        closed++;
        await new Promise(r => setTimeout(r, 500)); // Rate limit
      } catch(e) {
        console.error('[AutoTrade] Close error:', e.message);
      }
    }
  } catch(e) {
    console.error('[AutoTrade] closeAll error:', e.message);
  }
  return closed;
}

function calcRSI(closes, period = 14) {
  if (closes.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const d = closes[i] - closes[i-1];
    if (d > 0) gains += d; else losses += Math.abs(d);
  }
  const ag = gains/period, al = losses/period;
  if (al === 0) return 100;
  return 100 - (100 / (1 + ag/al));
}

function calcEMA(closes, period) {
  if (closes.length < period) return closes[closes.length-1];
  const k = 2/(period+1);
  let ema = closes.slice(0, period).reduce((a,b)=>a+b,0)/period;
  for (let i = period; i < closes.length; i++) ema = closes[i]*k + ema*(1-k);
  return ema;
}

async function sendNotification(type, subject, body) {
  try {
    const base = process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'http://localhost:3000';
    await fetch(`${base}/api/notify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, subject, body })
    });
  } catch(e) {
    console.error('[AutoTrade] Notification error:', e.message);
  }
}

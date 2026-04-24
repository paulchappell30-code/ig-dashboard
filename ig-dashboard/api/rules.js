// Rule Engine — evaluates technical indicators and triggers orders
// GET  /api/rules — check rule engine status
// POST /api/rules — evaluate all active rules and execute triggers
const fetch = require('node-fetch');

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
  'Crude Oil': 'CC.D.LCO.USS.IP',
};

// Price resolution: number of candles to fetch per indicator
const INDICATOR_CANDLES = {
  RSI: 30,
  SMA: 60,
  EMA: 60,
  MACD: 60,
  BOLLINGER: 30,
  PRICE_CHANGE: 2,
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Auth check
  const authHeader = req.headers['authorization'] || '';
  const cronSecret = process.env.CRON_SECRET || '';
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorised' });
  }

  if (req.method === 'GET') {
    return res.status(200).json({
      status: 'Rule engine ready',
      env: process.env.IG_ENV || 'demo',
      hasCredentials: !!(process.env.IG_USERNAME && process.env.IG_PASSWORD),
      rules: getRules(),
      time: new Date().toISOString()
    });
  }

  // POST — evaluate rules
  const { manualRun, ruleId } = req.body || {};
  const allRules = getRules();
  const rulesToEval = ruleId
    ? allRules.filter(r => r.id === ruleId)
    : allRules.filter(r => r.active !== false);

  if (!rulesToEval.length) {
    return res.status(200).json({ message: 'No active rules to evaluate', results: [] });
  }

  // Authenticate with IG
  const igBase = IG_BASES[process.env.IG_ENV || 'demo'];
  let cst, xst;
  try {
    const authRes = await fetch(`${igBase}/session`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-IG-API-KEY': process.env.IG_API_KEY || '',
        'Version': '2'
      },
      body: JSON.stringify({
        identifier: process.env.IG_USERNAME,
        password: process.env.IG_PASSWORD
      })
    });
    if (!authRes.ok) {
      const err = await authRes.json();
      return res.status(500).json({ error: 'IG auth failed', detail: err });
    }
    cst = authRes.headers.get('CST');
    xst = authRes.headers.get('X-SECURITY-TOKEN');
    if (!cst) return res.status(500).json({ error: 'No CST token' });
    console.log('[Rules] Authenticated with IG');
  } catch (err) {
    return res.status(500).json({ error: 'Auth error', detail: err.message });
  }

  const results = [];

  for (const rule of rulesToEval) {
    try {
      const result = await evaluateRule(rule, igBase, cst, xst);
      results.push(result);

      if (result.triggered) {
        console.log('[Rules] Rule triggered:', rule.name, '— action:', rule.action);

        if (rule.action === 'alert') {
          await sendNotification('rule',
            `⚡ Rule Alert: ${rule.name}`,
            `Rule triggered — alert only, no order placed.\n\nRule: ${rule.name}\nInstrument: ${rule.instr}\nIndicator: ${rule.indicator} ${rule.condition} ${rule.threshold}\nCurrent value: ${result.indicatorValue}\nTime: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`
          );
        } else {
          // Place order
          const orderResult = await placeOrder(rule, igBase, cst, xst);
          result.order = orderResult;

          if (orderResult.success) {
            await sendNotification('rule',
              `✅ Rule Executed: ${rule.name}`,
              `Rule triggered and order placed.\n\nRule: ${rule.name}\nInstrument: ${rule.instr}\nAction: ${rule.action} £${rule.size}\nIndicator: ${rule.indicator} = ${result.indicatorValue}\nDeal reference: ${orderResult.dealReference}\nTime: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`
            );
          } else {
            await sendNotification('error',
              `❌ Rule Order Failed: ${rule.name}`,
              `Rule triggered but order failed.\n\nRule: ${rule.name}\nInstrument: ${rule.instr}\nError: ${orderResult.error}\nTime: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`
            );
          }
        }
      }
    } catch (err) {
      console.error('[Rules] Error evaluating rule:', rule.name, err.message);
      results.push({ rule: rule.name, error: err.message });
    }
  }

  res.status(200).json({ results, time: new Date().toISOString() });
};

// ─── RULE EVALUATION ─────────────────────────────────────────────────────────

async function evaluateRule(rule, igBase, cst, xst) {
  const epic = EPIC_MAP[rule.instr];
  if (!epic) return { rule: rule.name, error: 'Unknown instrument: ' + rule.instr };

  const indicator = (rule.indicator || '').toUpperCase().replace(' ', '_');
  const candles = INDICATOR_CANDLES[indicator] || 30;

  // Fetch historical prices
  const prices = await fetchPrices(epic, candles, igBase, cst, xst);
  if (!prices || prices.length < 2) {
    return { rule: rule.name, error: 'Insufficient price data', triggered: false };
  }

  // Calculate indicator value
  let indicatorValue, indicatorValue2;
  const closes = prices.map(p => p.close);

  switch (indicator) {
    case 'RSI':
      indicatorValue = calculateRSI(closes, 14);
      break;
    case 'SMA':
    case 'SMA_CROSS': {
      const fast = parseInt(rule.fastPeriod) || 20;
      const slow = parseInt(rule.slowPeriod) || 50;
      indicatorValue = calculateSMA(closes, fast);
      indicatorValue2 = calculateSMA(closes, slow);
      break;
    }
    case 'EMA': {
      const fast = parseInt(rule.fastPeriod) || 12;
      const slow = parseInt(rule.slowPeriod) || 26;
      indicatorValue = calculateEMA(closes, fast);
      indicatorValue2 = calculateEMA(closes, slow);
      break;
    }
    case 'MACD': {
      const macd = calculateMACD(closes);
      indicatorValue = macd.macd;
      indicatorValue2 = macd.signal;
      break;
    }
    case 'BOLLINGER': {
      const bb = calculateBollinger(closes, 20, 2);
      indicatorValue = closes[closes.length - 1];
      indicatorValue2 = bb;
      break;
    }
    case 'PRICE_CHANGE':
      indicatorValue = ((closes[closes.length - 1] - closes[closes.length - 2]) / closes[closes.length - 2]) * 100;
      break;
    default:
      return { rule: rule.name, error: 'Unknown indicator: ' + rule.indicator, triggered: false };
  }

  // Evaluate condition
  const threshold = parseFloat(rule.threshold);
  const condition = (rule.condition || '').toLowerCase();
  let triggered = false;

  // For crossover rules, compare two values
  if (indicator === 'SMA_CROSS' || indicator === 'EMA' || indicator === 'MACD') {
    const prevCloses = closes.slice(0, -1);
    let prevVal1, prevVal2;

    if (indicator === 'SMA_CROSS') {
      prevVal1 = calculateSMA(prevCloses, parseInt(rule.fastPeriod) || 20);
      prevVal2 = calculateSMA(prevCloses, parseInt(rule.slowPeriod) || 50);
    } else if (indicator === 'EMA') {
      prevVal1 = calculateEMA(prevCloses, parseInt(rule.fastPeriod) || 12);
      prevVal2 = calculateEMA(prevCloses, parseInt(rule.slowPeriod) || 26);
    } else {
      const prevMacd = calculateMACD(prevCloses);
      prevVal1 = prevMacd.macd;
      prevVal2 = prevMacd.signal;
    }

    if (condition.includes('above')) {
      triggered = prevVal1 <= prevVal2 && indicatorValue > indicatorValue2;
    } else if (condition.includes('below')) {
      triggered = prevVal1 >= prevVal2 && indicatorValue < indicatorValue2;
    } else {
      triggered = condition === 'above' ? indicatorValue > indicatorValue2 : indicatorValue < indicatorValue2;
    }
  } else if (indicator === 'BOLLINGER') {
    const bb = indicatorValue2;
    if (condition === 'below') triggered = indicatorValue < bb.lower;
    else if (condition === 'above') triggered = indicatorValue > bb.upper;
    else if (condition.includes('above')) triggered = indicatorValue > bb.upper;
    else if (condition.includes('below')) triggered = indicatorValue < bb.lower;
  } else {
    if (condition === 'below' || condition === '< (below)') triggered = indicatorValue < threshold;
    else if (condition === 'above' || condition === '> (above)') triggered = indicatorValue > threshold;
    else if (condition === 'crosses above') {
      const prev = indicator === 'RSI'
        ? calculateRSI(closes.slice(0, -1), 14)
        : closes[closes.length - 2];
      triggered = prev <= threshold && indicatorValue > threshold;
    } else if (condition === 'crosses below') {
      const prev = indicator === 'RSI'
        ? calculateRSI(closes.slice(0, -1), 14)
        : closes[closes.length - 2];
      triggered = prev >= threshold && indicatorValue < threshold;
    }
  }

  return {
    rule: rule.name,
    instr: rule.instr,
    indicator: rule.indicator,
    indicatorValue: typeof indicatorValue === 'number' ? indicatorValue.toFixed(4) : indicatorValue,
    indicatorValue2: indicatorValue2 ? (typeof indicatorValue2 === 'object' ? JSON.stringify(indicatorValue2) : Number(indicatorValue2).toFixed(4)) : undefined,
    threshold: rule.threshold,
    condition: rule.condition,
    triggered,
    action: rule.action,
    time: new Date().toISOString()
  };
}

// ─── IG PRICE FETCH ───────────────────────────────────────────────────────────

async function fetchPrices(epic, count, igBase, cst, xst) {
  try {
    const url = `${igBase}/prices/${epic}?resolution=DAY&max=${count}&pageSize=0`;
    const res = await fetch(url, {
      headers: {
        'X-IG-API-KEY': process.env.IG_API_KEY || '',
        'CST': cst,
        'X-SECURITY-TOKEN': xst,
        'Version': '3'
      }
    });
    if (!res.ok) {
      console.error('[Rules] Price fetch failed:', res.status);
      return null;
    }
    const data = await res.json();
    return (data.prices || []).map(p => ({
      open: p.openPrice && p.openPrice.bid || 0,
      high: p.highPrice && p.highPrice.bid || 0,
      low: p.lowPrice && p.lowPrice.bid || 0,
      close: p.closePrice && p.closePrice.bid || 0,
      time: p.snapshotTime
    })).filter(p => p.close > 0);
  } catch (err) {
    console.error('[Rules] fetchPrices error:', err.message);
    return null;
  }
}

// ─── ORDER PLACEMENT ──────────────────────────────────────────────────────────

async function placeOrder(rule, igBase, cst, xst) {
  const epic = EPIC_MAP[rule.instr];
  const direction = rule.action.toUpperCase() === 'BUY' ? 'BUY' : 'SELL';
  const size = parseInt(rule.size) || 1;

  try {
    const res = await fetch(`${igBase}/positions/otc`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-IG-API-KEY': process.env.IG_API_KEY || '',
        'CST': cst,
        'X-SECURITY-TOKEN': xst,
        'Version': '2'
      },
      body: JSON.stringify({
        epic,
        direction,
        size,
        orderType: rule.orderType || 'MARKET',
        expiry: '-',
        guaranteedStop: false,
        forceOpen: true,
        currencyCode: 'GBP',
        ...(rule.limitPrice ? { level: parseFloat(rule.limitPrice) } : {})
      })
    });
    const data = await res.json();
    if (data.dealReference) {
      return { success: true, dealReference: data.dealReference, direction, size };
    }
    return { success: false, error: data.errorCode || 'Unknown error' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

// ─── TECHNICAL INDICATORS ────────────────────────────────────────────────────

function calculateRSI(closes, period = 14) {
  if (closes.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gains += diff;
    else losses += Math.abs(diff);
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function calculateSMA(closes, period) {
  if (closes.length < period) return closes[closes.length - 1];
  const slice = closes.slice(-period);
  return slice.reduce((a, b) => a + b, 0) / slice.length;
}

function calculateEMA(closes, period) {
  if (closes.length < period) return closes[closes.length - 1];
  const k = 2 / (period + 1);
  let ema = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < closes.length; i++) {
    ema = closes[i] * k + ema * (1 - k);
  }
  return ema;
}

function calculateMACD(closes, fast = 12, slow = 26, signal = 9) {
  const emaFast = calculateEMA(closes, fast);
  const emaSlow = calculateEMA(closes, slow);
  const macdLine = emaFast - emaSlow;
  const macdHistory = [];
  for (let i = slow; i <= closes.length; i++) {
    const f = calculateEMA(closes.slice(0, i), fast);
    const s = calculateEMA(closes.slice(0, i), slow);
    macdHistory.push(f - s);
  }
  const signalLine = calculateEMA(macdHistory, signal);
  return { macd: macdLine, signal: signalLine, histogram: macdLine - signalLine };
}

function calculateBollinger(closes, period = 20, stdDev = 2) {
  const sma = calculateSMA(closes, period);
  const slice = closes.slice(-period);
  const variance = slice.reduce((sum, p) => sum + Math.pow(p - sma, 2), 0) / period;
  const std = Math.sqrt(variance);
  return { upper: sma + stdDev * std, middle: sma, lower: sma - stdDev * std };
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function getRules() {
  try {
    const raw = process.env.RULE_ENGINE;
    if (raw) {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : [];
    }
  } catch (e) {
    console.warn('[Rules] Could not parse RULE_ENGINE env var:', e.message);
  }
  return [];
}

async function sendNotification(type, subject, body) {
  try {
    const base = process.env.VERCEL_URL
      ? `https://${process.env.VERCEL_URL}`
      : 'http://localhost:3000';
    await fetch(`${base}/api/notify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, subject, body })
    });
  } catch (e) {
    console.error('[Rules] Notification error:', e.message);
  }
}

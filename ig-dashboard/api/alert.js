// ─── RSI ALERT TRIGGER ────────────────────────────────────────────────────────
// Lightweight endpoint — checks TD RSI for FX/commodity pairs every 2 mins
// Only calls full autotrade engine when RSI is near extreme (≤33 or ≥67)
// Consumes 1 TD credit per instrument per check (not full indicator set)
// Set cron: */2 8-16 * * 1-5 (every 2 mins during market hours)

const fetch = require('node-fetch');

const ALERT_INSTRUMENTS = [
  { instr: 'GBP/USD', symbol: 'GBP/USD', epic: 'CS.D.GBPUSD.MINI.IP' },
  { instr: 'EUR/USD', symbol: 'EUR/USD', epic: 'CS.D.EURUSD.MINI.IP' },
  { instr: 'USD/JPY', symbol: 'USD/JPY', epic: 'CS.D.USDJPY.MINI.IP' },
  { instr: 'EUR/GBP', symbol: 'EUR/GBP', epic: 'CS.D.EURGBP.MINI.IP' },
];

// Thresholds — slightly wider than engine's 32/68 to give early warning
const RSI_OVERSOLD  = 33;
const RSI_OVERBOUGHT = 67;

// Rate limit — don't trigger engine more than once per 15 mins per instrument
const TRIGGER_COOLDOWN_MS = 15 * 60 * 1000;
const lastTrigger = {}; // in-memory, resets per invocation (fine for rate limiting)

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Auth check
  const auth = req.headers['authorization'] || '';
  const cronSecret = process.env.CRON_SECRET || 'BBpass1-';
  if (!auth.includes(cronSecret) && req.method === 'POST') {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const TD_KEY = process.env.TWELVE_DATA_KEY;
  const BASE = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
  const log = [];
  const L = msg => { log.push(msg); console.log('[Alert]', msg); };

  if (!TD_KEY) return res.status(200).json({ action: 'no_td_key', log });

  try {
    // Fetch RSI for all alert instruments in one TD call (saves credits)
    const symbols = ALERT_INSTRUMENTS.map(i => i.symbol).join(',');
    const tdUrl = `https://api.twelvedata.com/rsi?symbol=${encodeURIComponent(symbols)}&interval=1h&time_period=14&apikey=${TD_KEY}&outputsize=1`;
    const tdRes = await fetch(tdUrl, { timeout: 8000 });
    if (!tdRes.ok) {
      L(`TD fetch failed: ${tdRes.status}`);
      return res.status(200).json({ action: 'td_error', log });
    }
    const tdData = await tdRes.json();

    // TD returns single object if one symbol, array-keyed object if multiple
    const alerts = [];
    for (const instr of ALERT_INSTRUMENTS) {
      try {
        // Handle both single and multi-symbol TD response formats
        const instrData = tdData[instr.symbol] || tdData;
        if (instrData.status === 'error') {
          L(`${instr.instr}: TD error — ${instrData.message}`);
          continue;
        }
        const rsiVal = parseFloat(instrData.values?.[0]?.rsi || instrData.value);
        if (isNaN(rsiVal)) { L(`${instr.instr}: no RSI data`); continue; }

        L(`${instr.instr}: RSI ${rsiVal.toFixed(1)}`);

        if (rsiVal <= RSI_OVERSOLD || rsiVal >= RSI_OVERBOUGHT) {
          const direction = rsiVal <= RSI_OVERSOLD ? 'oversold' : 'overbought';
          alerts.push({ instr: instr.instr, rsi: rsiVal, direction });
          L(`⚡ ${instr.instr} RSI ${rsiVal.toFixed(1)} — ${direction} alert`);
        }
      } catch(e) { L(`${instr.instr}: parse error — ${e.message}`); }
    }

    if (!alerts.length) {
      L('No RSI extremes detected');
      return res.status(200).json({ action: 'no_alerts', log });
    }

    // RSI extreme detected — trigger full engine
    L(`Triggering engine for ${alerts.length} alert(s)...`);
    try {
      const engineRes = await fetch(`${BASE}/api/autotrade`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${cronSecret}`
        },
        body: JSON.stringify({ manualRun: true, triggeredBy: 'rsi_alert', alerts }),
        timeout: 25000
      });
      const engineData = await engineRes.json();
      L(`Engine result: ${engineData.action}`);
      return res.status(200).json({
        action: 'engine_triggered',
        alerts,
        engineAction: engineData.action,
        log: [...log, ...(engineData.log || [])]
      });
    } catch(e) {
      L(`Engine trigger failed: ${e.message}`);
      return res.status(200).json({ action: 'engine_error', alerts, log });
    }

  } catch(e) {
    L(`Alert error: ${e.message}`);
    return res.status(200).json({ action: 'error', error: e.message, log });
  }
};

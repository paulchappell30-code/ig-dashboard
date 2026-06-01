// ─── RSI ALERT TRIGGER ────────────────────────────────────────────────────────
// Sole TD data source — fetches RSI for FX pairs, saves to DB cache
// Cron: 0 8,12,16 * * 1-5 (8am, 12pm, 4pm UTC weekdays — ~12 TD credits/day)
// 4-hour cache TTL means autotrade always has fresh-enough data within trading hours
// Autotrade reads DB cache only — never fetches TD directly

const fetch = require('node-fetch');

const ALERT_INSTRUMENTS = [
  { instr: 'GBP/USD', symbol: 'GBP/USD', epic: 'CS.D.GBPUSD.TODAY.IP' },
  { instr: 'EUR/USD', symbol: 'EUR/USD', epic: 'CS.D.EURUSD.TODAY.IP' },
  { instr: 'USD/JPY', symbol: 'USD/JPY', epic: 'CS.D.USDJPY.TODAY.IP' },
  { instr: 'EUR/GBP', symbol: 'EUR/GBP', epic: 'CS.D.EURGBP.TODAY.IP' },
];

const RSI_OVERSOLD   = 33;
const RSI_OVERBOUGHT = 67;
const CACHE_TTL_MS   = 60 * 60 * 1000; // 1 hour

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const auth = req.headers['authorization'] || '';
  const cronSecret = process.env.CRON_SECRET || 'Bambip49';
  if (req.method === 'POST' && !auth.includes(cronSecret)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const TD_KEY  = process.env.TWELVE_DATA_KEY;
  const BASE    = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
  const log     = [];
  const L       = msg => { log.push(msg); console.log('[Alert]', msg); };

  if (!TD_KEY) return res.status(200).json({ action: 'no_td_key', log });

  // ── Check if cache is still fresh — skip fetch if <10 mins old ──────────────
  try {
    const { sql } = require('@vercel/postgres');
    const cacheRow = await sql`
      SELECT created_at FROM engine_events
      WHERE event_type = 'td_cache'
      ORDER BY created_at DESC LIMIT 1
    `;
    if (cacheRow.rows.length > 0) {
      const age = Date.now() - new Date(cacheRow.rows[0].created_at).getTime();
      if (age < 3 * 60 * 60 * 1000) { // 3 hours — cron runs every 4-5 hours so this always allows fetch
        L(`Cache fresh (${Math.round(age/60000)}m old) — skipping TD fetch`);
        return res.status(200).json({ action: 'cache_fresh', ageMinutes: Math.round(age/60000), log });
      }
    }
  } catch(e) { L('Cache check error: ' + e.message); }

  // ── Fetch RSI + MACD for each instrument with delays ─────────────────────────
  const alerts   = [];
  const tdCache  = {};

  for (const instr of ALERT_INSTRUMENTS) {
    try {
      await new Promise(r => setTimeout(r, 2000)); // 2s between calls = max 2/min

      // Fetch RSI
      const rsiRes = await fetch(
        `https://api.twelvedata.com/rsi?symbol=${encodeURIComponent(instr.symbol)}&interval=1h&time_period=14&apikey=${TD_KEY}&outputsize=1`,
        { timeout: 8000 }
      );
      if (!rsiRes.ok) { L(`${instr.instr}: RSI fetch ${rsiRes.status}`); continue; }
      const rsiData = await rsiRes.json();
      if (rsiData.status === 'error') {
        L(`${instr.instr}: ${rsiData.message}`);
        // If daily limit hit, stop trying other instruments
        if (rsiData.message && rsiData.message.includes('run out of API credits for the day')) {
          L('Daily TD limit hit — stopping all fetches until midnight UTC');
          break;
        }
        continue;
      }
      const rsi = parseFloat(rsiData.values?.[0]?.rsi);
      if (isNaN(rsi)) { L(`${instr.instr}: no RSI value`); continue; }

      // MACD removed — saves 4 TD credits per run, calculated locally from DB candles
      tdCache[instr.instr] = { rsi, macd: null, signal: null };
      L(`${instr.instr}: RSI ${rsi.toFixed(1)}`);

      if (rsi <= RSI_OVERSOLD || rsi >= RSI_OVERBOUGHT) {
        const direction = rsi <= RSI_OVERSOLD ? 'oversold' : 'overbought';
        alerts.push({ instr: instr.instr, rsi, direction });
        L(`⚡ ${instr.instr} RSI ${rsi.toFixed(1)} — ${direction} ALERT`);
      }

    } catch(e) { L(`${instr.instr}: error — ${e.message}`); }
  }

  // ── Save to DB cache ──────────────────────────────────────────────────────────
  if (Object.keys(tdCache).length > 0) {
    try {
      const { sql } = require('@vercel/postgres');
      await sql`INSERT INTO engine_events (event_type, details, created_at)
                VALUES ('td_cache', ${tdCache}, NOW())`;
      await sql`DELETE FROM engine_events
                WHERE event_type = 'td_cache'
                AND created_at < NOW() - INTERVAL '2 hours'`;
      L(`TD cache saved: ${Object.keys(tdCache).length} instruments`);
    } catch(e) { 
      L('Cache save error: ' + e.message + ' — ' + e.stack?.split('\n')[0]);
      console.error('[Alert] Cache save failed:', e);
    }
  }

  // ── Trigger engine if RSI extreme found ───────────────────────────────────────
  if (alerts.length > 0) {
    L(`Triggering engine for ${alerts.length} alert(s)...`);
    try {
      const engineRes = await fetch(`${BASE}/api/autotrade`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${cronSecret}` },
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
      L(`Engine trigger error: ${e.message}`);
      return res.status(200).json({ action: 'engine_error', alerts, log });
    }
  }

  L('No RSI extremes — cache updated, no engine trigger');
  return res.status(200).json({ action: 'no_alerts', tdCache, log });
};

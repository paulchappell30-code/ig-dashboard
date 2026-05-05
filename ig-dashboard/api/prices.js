// Price History Collector v3
// Runs every 5 minutes via cron to build local price history
// This eliminates dependence on IG's historical data allowance
const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

const INSTRUMENTS = {
  'FTSE 100':  'IX.D.FTSE.DAILY.IP',
  'S&P 500':   'IX.D.SPTRD.DAILY.IP',
  'DAX 40':    'IX.D.DAX.DAILY.IP',
  'Dow Jones': 'IX.D.DOW.DAILY.IP',
  'Brent Oil': 'CC.D.LCO.USS.IP',
  'GBP/USD':   'CS.D.GBPUSD.MINI.IP',
  'EUR/USD':   'CS.D.EURUSD.MINI.IP',
  'USD/JPY':   'CS.D.USDJPY.MINI.IP',
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Auth check
  const cronSecret = process.env.CRON_SECRET || '';
  if (cronSecret) {
    const token = (req.headers['authorization'] || '').replace('Bearer ', '').trim();
    const ref = req.headers['referer'] || req.headers['origin'] || '';
    const fromDash = ref.includes('vercel.app') || ref.includes('localhost');
    if (!fromDash && token !== cronSecret) return res.status(401).json({ error: 'Unauthorised' });
  }

  if (req.method === 'GET') {
    // Return stored price history for an instrument
    const epic = req.query.epic;
    const resolution = req.query.resolution || 'DAY';
    const limit = parseInt(req.query.limit) || 60;
    if (!epic) return res.status(400).json({ error: 'epic required' });

    try {
      const { sql } = require('@vercel/postgres');
      const result = await sql`
        SELECT * FROM price_history
        WHERE epic = ${epic} AND resolution = ${resolution}
        ORDER BY candle_time DESC
        LIMIT ${limit}
      `;
      return res.status(200).json({
        epic, resolution,
        candles: result.rows.reverse(), // Return chronological order
        count: result.rows.length
      });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // POST — collect latest prices from IG and store
  const igBase = IG_BASES[process.env.IG_ENV || 'demo'];
  const log = [];
  const addLog = msg => { console.log('[PriceCollector]', msg); log.push(msg); };

  addLog('Price collection started — ' + new Date().toISOString());

  // Authenticate with IG
  let cst, xst;
  try {
    const authRes = await fetch(`${igBase}/session`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-IG-API-KEY': process.env.IG_API_KEY || '', 'Version': '2' },
      body: JSON.stringify({ identifier: process.env.IG_USERNAME, password: process.env.IG_PASSWORD })
    });
    if (!authRes.ok) return res.status(500).json({ error: 'IG auth failed', log });
    cst = authRes.headers.get('CST');
    xst = authRes.headers.get('X-SECURITY-TOKEN');
  } catch(e) {
    return res.status(500).json({ error: 'Auth error: ' + e.message, log });
  }

  const igHeaders = {
    'Content-Type': 'application/json',
    'X-IG-API-KEY': process.env.IG_API_KEY || '',
    'CST': cst, 'X-SECURITY-TOKEN': xst,
  };

  // Initialise DB tables if needed
  try {
    const { sql } = require('@vercel/postgres');
    await sql`
      CREATE TABLE IF NOT EXISTS price_history (
        id SERIAL PRIMARY KEY,
        epic VARCHAR(100) NOT NULL,
        instrument VARCHAR(50),
        resolution VARCHAR(20) NOT NULL,
        candle_time TIMESTAMPTZ NOT NULL,
        open_price DECIMAL(15,4),
        high_price DECIMAL(15,4),
        low_price DECIMAL(15,4),
        close_price DECIMAL(15,4),
        volume INTEGER,
        collected_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(epic, resolution, candle_time)
      )
    `;
    await sql`CREATE INDEX IF NOT EXISTS idx_price_history_epic_res ON price_history(epic, resolution, candle_time DESC)`;
  } catch(e) {
    addLog('DB init warning: ' + e.message);
  }

  // Fetch and store prices for each instrument
  const results = [];
  let totalStored = 0;

  for (const [name, epic] of Object.entries(INSTRUMENTS)) {
    try {
      // Fetch last 10 daily candles (conservative to preserve allowance)
      const priceRes = await fetch(`${igBase}/prices/${epic}?resolution=DAY&max=10&pageSize=0`, {
        headers: { ...igHeaders, 'Version': '3' }
      });

      if (!priceRes.ok) {
        addLog(`${name}: price fetch failed (${priceRes.status})`);
        results.push({ name, status: 'failed', statusCode: priceRes.status });
        continue;
      }

      const priceData = await priceRes.json();
      const candles = priceData.prices || [];

      if (!candles.length) {
        addLog(`${name}: no candles returned`);
        continue;
      }

      // Store each candle in DB
      const { sql } = require('@vercel/postgres');
      let stored = 0;

      for (const candle of candles) {
        try {
          const closePrice = candle.closePrice?.bid || candle.closePrice?.mid || 0;
          if (!closePrice) continue;

          // Parse IG date format: "2026/05/05 00:00:00"
          const rawTime = candle.snapshotTime;
          const parsedTime = rawTime ? rawTime.replace(/(\d+)\/(\d+)\/(\d+)/, '$3-$2-$1').replace(' ', 'T') : null;
          if (!parsedTime) continue;

          await sql`
            INSERT INTO price_history (epic, instrument, resolution, candle_time, open_price, high_price, low_price, close_price)
            VALUES (
              ${epic}, ${name}, 'DAY', ${parsedTime},
              ${candle.openPrice?.bid || 0},
              ${candle.highPrice?.bid || 0},
              ${candle.lowPrice?.bid || 0},
              ${closePrice}
            )
            ON CONFLICT (epic, resolution, candle_time) DO UPDATE SET
              close_price = EXCLUDED.close_price,
              high_price = EXCLUDED.high_price,
              low_price = EXCLUDED.low_price
          `;
          stored++;
        } catch(e) {
          if (!e.message.includes('unique')) {
            addLog(`${name}: candle insert error — ${e.message}`);
          }
        }
      }

      totalStored += stored;
      addLog(`${name}: ${stored} candles stored (${candles.length} fetched)`);
      results.push({ name, epic, candles: candles.length, stored, status: 'ok' });

      // Also store current snapshot price (doesn't count against allowance)
      try {
        const snapRes = await fetch(`${igBase}/markets/${epic}`, {
          headers: { ...igHeaders, 'Version': '3' }
        });
        if (snapRes.ok) {
          const snapData = await snapRes.json();
          const bid = snapData.snapshot?.bid;
          if (bid) {
            const now = new Date().toISOString();
            await sql`
              INSERT INTO price_history (epic, instrument, resolution, candle_time, open_price, high_price, low_price, close_price)
              VALUES (${epic}, ${name}, 'SNAPSHOT', ${now}, ${bid}, ${bid}, ${bid}, ${bid})
              ON CONFLICT (epic, resolution, candle_time) DO NOTHING
            `;
          }
        }
      } catch(e) { /* Snapshot storage is best-effort */ }

      // Small delay to avoid rate limiting
      await new Promise(r => setTimeout(r, 300));

    } catch(e) {
      addLog(`${name}: error — ${e.message}`);
      results.push({ name, status: 'error', error: e.message });
    }
  }

  addLog(`Collection complete. Total candles stored: ${totalStored}`);

  return res.status(200).json({
    success: true,
    totalStored,
    results,
    log,
    time: new Date().toISOString()
  });
};

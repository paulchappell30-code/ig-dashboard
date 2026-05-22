// api/backfill.js — One-time historical data backfill from Yahoo Finance
// Fetches 2 years of daily closes and inserts into price_history table
// Call via: POST /api/backfill with Authorization: Bearer <CRON_SECRET>

const { sql } = require('@vercel/postgres');

const INSTRUMENTS = [
  { name: 'FTSE 100',  ticker: '%5EFTSE',    scale: 1 },
  { name: 'DAX 40',    ticker: '%5EGDAXI',   scale: 1 },
  { name: 'S&P 500',   ticker: '%5EGSPC',    scale: 1 },
  { name: 'Dow Jones', ticker: '%5EDJI',      scale: 1 },
  { name: 'CAC 40',    ticker: '%5EFCHI',     scale: 1 },
  { name: 'Nikkei 225',ticker: '%5EN225',     scale: 1 },
  { name: 'Nasdaq',    ticker: '%5EIXIC',     scale: 1 },
  { name: 'Brent Oil', ticker: 'BZ%3DF',      scale: 1 },
  { name: 'Gold',      ticker: 'GC%3DF',      scale: 1 },
  { name: 'Silver',    ticker: 'SI%3DF',      scale: 1 },
  { name: 'Copper',    ticker: 'HG%3DF',      scale: 100 }, // Yahoo gives $/lb, scale to cents
  { name: 'GBP/USD',   ticker: 'GBPUSD%3DX',  scale: 10000 }, // Store as pips
  { name: 'EUR/USD',   ticker: 'EURUSD%3DX',  scale: 10000 },
  { name: 'USD/JPY',   ticker: 'USDJPY%3DX',  scale: 100 },
  { name: 'EUR/GBP',   ticker: 'EURGBP%3DX',  scale: 10000 },
];

const EPIC_MAP = {
  'FTSE 100':   'IX.D.FTSE.DAILY.IP',
  'DAX 40':     'IX.D.DAX.DAILY.IP',
  'S&P 500':    'IX.D.SPTRD.DAILY.IP',
  'Dow Jones':  'IX.D.DOW.DAILY.IP',
  'CAC 40':     'IX.D.CAC.DAILY.IP',
  'Nikkei 225': 'IX.D.NIKKEI.DAILY.IP',
  'Nasdaq':     'IX.D.NASDAQ.CASH.IP',
  'Brent Oil':  'CC.D.LCO.USS.IP',
  'Gold':       'CS.D.USCGC.TODAY.IP',
  'Silver':     'CS.D.USCSI.TODAY.IP',
  'Copper':     'CS.D.COPPER.TODAY.IP',
  'GBP/USD':    'CS.D.GBPUSD.TODAY.IP',
  'EUR/USD':    'CS.D.EURUSD.TODAY.IP',
  'USD/JPY':    'CS.D.USDJPY.TODAY.IP',
  'EUR/GBP':    'CS.D.EURGBP.TODAY.IP',
};

async function fetchYahoo(ticker, range = '2y') {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d&range=${range}&includeAdjustedClose=true`;
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0',
      'Accept': 'application/json',
    }
  });
  if (!res.ok) throw new Error(`Yahoo ${ticker}: HTTP ${res.status}`);
  const data = await res.json();
  const chart = data.chart?.result?.[0];
  if (!chart) throw new Error(`Yahoo ${ticker}: no data`);
  const timestamps = chart.timestamp || [];
  const closes = chart.indicators?.quote?.[0]?.close || [];
  const opens = chart.indicators?.quote?.[0]?.open || [];
  const highs = chart.indicators?.quote?.[0]?.high || [];
  const lows = chart.indicators?.quote?.[0]?.low || [];
  return timestamps.map((ts, i) => ({
    date: new Date(ts * 1000).toISOString().split('T')[0],
    open: opens[i],
    high: highs[i],
    low: lows[i],
    close: closes[i],
  })).filter(c => c.close !== null && c.close !== undefined && !isNaN(c.close));
}

module.exports = async (req, res) => {
  // Auth check
  const secret = process.env.CRON_SECRET || '';
  const token = (req.headers['authorization'] || '').replace('Bearer ', '').trim();
  if (token !== secret) return res.status(401).json({ error: 'Unauthorised' });
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });

  const log = [];
  const L = msg => { console.log('[Backfill]', msg); log.push(msg); };
  const specificInstrument = req.body?.instrument; // Optional: backfill single instrument
  const range = req.body?.range || '2y'; // Default 2 years

  L(`Starting backfill — range: ${range}${specificInstrument ? ' instrument: ' + specificInstrument : ' (all)'}`);

  let totalInserted = 0;
  let totalSkipped = 0;
  const results = {};

  const instruments = specificInstrument
    ? INSTRUMENTS.filter(i => i.name === specificInstrument)
    : INSTRUMENTS;

  for (const instr of instruments) {
    try {
      L(`Fetching ${instr.name} (${instr.ticker})...`);
      const candles = await fetchYahoo(instr.ticker, range);
      L(`${instr.name}: ${candles.length} candles from Yahoo`);

      // Check existing candles to avoid duplicates
      const existing = await sql`
        SELECT candle_time::date as dt FROM price_history
        WHERE instrument = ${instr.name} AND resolution = 'DAY'
      `;
      const existingDates = new Set(existing.rows.map(r =>
        new Date(r.dt).toISOString().split('T')[0]
      ));

      let inserted = 0;
      let skipped = 0;
      for (const c of candles) {
        if (existingDates.has(c.date)) { skipped++; continue; }
        const epic = EPIC_MAP[instr.name] || '';
        // Scale prices to match IG contract units
        const scale = instr.scale || 1;
        const closePrice = c.close * scale;
        const openPrice = c.open * scale;
        const highPrice = c.high * scale;
        const lowPrice = c.low * scale;
        await sql`
          INSERT INTO price_history (epic, instrument, resolution, candle_time, open_price, high_price, low_price, close_price)
          VALUES (${epic}, ${instr.name}, 'DAY', ${c.date}::timestamptz, ${openPrice}, ${highPrice}, ${lowPrice}, ${closePrice})
          ON CONFLICT DO NOTHING
        `;
        inserted++;
      }

      L(`${instr.name}: inserted ${inserted}, skipped ${skipped} existing`);
      totalInserted += inserted;
      totalSkipped += skipped;
      results[instr.name] = { inserted, skipped, total: candles.length };

      // Small delay between requests to be respectful
      await new Promise(r => setTimeout(r, 500));

    } catch (e) {
      L(`${instr.name}: ERROR — ${e.message}`);
      results[instr.name] = { error: e.message };
    }
  }

  L(`Backfill complete — ${totalInserted} inserted, ${totalSkipped} skipped`);
  return res.status(200).json({
    success: true, totalInserted, totalSkipped, results, log
  });
};

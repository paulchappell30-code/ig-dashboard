// Database API v4
// GET  /api/db?action=init         — initialise/migrate tables
// GET  /api/db?action=trades       — get trade history
// GET  /api/db?action=equity       — get equity curve
// GET  /api/db?action=stats        — performance stats
// GET  /api/db?action=calibration  — AI confidence calibration report
// GET  /api/db?action=timeofday    — time-of-day performance analysis
// GET  /api/db?action=optimize     — get current optimised parameters
// POST /api/db                     — save trade, equity snapshot, event, outcome
const { sql } = require('@vercel/postgres');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  if (!process.env.POSTGRES_URL) {
    return res.status(200).json({ configured: false, message: 'Vercel Postgres not configured' });
  }

  try {
    if (req.method === 'GET') {
      const action = req.query.action || 'trades';

      if (action === 'init') {
        await initTables();
        return res.status(200).json({ success: true, message: 'Tables initialised/migrated' });
      }

      if (action === 'trades') {
        const limit = parseInt(req.query.limit) || 100;
        const result = await sql`SELECT * FROM trades ORDER BY opened_at DESC LIMIT ${limit}`;
        return res.status(200).json({ trades: result.rows });
      }

      if (action === 'equity') {
        const days = parseInt(req.query.days) || 30;
        const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
        const result = await sql`SELECT * FROM equity_snapshots WHERE snapshot_time > ${cutoff} ORDER BY snapshot_time ASC`;
        return res.status(200).json({ snapshots: result.rows });
      }

      if (action === 'candles') {
        // Return historical candles for an instrument (for pairs analysis)
        const instrument = req.query.instrument;
        const limit = parseInt(req.query.limit || '600');
        const resolution = req.query.resolution || 'DAY';
        if(!instrument) return res.status(400).json({ error: 'instrument required' });
        try {
          const rows = await sql`
            SELECT candle_time, close_price, open_price, high_price, low_price, instrument, epic
            FROM price_history
            WHERE (instrument = ${instrument} OR epic ILIKE ${instrument} OR instrument ILIKE ${'%' + instrument + '%'})
            AND resolution = ${resolution}
            AND close_price > 0.0001
            ORDER BY candle_time DESC
            LIMIT ${limit}
          `;
          return res.status(200).json({ candles: rows.rows.reverse() });
        } catch(e) { return res.status(200).json({ error: e.message, candles: [] }); }
      }

      if (action === 'tdcache') {
        try {
          const row = await sql`SELECT details, created_at FROM engine_events WHERE event_type = 'td_cache' ORDER BY created_at DESC LIMIT 1`;
          if (row.rows.length === 0) return res.status(200).json({ error: 'No cache', instruments: {} });
          const age = Math.round((Date.now() - new Date(row.rows[0].created_at).getTime()) / 60000);
          return res.status(200).json({ instruments: row.rows[0].details || {}, ageMinutes: age });
        } catch(e) { return res.status(200).json({ error: e.message, instruments: {} }); }
      }

      if (action === 'import_trade') {
        // Manual trade import for missed trades
        const t = req.body.trade;
        if (!t) return res.status(400).json({ error: 'No trade data' });
        try {
          await sql`
            INSERT INTO trades (
              deal_id, instrument, epic, direction, size,
              open_level, close_level, opened_at, closed_at,
              profit_loss, signal_score, ai_confidence, status,
              regime, trade_type, close_reason, ai_was_correct
            ) VALUES (
              ${t.dealId}, ${t.instrument}, ${t.epic||null}, ${t.direction}, ${t.size},
              ${t.openLevel}, ${t.closeLevel||null}, ${t.openedAt}::timestamptz, 
              ${t.closedAt||null}::timestamptz,
              ${t.profitLoss||null}, ${t.signalScore||null}, ${t.aiConfidence||null},
              ${t.status||'closed'}, ${t.regime||'ranging'}, 
              ${t.tradeType||'hourly_mr'}, ${t.closeReason||'manual'},
              ${t.profitLoss > 0 ? true : false}
            )
            ON CONFLICT (deal_id) DO UPDATE SET
              close_level = EXCLUDED.close_level,
              closed_at = EXCLUDED.closed_at,
              profit_loss = EXCLUDED.profit_loss,
              status = EXCLUDED.status,
              close_reason = EXCLUDED.close_reason,
              ai_was_correct = EXCLUDED.ai_was_correct
          `;
          return res.status(200).json({ success: true, instrument: t.instrument });
        } catch(e) { return res.status(200).json({ error: e.message }); }
      }

      if (action === 'stats') {
        const stats = await getStats();
        return res.status(200).json(stats);
      }

      if (action === 'sentiment_history') {
      const instrument = req.query.instrument || '';
      const days = parseInt(req.query.days) || 30;
      const result = await sql`
        SELECT instrument, long_pct, short_pct, recorded_at
        FROM sentiment_history
        WHERE instrument = ${instrument}
        AND recorded_at > NOW() - INTERVAL '${days} days'
        ORDER BY recorded_at ASC
      `;
      // Detect significant shifts (>10% change in long_pct over 7 days)
      const rows = result.rows;
      let shift = null;
      if (rows.length >= 2) {
        const first = parseFloat(rows[0].long_pct);
        const last = parseFloat(rows[rows.length-1].long_pct);
        const change = last - first;
        if (Math.abs(change) >= 10) {
          shift = { direction: change > 0 ? 'increasingly_long' : 'increasingly_short', change: change.toFixed(1) };
        }
      }
      return res.status(200).json({ history: rows, shift, instrument });
    }

    if (action === 'calibration') {
        const cal = await getCalibration();
        return res.status(200).json(cal);
      }

      if (action === 'timeofday') {
        const tod = await getTimeOfDay();
        return res.status(200).json(tod);
      }

      if (action === 'optimize') {
        const params = await getOptimizedParams();
        return res.status(200).json(params);
      }
    }

    if (req.method === 'POST') {
      const { type, data } = req.body || {};

      if (type === 'backfill') {
        // One-time historical backfill from Yahoo Finance
        const range = data?.range || '2y';
        const specificInstrument = data?.instrument || null;
        const log = [];
        const L = msg => { console.log('[Backfill]', msg); log.push(msg); };

        const YAHOO_INSTRUMENTS = [
          { name:'FTSE 100',  ticker:'%5EFTSE',     scale:1 },
          { name:'DAX 40',    ticker:'%5EGDAXI',    scale:1 },
          { name:'S&P 500',   ticker:'%5EGSPC',     scale:1 },
          { name:'Dow Jones', ticker:'%5EDJI',       scale:1 },
          { name:'CAC 40',    ticker:'%5EFCHI',      scale:1 },
          { name:'Nikkei 225',ticker:'%5EN225',      scale:1 },
          { name:'Nasdaq',    ticker:'%5EIXIC',      scale:1 },
          { name:'Brent Oil', ticker:'BZ%3DF',       scale:1 },
          { name:'Gold',      ticker:'GC%3DF',       scale:1 },
          { name:'Silver',    ticker:'SI%3DF',       scale:1 },
          { name:'Copper',    ticker:'HG%3DF',       scale:100 },
          { name:'GBP/USD',   ticker:'GBPUSD%3DX',   scale:10000 },
          { name:'EUR/USD',   ticker:'EURUSD%3DX',   scale:10000 },
          { name:'USD/JPY',   ticker:'USDJPY%3DX',   scale:100 },
          { name:'EUR/GBP',   ticker:'EURGBP%3DX',   scale:10000 },
        ];
        const EPIC_MAP_BF = {
          'FTSE 100':'IX.D.FTSE.DAILY.IP','DAX 40':'IX.D.DAX.DAILY.IP',
          'S&P 500':'IX.D.SPTRD.DAILY.IP','Dow Jones':'IX.D.DOW.DAILY.IP',
          'CAC 40':'IX.D.CAC.DAILY.IP','Nikkei 225':'IX.D.NIKKEI.DAILY.IP',
          'Nasdaq':'IX.D.NASDAQ.CASH.IP','Brent Oil':'CC.D.LCO.USS.IP',
          'Gold':'CS.D.USCGC.TODAY.IP','Silver':'CS.D.USCSI.TODAY.IP',
          'Copper':'CS.D.COPPER.TODAY.IP','GBP/USD':'CS.D.GBPUSD.TODAY.IP',
          'EUR/USD':'CS.D.EURUSD.TODAY.IP','USD/JPY':'CS.D.USDJPY.TODAY.IP',
          'EUR/GBP':'CS.D.EURGBP.TODAY.IP',
        };

        const instruments = specificInstrument
          ? YAHOO_INSTRUMENTS.filter(i => i.name === specificInstrument)
          : YAHOO_INSTRUMENTS;

        let totalInserted = 0; let totalSkipped = 0; const results = {};

        for (const instr of instruments) {
          try {
            const url = `https://query1.finance.yahoo.com/v8/finance/chart/${instr.ticker}?interval=1d&range=${range}`;
            const yr = await fetch(url, { headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'} });
            if (!yr.ok) throw new Error(`HTTP ${yr.status}`);
            const yd = await yr.json();
            const chart = yd.chart?.result?.[0];
            if (!chart) throw new Error('no chart data');
            const timestamps = chart.timestamp || [];
            const closes = chart.indicators?.quote?.[0]?.close || [];
            const opens = chart.indicators?.quote?.[0]?.open || [];
            const highs = chart.indicators?.quote?.[0]?.high || [];
            const lows = chart.indicators?.quote?.[0]?.low || [];

            const candles = timestamps.map((ts, i) => ({
              date: new Date(ts * 1000).toISOString().split('T')[0],
              open: opens[i], high: highs[i], low: lows[i], close: closes[i]
            })).filter(c => c.close != null && !isNaN(c.close));

            const existing = await sql`SELECT candle_time::date as dt FROM price_history WHERE instrument=${instr.name} AND resolution='DAY'`;
            const existingDates = new Set(existing.rows.map(r => new Date(r.dt).toISOString().split('T')[0]));

            let inserted = 0; let skipped = 0;
            for (const c of candles) {
              if (existingDates.has(c.date)) { skipped++; continue; }
              const scale = instr.scale || 1;
              await sql`INSERT INTO price_history (epic,instrument,resolution,candle_time,open_price,high_price,low_price,close_price)
                VALUES (${EPIC_MAP_BF[instr.name]||''},${instr.name},'DAY',${c.date}::timestamptz,
                ${c.open*scale},${c.high*scale},${c.low*scale},${c.close*scale})
                ON CONFLICT DO NOTHING`;
              inserted++;
            }
            L(`${instr.name}: ${inserted} inserted, ${skipped} skipped`);
            totalInserted += inserted; totalSkipped += skipped;
            results[instr.name] = { inserted, skipped };
            await new Promise(r => setTimeout(r, 300));
          } catch(e) {
            L(`${instr.name}: ERROR ${e.message}`);
            results[instr.name] = { error: e.message };
          }
        }
        L(`Done — ${totalInserted} inserted, ${totalSkipped} skipped`);
        return res.status(200).json({ success:true, totalInserted, totalSkipped, results, log });
      }

      if (type === 'backfill_volume') {
        // Backfill volume for existing daily candles from Yahoo Finance
        const specificInstrument = data?.instrument || null;
        const log = []; const L = msg => { console.log('[VolumeBackfill]', msg); log.push(msg); };

        const YAHOO_VOL = [
          { name:'FTSE 100',  ticker:'%5EFTSE' },
          { name:'S&P 500',   ticker:'%5EGSPC' },
          { name:'DAX 40',    ticker:'%5EGDAXI' },
          { name:'Dow Jones', ticker:'%5EDJI' },
          { name:'Nasdaq',    ticker:'%5EIXIC' },
          { name:'Gold',      ticker:'GC%3DF' },
          { name:'Silver',    ticker:'SI%3DF' },
          { name:'Brent Oil', ticker:'BZ%3DF' },
          { name:'GBP/USD',   ticker:'GBPUSD%3DX' },
          { name:'EUR/USD',   ticker:'EURUSD%3DX' },
          { name:'EUR/GBP',   ticker:'EURGBP%3DX' },
          { name:'USD/JPY',   ticker:'USDJPY%3DX' },
          { name:'Copper',    ticker:'HG%3DF' },
        ];

        const instruments = specificInstrument
          ? YAHOO_VOL.filter(i => i.name === specificInstrument)
          : YAHOO_VOL;

        let totalUpdated = 0;
        const results = {};

        for(const instr of instruments) {
          try {
            // Yahoo max range for daily = 2y
            const url = `https://query1.finance.yahoo.com/v8/finance/chart/${instr.ticker}?interval=1d&range=2y`;
            const yr = await fetch(url, { headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'} });
            if(!yr.ok) throw new Error(`HTTP ${yr.status}`);
            const yd = await yr.json();
            const chart = yd.chart?.result?.[0];
            if(!chart) throw new Error('no chart data');

            const timestamps = chart.timestamp || [];
            const volumes = chart.indicators?.quote?.[0]?.volume || [];

            // Build date→volume map
            const volMap = {};
            timestamps.forEach((ts, i) => {
              const dt = new Date(ts*1000).toISOString().substring(0,10);
              if(volumes[i] && volumes[i] > 0) volMap[dt] = volumes[i];
            });

            L(`${instr.name}: ${Object.keys(volMap).length} days of volume from Yahoo`);

            // Update in batches — find candles missing volume
            const missing = await sql`
              SELECT id, candle_time::date as dt FROM price_history
              WHERE instrument=${instr.name} AND resolution='DAY'
              AND (volume IS NULL OR volume = 0)
              ORDER BY candle_time ASC`;

            let updated = 0;
            for(const row of missing.rows) {
              const dt = new Date(row.dt).toISOString().substring(0,10);
              const vol = volMap[dt];
              if(vol) {
                await sql`UPDATE price_history SET volume=${String(vol)} WHERE id=${row.id}`;
                updated++;
              }
            }

            L(`${instr.name}: ${updated}/${missing.rows.length} candles updated with volume`);
            totalUpdated += updated;
            results[instr.name] = { updated, missing: missing.rows.length, total: Object.keys(volMap).length };
            await new Promise(r => setTimeout(r, 400));
          } catch(e) {
            L(`${instr.name}: ERROR ${e.message}`);
            results[instr.name] = { error: e.message };
          }
        }

        L(`Volume backfill done — ${totalUpdated} candles updated`);
        return res.status(200).json({ success:true, totalUpdated, results, log });
      }

      if (type === 'backfill_minute') {
        // Backfill 7 days of 1-minute candles from Yahoo Finance (max available)
        const specificInstrument = data?.instrument || null;
        const log = []; const L = msg => { console.log('[MinuteBackfill]', msg); log.push(msg); };

        const YAHOO_MINUTE = [
          { name:'FTSE 100',  ticker:'%5EFTSE',     scale:1 },
          { name:'DAX 40',    ticker:'%5EGDAXI',    scale:1 },
          { name:'S&P 500',   ticker:'%5EGSPC',     scale:1 },
          { name:'Nasdaq',    ticker:'%5EIXIC',     scale:1 },
          { name:'Gold',      ticker:'GC%3DF',      scale:1 },
          { name:'Brent Oil', ticker:'BZ%3DF',      scale:1 },
          { name:'GBP/USD',   ticker:'GBPUSD%3DX',  scale:10000 },
          { name:'EUR/USD',   ticker:'EURUSD%3DX',  scale:10000 },
          { name:'EUR/GBP',   ticker:'EURGBP%3DX',  scale:10000 },
          { name:'USD/JPY',   ticker:'USDJPY%3DX',  scale:100 },
        ];
        const EPIC_MAP_M = {
          'FTSE 100':'IX.D.FTSE.DAILY.IP','DAX 40':'IX.D.DAX.DAILY.IP',
          'S&P 500':'IX.D.SPTRD.DAILY.IP','Nasdaq':'IX.D.NASDAQ.CASH.IP',
          'Gold':'CS.D.USCGC.TODAY.IP','Brent Oil':'CC.D.LCO.USS.IP',
          'GBP/USD':'CS.D.GBPUSD.TODAY.IP','EUR/USD':'CS.D.EURUSD.TODAY.IP',
          'EUR/GBP':'CS.D.EURGBP.TODAY.IP','USD/JPY':'CS.D.USDJPY.TODAY.IP',
        };

        const instruments = specificInstrument
          ? YAHOO_MINUTE.filter(i => i.name === specificInstrument)
          : YAHOO_MINUTE;

        let totalInserted = 0; let totalSkipped = 0; const results = {};

        for (const instr of instruments) {
          try {
            const url = `https://query1.finance.yahoo.com/v8/finance/chart/${instr.ticker}?interval=1m&range=7d`;
            const yr = await fetch(url, { headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'} });
            if (!yr.ok) throw new Error(`HTTP ${yr.status}`);
            const yd = await yr.json();
            const chart = yd.chart?.result?.[0];
            if (!chart) throw new Error('no chart data');

            const timestamps = chart.timestamp || [];
            const closes = chart.indicators?.quote?.[0]?.close || [];
            const opens = chart.indicators?.quote?.[0]?.open || [];
            const highs = chart.indicators?.quote?.[0]?.high || [];
            const lows = chart.indicators?.quote?.[0]?.low || [];

            const volumes = chart.indicators?.quote?.[0]?.volume || [];
            const candles = timestamps.map((ts, i) => ({
              time: new Date(ts * 1000).toISOString(),
              open: opens[i], high: highs[i], low: lows[i], close: closes[i],
              volume: volumes[i] || 0
            })).filter(c => c.close != null && !isNaN(c.close) && c.close > 0);

            const existing = await sql`
              SELECT candle_time FROM price_history
              WHERE instrument=${instr.name} AND resolution='MINUTE'`;
            const existingTimes = new Set(existing.rows.map(r =>
              new Date(r.candle_time).toISOString().substring(0,16)
            ));

            let inserted = 0; let skipped = 0;
            const scale = instr.scale || 1;
            const epic = EPIC_MAP_M[instr.name] || '';

            const newCandles = candles.filter(c => {
              const timeKey = c.time.substring(0,16);
              return !existingTimes.has(timeKey);
            });
            skipped = candles.length - newCandles.length;

            const BATCH = 100;
            for(let b = 0; b < newCandles.length; b += BATCH) {
              const chunk = newCandles.slice(b, b + BATCH);
              const vals = chunk.map(c =>
                `('${epic}','${instr.name}','MINUTE','${c.time}',${c.open*scale},${c.high*scale},${c.low*scale},${c.close*scale},${Math.round(c.volume||0).toString()})`
              ).join(',');
              await sql.query(
                `INSERT INTO price_history (epic,instrument,resolution,candle_time,open_price,high_price,low_price,close_price,volume)
                 VALUES ${vals} ON CONFLICT DO NOTHING`
              );
              inserted += chunk.length;
            }
            L(`${instr.name}: ${inserted} inserted, ${skipped} skipped (${candles.length} total)`);
            totalInserted += inserted; totalSkipped += skipped;
            results[instr.name] = { inserted, skipped, total: candles.length };
            await new Promise(r => setTimeout(r, 300));
          } catch(e) {
            L(`${instr.name}: ERROR ${e.message}`);
            results[instr.name] = { error: e.message };
          }
        }
        L(`Minute backfill done — ${totalInserted} inserted, ${totalSkipped} skipped`);
        return res.status(200).json({ success:true, totalInserted, totalSkipped, results, log });
      }

      if (type === 'backfill_hourly') {
        // Backfill 60 days of hourly candles from Yahoo Finance
        const specificInstrument = data?.instrument || null;
        const log = []; const L = msg => { console.log('[HourlyBackfill]', msg); log.push(msg); };

        const YAHOO_HOURLY = [
          { name:'FTSE 100',  ticker:'%5EFTSE',     scale:1 },
          { name:'DAX 40',    ticker:'%5EGDAXI',    scale:1 },
          { name:'S&P 500',   ticker:'%5EGSPC',     scale:1 },
          { name:'Dow Jones', ticker:'%5EDJI',      scale:1 },
          { name:'Nasdaq',    ticker:'%5EIXIC',     scale:1 },
          { name:'GBP/USD',   ticker:'GBPUSD%3DX',  scale:10000 },
          { name:'EUR/USD',   ticker:'EURUSD%3DX',  scale:10000 },
          { name:'USD/JPY',   ticker:'USDJPY%3DX',  scale:100 },
          { name:'EUR/GBP',   ticker:'EURGBP%3DX',  scale:10000 },
          { name:'Gold',      ticker:'GC%3DF',      scale:1 },
          { name:'Silver',    ticker:'SI%3DF',      scale:1 },
          { name:'Brent Oil', ticker:'BZ%3DF',      scale:1 },
        ];
        const EPIC_MAP_H = {
          'FTSE 100':'IX.D.FTSE.DAILY.IP','DAX 40':'IX.D.DAX.DAILY.IP',
          'S&P 500':'IX.D.SPTRD.DAILY.IP','Dow Jones':'IX.D.DOW.DAILY.IP',
          'Nasdaq':'IX.D.NASDAQ.CASH.IP','GBP/USD':'CS.D.GBPUSD.TODAY.IP',
          'EUR/USD':'CS.D.EURUSD.TODAY.IP','USD/JPY':'CS.D.USDJPY.TODAY.IP',
          'EUR/GBP':'CS.D.EURGBP.TODAY.IP','Gold':'CS.D.USCGC.TODAY.IP',
          'Silver':'CS.D.USCSI.TODAY.IP','Brent Oil':'CC.D.LCO.USS.IP',
        };

        const instruments = specificInstrument
          ? YAHOO_HOURLY.filter(i => i.name === specificInstrument)
          : YAHOO_HOURLY;

        let totalInserted = 0; let totalSkipped = 0; const results = {};

        for (const instr of instruments) {
          try {
            // Yahoo hourly: interval=1h, range=60d
            const url = `https://query1.finance.yahoo.com/v8/finance/chart/${instr.ticker}?interval=1h&range=60d`;
            const yr = await fetch(url, { headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'} });
            if (!yr.ok) throw new Error(`HTTP ${yr.status}`);
            const yd = await yr.json();
            const chart = yd.chart?.result?.[0];
            if (!chart) throw new Error('no chart data');

            const timestamps = chart.timestamp || [];
            const closes = chart.indicators?.quote?.[0]?.close || [];
            const opens = chart.indicators?.quote?.[0]?.open || [];
            const highs = chart.indicators?.quote?.[0]?.high || [];
            const lows = chart.indicators?.quote?.[0]?.low || [];

            const candles = timestamps.map((ts, i) => ({
              time: new Date(ts * 1000).toISOString(),
              open: opens[i], high: highs[i], low: lows[i], close: closes[i]
            })).filter(c => c.close != null && !isNaN(c.close));

            // Check existing hourly candles
            const existing = await sql`
              SELECT candle_time FROM price_history
              WHERE instrument=${instr.name} AND resolution='HOUR'`;
            const existingTimes = new Set(existing.rows.map(r =>
              new Date(r.candle_time).toISOString().substring(0,16)
            ));

            let inserted = 0; let skipped = 0;
            const scale = instr.scale || 1;
            const epicH = EPIC_MAP_H[instr.name] || '';

            const newCandlesH = candles.filter(c => {
              const timeKey = c.time.substring(0,16);
              return !existingTimes.has(timeKey);
            });
            skipped = candles.length - newCandlesH.length;

            const volumesH = chart.indicators?.quote?.[0]?.volume || [];
            const BATCH_H = 100;
            for(let b = 0; b < newCandlesH.length; b += BATCH_H) {
              const chunk = newCandlesH.slice(b, b + BATCH_H);
              const vals = chunk.map((c,ci) =>
                `('${epicH}','${instr.name}','HOUR','${c.time}',${c.open*scale},${c.high*scale},${c.low*scale},${c.close*scale},${Math.round(volumesH[b+ci]||0)})`
              ).join(',');
              await sql.query(
                `INSERT INTO price_history (epic,instrument,resolution,candle_time,open_price,high_price,low_price,close_price,volume)
                 VALUES ${vals} ON CONFLICT DO NOTHING`
              );
              inserted += chunk.length;
            }
            L(`${instr.name}: ${inserted} inserted, ${skipped} skipped (${candles.length} total)`);
            totalInserted += inserted; totalSkipped += skipped;
            results[instr.name] = { inserted, skipped, total: candles.length };
            await new Promise(r => setTimeout(r, 300));
          } catch(e) {
            L(`${instr.name}: ERROR ${e.message}`);
            results[instr.name] = { error: e.message };
          }
        }
        L(`Hourly backfill done — ${totalInserted} inserted, ${totalSkipped} skipped`);
        return res.status(200).json({ success:true, totalInserted, totalSkipped, results, log });
      }

      if (type === 'delete_candles') {
        // Delete candles for an instrument in a date range
        const { instrument, before, after } = data || {};
        if(!instrument) return res.status(400).json({ error: 'instrument required' });
        try {
          let result;
          if(after) {
            // Delete between after and before dates
            const from = after || '2000-01-01';
            const to = before || '2030-01-01';
            result = await sql`
              DELETE FROM price_history 
              WHERE instrument = ${instrument}
              AND candle_time >= ${from}::timestamptz
              AND candle_time <= ${to}::timestamptz
              AND resolution = 'DAY'
            `;
          } else {
            result = await sql`
              DELETE FROM price_history 
              WHERE instrument = ${instrument}
              AND candle_time < ${before||'2026-01-01'}::timestamptz
              AND resolution = 'DAY'
            `;
          }
          return res.status(200).json({ success: true, instrument });
        } catch(e) { return res.status(200).json({ error: e.message }); }
      }

      if (type === 'delete_trade') {
        const { dealId } = data || {};
        if (!dealId) return res.status(400).json({ error: 'No dealId' });
        try {
          await sql`DELETE FROM trades WHERE deal_id = ${dealId}`;
          return res.status(200).json({ success: true, deleted: dealId });
        } catch(e) { return res.status(200).json({ error: e.message }); }
      }

      if (type === 'import_trade') {
        const t = data;
        if (!t) return res.status(400).json({ error: 'No trade data' });
        try {
          await sql`
            INSERT INTO trades (
              deal_id, instrument, epic, direction, size,
              open_level, close_level, opened_at, closed_at,
              profit_loss, signal_score, ai_confidence, status,
              regime, trade_type, close_reason, ai_was_correct
            ) VALUES (
              ${t.dealId}, ${t.instrument}, ${t.epic||null}, ${t.direction}, ${t.size},
              ${t.openLevel}, ${t.closeLevel||null}, ${t.openedAt}::timestamptz,
              ${t.closedAt||null}::timestamptz,
              ${t.profitLoss||null}, ${t.signalScore||null}, ${t.aiConfidence||null},
              ${t.status||'closed'}, ${t.regime||'ranging'},
              ${t.tradeType||'hourly_mr'}, ${t.closeReason||'manual'},
              ${(t.profitLoss||0) > 0 ? true : false}
            )
            ON CONFLICT (deal_id) DO UPDATE SET
              close_level = EXCLUDED.close_level,
              closed_at = EXCLUDED.closed_at,
              profit_loss = EXCLUDED.profit_loss,
              status = EXCLUDED.status,
              close_reason = EXCLUDED.close_reason,
              ai_was_correct = EXCLUDED.ai_was_correct
          `;
          return res.status(200).json({ success: true, instrument: t.instrument });
        } catch(e) { return res.status(200).json({ error: e.message }); }
      }

      if (type === 'sentiment') {
        const { instrument, epic, longPct, shortPct } = data || {};
        if (instrument && longPct !== undefined) {
          await sql`INSERT INTO sentiment_history (instrument, epic, long_pct, short_pct)
            VALUES (${instrument}, ${epic||''}, ${parseFloat(longPct)}, ${parseFloat(shortPct||100-longPct)})`;
        }
        return res.status(200).json({ success: true });
      }

      if (type === 'trade_opened') {
        const openHour = new Date().getUTCHours();
        await sql`
          INSERT INTO trades (
            deal_id, deal_reference, instrument, epic, direction,
            size, open_level, opened_at, signal_score, ai_confidence,
            ai_reasoning, signal_reasons, status, regime, data_source,
            open_hour, stop_level, stop_distance, trade_type
          ) VALUES (
            ${data.dealId}, ${data.dealReference}, ${data.instrument},
            ${data.epic}, ${data.direction}, ${data.size}, ${data.openLevel},
            NOW(), ${data.signalScore}, ${data.aiConfidence},
            ${data.aiReasoning||null}, ${JSON.stringify(data.signalReasons)},
            'open', ${data.regime||null}, ${data.dataSource||null},
            ${openHour}, ${data.stopLevel||null}, ${data.stopDistance||null}, ${data.tradeType||'hourly_mr'}
          )
          ON CONFLICT (deal_id) DO UPDATE SET status = 'open'
        `;
        return res.status(200).json({ success: true });
      }

      if (type === 'trade_closed') {
        const trade = await sql`SELECT * FROM trades WHERE deal_id = ${data.dealId} LIMIT 1`;
        const t = trade.rows[0];
        const profitLoss = parseFloat(data.profitLoss || 0);
        const aiWasCorrect = t ? (
          (t.direction === 'BUY' && profitLoss > 0) ||
          (t.direction === 'SELL' && profitLoss > 0)
        ) : null;
        const holdingMinutes = t ? Math.round((Date.now() - new Date(t.opened_at).getTime()) / 60000) : null;

        await sql`
          UPDATE trades SET
            close_level = ${data.closeLevel},
            closed_at = NOW(),
            profit_loss = ${profitLoss},
            profit_loss_pct = ${data.profitLossPct||null},
            status = 'closed',
            close_reason = ${data.closeReason||'manual'},
            ai_was_correct = ${aiWasCorrect},
            holding_minutes = ${holdingMinutes},
            partial_close = ${data.partialClose||false}
          WHERE deal_id = ${data.dealId}
        `;
        return res.status(200).json({ success: true });
      }

      if (type === 'equity_snapshot') {
        await sql`
          INSERT INTO equity_snapshots (balance, profit_loss, available, snapshot_time)
          VALUES (${data.balance}, ${data.profitLoss}, ${data.available}, NOW())
        `;
        return res.status(200).json({ success: true });
      }

      if (type === 'engine_event') {
        await sql`
          INSERT INTO engine_events (event_type, instrument, details, created_at)
          VALUES (${data.eventType}, ${data.instrument||null}, ${JSON.stringify(data.details)}, NOW())
        `;
        return res.status(200).json({ success: true });
      }

      if (type === 'save_params') {
        await sql`
          INSERT INTO optimized_params (signal_threshold, ai_confidence_min, params_json, created_at, backtest_score)
          VALUES (${data.signalThreshold}, ${data.aiConfidenceMin}, ${JSON.stringify(data)}, NOW(), ${data.backtestScore||null})
        `;
        return res.status(200).json({ success: true });
      }
    }

    return res.status(400).json({ error: 'Unknown action' });

  } catch(err) {
    console.error('[DB] Error:', err.message);
    if (err.message.includes('relation') && err.message.includes('does not exist')) {
      return res.status(200).json({ error: 'Tables not initialised', hint: 'Call /api/db?action=init first', configured: true });
    }
    return res.status(500).json({ error: err.message });
  }
};

async function initTables() {
  // Core trades table with v4 columns
  await sql`
    CREATE TABLE IF NOT EXISTS trades (
      id SERIAL PRIMARY KEY,
      deal_id VARCHAR(50) UNIQUE,
      deal_reference VARCHAR(50),
      instrument VARCHAR(50),
      epic VARCHAR(100),
      direction VARCHAR(10),
      size DECIMAL(10,4),
      open_level DECIMAL(15,4),
      close_level DECIMAL(15,4),
      opened_at TIMESTAMPTZ DEFAULT NOW(),
      closed_at TIMESTAMPTZ,
      profit_loss DECIMAL(10,4),
      profit_loss_pct DECIMAL(8,4),
      signal_score INTEGER,
      ai_confidence INTEGER,
      ai_reasoning TEXT,
      signal_reasons JSONB,
      status VARCHAR(20) DEFAULT 'open',
      close_reason VARCHAR(50),
      regime VARCHAR(20),
      data_source VARCHAR(30),
      open_hour INTEGER,
      stop_level DECIMAL(15,4),
      stop_distance DECIMAL(10,4),
      ai_was_correct BOOLEAN,
      holding_minutes INTEGER,
      partial_close BOOLEAN DEFAULT false,
      trade_type VARCHAR(20) DEFAULT 'hourly_mr',
      close_reason VARCHAR(30)
    )
  `;

  // Pending entries table (pullback entry timing)
  await sql`
    CREATE TABLE IF NOT EXISTS pending_entries (
      id SERIAL PRIMARY KEY,
      epic VARCHAR(100), instrument VARCHAR(100),
      direction VARCHAR(10), signal_price NUMERIC, target_entry NUMERIC,
      size NUMERIC, stop_dist NUMERIC, trade_type VARCHAR(50),
      score INTEGER, ai_confidence INTEGER, ai_reasoning TEXT,
      expiry_time TIMESTAMPTZ, status VARCHAR(20) DEFAULT 'waiting',
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `.catch(()=>{});

  // Pairs trades table
  await sql`
    CREATE TABLE IF NOT EXISTS pairs_trades (
      id SERIAL PRIMARY KEY,
      pair_id VARCHAR(50) NOT NULL,
      instr_a VARCHAR(50), instr_b VARCHAR(50),
      epic_a VARCHAR(100), epic_b VARCHAR(100),
      direction_a VARCHAR(10), direction_b VARCHAR(10),
      size_a DECIMAL(10,4), size_b DECIMAL(10,4),
      deal_id_a VARCHAR(50), deal_id_b VARCHAR(50),
      entry_z DECIMAL(8,4), stop_z DECIMAL(8,4), target_z DECIMAL(8,4),
      close_z DECIMAL(8,4), close_reason VARCHAR(30),
      ai_confidence INTEGER,
      status VARCHAR(20) DEFAULT 'open',
      opened_at TIMESTAMPTZ DEFAULT NOW(),
      closed_at TIMESTAMPTZ
    )
  `;

  // Add missing columns to existing tables (migration)
  const cols = [
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS ai_reasoning TEXT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS regime VARCHAR(20)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS data_source VARCHAR(30)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS open_hour INTEGER",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS stop_level DECIMAL(15,4)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS stop_distance DECIMAL(10,4)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS trade_type VARCHAR(20) DEFAULT 'hourly_mr'",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS close_reason VARCHAR(30)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS pyramid_added BOOLEAN DEFAULT false",
    "ALTER TABLE price_history ALTER COLUMN volume TYPE BIGINT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS ai_was_correct BOOLEAN",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS holding_minutes INTEGER",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS partial_close BOOLEAN DEFAULT false",
  ];
  for (const col of cols) {
    try { await sql.query(col); } catch(e) { /* column may already exist */ }
  }

  await sql`
    CREATE TABLE IF NOT EXISTS equity_snapshots (
      id SERIAL PRIMARY KEY,
      balance DECIMAL(12,2),
      profit_loss DECIMAL(10,2),
      available DECIMAL(12,2),
      snapshot_time TIMESTAMPTZ DEFAULT NOW()
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS engine_events (
      id SERIAL PRIMARY KEY,
      event_type VARCHAR(50),
      instrument VARCHAR(50),
      details JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `;

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
      volume BIGINT,
      collected_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(epic, resolution, candle_time)
    )
  `;

  await sql`CREATE INDEX IF NOT EXISTS idx_price_history_epic_res ON price_history(epic, resolution, candle_time DESC)`;

  // Optimized parameters table
  await sql`
    CREATE TABLE IF NOT EXISTS optimized_params (
      id SERIAL PRIMARY KEY,
      signal_threshold INTEGER,
      ai_confidence_min INTEGER,
      params_json JSONB,
      backtest_score DECIMAL(8,4),
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `;

  // Views
  await sql`
    CREATE OR REPLACE VIEW daily_stats AS
    SELECT
      DATE(closed_at AT TIME ZONE 'Europe/London') as trade_date,
      COUNT(*) as total_trades,
      SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as winning_trades,
      SUM(CASE WHEN profit_loss <= 0 THEN 1 ELSE 0 END) as losing_trades,
      SUM(profit_loss) as total_pnl,
      AVG(profit_loss) as avg_pnl,
      MAX(profit_loss) as best_trade,
      MIN(profit_loss) as worst_trade,
      AVG(holding_minutes) as avg_holding_mins
    FROM trades
    WHERE status = 'closed' AND closed_at IS NOT NULL
    GROUP BY DATE(closed_at AT TIME ZONE 'Europe/London')
    ORDER BY trade_date DESC
  `;

  // Sentiment history table
  await sql`CREATE TABLE IF NOT EXISTS sentiment_history (
    id SERIAL PRIMARY KEY,
    instrument VARCHAR(50),
    epic VARCHAR(100),
    long_pct DECIMAL(5,2),
    short_pct DECIMAL(5,2),
    recorded_at TIMESTAMPTZ DEFAULT NOW()
  )`;
  await sql`CREATE INDEX IF NOT EXISTS idx_sentiment_instr ON sentiment_history(instrument, recorded_at DESC)`;

  console.log('[DB] v4 tables initialised');
}

async function getStats() {
  try {
    const [totalRes, winRes, pnlRes, bestRes, worstRes, recentRes, aiRes] = await Promise.all([
      sql`SELECT COUNT(*) as total FROM trades WHERE status = 'closed'`,
      sql`SELECT COUNT(*) as wins FROM trades WHERE status = 'closed' AND profit_loss > 0`,
      sql`SELECT SUM(profit_loss) as total_pnl, AVG(profit_loss) as avg_pnl FROM trades WHERE status = 'closed'`,
      sql`SELECT instrument, profit_loss FROM trades WHERE status = 'closed' ORDER BY profit_loss DESC LIMIT 1`,
      sql`SELECT instrument, profit_loss FROM trades WHERE status = 'closed' ORDER BY profit_loss ASC LIMIT 1`,
      sql`SELECT * FROM daily_stats LIMIT 30`,
      sql`SELECT AVG(CASE WHEN ai_was_correct THEN 1.0 ELSE 0.0 END) as ai_accuracy, COUNT(*) as ai_total FROM trades WHERE status = 'closed' AND ai_was_correct IS NOT NULL`,
    ]);

    const total = parseInt(totalRes.rows[0]?.total || 0);
    const wins = parseInt(winRes.rows[0]?.wins || 0);
    const winRate = total > 0 ? ((wins / total) * 100).toFixed(1) : '0.0';
    const aiAccuracy = aiRes.rows[0]?.ai_accuracy ? (parseFloat(aiRes.rows[0].ai_accuracy) * 100).toFixed(1) : null;

    return {
      totalTrades: total, winningTrades: wins, losingTrades: total - wins,
      winRate: parseFloat(winRate), totalPnL: parseFloat(pnlRes.rows[0]?.total_pnl || 0),
      avgPnL: parseFloat(pnlRes.rows[0]?.avg_pnl || 0),
      bestTrade: bestRes.rows[0] || null, worstTrade: worstRes.rows[0] || null,
      dailyStats: recentRes.rows,
      aiAccuracy, aiTotal: parseInt(aiRes.rows[0]?.ai_total || 0)
    };
  } catch(e) { return { error: e.message }; }
}

async function getCalibration() {
  try {
    // Group trades by AI confidence bracket and show actual win rate per bracket
    const result = await sql`
      SELECT
        CASE
          WHEN ai_confidence >= 90 THEN '90-100%'
          WHEN ai_confidence >= 80 THEN '80-89%'
          WHEN ai_confidence >= 70 THEN '70-79%'
          WHEN ai_confidence >= 60 THEN '60-69%'
          ELSE 'below 60%'
        END as confidence_bracket,
        COUNT(*) as total_trades,
        SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as wins,
        ROUND(AVG(profit_loss)::numeric, 4) as avg_pnl,
        ROUND(100.0 * SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as actual_win_rate
      FROM trades
      WHERE status = 'closed' AND ai_confidence IS NOT NULL AND profit_loss IS NOT NULL
      GROUP BY confidence_bracket
      ORDER BY confidence_bracket DESC
    `;
    return { calibration: result.rows };
  } catch(e) { return { error: e.message }; }
}

async function getTimeOfDay() {
  try {
    const result = await sql`
      SELECT
        open_hour,
        COUNT(*) as total_trades,
        SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as wins,
        ROUND(AVG(profit_loss)::numeric, 4) as avg_pnl,
        ROUND(100.0 * SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as win_rate
      FROM trades
      WHERE status = 'closed' AND open_hour IS NOT NULL AND profit_loss IS NOT NULL
      GROUP BY open_hour
      ORDER BY open_hour ASC
    `;
    // Find best and worst hours
    const rows = result.rows;
    const best = rows.reduce((a, b) => parseFloat(a.avg_pnl) > parseFloat(b.avg_pnl) ? a : b, rows[0] || {});
    const worst = rows.reduce((a, b) => parseFloat(a.avg_pnl) < parseFloat(b.avg_pnl) ? a : b, rows[0] || {});
    return { byHour: rows, bestHour: best, worstHour: worst };
  } catch(e) { return { error: e.message }; }
}

async function getOptimizedParams() {
  try {
    const result = await sql`
      SELECT * FROM optimized_params ORDER BY created_at DESC LIMIT 1
    `;
    return result.rows[0] || { signal_threshold: 2, ai_confidence_min: 60 };
  } catch(e) { return { signal_threshold: 2, ai_confidence_min: 60 }; }
}

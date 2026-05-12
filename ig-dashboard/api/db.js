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
            open_hour, stop_level, stop_distance
          ) VALUES (
            ${data.dealId}, ${data.dealReference}, ${data.instrument},
            ${data.epic}, ${data.direction}, ${data.size}, ${data.openLevel},
            NOW(), ${data.signalScore}, ${data.aiConfidence},
            ${data.aiReasoning||null}, ${JSON.stringify(data.signalReasons)},
            'open', ${data.regime||null}, ${data.dataSource||null},
            ${openHour}, ${data.stopLevel||null}, ${data.stopDistance||null}
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
      partial_close BOOLEAN DEFAULT false
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
      volume INTEGER,
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

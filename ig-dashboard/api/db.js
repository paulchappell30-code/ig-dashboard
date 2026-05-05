// Database API — handles all Vercel Postgres operations
// GET  /api/db?action=init        — initialise tables
// GET  /api/db?action=trades      — get trade history
// GET  /api/db?action=equity      — get equity curve
// GET  /api/db?action=stats       — get performance stats
// POST /api/db                    — save trade, equity snapshot, or event

const { sql } = require('@vercel/postgres');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Check if Postgres is configured
  if (!process.env.POSTGRES_URL) {
    return res.status(200).json({
      configured: false,
      message: 'Vercel Postgres not configured. Add a Postgres database in your Vercel project settings.'
    });
  }

  try {
    if (req.method === 'GET') {
      const action = req.query.action || 'trades';

      if (action === 'init') {
        await initTables();
        return res.status(200).json({ success: true, message: 'Tables initialised' });
      }

      if (action === 'trades') {
        const limit = parseInt(req.query.limit) || 100;
        const result = await sql`
          SELECT * FROM trades 
          ORDER BY opened_at DESC 
          LIMIT ${limit}
        `;
        return res.status(200).json({ trades: result.rows });
      }

      if (action === 'equity') {
        const days = parseInt(req.query.days) || 30;
        const result = await sql`
          SELECT * FROM equity_snapshots 
          WHERE snapshot_time > NOW() - INTERVAL '${days} days'
          ORDER BY snapshot_time ASC
        `;
        return res.status(200).json({ snapshots: result.rows });
      }

      if (action === 'stats') {
        const stats = await getStats();
        return res.status(200).json(stats);
      }

      if (action === 'calendar') {
        const events = await getEconomicCalendar();
        return res.status(200).json({ events });
      }
    }

    if (req.method === 'POST') {
      const { type, data } = req.body || {};

      if (type === 'trade_opened') {
        await sql`
          INSERT INTO trades (
            deal_id, deal_reference, instrument, epic, direction, 
            size, open_level, opened_at, signal_score, ai_confidence,
            signal_reasons, status
          ) VALUES (
            ${data.dealId}, ${data.dealReference}, ${data.instrument}, 
            ${data.epic}, ${data.direction}, ${data.size}, ${data.openLevel},
            NOW(), ${data.signalScore}, ${data.aiConfidence},
            ${JSON.stringify(data.signalReasons)}, 'open'
          )
          ON CONFLICT (deal_id) DO UPDATE SET status = 'open'
        `;
        return res.status(200).json({ success: true });
      }

      if (type === 'trade_closed') {
        await sql`
          UPDATE trades SET
            close_level = ${data.closeLevel},
            closed_at = NOW(),
            profit_loss = ${data.profitLoss},
            profit_loss_pct = ${data.profitLossPct},
            status = 'closed',
            close_reason = ${data.closeReason || 'manual'}
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
          VALUES (${data.eventType}, ${data.instrument || null}, ${JSON.stringify(data.details)}, NOW())
        `;
        return res.status(200).json({ success: true });
      }
    }

    return res.status(400).json({ error: 'Unknown action' });

  } catch (err) {
    console.error('[DB] Error:', err.message);
    // Return gracefully if DB not set up yet
    if (err.message.includes('relation') && err.message.includes('does not exist')) {
      return res.status(200).json({ error: 'Tables not initialised', hint: 'Call /api/db?action=init first', configured: true });
    }
    return res.status(500).json({ error: err.message });
  }
};

async function initTables() {
  // Trades table
  await sql`
    CREATE TABLE IF NOT EXISTS trades (
      id SERIAL PRIMARY KEY,
      deal_id VARCHAR(50) UNIQUE,
      deal_reference VARCHAR(50),
      instrument VARCHAR(50),
      epic VARCHAR(100),
      direction VARCHAR(10),
      size DECIMAL(10,2),
      open_level DECIMAL(15,4),
      close_level DECIMAL(15,4),
      opened_at TIMESTAMPTZ DEFAULT NOW(),
      closed_at TIMESTAMPTZ,
      profit_loss DECIMAL(10,2),
      profit_loss_pct DECIMAL(8,4),
      signal_score INTEGER,
      ai_confidence INTEGER,
      signal_reasons JSONB,
      status VARCHAR(20) DEFAULT 'open',
      close_reason VARCHAR(50)
    )
  `;

  // Equity snapshots table
  await sql`
    CREATE TABLE IF NOT EXISTS equity_snapshots (
      id SERIAL PRIMARY KEY,
      balance DECIMAL(12,2),
      profit_loss DECIMAL(10,2),
      available DECIMAL(12,2),
      snapshot_time TIMESTAMPTZ DEFAULT NOW()
    )
  `;

  // Engine events table
  await sql`
    CREATE TABLE IF NOT EXISTS engine_events (
      id SERIAL PRIMARY KEY,
      event_type VARCHAR(50),
      instrument VARCHAR(50),
      details JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `;

  // Daily stats view
  await sql`
    CREATE OR REPLACE VIEW daily_stats AS
    SELECT 
      DATE(closed_at) as trade_date,
      COUNT(*) as total_trades,
      SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as winning_trades,
      SUM(CASE WHEN profit_loss < 0 THEN 1 ELSE 0 END) as losing_trades,
      SUM(profit_loss) as total_pnl,
      AVG(profit_loss) as avg_pnl,
      MAX(profit_loss) as best_trade,
      MIN(profit_loss) as worst_trade
    FROM trades
    WHERE status = 'closed' AND closed_at IS NOT NULL
    GROUP BY DATE(closed_at)
    ORDER BY trade_date DESC
  `;

  console.log('[DB] Tables initialised successfully');
}

async function getStats() {
  try {
    const [totalRes, winRes, pnlRes, bestRes, worstRes, recentRes] = await Promise.all([
      sql`SELECT COUNT(*) as total FROM trades WHERE status = 'closed'`,
      sql`SELECT COUNT(*) as wins FROM trades WHERE status = 'closed' AND profit_loss > 0`,
      sql`SELECT SUM(profit_loss) as total_pnl, AVG(profit_loss) as avg_pnl FROM trades WHERE status = 'closed'`,
      sql`SELECT instrument, profit_loss FROM trades WHERE status = 'closed' ORDER BY profit_loss DESC LIMIT 1`,
      sql`SELECT instrument, profit_loss FROM trades WHERE status = 'closed' ORDER BY profit_loss ASC LIMIT 1`,
      sql`SELECT * FROM daily_stats LIMIT 30`,
    ]);

    const total = parseInt(totalRes.rows[0]?.total || 0);
    const wins = parseInt(winRes.rows[0]?.wins || 0);
    const winRate = total > 0 ? ((wins / total) * 100).toFixed(1) : '0.0';

    return {
      totalTrades: total,
      winningTrades: wins,
      losingTrades: total - wins,
      winRate: parseFloat(winRate),
      totalPnL: parseFloat(pnlRes.rows[0]?.total_pnl || 0),
      avgPnL: parseFloat(pnlRes.rows[0]?.avg_pnl || 0),
      bestTrade: bestRes.rows[0] || null,
      worstTrade: worstRes.rows[0] || null,
      dailyStats: recentRes.rows
    };
  } catch(e) {
    return { error: e.message };
  }
}

async function getEconomicCalendar() {
  // Fetch from a free economic calendar API
  try {
    const fetch = require('node-fetch');
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const dateStr = tomorrow.toISOString().split('T')[0];
    
    // Use TradingEconomics or similar free API
    // For now return key known events structure
    const res = await fetch(`https://economic-calendar.tradingeconomics.com/calendar?g=united+kingdom&d1=${dateStr}&d2=${dateStr}`);
    if (res.ok) {
      const data = await res.json();
      return data.slice(0, 10);
    }
  } catch(e) {
    console.warn('[DB] Calendar fetch failed:', e.message);
  }
  return [];
}

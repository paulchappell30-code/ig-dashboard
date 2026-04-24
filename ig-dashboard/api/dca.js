// DCA Executor — runs on a schedule via Vercel Cron
// POST /api/dca — manually trigger DCA execution
// GET  /api/dca — check DCA schedule status
const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

// Epic map for instruments
const EPIC_MAP = {
  'FTSE 100':  'IX.D.FTSE.DAILY.IP',
  'S&P 500':   'IX.D.SPTRD.DAILY.IP',
  'DAX 40':    'IX.D.DAX.DAILY.IP',
  'Dow Jones': 'IX.D.DOW.DAILY.IP',
  'Brent Oil': 'CC.D.LCO.USS.IP',
  'Crude Oil': 'CC.D.LCO.USS.IP',
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // Verify cron secret to prevent unauthorised execution
  const authHeader = req.headers['authorization'] || '';
  const cronSecret = process.env.CRON_SECRET || '';
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    // Allow requests with no secret set (initial setup)
    if (cronSecret) {
      return res.status(401).json({ error: 'Unauthorised' });
    }
  }

  if (req.method === 'GET') {
    return res.status(200).json({
      status: 'DCA executor ready',
      env: process.env.IG_ENV || 'demo',
      hasCredentials: !!(process.env.IG_USERNAME && process.env.IG_PASSWORD),
      schedules: getSchedules(),
      time: new Date().toISOString()
    });
  }

  // POST — execute DCA schedules
  const { schedules, manualRun, instrument } = req.body || {};
  const schedulesToRun = manualRun && instrument
    ? [{ instr: instrument, amt: req.body.amt || '100', freq: 'Manual' }]
    : getSchedules();

  if (!schedulesToRun.length) {
    return res.status(200).json({ message: 'No active DCA schedules to run', executed: [] });
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
      console.error('[DCA] Auth failed:', err);
      return res.status(500).json({ error: 'IG authentication failed', detail: err });
    }

    cst = authRes.headers.get('CST');
    xst = authRes.headers.get('X-SECURITY-TOKEN');

    if (!cst) {
      return res.status(500).json({ error: 'No CST token received from IG' });
    }

    console.log('[DCA] Authenticated with IG successfully');
  } catch (err) {
    return res.status(500).json({ error: 'IG auth error', detail: err.message });
  }

  // Execute each schedule
  const results = [];
  const now = new Date();

  for (const schedule of schedulesToRun) {
    if (!manualRun && !isDueToday(schedule, now)) {
      results.push({ instr: schedule.instr, skipped: true, reason: 'Not due today' });
      continue;
    }

    const epic = EPIC_MAP[schedule.instr];
    if (!epic) {
      results.push({ instr: schedule.instr, skipped: true, reason: 'Unknown instrument' });
      continue;
    }

    // Get current market price
    let currentPrice;
    try {
      const mktRes = await fetch(`${igBase}/markets/${epic}`, {
        headers: {
          'X-IG-API-KEY': process.env.IG_API_KEY || '',
          'CST': cst,
          'X-SECURITY-TOKEN': xst,
          'Version': '1'
        }
      });
      const mktData = await mktRes.json();
      currentPrice = mktData.snapshot && mktData.snapshot.offer;
    } catch (e) {
      console.error('[DCA] Price fetch error:', e.message);
    }

    // Calculate deal size: amount / price = size in £/point
    const amount = parseFloat(String(schedule.amt).replace('£', '')) || 100;
    const dealSize = currentPrice ? Math.max(1, Math.round(amount / currentPrice)) : 1;

    // Check volatility skip if enabled
    if (schedule.skipVolatility && currentPrice) {
      const skipRes = await checkVolatility(igBase, epic, cst, xst);
      if (skipRes.spike) {
        const msg = `DCA skipped for ${schedule.instr} — volatility spike detected (${skipRes.pct}%)`;
        results.push({ instr: schedule.instr, skipped: true, reason: msg });
        await sendNotification('dca', `⚠️ DCA Skipped: ${schedule.instr}`,
          `DCA order was skipped due to high volatility.\n\nInstrument: ${schedule.instr}\nVolatility: ${skipRes.pct}%\nThreshold: 3%\nScheduled amount: £${amount}`);
        continue;
      }
    }

    // Place market order
    try {
      const orderRes = await fetch(`${igBase}/positions/otc`, {
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
          direction: 'BUY',
          size: dealSize,
          orderType: 'MARKET',
          expiry: '-',
          guaranteedStop: false,
          forceOpen: true,
          currencyCode: 'GBP'
        })
      });

      const orderData = await orderRes.json();
      console.log('[DCA] Order response:', JSON.stringify(orderData));

      if (orderData.dealReference) {
        const result = {
          instr: schedule.instr,
          epic,
          amount: `£${amount}`,
          dealSize,
          price: currentPrice,
          dealReference: orderData.dealReference,
          status: 'executed',
          time: now.toISOString()
        };
        results.push(result);

        // Send success email
        await sendNotification('dca',
          `✅ DCA Executed: ${schedule.instr}`,
          `Your DCA order has been placed successfully.\n\nInstrument: ${schedule.instr}\nAmount: £${amount}\nDeal size: £${dealSize}/pt\nPrice: ${currentPrice}\nDeal reference: ${orderData.dealReference}\nTime: ${now.toLocaleString('en-GB', { timeZone: 'Europe/London' })}\nFrequency: ${schedule.freq}`
        );
      } else {
        results.push({
          instr: schedule.instr,
          status: 'failed',
          error: orderData.errorCode || 'Unknown error',
          raw: orderData
        });

        await sendNotification('error',
          `❌ DCA Failed: ${schedule.instr}`,
          `Your DCA order failed to execute.\n\nInstrument: ${schedule.instr}\nAmount: £${amount}\nError: ${orderData.errorCode || 'Unknown'}\nTime: ${now.toLocaleString('en-GB', { timeZone: 'Europe/London' })}\n\nPlease check your IG account and dashboard.`
        );
      }
    } catch (err) {
      console.error('[DCA] Order error:', err.message);
      results.push({ instr: schedule.instr, status: 'error', error: err.message });
    }
  }

  console.log('[DCA] Run complete. Results:', JSON.stringify(results));
  res.status(200).json({ executed: results, time: now.toISOString() });
};

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function getSchedules() {
  try {
    const raw = process.env.DCA_SCHEDULES;
    if (raw) {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed.filter(s => s.active !== false) : [];
    }
  } catch (e) {
    console.warn('[DCA] Could not parse DCA_SCHEDULES env var:', e.message);
  }
  return [];
}

function isDueToday(schedule, now) {
  const freq = (schedule.freq || '').toLowerCase();
  const day = schedule.day || 1;

  if (freq === 'daily') return true;

  if (freq === 'weekly') {
    // Run on Monday (1) by default, or specified day
    return now.getDay() === (schedule.weekday || 1);
  }

  if (freq === 'monthly') {
    return now.getDate() === day;
  }

  return false;
}

async function checkVolatility(igBase, epic, cst, xst) {
  try {
    const res = await fetch(`${igBase}/markets/${epic}`, {
      headers: {
        'X-IG-API-KEY': process.env.IG_API_KEY || '',
        'CST': cst, 'X-SECURITY-TOKEN': xst, 'Version': '1'
      }
    });
    const data = await res.json();
    const pct = Math.abs(data.snapshot && data.snapshot.percentageChange || 0);
    return { spike: pct > 3, pct: pct.toFixed(2) };
  } catch (e) {
    return { spike: false, pct: 0 };
  }
}

async function sendNotification(type, subject, body) {
  try {
    const notifyUrl = process.env.VERCEL_URL
      ? `https://${process.env.VERCEL_URL}/api/notify`
      : 'http://localhost:3000/api/notify';

    await fetch(notifyUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, subject, body })
    });
  } catch (e) {
    console.error('[DCA] Notification error:', e.message);
  }
}

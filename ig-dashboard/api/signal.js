// Signal Webhook Receiver
// Accepts incoming signals from ProRealTime, TradingView, or any webhook source
// POST /api/signal
// Body: { epic, direction, instrument, source, strength, indicators }
const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

// Map instrument names to epics (accepts various formats)
const EPIC_MAP = {
  // Standard names
  'FTSE 100': 'IX.D.FTSE.DAILY.IP',
  'S&P 500':  'IX.D.SPTRD.DAILY.IP',
  'DAX 40':   'IX.D.DAX.DAILY.IP',
  'Dow Jones':'IX.D.DOW.DAILY.IP',
  'Brent Oil':'CC.D.LCO.USS.IP',
  'GBP/USD':  'CS.D.GBPUSD.MINI.IP',
  'EUR/USD':  'CS.D.EURUSD.MINI.IP',
  'USD/JPY':  'CS.D.USDJPY.MINI.IP',
  // Short names
  'FTSE':     'IX.D.FTSE.DAILY.IP',
  'SPX':      'IX.D.SPTRD.DAILY.IP',
  'SP500':    'IX.D.SPTRD.DAILY.IP',
  'DAX':      'IX.D.DAX.DAILY.IP',
  'DOW':      'IX.D.DOW.DAILY.IP',
  'OIL':      'CC.D.LCO.USS.IP',
  'GBPUSD':   'CS.D.GBPUSD.MINI.IP',
  'EURUSD':   'CS.D.EURUSD.MINI.IP',
  'USDJPY':   'CS.D.USDJPY.MINI.IP',
  // IG epics passed directly
  'IX.D.FTSE.DAILY.IP':  'IX.D.FTSE.DAILY.IP',
  'IX.D.SPTRD.DAILY.IP': 'IX.D.SPTRD.DAILY.IP',
  'IX.D.DAX.DAILY.IP':   'IX.D.DAX.DAILY.IP',
  'IX.D.DOW.DAILY.IP':   'IX.D.DOW.DAILY.IP',
  'CC.D.LCO.USS.IP':     'CC.D.LCO.USS.IP',
  'CS.D.GBPUSD.MINI.IP': 'CS.D.GBPUSD.MINI.IP',
  'CS.D.EURUSD.MINI.IP': 'CS.D.EURUSD.MINI.IP',
  'CS.D.USDJPY.MINI.IP': 'CS.D.USDJPY.MINI.IP',
};

const INSTRUMENT_NAMES = {
  'IX.D.FTSE.DAILY.IP':  'FTSE 100',
  'IX.D.SPTRD.DAILY.IP': 'S&P 500',
  'IX.D.DAX.DAILY.IP':   'DAX 40',
  'IX.D.DOW.DAILY.IP':   'Dow Jones',
  'CC.D.LCO.USS.IP':     'Brent Oil',
  'CS.D.GBPUSD.MINI.IP': 'GBP/USD',
  'CS.D.EURUSD.MINI.IP': 'EUR/USD',
  'CS.D.USDJPY.MINI.IP': 'USD/JPY',
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Signal-Key');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // GET — return endpoint info
  if (req.method === 'GET') {
    return res.status(200).json({
      status: 'Signal webhook receiver ready',
      version: '1.0',
      accepts: {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Signal-Key': 'YOUR_SIGNAL_KEY' },
        body: {
          instrument: 'FTSE 100 | FTSE | IX.D.FTSE.DAILY.IP | etc',
          direction: 'BUY | SELL',
          source: 'ProRealTime | TradingView | Manual',
          strength: '1-5 (optional, default 3)',
          indicators: 'Description of signal (optional)',
          price: 'Current price (optional)',
        }
      },
      supportedInstruments: Object.keys(EPIC_MAP).filter(k => !k.includes('.')),
      time: new Date().toISOString()
    });
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  // Signal key auth — separate from cron secret
  const signalKey = process.env.SIGNAL_KEY || process.env.CRON_SECRET || '';
  if (signalKey) {
    const provided = req.headers['x-signal-key'] || 
                     (req.headers['authorization'] || '').replace('Bearer ', '').trim();
    if (provided !== signalKey) {
      console.warn('[Signal] Unauthorised attempt from', req.headers['x-forwarded-for']);
      return res.status(401).json({ error: 'Invalid signal key' });
    }
  }

  const body = req.body || {};
  const log = [];
  const L = msg => { console.log('[Signal]', msg); log.push(msg); };

  L('=== Signal received === ' + new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' }));
  L('Source: ' + (body.source || 'unknown'));
  L('Raw body: ' + JSON.stringify(body));

  // Parse instrument
  const rawInstrument = body.instrument || body.epic || body.symbol || '';
  const epic = EPIC_MAP[rawInstrument] || EPIC_MAP[rawInstrument?.toUpperCase()] || null;
  const instr = INSTRUMENT_NAMES[epic] || rawInstrument;

  if (!epic) {
    L('Unknown instrument: ' + rawInstrument);
    return res.status(400).json({
      error: 'Unknown instrument: ' + rawInstrument,
      supported: Object.keys(EPIC_MAP).filter(k => !k.includes('.')),
      log
    });
  }

  // Parse direction
  const rawDir = (body.direction || body.action || body.side || '').toUpperCase();
  const direction = rawDir.includes('BUY') || rawDir === 'LONG' ? 'BUY' :
                    rawDir.includes('SELL') || rawDir === 'SHORT' ? 'SELL' : null;

  if (!direction) {
    L('Invalid direction: ' + rawDir);
    return res.status(400).json({ error: 'Direction must be BUY or SELL', received: rawDir, log });
  }

  const strength = parseInt(body.strength) || 3;
  const source = body.source || 'External';
  const indicators = body.indicators || body.reason || body.message || '';
  const signalPrice = parseFloat(body.price) || null;

  L(`Signal: ${direction} ${instr} (${epic}) | Strength: ${strength}/5 | Source: ${source}`);
  if (indicators) L('Indicators: ' + indicators);

  // Check if auto-trading is enabled
  if (process.env.AUTO_TRADING_ENABLED === 'false') {
    L('Auto-trading disabled — signal logged but not executed');
    await saveToDb('engine_event', {
      eventType: 'signal_received_disabled',
      instrument: instr,
      details: { epic, direction, strength, source, indicators }
    });
    return res.status(200).json({ action: 'logged_only', reason: 'Auto-trading disabled', log });
  }

  // Authenticate with IG
  const igBase = IG_BASES[process.env.IG_ENV || 'demo'];
  let cst, xst;
  try {
    const ar = await fetch(`${igBase}/session`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-IG-API-KEY': process.env.IG_API_KEY || '', 'Version': '2' },
      body: JSON.stringify({ identifier: process.env.IG_USERNAME, password: process.env.IG_PASSWORD })
    });
    if (!ar.ok) { L('IG auth failed'); return res.status(500).json({ error: 'IG auth failed', log }); }
    cst = ar.headers.get('CST');
    xst = ar.headers.get('X-SECURITY-TOKEN');
  } catch(e) { return res.status(500).json({ error: 'Auth: ' + e.message, log }); }

  const igH = { 'Content-Type': 'application/json', 'X-IG-API-KEY': process.env.IG_API_KEY || '', 'CST': cst, 'X-SECURITY-TOKEN': xst };

  // Get account state
  let balance, available, dailyPL;
  try {
    const ar = await fetch(`${igBase}/accounts`, { headers: { ...igH, 'Version': '1' } });
    const ad = await ar.json();
    const acct = ad.accounts && ad.accounts.find(a => a.accountType === 'SPREADBET');
    if (!acct) return res.status(500).json({ error: 'No spreadbet account', log });
    balance = acct.balance.balance;
    available = acct.balance.available;
    dailyPL = acct.balance.profitLoss;
    L(`Account: £${balance} | Available: £${available} | Daily P&L: £${dailyPL}`);
  } catch(e) { return res.status(500).json({ error: 'Account: ' + e.message, log }); }

  // Daily limit checks
  const plPct = balance > 0 ? (dailyPL / balance) * 100 : 0;
  const dailyLossLimit = parseFloat(process.env.DAILY_LOSS_LIMIT || '1.0');
  const dailyProfitLock = parseFloat(process.env.DAILY_PROFIT_LOCK || '2.0');

  if (plPct <= -dailyLossLimit) {
    L(`Daily loss limit hit (${plPct.toFixed(2)}%) — rejecting signal`);
    return res.status(200).json({ action: 'rejected', reason: 'daily_loss_limit', plPct, log });
  }

  if (plPct >= dailyProfitLock) {
    L(`Daily profit lock hit (${plPct.toFixed(2)}%) — rejecting signal`);
    return res.status(200).json({ action: 'rejected', reason: 'profit_lock', plPct, log });
  }

  // Check existing positions
  let openPos = [];
  try {
    const pr = await fetch(`${igBase}/positions`, { headers: { ...igH, 'Version': '1' } });
    const pd = await pr.json();
    openPos = pd.positions || [];
    const maxPositions = parseInt(process.env.MAX_POSITIONS || '3');

    // Check if already in this instrument
    if (openPos.some(p => p.market.epic === epic)) {
      L(`Already have position in ${instr} — rejecting duplicate`);
      return res.status(200).json({ action: 'rejected', reason: 'duplicate_position', instrument: instr, log });
    }

    if (openPos.length >= maxPositions) {
      L(`Max positions (${maxPositions}) reached`);
      return res.status(200).json({ action: 'rejected', reason: 'max_positions', log });
    }
  } catch(e) { L('Position check error: ' + e.message); }

  // Get market info for margin check
  let currentPrice = signalPrice;
  let marginPct = 0.05;
  let minSize = 0.01;
  try {
    const mktRes = await fetch(`${igBase}/markets/${epic}`, { headers: { ...igH, 'Version': '3' } });
    if (mktRes.ok) {
      const mktData = await mktRes.json();
      currentPrice = currentPrice || mktData.snapshot?.bid || mktData.snapshot?.offer || 10000;
      const bands = mktData.instrument?.marginDepositBands || [];
      minSize = mktData.dealingRules?.minDealSize?.value || 0.01;

      // Calculate position size based on Kelly / account size
      const winRate = 0.5; // Default until we have history
      const atr = currentPrice * 0.005; // Rough 0.5% ATR estimate
      const riskAmount = balance * 0.01; // 1% risk per trade
      const stopPoints = atr * 2;
      let sz = Math.max(minSize, Math.min(Math.floor(riskAmount / stopPoints * 100) / 100,
        parseInt(process.env.MAX_SIZE_PER_TRADE || '5')));

      // Margin check
      const notional = currentPrice * sz;
      const band = bands.find(b => notional >= b.min && (b.max === null || notional < b.max)) || bands[0] || { margin: 5 };
      marginPct = band.margin / 100;
      const requiredMargin = notional * marginPct;

      L(`Market: price ${currentPrice}, margin ${band.margin}%, size ${sz}, need £${requiredMargin.toFixed(0)}`);

      if (requiredMargin > available * 0.85) {
        // Reduce size to fit
        sz = Math.floor((available * 0.85) / (currentPrice * marginPct) * 100) / 100;
        sz = Math.max(minSize, sz);
        L(`Reduced size to ${sz} units for margin`);
      }

      // AI confirmation
      const requireAI = process.env.REQUIRE_AI_CONFIRM !== 'false';
      const aiConfMin = parseInt(process.env.AI_CONFIDENCE_MIN || '60');
      let aiApproved = !requireAI;
      let aiConfidence = 100;
      let aiReasoning = 'AI confirmation not required';

      if (requireAI) {
        try {
          L('Requesting AI confirmation...');
          const prompt = `Trading risk manager. A ${source} signal has triggered. Should we execute this trade?

SIGNAL DETAILS:
Instrument: ${instr}
Direction: ${direction}
Signal strength: ${strength}/5
Source: ${source}
Indicators: ${indicators || 'Not specified'}
Current price: ${currentPrice}

ACCOUNT STATE:
Balance: £${balance}
Available: £${available}
Daily P&L: ${plPct.toFixed(2)}% (Lock: +${dailyProfitLock}%, Limit: -${dailyLossLimit}%)
Open positions: ${openPos.length}
Proposed size: ${sz} units

Consider: Is this signal from a reputable source? Does the direction make sense for current market conditions? Is the risk appropriate?

Respond ONLY: {"approved":true,"confidence":75,"reasoning":"2 sentences","risk":"main risk"}`;

          const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY || '', 'anthropic-version': '2023-06-01' },
            body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: 200, messages: [{ role: 'user', content: prompt }] })
          });
          const aiData = await aiRes.json();
          const aiText = aiData.content && aiData.content[0] && aiData.content[0].text || '{}';
          const result = JSON.parse(aiText.replace(/```json|```/g, '').trim());
          aiApproved = result.approved === true && (result.confidence || 0) >= aiConfMin;
          aiConfidence = result.confidence || 0;
          aiReasoning = result.reasoning || '';
          L(`AI: ${aiApproved ? '✅' : '❌'} (${aiConfidence}%) — ${aiReasoning}`);
        } catch(e) {
          L('AI error — proceeding: ' + e.message);
          aiApproved = true;
        }
      }

      if (!aiApproved) {
        L('Signal rejected by AI');
        await saveToDb('engine_event', {
          eventType: 'signal_ai_rejected',
          instrument: instr,
          details: { epic, direction, source, strength, aiConfidence, aiReasoning }
        });
        return res.status(200).json({ action: 'ai_rejected', aiConfidence, reasoning: aiReasoning, log });
      }

      // Place order
      L(`Placing ${direction} ${sz} units on ${instr}...`);
      const ob = {
        epic, direction, size: sz, orderType: 'MARKET',
        expiry: 'DFB', guaranteedStop: false, forceOpen: true,
        currencyCode: 'GBP', dealType: 'SPREADBET',
      };

      // Add trailing stop
      const trailingStopPct = parseFloat(process.env.TRAILING_STOP_PCT || '1.5');
      if (trailingStopPct > 0) {
        const stopDist = Math.max(10, currentPrice * 0.01);
        ob.trailingStop = true;
        ob.trailingStopDistance = Math.round(stopDist);
        ob.trailingStopIncrement = Math.max(1, Math.round(stopDist / 4));
      }

      let ref;
      const or = await fetch(`${igBase}/positions/otc`, {
        method: 'POST', headers: { ...igH, 'Version': '1' }, body: JSON.stringify(ob)
      });
      const od = await or.json();

      if (od.dealReference) {
        ref = od.dealReference;
      } else {
        // Retry without trailing stop
        L('Retry without trailing stop: ' + (od.errorCode || '?'));
        delete ob.trailingStop; delete ob.trailingStopDistance; delete ob.trailingStopIncrement;
        const r2 = await fetch(`${igBase}/positions/otc`, { method: 'POST', headers: { ...igH, 'Version': '1' }, body: JSON.stringify(ob) });
        const d2 = await r2.json();
        if (!d2.dealReference) {
          L('Order failed: ' + (d2.errorCode || '?'));
          return res.status(200).json({ action: 'order_failed', error: d2.errorCode, log });
        }
        ref = d2.dealReference;
      }

      await new Promise(r => setTimeout(r, 1500));
      const cr = await fetch(`${igBase}/confirms/${ref}`, { headers: { ...igH, 'Version': '1' } });
      const confirm = await cr.json();

      if (confirm.dealStatus === 'ACCEPTED') {
        L(`✅ ACCEPTED ref:${ref} level:${confirm.level}`);

        await saveToDb('trade_opened', {
          dealId: confirm.dealId, dealReference: ref,
          instrument: instr, epic, direction, size: sz,
          openLevel: confirm.level, signalScore: strength,
          aiConfidence, signalReasons: [source + ' signal', indicators].filter(Boolean),
          regime: 'external', dataSource: source
        });

        await sendNotify('dca', `✅ ${source} Signal Executed: ${direction} ${instr}`,
          `Signal from ${source} executed successfully.\n\nInstrument: ${instr}\nDirection: ${direction}\nSize: ${sz} units\nPrice: ${confirm.level}\nRef: ${ref}\n\nSignal strength: ${strength}/5\nAI confidence: ${aiConfidence}%\n${aiReasoning}\n\nIndicators: ${indicators || 'Not specified'}\n\nDaily P&L: ${plPct.toFixed(2)}%\nTime: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`
        );

        return res.status(200).json({
          action: 'trade_placed', source,
          instrument: instr, direction, size: sz,
          dealReference: ref, level: confirm.level,
          aiConfidence, log
        });
      } else {
        L('Order rejected: ' + (confirm.reason || confirm.dealStatus));
        return res.status(200).json({ action: 'order_rejected', reason: confirm.reason, log });
      }
    }
  } catch(e) {
    L('Error: ' + e.message);
    return res.status(500).json({ error: e.message, log });
  }
};

async function saveToDb(type, data) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    await fetch(`${base}/api/db`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, data })
    });
  } catch(e) {}
}

async function sendNotify(type, subject, body) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    await fetch(`${base}/api/notify`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, subject, body })
    });
  } catch(e) {}
}

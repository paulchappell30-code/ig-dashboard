// Twelve Data Signal Engine
// Fetches RSI, MACD, Bollinger Bands from Twelve Data free API
// Called by autotrade engine to get higher quality signals
// GET /api/indicators?symbol=FTSE&interval=1h
const fetch = require('node-fetch');

// Map our instrument names to Twelve Data symbols
const TD_SYMBOLS = {
  'FTSE 100':  'SPX500',      // Twelve Data uses SPX as proxy - or use IX:FTSE
  'S&P 500':   'SPX500',
  'DAX 40':    'DAX40',
  'Dow Jones': 'DJI',
  'Brent Oil': 'BCO/USD',
  'GBP/USD':   'GBP/USD',
  'EUR/USD':   'EUR/USD',
  'USD/JPY':   'USD/JPY',
};

// Better symbol mapping for indices
const TD_SYMBOL_MAP = {
  'IX.D.FTSE.DAILY.IP':  'IXIC',      // Use FTSE via Twelve Data
  'IX.D.SPTRD.DAILY.IP': 'SPX',
  'IX.D.DAX.DAILY.IP':   'DAX',
  'IX.D.DOW.DAILY.IP':   'DJI',
  'CC.D.LCO.USS.IP':     'BCO/USD',
  'CS.D.GBPUSD.MINI.IP': 'GBP/USD',
  'CS.D.EURUSD.MINI.IP': 'EUR/USD',
  'CS.D.USDJPY.MINI.IP': 'USD/JPY',
};

// Intraday symbols that work well on Twelve Data free tier
const INTRADAY_SYMBOLS = {
  'FTSE 100':  'IX:FTSE',
  'S&P 500':   'SPX',
  'DAX 40':    'DAX',
  'Dow Jones': 'DJI',
  'Brent Oil': 'BCO/USD',
  'GBP/USD':   'GBP/USD',
  'EUR/USD':   'EUR/USD',
  'USD/JPY':   'USD/JPY',
};

const EVENT_IMPACT = {
  // UK events → FTSE, GBP/USD
  'UK': {
    instruments: ['FTSE 100', 'GBP/USD'],
    events: ['CPI', 'GDP', 'Unemployment', 'Retail Sales', 'PMI', 'Interest Rate', 'BoE', 'Inflation']
  },
  // US events → S&P, Dow, Nasdaq, GBP/USD, EUR/USD, USD/JPY
  'US': {
    instruments: ['S&P 500', 'Dow Jones', 'Nasdaq', 'GBP/USD', 'EUR/USD', 'USD/JPY'],
    events: ['NFP', 'Non-Farm', 'CPI', 'GDP', 'Fed', 'FOMC', 'Unemployment', 'Retail', 'PMI', 'ISM']
  },
  // EU events → DAX, CAC, EUR/USD
  'EU': {
    instruments: ['DAX 40', 'CAC 40', 'EUR/USD'],
    events: ['ECB', 'CPI', 'GDP', 'PMI', 'Unemployment', 'ZEW', 'IFO']
  },
  // Germany events → DAX
  'DE': {
    instruments: ['DAX 40'],
    events: ['CPI', 'GDP', 'PMI', 'IFO', 'ZEW', 'Unemployment']
  },
  // Japan events → Nikkei, USD/JPY
  'JP': {
    instruments: ['Nikkei 225', 'USD/JPY'],
    events: ['BOJ', 'CPI', 'GDP', 'Tankan', 'PMI']
  },
};

const BEAT_DIRECTION = {
  // Good US data = stocks up, USD up (bad for GBP/USD, EUR/USD)
  'S&P 500':   { beat: 'BUY',  miss: 'SELL' },
  'Dow Jones': { beat: 'BUY',  miss: 'SELL' },
  'Nasdaq':    { beat: 'BUY',  miss: 'SELL' },
  'FTSE 100':  { beat: 'BUY',  miss: 'SELL' },
  'DAX 40':    { beat: 'BUY',  miss: 'SELL' },
  'CAC 40':    { beat: 'BUY',  miss: 'SELL' },
  'Nikkei 225':{ beat: 'BUY',  miss: 'SELL' },
  // For FX: good UK/EU data = currency up vs USD
  'GBP/USD':   { beat: 'BUY',  miss: 'SELL' }, // UK beat = GBP up
  'EUR/USD':   { beat: 'BUY',  miss: 'SELL' }, // EU beat = EUR up
  'USD/JPY':   { beat: 'SELL', miss: 'BUY'  }, // JPY safe haven: risk-off = JPY up = pair down
  'Brent Oil': { beat: 'BUY',  miss: 'SELL' },
  'Gold':      { beat: 'SELL', miss: 'BUY'  }, // Good data = less safe haven demand
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const apiKey = process.env.TWELVE_DATA_KEY;
  if (!apiKey) {
    return res.status(200).json({
      error: 'TWELVE_DATA_KEY not configured',
      hint: 'Get a free API key at twelvedata.com and add TWELVE_DATA_KEY to Vercel environment variables'
    });
  }

  // GET — fetch indicators for a single instrument

  // ─── CALENDAR ACTIONS ────────────────────────────────────────────────────────
  if (req.query.action === 'upcoming' || req.query.action === 'recent' || req.query.action === 'surprise') {
    const finnhubKey = process.env.FINNHUB_API_KEY;
    if (!finnhubKey) {
      return res.status(200).json({
        configured: false,
        message: 'Add FINNHUB_API_KEY to Vercel (free at finnhub.io)',
        surprises: {}, upcomingBlocks: []
      });
    }

    const now = new Date();
    const todayStr = now.toISOString().split('T')[0];
    const tomorrowStr = new Date(now.getTime() + 86400000).toISOString().split('T')[0];
    const action = req.query.action;

    try {
      const calRes = await fetch(`https://finnhub.io/api/v1/calendar/economic?from=${todayStr}&to=${tomorrowStr}&token=${finnhubKey}`);
      const calData = await calRes.json();
      const events = calData.economicCalendar || [];

      if (action === 'upcoming') {
        const upcoming = events
          .filter(e => e.impact === 'high' && new Date(e.time) > now)
          .map(e => ({ time:e.time, event:e.event, country:e.country, impact:e.impact, forecast:e.estimate, previous:e.prev, minutesUntil:Math.round((new Date(e.time)-now)/60000) }))
          .sort((a,b) => a.minutesUntil - b.minutesUntil);
        return res.status(200).json({ upcoming, count: upcoming.length });
      }

      if (action === 'recent') {
        const recentCutoff = new Date(now.getTime() - 4*60*60*1000);
        const recent = events
          .filter(e => e.actual != null && new Date(e.time) > recentCutoff && new Date(e.time) < now)
          .map(e => ({ time:e.time, event:e.event, country:e.country, impact:e.impact, actual:e.actual, forecast:e.estimate, previous:e.prev, beat:e.estimate?(parseFloat(e.actual)>parseFloat(e.estimate)):null }));
        return res.status(200).json({ recent, count: recent.length });
      }

      if (action === 'surprise') {
        const surprises = {};
        const upcomingBlocks = [];
        const log = [];
        const recentCutoff = new Date(now.getTime() - 4*60*60*1000);

        for (const event of events) {
          const eventTime = new Date(event.time);
          const isReleased = eventTime < now && eventTime > recentCutoff;
          const isUpcoming = eventTime > now && eventTime < new Date(now.getTime() + 30*60*1000);

          if (event.impact === 'high' && isUpcoming) {
            upcomingBlocks.push({ time:event.time, event:event.event, country:event.country, minutesUntil:Math.round((eventTime-now)/60000) });
          }

          if (isReleased && event.actual != null && event.estimate != null) {
            const actual = parseFloat(event.actual);
            const estimate = parseFloat(event.estimate);
            if (isNaN(actual) || isNaN(estimate)) continue;
            const surprise = actual - estimate;
            const pctSurprise = estimate !== 0 ? (surprise/Math.abs(estimate))*100 : 0;

            let country = null;
            if (['United States','US'].includes(event.country)) country = 'US';
            else if (['United Kingdom','UK'].includes(event.country)) country = 'UK';
            else if (['European Union','Euro Area'].includes(event.country)) country = 'EU';
            else if (event.country === 'Germany') country = 'DE';
            else if (event.country === 'Japan') country = 'JP';
            if (!country || !EVENT_IMPACT[country] || event.impact !== 'high') continue;

            const isBeat = surprise > 0;
            const magnitude = Math.min(3, Math.abs(pctSurprise) > 20 ? 3 : Math.abs(pctSurprise) > 10 ? 2 : 1);
            log.push(`${event.event} (${country}): ${isBeat?'BEAT':'MISS'} by ${pctSurprise.toFixed(1)}%`);

            for (const instr of EVENT_IMPACT[country].instruments) {
              const dir = BEAT_DIRECTION[instr];
              if (!dir) continue;
              const scoreAdj = isBeat ? (dir.beat==='BUY'?magnitude:-magnitude) : (dir.miss==='SELL'?-magnitude:magnitude);
              surprises[instr] = (surprises[instr]||0) + scoreAdj;
            }
          }
        }

        return res.status(200).json({
          surprises, upcomingBlocks,
          shouldBlock: upcomingBlocks.length > 0,
          nextBlock: upcomingBlocks[0] || null,
          log, eventsProcessed: events.length,
          configured: true, time: now.toISOString()
        });
      }
    } catch(e) {
      return res.status(200).json({ surprises:{}, upcomingBlocks:[], error:e.message, configured:true });
    }
  }

  if (req.method === 'GET') {
    // Bulk price fetch for watchlist
    if (req.query.action === 'prices') {
      const symbols = (req.query.symbols || '').split(',').filter(Boolean).slice(0, 12);
      if (!symbols.length) return res.status(400).json({ error: 'symbols required' });
      // Cache prices for 5 minutes to avoid burning TD credits on every dashboard refresh
      const cacheKey = symbols.join(',');
      if (!global._priceCache) global._priceCache = {};
      const cached = global._priceCache[cacheKey];
      if (cached && Date.now() - cached.ts < 5 * 60 * 1000) {
        return res.status(200).json({ prices: cached.prices, cached: true });
      }
      try {
        const prices = {};
        // Batch fetch using Twelve Data price endpoint
        const symbolStr = symbols.map(s => encodeURIComponent(s.trim())).join('%2C');
        const res2 = await fetch(`https://api.twelvedata.com/price?symbol=${symbolStr}&apikey=${apiKey}`);
        const data = await res2.json();
        // Response is either {price: "123"} for single or {SYMBOL: {price: "123"}} for multiple
        if (data.price) {
          prices[symbols[0]] = data.price;
        } else {
          for (const [sym, val] of Object.entries(data)) {
            if (val && val.price) prices[sym] = val.price;
          }
        }
        global._priceCache[cacheKey] = { prices, ts: Date.now() };
        return res.status(200).json({ prices, count: Object.keys(prices).length });
      } catch(e) {
        return res.status(500).json({ error: e.message });
      }
    }

    const instrument = req.query.instrument || req.query.symbol || 'GBP/USD';
    const interval = req.query.interval || '1h';
    const symbol = INTRADAY_SYMBOLS[instrument] || instrument;

    try {
      const indicators = await fetchIndicators(symbol, interval, apiKey);
      return res.status(200).json({ instrument, symbol, interval, ...indicators });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // POST — fetch indicators for all instruments and return signal scores
  if (req.method === 'POST') {
    const interval = req.body?.interval || '1h';
    const log = [];
    const L = msg => { console.log('[TwelveData]', msg); log.push(msg); };

    L('Fetching indicators from Twelve Data — interval: ' + interval);

    const results = {};
    const signals = [];

    // Fetch with small delays to respect rate limits (8 calls/min on free tier)
    for (const [instr, symbol] of Object.entries(INTRADAY_SYMBOLS)) {
      try {
        const data = await fetchIndicators(symbol, interval, apiKey);
        results[instr] = data;

        if (data.error) {
          L(`${instr}: ${data.error}`);
          continue;
        }

        // Calculate signal score from Twelve Data indicators
        const score = scoreTwelveData(data, instr, L);
        results[instr].score = score;
        results[instr].direction = score >= 2 ? 'BUY' : score <= -2 ? 'SELL' : 'NEUTRAL';

        L(`${instr}: RSI=${data.rsi?.toFixed(1)} MACD=${data.macd?.toFixed(2)} Score=${score} → ${results[instr].direction}`);

        if (Math.abs(score) >= 2) {
          signals.push({
            instr, symbol,
            direction: results[instr].direction,
            score,
            rsi: data.rsi,
            macd: data.macd,
            macdSignal: data.macdSignal,
            bbUpper: data.bbUpper,
            bbLower: data.bbLower,
            bbMiddle: data.bbMiddle,
            currentPrice: data.currentPrice,
            source: 'TwelveData'
          });
        }

        // Respect rate limit — 8 requests per minute on free tier
        await new Promise(r => setTimeout(r, 8000));

      } catch(e) {
        L(`${instr}: error — ${e.message}`);
      }
    }

    L(`Signals found: ${signals.length}`);

    return res.status(200).json({
      success: true,
      interval,
      signals,
      allResults: results,
      log,
      time: new Date().toISOString()
    });
  }
};

async function fetchIndicators(symbol, interval, apiKey) {
  const base = 'https://api.twelvedata.com';

  // Fetch RSI, MACD, Bollinger Bands, and current price in parallel
  const [rsiRes, macdRes, bbRes, priceRes] = await Promise.all([
    fetch(`${base}/rsi?symbol=${encodeURIComponent(symbol)}&interval=${interval}&time_period=14&apikey=${apiKey}`),
    fetch(`${base}/macd?symbol=${encodeURIComponent(symbol)}&interval=${interval}&fast_period=12&slow_period=26&signal_period=9&apikey=${apiKey}`),
    fetch(`${base}/bbands?symbol=${encodeURIComponent(symbol)}&interval=${interval}&time_period=20&apikey=${apiKey}`),
    fetch(`${base}/price?symbol=${encodeURIComponent(symbol)}&apikey=${apiKey}`),
  ]);

  const [rsiData, macdData, bbData, priceData] = await Promise.all([
    rsiRes.json(), macdRes.json(), bbRes.json(), priceRes.json()
  ]);

  // Check for errors
  if (rsiData.status === 'error') {
    return { error: rsiData.message || 'API error', symbol };
  }

  const result = {
    symbol,
    rsi: parseFloat(rsiData.values?.[0]?.rsi) || null,
    macd: parseFloat(macdData.values?.[0]?.macd) || null,
    macdSignal: parseFloat(macdData.values?.[0]?.macd_signal) || null,
    macdHist: parseFloat(macdData.values?.[0]?.macd_hist) || null,
    bbUpper: parseFloat(bbData.values?.[0]?.upper_band) || null,
    bbMiddle: parseFloat(bbData.values?.[0]?.middle_band) || null,
    bbLower: parseFloat(bbData.values?.[0]?.lower_band) || null,
    currentPrice: parseFloat(priceData.price) || null,
    fetchedAt: new Date().toISOString(),
  };

  // Bollinger position
  if (result.currentPrice && result.bbUpper && result.bbLower) {
    const bbRange = result.bbUpper - result.bbLower;
    result.bbPosition = bbRange > 0
      ? ((result.currentPrice - result.bbLower) / bbRange * 100).toFixed(1)
      : 50;
    result.bbZone = result.currentPrice < result.bbLower ? 'below_lower'
      : result.currentPrice > result.bbUpper ? 'above_upper'
      : 'within';
  }

  // MACD crossover
  if (result.macd !== null && result.macdSignal !== null) {
    result.macdCrossover = result.macd > result.macdSignal ? 'bullish' : 'bearish';
  }

  return result;
}

function scoreTwelveData(data, instr, L) {
  let score = 0;

  // RSI scoring
  if (data.rsi !== null) {
    if (data.rsi < 25) { score += 4; }
    else if (data.rsi < 30) { score += 3; }
    else if (data.rsi < 40) { score += 2; }
    else if (data.rsi > 75) { score -= 4; }
    else if (data.rsi > 70) { score -= 3; }
    else if (data.rsi > 60) { score -= 2; }
  }

  // MACD scoring
  if (data.macd !== null && data.macdSignal !== null) {
    if (data.macd > data.macdSignal && data.macdHist > 0) { score += 2; }
    else if (data.macd > data.macdSignal) { score += 1; }
    else if (data.macd < data.macdSignal && data.macdHist < 0) { score -= 2; }
    else if (data.macd < data.macdSignal) { score -= 1; }
  }

  // Bollinger Bands scoring
  if (data.bbZone) {
    if (data.bbZone === 'below_lower') { score += 3; }
    else if (data.bbZone === 'above_upper') { score -= 3; }
    // Within bands — slight bias toward middle
    else if (data.bbPosition && parseFloat(data.bbPosition) < 30) { score += 1; }
    else if (data.bbPosition && parseFloat(data.bbPosition) > 70) { score -= 1; }
  }

  return score;
}

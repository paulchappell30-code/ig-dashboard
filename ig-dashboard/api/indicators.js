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
  if (req.method === 'GET') {
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

// Economic Calendar Engine
// Fetches high-impact events, calculates surprise scores, adjusts trading signals
// GET /api/calendar?action=upcoming  — get upcoming events today
// GET /api/calendar?action=recent    — get recently released events with actual vs expected
// GET /api/calendar?action=surprise  — get surprise scores for signal adjustment
const fetch = require('node-fetch');

// Map economic events to affected instruments
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

// Which direction does a beat mean for each instrument?
// beat = actual > expected
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

  const action = req.query.action || 'surprise';
  const apiKey = process.env.FINNHUB_API_KEY;

  if (!apiKey) {
    // Return neutral scores if no API key configured
    return res.status(200).json({
      configured: false,
      message: 'Add FINNHUB_API_KEY to Vercel environment variables (free at finnhub.io)',
      surprises: {},
      upcomingBlocks: []
    });
  }

  const now = new Date();
  const todayStr = now.toISOString().split('T')[0];
  const tomorrowStr = new Date(now.getTime() + 86400000).toISOString().split('T')[0];

  try {
    // Fetch today's economic calendar from Finnhub
    const calRes = await fetch(
      `https://finnhub.io/api/v1/calendar/economic?from=${todayStr}&to=${tomorrowStr}&token=${apiKey}`
    );
    const calData = await calRes.json();
    const events = calData.economicCalendar || [];

    if (action === 'upcoming') {
      // Return upcoming high-impact events
      const upcoming = events
        .filter(e => e.impact === 'high' && new Date(e.time) > now)
        .map(e => ({
          time: e.time,
          event: e.event,
          country: e.country,
          impact: e.impact,
          forecast: e.estimate,
          previous: e.prev,
          minutesUntil: Math.round((new Date(e.time) - now) / 60000)
        }))
        .sort((a, b) => a.minutesUntil - b.minutesUntil);

      return res.status(200).json({ upcoming, count: upcoming.length });
    }

    if (action === 'recent') {
      // Return recently released events with actual values
      const recentCutoff = new Date(now.getTime() - 4 * 60 * 60 * 1000); // Last 4 hours
      const recent = events
        .filter(e => e.actual !== null && e.actual !== undefined && new Date(e.time) > recentCutoff && new Date(e.time) < now)
        .map(e => ({
          time: e.time,
          event: e.event,
          country: e.country,
          impact: e.impact,
          actual: e.actual,
          forecast: e.estimate,
          previous: e.prev,
          beat: e.estimate ? (parseFloat(e.actual) > parseFloat(e.estimate)) : null
        }));

      return res.status(200).json({ recent, count: recent.length });
    }

    if (action === 'surprise') {
      // Calculate surprise scores for signal adjustment
      const surprises = {}; // instrument -> score adjustment
      const upcomingBlocks = []; // times to block trading
      const log = [];

      // Process released events from last 4 hours
      const recentCutoff = new Date(now.getTime() - 4 * 60 * 60 * 1000);

      for (const event of events) {
        const eventTime = new Date(event.time);
        const isReleased = eventTime < now && eventTime > recentCutoff;
        const isUpcoming = eventTime > now && eventTime < new Date(now.getTime() + 30 * 60 * 1000);

        // Block window: ±10 minutes around high-impact events
        if (event.impact === 'high' && isUpcoming) {
          upcomingBlocks.push({
            time: event.time,
            event: event.event,
            country: event.country,
            minutesUntil: Math.round((eventTime - now) / 60000)
          });
        }

        // Calculate surprise score for released events
        if (isReleased && event.actual != null && event.estimate != null) {
          const actual = parseFloat(event.actual);
          const estimate = parseFloat(event.estimate);
          if (isNaN(actual) || isNaN(estimate)) continue;

          const surprise = actual - estimate;
          const pctSurprise = estimate !== 0 ? (surprise / Math.abs(estimate)) * 100 : 0;

          // Find which country this maps to
          let country = null;
          if (event.country === 'United States' || event.country === 'US') country = 'US';
          else if (event.country === 'United Kingdom' || event.country === 'UK') country = 'UK';
          else if (event.country === 'European Union' || event.country === 'Euro Area') country = 'EU';
          else if (event.country === 'Germany') country = 'DE';
          else if (event.country === 'Japan') country = 'JP';

          if (!country || !EVENT_IMPACT[country]) continue;

          // Only process high-impact events
          if (event.impact !== 'high') continue;

          const isBeat = surprise > 0;
          const magnitude = Math.min(3, Math.abs(pctSurprise) > 20 ? 3 : Math.abs(pctSurprise) > 10 ? 2 : 1);

          log.push(`${event.event} (${country}): actual=${actual} vs forecast=${estimate} → ${isBeat ? 'BEAT' : 'MISS'} by ${pctSurprise.toFixed(1)}% (magnitude ${magnitude})`);

          // Apply score to affected instruments
          for (const instr of EVENT_IMPACT[country].instruments) {
            const direction = BEAT_DIRECTION[instr];
            if (!direction) continue;

            const scoreAdj = isBeat
              ? (direction.beat === 'BUY' ? magnitude : -magnitude)
              : (direction.miss === 'SELL' ? -magnitude : magnitude);

            surprises[instr] = (surprises[instr] || 0) + scoreAdj;
            log.push(`  → ${instr}: ${scoreAdj > 0 ? '+' : ''}${scoreAdj}`);
          }
        }
      }

      // Decay effect — events older than 2 hours have half impact
      const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);
      for (const event of events) {
        const eventTime = new Date(event.time);
        if (eventTime < twoHoursAgo && eventTime > recentCutoff) {
          // Halve the scores for older events
          for (const instr of Object.keys(surprises)) {
            surprises[instr] = Math.round(surprises[instr] * 0.5);
          }
        }
      }

      return res.status(200).json({
        surprises,
        upcomingBlocks,
        shouldBlock: upcomingBlocks.length > 0,
        nextBlock: upcomingBlocks[0] || null,
        log,
        eventsProcessed: events.length,
        time: now.toISOString()
      });
    }

  } catch(e) {
    console.error('[Calendar]', e.message);
    return res.status(200).json({ surprises: {}, upcomingBlocks: [], error: e.message });
  }

  return res.status(400).json({ error: 'Unknown action' });
};

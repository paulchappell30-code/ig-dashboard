// Automated Trading Engine v4
// Features: Price history DB, regime detection, news sentiment, time filter,
// active position management, Kelly sizing, portfolio heat, sentiment divergence
const fetch = require('node-fetch');
const TD_CACHE_TTL = 60 * 60 * 1000; // 60 min Twelve Data cache TTL — DB-backed

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

// Maps trading epic -> DB stored epic (candles stored under old MINI epics)
const DB_EPIC_MAP = {
  'CS.D.GBPUSD.TODAY.IP': 'CS.D.GBPUSD.MINI.IP',
  'CS.D.EURUSD.TODAY.IP': 'CS.D.EURUSD.MINI.IP',
  'CS.D.USDJPY.TODAY.IP': 'CS.D.USDJPY.MINI.IP',
  'CS.D.EURGBP.TODAY.IP': 'CS.D.EURGBP.MINI.IP',
};

// TODAY FX contracts are priced in pips*10000 not decimal
// ATR from DB candles is in decimal (e.g. 0.0055) — multiply by 10000 for stop points
const CONTRACT_PRICE_SCALE = {
  'CS.D.GBPUSD.TODAY.IP': 10000,
  'CS.D.EURUSD.TODAY.IP': 10000,
  'CS.D.USDJPY.TODAY.IP': 100,
  'CS.D.EURGBP.TODAY.IP': 10000,
};

const EPIC_MAP = {
  'FTSE 100':'IX.D.FTSE.DAILY.IP','S&P 500':'IX.D.SPTRD.DAILY.IP',
  'DAX 40':'IX.D.DAX.DAILY.IP','Dow Jones':'IX.D.DOW.DAILY.IP',
  'Brent Oil':'CC.D.LCO.USS.IP','GBP/USD':'CS.D.GBPUSD.TODAY.IP',
  'EUR/USD':'CS.D.EURUSD.TODAY.IP','USD/JPY':'CS.D.USDJPY.TODAY.IP',
  'CAC 40':'IX.D.CAC.DAILY.IP',
  'Nikkei 225':'IX.D.NIKKEI.DAILY.IP',
  'Nasdaq':'IX.D.NASDAQ.CASH.IP',
  'Gold':'CS.D.USCGC.TODAY.IP',
  'Silver':'CS.D.USCSI.TODAY.IP',
  'Copper':'CS.D.COPPER.TODAY.IP',
  'EUR/GBP':'CS.D.EURGBP.TODAY.IP',
};

const CORRELATION_GROUPS = {
  'IX.D.FTSE.DAILY.IP':'indices','IX.D.SPTRD.DAILY.IP':'indices',
  'IX.D.DAX.DAILY.IP':'indices','IX.D.DOW.DAILY.IP':'indices',
  'IX.D.CAC.DAILY.IP':'indices',
  'IX.D.NIKKEI.DAILY.IP':'indices',
  'IX.D.NASDAQ.CASH.IP':'indices',
  'CC.D.LCO.USS.IP':'commodities',
  'CS.D.USCSI.TODAY.IP':'commodities',
  'CS.D.COPPER.TODAY.IP':'commodities',
  'CS.D.EURGBP.TODAY.IP':'fx',
  'CS.D.USCGC.TODAY.IP':'commodities',
  'CS.D.GBPUSD.TODAY.IP':'fx','CS.D.EURUSD.TODAY.IP':'fx','CS.D.USDJPY.TODAY.IP':'fx',
};

const TRADING_HOURS = {
  indices:{open:7,close:21},    // Extended to 9pm UTC (10pm BST) — covers US session
  us_indices:{open:13,close:21}, // US markets only: 2:30pm-9pm BST
  nikkei:{open:0,close:6},
  commodities:{open:1,close:23},
  fx:{open:0,close:24},
};

const PREFERRED_WINDOWS = [{open:8,close:10},{open:13,close:15}];

const DEFAULT_CONFIG = {
  dailyProfitLock:2.0,dailyLossLimit:1.0,maxDrawdownPct:5.0,
  maxPositions:3,defaultSize:1,maxSizePerTrade:5,maxPortfolioHeat:300,
  requireAIConfirm:true,aiConfidenceMin:60,enabled:true,
  trailingStopPct:1.5,signalThreshold:2,useNewsFilter:true,
  usePreferredWindow:false,useKellyCriterion:true,winRateLookback:20,
  eodClose:true,eodCloseTime:{h:21,m:0},
};

const priceCache = {};
const CACHE_TTL = 20*60*1000;
const MAX_CANDLES_IG = 10;

// ── HELPER FUNCTIONS ──────────────────────────────────────────────────────────
async function saveToDb(type, data) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    await fetch(`${base}/api/db`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, data })
    });
  } catch(e) { console.log('[saveToDb]', e.message); }
}

async function sendNotify(type, subject, body) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    await fetch(`${base}/api/notify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, subject, body })
    });
  } catch(e) { console.log('[sendNotify]', e.message); }
}

async function closeAll(igBase, igH) {
  try {
    const pr = await fetch(`${igBase}/positions`, { headers: { ...igH, 'Version': '1' } });
    const pd = await pr.json();
    const positions = pd.positions || [];
    let closed = 0;
    for (const p of positions) {
      try {
        const epic = p.market.epic;
        const dir = p.position.direction === 'BUY' ? 'SELL' : 'BUY';
        const size = p.position.dealSize || p.position.size;
        const ob = { epic, direction: dir, size, orderType: 'MARKET', expiry: 'DFB',
          guaranteedStop: false, forceOpen: false, currencyCode: 'GBP', dealType: 'SPREADBET' };
        const r = await fetch(`${igBase}/positions/otc`, {
          method: 'POST', headers: { ...igH, 'Version': '1' }, body: JSON.stringify(ob)
        });
        const d = await r.json();
        if (d.dealReference) closed++;
      } catch(e) { console.log('[closeAll]', e.message); }
    }
    return closed;
  } catch(e) { return 0; }
}

function getNewsAdj(instr, sentiment) {
  if (!sentiment) return 0;
  const s = sentiment.toLowerCase();
  const positive = ['strong', 'rally', 'surge', 'gain', 'rise', 'bullish', 'optimism', 'growth'];
  const negative = ['weak', 'fall', 'drop', 'decline', 'bearish', 'concern', 'risk', 'tension', 'inflation'];
  let score = 0;
  positive.forEach(w => { if (s.includes(w)) score++; });
  negative.forEach(w => { if (s.includes(w)) score--; });
  return Math.max(-2, Math.min(2, Math.round(score / 2)));
}

async function getIGSentiment(epic, igBase, igH, L, base, instrName) {
  const ids = {
    'IX.D.FTSE.DAILY.IP': 'FTSE', 'IX.D.SPTRD.DAILY.IP': 'SPTRD',
    'IX.D.DAX.DAILY.IP': 'DAX', 'IX.D.DOW.DAILY.IP': 'DOW',
    'CC.D.LCO.USS.IP': 'LCO', 'CS.D.GBPUSD.TODAY.IP': 'GBPUSD',
    'CS.D.EURUSD.TODAY.IP': 'EURUSD', 'CS.D.USDJPY.TODAY.IP': 'USDJPY',
    'CS.D.USCGC.TODAY.IP': 'GOLD', 'CS.D.USCSI.TODAY.IP': 'SILVER',
    'CS.D.COPPER.TODAY.IP': 'COPPER', 'CS.D.EURGBP.TODAY.IP': 'EURGBP',
  };
  const id = ids[epic]; if (!id) return 0;
  try {
    const r = await fetch(`${igBase}/clientsentiment/${id}`, { headers: { ...igH, 'Version': '1' } });
    if (!r.ok) return 0;
    const d = await r.json();
    if (!d || (!d.clientSentimentList && !d.longPositionPercentage)) return 0;
    const sentiment = d.clientSentimentList?.[0] || d;
    const lp = sentiment.longPositionPercentage || d.longPositionPercentage || 50;
    const sp = sentiment.shortPositionPercentage || d.shortPositionPercentage || (100 - lp);
    // Save sentiment history to DB
    if (base && instrName) {
      fetch(`${base}/api/db`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'sentiment', data: { instrument: instrName, epic, longPct: lp, shortPct: sp } })
      }).catch(() => {});
    }
    if (lp > 70) { L(`${id}: ${lp}% long — contrarian SELL`); return -2; }
    if (lp > 60) { L(`${id}: ${lp}% long — mild contrarian SELL`); return -1; }
    if (lp < 30) { L(`${id}: ${lp}% long — contrarian BUY`); return 2; }
    if (lp < 40) { L(`${id}: ${lp}% long — mild contrarian BUY`); return 1; }
    return 0;
  } catch(e) { return 0; }
}


async function aiConfirm(sig, cfg, plPct, openCount, winRate, L) {
  L(`AI: ${sig.instr} ${sig.direction} (${sig.regime})...`);
  // Determine trade strategy type
  const isTrendPullback = sig.trendPullback && sig.trendPullback.signal > 0;
  const isBreakout = sig.breakoutSignal && sig.breakoutSignal.signal > 0;

  const regimeContext = isTrendPullback
  ? `TREND PULLBACK trade: ${sig.trendPullback.reason}.
STRATEGY: Trading WITH the ${sig.regime} — entering on a healthy pullback/bounce, NOT fading the trend.
APPROVAL RULE: APPROVE if (1) score ≥2, AND (2) MACD confirms trend direction, AND (3) RSI is in pullback zone (38-58 for uptrend BUY, 42-62 for downtrend SELL).
This is a multi-day trade — do not reject because momentum seems moderate. Pullbacks in trends have good expectancy.`
  : isBreakout
  ? `BREAKOUT trade: ${sig.breakoutSignal.reason}.
STRATEGY: Price has broken out of a 20-candle consolidation range with momentum confirmation.
APPROVAL RULE: APPROVE if (1) score ≥2, AND (2) RSI confirms direction (>52 for BUY, <48 for SELL), AND (3) the breakout level is meaningful (not noise).
This is a multi-day trade — breakouts can run significantly if genuine.`
  : sig.meanReversion && sig.tdRsi
  ? `MEAN REVERSION trade in ranging market.
PRIMARY SIGNAL: TD Hourly RSI ${sig.tdRsi.toFixed(1)} is ${sig.direction==='SELL'?'overbought':'oversold'} — this IS the entry trigger.
Daily RSI: ${sig.rsi.toFixed(1)} (context only — hourly RSI extreme is the signal).
APPROVAL RULE: APPROVE if (1) the triggering RSI is ≤33 (oversold BUY) or ≥67 (overbought SELL), AND (2) score ≥2, AND (3) momentum does not STRONGLY contradict (i.e. momentum < +2% for SELL or > -2% for BUY).
The RSI extreme justifies the trade. Daily RSI being neutral is acceptable — the hourly extreme is the mean reversion trigger on a shorter timeframe.`
  : sig.meanReversion
  ? `MEAN REVERSION trade: Daily RSI ${sig.rsi.toFixed(1)} is ${sig.direction==='SELL'?'overbought (≥67)':'oversold (≤33)'} in ranging market — fading the RSI extreme. RSI ≥67 or ≤33 in a ranging regime IS the primary signal. APPROVE if score ≥2 and momentum does not strongly contradict.`
  : sig.regime==='ranging'
  ? `RANGING regime (non-mean-reversion): Only approve if score ≥6 AND RSI is extended (≥65 or ≤35) AND momentum confirms direction. Calendar surprise scores alone do not justify a trade without RSI confirmation. Reject neutral RSI trades.`
  : `TRENDING regime (${sig.regime}): Evaluate if direction aligns with trend and if entry timing is good.`;

  const prompt = `Trading risk manager. Approve this spread bet?
INSTRUMENT:${sig.instr} DIRECTION:${sig.direction} REGIME:${sig.regime}${sig.meanReversion?' [MEAN REVERSION]':''}
${sig.tdRsi?`TRIGGER: TD Hourly RSI ${sig.tdRsi.toFixed(1)} — THIS IS THE ENTRY SIGNAL (not the daily RSI)
Daily RSI: ${sig.rsi.toFixed(1)} (context only)`:`RSI (daily): ${sig.rsi.toFixed(1)}`}
SCORE:${sig.score} (raw:${sig.rawScore} news:${sig.newsAdj} sentiment:${sig.sentAdj} td:${sig.tdAdj||0})
TECHNICALS: SMA20/50:${sig.sma20.toFixed(0)}/${sig.sma50.toFixed(0)} MACD:${sig.macd.toFixed(4)} MOM:${sig.momentum.toFixed(2)}% BB:${sig.bbPos}
${sig.divergence&&sig.divergence.type!=='none'?`RSI DIVERGENCE: ${sig.divergence.type.toUpperCase()} — ${sig.divergence.description} (strength:${sig.divergence.strength}/3)`:''}
ATR:${sig.atr.toFixed(0)} DATA:${sig.candles} candles from ${sig.src}
WinRate:${(winRate*100).toFixed(1)}% P&L:${plPct.toFixed(2)}% OpenPos:${openCount}/${cfg.maxPositions}
Reasons: ${sig.reasons.join(', ')}
CONTEXT: ${regimeContext}
Respond ONLY: {"approved":true,"confidence":72,"reasoning":"2-3 sentences"}`;

  const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
  const r = await fetch(`${base}/api/claude`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: 200,
      messages: [{ role: 'user', content: prompt }] })
  });
  const data = await r.json();
  const text = data.content?.[0]?.text || '{"approved":false,"confidence":0,"reasoning":"No response"}';
  const result = JSON.parse(text.replace(/```json|```/g,'').trim());
  const icon = result.approved ? '✅' : '❌';
  L(`AI:${icon}(${result.confidence}%) ${result.reasoning}`);
  return { approved: result.approved, confidence: result.confidence, reasoning: result.reasoning };
}


async function fetchNews(L) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    // Use Finnhub calendar endpoint for news — no TD credits consumed
    const today = new Date().toISOString().split('T')[0];
    const r = await fetch(`${base}/api/indicators?action=recent&date=${today}`, { timeout: 5000 });
    if (!r.ok) return null;
    const d = await r.json();
    if (d.summary) return d.summary;
    // Fallback: summarise recent events as sentiment string
    const events = d.events || [];
    if (!events.length) return null;
    const pos = events.filter(e => (e.actual||0) > (e.forecast||0)).length;
    const neg = events.filter(e => (e.actual||0) < (e.forecast||0)).length;
    if (pos > neg) return 'Positive economic data beats supporting risk-on sentiment.';
    if (neg > pos) return 'Negative economic data misses weighing on risk sentiment.';
    return 'Mixed economic data with no clear directional bias.';
  } catch(e) { L('News fetch failed: ' + e.message); return null; }
}

async function managePositions(openPos, igBase, igH, cfg, balance, L) {
  for (const p of openPos) {
    try {
      const epic = p.market.epic;
      const dir = p.position.direction;
      const sz = p.position.dealSize || p.position.size || 1;
      const openLevel = p.position.openLevel;
      const current = p.market.bid || openLevel;
      const upl = dir === 'BUY' ? (current - openLevel) * sz : (openLevel - current) * sz;

      // Partial close: if profit >= 1x ATR close 50%
      const dbEpic = DB_EPIC_MAP[epic] || epic;
      const closes = await getDbPrices(dbEpic, 20, L) || [];
      const atr = closes.length >= 5 ? calcATR(closes) : 50;
      const atrProfit = atr * sz;

      if (upl > 0 && sz >= 0.01) {
        if (upl >= atrProfit && !p.position.partialClosed) {
          const halfSize = parseFloat((sz / 2).toFixed(2));
          if (halfSize >= 0.01) {
            L(`${p.market.instrumentName}: profit ${upl.toFixed(2)} >= 1x ATR ${atrProfit.toFixed(2)} — partial close £${halfSize}/pt`);
            try {
              const closeBody = { epic, direction: dir === 'BUY' ? 'SELL' : 'BUY',
                size: halfSize, orderType: 'MARKET', expiry: 'DFB',
                guaranteedStop: false, forceOpen: false, currencyCode: 'GBP', dealType: 'SPREADBET' };
              const cr = await fetch(`${igBase}/positions/otc`, {
                method: 'POST', headers: { ...igH, 'Version': '1' }, body: JSON.stringify(closeBody)
              });
              const cd = await cr.json();
              if (cd.dealReference) L(`Partial close confirmed: ${cd.dealReference}`);
            } catch(e) { L('Partial close error: ' + e.message); }
          }
        }
      }
    } catch(e) { L('Position manage error: ' + e.message); }
  }
}

function kellySize(winRate, balance, atr, price, cfg) {
  const w = Math.max(0.3, Math.min(0.8, winRate));
  const r = 1.5; // reward:risk ratio
  const kelly = Math.max(0, w - (1 - w) / r);
  const quarterKelly = kelly * 0.25;
  const riskAmt = balance * Math.min(0.02, quarterKelly);
  const stopPts = Math.max(10, atr * 1.5);
  return Math.max(0.01, Math.min(parseFloat((riskAmt / stopPts).toFixed(2)), cfg.maxSizePerTrade));
}

function calcSMA(closes, period) {
  const n = Math.min(period, closes.length);
  return closes.slice(-n).reduce((a, b) => a + b, 0) / n;
}

function isMarketOpen(group) {
  if (group === 'fx') return true;
  const h = TRADING_HOURS[group] || { open: 7, close: 21 };
  const u = new Date().getUTCHours();
  return u >= h.open && u < h.close;
}

function isPreferredWindow() {
  const u = new Date().getUTCHours();
  return PREFERRED_WINDOWS.some(w => u >= w.open && u < w.close);
}

async function nearHighImpact(L) {
  const h = new Date().getUTCHours(), m = new Date().getUTCMinutes();
  const times = [{h:7,m:0},{h:8,m:30},{h:9,m:0},{h:12,m:30},{h:14,m:0},{h:18,m:0}];
  for (const t of times) {
    if (Math.abs((h*60+m) - (t.h*60+t.m)) <= 30) {
      L(`Calendar: near ${t.h}:${String(t.m).padStart(2,'0')} UTC`);
      return true;
    }
  }
  return false;
}

module.exports = async (req,res) => {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type,Authorization');
  if(req.method==='OPTIONS') return res.status(200).end();

  const cronSecret=process.env.CRON_SECRET||'';
  if(req.method==='POST'&&cronSecret){
    const token=(req.headers['authorization']||'').replace('Bearer ','').trim();
    const ref=req.headers['referer']||req.headers['origin']||'';
    const fromDash=ref.includes('vercel.app')||ref.includes('localhost');
    if(!fromDash&&token!==cronSecret) return res.status(401).json({error:'Unauthorised'});
  }

  const cfg={...DEFAULT_CONFIG,
    dailyProfitLock:parseFloat(process.env.DAILY_PROFIT_LOCK||DEFAULT_CONFIG.dailyProfitLock),
    dailyLossLimit:parseFloat(process.env.DAILY_LOSS_LIMIT||DEFAULT_CONFIG.dailyLossLimit),
    maxPositions:parseInt(process.env.MAX_POSITIONS||DEFAULT_CONFIG.maxPositions),
    defaultSize:parseInt(process.env.DEFAULT_SIZE||DEFAULT_CONFIG.defaultSize),
    maxSizePerTrade:parseInt(process.env.MAX_SIZE_PER_TRADE||DEFAULT_CONFIG.maxSizePerTrade),
    maxPortfolioHeat:parseFloat(process.env.MAX_PORTFOLIO_HEAT||DEFAULT_CONFIG.maxPortfolioHeat),
    requireAIConfirm:process.env.REQUIRE_AI_CONFIRM!=='false',
    aiConfidenceMin:parseInt(process.env.AI_CONFIDENCE_MIN||DEFAULT_CONFIG.aiConfidenceMin),
    enabled:process.env.AUTO_TRADING_ENABLED!=='false',
    trailingStopPct:parseFloat(process.env.TRAILING_STOP_PCT||DEFAULT_CONFIG.trailingStopPct),
    signalThreshold:parseInt(process.env.SIGNAL_THRESHOLD||DEFAULT_CONFIG.signalThreshold),
    useNewsFilter:process.env.USE_NEWS_FILTER!=='false',
    usePreferredWindow:process.env.USE_PREFERRED_WINDOW==='true',
    useKellyCriterion:process.env.USE_KELLY!=='false',
    eodClose:process.env.EOD_CLOSE!=='false',
  };

  // Load optimised params from DB if available
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    const optRes = await fetch(`${base}/api/db?action=optimize`);
    if (optRes.ok) {
      const optData = await optRes.json();
      if (optData.signal_threshold) cfg.signalThreshold = optData.signal_threshold;
      if (optData.ai_confidence_min) cfg.aiConfidenceMin = optData.ai_confidence_min;
    }
  } catch(e) { /* Use env var defaults if DB unavailable */ }

  if(req.method==='GET') return res.status(200).json({status:'Auto-trading engine v4 ready',cfg,version:'4.0',time:new Date().toISOString()});
  if(!cfg.enabled) return res.status(200).json({message:'Auto-trading disabled'});

  // Override config with values from dashboard UI request body
  const body=req.body||{};

  // Kill switch — return immediately without trading
  if(body.killSwitch === true){
    return res.status(200).json({action:'paused',message:'Trading paused by user',log:['Trading paused via dashboard']});
  }
  if(body.calendarEnabled!==undefined) cfg.calendarEnabled=!!body.calendarEnabled;
  if(body.requireAIConfirm!==undefined) cfg.requireAIConfirm=!!body.requireAIConfirm;
  if(body.signalThreshold!==undefined) cfg.signalThreshold=parseInt(body.signalThreshold);
  if(body.dailyProfitLock!==undefined) cfg.dailyProfitLock=parseFloat(body.dailyProfitLock);
  if(body.dailyLossLimit!==undefined) cfg.dailyLossLimit=parseFloat(body.dailyLossLimit);
  if(body.maxPositions!==undefined) cfg.maxPositions=parseInt(body.maxPositions);
  if(body.defaultSize!==undefined) cfg.defaultSize=parseInt(body.defaultSize);
  if(body.eodClose!==undefined) cfg.eodClose=!!body.eodClose;
  if(body.aiConfidenceMin!==undefined) cfg.aiConfidenceMin=parseInt(body.aiConfidenceMin);

  const igBase=IG_BASES[process.env.IG_ENV||'demo'];
  let cst,xst;
  const log=[];
  const L=msg=>{console.log('[ATv3]',msg);log.push(msg);};

  L('=== Engine v4 === '+new Date().toLocaleString('en-GB',{timeZone:'Europe/London'}));

  // Auth
  try{
    const ar=await fetch(`${igBase}/session`,{method:'POST',
      headers:{'Content-Type':'application/json','X-IG-API-KEY':process.env.IG_API_KEY||'','Version':'2'},
      body:JSON.stringify({identifier:process.env.IG_USERNAME,password:process.env.IG_PASSWORD})});
    if(!ar.ok) return res.status(500).json({error:'IG auth failed',log});
    cst=ar.headers.get('CST');xst=ar.headers.get('X-SECURITY-TOKEN');
    if(!cst) return res.status(500).json({error:'No CST',log});
    L('✅ Authenticated');
  }catch(e){return res.status(500).json({error:'Auth: '+e.message,log});}

  const igH={'Content-Type':'application/json','X-IG-API-KEY':process.env.IG_API_KEY||'','CST':cst,'X-SECURITY-TOKEN':xst};

  // Twelve Data — fetch indicators with 30-min cache to stay within 800 daily credits
  let tdSignals = {};
  if (process.env.TWELVE_DATA_KEY) {
      // Read TD signals from DB cache (populated by alert cron)
      // Autotrade never fetches TD directly — avoids per-minute rate limit conflicts
      let tdCacheHit = false;
      try {
        const {sql:tdSql} = require('@vercel/postgres');
        const tdCacheRow = await tdSql`SELECT details, created_at FROM engine_events WHERE event_type = 'td_cache' ORDER BY created_at DESC LIMIT 1`;
        if (tdCacheRow.rows.length > 0) {
          const age = Date.now() - new Date(tdCacheRow.rows[0].created_at).getTime();
          if (age < TD_CACHE_TTL) {
            tdSignals = JSON.parse(JSON.stringify(tdCacheRow.rows[0].details));
            tdCacheHit = true;
            L(`Twelve Data: using cached data (${Math.round(age/60000)}m old)`);
          } else {
            L(`Twelve Data: cache expired (${Math.round(age/60000)}m old) — alert cron will refresh`);
          }
        } else {
          L('Twelve Data: no cache yet — alert cron will populate');
        }
      } catch(e) { L('Twelve Data cache read error: ' + e.message); }
      if (!tdCacheHit) { L('Twelve Data: no cached data this run'); }
  } // end if TWELVE_DATA_KEY

  // Account
  let balance,dailyPL,available;
  try{
    const ar=await fetch(`${igBase}/accounts`,{headers:{...igH,'Version':'1'}});
    const ad=await ar.json();
    const acct=ad.accounts&&ad.accounts.find(a=>a.accountType==='SPREADBET');
    if(!acct){L('No spreadbet account');return res.status(200).json({log});}
    balance=acct.balance.balance;dailyPL=acct.balance.profitLoss;available=acct.balance.available;
    L(`Account: £${balance} | P&L: £${dailyPL} | Available: £${available}`);
    await saveToDb('equity_snapshot',{balance,profitLoss:dailyPL,available});
  }catch(e){L('Account error: '+e.message);return res.status(200).json({log});}

  const plPct=balance>0?(dailyPL/balance)*100:0;
  L(`P&L: ${plPct.toFixed(2)}% | Lock: +${cfg.dailyProfitLock}% | Limit: -${cfg.dailyLossLimit}%`);

  // Daily limits
  if(plPct<=-cfg.dailyLossLimit){
    L(`LOSS LIMIT HIT (${plPct.toFixed(2)}%) — closing all`);
    const closed=await closeAll(igBase,igH);
    await sendNotify('error','🛑 Daily Loss Limit Hit',`P&L: ${plPct.toFixed(2)}%\nClosed: ${closed} positions\nBalance: £${balance}`);
    return res.status(200).json({action:'loss_limit_hit',closed,log});
  }
  let profitLockActive = false;
  if(plPct>=cfg.dailyProfitLock){
    profitLockActive = true;
    L(`PROFIT LOCK ACTIVE (${plPct.toFixed(2)}%) — continuing with reduced risk (0.5%)`);
    // Only send email once per day
    try {
      const {sql:plSql} = require('@vercel/postgres');
      const todayStr = new Date().toISOString().split('T')[0];
      const alreadyNotified = await plSql`
        SELECT 1 FROM engine_events
        WHERE event_type = 'profit_lock_notified'
        AND created_at::date = ${todayStr}::date
        LIMIT 1
      `;
      if (alreadyNotified.rows.length === 0) {
        await sendNotify('dca','✅ Daily Profit Locked',`P&L: +${plPct.toFixed(2)}%\nTarget: +${cfg.dailyProfitLock}%\nContinuing to trade with 0.5% risk per position.`);
        await plSql`INSERT INTO engine_events (event_type, details, created_at) VALUES ('profit_lock_notified', ${JSON.stringify({pct:plPct.toFixed(2)})}, NOW())`;
        L('Profit lock email sent');
      } else {
        L('Profit lock email already sent today — skipping');
      }
    } catch(e) { L('Profit lock notify error: '+e.message); }
  }

  // Positions + portfolio heat
  let openPos=[],portfolioHeat=0;
  try{
    const pr=await fetch(`${igBase}/positions`,{headers:{...igH,'Version':'1'}});
    const pd=await pr.json();openPos=pd.positions||[];
    portfolioHeat=openPos.reduce((s,p)=>s+(p.position.size||1)*50,0);
    L(`Open: ${openPos.length}/${cfg.maxPositions} | Heat: £${portfolioHeat}/${cfg.maxPortfolioHeat}`);
    await managePositions(openPos,igBase,igH,cfg,balance,L);
  }catch(e){L('Positions error: '+e.message);}

  if(openPos.length>=cfg.maxPositions){L('Max positions');return res.status(200).json({action:'max_positions',log});}
  if(portfolioHeat>=cfg.maxPortfolioHeat){L('Portfolio heat limit');return res.status(200).json({action:'heat_limit',log});}

  // Time filter
  if(cfg.usePreferredWindow&&!isPreferredWindow()){
    L('Outside preferred window');return res.status(200).json({action:'outside_window',log});
  }

  // Calendar
  // Smart Calendar — fetch surprise scores and upcoming blocks
  let calSurprises = {};
  if(cfg.calendarEnabled){
    try {
      const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
      const calRes = await fetch(`${base}/api/indicators?action=surprise`);
      if(calRes.ok){
        const calData = await calRes.json();
        calSurprises = calData.surprises || {};
        // Block if high-impact event imminent (within 10 mins)
        if(calData.shouldBlock && calData.nextBlock && calData.nextBlock.minutesUntil <= 10){
          L(`Calendar: blocking — ${calData.nextBlock.event} in ${calData.nextBlock.minutesUntil} mins`);
          return res.status(200).json({action:'calendar_block',event:calData.nextBlock.event,log});
        }
        if(Object.keys(calSurprises).length > 0) L(`Calendar surprises active: ${JSON.stringify(calSurprises)}`);
      }
    } catch(e) {
      // Fall back to time-based blocking if calendar API unavailable
      if(await nearHighImpact(L)) return res.status(200).json({action:'calendar_block',log});
    }
  }

  // End-of-day position close
  if(cfg.eodClose){
    const now = new Date();
    const utcH = now.getUTCHours();
    const utcM = now.getUTCMinutes();
    const isEOD = utcH === cfg.eodCloseTime.h && utcM >= cfg.eodCloseTime.m && utcM < cfg.eodCloseTime.m + 5;
    const isFridayEOD = now.getUTCDay() === 5 && utcH >= 15 && utcH < 17; // Friday close earlier

    if(isEOD || isFridayEOD){
      // Close positions based on trade type:
      // hourly_mr → always close at EOD (intraday only)
      // daily_mr → only close if held >3 days OR it's Friday
      // directional → only close if Friday (let trends run)
      try {
        const posRes = await fetch(`${igBase}/positions`, {headers:{...igH,'Version':'1'}});
        const posData = await posRes.json();
        const positions = posData.positions || [];
        const isFriday = now.getUTCDay() === 5;

        // Fetch trade types from DB
        const {sql:eodSql} = require('@vercel/postgres');
        const tradeTypeRows = await eodSql`
          SELECT deal_id, details->>'tradeType' as trade_type, created_at
          FROM trades WHERE status = 'open'
        `.catch(() => ({ rows: [] }));
        const tradeTypeMap = {};
        tradeTypeRows.rows.forEach(r => { tradeTypeMap[r.deal_id] = r.trade_type || 'hourly_mr'; });

        const toClose = [];
        for(const p of positions){
          const dealId = p.position.dealId;
          const tradeType = tradeTypeMap[dealId] || 'hourly_mr';
          const openTime = new Date(p.position.createdDateUtc || Date.now());
          const daysHeld = (Date.now() - openTime.getTime()) / (1000*60*60*24);

          let shouldClose = false;
          if(tradeType === 'hourly_mr') shouldClose = true; // always close intraday
          else if(tradeType === 'daily_mr') shouldClose = daysHeld >= 3 || isFriday;
          else if(tradeType === 'directional') shouldClose = isFriday;
          else shouldClose = true; // default: close

          if(shouldClose){
            L(`EOD close: ${p.market.instrumentName} (${tradeType}, held ${daysHeld.toFixed(1)}d)`);
            toClose.push(p);
          } else {
            L(`EOD skip: ${p.market.instrumentName} (${tradeType}, held ${daysHeld.toFixed(1)}d — letting run)`);
          }
        }

        if(toClose.length > 0){
          L(`EOD close: closing ${toClose.length}/${positions.length} position(s)`);
          for(const p of toClose){
            const closeBody = {
              epic: p.market.epic,
              direction: p.position.direction === 'BUY' ? 'SELL' : 'BUY',
              size: p.position.size || p.position.dealSize,
              orderType: 'MARKET', expiry: 'DFB',
              guaranteedStop: false, forceOpen: false,
              currencyCode: 'GBP', dealType: 'SPREADBET'
            };
            try {
              const cr = await fetch(`${igBase}/positions/otc`, {method:'POST', headers:{...igH,'Version':'1'}, body:JSON.stringify(closeBody)});
              const cd = await cr.json();
              L(`EOD closed ${p.market.instrumentName}: ref ${cd.dealReference||'failed'}`);
              if(cd.dealReference){
                await saveToDb('trade_closed', {
                  dealId: p.position.dealId,
                  closeLevel: p.market.bid,
                  closeReason: isFridayEOD ? 'friday_eod' : 'eod_close',
                  profitLoss: p.position.direction === 'BUY'
                    ? (p.market.bid - p.position.openLevel) * (p.position.size || 1)
                    : (p.position.openLevel - p.market.offer) * (p.position.size || 1)
                });
              }
            } catch(e){ L(`EOD close error ${p.market.instrumentName}: ${e.message}`); }
          }
          await sendNotify('dca', '🔔 EOD: All positions closed',
            `End of day close executed.
${positions.map(p=>p.market.instrumentName).join(', ')}
Time: ${now.toLocaleString('en-GB',{timeZone:'Europe/London'})}`);
          return res.status(200).json({action:'eod_close', closed: positions.length, log});
        }
      } catch(e){ L(`EOD check error: ${e.message}`); }
    }
  }

  // News sentiment
  let newsSentiment={};
  if(cfg.useNewsFilter&&process.env.NEWS_API_KEY) newsSentiment=await fetchNews(L);

  // Kelly win rate
  let winRate=0.5;
  try{
    const base=process.env.PRODUCTION_URL||`https://${process.env.VERCEL_URL}`;
    const sr=await fetch(`${base}/api/db?action=stats`);
    const stats=await sr.json();
    if(stats.totalTrades>=5){winRate=stats.winRate/100;L(`Kelly win rate: ${stats.winRate}%`);}
  }catch(e){}

  // Signal evaluation
  const occupied=new Set(openPos.map(p=>CORRELATION_GROUPS[p.market.epic]).filter(Boolean));
  L('Occupied: '+(([...occupied].join(', '))||'none'));
  const signals=[];

  for(const instr of Object.keys(EPIC_MAP)){
    const epic=EPIC_MAP[instr];const grp=CORRELATION_GROUPS[epic];
    if(openPos.some(p=>p.market.epic===epic)){L(`${instr}: open`);continue;}
    if(grp&&occupied.has(grp)){L(`${instr}: group occupied`);continue;}
    // Nikkei has different hours
    const mktHrs = instr === 'Nikkei 225' ? 'nikkei' : grp;
    if(!isMarketOpen(mktHrs)){L(`${instr}: market closed`);continue;}

    try{
      // Try DB first, fall back to IG, then Twelve Data
      const dbEpic = DB_EPIC_MAP[epic] || epic;
      let closes=await getDbPrices(dbEpic,60,L);
      let src='DB';
      if(!closes||closes.length<5){
        // Only hit IG historical if not blocked — avoids burning allowance
        if(!process.env.IG_HISTORICAL_BLOCKED){
          const candles=await getIGPrices(epic,MAX_CANDLES_IG,igBase,igH);
          if(candles&&candles.length>=5){closes=candles.map(c=>c.close);src='IG';}
          else if(candles===null){L(`${instr}: IG historical blocked — skipping`);}
        }
      }

      const newsAdj=getNewsAdj(instr,newsSentiment);
      let sentAdj=0; try { sentAdj=await getIGSentiment(epic,igBase,igH,L)||0; } catch(e) { sentAdj=0; }
      let sc=0,regime='unknown',tdAdj=0;
      let divergence={type:'none',strength:0,description:'no data'};
      let trendPullback=null;
      let breakoutSignal=null;

      if(closes&&closes.length>=5){
        L(`${instr}: ${closes.length} candles from ${src}`);
        regime=detectRegime(closes);
        sc=calcScore(closes,regime);

        // Declare dir early so trend/breakout can set it before mean reversion check
        let dir = sc > 0 ? 'BUY' : 'SELL';

        // Trend pullback signal — overrides ranging score in trending markets
        if(regime === 'uptrend' || regime === 'downtrend'){
          trendPullback = calcTrendPullback(closes, regime);
          if(trendPullback.signal > 0){
            sc = trendPullback.signal;
            dir = trendPullback.direction;
            L(`${instr}: 📈 ${trendPullback.reason} (score:${sc})`);
          }
        }

        // Breakout signal — works in any regime
        if(regime === 'ranging'){
          breakoutSignal = calcBreakout(closes);
          if(breakoutSignal.signal > 0){
            sc = Math.max(sc, breakoutSignal.signal);
            dir = breakoutSignal.direction;
            L(`${instr}: 💥 ${breakoutSignal.reason} (score:${sc})`);
          }
        }

        // RSI Divergence detection — adds to score and passes to AI
        const divergence = detectRSIDivergence(closes);
        if(divergence.type==='bearish' && divergence.strength>0){
          sc -= divergence.strength; // Bearish divergence reduces score (supports SELL)
          L(`${instr}: ⚠️ Bearish RSI divergence (${divergence.description}) adj -${divergence.strength}`);
        } else if(divergence.type==='bullish' && divergence.strength>0){
          sc += divergence.strength; // Bullish divergence increases score (supports BUY)
          L(`${instr}: ⚠️ Bullish RSI divergence (${divergence.description}) adj +${divergence.strength}`);
        }

        if(tdSignals[instr]){
          const tdRsi = tdSignals[instr].rsi;
          const tdMacd = tdSignals[instr].macd;
          // Derive TD score from RSI (no pre-computed score in alert cache)
          let tdScore = 0;
          if(tdRsi !== null && !isNaN(tdRsi)){
            if(tdRsi <= 30) tdScore = 2;
            else if(tdRsi <= 35) tdScore = 1;
            else if(tdRsi >= 70) tdScore = -2;
            else if(tdRsi >= 65) tdScore = -1;
          }
          if(tdMacd !== null && !isNaN(tdMacd)){
            if(tdMacd > 0) tdScore += 1;
            else if(tdMacd < 0) tdScore -= 1;
          }
          tdAdj = tdScore;
          if(tdRsi !== null && !isNaN(tdRsi)) L(`${instr}: TD adj ${tdAdj} (RSI:${Math.round(tdRsi)})`);
        }
      } else if(tdSignals[instr]){
        // No candles — use Twelve Data as primary signal
        // Derive score from TD RSI when no candles available
        const tdRsiPrimary = tdSignals[instr].rsi;
        const tdMacdPrimary = tdSignals[instr].macd;
        sc = 0;
        if(tdRsiPrimary !== null && !isNaN(tdRsiPrimary)){
          if(tdRsiPrimary <= 30) sc = 2;
          else if(tdRsiPrimary <= 35) sc = 1;
          else if(tdRsiPrimary >= 70) sc = -2;
          else if(tdRsiPrimary >= 65) sc = -1;
        }
        if(tdMacdPrimary !== null && !isNaN(tdMacdPrimary)){
          sc += tdMacdPrimary > 0 ? 1 : -1;
        }
        regime = 'ranging'; // default when no candle data
        closes = []; // empty array to prevent crashes
        src = 'TwelveData';
        L(`${instr}: no candles — TD primary signal (RSI:${tdRsiPrimary?.toFixed(0)} score:${sc})`);
      } else {
        L(`${instr}: no data from any source — skip`);
        continue;
      }

      if(isNaN(sc)){L(`${instr}: invalid score — skip`);continue;}
      const safeNewsAdj = newsAdj||0; const safeSentAdj = sentAdj||0; const safeTdAdj = tdAdj||0;
      // Apply economic surprise score if available
      const calAdj = calSurprises[instr] || 0;
      if(calAdj !== 0) L(`${instr}: calendar surprise adj ${calAdj}`);
      const total=sc+safeNewsAdj+safeSentAdj+safeTdAdj+calAdj;
      L(`${instr}: score ${sc}+news${safeNewsAdj}+sent${safeSentAdj}+td${safeTdAdj}+cal${calAdj}=${total} regime:${regime}`);

      // Mean reversion check BEFORE score threshold — RSI extreme overrides low score
      const rsiPreCheck = calcRSI(closes);
      // Also check TD hourly RSI for mean reversion — catches intraday extremes
      const tdRsiForMR = tdSignals[instr]?.rsi || null;
      const effectiveRSI = (tdRsiForMR && (tdRsiForMR <= 33 || tdRsiForMR >= 67)) ? tdRsiForMR : rsiPreCheck;
      const isMeanReversionCandidate = regime === 'ranging' && (effectiveRSI >= 67 || effectiveRSI <= 33);
      if(!isMeanReversionCandidate && Math.abs(total)<=cfg.signalThreshold-1){L(`${instr}: score ${total} below threshold ${cfg.signalThreshold}`);continue;}
      if(isMeanReversionCandidate){L(`${instr}: mean reversion candidate RSI ${effectiveRSI.toFixed(1)}${tdRsiForMR===effectiveRSI?' (TD hourly)':' (daily)'} — bypassing score filter`);}

      // Mean reversion override for ranging regime
      const rsiForMR=effectiveRSI; // uses TD hourly if more extreme
      let meanReversion=false;
      // dir already declared above, update based on total score
      if(!trendPullback?.signal && !breakoutSignal?.signal) dir=total>0?'BUY':'SELL';
      if(regime==='ranging'){
        if(rsiForMR>=68){
          // Overbought in ranging — mean reversion SELL
          dir='SELL';
          meanReversion=true;
          L(`${instr}: mean reversion SELL (RSI:${rsiForMR.toFixed(1)} overbought in ranging)`);
        } else if(rsiForMR<=32){
          // Oversold in ranging — mean reversion BUY
          dir='BUY';
          meanReversion=true;
          L(`${instr}: mean reversion BUY (RSI:${rsiForMR.toFixed(1)} oversold in ranging)`);
        } else if(Math.abs(total)<3){
          // Ranging + weak signal + neutral RSI = skip
          L(`${instr}: ranging regime + neutral RSI (${rsiForMR.toFixed(1)}) + weak signal — skip`);
          continue;
        }
      }
      const atr=calcATR(closes);
      const sz=cfg.useKellyCriterion?kellySize(winRate,balance,atr,closes[closes.length-1],cfg):cfg.defaultSize;
      const rsi=calcRSI(closes);const sma20=calcSMA(closes,20);const sma50=calcSMA(closes,50);
      const macd=calcEMA(closes,12)-calcEMA(closes,26);
      const mom=closes.length>=10?((closes[closes.length-1]-closes[closes.length-10])/closes[closes.length-10])*100:0;
      const bb=calcBB(closes);
      const bbPos=closes[closes.length-1]<bb.lower?'below lower':closes[closes.length-1]>bb.upper?'above upper':'within';
      const reasons=[
        rsi<35?`RSI oversold(${rsi.toFixed(0)})`:rsi>65?`RSI overbought(${rsi.toFixed(0)})`:`RSI neutral(${rsi.toFixed(0)})`,
        sma20>sma50?'SMA bullish':'SMA bearish',
        macd>0?'MACD+':'MACD-',
        `Mom ${mom.toFixed(1)}%`,`BB:${bbPos}`,`Regime:${regime}`,
        newsAdj!==0?`News:${newsAdj>0?'+':''}${newsAdj}`:'',
        sentAdj!==0?`Sentiment:${sentAdj>0?'+':''}${sentAdj}`:'',
      ].filter(Boolean);

      signals.push({instr,epic,direction:dir,score:total,rawScore:sc,newsAdj,sentAdj,tdAdj,calAdj,regime,meanReversion,
        reasons,rsi,tdRsi:tdRsiForMR,effectiveRSI,sma20,sma50,macd,momentum:mom,lastClose:closes[closes.length-1],
        atr,suggestedSize:sz,bb,bbPos,src,candles:closes.length,divergence,trendPullback,breakoutSignal});
      // Note: group only marked occupied on open position, not on signal
    }catch(e){L(`${instr}: ${e.message}`);}
  }

  if(!signals.length){L('No signals');return res.status(200).json({action:'no_signals',log});}
  signals.sort((a,b)=>Math.abs(b.score)-Math.abs(a.score));

  // Keep only the best signal per correlation group
  // Mean reversion signals (RSI extreme) take priority over higher-scoring non-MR signals
  const seenGroups=new Set();
  // Sort: mean reversion first within score, then by absolute score
  signals.sort((a,b)=>{
    if(a.meanReversion&&!b.meanReversion)return -1;
    if(!a.meanReversion&&b.meanReversion)return 1;
    return Math.abs(b.score)-Math.abs(a.score);
  });
  const filteredSignals=signals.filter(sig=>{
    const grp=CORRELATION_GROUPS[sig.epic];
    if(!grp){return true;}
    if(seenGroups.has(grp))return false;
    seenGroups.add(grp);return true;
  });
  const mrCount=filteredSignals.filter(s=>s.meanReversion).length;
  L(`${signals.length} signal(s) — ${filteredSignals.length} after group filter (${mrCount} mean reversion)`);

  for(const sig of filteredSignals.slice(0,3)){
    let approved=!cfg.requireAIConfirm,confidence=100,reasoning='AI not required';
    if(cfg.requireAIConfirm){
      try{
        const air=await aiConfirm(sig,cfg,plPct,openPos.length,winRate,L);
        approved=air.approved;confidence=air.confidence;reasoning=air.reasoning;
      }catch(e){L('AI error — trade skipped: '+e.message);approved=false;confidence=0;reasoning='AI call failed';}
    }
    if(!approved){L(`${sig.instr}: AI rejected (${confidence}%)`);continue;}

    // Size using 1% risk / stop distance (min £0.01/pt)
    const stopPts = Math.max(5, (sig.atr||0) * 1.5);
    const riskAmt = balance * 0.01;
    const kellySz = parseFloat((riskAmt / stopPts).toFixed(2));
    const sz = Math.max(0.01, Math.min(kellySz, cfg.maxSizePerTrade));
    L(`${sig.instr}: size £${sz}/pt (risk £${riskAmt.toFixed(2)} / ${stopPts.toFixed(0)}pt stop)`);

    // Margin check — verify account has sufficient funds before placing
    try {
      const mktRes=await fetch(`${igBase}/markets/${sig.epic}`,{headers:{...igH,'Version':'3'}});
      if(mktRes.ok){
        const mktData=await mktRes.json();
        // Get margin % from marginDepositBands for our position size
        const bands=mktData.instrument?.marginDepositBands||[];
        const currentPrice=sig.lastClose||10000;
        const notional=currentPrice*sz;
        const band=bands.find(b=>notional>=b.min&&(b.max===null||notional<b.max))||bands[0]||{margin:5};
        const marginPct=band.margin/100;
        const requiredMargin=notional*marginPct;
        const minNotional=currentPrice*0.01; // Min 0.01 units
        const minMargin=minNotional*marginPct;
        L(`${sig.instr}: notional £${notional.toFixed(0)}, margin ${band.margin}%, need £${requiredMargin.toFixed(0)}, have £${available.toFixed(0)}`);
        if(requiredMargin>available*0.85){
          // Calculate max affordable size
          const maxAffordable=Math.floor((available*0.85)/(currentPrice*marginPct)*100)/100;
          const minSize=mktData.dealingRules?.minDealSize?.value||0.01;
          if(maxAffordable>=minSize){
            L(`${sig.instr}: reducing size to ${maxAffordable} units (max affordable)`);
            sig.suggestedSize=maxAffordable;
          }else{
            L(`${sig.instr}: cannot afford minimum size (need £${minMargin.toFixed(0)}) — skip`);
            continue;
          }
        }else{
          L(`${sig.instr}: margin OK ✅`);
        }
      }
    }catch(e){L('Margin check error: '+e.message);}

    // Sizing: risk amount = 1% of balance, size = riskAmt / stopDistance
    // This ensures size × stopDist always = 1% of account regardless of instrument
    // ATR from DB candles is already in contract price units (no scaling needed)
    // Tiered stop: hourly RSI MR uses tight 0.5x ATR, daily uses 1.5x ATR
    // Price scale only applies to DB candles (stored in decimal e.g. 1.3350)
    // IG candles are already in contract units (e.g. 13350) — no scaling needed
    const priceScale = sig.src === 'DB' ? (CONTRACT_PRICE_SCALE[sig.epic] || 1) : 1;
    const scaledATR = (sig.atr || 0) * priceScale;
    const isTrendTrade = sig.trendPullback?.signal > 0;
    const isBreakoutTrade = sig.breakoutSignal?.signal > 0;
    const atrMult = (sig.meanReversion && sig.tdRsi) ? 0.5 : (isTrendTrade || isBreakoutTrade) ? 2.0 : 1.5;
    const stopType = sig.meanReversion && sig.tdRsi ? 'hourly MR (0.5x ATR)'
      : sig.meanReversion ? 'daily MR (1.5x ATR)'
      : isTrendTrade ? 'trend pullback (2x ATR)'
      : isBreakoutTrade ? 'breakout (2x ATR)'
      : 'standard (1.5x ATR)';
    const minStop = Math.max(10, priceScale * 5);
    const tradeStopDist = scaledATR > 0 ? Math.max(minStop, Math.round(scaledATR * atrMult)) : minStop;
    const tradeRiskAmt = balance * (profitLockActive ? 0.005 : 0.01);
    const riskSz = parseFloat((tradeRiskAmt / tradeStopDist).toFixed(2));
    const finalSz = Math.max(0.01, Math.min(riskSz, cfg.maxSizePerTrade));
    const actualRisk = (finalSz * tradeStopDist).toFixed(2);
    L(`${sig.instr}: size £${finalSz}/pt (risk £${tradeRiskAmt.toFixed(2)} / ${tradeStopDist}pt stop — ${stopType})`);

    L(`${sig.instr}: size £${finalSz}/pt × ${tradeStopDist}pt stop = £${actualRisk} risk (${((parseFloat(actualRisk)/balance)*100).toFixed(1)}% of account)`);
    L(`Placing ${sig.direction} ${finalSz} on ${sig.instr} (regime:${sig.regime})...`);

    try{
      const ob={epic:sig.epic,direction:sig.direction,size:finalSz,orderType:'MARKET',
        expiry:'DFB',guaranteedStop:false,forceOpen:true,currencyCode:'GBP',dealType:'SPREADBET'};
      // Verify stop distance against IG's minimum for this instrument
      let finalStopDist = tradeStopDist;
      try {
        const mktR = await fetch(`${igBase}/markets/${sig.epic}`, {headers:{...igH,'Version':'3'}});
        if(mktR.ok){
          const mktD = await mktR.json();
          const minStop = mktD.dealingRules?.minNormalStopOrLimitDistance?.value || 0;
          const minStopUnit = mktD.dealingRules?.minNormalStopOrLimitDistance?.unit || 'POINTS';
          if(minStopUnit === 'POINTS' && minStop > finalStopDist){
            L(`Stop adjusted from ${finalStopDist} to ${minStop}pts (IG minimum)`);
            finalStopDist = Math.ceil(minStop * 1.1); // 10% above minimum
          }
          const minSize = mktD.dealingRules?.minDealSize?.value || 0.01;
          if(finalSz < minSize){
            L(`Size ${finalSz} below IG minimum ${minSize} — adjusting`);
            finalSz = minSize;
          }
        }
      } catch(e){ L(`Market rules check failed: ${e.message}`); }
      ob.stopDistance=finalStopDist;
      L(`Stop loss: ${finalStopDist}pts (${stopType}) — max loss £${(finalSz*finalStopDist).toFixed(2)}`);
      const trailDist=Math.max(minStop,Math.round(tradeStopDist*1.5));
      const trailIncrement=Math.max(1,Math.round(trailDist/5));
      ob.trailingStop=true;
      ob.trailingStopDistance=trailDist;
      ob.trailingStopIncrement=trailIncrement;
      L(`Trailing stop: ${trailDist}pts distance, ${trailIncrement}pt increment`);
      let ref;
      const or=await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(ob)});
      const od=await or.json();
      if(od.dealReference){ref=od.dealReference;}
      else{
        // Retry without trailing stop
        delete ob.trailingStop;delete ob.trailingStopDistance;delete ob.trailingStopIncrement;
        const or2=await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(ob)});
        const od2=await or2.json();
        if(od2.dealReference){ref=od2.dealReference;L(`Retry without trailing stop: ${od.errorCode||od.error||JSON.stringify(od).slice(0,100)}`);}
        else{L(`Retry failed: ${od2.errorCode||od2.error||JSON.stringify(od2).slice(0,100)}`);continue;}
      }
      const cr=await fetch(`${igBase}/confirms/${ref}`,{headers:{...igH,'Version':'1'}});
      const cd=await cr.json();
      if(cd.dealStatus==='ACCEPTED'){
        L(`✅ ACCEPTED ref:${ref} level:${cd.level}`);

        // Set trailing stop via separate PUT call after position confirmed
        // This works for account types where trailing stop on opening order is rejected
        if(cd.dealId){
          try{
            const tsBody={
              trailingStop:true,
              trailingStopDistance:trailDist,
              trailingStopIncrement:trailIncrement,
              stopLevel:null,
              limitLevel:null,
              limitedRiskPremium:null
            };
            const tsr=await fetch(`${igBase}/positions/otc/${cd.dealId}`,{
              method:'PUT',
              headers:{...igH,'Version':'2'},
              body:JSON.stringify(tsBody)
            });
            const tsd=await tsr.json();
            if(tsd.dealReference){
              L(`✅ Trailing stop set: ${trailDist}pts distance, ${trailIncrement}pt increment`);
            } else {
              L(`⚠️ Trailing stop PUT failed: ${tsd.errorCode||JSON.stringify(tsd).slice(0,80)}`);
              // Fallback: set fixed stop at stop distance
              L(`Setting fixed stop at ${tradeStopDist}pts instead`);
            }
          }catch(e){L(`Trailing stop error: ${e.message}`);}
        }

        // Determine trade type for EOD close logic
      const tradeType = sig.tdRsi ? 'hourly_mr' : sig.meanReversion ? 'daily_mr' : (sig.trendPullback?.signal>0) ? 'trend' : (sig.breakoutSignal?.signal>0) ? 'breakout' : 'directional';
      await saveToDb('trade_opened',{dealId:cd.dealId,dealReference:ref,instrument:sig.instr,epic:sig.epic,
          direction:sig.direction,size:finalSz,openLevel:cd.level,signalScore:sig.score,
          aiConfidence:confidence,aiReasoning:reasoning,signalReasons:sig.reasons.join(', '),
          regime:sig.regime,atr:sig.atr,stopDistance:tradeStopDist,tradeType});
        await sendNotify('trade',`🎯 Trade Placed: ${sig.instr} ${sig.direction}`,
          `Instrument: ${sig.instr}\nDirection: ${sig.direction}\nSize: £${finalSz}/pt\nLevel: ${cd.level}\nStop: ${tradeStopDist}pts\nMax Loss: £${actualRisk}\nAI: ${confidence}% — ${reasoning}\nRegime: ${sig.regime}\nScore: ${sig.score}\nType: ${tradeType} (${tradeType==='hourly_mr'?'closes tonight':tradeType==='daily_mr'?'holds up to 3 days':'holds until Friday'})`);
        return res.status(200).json({action:'trade_placed',instrument:sig.instr,direction:sig.direction,level:cd.level,size:finalSz,log});
      }else{
        L(`Rejected: ${cd.reason||cd.dealStatus}`);
        await sendNotify('error',`❌ Order Rejected: ${sig.instr}`,`Reason: ${cd.reason||cd.dealStatus}`);
      }
    }catch(e){L('Trade error: '+e.message);}
  }

  L('No trades placed');
  return res.status(200).json({action:'no_trades',signals:signals.length,log});
};

// ── PRICE DATA ────────────────────────────────────────────────────────────────
async function getDbPrices(epic,limit,L){
  try{
    const {sql}=require('@vercel/postgres');
    const r=await sql`SELECT close_price FROM price_history WHERE epic=${epic} AND resolution='DAY' AND close_price>0 ORDER BY candle_time DESC LIMIT ${limit}`;
    if(r.rows.length<5)return null;
    return r.rows.map(row=>parseFloat(row.close_price)).reverse();
  }catch(e){return null;}
}

async function getIGPrices(epic,count,igBase,igH){
  const key=`${epic}_DAY_${count}`;
  const c=priceCache[key];
  if(c&&Date.now()-c.ts<CACHE_TTL)return c.data;
  const r=await fetch(`${igBase}/prices/${epic}?resolution=DAY&max=${count}&pageSize=0`,{headers:{...igH,'Version':'3'}});
  if(r.status===403){console.log('[IG Historical] 403 blocked');return null;}
  if(!r.ok)return null;
  const d=await r.json();
  const candles=(d.prices||[]).filter(p=>p.closePrice?.bid>0).map(p=>({
    close:p.closePrice.bid,high:p.highPrice?.bid||p.closePrice.bid,
    low:p.lowPrice?.bid||p.closePrice.bid,open:p.openPrice?.bid||p.closePrice.bid
  }));
  if(candles.length>0)priceCache[key]={data:candles,ts:Date.now()};
  return candles.length>=5?candles:null;
}

// ── INDICATORS ────────────────────────────────────────────────────────────────
function calcRSI(closes,period=14){
  const n=closes.length;if(n<period+1)return 50;
  let g=0,l=0;
  for(let i=n-period;i<n;i++){const d=closes[i]-closes[i-1];if(d>0)g+=d;else l+=Math.abs(d);}
  const ag=g/period,al=l/period;
  return al===0?100:100-(100/(1+ag/al));
}

function calcEMA(closes,period){
  if(closes.length<period)return closes[closes.length-1];
  const k=2/(period+1);
  let ema=closes.slice(0,period).reduce((a,b)=>a+b,0)/period;
  for(let i=period;i<closes.length;i++)ema=closes[i]*k+ema*(1-k);
  return ema;
}

function calcScore(closes,regime){
  const n=closes.length;if(n<5)return 0;let s=0;
  const rsi=calcRSI(closes);
  if(rsi<35)s+=2;else if(rsi<45)s+=1;else if(rsi>65)s-=2;else if(rsi>55)s-=1;
  const sma5=closes.slice(-Math.min(5,n)).reduce((a,b)=>a+b,0)/Math.min(5,n);
  const sma10=closes.slice(-Math.min(10,n)).reduce((a,b)=>a+b,0)/Math.min(10,n);
  if(sma5>sma10)s+=1;else s-=1;
  const mom=n>=5?((closes[n-1]-closes[n-5])/closes[n-5])*100:0;
  if(mom>1)s+=1;else if(mom<-1)s-=1;
  return s;
}

function detectRegime(closes){
  const n=closes.length;if(n<10)return'ranging';
  
  // SMA-based trend detection
  const sma10 = closes.slice(-Math.min(10,n)).reduce((a,b)=>a+b,0)/Math.min(10,n);
  const sma20 = closes.slice(-Math.min(20,n)).reduce((a,b)=>a+b,0)/Math.min(20,n);
  const sma50 = closes.slice(-Math.min(50,n)).reduce((a,b)=>a+b,0)/Math.min(50,n);
  const price = closes[n-1];
  
  // Slope of SMA20 over last 5 candles
  const sma20_5ago = closes.slice(-Math.min(25,n),-5).slice(-Math.min(20,n-5)).reduce((a,b)=>a+b,0)/Math.min(20,n-5);
  const slopePct = ((sma20 - sma20_5ago) / sma20_5ago) * 100;
  
  // Consecutive higher highs / lower lows
  const recent10 = closes.slice(-Math.min(10,n));
  let higherHighs=0, lowerLows=0;
  for(let i=1;i<recent10.length;i++){
    if(recent10[i]>recent10[i-1])higherHighs++;
    else lowerLows++;
  }
  
  // Strong uptrend: price > SMA10 > SMA20, slope up, more higher highs
  if(price>sma10 && sma10>sma20 && slopePct>1.5 && higherHighs>lowerLows+2) return 'uptrend';
  
  // Strong downtrend: price < SMA10 < SMA20, slope down, more lower lows
  if(price<sma10 && sma10<sma20 && slopePct<-1.5 && lowerLows>higherHighs+2) return 'downtrend';
  
  // Weak trend signals — use old slope method as tiebreaker
  const mid=Math.floor(n/2);
  const h1=closes.slice(0,mid).reduce((a,b)=>a+b,0)/mid;
  const h2=closes.slice(mid).reduce((a,b)=>a+b,0)/(n-mid);
  const slope=((h2-h1)/h1)*100;
  const recent=closes.slice(-5);
  const rr=Math.max(...recent)-Math.min(...recent);
  const tr=Math.max(...closes)-Math.min(...closes);
  if(Math.abs(slope)>3&&rr/(tr||1)>0.3)return slope>0?'uptrend':'downtrend';
  
  return'ranging';
}

// ── TREND PULLBACK SIGNAL ────────────────────────────────────────────────────
// In uptrend: BUY when RSI pulls back to 40-55 (healthy dip, not oversold)
// In downtrend: SELL when RSI bounces to 45-60 (healthy bounce, not overbought)
function calcTrendPullback(closes, regime) {
  const n = closes.length;
  if(n < 10) return { signal: 0, reason: 'insufficient data' };
  if(regime !== 'uptrend' && regime !== 'downtrend') return { signal: 0, reason: 'not trending' };

  const rsi = calcRSI(closes);
  const sma10 = closes.slice(-Math.min(10,n)).reduce((a,b)=>a+b,0)/Math.min(10,n);
  const sma20 = closes.slice(-Math.min(20,n)).reduce((a,b)=>a+b,0)/Math.min(20,n);
  const price = closes[n-1];
  const mom = n>=5 ? ((price - closes[n-5])/closes[n-5])*100 : 0;
  const ema12 = calcEMA(closes,12);
  const ema26 = calcEMA(closes,26);
  const macd = ema12 - ema26;

  if(regime === 'uptrend') {
    // Pullback entry: RSI dipped to 40-55 (not oversold, just resting)
    // MACD still positive (trend intact), price near SMA10-SMA20 (support)
    const rsiPullback = rsi >= 38 && rsi <= 58;
    const nearSupport = price <= sma10 * 1.01; // within 1% of SMA10
    const macdPositive = macd > 0;
    const momentumRecovering = mom > -2; // not falling hard

    if(rsiPullback && nearSupport && macdPositive && momentumRecovering) {
      return { signal: 3, direction: 'BUY', reason: `Uptrend pullback: RSI ${rsi.toFixed(0)} near SMA10, MACD+` };
    }
    if(rsiPullback && macdPositive) {
      return { signal: 2, direction: 'BUY', reason: `Uptrend pullback: RSI ${rsi.toFixed(0)}, MACD+` };
    }
  }

  if(regime === 'downtrend') {
    // Bounce entry: RSI recovered to 42-62 (not overbought, just bouncing)
    // MACD still negative (trend intact), price near SMA10 (resistance)
    const rsiBounce = rsi >= 42 && rsi <= 62;
    const nearResistance = price >= sma10 * 0.99;
    const macdNegative = macd < 0;
    const momentumFading = mom < 2;

    if(rsiBounce && nearResistance && macdNegative && momentumFading) {
      return { signal: 3, direction: 'SELL', reason: `Downtrend bounce: RSI ${rsi.toFixed(0)} near SMA10, MACD-` };
    }
    if(rsiBounce && macdNegative) {
      return { signal: 2, direction: 'SELL', reason: `Downtrend bounce: RSI ${rsi.toFixed(0)}, MACD-` };
    }
  }

  return { signal: 0, reason: `${regime} but no pullback entry` };
}

// ── BREAKOUT SIGNAL ───────────────────────────────────────────────────────────
// Price closes above 20-candle high (bullish) or below 20-candle low (bearish)
// with RSI confirmation and ATR expansion
function calcBreakout(closes) {
  const n = closes.length;
  if(n < 22) return { signal: 0, reason: 'insufficient data' };

  const price = closes[n-1];
  const prevPrice = closes[n-2];
  const lookback = closes.slice(-21, -1); // last 20 candles excluding current
  const high20 = Math.max(...lookback);
  const low20 = Math.min(...lookback);
  const rsi = calcRSI(closes);
  const atr = calcATR(closes);
  const atrAvg = calcATR(closes.slice(0,-5)); // ATR excluding recent 5
  const atrExpansion = atr / (atrAvg || atr); // >1.3 = expanding volatility
  const priceMove = Math.abs(price - prevPrice);
  const movePct = (priceMove / prevPrice) * 100;

  // Bullish breakout: close above 20-candle high
  if(price > high20 && prevPrice <= high20) {
    const rsiConfirm = rsi > 52; // RSI crossing above midline
    const volatilityExpanding = atrExpansion > 1.1 || movePct > 0.5;
    const strength = (rsiConfirm ? 1 : 0) + (volatilityExpanding ? 1 : 0) + (movePct > 1 ? 1 : 0);
    if(strength >= 1) {
      return { signal: strength + 1, direction: 'BUY',
        reason: `Breakout above ${high20.toFixed(0)} | RSI ${rsi.toFixed(0)} | ATR ×${atrExpansion.toFixed(1)}` };
    }
  }

  // Bearish breakout: close below 20-candle low
  if(price < low20 && prevPrice >= low20) {
    const rsiConfirm = rsi < 48;
    const volatilityExpanding = atrExpansion > 1.1 || movePct > 0.5;
    const strength = (rsiConfirm ? 1 : 0) + (volatilityExpanding ? 1 : 0) + (movePct > 1 ? 1 : 0);
    if(strength >= 1) {
      return { signal: strength + 1, direction: 'SELL',
        reason: `Breakdown below ${low20.toFixed(0)} | RSI ${rsi.toFixed(0)} | ATR ×${atrExpansion.toFixed(1)}` };
    }
  }

  return { signal: 0, reason: 'no breakout' };
}

function calcBB(c,p=20){const n=Math.min(p,c.length);const sma=c.slice(-n).reduce((a,b)=>a+b,0)/n;
  const std=Math.sqrt(c.slice(-n).reduce((s,v)=>s+Math.pow(v-sma,2),0)/n);
  return{upper:sma+2*std,middle:sma,lower:sma-2*std};}

function calcATR(closes,p=14){
  const c=typeof closes[0]==='object'?closes:closes.map(v=>({close:v,high:v*1.001,low:v*0.999}));
  const n=Math.min(p,c.length-1);if(n<1)return 50;
  const trs=c.slice(-n).map((x,i,a)=>i===0?x.high-x.low:Math.max(x.high-x.low,Math.abs(x.high-a[i-1].close),Math.abs(x.low-a[i-1].close)));
  return trs.reduce((a,b)=>a+b,0)/trs.length;}

// RSI Divergence Detection
// Bearish divergence: price makes higher high but RSI makes lower high → weakening uptrend → SELL signal
// Bullish divergence: price makes lower low but RSI makes higher low → weakening downtrend → BUY signal
// Returns: { type: 'bearish'|'bullish'|'none', strength: 0-3, description: string }
function detectRSIDivergence(closes, lookback=10) {
  const n = closes.length;
  if (n < lookback + 5) return { type: 'none', strength: 0, description: 'insufficient data' };

  // Calculate RSI at each point over lookback window
  const rsiSeries = [];
  for (let i = n - lookback; i <= n; i++) {
    if (i >= 14) rsiSeries.push(calcRSI(closes.slice(0, i)));
  }
  if (rsiSeries.length < 4) return { type: 'none', strength: 0, description: 'insufficient RSI data' };

  const recentPrices = closes.slice(-lookback);
  const recentRSI = rsiSeries;

  // Find price highs and lows in the window
  const priceNow = recentPrices[recentPrices.length - 1];
  const priceMid = recentPrices[Math.floor(recentPrices.length / 2)];
  const rsiNow = recentRSI[recentRSI.length - 1];
  const rsiMid = recentRSI[Math.floor(recentRSI.length / 2)];

  const priceChange = ((priceNow - priceMid) / priceMid) * 100;
  const rsiChange = rsiNow - rsiMid;

  // Bearish divergence: price up, RSI down (weakening rally)
  if (priceChange > 0.5 && rsiChange < -3 && rsiNow > 55) {
    const strength = Math.min(3, Math.floor(Math.abs(rsiChange) / 3));
    return {
      type: 'bearish',
      strength,
      description: `Price +${priceChange.toFixed(1)}% but RSI ${rsiChange.toFixed(1)} pts (weakening rally)`
    };
  }

  // Bullish divergence: price down, RSI up (weakening selloff)
  if (priceChange < -0.5 && rsiChange > 3 && rsiNow < 45) {
    const strength = Math.min(3, Math.floor(Math.abs(rsiChange) / 3));
    return {
      type: 'bullish',
      strength,
      description: `Price ${priceChange.toFixed(1)}% but RSI +${rsiChange.toFixed(1)} pts (weakening selloff)`
    };
  }

  return { type: 'none', strength: 0, description: 'no divergence' };
}

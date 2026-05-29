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
  // MINI epics used for historical DB storage — Yahoo backfill uses TODAY epics directly
  // so these mappings only apply if MINI candles exist (older data)
  // 'CS.D.GBPUSD.TODAY.IP': 'CS.D.GBPUSD.MINI.IP',
  // 'CS.D.EURUSD.TODAY.IP': 'CS.D.EURUSD.MINI.IP',
  // 'CS.D.USDJPY.TODAY.IP': 'CS.D.USDJPY.MINI.IP',
  // 'CS.D.EURGBP.TODAY.IP': 'CS.D.EURGBP.MINI.IP',
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
  'IX.D.ASX.DAILY.IP':'indices',        // Australia 200
  'CC.D.LCO.USS.IP':'commodities',
  'CS.D.USCRUDE.TODAY.IP':'commodities', // WTI Oil
  'CS.D.USCSI.TODAY.IP':'commodities',
  'CS.D.COPPER.TODAY.IP':'commodities',
  'CS.D.EURGBP.TODAY.IP':'fx',
  'CS.D.USCGC.TODAY.IP':'commodities',
  'CS.D.GBPUSD.TODAY.IP':'fx','CS.D.EURUSD.TODAY.IP':'fx','CS.D.USDJPY.TODAY.IP':'fx',
  'CS.D.USDCAD.TODAY.IP':'fx',           // USD/CAD
};

// Pairs trading definitions — FX and indices only (live prices available)
const PAIRS_DEFINITIONS = [
  // Backtest: 92.3% WR, PF 13.0, +0.68% exp at 1.75σ entry — OPTIMAL
  { id:'gbpusd_eurusd', instrA:'GBP/USD', instrB:'EUR/USD',
    epicA:'CS.D.GBPUSD.TODAY.IP', epicB:'CS.D.EURUSD.TODAY.IP',
    minDays:60, lookbackDays:60, entryZ:1.75, exitZ:0.25, stopZ:3.5,
    description:'GBP/USD vs EUR/USD — dollar pairs, 92.3% WR at 1.75σ' },
  // Backtest: 60% WR, PF 1.68, +2.02% exp — grid search optimal 1.75σ/1.0σ
  // stopZ 2.5: tighter stop on volatile Brent/Gold ratio — PF drops significantly at 3.0σ
  { id:'brent_gold', instrA:'Brent Oil', instrB:'Gold',
    epicA:'CC.D.LCO.USS.IP', epicB:'CS.D.USCGC.TODAY.IP',
    minDays:60, lookbackDays:60, entryZ:1.75, exitZ:1.0, stopZ:2.5,
    description:'Brent vs Gold — hold until 1σ reversion, exit early loses edge' },
  // Backtest: 75% WR, +1.03% exp at 2.5σ — re-enabled at extreme entry only
  // stopZ 3.5: wider stop on slow-moving EUR triangular — 3.0σ stops out trades that revert
  { id:'eurusd_eurgbp', instrA:'EUR/USD', instrB:'EUR/GBP',
    epicA:'CS.D.EURUSD.TODAY.IP', epicB:'CS.D.EURGBP.TODAY.IP',
    minDays:60, lookbackDays:60, entryZ:2.5, exitZ:1.0, stopZ:3.5,
    description:'EUR triangular relationship — only trade extreme 2.5σ dislocations' },
  // ── GRID SEARCH DEPLOY TIER — added 27/05/2026 ────────────────────────────
  // Grid search: score 26.79 | 81.8% WR | 0.81% exp | 11 trades over 500d
  // lookbackDays 60: confirmed optimal in lookback sweep (score 26.79 vs 6.54 at 90d)
  { id:'dow_sp500', instrA:'Dow Jones', instrB:'S&P 500',
    epicA:'IX.D.DOW.DAILY.IP', epicB:'IX.D.SPTRD.DAILY.IP',
    minDays:60, lookbackDays:60, entryZ:2.25, exitZ:0.75, stopZ:3.0,
    description:'Dow vs S&P 500 — US mega-cap divergence, 81.8% WR at 2.25σ ⭐' },
  // Grid search: score 21.33 | 72.7% WR | 3.17% exp | 11 trades over 500d
  // lookbackDays 90: lookback sweep shows 90d score 39.66 vs 21.33 at 60d — macro cycle pair
  // dbPriceScale: Yahoo stores Copper in USc/lb (*100 vs IG pence/lb), Gold in USD/oz (×100 for pence)
  // Ratio is internally consistent for Z-score but live IG prices must be used for sizing
  { id:'copper_gold', instrA:'Copper', instrB:'Gold',
    epicA:'CS.D.COPPER.TODAY.IP', epicB:'CS.D.USCGC.TODAY.IP',
    minDays:90, lookbackDays:90, entryZ:2.0, exitZ:0.25, stopZ:3.0,
    dbPriceScaleA: 1.0,   // Copper DB price ~636 pence/lb — close to IG units, use as-is
    dbPriceScaleB: 0.073, // Gold DB price ~4524 USD/oz → ×0.073 ≈ 330 pence/oz (approx GBP/100)
    description:'Copper vs Gold — risk sentiment proxy, 72.7% WR, 3.17% exp ⭐' },
  // Grid search: score 17.10 | 70.6% WR | 1.44% exp | 17 trades over 500d
  // lookbackDays 60: confirmed optimal in lookback sweep (score 17.1 vs 1.27 at 90d)
  { id:'ftse_sp500', instrA:'FTSE 100', instrB:'S&P 500',
    epicA:'IX.D.FTSE.DAILY.IP', epicB:'IX.D.SPTRD.DAILY.IP',
    minDays:60, lookbackDays:60, entryZ:2.0, exitZ:1.0, stopZ:3.0,
    description:'FTSE vs S&P 500 — UK/US equity divergence, 70.6% WR at 2σ ⭐' },
  // ── UNIVERSE SEARCH DEPLOY TIER — added 27/05/2026 ───────────────────────
  // Universe search: score 107.3 | 82.8% WR | 2.71% exp | 29 trades over 500d
  // Highest statistically-reliable score in universe search — 29 trades is meaningful
  // Yen dynamics + BoJ policy divergence from Fed creates reliable mean-reversion
  { id:'nikkei_sp500', instrA:'Japan 225', instrB:'S&P 500',
    epicA:'IX.D.NIKKEI.DAILY.IP', epicB:'IX.D.SPTRD.DAILY.IP',
    minDays:60, lookbackDays:60, entryZ:1.25, exitZ:0.5, stopZ:3.0,
    description:'Nikkei vs S&P 500 — yen dynamics + risk divergence, 82.8% WR ⭐' },
  // Universe search: score 23.57 | 84.8% WR | 1.12% exp | 33 trades over 500d
  // Highest trade count (33) = most statistically reliable result in entire search
  // ASX time-zone gap creates frequent short-lived divergences from US session
  { id:'asx_sp500', instrA:'Australia 200', instrB:'S&P 500',
    epicA:'IX.D.ASX.DAILY.IP', epicB:'IX.D.SPTRD.DAILY.IP',
    minDays:45, lookbackDays:45, entryZ:1.5, exitZ:0.5, stopZ:3.0,
    description:'ASX vs S&P 500 — time-zone gap divergences, 84.8% WR, 33 trades ⭐' },
  // Universe search: score 57.2 | 86.7% WR | 2.73% exp | 15 trades over 500d
  // Strong cross-asset pair — CAD is a petrocurrency, USD/CAD moves inversely with oil
  // DB prices: USD/CAD from Yahoo (USDCAD=X), WTI from Yahoo (CL=F) — commodity pair
  { id:'usdcad_wti', instrA:'USD/CAD', instrB:'WTI Oil',
    epicA:'CS.D.USDCAD.TODAY.IP', epicB:'CS.D.USCRUDE.TODAY.IP',
    minDays:90, lookbackDays:90, entryZ:1.25, exitZ:0.75, stopZ:3.0,
    dbPriceScaleB: 0.787, // WTI Yahoo ~$88/bbl → ×0.787 ≈ 69 GBP/bbl (approx at 1.27 FX)
    description:'USD/CAD vs WTI — petrocurrency relationship, 86.7% WR ⭐' },
  // FTSE/DAX: 27.3% WR — permanently disabled
];

const TRADING_HOURS = {
  indices:{open:7,close:21},    // Extended to 9pm UTC (10pm BST) — covers US session
  us_indices:{open:13,close:21}, // US markets only: 2:30pm-9pm BST
  nikkei:{open:0,close:6},
  commodities:{open:1,close:23},
  fx:{open:0,close:24},
};

const PREFERRED_WINDOWS = [{open:8,close:10},{open:13,close:15}];

const DEFAULT_CONFIG = {
  dailyProfitLock:3.0,dailyLossLimit:1.0,maxDrawdownPct:5.0, // profit lock raised to 3% to accommodate pairs trades
  maxPositions:3,defaultSize:1,maxSizePerTrade:5,maxPortfolioHeat:300,
  requireAIConfirm:true,aiConfidenceMin:60,enabled:true,
  trailingStopPct:1.5,signalThreshold:2,useNewsFilter:true,
  usePreferredWindow:false,useKellyCriterion:true,winRateLookback:20,
  eodClose:true,eodCloseTime:{h:21,m:0},
  // Pairs trading config
  pairsEnabled:true,
  pairsZEntry:2.0,      // Z-score threshold to enter (editable via env var)
  pairsZStop:3.5,       // Z-score stop loss level
  pairsZTarget:0.5,     // Z-score target (close when reverts to this)
  pairsMaxSlots:2,      // Max simultaneous pairs trades — separate from directional slots
  pairsRiskPct:0.04,    // 4% risk per pairs trade (up from 1% — 90.9% WR, PF 12.16 justifies)
  // Note: pairs risk is split across two legs so effective single-leg risk is 2%
  // At 4% with £526 balance = £21 risk per pairs trade
};

const priceCache = {};
const CACHE_TTL = 20*60*1000;
const MAX_CANDLES_IG = 10;

// ── HELPER FUNCTIONS ──────────────────────────────────────────────────────────
async function saveToDb(type, data) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    const r = await fetch(`${base}/api/db`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, data })
    });
    if(!r.ok){
      const txt = await r.text().catch(()=>'');
      console.error(`[saveToDb] ${type} failed ${r.status}: ${txt.substring(0,200)}`);
    }
  } catch(e) { console.error('[saveToDb]', type, e.message); }
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

    // Only close engine-opened positions — skip manual trades
    let engineDealIds = new Set();
    try {
      const {sql: caSql} = require('@vercel/postgres');
      const [dirRows, pairRows] = await Promise.all([
        caSql`SELECT deal_id FROM trades WHERE status='open'`.catch(()=>({rows:[]})),
        caSql`SELECT deal_id_a, deal_id_b FROM pairs_trades WHERE status='open'`.catch(()=>({rows:[]})),
      ]);
      dirRows.rows.forEach(r => engineDealIds.add(r.deal_id));
      pairRows.rows.forEach(r => {
        if(r.deal_id_a) engineDealIds.add(r.deal_id_a);
        if(r.deal_id_b) engineDealIds.add(r.deal_id_b);
      });
    } catch(e) { console.log('[closeAll] DB filter error:', e.message); }

    let closed = 0;
    for (const p of positions) {
      try {
        const dealId = p.position.dealId;
        // Skip manual positions not tracked by engine
        if (engineDealIds.size > 0 && !engineDealIds.has(dealId)) {
          console.log('[closeAll] Skipping manual position:', p.market.instrumentName);
          continue;
        }
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
WinRate:${(winRate*100).toFixed(1)}% P&L:${plPct.toFixed(2)}% OpenPos:${typeof directionalOpen!=='undefined'?directionalOpen:openCount}/${cfg.maxPositions}
Reasons: ${sig.reasons.join(', ')}
${sig.pairsCtx ? `PAIRS CONTEXT: ${sig.instr} is ${sig.pairsCtx.signal.replace('_',' ')} vs ${sig.pairsCtx.partner} (Z-score: ${sig.pairsCtx.zscore.toFixed(2)}, ${sig.pairsCtx.n} days data)
${sig.pairsCtx.signal === 'cheap' && sig.direction === 'BUY' ? '✅ CONFLUENCE: Pairs signal CONFIRMS this BUY — instrument cheap vs partner' :
  sig.pairsCtx.signal === 'expensive' && sig.direction === 'SELL' ? '✅ CONFLUENCE: Pairs signal CONFIRMS this SELL — instrument expensive vs partner' :
  sig.pairsCtx.signal === 'expensive' && sig.direction === 'BUY' ? '⚠️ CONTRADICTION: Pairs signal OPPOSES this BUY — instrument already expensive vs partner' :
  sig.pairsCtx.signal === 'cheap' && sig.direction === 'SELL' ? '⚠️ CONTRADICTION: Pairs signal OPPOSES this SELL — instrument already cheap vs partner' :
  'Pairs signal neutral — no confluence or contradiction'}` : 'PAIRS CONTEXT: No pairs data for this instrument'}
CONTEXT: ${regimeContext}
Respond ONLY: {"approved":true,"confidence":72,"reasoning":"2-3 sentences"}`;

  const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
  const r = await fetch(`${base}/api/claude`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'claude-haiku-4-5', max_tokens: 150,
      messages: [{ role: 'user', content: prompt }] }),
  });
  if(!r.ok){ throw new Error(`Claude API ${r.status}`); }
  const data = await r.json();
  if(data.error){ throw new Error(`Claude error: ${data.error.message||JSON.stringify(data.error)}`); }
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
  const {sql: mgSql} = require('@vercel/postgres');
  const dealIds = openPos.map(p => p.position.dealId).filter(Boolean);

  // Build set of deal IDs that belong to open pairs trades — these are NEVER partially closed.
  // Partial closing one leg destroys the hedge and leaves a naked position on the other leg.
  // Pairs legs are held in full until the Z-score hits the exit threshold, then closed together.
  let pairsDealIds = new Set();
  try {
    if (dealIds.length > 0) {
      const pairRows = await mgSql`
        SELECT deal_id_a, deal_id_b FROM pairs_trades
        WHERE deal_id_a = ANY(${dealIds}) OR deal_id_b = ANY(${dealIds})`;
      pairRows.rows.forEach(r => {
        if(r.deal_id_a) pairsDealIds.add(r.deal_id_a);
        if(r.deal_id_b) pairsDealIds.add(r.deal_id_b);
      });
    }
  } catch(e) { L('Pairs deal ID check error: ' + e.message); }

  // Load partial_close flags for directional trades only
  let partialClosedSet = new Set();
  try {
    if (dealIds.length > 0) {
      const pcRows = await mgSql`
        SELECT deal_id FROM trades
        WHERE deal_id = ANY(${dealIds})
        AND partial_close = true`;
      pcRows.rows.forEach(r => partialClosedSet.add(r.deal_id));
    }
  } catch(e) { L('Partial close DB check error: ' + e.message); }

  for (const p of openPos) {
    try {
      const epic = p.market.epic;
      const dir = p.position.direction;
      const sz = p.position.dealSize || p.position.size || 1;
      const openLevel = p.position.openLevel;
      const current = p.market.bid || openLevel;
      const upl = dir === 'BUY' ? (current - openLevel) * sz : (openLevel - current) * sz;
      const dealId = p.position.dealId;

      // Skip partial close entirely for pairs legs — managed by pairs close logic instead
      if (pairsDealIds.has(dealId)) {
        if (upl > 0) L(`${p.market.instrumentName}: pairs leg +£${upl.toFixed(2)} — holding for Z-score exit`);
        continue;
      }

      // Partial close for directional trades only: if profit >= 3x ATR close 50%
      // 3x ATR gives the trade room to develop before locking in — 1x was too early
      const dbEpic = DB_EPIC_MAP[epic] || epic;
      const closes = await getDbPrices(dbEpic, 20, L) || [];
      const atr = closes.length >= 5 ? calcATR(closes) : 50;
      const atrProfit = atr * sz * 3;

      if (upl > 0 && sz >= 0.01) {
        if (upl >= atrProfit && !partialClosedSet.has(dealId)) {
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
              if (cd.dealReference) {
                L(`Partial close confirmed: ${cd.dealReference}`);
                try {
                  await mgSql`UPDATE trades SET partial_close = true WHERE deal_id = ${dealId}`;
                  partialClosedSet.add(dealId);
                } catch(e) { L('Partial close DB update error: ' + e.message); }
              }
            } catch(e) { L('Partial close error: ' + e.message); }
          }
        } else if (partialClosedSet.has(dealId)) {
          L(`${p.market.instrumentName}: partial close already done — holding remainder`);
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
  try { // Top-level catch to ensure JSON error response always

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
    pairsEnabled:process.env.PAIRS_ENABLED!=='false',
    pairsZEntry:parseFloat(process.env.PAIRS_Z_ENTRY||DEFAULT_CONFIG.pairsZEntry),
    pairsZStop:parseFloat(process.env.PAIRS_Z_STOP||DEFAULT_CONFIG.pairsZStop),
    pairsZTarget:parseFloat(process.env.PAIRS_Z_TARGET||DEFAULT_CONFIG.pairsZTarget),
    pairsMaxSlots:parseInt(process.env.PAIRS_MAX_SLOTS||DEFAULT_CONFIG.pairsMaxSlots),
    pairsRiskPct:parseFloat(process.env.PAIRS_RISK_PCT||DEFAULT_CONFIG.pairsRiskPct),
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

  const now = new Date(); // define once at top level for use throughout
  L('=== Engine v4 === '+now.toLocaleString('en-GB',{timeZone:'Europe/London'}));

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

    // Detect stop-loss triggered positions (in DB as open but not in IG positions)
    try {
      const {sql:slSql} = require('@vercel/postgres');
      const dbOpenTrades = await slSql`SELECT deal_id, epic, instrument FROM trades WHERE status = 'open'`;
      const igOpenIds = new Set(openPos.map(p => p.position.dealId));
      for(const dbTrade of dbOpenTrades.rows){
        if(!igOpenIds.has(dbTrade.deal_id)){
          // Position closed by IG (stop loss or limit hit) — record it
          L(`⚠️ ${dbTrade.instrument}: position closed by IG (stop loss) — recording and setting cooldown`);
          await slSql`UPDATE trades SET status='closed', close_reason='stop_loss', closed_at=NOW() WHERE deal_id=${dbTrade.deal_id}`;
        }
      }
    } catch(e){ L('Stop loss detection error: ' + e.message); }

    // Only manage positions the engine opened — skip manual trades
    // Build set of all engine deal IDs (from both trades and pairs_trades)
    try {
      const {sql: mgFilterSql} = require('@vercel/postgres');
      const [dirRows, pairRows] = await Promise.all([
        mgFilterSql`SELECT deal_id FROM trades WHERE status='open'`.catch(()=>({rows:[]})),
        mgFilterSql`SELECT deal_id_a, deal_id_b FROM pairs_trades WHERE status='open'`.catch(()=>({rows:[]})),
      ]);
      const engineDealIds = new Set();
      const pairsDealIds = new Set();
      dirRows.rows.forEach(r => engineDealIds.add(r.deal_id));
      pairRows.rows.forEach(r => {
        if(r.deal_id_a) { engineDealIds.add(r.deal_id_a); pairsDealIds.add(r.deal_id_a); }
        if(r.deal_id_b) { engineDealIds.add(r.deal_id_b); pairsDealIds.add(r.deal_id_b); }
      });
      const manualPos = openPos.filter(p => !engineDealIds.has(p.position.dealId));
      if(manualPos.length > 0) L(`Skipping ${manualPos.length} manual position(s) — not managed by engine`);
      openPos = openPos.filter(p => engineDealIds.has(p.position.dealId));
      // Separate counts: pairs legs don't consume directional slots
      const pairsLegCount = openPos.filter(p => pairsDealIds.has(p.position.dealId)).length;
      const directionalOpen = openPos.length - pairsLegCount;
      L(`Open: ${directionalOpen}/${cfg.maxPositions} directional | ${Math.round(pairsLegCount/2)}/${cfg.pairsMaxSlots} pairs | Heat: £${portfolioHeat}/${cfg.maxPortfolioHeat}`);
    } catch(e) { L('Engine position filter error: ' + e.message); }

    await managePositions(openPos,igBase,igH,cfg,balance,L);
  }catch(e){L('Positions error: '+e.message);}

  if((typeof directionalOpen !== 'undefined' ? directionalOpen : openPos.length)>=cfg.maxPositions){L('Max positions');return res.status(200).json({action:'max_positions',log});}
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
    const utcH = now.getUTCHours();
    const utcM = now.getUTCMinutes();
    // EOD: close 10 mins before configured time to avoid spread widening at session close
    // e.g. configured 21:00 UTC → actually closes at 20:50 UTC
    const eodMins = cfg.eodCloseTime.h * 60 + cfg.eodCloseTime.m - 10;
    const nowMins = utcH * 60 + utcM;
    const isEOD = nowMins >= eodMins && nowMins < eodMins + 60; // window: 10 mins early, up to 1hr after
    const isFridayEOD = now.getUTCDay() === 5 && nowMins >= eodMins && nowMins < eodMins + 60; // Friday same as weekdays (20:50 UTC)

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
          SELECT deal_id, COALESCE(trade_type, 'hourly_mr') as trade_type, created_at
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
          // ── OVERNIGHT / WEEKEND CLOSE RULES ────────────────────────────────
          // Calculate current P&L % for this position
          const posOpenLevel = p.position.openLevel;
          const posBid = p.market.bid || posOpenLevel;
          const posDir = p.position.direction;
          const posPnlPct = posDir === 'BUY'
            ? (posBid - posOpenLevel) / posOpenLevel * 100
            : (posOpenLevel - posBid) / posOpenLevel * 100;
          const isProfitable = posPnlPct > 0;
          const isWellProfitable = posPnlPct >= 1.0; // up 1%+ = hold over weekend

          // hourly_mr  → always close EOD (intraday scalp, never hold overnight)
          // daily_mr   → hold overnight, always close Friday (short-term bounce)
          // trend      → hold up to 20 days; hold weekends ONLY if profitable
          // breakout   → hold up to 15 days; hold weekends ONLY if profitable
          // directional→ hold indefinitely; hold weekends if profitable
          if(tradeType === 'hourly_mr') {
            shouldClose = true;
          } else if(tradeType === 'daily_mr') {
            shouldClose = daysHeld >= 5 || isFriday; // always close MR on Friday
          } else if(tradeType === 'trend') {
            if(daysHeld >= 20) shouldClose = true;          // max hold reached
            else if(isFriday && !isProfitable) shouldClose = true;  // losing — cut Friday
            else if(isFriday && isProfitable) {
              L(`${p.market.instrumentName}: trend trade profitable (+${posPnlPct.toFixed(2)}%) — holding over weekend`);
              shouldClose = false; // winning trend — hold the weekend
            }
          } else if(tradeType === 'breakout') {
            if(daysHeld >= 15) shouldClose = true;
            else if(isFriday && !isProfitable) shouldClose = true;
            else if(isFriday && isProfitable) {
              L(`${p.market.instrumentName}: breakout trade profitable (+${posPnlPct.toFixed(2)}%) — holding over weekend`);
              shouldClose = false;
            }
          } else if(tradeType === 'directional') {
            shouldClose = isFriday && !isProfitable;
          } else {
            shouldClose = true;
          }
          if(shouldClose) L(`${p.market.instrumentName}: closing (${tradeType}, ${daysHeld.toFixed(1)}d held, P&L ${posPnlPct.toFixed(2)}%)`);

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

  // ── PAIRS Z-SCORE CALCULATION ─────────────────────────────────────────────
  // Calculate live Z-scores for instrument pairs using DB candle history
  // Used to add confluence/contradiction context to AI signal evaluation
  // PAIRS_DEFINITIONS defined at module level above
    const pairsZScores = {}; // keyed by instrA → { zscore, instrB, direction }

  try {
    const {sql: pairSql} = require('@vercel/postgres');
    for(const pair of PAIRS_DEFINITIONS) {
      try {
        const lb = pair.lookbackDays || 60;
        // Fetch enough history to have lb days of aligned ratio data
        // Fetch 2× lookback as buffer for alignment gaps (weekends, holidays)
        const fetchLimit = Math.ceil(lb * 2.5);
        const rowsA = await pairSql`
          SELECT close_price, candle_time::date as dt FROM price_history
          WHERE instrument = ${pair.instrA} AND resolution = 'DAY'
          ORDER BY candle_time DESC LIMIT ${fetchLimit}`;
        const rowsB = await pairSql`
          SELECT close_price, volume, candle_time::date as dt FROM price_history
          WHERE instrument = ${pair.instrB} AND resolution = 'DAY'
          ORDER BY candle_time DESC LIMIT ${fetchLimit}`;

        if(rowsA.rows.length < 10 || rowsB.rows.length < 10) continue;

        // Align by date then reverse to chronological order
        const mapB = {};
        rowsB.rows.forEach(r => { mapB[r.dt] = { price: parseFloat(r.close_price), vol: parseInt(r.volume||0) }; });
        const aligned = rowsA.rows
          .filter(r => mapB[r.dt] && mapB[r.dt].price > 0)
          .map(r => ({ ratio: parseFloat(r.close_price) / mapB[r.dt].price, volA: 0, volB: mapB[r.dt].vol }))
          .reverse(); // oldest first

        if(aligned.length < 10) continue;

        // Rolling window: use only the most recent `lb` aligned days for mean/std
        // This matches the backtest's rolling lookback — engine and backtest now consistent
        const window = aligned.slice(-lb);
        const ratios = window.map(r => r.ratio);
        const volsA = new Array(aligned.length).fill(0);
        const volsB = aligned.map(r => r.volB);

        const mean = ratios.reduce((a,b) => a+b, 0) / ratios.length;
        const std = Math.sqrt(ratios.reduce((a,b) => a + Math.pow(b-mean,2), 0) / ratios.length);
        const current = ratios[ratios.length - 1];
        const zscore = std > 0 ? (current - mean) / std : 0;

        // Store for both instruments in the pair
        // Positive Z = instrA expensive vs instrB
        // Negative Z = instrA cheap vs instrB
        const volRatioA = 1.0; // volA not available from date-aligned query — volume confluence uses B only
        const volRatioB = calcVolumeRatio(volsB.slice(-lb), 20);
        pairsZScores[pair.instrA] = { zscore, partner: pair.instrB, n: ratios.length,
          mean, std, current,
          _volumesA: volsA, _volumesB: volsB,
          volRatioA, volRatioB,
          signal: zscore > 2 ? 'expensive' : zscore < -2 ? 'cheap' : zscore > 1.5 ? 'slightly_expensive' : zscore < -1.5 ? 'slightly_cheap' : 'neutral' };
        // From instrB perspective, Z is inverted
        pairsZScores[pair.instrB] = { zscore: -zscore, partner: pair.instrA, n: ratios.length,
          mean: mean > 0 ? 1/mean : 0, std, current: current > 0 ? 1/current : 0,
          signal: -zscore > 2 ? 'expensive' : -zscore < -2 ? 'cheap' : -zscore > 1.5 ? 'slightly_expensive' : -zscore < -1.5 ? 'slightly_cheap' : 'neutral' };

        if(Math.abs(zscore) >= 1.0) {
          L(`Pairs: ${pair.instrA}/${pair.instrB} Z=${zscore.toFixed(2)} n=${ratios.length}d (lb:${lb}d) (${pairsZScores[pair.instrA].signal})`);
        } else {
          L(`Pairs: ${pair.instrA}/${pair.instrB} Z=${zscore.toFixed(2)} n=${ratios.length}d (lb:${lb}d) (neutral)`);
        }
      } catch(e) { /* skip pair on error */ }
    }
  } catch(e) { L('Pairs Z-score error: ' + e.message); }

  // Signal evaluation
  const occupied=new Set(openPos.map(p=>CORRELATION_GROUPS[p.market.epic]).filter(Boolean));
  L('Occupied: '+(([...occupied].join(', '))||'none'));

  // ── PYRAMID ADDING ────────────────────────────────────────────────────────
  // If an existing trend/breakout trade is up 1%+ on day 2+, add a second unit
  // Backtest shows SMA crossover trades that start winning tend to continue
  try {
    const {sql:pyrSql} = require('@vercel/postgres');
    for(const pos of openPos) {
      const epic = pos.market.epic;
      const openLevel = pos.position.openLevel;
      const currentBid = pos.market.bid || openLevel;
      const dir = pos.position.direction;
      const pnlPct = dir === 'BUY'
        ? (currentBid - openLevel) / openLevel * 100
        : (openLevel - currentBid) / openLevel * 100;

      // Get trade details from DB
      const dbTrade = await pyrSql`
        SELECT trade_type, created_at, pyramid_added
        FROM trades WHERE deal_id=${pos.position.dealId} AND status='open'
        LIMIT 1`.catch(()=>({rows:[]}));
      const trade = dbTrade.rows[0];
      if(!trade) continue;

      const tradeType = trade.trade_type || '';
      const pyramidAdded = trade.pyramid_added || false;
      const daysHeld = (Date.now() - new Date(trade.created_at).getTime()) / (1000*60*60*24);

      // Only pyramid trend/breakout trades, on day 2+, up 1%+, not already pyramided
      if((tradeType === 'trend' || tradeType === 'breakout')
        && !pyramidAdded && daysHeld >= 1.5 && pnlPct >= 1.0
        && (typeof directionalOpen !== 'undefined' ? directionalOpen : openPos.length) < cfg.maxPositions) {

        const pyrSize = parseFloat((pos.position.dealSize * 0.5).toFixed(2)); // add 50% of original
        L(`🔺 Pyramid: ${pos.market.instrumentName} up ${pnlPct.toFixed(1)}% after ${daysHeld.toFixed(1)}d — adding £${pyrSize}/pt`);

        const pyrBody = { epic, direction: dir, size: pyrSize,
          orderType:'MARKET', expiry:'DFB', guaranteedStop:false,
          forceOpen:true, currencyCode:'GBP', dealType:'SPREADBET' };
        const pr = await fetch(`${igBase}/positions/otc`, {
          method:'POST', headers:{...igH,'Version':'1'}, body:JSON.stringify(pyrBody)});
        const pd = await pr.json();

        if(pd.dealReference) {
          L(`✅ Pyramid added: ref ${pd.dealReference}`);
          // Mark original trade as pyramided
          await pyrSql`UPDATE trades SET pyramid_added=true WHERE deal_id=${pos.position.dealId}`.catch(()=>{});
        } else {
          L(`⚠️ Pyramid failed: ${pd.errorCode||'unknown'}`);
        }
      }
    }
  } catch(e) { L(`Pyramid check error: ${e.message}`); }

  // ── 19:30 UTC VOLATILITY WARNING ─────────────────────────────────────────
  // AI analysis found consistent multi-instrument volatility spikes at 19:30 UTC
  // (3:30pm ET — US market close / options expiry time)
  // Avoid opening NEW positions in the 19:20-19:45 UTC window
  const utcMins = now.getUTCHours() * 60 + now.getUTCMinutes();
  const is1930Window = utcMins >= 19*60+20 && utcMins <= 19*60+45;
  if(is1930Window) {
    L('⚠️ 19:30 UTC volatility window — skipping new position opens');
    return res.status(200).json({ action:'volatility_window', log,
      message:'19:30 UTC witching hour — no new positions' });
  }

  // Cooldown: block re-entry on same instrument within 4 hours of a stop loss
  const recentlyStoppedEpics = new Set();
  try {
    const {sql:coolSql} = require('@vercel/postgres');
    const stoppedRows = await coolSql`
      SELECT epic FROM trades
      WHERE status = 'closed'
      AND close_reason = 'stop_loss'
      AND closed_at > NOW() - INTERVAL '4 hours'
    `.catch(() => ({ rows: [] }));
    stoppedRows.rows.forEach(r => recentlyStoppedEpics.add(r.epic));
    if(recentlyStoppedEpics.size > 0){
      L(`Cooldown active for: ${[...recentlyStoppedEpics].join(', ')}`);
    }
  } catch(e) { L('Cooldown check error: ' + e.message); }

  const signals=[];

  for(const instr of Object.keys(EPIC_MAP)){
    const epic=EPIC_MAP[instr];const grp=CORRELATION_GROUPS[epic];
    if(openPos.some(p=>p.market.epic===epic)){L(`${instr}: open`);continue;}
    if(grp&&occupied.has(grp)){L(`${instr}: group occupied`);continue;}
    if(recentlyStoppedEpics.has(epic)){L(`${instr}: cooldown (stopped out within 4h)`);continue;}
    // Nikkei has different hours
    const mktHrs = instr === 'Nikkei 225' ? 'nikkei' : grp;
    if(!isMarketOpen(mktHrs)){L(`${instr}: market closed`);continue;}

    try{
      // Try DB first, fall back to IG, then Twelve Data
      const dbEpic = DB_EPIC_MAP[epic] || epic;
      let closes=await getDbPrices(dbEpic,500,L);
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
      let dir='BUY'; // default, overridden by scoring

      if(closes&&closes.length>=5){
        L(`${instr}: ${closes.length} candles from ${src}`);
        regime=detectRegime(closes);
        sc=calcScore(closes,regime);

        // Update dir based on score
        dir = sc > 0 ? 'BUY' : 'SELL';

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

        // Volume confirmation for Brent Oil — only trade high-volume moves
        // Low volume Brent moves (0.64x avg) are unreliable — filter them out
        if(epic.includes('LCO') || instr === 'Brent Oil') {
          const brentVolRatio = calcVolumeRatio(closes._volumes, 20);
          if(brentVolRatio > 0 && brentVolRatio !== 1.0) {
            if(brentVolRatio < 0.8 && sc > 0) {
              L(`${instr}: ⚠️ Low volume (${brentVolRatio}x avg) — reducing conviction`);
              sc = Math.max(0, sc - 1);
            } else if(brentVolRatio > 1.5) {
              L(`${instr}: 📊 High volume (${brentVolRatio}x avg) — boosting conviction`);
              sc = Math.min(sc + 1, 6);
            }
          }
        }

        // Gold-specific 20-day breakout — works in ALL regimes (57% WR in backtest)
        // Gold trends strongly after breakouts regardless of regime
        if(epic.includes('USCGC') || instr === 'Gold') {
          const n = closes.length;
          if(n >= 22) {
            const high20 = Math.max(...closes.slice(-21,-1));
            const low20  = Math.min(...closes.slice(-21,-1));
            const price  = closes[n-1];
            const prev   = closes[n-2];
            if(prev < high20 && price > high20) {
              sc = Math.max(sc, 3); dir = 'BUY'; tradeType = 'breakout';
              L(`${instr}: 🥇 Gold 20-day high breakout at ${price.toFixed(0)} (score:${sc})`);
            } else if(prev > low20 && price < low20) {
              sc = Math.max(sc, 3); dir = 'SELL'; tradeType = 'breakout';
              L(`${instr}: 🥇 Gold 20-day low breakout at ${price.toFixed(0)} (score:${sc})`);
            }
          }
        }

        // ── DAX AFTER-HOURS LEAD INDICATOR ──────────────────────────────────
        // When DAX makes >1% hourly move after 16:30 UTC, US equities
        // tend to follow at next day's 13:30 UTC open (Finding 4 from AI analysis)
        const utcHour = now.getUTCHours();
        const isAfterDaxClose = utcHour >= 16 && utcHour <= 21;
        if(isAfterDaxClose && (epic.includes('SPTRD') || epic.includes('NASDAQ'))) {
          try {
            const {sql:daxSql} = require('@vercel/postgres');
            const daxRow = await daxSql`
              SELECT close_price FROM price_history
              WHERE instrument='DAX 40' AND resolution='HOUR'
              AND candle_time > NOW() - INTERVAL '3 hours'
              ORDER BY candle_time DESC LIMIT 3`.catch(()=>({rows:[]}));
            if(daxRow.rows.length >= 2) {
              const daxLast = parseFloat(daxRow.rows[0].close_price);
              const daxPrev = parseFloat(daxRow.rows[daxRow.rows.length-1].close_price);
              const daxMove = (daxLast - daxPrev) / daxPrev * 100;
              if(Math.abs(daxMove) > 1.0) {
                const daxDir = daxMove > 0 ? 'BUY' : 'SELL';
                L(`${instr}: 🇩🇪 DAX lead signal ${daxMove>0?'+':''}${daxMove.toFixed(2)}% after-hours → expect US ${daxDir} at tomorrow's open`);
                if(daxDir === dir || !dir) {
                  sc = Math.max(sc, 1); // adds 1 to score as confluence
                  dir = dir || daxDir;
                }
              }
            }
          } catch(e) { /* skip on error */ }
        }

        // SMA Crossover signal — trend following, works in any regime
        // Backtest shows 88.9% WR on Nasdaq, 62.5% on S&P 500 over 2 years
        const smaCross = calcSmaCrossover(closes);
        if(smaCross.signal > 0 && closes.length >= 55) {
          // Only override if no existing strong signal in opposite direction
          if(smaCross.direction === dir || sc < 2) {
            sc = Math.max(sc, smaCross.signal);
            dir = smaCross.direction;
            tradeType = 'trend';
            L(`${instr}: 📊 ${smaCross.reason} (score:${sc})`);
          }
        }

        // Momentum signal — backtest shows edge on indices
        const momentum = calcMomentum(closes);
        if(momentum.signal > 0 && (epic.includes('SPTRD')||epic.includes('NASDAQ')||epic.includes('FTSE')||epic.includes('DAX'))) {
          if(momentum.direction === dir || !dir) {
            sc = Math.max(sc, momentum.signal);
            dir = dir || momentum.direction;
            tradeType = 'trend';
            L(`${instr}: 🚀 ${momentum.reason} (score:${sc})`);
          }
        }

        // RSI Divergence detection — adds to score and passes to AI
        const divergence = detectRSIDivergence(closes);
        if(divergence.type==='bearish' && divergence.strength>0){
          sc -= divergence.strength; // Bearish divergence reduces score (supports SELL)
          L(`${instr}: ⚠️ Bearish RSI divergence (${divergence.description}) adj -${divergence.strength}`);
          // For GBP/USD: standalone signal (75% WR in backtest)
          if(epic.includes('GBPUSD') && divergence.strength >= 1 && !dir) {
            sc = Math.max(sc, 2); dir = 'SELL'; tradeType = 'trend';
            L(`${instr}: 📉 RSI divergence standalone SELL signal`);
          }
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
      // Update dir based on total score (unless trend/breakout already set it)
      if(!trendPullback?.signal && !breakoutSignal?.signal) dir=total>0?'BUY':'SELL';
      if(regime==='ranging'){
        if(rsiForMR>=68){
          // Overbought in ranging — mean reversion SELL
          // Trend filter: only SELL if daily SMA not in clear uptrend
          const sma10mr = closes.length>=10 ? closes.slice(-10).reduce((a,b)=>a+b,0)/10 : null;
          const sma20mr = closes.length>=20 ? closes.slice(-20).reduce((a,b)=>a+b,0)/20 : null;
          const inUptrend = sma10mr && sma20mr && sma10mr > sma20mr * 1.002;
          if(inUptrend){
            L(`${instr}: mean reversion SELL blocked — SMA10 (${sma10mr.toFixed(0)}) above SMA20 (${sma20mr.toFixed(0)}) — trend filter`);
          } else {
            dir='SELL';
            meanReversion=true;
            L(`${instr}: mean reversion SELL (RSI:${rsiForMR.toFixed(1)} overbought in ranging${sma10mr?', SMA trend OK':''})`);
          }
        } else if(rsiForMR<=33){
          // Oversold in ranging — mean reversion BUY
          // Trend filter: only BUY if daily SMA not in clear downtrend
          const sma10mr = closes.length>=10 ? closes.slice(-10).reduce((a,b)=>a+b,0)/10 : null;
          const sma20mr = closes.length>=20 ? closes.slice(-20).reduce((a,b)=>a+b,0)/20 : null;
          const inDowntrend = sma10mr && sma20mr && sma10mr < sma20mr * 0.998; // >0.2% below SMA20
          if(inDowntrend){
            L(`${instr}: mean reversion BUY blocked — daily SMA10 (${sma10mr.toFixed(0)}) below SMA20 (${sma20mr.toFixed(0)}) — trend filter`);
          } else {
            dir='BUY';
            meanReversion=true;
            L(`${instr}: mean reversion BUY (RSI:${rsiForMR.toFixed(1)} oversold in ranging${sma10mr?', SMA trend OK':''})`);
          }
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

      // Add pairs context for this instrument
      const pairsCtx = pairsZScores[instr] || null;
      signals.push({instr,epic,direction:dir,score:total,rawScore:sc,newsAdj,sentAdj,tdAdj,calAdj,regime,meanReversion,
        reasons,rsi,tdRsi:tdRsiForMR,effectiveRSI,sma20,sma50,macd,momentum:mom,lastClose:closes[closes.length-1],
        atr,suggestedSize:sz,bb,bbPos,src,candles:closes.length,divergence,trendPullback,breakoutSignal,pairsCtx});
      // Note: group only marked occupied on open position, not on signal
    }catch(e){L(`${instr}: ${e.message}`);}
  }

  if(!signals.length){L('No signals');return res.status(200).json({action:'no_signals',log});}
  signals.sort((a,b)=>Math.abs(b.score)-Math.abs(a.score));

  // Allow up to 2 signals per correlation group
  // Priority: mean reversion > breakout > trend > regular
  const GROUP_MAX = 2;
  const groupCounts = {};
  signals.sort((a,b)=>{
    const aPri = a.meanReversion ? 3 : a.breakoutSignal?.signal>0 ? 2 : a.trendPullback?.signal>0 ? 1 : 0;
    const bPri = b.meanReversion ? 3 : b.breakoutSignal?.signal>0 ? 2 : b.trendPullback?.signal>0 ? 1 : 0;
    if(aPri !== bPri) return bPri - aPri;
    return Math.abs(b.score)-Math.abs(a.score);
  });
  const filteredSignals=signals.filter(sig=>{
    const grp=CORRELATION_GROUPS[sig.epic];
    if(!grp){return true;}
    groupCounts[grp] = (groupCounts[grp]||0);
    if(groupCounts[grp] >= GROUP_MAX) return false;
    groupCounts[grp]++;
    return true;
  });
  const mrCount=filteredSignals.filter(s=>s.meanReversion).length;
  const boCount=filteredSignals.filter(s=>s.breakoutSignal?.signal>0).length;
  L(`${signals.length} signal(s) — ${filteredSignals.length} after group filter (${mrCount} MR, ${boCount} breakout)`);

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
    // DB candles for FX are stored pre-scaled (e.g. GBPUSD*10000, USDJPY*100)
    // ATR from DB candles is already in contract units — do NOT scale again
    // IG live prices are also in contract units — no scaling needed
    const priceScale = 1; // scaling already applied during backfill
    const scaledATR = (sig.atr || 0);
    const isTrendTrade = sig.trendPullback?.signal > 0;
    const isBreakoutTrade = sig.breakoutSignal?.signal > 0;
    // ATR stop multiplier — tighter for MR (avoids catastrophic losses like Apr crash)
    // Volatility-aware: if recent ATR is >2× historical average, tighten stop further
    const recentATR = scaledATR;
    const closes10 = sig.closes ? sig.closes.slice(-10) : [];
    const avgATR10 = closes10.length >= 2
      ? closes10.slice(1).reduce((s,p,i)=>s+Math.abs(p-closes10[i]),0)/(closes10.length-1)
      : recentATR;
    const isHighVol = recentATR > avgATR10 * 1.8; // ATR 80% above recent avg = high vol
    const atrMult = (sig.meanReversion && sig.tdRsi) ? 0.5
      : (isTrendTrade || isBreakoutTrade) ? 2.0
      : (isDailyMR && isHighVol) ? 1.0    // tighter stop in high vol MR (avoids -6% losses)
      : isDailyMR ? 1.25                   // slightly tighter than default for MR
      : 1.5;                               // default
    const stopType = sig.meanReversion && sig.tdRsi ? 'hourly MR (0.5x ATR)'
      : sig.meanReversion ? 'daily MR (1.5x ATR)'
      : isTrendTrade ? 'trend pullback (2x ATR)'
      : isBreakoutTrade ? 'breakout (2x ATR)'
      : 'standard (1.5x ATR)';
    // minStop in contract units (after ATR scaling) — fixed minimums by instrument type
    const minStop = sig.src === 'DB' && priceScale > 1
      ? Math.max(5, Math.round(priceScale * 0.001))  // 0.1% of scale e.g. 10pts for FX
      : Math.max(5, 10);                              // 10pts for IG-sourced candles
    const tradeStopDist = scaledATR > 0 ? Math.max(minStop, Math.round(scaledATR * atrMult)) : minStop;
    // Tiered risk % by signal quality
    // Daily MR (strongest signal): 2%
    // Hourly MR with trend filter passed: 1.5%
    // Trend pullback: 1.5%
    // Breakout confirmed: 1%
    // Standard/other: 1%
    const isHourlyMR = sig.tdRsi && sig.meanReversion;
    const isDailyMR = sig.meanReversion && !sig.tdRsi;
    // Check pairs confluence — does pairs signal confirm this trade direction?
    const pairsConfirms = sig.pairsCtx && (
      (sig.pairsCtx.signal === 'cheap' && sig.direction === 'BUY') ||
      (sig.pairsCtx.signal === 'expensive' && sig.direction === 'SELL')
    );
    const pairsContradicts = sig.pairsCtx && (
      (sig.pairsCtx.signal === 'expensive' && sig.direction === 'BUY') ||
      (sig.pairsCtx.signal === 'cheap' && sig.direction === 'SELL')
    );
    // ── DYNAMIC SIZING — scales with signal conviction ──────────────────────
    // Base risk by signal type
    let baseRiskPct;
    if(isDailyMR && pairsConfirms)       baseRiskPct = 0.025; // 2.5% daily MR + pairs
    else if(isDailyMR)                   baseRiskPct = 0.02;  // 2.0% daily MR
    else if(isHourlyMR && pairsConfirms) baseRiskPct = 0.02;  // 2.0% hourly MR + pairs
    else if(isHourlyMR)                  baseRiskPct = 0.015; // 1.5% hourly MR
    else if(isTrendTrade)                baseRiskPct = 0.025; // 2.5% trend (SMA cross — 71% WR)
    else if(isBreakoutTrade)             baseRiskPct = 0.02;  // 2.0% breakout (57% WR on Gold)
    else                                 baseRiskPct = 0.01;  // 1.0% default
    if(pairsContradicts) baseRiskPct = Math.min(baseRiskPct, 0.01); // Cap at 1% if pairs contradicts
    if(pairsConfirms) L(`${sig.instr}: ✅ Pairs confluence — risk boosted to ${(baseRiskPct*100).toFixed(1)}%`);
    if(pairsContradicts) L(`${sig.instr}: ⚠️ Pairs contradiction — risk capped at ${(baseRiskPct*100).toFixed(1)}%`);
    // Boost size based on AI confidence (60%=1x, 80%=1.3x, 95%=1.5x)
    const aiBoost = sig.aiConfidence >= 90 ? 1.5
      : sig.aiConfidence >= 80 ? 1.3
      : sig.aiConfidence >= 70 ? 1.1
      : 1.0;
    const riskPct = profitLockActive ? 0.005 : Math.min(baseRiskPct * aiBoost, 0.04);
    const tradeRiskAmt = balance * riskPct;
    const riskSz = parseFloat((tradeRiskAmt / tradeStopDist).toFixed(2));
    const finalSz = Math.max(0.01, Math.min(riskSz, cfg.maxSizePerTrade));
    const actualRisk = (finalSz * tradeStopDist).toFixed(2);
    L(`${sig.instr}: size £${finalSz}/pt | risk ${(riskPct*100).toFixed(1)}% = £${tradeRiskAmt.toFixed(2)} | stop ${tradeStopDist}pt | ${stopType}`);

    L(`${sig.instr}: size £${finalSz}/pt × ${tradeStopDist}pt stop = £${actualRisk} risk (${((parseFloat(actualRisk)/balance)*100).toFixed(1)}% of account)`);
    L(`Placing ${sig.direction} ${finalSz} on ${sig.instr} (regime:${sig.regime})...`);

    // ── 1-MINUTE PULLBACK ENTRY ────────────────────────────────────────────────
    // For trend/breakout signals: wait up to 2 hours for a better entry on 1m chart
    // For MR signals: enter immediately (timing-sensitive, don't wait)
    // Pullback entry — waits for 0.2% dip before entering trend/breakout trades
    // Requires pending_entries table (created via Init DB in Journal tab)
    const usePullbackEntry = (isTrendTrade || isBreakoutTrade) && !sig.tdRsi;
    let entryImproved = false;

    if(usePullbackEntry) {
      try {
        // Check if pending_entries table exists first
        const {sql:peSql} = require('@vercel/postgres');
        const tableCheck = await peSql`SELECT 1 FROM pending_entries LIMIT 1`.catch(()=>null);
        if(!tableCheck) {
          L(`${sig.instr}: pending_entries table not ready — entering at market`);
          throw new Error('table_not_ready'); // fall through to market order
        }

        const existing = await peSql`
          SELECT * FROM pending_entries
          WHERE epic=${sig.epic} AND status='waiting'
          ORDER BY created_at DESC LIMIT 1`.catch(()=>({rows:[]}));

        if(existing.rows.length === 0) {
          // No pending entry — create one and wait
          const currentPrice = sig.closes?.[sig.closes.length-1] || 0;
          const targetEntry = sig.direction === 'BUY'
            ? currentPrice * 0.998  // 0.2% pullback for buys
            : currentPrice * 1.002; // 0.2% bounce for sells
          const expiryTime = new Date(Date.now() + 2*60*60*1000).toISOString(); // 2hr limit

          await peSql`
            INSERT INTO pending_entries
            (epic, instrument, direction, signal_price, target_entry, size, stop_dist,
             trade_type, score, ai_confidence, ai_reasoning, expiry_time, status)
            VALUES (${sig.epic}, ${sig.instr}, ${sig.direction}, ${currentPrice},
            ${targetEntry}, ${finalSz}, ${tradeStopDist}, ${tradeType},
            ${sig.score}, ${confidence}, ${reasoning}, ${expiryTime}, 'waiting')
            ON CONFLICT DO NOTHING`.catch(()=>{});

          L(`${sig.instr}: 📍 Pullback entry queued — waiting for ${sig.direction==='BUY'?'dip to':'bounce to'} ${targetEntry.toFixed(2)} (current: ${currentPrice.toFixed(2)}) — 2hr limit`);
          return res.status(200).json({action:'pending_entry', instrument:sig.instr,
            direction:sig.direction, targetEntry, currentPrice, log});

        } else {
          // Pending entry exists — check if price has reached target
          const pe = existing.rows[0];
          const hoursWaiting = (Date.now() - new Date(pe.created_at).getTime()) / (1000*60*60);
          const currentPrice = sig.closes?.[sig.closes.length-1] || 0;
          const targetReached = sig.direction === 'BUY'
            ? currentPrice <= pe.target_entry
            : currentPrice >= pe.target_entry;
          const expired = new Date() > new Date(pe.expiry_time);

          if(targetReached) {
            L(`${sig.instr}: ✅ Pullback target reached at ${currentPrice.toFixed(2)} (was ${pe.signal_price}) — entering now`);
            entryImproved = true;
            // Mark as filled and proceed with entry below
            await peSql`UPDATE pending_entries SET status='filled' WHERE id=${pe.id}`.catch(()=>{});
          } else if(expired) {
            L(`${sig.instr}: ⏱️ Pullback wait expired (${hoursWaiting.toFixed(1)}h) — entering at market`);
            await peSql`UPDATE pending_entries SET status='expired' WHERE id=${pe.id}`.catch(()=>{});
            // Proceed with market entry below
          } else {
            L(`${sig.instr}: ⏳ Waiting for pullback to ${pe.target_entry.toFixed(2)} (current: ${currentPrice.toFixed(2)}, ${hoursWaiting.toFixed(1)}h elapsed)`);
            return res.status(200).json({action:'waiting_for_pullback', instrument:sig.instr,
              targetEntry:pe.target_entry, currentPrice, hoursWaiting, log});
          }
        }
      } catch(e) {
        if(e.message !== 'table_not_ready') {
          L(`Pullback entry check error: ${e.message} — entering at market`);
        }
        // Fall through to market order below
      }
    }

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
            // IG requires current stopLevel when converting to trailing stop
            const currentStopLevel = cd.level
              ? (sig.direction==='BUY'
                  ? cd.level - finalStopDist
                  : cd.level + finalStopDist)
              : null;
            const tsBody={
              trailingStop:true,
              trailingStopDistance:trailDist,
              trailingStopIncrement:trailIncrement,
              stopLevel:currentStopLevel,
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
      // Save trade to DB with visible error logging
        try {
          const base2 = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
          const dbr = await fetch(`${base2}/api/db`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ type:'trade_opened', data:{
              dealId:cd.dealId, dealReference:ref, instrument:sig.instr, epic:sig.epic,
              direction:sig.direction, size:finalSz, openLevel:cd.level, signalScore:sig.score,
              aiConfidence:confidence, aiReasoning:reasoning,
              signalReasons:(sig.reasons||[]).join(', '),
              regime:sig.regime, atr:sig.atr, stopDistance:finalStopDist||tradeStopDist, tradeType
            }})
          });
          const dbd = await dbr.json().catch(()=>({}));
          L(`DB save: ${dbr.ok?'✅ saved':'❌ failed '+dbr.status+' '+JSON.stringify(dbd).substring(0,100)}`);
        } catch(e){ L(`DB save error: ${e.message}`); }
        await sendNotify('trade',`🎯 Trade Placed: ${sig.instr} ${sig.direction}`,
          `Instrument: ${sig.instr}\nDirection: ${sig.direction}\nSize: £${finalSz}/pt\nLevel: ${cd.level}\nStop: ${tradeStopDist}pts\nMax Loss: £${actualRisk}\nAI: ${confidence}% — ${reasoning}\nRegime: ${sig.regime}\nScore: ${sig.score}\nType: ${tradeType} (${tradeType==='hourly_mr'?'closes tonight':tradeType==='daily_mr'?'holds up to 3 days':'holds until Friday'})`);
        return res.status(200).json({action:'trade_placed',instrument:sig.instr,direction:sig.direction,level:cd.level,size:finalSz,log});
      }else{
        L(`Rejected: ${cd.reason||cd.dealStatus}`);
        await sendNotify('error',`❌ Order Rejected: ${sig.instr}`,`Reason: ${cd.reason||cd.dealStatus}`);
      }
    }catch(e){L('Trade error: '+e.message);}
  }

  // ── PAIRS TRADING ────────────────────────────────────────────────────────────
  // Pairs trades use SEPARATE slots (pairsMaxSlots) from directional trades (maxPositions)
  // Count directional-only open positions for slot check
  const openCount = openPos.length;
  if(cfg.pairsEnabled) {
    try {
      const {sql: pSql} = require('@vercel/postgres');

      // Check existing pairs trades
      // Create table if not exists (safe to run every time)
      await pSql`CREATE TABLE IF NOT EXISTS pairs_trades (
        id SERIAL PRIMARY KEY, pair_id VARCHAR(50), instr_a VARCHAR(50), instr_b VARCHAR(50),
        epic_a VARCHAR(100), epic_b VARCHAR(100), direction_a VARCHAR(10), direction_b VARCHAR(10),
        size_a DECIMAL(10,4), size_b DECIMAL(10,4), deal_id_a VARCHAR(50), deal_id_b VARCHAR(50),
        entry_z DECIMAL(8,4), stop_z DECIMAL(8,4), target_z DECIMAL(8,4), close_z DECIMAL(8,4),
        close_reason VARCHAR(30), ai_confidence INTEGER, status VARCHAR(20) DEFAULT 'open',
        partial_close BOOLEAN DEFAULT false,
        opened_at TIMESTAMPTZ DEFAULT NOW(), closed_at TIMESTAMPTZ
      )`.catch(()=>{});
      // Add partial_close to existing tables that predate this column
      await pSql`ALTER TABLE pairs_trades ADD COLUMN IF NOT EXISTS partial_close BOOLEAN DEFAULT false`.catch(()=>{});
      const openPairs = await pSql`SELECT pair_id, instr_a, instr_b, direction_a, deal_id_a, deal_id_b,
        entry_z, stop_z, target_z, opened_at FROM pairs_trades WHERE status='open'`.catch(()=>({rows:[]}));
      const openPairIds = new Set(openPairs.rows.map(r=>r.pair_id));
      L(`Open pairs: ${openPairs.rows.length}/${cfg.pairsMaxSlots}`);

    // ── EUR/USD TRIANGULATION LAG DETECTOR ──────────────────────────────────
    // Finding 5: EUR/USD lags GBP/USD by ~30min due to triangular relationship
    // When GBP/USD moves significantly, EUR/USD should follow — trade the lag
    try {
      const {sql:triSql} = require('@vercel/postgres');
      const [gbpRow, eurRow] = await Promise.all([
        triSql`SELECT close_price FROM price_history WHERE instrument='GBP/USD'
               AND resolution='MINUTE' AND candle_time > NOW() - INTERVAL '45 minutes'
               ORDER BY candle_time ASC`.catch(()=>({rows:[]})),
        triSql`SELECT close_price FROM price_history WHERE instrument='EUR/USD'
               AND resolution='MINUTE' AND candle_time > NOW() - INTERVAL '45 minutes'
               ORDER BY candle_time ASC`.catch(()=>({rows:[]}))
      ]);

      if(gbpRow.rows.length >= 5 && eurRow.rows.length >= 5) {
        const gbpFirst = parseFloat(gbpRow.rows[0].close_price);
        const gbpLast = parseFloat(gbpRow.rows[gbpRow.rows.length-1].close_price);
        const eurFirst = parseFloat(eurRow.rows[0].close_price);
        const eurLast = parseFloat(eurRow.rows[eurRow.rows.length-1].close_price);

        const gbpMove = (gbpLast - gbpFirst) / gbpFirst * 100;
        const eurMove = (eurLast - eurFirst) / eurFirst * 100;
        const divergence = Math.abs(gbpMove - eurMove);

        // GBP moved >0.1% but EUR hasn't caught up (divergence >0.05%)
        if(Math.abs(gbpMove) > 0.1 && divergence > 0.05) {
          const lagDir = gbpMove > 0 ? 'BUY' : 'SELL';
          L(`💡 EUR/USD triangulation lag: GBP/USD ${gbpMove>0?'+':''}${gbpMove.toFixed(3)}% | EUR/USD ${eurMove>0?'+':''}${eurMove.toFixed(3)}% | divergence ${divergence.toFixed(3)}% — EUR/USD should ${lagDir}`);
        }
      }
    } catch(e) { /* skip */ }

      // Check if any open pairs need closing (Z-score reverted or stopped)
      for(const pt of openPairs.rows) {
        const pairDef = PAIRS_DEFINITIONS.find(p=>p.id===pt.pair_id);
        if(!pairDef) continue;
        const pz = pairsZScores[pt.instr_a];
        if(!pz) continue;
        const currentZ = pz.zscore;
        // Use per-pair thresholds from backtest optimisation
        const pairExitZ = pairDef.exitZ || cfg.pairsZTarget;
        const pairStopZ = pairDef.stopZ || cfg.pairsZStop;
        // Days held for this pairs trade
        const pairDaysHeld = pt.opened_at
          ? (Date.now() - new Date(pt.opened_at).getTime()) / (1000*60*60*24) : 0;
        const pairMaxHold = 90; // safety limit — backtest shows 56d was still a winner

        const shouldClose =
          (pt.direction_a==='BUY' && (currentZ >= -pairExitZ || currentZ <= -pairStopZ)) ||
          (pt.direction_a==='SELL' && (currentZ <= pairExitZ || currentZ >= pairStopZ)) ||
          pairDaysHeld >= pairMaxHold; // 90-day safety limit

        if(shouldClose) {
          const reason = pairDaysHeld >= pairMaxHold ? 'max_hold'
            : Math.abs(currentZ) <= pairExitZ ? 'mean_revert' : 'stop_loss';
          L(`Pairs close: ${pt.instr_a}/${pt.instr_b} Z=${currentZ.toFixed(2)} (${reason})`);
          // Close both legs
          for(const dealId of [pt.deal_id_a, pt.deal_id_b].filter(Boolean)) {
            const posR = await fetch(`${igBase}/positions`,{headers:{...igH,'Version':'1'}});
            const posD = await posR.json();
            const pos = (posD.positions||[]).find(p=>p.position.dealId===dealId);
            if(pos) {
              const closeBody={epic:pos.market.epic,direction:pos.position.direction==='BUY'?'SELL':'BUY',
                size:pos.position.size,orderType:'MARKET',expiry:'DFB',
                guaranteedStop:false,forceOpen:false,currencyCode:'GBP',dealType:'SPREADBET'};
              await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(closeBody)});
            }
          }
          await pSql`UPDATE pairs_trades SET status='closed', close_z=${currentZ}, close_reason=${reason}, closed_at=NOW() WHERE pair_id=${pt.pair_id} AND status='open'`.catch(()=>{});
          L(`Pairs closed: ${pt.instr_a}/${pt.instr_b} reason:${reason}`);
        }
      }

      // Look for new pairs signals
      if(openPairs.rows.length < cfg.pairsMaxSlots) {
        for(const pair of PAIRS_DEFINITIONS) {
          if(openPairIds.has(pair.id)) continue; // already open
          const pz = pairsZScores[pair.instrA];
          if(!pz || pz.n < pair.minDays) continue;
          const absZ = Math.abs(pz.zscore);
          // Use per-pair entry threshold from backtest optimisation
      const pairEntryZ = pair.entryZ || cfg.pairsZEntry;
      if(absZ < pairEntryZ) continue;

      // Volume confirmation for pairs — check if divergence driven by volume
      const pairVolRatioA = pz?.volRatioA || 1.0;
      const pairVolRatioB = pz?.volRatioB || 1.0;
      const pairVolConfirmed = pairVolRatioA > 1.2 || pairVolRatioB > 1.2;
      const pairVolWeak = pairVolRatioA < 0.6 && pairVolRatioB < 0.6;
      if(pairVolWeak) {
        L(`Pairs ${pair.instrA}/${pair.instrB}: ⚠️ Low volume on both legs (${pairVolRatioA}x, ${pairVolRatioB}x) — divergence may not revert`);
      } else if(pairVolConfirmed) {
        L(`Pairs ${pair.instrA}/${pair.instrB}: ✅ Volume confirmed (${pairVolRatioA}x, ${pairVolRatioB}x) — institutional divergence`);
      }

          // Direction: negative Z = A cheap vs B → BUY A, SELL B
          const dirA = pz.zscore < 0 ? 'BUY' : 'SELL';
          const dirB = pz.zscore < 0 ? 'SELL' : 'BUY';
          L(`Pairs signal: ${pair.instrA}/${pair.instrB} Z=${pz.zscore.toFixed(2)} ${dirA} ${pair.instrA} / ${dirB} ${pair.instrB}`);

          // AI confirmation
          const pairsPrompt = `Trading risk manager. Approve this PAIRS trade?
PAIR: ${pair.instrA} vs ${pair.instrB}
STRATEGY: Statistical arbitrage — Z-score divergence from ${pz.n}-day mean
Z-SCORE: ${pz.zscore.toFixed(2)} (entry threshold: ±${pairEntryZ})
DIRECTION: ${dirA} ${pair.instrA} / ${dirB} ${pair.instrB}
VOLUME: ${pair.instrA} ${pairVolRatioA}x avg | ${pair.instrB} ${pairVolRatioB}x avg${pairVolConfirmed?' — HIGH VOLUME (institutional)':pairVolWeak?' — LOW VOLUME (retail noise)':' — normal'}
DESCRIPTION: ${pair.description}
RATIO: Current ${(pz.current||0).toFixed(4)} vs Mean ${(pz.mean||0).toFixed(4)} | σ: ${(pz.std||0).toFixed(4)}
STOP: Z=±${pair.stopZ} (${(pair.stopZ-absZ).toFixed(1)}σ away)
EXIT TARGET: Z=±${pair.exitZ} (${(absZ-pair.exitZ).toFixed(1)}σ reversion needed)
DATA: ${pz.n} days of history | Signal strength: ${absZ>=2.5?'Strong':absZ>=2?'Moderate':'Weak'}
APPROVAL RULES: Approve if (1) |Z| ≥ ${pairEntryZ}, (2) sufficient history (≥${pair.minDays} days), (3) no fundamental reason for permanent divergence.
Account P&L: ${plPct.toFixed(2)}% | Open positions: ${openCount}/${cfg.maxPositions}
Respond ONLY: {"approved":true,"confidence":72,"reasoning":"2-3 sentences"}`;

          let pairsApproved = false; let pairsConfidence = 0; let pairsReasoning = '';
          try {
            const base2 = process.env.PRODUCTION_URL||`https://${process.env.VERCEL_URL}`;
            const aiR = await fetch(`${base2}/api/claude`,{method:'POST',
              headers:{'Content-Type':'application/json'},
              body:JSON.stringify({model:'claude-haiku-4-5',max_tokens:150,
                messages:[{role:'user',content:pairsPrompt}]})});
            if(aiR.ok){
              const aiD = await aiR.json();
              const txt = aiD.content?.[0]?.text||'';
              const clean = txt.replace(/```json|```/g,'').trim();
              const parsed = JSON.parse(clean);
              pairsApproved = parsed.approved===true;
              pairsConfidence = parsed.confidence||0;
              pairsReasoning = parsed.reasoning||'';
            }
          } catch(e){ L(`Pairs AI error: ${e.message}`); }

          L(`Pairs AI: ${pairsApproved?'✅':'❌'}(${pairsConfidence}%) ${pairsReasoning}`);
          if(!pairsApproved || pairsConfidence < cfg.aiConfidenceMin) continue;

          // Size both legs — pairs trades exempt from profit lock
          // (multi-day positions shouldn't be blocked by intraday P&L)
          const riskAmt = balance * cfg.pairsRiskPct;
          const zStopDist = pair.stopZ - absZ; // σ distance to stop (per-pair threshold)
          // Get current prices for sizing — IG market snapshot primary, DB candle fallback
          const [priceFeedA, priceFeedB] = await Promise.all([
            fetch(`${igBase}/markets/${pair.epicA}`,{headers:{...igH,'Version':'3'}}).then(r=>r.json()).catch(()=>null),
            fetch(`${igBase}/markets/${pair.epicB}`,{headers:{...igH,'Version':'3'}}).then(r=>r.json()).catch(()=>null),
          ]);
          let priceA = priceFeedA?.snapshot?.bid || 0;
          let priceB = priceFeedB?.snapshot?.bid || 0;
          const minStopA = priceFeedA?.dealingRules?.minNormalStopOrLimitDistance?.value || 5;
          const minStopB = priceFeedB?.dealingRules?.minNormalStopOrLimitDistance?.value || 5;

          // Fallback to last DB candle close if IG snapshot unavailable
          if(!priceA || !priceB) {
            try {
              const {sql: priceSql} = require('@vercel/postgres');
              if(!priceA) {
                const rA = await priceSql`SELECT close_price FROM price_history WHERE instrument=${pair.instrA} AND resolution='DAY' ORDER BY candle_time DESC LIMIT 1`;
                if(rA.rows.length) {
                  const raw = parseFloat(rA.rows[0].close_price);
                  priceA = raw * (pair.dbPriceScaleA || 1.0);
                  L(`Pairs: ${pair.instrA} using DB price ${priceA.toFixed(1)} (scaled from ${raw}, IG snapshot unavailable)`);
                }
              }
              if(!priceB) {
                const rB = await priceSql`SELECT close_price FROM price_history WHERE instrument=${pair.instrB} AND resolution='DAY' ORDER BY candle_time DESC LIMIT 1`;
                if(rB.rows.length) {
                  const raw = parseFloat(rB.rows[0].close_price);
                  priceB = raw * (pair.dbPriceScaleB || 1.0);
                  L(`Pairs: ${pair.instrB} using DB price ${priceB.toFixed(1)} (scaled from ${raw}, IG snapshot unavailable)`);
                }
              }
            } catch(e) { L(`Pairs: DB price fallback error — ${e.message}`); }
          }

          if(!priceA || !priceB){ L(`Pairs: could not get prices for ${pair.instrA}/${pair.instrB} — skipping`); continue; }

          // Safety: if using scaled DB prices for a commodity pair, skip execution
          // DB unit scaling is approximate — only trade with confirmed live IG prices
          const usingDbFallback = (!priceFeedA?.snapshot?.bid || !priceFeedB?.snapshot?.bid);
          if(usingDbFallback && (pair.dbPriceScaleA !== undefined || pair.dbPriceScaleB !== undefined)) {
            L(`Pairs: ${pair.instrA}/${pair.instrB} — commodity pair requires live IG prices for sizing, skipping until market open`);
            continue;
          }

          // Stop distance in points = zStopDist * σ * priceB (approx)
          const pzStats = pairsZScores[pair.instrA];
          const stopPtsA = Math.max(minStopA*1.5, Math.round(zStopDist * (pzStats?.std||0.01) * priceB * 3));
          const stopPtsB = Math.max(minStopB*1.5, Math.round(zStopDist * (pzStats?.std||0.01) * priceA * 3));
          const sizeA = Math.max(0.01, Math.min(parseFloat((riskAmt/2/stopPtsA).toFixed(2)), cfg.maxSizePerTrade));
          const sizeB = Math.max(0.01, Math.min(parseFloat((riskAmt/2/stopPtsB).toFixed(2)), cfg.maxSizePerTrade));

          L(`Pairs sizing: ${pair.instrA} £${sizeA}/pt stop ${stopPtsA}pts | ${pair.instrB} £${sizeB}/pt stop ${stopPtsB}pts`);

          // Open leg A
          let dealIdA = null, dealIdB = null;
          try {
            const bodyA = {epic:pair.epicA,direction:dirA,size:sizeA,orderType:'MARKET',
              expiry:'DFB',guaranteedStop:false,forceOpen:true,currencyCode:'GBP',
              dealType:'SPREADBET',stopDistance:stopPtsA*3}; // 3× safety stop on IG
            const rA = await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(bodyA)});
            const dA = await rA.json();
            if(dA.dealReference){
              await new Promise(r=>setTimeout(r,500));
              const cA = await fetch(`${igBase}/confirms/${dA.dealReference}`,{headers:{...igH,'Version':'1'}});
              const cdA = await cA.json();
              if(cdA.dealStatus==='ACCEPTED'){ dealIdA=cdA.dealId; L(`Pairs leg A: ✅ ${pair.instrA} ${dirA} at ${cdA.level}`); }
              else { L(`Pairs leg A rejected: ${cdA.reason}`); continue; }
            }
          } catch(e){ L(`Pairs leg A error: ${e.message}`); continue; }

          // Open leg B — if this fails, close leg A immediately
          try {
            const bodyB = {epic:pair.epicB,direction:dirB,size:sizeB,orderType:'MARKET',
              expiry:'DFB',guaranteedStop:false,forceOpen:true,currencyCode:'GBP',
              dealType:'SPREADBET',stopDistance:stopPtsB*3};
            const rB = await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(bodyB)});
            const dB = await rB.json();
            if(dB.dealReference){
              await new Promise(r=>setTimeout(r,500));
              const cB = await fetch(`${igBase}/confirms/${dB.dealReference}`,{headers:{...igH,'Version':'1'}});
              const cdB = await cB.json();
              if(cdB.dealStatus==='ACCEPTED'){ dealIdB=cdB.dealId; L(`Pairs leg B: ✅ ${pair.instrB} ${dirB} at ${cdB.level}`); }
              else {
                L(`Pairs leg B rejected: ${cdB.reason} — closing leg A`);
                // Close leg A since leg B failed
                const posR = await fetch(`${igBase}/positions`,{headers:{...igH,'Version':'1'}});
                const posD = await posR.json();
                const posA = (posD.positions||[]).find(p=>p.position.dealId===dealIdA);
                if(posA){ const cbA={epic:pair.epicA,direction:dirA==='BUY'?'SELL':'BUY',size:sizeA,orderType:'MARKET',expiry:'DFB',guaranteedStop:false,forceOpen:false,currencyCode:'GBP',dealType:'SPREADBET'};
                  await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(cbA)}); }
                continue;
              }
            }
          } catch(e){
            L(`Pairs leg B error: ${e.message} — closing leg A`);
            continue;
          }

          // Both legs open — save to DB
          try {
            await pSql`INSERT INTO pairs_trades (pair_id, instr_a, instr_b, epic_a, epic_b,
              direction_a, direction_b, size_a, size_b, deal_id_a, deal_id_b,
              entry_z, stop_z, target_z, ai_confidence, status, opened_at)
              VALUES (${pair.id},${pair.instrA},${pair.instrB},${pair.epicA},${pair.epicB},
              ${dirA},${dirB},${sizeA},${sizeB},${dealIdA},${dealIdB},
              ${pz.zscore},${pz.zscore<0?-pair.stopZ:pair.stopZ},${pz.zscore<0?-pair.exitZ:pair.exitZ},
              ${pairsConfidence},'open',NOW())`;
            L(`Pairs trade saved: ${pair.instrA}/${pair.instrB} Z=${pz.zscore.toFixed(2)}`);
          } catch(e){ L(`Pairs DB save error: ${e.message}`); }

          await sendNotify('trade',`⚖️ Pairs Trade: ${pair.instrA}/${pair.instrB}`,
            `${dirA} ${pair.instrA} £${sizeA}/pt | ${dirB} ${pair.instrB} £${sizeB}/pt\nZ-score: ${pz.zscore.toFixed(2)} | Entry: ±${pairEntryZ} | Exit: ±${pair.exitZ} | Stop: ±${pair.stopZ}\nAI: ${pairsConfidence}% — ${pairsReasoning}`);
          break; // One pairs trade per run
        }
      }
    } catch(e){ L(`Pairs trading error: ${e.message}`); }
  }

  L('No trades placed');
  return res.status(200).json({action:'no_trades',signals:signals.length,log});
  } catch(topErr) {
    console.error('[Autotrade top-level error]', topErr.message, topErr.stack);
    return res.status(500).json({error:topErr.message, stack:topErr.stack?.split('\n')[1]||'', log:[]});
  }
};

// ── PRICE DATA ────────────────────────────────────────────────────────────────
function calcVolumeRatio(volumes, lookback=20) {
  // Returns ratio of recent volume vs average — >1.5 = high vol, <0.7 = low vol
  if(!volumes || volumes.length < 5) return 1.0;
  const recent = volumes[volumes.length-1] || 0;
  const avg = volumes.slice(-lookback).reduce((a,b)=>a+b,0) / Math.min(lookback, volumes.length);
  return avg > 0 ? parseFloat((recent/avg).toFixed(2)) : 1.0;
}

async function getDbPrices(epic,limit,L){
  try{
    const {sql}=require('@vercel/postgres');
    const r=await sql`SELECT close_price, volume FROM price_history WHERE (epic=${epic} OR instrument=${epic}) AND resolution='DAY' AND close_price>0 ORDER BY candle_time DESC LIMIT ${limit}`;
    if(r.rows.length<5)return null;
    const closes = r.rows.map(row=>parseFloat(row.close_price)).reverse();
    closes._volumes = r.rows.map(row=>parseInt(row.volume||0)).reverse();
    return closes;
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
  if(n < 23) return { signal: 0, reason: 'insufficient data' };

  const price = closes[n-1];     // today's close
  const prev1 = closes[n-2];     // yesterday
  const prev2 = closes[n-3];     // two days ago
  // Lookback: 20 candles excluding the last 2 (so we check if both recent candles broke out)
  const lookback = closes.slice(-22, -2);
  const high20 = Math.max(...lookback);
  const low20 = Math.min(...lookback);
  const rsi = calcRSI(closes);
  const atr = calcATR(closes);
  const atrAvg = calcATR(closes.slice(0,-5));
  const atrExpansion = atr / (atrAvg || atr);
  const priceMove = Math.abs(price - prev1);
  const movePct = (priceMove / prev1) * 100;

  // Bullish breakout: BOTH last 2 candles close above 20-candle high (2-candle confirmation)
  if(price > high20 && prev1 > high20 && prev2 <= high20) {
    const rsiConfirm = rsi > 52;
    const volatilityExpanding = atrExpansion > 1.1 || movePct > 0.5;
    const strength = (rsiConfirm ? 1 : 0) + (volatilityExpanding ? 1 : 0) + (movePct > 1 ? 1 : 0);
    if(strength >= 1) {
      return { signal: strength + 1, direction: 'BUY',
        reason: `Breakout confirmed (2 candles) above ${high20.toFixed(0)} | RSI ${rsi.toFixed(0)} | ATR ×${atrExpansion.toFixed(1)}` };
    }
  }

  // Bearish breakout: BOTH last 2 candles close below 20-candle low (2-candle confirmation)
  if(price < low20 && prev1 < low20 && prev2 >= low20) {
    const rsiConfirm = rsi < 48;
    const volatilityExpanding = atrExpansion > 1.1 || movePct > 0.5;
    const strength = (rsiConfirm ? 1 : 0) + (volatilityExpanding ? 1 : 0) + (movePct > 1 ? 1 : 0);
    if(strength >= 1) {
      return { signal: strength + 1, direction: 'SELL',
        reason: `Breakdown confirmed (2 candles) below ${low20.toFixed(0)} | RSI ${rsi.toFixed(0)} | ATR ×${atrExpansion.toFixed(1)}` };
    }
  }

  // Show unconfirmed breakout in log for awareness (no signal)
  if(price < low20 && prev1 >= low20) {
    return { signal: 0, reason: `Unconfirmed breakdown below ${low20.toFixed(0)} — needs 2nd candle` };
  }
  if(price > high20 && prev1 <= high20) {
    return { signal: 0, reason: `Unconfirmed breakout above ${high20.toFixed(0)} — needs 2nd candle` };
  }

  return { signal: 0, reason: 'no breakout' };
}

function calcMomentum(closes) {
  const n = closes.length;
  if(n < 10) return { signal: 0 };
  const ret5 = (closes[n-1] - closes[n-6]) / closes[n-6] * 100;
  // Strong momentum continuation — >3% move in 5 days
  if(ret5 > 3.5) return { signal:2, direction:'BUY', reason:`5-day momentum +${ret5.toFixed(1)}%`, type:'trend' };
  if(ret5 < -3.5) return { signal:2, direction:'SELL', reason:`5-day momentum ${ret5.toFixed(1)}%`, type:'trend' };
  return { signal: 0 };
}

function calcSmaCrossover(closes) {
  const n = closes.length;
  if(n < 55) return { signal: 0 };
  const sma20now  = closes.slice(-20).reduce((a,b)=>a+b,0)/20;
  const sma50now  = closes.slice(-50).reduce((a,b)=>a+b,0)/50;
  const sma20prev = closes.slice(-21,-1).reduce((a,b)=>a+b,0)/20;
  const sma50prev = closes.slice(-51,-1).reduce((a,b)=>a+b,0)/50;
  // Golden cross: SMA20 crosses above SMA50
  if(sma20prev <= sma50prev && sma20now > sma50now)
    return { signal:3, direction:'BUY', reason:'Golden cross SMA20>SMA50', type:'trend' };
  // Death cross: SMA20 crosses below SMA50
  if(sma20prev >= sma50prev && sma20now < sma50now)
    return { signal:3, direction:'SELL', reason:'Death cross SMA20<SMA50', type:'trend' };
  return { signal: 0 };
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

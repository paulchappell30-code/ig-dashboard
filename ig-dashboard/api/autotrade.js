// Automated Trading Engine v4
// Features: Price history DB, regime detection, news sentiment, time filter,
// active position management, Kelly sizing, portfolio heat, sentiment divergence
const fetch = require('node-fetch');
const TD_CACHE_TTL = 30 * 60 * 1000; // 30 min Twelve Data cache TTL

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

const EPIC_MAP = {
  'FTSE 100':'IX.D.FTSE.DAILY.IP','S&P 500':'IX.D.SPTRD.DAILY.IP',
  'DAX 40':'IX.D.DAX.DAILY.IP','Dow Jones':'IX.D.DOW.DAILY.IP',
  'Brent Oil':'CC.D.LCO.USS.IP','GBP/USD':'CS.D.GBPUSD.MINI.IP',
  'EUR/USD':'CS.D.EURUSD.MINI.IP','USD/JPY':'CS.D.USDJPY.MINI.IP',
  'CAC 40':'IX.D.CAC.DAILY.IP',
  'Nikkei 225':'IX.D.NIKKEI.DAILY.IP',
  'Nasdaq':'IX.D.NASDAQ.CASH.IP',
  'Gold':'CS.D.USCGC.TODAY.IP',
  'Silver':'CS.D.USCSC.TODAY.IP',
  'Copper':'CS.D.COPPER.TODAY.IP',
  'EUR/GBP':'CS.D.EURGBP.MINI.IP',
};

const CORRELATION_GROUPS = {
  'IX.D.FTSE.DAILY.IP':'indices','IX.D.SPTRD.DAILY.IP':'indices',
  'IX.D.DAX.DAILY.IP':'indices','IX.D.DOW.DAILY.IP':'indices',
  'IX.D.CAC.DAILY.IP':'indices',
  'IX.D.NIKKEI.DAILY.IP':'indices',
  'IX.D.NASDAQ.CASH.IP':'indices',
  'CC.D.LCO.USS.IP':'commodities',
  'CS.D.USCSC.TODAY.IP':'commodities',
  'CS.D.COPPER.TODAY.IP':'commodities',
  'CS.D.EURGBP.MINI.IP':'fx',
  'CS.D.USCGC.TODAY.IP':'commodities',
  'CS.D.GBPUSD.MINI.IP':'fx','CS.D.EURUSD.MINI.IP':'fx','CS.D.USDJPY.MINI.IP':'fx',
};

const TRADING_HOURS = {
  indices:{open:7,close:16},
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
  eodClose:true,eodCloseTime:{h:16,m:15},
};

const priceCache = {};
const CACHE_TTL = 20*60*1000;
const MAX_CANDLES_IG = 10;

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
    try {
      // Use cache if fresh (saves ~144 credits/day vs fetching every 5 mins)
      if (globalThis._tdCache && globalThis._tdCache.data && Date.now() - globalThis._tdCache.ts < TD_CACHE_TTL) {
        tdSignals = globalThis._tdCache.data;
        L(`Twelve Data: using cached data (${Math.round((Date.now()-globalThis._tdCache.ts)/60000)}m old)`);
      } else {
        const TD_KEY = process.env.TWELVE_DATA_KEY;
        const TD_BASE = 'https://api.twelvedata.com';
        // Only 2 FX instruments — 6 API calls total, well within rate limit
        const TD_INSTRUMENTS = [
          { instr: 'GBP/USD', symbol: 'GBP/USD' },
          { instr: 'EUR/USD', symbol: 'EUR/USD' },
        ];
        const TD_INTERVALS = ['1h', '1day']; // Fetch both timeframes
        let tdLoaded = 0;
        const newTdSignals = {};
        for (const { instr, symbol } of TD_INSTRUMENTS) {
          try {
            // Fetch with retry on connection reset
            const tdGet = async (url) => {
              for(let i=0;i<2;i++){
                try{ return await fetch(url); }
                catch(e){ if(i===0) await new Promise(r=>setTimeout(r,1500)); else throw e; }
              }
            };
            const [rsiRes, macdRes] = await Promise.all([
              tdGet(`${TD_BASE}/rsi?symbol=${encodeURIComponent(symbol)}&interval=1h&time_period=14&apikey=${TD_KEY}`),
              tdGet(`${TD_BASE}/macd?symbol=${encodeURIComponent(symbol)}&interval=1h&fast_period=12&slow_period=26&signal_period=9&apikey=${TD_KEY}`),
            ]);
            const [rsiD, macdD] = await Promise.all([rsiRes.json(), macdRes.json()]);
            const rsiDaily = null; // Daily RSI removed to stay within 8 calls/min free tier
            if (rsiD.status === 'error') { L(`TD ${instr}: ${rsiD.message}`); continue; }
            const rsi = parseFloat(rsiD.values?.[0]?.rsi);
            const macd = parseFloat(macdD.values?.[0]?.macd);
            const macdSig = parseFloat(macdD.values?.[0]?.macd_signal);
            let sc = 0;
            if (rsi < 25) sc += 4; else if (rsi < 30) sc += 3; else if (rsi < 40) sc += 2;
            else if (rsi > 75) sc -= 4; else if (rsi > 70) sc -= 3; else if (rsi > 60) sc -= 2;
            if (macd !== null && macdSig !== null && !isNaN(macd) && !isNaN(macdSig) && isFinite(macd) && isFinite(macdSig)) { if (macd > macdSig) sc += 2; else sc -= 2; }
            // Multi-timeframe alignment bonus/penalty
            if (rsiDaily !== null) {
              const hourlyBull = rsi < 50; const dailyBull = rsiDaily < 50;
              if (hourlyBull === dailyBull) sc += 1; // Aligned — boost
              else sc -= 1; // Conflicting — reduce
            }
            newTdSignals[instr] = { score: sc, rsi, macd, macdCrossover: macd > macdSig ? 'bullish' : 'bearish' };
            L(`TD ${instr}: RSI=${rsi?.toFixed(1)} MACD=${macd?.toFixed(4)} score=${sc}`);
            tdLoaded++;
            await new Promise(r => setTimeout(r, 8000)); // 8s delay = stay under 8 calls/min
          } catch(e) { L(`TD ${instr}: ${e.message.replace(TD_KEY,'***')}`); }
        }
        if (!globalThis._tdCache) globalThis._tdCache = {};
        globalThis._tdCache.data = newTdSignals;
        globalThis._tdCache.ts = Date.now();
        tdSignals = newTdSignals;
        if(tdLoaded===0 && globalThis._tdCache?.data && Object.keys(globalThis._tdCache.data).length>0){
          L('Twelve Data: fetch failed — using stale cache');
          tdSignals=globalThis._tdCache.data;
        } else {
          L(`Twelve Data: ${tdLoaded} instruments fetched and cached for 30 mins`);
        }
      }
    } catch(e) { L('Twelve Data failed: ' + e.message); }
  }

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
  if(plPct>=cfg.dailyProfitLock){
    L(`PROFIT LOCK HIT (${plPct.toFixed(2)}%)`);
    await sendNotify('dca','✅ Daily Profit Locked',`P&L: +${plPct.toFixed(2)}%\nTarget: +${cfg.dailyProfitLock}%`);
    return res.status(200).json({action:'profit_lock_hit',log});
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
      // Close all open positions
      try {
        const posRes = await fetch(`${igBase}/positions`, {headers:{...igH,'Version':'1'}});
        const posData = await posRes.json();
        const positions = posData.positions || [];

        if(positions.length > 0){
          L(`EOD close: closing ${positions.length} position(s)`);
          for(const p of positions){
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
      let closes=await getDbPrices(epic,60,L);
      let src='DB';
      if(!closes||closes.length<5){
        const candles=await getIGPrices(epic,MAX_CANDLES_IG,igBase,igH);
        if(candles&&candles.length>=5){closes=candles.map(c=>c.close);src='IG';}
      }

      const newsAdj=getNewsAdj(instr,newsSentiment);
      let sentAdj=0; try { sentAdj=await getIGSentiment(epic,igBase,igH,L)||0; } catch(e) { sentAdj=0; }
      let sc=0,regime='unknown',tdAdj=0;

      if(closes&&closes.length>=5){
        L(`${instr}: ${closes.length} candles from ${src}`);
        regime=detectRegime(closes);
        sc=calcScore(closes,regime);
        if(tdSignals[instr]){
          tdAdj=Math.round(tdSignals[instr].score/2);
          L(`${instr}: TD adj ${tdAdj} (RSI:${tdSignals[instr].rsi?.toFixed(0)})`);
        }
      } else if(tdSignals[instr]){
        // No candles — use Twelve Data as primary signal
        sc=tdSignals[instr].score;
        src='TwelveData';
        L(`${instr}: no candles — TD primary signal (RSI:${tdSignals[instr].rsi?.toFixed(0)} score:${sc})`);
      } else {
        L(`${instr}: no data from any source — skip`);
        continue;
      }

      const safeNewsAdj = newsAdj||0; const safeSentAdj = sentAdj||0; const safeTdAdj = tdAdj||0;
      // Apply economic surprise score if available
      const calAdj = calSurprises[instr] || 0;
      if(calAdj !== 0) L(`${instr}: calendar surprise adj ${calAdj}`);
      const total=sc+safeNewsAdj+safeSentAdj+safeTdAdj+calAdj;
      L(`${instr}: score ${sc}+news${safeNewsAdj}+sent${safeSentAdj}+td${safeTdAdj}+cal${calAdj}=${total} regime:${regime}`);

      if(Math.abs(total)<=cfg.signalThreshold-1){L(`${instr}: score ${total} below threshold ${cfg.signalThreshold}`);continue;}

      // Mean reversion override for ranging regime
      const rsiForMR=calcRSI(closes);
      let meanReversion=false;
      let dir=total>0?'BUY':'SELL';
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
        reasons,rsi,sma20,sma50,macd,momentum:mom,lastClose:closes[closes.length-1],
        atr,suggestedSize:sz,bb,bbPos,src,candles:closes.length});
      if(grp)occupied.add(grp);
    }catch(e){L(`${instr}: ${e.message}`);}
  }

  if(!signals.length){L('No signals');return res.status(200).json({action:'no_signals',log});}
  signals.sort((a,b)=>Math.abs(b.score)-Math.abs(a.score));
  L(`${signals.length} signal(s)`);

  for(const sig of signals.slice(0,2)){
    let approved=!cfg.requireAIConfirm,confidence=100,reasoning='AI not required';
    if(cfg.requireAIConfirm){
      try{
        const air=await aiConfirm(sig,cfg,plPct,openPos.length,winRate,L);
        approved=air.approved;confidence=air.confidence;reasoning=air.reasoning;
      }catch(e){L('AI error: '+e.message);approved=true;}
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

    const finalSz = Math.max(0.01, Math.min(sig.suggestedSize, cfg.maxSizePerTrade));
    L(`Placing ${sig.direction} ${finalSz} on ${sig.instr} (regime:${sig.regime})...`);

    try{
      const ob={epic:sig.epic,direction:sig.direction,size:finalSz,orderType:'MARKET',
        expiry:'DFB',guaranteedStop:false,forceOpen:true,currencyCode:'GBP',dealType:'SPREADBET'};
      // ATR-based stop loss (1.5x ATR)
      const stopDist=sig.atr>0?Math.max(10,Math.round(sig.atr*1.5)):null;
      if(stopDist){ob.stopDistance=stopDist;L(`Stop loss: ${stopDist}pts (1.5x ATR)`);}
      if(cfg.trailingStopPct>0&&sig.atr>0){
        const sd=Math.max(10,sig.atr*2);
        ob.trailingStop=true;ob.trailingStopDistance=Math.round(sd);ob.trailingStopIncrement=Math.max(1,Math.round(sd/4));
        L(`Trailing stop: ${Math.round(sd)}pts`);
      }
      let ref;
      const or=await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(ob)});
      const od=await or.json();
      if(od.dealReference){ref=od.dealReference;}
      else{
        L('Retry without trailing stop: '+(od.errorCode||'?'));
        delete ob.trailingStop;delete ob.trailingStopDistance;delete ob.trailingStopIncrement;
        const r2=await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(ob)});
        const d2=await r2.json();
        if(!d2.dealReference){L('Retry failed: '+(d2.errorCode||'?'));continue;}
        ref=d2.dealReference;
      }
      await new Promise(r=>setTimeout(r,1500));
      const cr=await fetch(`${igBase}/confirms/${ref}`,{headers:{...igH,'Version':'1'}});
      const confirm=await cr.json();
      if(confirm.dealStatus==='ACCEPTED'){
        L(`✅ ACCEPTED ref:${ref} level:${confirm.level}`);
        await saveToDb('trade_opened',{dealId:confirm.dealId,dealReference:ref,
          instrument:sig.instr,epic:sig.epic,direction:sig.direction,size:finalSz,
          openLevel:confirm.level,signalScore:sig.score,aiConfidence:confidence,
          signalReasons:sig.reasons,regime:sig.regime,dataSource:sig.src});
        await sendNotify('dca',`✅ v3 Auto-Trade: ${sig.direction} ${sig.instr}`,
          `Instrument: ${sig.instr}\nDirection: ${sig.direction}\nSize: ${finalSz} units (Kelly)\nPrice: ${confirm.level}\nRef: ${ref}\n\nScore: ${sig.score} (raw${sig.rawScore}+news${sig.newsAdj}+sent${sig.sentAdj})\nRegime: ${sig.regime}\nAI: ${confidence}%\n${reasoning}\n\nSignals:\n${sig.reasons.join('\n')}\n\nP&L: ${plPct.toFixed(2)}% | Balance: £${balance}\nTime: ${new Date().toLocaleString('en-GB',{timeZone:'Europe/London'})}`);
        return res.status(200).json({action:'trade_placed',version:3,instrument:sig.instr,
          direction:sig.direction,size:finalSz,ref,level:confirm.level,aiConfidence:confidence,regime:sig.regime,log});
      }else{
        L(`Rejected: ${confirm.reason||confirm.dealStatus}`);
        await sendNotify('error',`❌ Order Rejected: ${sig.instr}`,`Reason: ${confirm.reason||confirm.dealStatus}`);
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
  if(!r.ok)return null;
  const d=await r.json();
  const candles=(d.prices||[]).map(p=>({open:p.openPrice?.bid||0,high:p.highPrice?.bid||0,low:p.lowPrice?.bid||0,close:p.closePrice?.bid||p.closePrice?.mid||0})).filter(c=>c.close>0);
  priceCache[key]={data:candles,ts:Date.now()};
  return candles;
}

// ── REGIME DETECTION ──────────────────────────────────────────────────────────
function detectRegime(closes){
  const n=closes.length;if(n<10)return 'unknown';
  const recent=closes.slice(-5);
  const recentRange=Math.max(...recent)-Math.min(...recent);
  const totalRange=Math.max(...closes)-Math.min(...closes);
  const trendStr=recentRange/(totalRange||1);
  const mid=Math.floor(n/2);
  const h1=closes.slice(0,mid).reduce((a,b)=>a+b,0)/mid;
  const h2=closes.slice(mid).reduce((a,b)=>a+b,0)/(n-mid);
  const slope=((h2-h1)/h1)*100;
  if(Math.abs(slope)>3&&trendStr>0.3)return slope>0?'uptrend':'downtrend';
  return 'ranging';
}

// ── SIGNAL SCORING ────────────────────────────────────────────────────────────
function calcScore(closes,regime){
  const n=closes.length;if(n<5)return 0;
  let s=0;
  const rsi=calcRSI(closes);
  const sma20=calcSMA(closes,Math.min(20,n));const sma50=calcSMA(closes,Math.min(50,n));
  const macd=calcEMA(closes,Math.min(12,n))-calcEMA(closes,Math.min(26,n));
  const mom=n>=10?((closes[n-1]-closes[n-10])/closes[n-10])*100:0;
  const bb=calcBB(closes);
  if(regime==='ranging'){
    if(rsi<25)s+=4;else if(rsi<35)s+=3;else if(rsi>75)s-=4;else if(rsi>65)s-=3;
    if(closes[n-1]<bb.lower)s+=3;else if(closes[n-1]>bb.upper)s-=3;
    if(mom>1)s+=1;else if(mom<-1)s-=1;
    if(sma20>sma50)s+=1;else s-=1;if(macd>0)s+=1;else s-=1;
  }else if(regime==='uptrend'){
    if(sma20>sma50)s+=3;else s-=3;
    if(mom>2)s+=3;else if(mom>1)s+=2;else if(mom<-1)s-=2;
    if(macd>0)s+=2;else s-=1;
    if(rsi<45)s+=2;else if(rsi>75)s-=2;
  }else if(regime==='downtrend'){
    if(sma20<sma50)s-=3;else s+=3;
    if(mom<-2)s-=3;else if(mom<-1)s-=2;else if(mom>1)s+=2;
    if(macd<0)s-=2;else s+=1;
    if(rsi>55)s-=2;
  }else{
    if(rsi<30)s+=2;else if(rsi<40)s+=1;else if(rsi>70)s-=2;else if(rsi>60)s-=1;
    if(sma20>sma50)s+=1;else s-=1;if(macd>0)s+=1;else s-=1;
    if(mom>1)s+=1;else if(mom<-1)s-=1;
    if(closes[n-1]<bb.lower)s+=1;else if(closes[n-1]>bb.upper)s-=1;
  }
  return s;
}

// ── KELLY SIZING ──────────────────────────────────────────────────────────────
function kellySize(winRate,balance,atr,price,cfg){
  const b=1;const kelly=Math.max(0,(b*winRate-(1-winRate))/b)/2;
  const risk=balance*Math.min(kelly,0.1);
  const stop=atr>0?atr*2:50;
  return Math.max(1,Math.min(Math.floor(risk/stop),cfg.maxSizePerTrade));
}

// ── IG SENTIMENT (contrarian) ─────────────────────────────────────────────────
async function getIGSentiment(epic,igBase,igH,L,base,instrName){
  const ids={
    'IX.D.FTSE.DAILY.IP':'FTSE','IX.D.SPTRD.DAILY.IP':'SPTRD','IX.D.DAX.DAILY.IP':'DAX',
    'IX.D.DOW.DAILY.IP':'DOW','CC.D.LCO.USS.IP':'LCO','CS.D.GBPUSD.MINI.IP':'GBPUSD',
    'CS.D.EURUSD.MINI.IP':'EURUSD','CS.D.USDJPY.MINI.IP':'USDJPY',
    'CS.D.USCGC.TODAY.IP':'GOLD','CS.D.USCSC.TODAY.IP':'SILVER',
    'CS.D.COPPER.TODAY.IP':'COPPER','CS.D.EURGBP.MINI.IP':'EURGBP',
  };
  const id=ids[epic];if(!id)return 0;
  try{
    const r=await fetch(`${igBase}/clientsentiment/${id}`,{headers:igH});
    if(!r.ok)return 0;const d=await r.json();
    if(!d||!d.clientSentimentList&&!d.longPositionPercentage) return 0;
    const sentiment = d.clientSentimentList?.[0] || d;
    const lp=sentiment.longPositionPercentage||d.longPositionPercentage||50;
    const sp=sentiment.shortPositionPercentage||d.shortPositionPercentage||(100-lp);
    // Save sentiment history to DB
    if(base && instrName){
      fetch(`${base}/api/db`,{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({type:'sentiment',data:{instrument:instrName,epic,longPct:lp,shortPct:sp}})
      }).catch(()=>{});
    }
    if(lp>70){L(`${id}: ${lp}% long — contrarian SELL`);return -2;}
    if(lp<30){L(`${id}: ${lp}% long — contrarian BUY`);return 2;}
    if(lp>60)return -1;if(lp<40)return 1;return 0;
  }catch(e){return 0;}
}

// ── NEWS SENTIMENT ────────────────────────────────────────────────────────────
async function fetchNews(L){
  try{
    const r=await fetch(`https://newsapi.org/v2/top-headlines?category=business&language=en&pageSize=20&apiKey=${process.env.NEWS_API_KEY}`);
    if(!r.ok)return {};
    const d=await r.json();
    const headlines=(d.articles||[]).map(a=>a.title).join('\n');
    if(!headlines)return {};
    const ar=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':process.env.ANTHROPIC_API_KEY||'','anthropic-version':'2023-06-01'},
      body:JSON.stringify({model:'claude-haiku-4-5-20251001',max_tokens:200,
        messages:[{role:'user',content:`Analyse these financial headlines and rate market sentiment. Use ONLY integers: -2, -1, 0, 1, or 2. No + signs.\n\nHeadlines:\n${headlines}\n\nRespond with ONLY this exact JSON (no other text, no + signs before numbers):\n{"FTSE":0,"SP500":0,"DAX":0,"DOW":0,"OIL":0,"GBPUSD":0,"EURUSD":0,"USDJPY":0,"summary":"one sentence summary"}`}]})});
    const ad=await ar.json();
    const t=ad.content&&ad.content[0]&&ad.content[0].text||'{}';
    // Clean up common JSON issues (+ signs before numbers, trailing commas)
    const cleaned=t.replace(/```json|```/g,'').trim()
      .replace(/:\s*\+?(\d)/g,': $1')  // Remove + before numbers
      .replace(/,\s*}/g,'}')             // Remove trailing commas
      .replace(/,\s*]/g,']');
    const sentiment=JSON.parse(cleaned);
    L('News: '+sentiment.summary);return sentiment;
  }catch(e){L('News error: '+e.message);return {};}
}

function getNewsAdj(instr,s){
  const m={'FTSE 100':s.FTSE,'S&P 500':s.SP500,'DAX 40':s.DAX,'Dow Jones':s.DOW,
    'Brent Oil':s.OIL,'GBP/USD':s.GBPUSD,'EUR/USD':s.EURUSD,'USD/JPY':s.USDJPY};
  return m[instr]||0;
}

// ── POSITION MANAGEMENT ───────────────────────────────────────────────────────
async function managePositions(positions,igBase,igH,cfg,balance,L){
  for(const p of positions){
    try{
      const epic=p.market.epic;const dir=p.position.direction;
      const open=p.position.openLevel;const bid=p.market.bid;
      const sz=p.position.size||p.position.dealSize||1;
      const upl=dir==='BUY'?(bid-open)*sz:(open-bid)*sz;
      const uplPct=open>0?Math.abs(bid-open)/open*100:0;
      L(`Managing ${p.market.instrumentName}: UPL £${upl.toFixed(2)} (${uplPct.toFixed(2)}%)`);

      // Get ATR for this instrument
      const closes=await getDbPrices(epic,20,L);
      const candles = closes ? closes.map(c=>({close:c,high:c*1.001,low:c*0.999})) : [];
      const atr = candles.length>=5 ? calcATR(candles) : 50;

      // Partial close: if profit >= 1x ATR, close 50% and let rest run
      if(upl > 0 && sz > 1) {
        const atrProfit = atr * sz;
        if(upl >= atrProfit && !p.position.partialClosed) {
          const halfSize = Math.floor(sz / 2);
          if(halfSize >= 1) {
            L(`${p.market.instrumentName}: profit ${upl.toFixed(2)} >= 1x ATR ${atrProfit.toFixed(2)} — partial close ${halfSize} units`);
            try {
              const closeBody = {epic,direction:dir==='BUY'?'SELL':'BUY',size:halfSize,
                orderType:'MARKET',expiry:'DFB',guaranteedStop:false,forceOpen:false,
                currencyCode:'GBP',dealType:'SPREADBET'};
              const cr = await fetch(`${igBase}/positions/otc`,{method:'POST',
                headers:{...igH,'Version':'1'},body:JSON.stringify(closeBody)});
              const cd = await cr.json();
              if(cd.dealReference) {
                L(`Partial close order placed: ref ${cd.dealReference}`);
                await saveToDb('engine_event',{eventType:'partial_close',instrument:p.market.instrumentName,
                  details:{dealId:p.position.dealId,halfSize,upl,atr}});
              }
            } catch(e) { L(`Partial close error: ${e.message}`); }
          }
        }
      }

      // Signal reversal check
      if(closes&&closes.length>=5){
        const regime=detectRegime(closes);const sc=calcScore(closes,regime);
        if((dir==='BUY'&&sc<=-3)||(dir==='SELL'&&sc>=3)){
          L(`${p.market.instrumentName}: signal reversed (${sc}) — consider closing`);
          await saveToDb('engine_event',{eventType:'close_recommendation',instrument:p.market.instrumentName,
            details:{upl,sc,regime,dealId:p.position.dealId}});
        }
      }
    }catch(e){ L(`Position management error: ${e.message}`); }
  }
}

// ── AI CONFIRMATION ───────────────────────────────────────────────────────────
async function aiConfirm(sig,cfg,plPct,openCount,winRate,L){
  L(`AI: ${sig.instr} ${sig.direction} (${sig.regime})...`);
  const regimeContext = sig.meanReversion
  ? `MEAN REVERSION trade: RSI ${sig.rsi.toFixed(1)} is ${sig.direction==='SELL'?'overbought (≥68)':'oversold (≤32)'} in ranging market — fading the extreme. Evaluate if RSI extreme is genuine and if there is support/resistance to trade back to.`
  : sig.regime==='ranging'
  ? `RANGING regime: Only approve if RSI is clearly extended (>65 or <35) OR score is very strong (≥4). Neutral momentum trades in ranging markets have poor expectancy.`
  : `TRENDING regime (${sig.regime}): Evaluate if direction aligns with trend and if entry timing is good.`;

const prompt=`Trading risk manager. Approve this spread bet?
INSTRUMENT:${sig.instr} DIRECTION:${sig.direction} REGIME:${sig.regime}${sig.meanReversion?' [MEAN REVERSION]':''}
SCORE:${sig.score}(raw:${sig.rawScore} news:${sig.newsAdj} sentiment:${sig.sentAdj})
RSI:${sig.rsi.toFixed(1)} SMA20/50:${sig.sma20.toFixed(0)}/${sig.sma50.toFixed(0)} MACD:${sig.macd.toFixed(2)} MOM:${sig.momentum.toFixed(2)}% BB:${sig.bbPos}
ATR:${sig.atr.toFixed(0)} DATA:${sig.candles} candles from ${sig.src}
WinRate:${(winRate*100).toFixed(1)}% P&L:${plPct.toFixed(2)}% OpenPos:${openCount}/${cfg.maxPositions}
Reasons: ${sig.reasons.join(', ')}
CONTEXT: ${regimeContext}
Respond ONLY: {"approved":true,"confidence":75,"reasoning":"2 sentences","risk":"main risk"}`;
  const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',
    headers:{'Content-Type':'application/json','x-api-key':process.env.ANTHROPIC_API_KEY||'','anthropic-version':'2023-06-01'},
    body:JSON.stringify({model:'claude-haiku-4-5-20251001',max_tokens:250,messages:[{role:'user',content:prompt}]})});
  const d=await r.json();
  const t=d.content&&d.content[0]&&d.content[0].text||'{}';
  const result=JSON.parse(t.replace(/```json|```/g,'').trim());
  const approved=result.approved===true&&(result.confidence||0)>=cfg.aiConfidenceMin;
  L(`AI:${approved?'✅':'❌'}(${result.confidence}%) ${result.reasoning}`);
  return{approved,confidence:result.confidence||0,reasoning:result.reasoning||''};
}

// ── HELPERS ───────────────────────────────────────────────────────────────────
async function closeAll(igBase,igH){
  let n=0;
  try{const r=await fetch(`${igBase}/positions`,{headers:{...igH,'Version':'1'}});
    const d=await r.json();
    for(const p of(d.positions||[])){
      try{await fetch(`${igBase}/positions/otc/${p.position.dealId}`,{method:'DELETE',headers:{...igH,'Version':'1'}});n++;await new Promise(r=>setTimeout(r,500));}catch(e){}
    }}catch(e){}
  return n;
}

function isMarketOpen(group){
  if(group==='fx') return true; // FX trades 24hrs
  const h=TRADING_HOURS[group]||{open:7,close:16};
  const u=new Date().getUTCHours();return u>=h.open&&u<h.close;
}

function isPreferredWindow(){
  const u=new Date().getUTCHours();
  return PREFERRED_WINDOWS.some(w=>u>=w.open&&u<w.close);
}

async function nearHighImpact(L){
  const h=new Date().getUTCHours(),m=new Date().getUTCMinutes();
  const times=[{h:7,m:0},{h:8,m:30},{h:9,m:0},{h:12,m:30},{h:14,m:0},{h:18,m:0}];
  for(const t of times){if(Math.abs((h*60+m)-(t.h*60+t.m))<=30){L(`Calendar: near ${t.h}:${String(t.m).padStart(2,'0')} UTC`);return true;}}
  return false;
}

async function saveToDb(type,data){
  try{const base=process.env.PRODUCTION_URL||`https://${process.env.VERCEL_URL}`;
    await fetch(`${base}/api/db`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type,data})});
  }catch(e){}}

async function sendNotify(type,subject,body){
  try{const base=process.env.PRODUCTION_URL||`https://${process.env.VERCEL_URL}`;
    await fetch(`${base}/api/notify`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type,subject,body})});
  }catch(e){}}

function calcRSI(c,p=14){
  const period=Math.min(p,c.length-1);if(period<2)return 50;
  let g=0,l=0;for(let i=c.length-period;i<c.length;i++){const d=c[i]-c[i-1];if(d>0)g+=d;else l+=Math.abs(d);}
  const ag=g/period,al=l/period;if(al===0)return 100;return 100-(100/(1+ag/al));}

function calcSMA(c,p){const n=Math.min(p,c.length);return c.slice(-n).reduce((a,b)=>a+b,0)/n;}

function calcEMA(c,p){const n=Math.min(p,c.length);if(n<2)return c[c.length-1];const k=2/(n+1);
  let e=c.slice(0,n).reduce((a,b)=>a+b,0)/n;for(let i=n;i<c.length;i++)e=c[i]*k+e*(1-k);return e;}

function calcBB(c,p=20){const n=Math.min(p,c.length);const sma=c.slice(-n).reduce((a,b)=>a+b,0)/n;
  const std=Math.sqrt(c.slice(-n).reduce((s,v)=>s+Math.pow(v-sma,2),0)/n);
  return{upper:sma+2*std,middle:sma,lower:sma-2*std};}

function calcATR(closes,p=14){
  const c=typeof closes[0]==='object'?closes:closes.map(v=>({close:v,high:v*1.001,low:v*0.999}));
  const n=Math.min(p,c.length-1);if(n<1)return 50;
  const trs=c.slice(-n).map((x,i,a)=>i===0?x.high-x.low:Math.max(x.high-x.low,Math.abs(x.high-a[i-1].close),Math.abs(x.low-a[i-1].close)));
  return trs.reduce((a,b)=>a+b,0)/trs.length;}

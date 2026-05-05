// Automated Trading Engine v3
// Features: Price history DB, regime detection, news sentiment, time filter,
// active position management, Kelly sizing, portfolio heat, sentiment divergence
const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

const EPIC_MAP = {
  'FTSE 100':'IX.D.FTSE.DAILY.IP','S&P 500':'IX.D.SPTRD.DAILY.IP',
  'DAX 40':'IX.D.DAX.DAILY.IP','Dow Jones':'IX.D.DOW.DAILY.IP',
  'Brent Oil':'CC.D.LCO.USS.IP','GBP/USD':'CS.D.GBPUSD.MINI.IP',
  'EUR/USD':'CS.D.EURUSD.MINI.IP','USD/JPY':'CS.D.USDJPY.MINI.IP',
};

const CORRELATION_GROUPS = {
  'IX.D.FTSE.DAILY.IP':'indices','IX.D.SPTRD.DAILY.IP':'indices',
  'IX.D.DAX.DAILY.IP':'indices','IX.D.DOW.DAILY.IP':'indices',
  'CC.D.LCO.USS.IP':'commodities',
  'CS.D.GBPUSD.MINI.IP':'fx','CS.D.EURUSD.MINI.IP':'fx','CS.D.USDJPY.MINI.IP':'fx',
};

const TRADING_HOURS = {
  indices:{open:7,close:16},
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
  };

  if(req.method==='GET') return res.status(200).json({status:'Auto-trading engine v3 ready',cfg,version:'3.0',time:new Date().toISOString()});
  if(!cfg.enabled) return res.status(200).json({message:'Auto-trading disabled'});

  const igBase=IG_BASES[process.env.IG_ENV||'demo'];
  let cst,xst;
  const log=[];
  const L=msg=>{console.log('[ATv3]',msg);log.push(msg);};

  L('=== Engine v3 === '+new Date().toLocaleString('en-GB',{timeZone:'Europe/London'}));

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
  if(await nearHighImpact(L)) return res.status(200).json({action:'calendar_block',log});

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
    if(!isMarketOpen(grp)){L(`${instr}: closed`);continue;}

    try{
      // Try DB first (free), fall back to IG API
      let closes=await getDbPrices(epic,60,L);
      let src='DB';
      if(!closes||closes.length<5){
        const candles=await getIGPrices(epic,MAX_CANDLES_IG,igBase,igH);
        if(!candles||candles.length<5){L(`${instr}: no data`);continue;}
        closes=candles.map(c=>c.close);src='IG';
      }
      L(`${instr}: ${closes.length} candles from ${src}`);

      const regime=detectRegime(closes);
      const sc=calcScore(closes,regime);
      const newsAdj=getNewsAdj(instr,newsSentiment);
      const sentAdj=await getIGSentiment(epic,igBase,igH,L);
      const total=sc+newsAdj+sentAdj;
      L(`${instr}: score ${sc}+news${newsAdj}+sent${sentAdj}=${total} regime:${regime}`);

      if(Math.abs(total)<cfg.signalThreshold){L(`${instr}: below threshold`);continue;}

      const dir=total>0?'BUY':'SELL';
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

      signals.push({instr,epic,direction:dir,score:total,rawScore:sc,newsAdj,sentAdj,regime,
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

    const sz=Math.max(1,Math.min(sig.suggestedSize,cfg.maxSizePerTrade));
    L(`Placing ${sig.direction} ${sz} on ${sig.instr} (regime:${sig.regime})...`);

    try{
      const ob={epic:sig.epic,direction:sig.direction,size:sz,orderType:'MARKET',
        expiry:'DFB',guaranteedStop:false,forceOpen:true,currencyCode:'GBP',dealType:'SPREADBET'};
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
          instrument:sig.instr,epic:sig.epic,direction:sig.direction,size:sz,
          openLevel:confirm.level,signalScore:sig.score,aiConfidence:confidence,
          signalReasons:sig.reasons,regime:sig.regime,dataSource:sig.src});
        await sendNotify('dca',`✅ v3 Auto-Trade: ${sig.direction} ${sig.instr}`,
          `Instrument: ${sig.instr}\nDirection: ${sig.direction}\nSize: ${sz} units (Kelly)\nPrice: ${confirm.level}\nRef: ${ref}\n\nScore: ${sig.score} (raw${sig.rawScore}+news${sig.newsAdj}+sent${sig.sentAdj})\nRegime: ${sig.regime}\nAI: ${confidence}%\n${reasoning}\n\nSignals:\n${sig.reasons.join('\n')}\n\nP&L: ${plPct.toFixed(2)}% | Balance: £${balance}\nTime: ${new Date().toLocaleString('en-GB',{timeZone:'Europe/London'})}`);
        return res.status(200).json({action:'trade_placed',version:3,instrument:sig.instr,
          direction:sig.direction,size:sz,ref,level:confirm.level,aiConfidence:confidence,regime:sig.regime,log});
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
async function getIGSentiment(epic,igBase,igH,L){
  const ids={'IX.D.FTSE.DAILY.IP':'FTSE','IX.D.SPTRD.DAILY.IP':'SPTRD','IX.D.DAX.DAILY.IP':'DAX',
    'IX.D.DOW.DAILY.IP':'DOW','CC.D.LCO.USS.IP':'LCO','CS.D.GBPUSD.MINI.IP':'GBPUSD',
    'CS.D.EURUSD.MINI.IP':'EURUSD','CS.D.USDJPY.MINI.IP':'USDJPY'};
  const id=ids[epic];if(!id)return 0;
  try{
    const r=await fetch(`${igBase}/clientsentiment/${id}`,{headers:igH});
    if(!r.ok)return 0;const d=await r.json();
    const lp=d.longPositionPercentage||50;
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
        messages:[{role:'user',content:`Rate sentiment -2 to +2 for each market based on these headlines:\n${headlines}\n\nRespond ONLY JSON: {"FTSE":0,"SP500":0,"DAX":0,"DOW":0,"OIL":0,"GBPUSD":0,"EURUSD":0,"USDJPY":0,"summary":"one sentence"}`}]})});
    const ad=await ar.json();
    const t=ad.content&&ad.content[0]&&ad.content[0].text||'{}';
    const sentiment=JSON.parse(t.replace(/```json|```/g,'').trim());
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
      L(`Managing ${p.market.instrumentName}: UPL £${upl.toFixed(2)}`);
      const closes=await getDbPrices(epic,20,L);
      if(closes&&closes.length>=5){
        const regime=detectRegime(closes);const sc=calcScore(closes,regime);
        if((dir==='BUY'&&sc<=-3)||(dir==='SELL'&&sc>=3)){
          L(`${p.market.instrumentName}: signal reversed (${sc}) — logging recommendation`);
          await saveToDb('engine_event',{eventType:'close_recommendation',instrument:p.market.instrumentName,details:{upl,sc,regime}});
        }
      }
    }catch(e){}
  }
}

// ── AI CONFIRMATION ───────────────────────────────────────────────────────────
async function aiConfirm(sig,cfg,plPct,openCount,winRate,L){
  L(`AI: ${sig.instr} ${sig.direction} (${sig.regime})...`);
  const prompt=`Trading risk manager. Approve this trade?
INSTRUMENT:${sig.instr} DIRECTION:${sig.direction} REGIME:${sig.regime}
SCORE:${sig.score}(raw:${sig.rawScore} news:${sig.newsAdj} sentiment:${sig.sentAdj})
RSI:${sig.rsi.toFixed(1)} SMA20/50:${sig.sma20.toFixed(0)}/${sig.sma50.toFixed(0)} MACD:${sig.macd.toFixed(2)} MOM:${sig.momentum.toFixed(2)}% BB:${sig.bbPos}
ATR:${sig.atr.toFixed(0)} DATA:${sig.candles} candles from ${sig.src}
WinRate:${(winRate*100).toFixed(1)}% P&L:${plPct.toFixed(2)}% OpenPos:${openCount}/${cfg.maxPositions}
Reasons: ${sig.reasons.join(', ')}
Is this aligned with the ${sig.regime} regime? Does news/sentiment support it?
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
  const h=TRADING_HOURS[group]||{open:0,close:24};
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

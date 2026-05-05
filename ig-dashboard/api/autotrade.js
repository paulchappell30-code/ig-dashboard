// Automated Trading Engine v2
// Multi-timeframe, trailing stops, ATR sizing, calendar filter, DB journaling
const fetch = require('node-fetch');

const IG_BASES = { live: 'https://api.ig.com/gateway/deal', demo: 'https://demo-api.ig.com/gateway/deal' };

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

const MARKET_HOURS = {
  indices:{open:7,close:16}, commodities:{open:1,close:23}, fx:{open:0,close:24}
};

const DEFAULT_CONFIG = {
  dailyProfitLock:2.0,dailyLossLimit:1.0,maxDrawdownPct:5.0,
  maxPositions:3,defaultSize:1,maxSizePerTrade:5,
  requireAIConfirm:true,aiConfidenceMin:60,enabled:true,
  trailingStopPct:1.5,signalThreshold:3,multiTimeframe:true,calendarEnabled:true,
};

const priceCache = {};
const CACHE_TTL = 20*60*1000;

module.exports = async (req,res) => {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type,Authorization');
  if(req.method==='OPTIONS') return res.status(200).end();

  const cronSecret = process.env.CRON_SECRET||'';
  if(req.method==='POST'&&cronSecret){
    const token=(req.headers['authorization']||'').replace('Bearer ','').trim();
    const ref=req.headers['referer']||req.headers['origin']||'';
    const fromDash=ref.includes('vercel.app')||ref.includes('localhost');
    if(!fromDash&&token!==cronSecret) return res.status(401).json({error:'Unauthorised'});
  }

  const config={...DEFAULT_CONFIG,
    dailyProfitLock:parseFloat(process.env.DAILY_PROFIT_LOCK||DEFAULT_CONFIG.dailyProfitLock),
    dailyLossLimit:parseFloat(process.env.DAILY_LOSS_LIMIT||DEFAULT_CONFIG.dailyLossLimit),
    maxPositions:parseInt(process.env.MAX_POSITIONS||DEFAULT_CONFIG.maxPositions),
    defaultSize:parseInt(process.env.DEFAULT_SIZE||DEFAULT_CONFIG.defaultSize),
    maxSizePerTrade:parseInt(process.env.MAX_SIZE_PER_TRADE||DEFAULT_CONFIG.maxSizePerTrade),
    requireAIConfirm:process.env.REQUIRE_AI_CONFIRM!=='false',
    aiConfidenceMin:parseInt(process.env.AI_CONFIDENCE_MIN||DEFAULT_CONFIG.aiConfidenceMin),
    enabled:process.env.AUTO_TRADING_ENABLED!=='false',
    trailingStopPct:parseFloat(process.env.TRAILING_STOP_PCT||DEFAULT_CONFIG.trailingStopPct),
    signalThreshold:parseInt(process.env.SIGNAL_THRESHOLD||DEFAULT_CONFIG.signalThreshold),
    multiTimeframe:process.env.MULTI_TIMEFRAME!=='false',
    calendarEnabled:process.env.CALENDAR_FILTER!=='false',
  };

  if(req.method==='GET') return res.status(200).json({status:'Auto-trading engine v2 ready',config,version:'2.0',time:new Date().toISOString()});
  if(!config.enabled) return res.status(200).json({message:'Auto-trading disabled',config});

  const igBase=IG_BASES[process.env.IG_ENV||'demo'];
  let cst,xst;
  const log=[];
  const addLog=msg=>{console.log('[ATv2]',msg);log.push(msg);};

  addLog('Engine v2 — '+new Date().toLocaleString('en-GB',{timeZone:'Europe/London'}));

  // Auth
  try{
    const ar=await fetch(`${igBase}/session`,{method:'POST',
      headers:{'Content-Type':'application/json','X-IG-API-KEY':process.env.IG_API_KEY||'','Version':'2'},
      body:JSON.stringify({identifier:process.env.IG_USERNAME,password:process.env.IG_PASSWORD})});
    if(!ar.ok) return res.status(500).json({error:'IG auth failed',log});
    cst=ar.headers.get('CST'); xst=ar.headers.get('X-SECURITY-TOKEN');
    if(!cst) return res.status(500).json({error:'No CST',log});
  }catch(e){return res.status(500).json({error:'Auth: '+e.message,log});}

  const igH={'Content-Type':'application/json','X-IG-API-KEY':process.env.IG_API_KEY||'','CST':cst,'X-SECURITY-TOKEN':xst};

  // Account
  let balance,dailyPL,available;
  try{
    const ar=await fetch(`${igBase}/accounts`,{headers:{...igH,'Version':'1'}});
    const ad=await ar.json();
    const acct=ad.accounts&&ad.accounts.find(a=>a.accountType==='SPREADBET');
    if(!acct){addLog('No spreadbet account');return res.status(200).json({log});}
    balance=acct.balance.balance; dailyPL=acct.balance.profitLoss; available=acct.balance.available;
    addLog(`Account: £${balance} | P&L: £${dailyPL} | Available: £${available}`);
    await saveToDb('equity_snapshot',{balance,profitLoss:dailyPL,available});
  }catch(e){addLog('Account error: '+e.message);return res.status(200).json({log});}

  const plPct=balance>0?(dailyPL/balance)*100:0;
  addLog(`P&L: ${plPct.toFixed(2)}% | Lock: +${config.dailyProfitLock}% | Limit: -${config.dailyLossLimit}%`);

  if(plPct<=-config.dailyLossLimit){
    addLog(`LOSS LIMIT HIT (${plPct.toFixed(2)}%) — closing all`);
    const closed=await closeAll(igBase,igH);
    await sendNotify('error','🛑 Daily Loss Limit Hit',`P&L: ${plPct.toFixed(2)}%\nClosed: ${closed} positions`);
    return res.status(200).json({action:'loss_limit_hit',closed,log});
  }
  if(plPct>=config.dailyProfitLock){
    addLog(`PROFIT LOCK HIT (${plPct.toFixed(2)}%) — no new trades`);
    await sendNotify('dca','✅ Daily Profit Locked',`P&L: +${plPct.toFixed(2)}%`);
    return res.status(200).json({action:'profit_lock_hit',log});
  }

  // Positions
  let openPos=[];
  try{
    const pr=await fetch(`${igBase}/positions`,{headers:{...igH,'Version':'1'}});
    const pd=await pr.json(); openPos=pd.positions||[];
    addLog(`Open: ${openPos.length}/${config.maxPositions}`);
  }catch(e){addLog('Positions error: '+e.message);}

  if(openPos.length>=config.maxPositions){
    addLog('Max positions — skip');
    return res.status(200).json({action:'max_positions',log});
  }

  // Calendar check
  if(config.calendarEnabled&&await nearHighImpactTime(addLog)){
    return res.status(200).json({action:'calendar_block',log});
  }

  // Signals
  const occupied=new Set(openPos.map(p=>CORRELATION_GROUPS[p.market.epic]).filter(Boolean));
  const signals=[];

  for(const instr of Object.keys(EPIC_MAP)){
    const epic=EPIC_MAP[instr];
    const grp=CORRELATION_GROUPS[epic];
    if(openPos.some(p=>p.market.epic===epic)){addLog(`${instr}: position open`);continue;}
    if(grp&&occupied.has(grp)){addLog(`${instr}: group '${grp}' occupied`);continue;}
    if(!isOpen(grp)){addLog(`${instr}: market closed`);continue;}

    try{
      const daily=await getPrices(epic,'DAY',60,igBase,igH);
      if(!daily||daily.length<10){addLog(`${instr}: insufficient data`);continue;}

      const dc=daily.map(c=>c.close);
      const ds=score(dc);
      let cs=ds;

      if(config.multiTimeframe){
        const hourly=await getPrices(epic,'HOUR',48,igBase,igH);
        if(hourly&&hourly.length>=10){
          const hc=hourly.map(c=>c.close);
          const hs=score(hc);
          addLog(`${instr}: daily ${ds}, hourly ${hs}`);
          if(Math.sign(ds)!==Math.sign(hs)){addLog(`${instr}: timeframe conflict — skip`);continue;}
          cs=Math.floor((ds+hs)/2);
        }
      }

      if(Math.abs(cs)<config.signalThreshold){addLog(`${instr}: score ${cs} below threshold`);continue;}

      const direction=cs>0?'BUY':'SELL';
      const atr=calcATR(daily,14);
      const sz=calcSize(balance,atr,dc[dc.length-1],config);
      const rsi=calcRSI(dc,14);
      const sma20=calcSMA(dc,20),sma50=calcSMA(dc,50);
      const ema12=calcEMA(dc,12),ema26=calcEMA(dc,26);
      const macd=ema12-ema26;
      const mom=dc.length>=10?((dc[dc.length-1]-dc[dc.length-10])/dc[dc.length-10])*100:0;
      const bb=calcBB(dc,20);
      const bbPos=dc[dc.length-1]<bb.lower?'below lower':dc[dc.length-1]>bb.upper?'above upper':'within bands';
      const reasons=[];
      if(rsi<35)reasons.push(`RSI oversold (${rsi.toFixed(1)})`);
      else if(rsi>65)reasons.push(`RSI overbought (${rsi.toFixed(1)})`);
      reasons.push(sma20>sma50?'SMA bullish':'SMA bearish');
      reasons.push(macd>0?'MACD positive':'MACD negative');
      if(Math.abs(mom)>1)reasons.push(`Momentum ${mom.toFixed(1)}%`);
      reasons.push(`Bollinger: ${bbPos}`);

      addLog(`${instr}: score ${cs} → ${direction} | ATR ${atr.toFixed(0)} | size ${sz}`);
      signals.push({instr,epic,direction,score:cs,reasons,rsi,sma20,sma50,macd,momentum:mom,lastClose:dc[dc.length-1],atr,suggestedSize:sz,bb,bbPos});
      if(grp)occupied.add(grp);
    }catch(e){addLog(`${instr}: ${e.message}`);}
  }

  if(!signals.length){addLog('No signals');return res.status(200).json({action:'no_signals',log});}
  signals.sort((a,b)=>Math.abs(b.score)-Math.abs(a.score));
  addLog(`${signals.length} signal(s)`);

  for(const sig of signals.slice(0,2)){
    let approved=!config.requireAIConfirm,confidence=100,reasoning='AI not required';
    if(config.requireAIConfirm){
      try{
        const air=await aiConfirm(sig,config,plPct,openPos.length,addLog);
        approved=air.approved; confidence=air.confidence; reasoning=air.reasoning;
      }catch(e){addLog('AI error: '+e.message);approved=true;}
    }
    if(!approved){addLog(`${sig.instr}: AI rejected (${confidence}%)`);continue;}

    const sz=Math.min(sig.suggestedSize||config.defaultSize,config.maxSizePerTrade);
    addLog(`Placing ${sig.direction} ${sz} on ${sig.instr}...`);

    try{
      const ob={epic:sig.epic,direction:sig.direction,size:sz,orderType:'MARKET',
        expiry:'DFB',guaranteedStop:false,forceOpen:true,currencyCode:'GBP',dealType:'SPREADBET'};

      if(config.trailingStopPct>0){
        const stopDist=Math.max(10,sig.atr*2);
        ob.trailingStop=true; ob.trailingStopDistance=Math.round(stopDist);
        ob.trailingStopIncrement=Math.max(1,Math.round(stopDist/4));
        addLog(`Trailing stop: ${Math.round(stopDist)} pts`);
      }

      let ref;
      const or=await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(ob)});
      const od=await or.json();
      if(od.dealReference){ref=od.dealReference;}
      else{
        addLog('First attempt failed: '+od.errorCode+' — retrying without trailing stop');
        delete ob.trailingStop; delete ob.trailingStopDistance; delete ob.trailingStopIncrement;
        const r2=await fetch(`${igBase}/positions/otc`,{method:'POST',headers:{...igH,'Version':'1'},body:JSON.stringify(ob)});
        const d2=await r2.json();
        if(!d2.dealReference){addLog('Retry failed: '+d2.errorCode);continue;}
        ref=d2.dealReference;
      }

      await new Promise(r=>setTimeout(r,1000));
      const cr=await fetch(`${igBase}/confirms/${ref}`,{headers:{...igH,'Version':'1'}});
      const confirm=await cr.json();

      if(confirm.dealStatus==='ACCEPTED'){
        addLog(`✅ ACCEPTED ref:${ref} at ${confirm.level}`);
        await saveToDb('trade_opened',{dealId:confirm.dealId,dealReference:ref,
          instrument:sig.instr,epic:sig.epic,direction:sig.direction,size:sz,
          openLevel:confirm.level,signalScore:sig.score,aiConfidence:confidence,signalReasons:sig.reasons});
        await sendNotify('dca',`✅ Auto-Trade v2: ${sig.direction} ${sig.instr}`,
          `Instrument: ${sig.instr}\nDirection: ${sig.direction}\nSize: ${sz} units\nPrice: ${confirm.level}\nRef: ${ref}\n\nScore: ${sig.score}/7\nAI: ${confidence}%\n${reasoning}\n\nSignals:\n${sig.reasons.join('\n')}\n\nDaily P&L: ${plPct.toFixed(2)}%`);
        return res.status(200).json({action:'trade_placed',instrument:sig.instr,direction:sig.direction,size:sz,dealReference:ref,level:confirm.level,aiConfidence:confidence,log});
      }else{
        addLog(`Rejected: ${confirm.reason||confirm.dealStatus}`);
      }
    }catch(e){addLog('Trade error: '+e.message);}
  }

  addLog('No trades placed');
  return res.status(200).json({action:'no_trades',log});
};

// Helpers
async function closeAll(igBase,igH){
  let n=0;
  try{
    const r=await fetch(`${igBase}/positions`,{headers:{...igH,'Version':'1'}});
    const d=await r.json();
    for(const p of(d.positions||[])){
      try{await fetch(`${igBase}/positions/otc/${p.position.dealId}`,{method:'DELETE',headers:{...igH,'Version':'1'}});n++;await new Promise(r=>setTimeout(r,500));}catch(e){}
    }
  }catch(e){}
  return n;
}

async function nearHighImpactTime(addLog){
  const now=new Date();
  const h=now.getUTCHours(),m=now.getUTCMinutes();
  const times=[{h:7,m:0},{h:8,m:30},{h:9,m:0},{h:12,m:30},{h:14,m:0},{h:18,m:0}];
  for(const t of times){
    if(Math.abs((h*60+m)-(t.h*60+t.m))<=30){
      addLog(`Calendar: near high-impact time ${t.h}:${String(t.m).padStart(2,'0')} UTC — skip`);
      return true;
    }
  }
  return false;
}

function isOpen(group){
  const h=MARKET_HOURS[group]||{open:0,close:24};
  const u=new Date().getUTCHours();
  return u>=h.open&&u<h.close;
}

function score(closes){
  const n=closes.length;
  if(n<5)return 0;
  let s=0;
  const rsi=calcRSI(closes,14);
  const sma20=calcSMA(closes,Math.min(20,n));
  const sma50=calcSMA(closes,Math.min(50,n));
  const macd=calcEMA(closes,12)-calcEMA(closes,26);
  const mom=n>=10?((closes[n-1]-closes[n-10])/closes[n-10])*100:0;
  const bb=calcBB(closes,Math.min(20,n));
  if(rsi<30)s+=3;else if(rsi<40)s+=2;else if(rsi>70)s-=3;else if(rsi>60)s-=2;
  if(sma20>sma50)s+=1;else s-=1;
  if(macd>0)s+=1;else s-=1;
  if(mom>2)s+=2;else if(mom>1)s+=1;else if(mom<-2)s-=2;else if(mom<-1)s-=1;
  if(closes[n-1]<bb.lower)s+=2;else if(closes[n-1]>bb.upper)s-=2;
  return s;
}

function calcATR(candles,period=14){
  if(candles.length<period+1)return 50;
  const trs=candles.slice(-period).map((c,i,a)=>i===0?c.high-c.low:Math.max(c.high-c.low,Math.abs(c.high-a[i-1].close),Math.abs(c.low-a[i-1].close)));
  return trs.reduce((a,b)=>a+b,0)/period;
}

function calcSize(balance,atr,price,config){
  if(!atr||!price||atr===0)return config.defaultSize;
  const risk=balance*0.01;
  const stop=atr*2;
  return Math.max(1,Math.min(Math.floor(risk/stop),config.maxSizePerTrade));
}

async function aiConfirm(sig,config,plPct,openCount,addLog){
  addLog(`AI confirm: ${sig.instr} ${sig.direction}...`);
  const prompt=`Trading risk manager. Approve this trade?
INSTRUMENT:${sig.instr} DIRECTION:${sig.direction} SCORE:${sig.score} RSI:${sig.rsi.toFixed(1)} SMA20/50:${sig.sma20.toFixed(0)}/${sig.sma50.toFixed(0)} MACD:${sig.macd.toFixed(2)} MOM:${sig.momentum.toFixed(2)}% BOLLINGER:${sig.bbPos} ATR:${sig.atr.toFixed(0)}
Daily P&L:${plPct.toFixed(2)}% Lock:+${config.dailyProfitLock}% Limit:-${config.dailyLossLimit}% OpenPos:${openCount}/${config.maxPositions}
Respond ONLY:{"approved":true,"confidence":75,"reasoning":"reason","risk":"risk"}`;
  const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',
    headers:{'Content-Type':'application/json','x-api-key':process.env.ANTHROPIC_API_KEY||'','anthropic-version':'2023-06-01'},
    body:JSON.stringify({model:'claude-haiku-4-5-20251001',max_tokens:200,messages:[{role:'user',content:prompt}]})});
  const d=await r.json();
  const t=d.content&&d.content[0]&&d.content[0].text||'{}';
  const result=JSON.parse(t.replace(/```json|```/g,'').trim());
  const approved=result.approved===true&&(result.confidence||0)>=config.aiConfidenceMin;
  addLog(`AI:${approved?'✅':'❌'}(${result.confidence}%) ${result.reasoning}`);
  return{approved,confidence:result.confidence||0,reasoning:result.reasoning||''};
}

async function getPrices(epic,resolution,count,igBase,igH){
  const key=`${epic}_${resolution}_${count}`;
  const cached=priceCache[key];
  if(cached&&Date.now()-cached.ts<CACHE_TTL)return cached.data;
  const r=await fetch(`${igBase}/prices/${epic}?resolution=${resolution}&max=${count}&pageSize=0`,{headers:{...igH,'Version':'3'}});
  if(!r.ok)return null;
  const d=await r.json();
  const candles=(d.prices||[]).map(p=>({open:p.openPrice?.bid||0,high:p.highPrice?.bid||0,low:p.lowPrice?.bid||0,close:p.closePrice?.bid||p.closePrice?.mid||0,time:p.snapshotTime})).filter(c=>c.close>0);
  priceCache[key]={data:candles,ts:Date.now()};
  return candles;
}

async function saveToDb(type,data){
  try{
    const base=process.env.PRODUCTION_URL||`https://ig-dashboard-roan.vercel.app`;
    await fetch(`${base}/api/db`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type,data})});
  }catch(e){}
}

async function sendNotify(type,subject,body){
  try{
    const base=process.env.PRODUCTION_URL||`https://ig-dashboard-roan.vercel.app`;
    await fetch(`${base}/api/notify`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type,subject,body})});
  }catch(e){}
}

function calcRSI(c,p=14){if(c.length<p+1)return 50;let g=0,l=0;for(let i=c.length-p;i<c.length;i++){const d=c[i]-c[i-1];if(d>0)g+=d;else l+=Math.abs(d);}const ag=g/p,al=l/p;if(al===0)return 100;return 100-(100/(1+ag/al));}
function calcSMA(c,p){const n=Math.min(p,c.length);return c.slice(-n).reduce((a,b)=>a+b,0)/n;}
function calcEMA(c,p){if(c.length<p)return c[c.length-1];const k=2/(p+1);let e=c.slice(0,p).reduce((a,b)=>a+b,0)/p;for(let i=p;i<c.length;i++)e=c[i]*k+e*(1-k);return e;}
function calcBB(c,p=20){const n=Math.min(p,c.length);const sma=c.slice(-n).reduce((a,b)=>a+b,0)/n;const std=Math.sqrt(c.slice(-n).reduce((s,v)=>s+Math.pow(v-sma,2),0)/n);return{upper:sma+2*std,middle:sma,lower:sma-2*std};}

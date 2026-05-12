// Review & Optimisation Engine v4
// GET  /api/review?action=weekly    — weekly performance review email
// GET  /api/review?action=optimize  — run walk-forward optimisation
// POST /api/review                  — trigger manually
const fetch = require('node-fetch');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || req.body?.action || 'weekly';
  if (action === 'optimize') return await runOptimization(req, res);
  return await sendWeeklyReview(req, res);
};

async function sendWeeklyReview(req, res) {
  try {
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;
    const [statsRes, tradesRes, calRes, todRes] = await Promise.all([
      fetch(`${base}/api/db?action=stats`),
      fetch(`${base}/api/db?action=trades&limit=50`),
      fetch(`${base}/api/db?action=calibration`),
      fetch(`${base}/api/db?action=timeofday`),
    ]);
    const [stats, tradesData, calData, todData] = await Promise.all([
      statsRes.json(), tradesRes.json(), calRes.json(), todRes.json()
    ]);

    if (!stats.totalTrades) return res.status(200).json({ message: 'No trades yet' });

    const trades = tradesData.trades || [];
    const oneWeekAgo = new Date(Date.now() - 7*24*60*60*1000);
    const weekClosed = trades.filter(t => t.status==='closed' && new Date(t.opened_at) > oneWeekAgo);
    const weekPnL = weekClosed.reduce((s,t) => s+(t.profit_loss||0), 0);
    const weekWins = weekClosed.filter(t => (t.profit_loss||0) > 0).length;
    const weekWinRate = weekClosed.length > 0 ? ((weekWins/weekClosed.length)*100).toFixed(1) : 'N/A';

    const byInstr = {};
    weekClosed.forEach(t => {
      if (!byInstr[t.instrument]) byInstr[t.instrument] = {trades:0,pnl:0,wins:0};
      byInstr[t.instrument].trades++;
      byInstr[t.instrument].pnl += t.profit_loss||0;
      if((t.profit_loss||0) > 0) byInstr[t.instrument].wins++;
    });

    const calRows = calData.calibration || [];
    const calSummary = calRows.map(r => `  ${r.confidence_bracket}: ${r.total_trades} trades, ${r.actual_win_rate}% actual win rate`).join('\n');
    const todRows = todData.byHour || [];
    const bestHour = todData.bestHour;
    const worstHour = todData.worstHour;

    let aiAnalysis = '';
    if (process.env.ANTHROPIC_API_KEY && weekClosed.length > 0) {
      const tradeDetails = weekClosed.slice(0,15).map(t =>
        `${t.instrument} ${t.direction}: ${t.profit_loss>=0?'+':''}£${(t.profit_loss||0).toFixed(2)} | AI:${t.ai_confidence||'?'}% | Regime:${t.regime||'?'} | Hour:${t.open_hour??'?'}:00`
      ).join('\n');

      const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
        method:'POST',
        headers:{'Content-Type':'application/json','x-api-key':process.env.ANTHROPIC_API_KEY,'anthropic-version':'2023-06-01'},
        body:JSON.stringify({model:'claude-sonnet-4-5',max_tokens:600,messages:[{role:'user',content:
          `Trading performance coach. Analyse week results, give specific recommendations.
WEEK: ${weekClosed.length} trades | WR: ${weekWinRate}% | P&L: £${weekPnL.toFixed(2)}
AI CALIBRATION:\n${calSummary||'Insufficient data'}
BEST HOUR: ${bestHour?.open_hour??'N/A'}:00 UTC (avg £${parseFloat(bestHour?.avg_pnl||0).toFixed(2)})
WORST HOUR: ${worstHour?.open_hour??'N/A'}:00 UTC (avg £${parseFloat(worstHour?.avg_pnl||0).toFixed(2)})
TRADES:\n${tradeDetails}
Give: 1)Key patterns 2)AI calibration quality 3)Recommended threshold adjustment 4)Recommended confidence minimum 5)Best trading hours. Under 400 words, specific numbers.`}]})
      });
      const aiData = await aiRes.json();
      aiAnalysis = aiData.content?.[0]?.text || '';
    }

    const emailBody = `WEEKLY TRADING REPORT v4
${new Date().toLocaleDateString('en-GB',{weekday:'long',day:'numeric',month:'long',year:'numeric'})}

WEEK: ${weekClosed.length} trades | ${weekWinRate}% win rate | ${weekPnL>=0?'+':''}£${weekPnL.toFixed(2)}
ALL-TIME: ${stats.totalTrades} trades | ${stats.winRate}% WR | £${stats.totalPnL.toFixed(2)} P&L
AI ACCURACY: ${stats.aiAccuracy||'N/A'}% on ${stats.aiTotal} trades

AI CALIBRATION:
${calSummary||'Need more trades'}

BEST HOURS (UTC):
${todRows.slice(0,6).map(r=>`  ${r.open_hour}:00 — ${r.total_trades} trades, ${r.win_rate}% WR, avg £${parseFloat(r.avg_pnl).toFixed(2)}`).join('\n')||'Need more data'}

${aiAnalysis?'AI ANALYSIS:\n'+aiAnalysis:''}${priceSection}

Generated: ${new Date().toLocaleString('en-GB',{timeZone:'Europe/London'})}`;

    // Run weekly price analysis
    const priceAnalysis = await runWeeklyPriceAnalysis(base, process.env.ANTHROPIC_API_KEY);
    const priceSection = priceAnalysis ? `
WEEKLY MARKET OBSERVATIONS:
${priceAnalysis.marketData.map(m=>`  ${m.name}: RSI ${m.rsi} | ${m.regime} | Week ${m.weekChg} | Month ${m.monthChg}`).join('\n')}

${priceAnalysis.oversold.length ? 'OVERSOLD: '+priceAnalysis.oversold.map(m=>m.name+' RSI:'+m.rsi).join(', ') : ''}
${priceAnalysis.overbought.length ? 'OVERBOUGHT: '+priceAnalysis.overbought.map(m=>m.name+' RSI:'+m.rsi).join(', ') : ''}

MARKET INTELLIGENCE:
${priceAnalysis.analysis}
${priceAnalysis.sentimentShifts?.length ? '\nSENTIMENT SHIFTS:\n' + priceAnalysis.sentimentShifts.join('\n') : ''}` : '';

    const notifyRes = await fetch(`${base}/api/notify`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({type:'daily_summary',subject:`📊 Weekly Report — ${weekPnL>=0?'+':''}£${weekPnL.toFixed(2)} | ${weekWinRate}% WR`,body:emailBody})});
    const notifyData = await notifyRes.json();
    return res.status(200).json({success:true,weekTrades:weekClosed.length,weekPnL:weekPnL.toFixed(2),weekWinRate,emailSent:notifyData.sent});
  } catch(e) { return res.status(500).json({error:e.message}); }
}

async function runOptimization(req, res) {
  const log = [];
  const L = msg => { console.log('[Optimize]', msg); log.push(msg); };
  L('Walk-forward optimisation v4 started');
  try {
    const { sql } = require('@vercel/postgres');
    const base = process.env.PRODUCTION_URL || `https://${process.env.VERCEL_URL}`;

    const priceResult = await sql`
      SELECT epic, close_price, candle_time FROM price_history
      WHERE resolution='DAY' AND close_price>0 AND candle_time > NOW() - INTERVAL '30 days'
      ORDER BY epic, candle_time ASC
    `;

    if (priceResult.rows.length < 50) {
      L(`Only ${priceResult.rows.length} candles — need 50+ for optimisation`);
      return res.status(200).json({message:'Insufficient data',candles:priceResult.rows.length,log});
    }

    const byEpic = {};
    priceResult.rows.forEach(r => {
      if(!byEpic[r.epic]) byEpic[r.epic]=[];
      byEpic[r.epic].push(parseFloat(r.close_price));
    });
    L(`${Object.keys(byEpic).length} instruments, ${priceResult.rows.length} candles`);

    const results = [];
    for (const threshold of [1,2,3,4]) {
      let trades=0,wins=0,pnl=0;
      for (const [,closes] of Object.entries(byEpic)) {
        if(closes.length<15) continue;
        const r = backtest(closes, threshold);
        trades+=r.trades; wins+=r.wins; pnl+=r.pnl;
      }
      const wr = trades>0?(wins/trades)*100:0;
      const exp = trades>0?pnl/trades:0;
      const score = exp*(wr/100);
      results.push({threshold,trades,winRate:wr.toFixed(1),pnl:pnl.toFixed(4),expectancy:exp.toFixed(4),score:score.toFixed(4)});
      L(`Threshold ${threshold}: ${trades} trades | ${wr.toFixed(1)}% WR | score ${score.toFixed(4)}`);
    }

    const best = results.reduce((a,b) => parseFloat(a.score)>parseFloat(b.score)?a:b);
    L(`Best: threshold=${best.threshold} score=${best.score}`);

    // Calibration-based confidence minimum
    let bestConf = 60;
    const calRes = await fetch(`${base}/api/db?action=calibration`);
    const calData = await calRes.json();
    if (calData.calibration?.length > 0) {
      const profitable = calData.calibration.filter(r => parseFloat(r.actual_win_rate)>50 && parseInt(r.total_trades)>=3);
      if (profitable.length > 0) {
        const map = {'90-100%':90,'80-89%':80,'70-79%':70,'60-69%':60};
        profitable.sort((a,b)=>(map[a.confidence_bracket]||60)-(map[b.confidence_bracket]||60));
        bestConf = map[profitable[0].confidence_bracket] || 60;
        L(`Calibration suggests confidence min: ${bestConf}%`);
      }
    }

    await fetch(`${base}/api/db`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({type:'save_params',data:{signalThreshold:best.threshold,aiConfidenceMin:bestConf,
        backtestScore:parseFloat(best.score),backtestResults:results,optimizedAt:new Date().toISOString()}})});

    await fetch(`${base}/api/notify`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({type:'system',subject:'⚙️ Walk-Forward Optimisation Complete',
        body:`Signal threshold: ${best.threshold}\nAI confidence min: ${bestConf}%\nScore: ${best.score}\n\nApply: SIGNAL_THRESHOLD=${best.threshold} AI_CONFIDENCE_MIN=${bestConf} in Vercel`})});

    return res.status(200).json({success:true,bestThreshold:best.threshold,bestConfidence:bestConf,results,log});
  } catch(e) { L('Error: '+e.message); return res.status(500).json({error:e.message,log}); }
}

function backtest(closes, threshold) {
  let trades=0,wins=0,pnl=0,openTrade=null;
  for (let i=20;i<closes.length;i++) {
    const sc = quickScore(closes.slice(Math.max(0,i-60),i));
    if(!openTrade && Math.abs(sc)>=threshold) {
      openTrade={direction:sc>0?'BUY':'SELL',entry:closes[i],index:i};
    } else if(openTrade) {
      const holding=i-openTrade.index;
      const reversal=(openTrade.direction==='BUY'&&sc<=-threshold)||(openTrade.direction==='SELL'&&sc>=threshold);
      if(reversal||holding>=5) {
        const tp=openTrade.direction==='BUY'?closes[i]-openTrade.entry:openTrade.entry-closes[i];
        trades++; if(tp>0)wins++; pnl+=tp; openTrade=null;
      }
    }
  }
  return {trades,wins,pnl};
}

function quickScore(closes) {
  const n=closes.length; if(n<5) return 0;
  let s=0,g=0,l=0;
  const p=Math.min(14,n-1);
  for(let i=n-p;i<n;i++){const d=closes[i]-closes[i-1];if(d>0)g+=d;else l+=Math.abs(d);}
  const rsi=l===0?100:100-(100/(1+(g/p)/(l/p)));
  if(rsi<30)s+=3;else if(rsi<40)s+=2;else if(rsi>70)s-=3;else if(rsi>60)s-=2;
  const sma5=closes.slice(-5).reduce((a,b)=>a+b,0)/5;
  const sma10=closes.slice(-Math.min(10,n)).reduce((a,b)=>a+b,0)/Math.min(10,n);
  if(sma5>sma10)s+=1;else s-=1;
  const mom=n>=5?((closes[n-1]-closes[n-5])/closes[n-5])*100:0;
  if(mom>1)s+=1;else if(mom<-1)s-=1;
  return s;
}

// ─── WEEKLY PRICE ANALYSIS ────────────────────────────────────────────────────
async function runWeeklyPriceAnalysis(base, anthropicKey) {
  try {
    // Fetch last 30 candles for all instruments from DB
    const instruments = [
      { name: 'FTSE 100',    epic: 'IX.D.FTSE.DAILY.IP' },
      { name: 'S&P 500',     epic: 'IX.D.SPTRD.DAILY.IP' },
      { name: 'DAX 40',      epic: 'IX.D.DAX.DAILY.IP' },
      { name: 'Brent Oil',   epic: 'CC.D.LCO.USS.IP' },
      { name: 'Gold',        epic: 'CS.D.USCGC.TODAY.IP' },
      { name: 'Silver',      epic: 'CS.D.USCSC.TODAY.IP' },
      { name: 'GBP/USD',     epic: 'CS.D.GBPUSD.MINI.IP' },
      { name: 'EUR/USD',     epic: 'CS.D.EURUSD.MINI.IP' },
      { name: 'Copper',      epic: 'CS.D.COPPER.TODAY.IP' },
      { name: 'EUR/GBP',     epic: 'CS.D.EURGBP.MINI.IP' },
    ];

    const marketData = [];

    for (const instr of instruments) {
      try {
        const r = await fetch(`${base}/api/prices?epic=${encodeURIComponent(instr.epic)}&resolution=DAY&limit=30`);
        const d = await r.json();
        const candles = d.candles || [];
        if (candles.length < 5) continue;

        const closes = candles.map(c => parseFloat(c.close_price));
        const n = closes.length;
        const last = closes[n-1];
        const weekAgo = closes[Math.max(0, n-6)];
        const monthAgo = closes[0];

        // RSI
        let gains=0, losses=0;
        const period = Math.min(14, n-1);
        for (let i=n-period; i<n; i++) {
          const d = closes[i]-closes[i-1];
          if(d>0) gains+=d; else losses+=Math.abs(d);
        }
        const ag=gains/period, al=losses/period;
        const rsi = al===0 ? 100 : 100-(100/(1+ag/al));

        // Regime
        const mid = Math.floor(n/2);
        const h1 = closes.slice(0,mid).reduce((a,b)=>a+b,0)/mid;
        const h2 = closes.slice(mid).reduce((a,b)=>a+b,0)/(n-mid);
        const slope = ((h2-h1)/h1)*100;
        const regime = Math.abs(slope)>3 ? (slope>0?'uptrend':'downtrend') : 'ranging';

        // Week and month change
        const weekChg = ((last-weekAgo)/weekAgo*100).toFixed(1);
        const monthChg = ((last-monthAgo)/monthAgo*100).toFixed(1);

        // Consecutive days up/down
        let streak = 0;
        for (let i=n-1; i>0; i--) {
          if (closes[i]>closes[i-1] && streak>=0) streak++;
          else if (closes[i]<closes[i-1] && streak<=0) streak--;
          else break;
        }

        marketData.push({
          name: instr.name,
          rsi: rsi.toFixed(1),
          regime,
          weekChg: (weekChg>=0?'+':'')+weekChg+'%',
          monthChg: (monthChg>=0?'+':'')+monthChg+'%',
          streak: streak>0?`${streak} days up`:streak<0?`${Math.abs(streak)} days down`:'flat',
          lastPrice: last.toFixed(2),
          candles: n,
        });
      } catch(e) { /* skip instrument */ }
    }

    if (!marketData.length || !anthropicKey) return null;

    // Build prompt for Claude
    const dataStr = marketData.map(m =>
      `${m.name}: RSI ${m.rsi} | ${m.regime} | Week ${m.weekChg} | Month ${m.monthChg} | ${m.streak} | ${m.candles} candles`
    ).join('\n');

    // Find notable conditions
    const oversold = marketData.filter(m => parseFloat(m.rsi) <= 32);
    const overbought = marketData.filter(m => parseFloat(m.rsi) >= 68);
    const trending = marketData.filter(m => m.regime !== 'ranging');

    const prompt = `You are a quantitative market analyst. Review this week's price data across multiple instruments and identify the most interesting patterns, divergences, and potential opportunities for next week.

MARKET DATA (as of week ending):
${dataStr}

NOTABLE CONDITIONS:
- Oversold (RSI ≤32): ${oversold.map(m=>m.name+' RSI:'+m.rsi).join(', ') || 'None'}
- Overbought (RSI ≥68): ${overbought.map(m=>m.name+' RSI:'+m.rsi).join(', ') || 'None'}
- Trending: ${trending.map(m=>m.name+' ('+m.regime+')').join(', ') || 'All ranging'}

Analyse:
1. Which instruments show the most interesting setups for next week and why?
2. Any unusual divergences (e.g. oil up but gold down, indices diverging)?
3. Which RSI extremes look like genuine mean reversion opportunities vs momentum continuation?
4. Any correlations breaking down that could signal a larger move?
5. One specific instrument and direction to watch closely this week with the key level.

Keep it concise — this is a weekly briefing, not a full report. Focus on actionable observations.`;

    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type':'application/json', 'x-api-key':anthropicKey, 'anthropic-version':'2023-06-01' },
      body: JSON.stringify({ model:'claude-sonnet-4-5', max_tokens:800, messages:[{role:'user',content:prompt}] })
    });
    const aiData = await aiRes.json();
    const analysis = aiData.content?.[0]?.text || '';

    // Check for significant sentiment shifts vs last week
    const sentimentShifts = [];
    try {
      const instruments = marketData.map(m => m.name);
      for (const instr of instruments.slice(0,6)) {
        const sr = await fetch(`${base}/api/db?action=sentiment_history&instrument=${encodeURIComponent(instr)}&days=14`);
        const sd = await sr.json();
        if (sd.shift) {
          sentimentShifts.push(`${instr}: sentiment ${sd.shift.direction.replace('_',' ')} by ${sd.shift.change}% over 14 days`);
        }
      }
    } catch(e) {}

    return { marketData, analysis, oversold, overbought, trending, sentimentShifts };
  } catch(e) {
    console.error('[Weekly Price Analysis]', e.message);
    return null;
  }
}

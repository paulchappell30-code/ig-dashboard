// ─── STEP 1: BULK CANDLE DOWNLOAD ────────────────────────────────────────────
// Run this first in the browser console on the dashboard page.
// Downloads 500 days of daily candles for all new instruments via /api/prices.
// Wait for "All downloads complete" before running Step 2.

const NEW_INSTRUMENTS = [
  // Indices
  { instr: 'Russell 2000',     symbol: '^RUT' },
  { instr: 'Japan 225',        symbol: '^N225' },
  { instr: 'Hong Kong HS50',   symbol: '^HSI' },
  { instr: 'China A50',        symbol: 'XIN9.SI' },
  { instr: 'EU Stocks 50',     symbol: '^STOXX50E' },
  { instr: 'Australia 200',    symbol: '^AXJO' },
  { instr: 'Spain 35',         symbol: '^IBEX' },
  { instr: 'Switzerland SMI',  symbol: '^SSMI' },
  // FX
  { instr: 'AUD/USD',          symbol: 'AUDUSD=X' },
  { instr: 'USD/CAD',          symbol: 'USDCAD=X' },
  { instr: 'USD/CHF',          symbol: 'USDCHF=X' },
  { instr: 'EUR/CHF',          symbol: 'EURCHF=X' },
  { instr: 'EUR/JPY',          symbol: 'EURJPY=X' },
  { instr: 'GBP/JPY',          symbol: 'GBPJPY=X' },
  { instr: 'AUD/JPY',          symbol: 'AUDJPY=X' },
  // Bonds
  { instr: 'US 10yr T-Note',   symbol: 'ZN=F' },
  { instr: 'US 2yr T-Note',    symbol: 'ZT=F' },
  { instr: 'US 5yr T-Note',    symbol: 'ZF=F' },
  { instr: 'US Ultra T-Bond',  symbol: 'UB=F' },
  { instr: 'German Bund',      symbol: 'FGBL=F' },
  { instr: 'UK Long Gilt',     symbol: 'FGBM=F' },  // proxy — Gilt futures less liquid on Yahoo
  { instr: 'Italian BTP',      symbol: 'FBTP=F' },
  { instr: 'French OAT',       symbol: 'FOAT=F' },
  // Commodities
  { instr: 'WTI Oil',          symbol: 'CL=F' },
  { instr: 'Natural Gas',      symbol: 'NG=F' },
  { instr: 'Platinum',         symbol: 'PL=F' },
  { instr: 'Palladium',        symbol: 'PA=F' },
  { instr: 'Aluminium',        symbol: 'ALI=F' },
  { instr: 'Zinc',             symbol: 'ZNC=F' },
];

async function downloadAllCandles() {
  console.log(`Downloading candles for ${NEW_INSTRUMENTS.length} instruments...`);
  for(const inst of NEW_INSTRUMENTS) {
    try {
      const r = await fetch('/api/prices', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer Bambip49' },
        body: JSON.stringify({ action: 'backfill', instrument: inst.instr, symbol: inst.symbol, days: 500 })
      });
      const d = await r.json();
      console.log(`✅ ${inst.instr}: ${d.inserted || d.rows || '?'} candles`);
    } catch(e) {
      console.log(`❌ ${inst.instr}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 800)); // avoid hammering the API
  }
  console.log('All downloads complete — run Step 2 grid search');
}

// ─── STEP 2: GRID SEARCH ACROSS ALL PAIRS ────────────────────────────────────
// Run AFTER downloads complete. Tests all pair combinations with the same
// entry/exit/stop/lookback parameters used in the original grid search.

const PAIR_UNIVERSE = [
  // ── US Indices ──────────────────────────────────────────────────────────────
  ['russell_sp500',    'Russell 2000',   'S&P 500'],
  ['russell_dow',      'Russell 2000',   'Dow Jones'],
  ['nasdaq_sp500',     'Nasdaq',         'S&P 500'],       // already tested, re-run for consistency
  // ── European Indices ────────────────────────────────────────────────────────
  ['ftse_stoxx50',     'FTSE 100',       'EU Stocks 50'],
  ['dax_stoxx50',      'DAX 40',         'EU Stocks 50'],
  ['cac_stoxx50',      'CAC 40',         'EU Stocks 50'],
  ['ibex_cac',         'Spain 35',       'CAC 40'],
  ['smi_dax',          'Switzerland SMI','DAX 40'],
  ['smi_stoxx50',      'Switzerland SMI','EU Stocks 50'],
  // ── Asia-Pacific Indices ────────────────────────────────────────────────────
  ['nikkei_sp500',     'Japan 225',      'S&P 500'],
  ['nikkei_dax',       'Japan 225',      'DAX 40'],
  ['hsi_china',        'Hong Kong HS50', 'China A50'],
  ['asx_nikkei',       'Australia 200',  'Japan 225'],
  ['asx_sp500',        'Australia 200',  'S&P 500'],
  // ── FX Triangles ────────────────────────────────────────────────────────────
  ['audusd_usdjpy',    'AUD/USD',        'USD/JPY'],
  ['audusd_eurusd',    'AUD/USD',        'EUR/USD'],
  ['usdcad_usdchf',    'USD/CAD',        'USD/CHF'],
  ['eurchf_eurusd',    'EUR/CHF',        'EUR/USD'],
  ['eurchf_usdchf',    'EUR/CHF',        'USD/CHF'],
  ['gbpjpy_usdjpy',    'GBP/JPY',        'USD/JPY'],
  ['eurjpy_usdjpy',    'EUR/JPY',        'USD/JPY'],
  ['audjpy_usdjpy',    'AUD/JPY',        'USD/JPY'],
  ['gbpusd_audusd',    'GBP/USD',        'AUD/USD'],
  // ── Commodities ─────────────────────────────────────────────────────────────
  ['wti_brent',        'WTI Oil',        'Brent Oil'],
  ['platinum_gold',    'Platinum',       'Gold'],
  ['palladium_gold',   'Palladium',      'Gold'],
  ['platinum_palladium','Platinum',      'Palladium'],
  ['natgas_brent',     'Natural Gas',    'Brent Oil'],
  ['aluminium_copper', 'Aluminium',      'Copper'],
  ['zinc_copper',      'Zinc',           'Copper'],
  // ── Cross-Asset ─────────────────────────────────────────────────────────────
  ['audusd_copper',    'AUD/USD',        'Copper'],
  ['usdcad_wti',       'USD/CAD',        'WTI Oil'],
  ['gold_sp500',       'Gold',           'S&P 500'],
  ['gold_bund',        'Gold',           'German Bund'],
  ['gold_tnote10',     'Gold',           'US 10yr T-Note'],
  // ── Bonds — Yield Curve ──────────────────────────────────────────────────────
  ['tnote10_tnote2',   'US 10yr T-Note', 'US 2yr T-Note'],   // US yield curve steepener
  ['tnote10_tnote5',   'US 10yr T-Note', 'US 5yr T-Note'],   // intermediate curve
  ['tnote5_tnote2',    'US 5yr T-Note',  'US 2yr T-Note'],   // short end curve
  ['bund_gilt',        'German Bund',    'UK Long Gilt'],     // EU vs UK rates
  ['bund_tnote10',     'German Bund',    'US 10yr T-Note'],   // transatlantic rates
  ['bund_btp',         'German Bund',    'Italian BTP'],      // core vs periphery spread
  ['bund_oat',         'German Bund',    'French OAT'],       // Germany vs France
  ['gilt_tnote10',     'UK Long Gilt',   'US 10yr T-Note'],   // UK vs US rates
  ['btp_oat',          'Italian BTP',    'French OAT'],       // Italy vs France
  ['ultrabond_tnote10','US Ultra T-Bond','US 10yr T-Note'],   // long end vs 10yr
  // ── Bonds vs Equities ────────────────────────────────────────────────────────
  ['tnote10_sp500',    'US 10yr T-Note', 'S&P 500'],          // classic flight to safety
  ['bund_dax',         'German Bund',    'DAX 40'],           // EU bonds vs equities
  ['gilt_ftse',        'UK Long Gilt',   'FTSE 100'],         // UK bonds vs equities
];

const ENTRY_ZS  = [1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5];
const EXIT_ZS   = [0.0, 0.25, 0.5, 0.75, 1.0];
const STOP_Z    = 3.0;
const LOOKBACKS = [45, 60, 90]; // test 3 key lookbacks simultaneously
const DAYS      = 500;
const MIN_TRADES = 5;

async function runUniverseGridSearch() {
  console.log(`\nStarting grid search: ${PAIR_UNIVERSE.length} pairs × ${ENTRY_ZS.length} entryZ × ${EXIT_ZS.length} exitZ × ${LOOKBACKS.length} lookbacks`);
  const totalCombos = PAIR_UNIVERSE.length * ENTRY_ZS.length * EXIT_ZS.length * LOOKBACKS.length;
  console.log(`Total combinations: ${totalCombos} — estimated time: ${Math.round(totalCombos * 0.5 / 60)} mins\n`);

  const results = {};
  let tested = 0;

  for(const [pairId, instrA, instrB] of PAIR_UNIVERSE) {
    let topScore = 0, topResult = null;

    for(const lb of LOOKBACKS) {
      for(const ez of ENTRY_ZS) {
        for(const xz of EXIT_ZS) {
          if(xz >= ez) continue;
          try {
            const r = await fetch(
              `/api/review?action=pairs_backtest&pair=${pairId}&instrA=${encodeURIComponent(instrA)}&instrB=${encodeURIComponent(instrB)}&entryZ=${ez}&exitZ=${xz}&stopZ=${STOP_Z}&lookback=${lb}&days=${DAYS}`
            );
            const d = await r.json();
            const s = d.summary;
            if(!s || s.totalTrades < MIN_TRADES) continue;

            const score = s.expectancy * Math.sqrt(s.totalTrades) * (s.winRate/100) * s.profitFactor;
            if(score > topScore) {
              topScore = score;
              topResult = { pairId, instrA, instrB, ez, xz, lb,
                trades: s.totalTrades, wr: s.winRate, exp: s.expectancy,
                pf: s.profitFactor, score: parseFloat(score.toFixed(2)) };
            }
            tested++;
          } catch(e) { /* skip failed requests */ }
        }
      }
    }

    if(topResult) {
      results[pairId] = topResult;
      console.log(`${instrA}/${instrB}: score ${topResult.score.toFixed(2)} — Entry ${topResult.ez}σ Exit ${topResult.xz}σ LB ${topResult.lb}d | ${topResult.trades} trades | ${topResult.wr}% WR | exp ${topResult.exp}%`);
    } else {
      console.log(`${instrA}/${instrB}: no qualifying result (< ${MIN_TRADES} trades across all params)`);
    }
  }

  // ── Final ranked summary ────────────────────────────────────────────────────
  console.log('\n════════════════════════════════════════');
  console.log('RANKED RESULTS (score ≥ 5.0):');
  console.log('════════════════════════════════════════');
  Object.values(results)
    .filter(r => r.score >= 5.0)
    .sort((a, b) => b.score - a.score)
    .forEach((r, i) => {
      const tier = r.score >= 20 ? '⭐ Deploy' : r.score >= 10 ? '👁 Watch' : '📋 Test';
      console.log(`${i+1}. [${tier}] ${r.instrA}/${r.instrB} — score ${r.score} | Entry ${r.ez}σ Exit ${r.xz}σ LB ${r.lb}d | ${r.trades}T ${r.wr}%WR ${r.exp}%exp`);
    });

  console.log('\nFull results object (copy for analysis):');
  console.log(JSON.stringify(results, null, 2));

  return results;
}

// ─── HOW TO USE ───────────────────────────────────────────────────────────────
// 1. Paste this entire script into the browser console on the dashboard page
// 2. Run: await downloadAllCandles()
// 3. Wait for "All downloads complete"
// 4. Run: await runUniverseGridSearch()
// 5. Paste results back for analysis

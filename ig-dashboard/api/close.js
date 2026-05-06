// Position close endpoint v2
// Closes spreadbet positions by placing an opposing trade (not DELETE)
const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,X-IG-API-KEY,CST,X-SECURITY-TOKEN');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { dealId } = req.body || {};
  if (!dealId) return res.status(400).json({ error: 'dealId required' });

  const env = process.env.IG_ENV || 'demo';
  const base = IG_BASES[env];
  const apiKey = process.env.IG_API_KEY || '';
  const cst = req.headers['cst'] || req.headers['CST'] || '';
  const xst = req.headers['x-security-token'] || req.headers['X-SECURITY-TOKEN'] || '';

  const igH = {
    'Content-Type': 'application/json',
    'X-IG-API-KEY': apiKey,
    'CST': cst,
    'X-SECURITY-TOKEN': xst,
  };

  console.log('[Close] dealId:', dealId, 'env:', env);

  try {
    // Step 1: Get position details
    const posRes = await fetch(`${base}/positions`, { headers: { ...igH, 'Version': '1' } });
    const posData = await posRes.json();
    const positions = posData.positions || [];
    const position = positions.find(p => p.position.dealId === dealId);

    if (!position) {
      console.log('[Close] Position not found in open positions');
      return res.status(200).json({ status: 404, ok: false, error: 'Position not found' });
    }

    const epic = position.market.epic;
    const size = position.position.size || position.position.dealSize;
    const direction = position.position.direction;
    const closeDirection = direction === 'BUY' ? 'SELL' : 'BUY';

    console.log('[Close] Found position:', epic, direction, size, '→ closing with', closeDirection);

    // Step 2: Place opposing trade to close
    // Method A: Use close endpoint with forceOpen: false
    const closeBody = {
      epic,
      direction: closeDirection,
      size,
      orderType: 'MARKET',
      expiry: 'DFB',
      guaranteedStop: false,
      forceOpen: false,  // false = close existing position
      currencyCode: 'GBP',
      dealType: 'SPREADBET',
    };

    const closeRes = await fetch(`${base}/positions/otc`, {
      method: 'POST',
      headers: { ...igH, 'Version': '1' },
      body: JSON.stringify(closeBody)
    });
    const closeData = await closeRes.json();
    console.log('[Close] Close order response:', JSON.stringify(closeData));

    if (!closeData.dealReference) {
      // Method B: Try DELETE with correct headers
      console.log('[Close] POST failed, trying DELETE...');
      const delRes = await fetch(`${base}/positions/otc/${dealId}`, {
        method: 'DELETE',
        headers: { ...igH, 'Version': '1' }
      });
      const delText = await delRes.text();
      console.log('[Close] DELETE response:', delRes.status, delText);
      if (!delRes.ok) {
        return res.status(200).json({ status: delRes.status, ok: false, error: delText });
      }
    }

    // Step 3: Check confirmation
    const ref = closeData.dealReference;
    if (ref) {
      await new Promise(r => setTimeout(r, 1000));
      const confirmRes = await fetch(`${base}/confirms/${ref}`, { headers: { ...igH, 'Version': '1' } });
      const confirm = await confirmRes.json();
      console.log('[Close] Confirmation:', JSON.stringify(confirm));

      if (confirm.dealStatus === 'ACCEPTED') {
        return res.status(200).json({ status: 200, ok: true, confirmation: confirm });
      } else {
        return res.status(200).json({ status: 200, ok: false, error: confirm.reason, confirmation: confirm });
      }
    }

    return res.status(200).json({ status: 200, ok: true, body: closeData });

  } catch(e) {
    console.error('[Close] Error:', e.message);
    return res.status(500).json({ error: e.message });
  }
};

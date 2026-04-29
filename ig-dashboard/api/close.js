// Dedicated position close endpoint — avoids Vercel routing issues with DELETE
const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,X-IG-API-KEY,CST,X-SECURITY-TOKEN,Version');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { dealId } = req.body || {};
  if (!dealId) return res.status(400).json({ error: 'dealId required' });

  const env = process.env.IG_ENV || 'demo';
  const base = IG_BASES[env] || IG_BASES.demo;
  const url = `${base}/positions/otc/${dealId}`;

  const apiKey = process.env.IG_API_KEY || req.headers['x-ig-api-key'] || '';
  const cst = req.headers['cst'] || req.headers['CST'] || '';
  const xst = req.headers['x-security-token'] || req.headers['X-SECURITY-TOKEN'] || '';
  
  console.log('[Close] CST present:', !!cst, 'XST present:', !!xst, 'API key present:', !!apiKey);

  console.log('[Close] Closing position:', dealId, 'at', url);

  try {
    const igRes = await fetch(url, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
        'X-IG-API-KEY': apiKey,
        'CST': cst,
        'X-SECURITY-TOKEN': xst,
        'Version': '1'
      }
    });

    const text = await igRes.text();
    console.log('[Close] IG response:', igRes.status, text);

    // Check confirmation if we got a deal reference
    let confirmData = null;
    if (text) {
      try {
        const data = JSON.parse(text);
        if (data.dealReference) {
          const confirmRes = await fetch(`${base}/confirms/${data.dealReference}`, {
            headers: {
              'X-IG-API-KEY': apiKey,
              'CST': cst,
              'X-SECURITY-TOKEN': xst,
              'Version': '1'
            }
          });
          confirmData = await confirmRes.json();
          console.log('[Close] Confirmation:', JSON.stringify(confirmData));
        }
      } catch(e) {}
    }

    res.status(igRes.status).json({
      status: igRes.status,
      ok: igRes.ok,
      body: text || null,
      confirmation: confirmData
    });

  } catch (err) {
    console.error('[Close] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
};

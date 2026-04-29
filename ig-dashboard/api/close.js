// Dedicated position close endpoint
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
  const apiKey = process.env.IG_API_KEY || '';

  // Get tokens from request headers — try all case variants
  const allHeaders = req.headers;
  const cst = allHeaders['cst'] || allHeaders['CST'] || '';
  const xst = allHeaders['x-security-token'] || allHeaders['X-SECURITY-TOKEN'] || allHeaders['x-security-Token'] || '';

  console.log('[Close] dealId:', dealId);
  console.log('[Close] env:', env);
  console.log('[Close] apiKey present:', !!apiKey);
  console.log('[Close] CST:', cst ? cst.substring(0, 10) + '...' : 'MISSING');
  console.log('[Close] XST:', xst ? xst.substring(0, 10) + '...' : 'MISSING');
  console.log('[Close] All header keys:', Object.keys(allHeaders).join(', '));

  const url = `${base}/positions/otc/${dealId}`;
  console.log('[Close] URL:', url);

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
    console.log('[Close] IG status:', igRes.status);
    console.log('[Close] IG body:', text);

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
          console.log('[Close] Confirm:', JSON.stringify(confirmData));
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

const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,X-IG-API-KEY,CST,X-SECURITY-TOKEN,Version');
  res.setHeader('Access-Control-Expose-Headers', 'CST,X-SECURITY-TOKEN');

  if (req.method === 'OPTIONS') return res.status(200).end();

  const path = (req.query.path || '').replace(/^\/+/, '');
  if (!path) return res.status(400).json({ error: 'No path specified' });

  const env = process.env.IG_ENV || 'demo';
  const base = IG_BASES[env] || IG_BASES.demo;
  const igUrl = `${base}/${path}`;

  const apiKey = process.env.IG_API_KEY || req.headers['x-ig-api-key'] || '';

  console.log(`[IG Proxy] ${req.method} ${igUrl}`);
  console.log(`[IG Proxy] API key present: ${!!apiKey}`);
  console.log(`[IG Proxy] API key value: ${apiKey.substring(0, 6)}...`);

  const igHeaders = {
    'Content-Type': 'application/json',
    'X-IG-API-KEY': apiKey,
    'Version': req.headers['version'] || '1',
  };

  if (req.headers['cst']) igHeaders['CST'] = req.headers['cst'];
  if (req.headers['x-security-token']) igHeaders['X-SECURITY-TOKEN'] = req.headers['x-security-token'];

  try {
    // Vercel auto-parses the body — use req.body directly
    let bodyString;
    if (['POST', 'PUT', 'DELETE'].includes(req.method) && req.body) {
      bodyString = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
    }

    console.log(`[IG Proxy] Body: ${bodyString}`);

    const igRes = await fetch(igUrl, {
      method: req.method,
      headers: igHeaders,
      body: bodyString || undefined,
    });

    const responseText = await igRes.text();

    console.log(`[IG Proxy] IG status: ${igRes.status}`);
    console.log(`[IG Proxy] IG body: ${responseText}`);

    const cst = igRes.headers.get('cst') || igRes.headers.get('CST');
    const xst = igRes.headers.get('x-security-token') || igRes.headers.get('X-SECURITY-TOKEN');

    console.log(`[IG Proxy] CST present: ${!!cst}`);
    console.log(`[IG Proxy] XST present: ${!!xst}`);

    if (cst) res.setHeader('CST', cst);
    if (xst) res.setHeader('X-SECURITY-TOKEN', xst);

    res.status(igRes.status).send(responseText);

  } catch (err) {
    console.error('[IG Proxy] Error:', err.message);
    console.error('[IG Proxy] Stack:', err.stack);
    res.status(500).json({ error: err.message });
  }
};

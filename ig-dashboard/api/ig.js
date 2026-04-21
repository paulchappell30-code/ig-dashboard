const fetch = require('node-fetch');

const IG_BASES = {
  live: 'https://api.ig.com/gateway/deal',
  demo: 'https://demo-api.ig.com/gateway/deal',
};

module.exports = async (req, res) => {
  // CORS headers — allow browser requests from same origin
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,X-IG-API-KEY,CST,X-SECURITY-TOKEN,Version,_method');
  res.setHeader('Access-Control-Expose-Headers', 'CST,X-SECURITY-TOKEN');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // Path comes from query param (set by vercel.json rewrite)
  const path = (req.query.path || '').replace(/^\/+/, '');
  if (!path) return res.status(400).json({ error: 'No path specified' });

  // Determine environment — prefer env var, fall back to header hint
  const env = process.env.IG_ENV || (req.headers['x-ig-env'] === 'live' ? 'live' : 'demo');
  const base = IG_BASES[env] || IG_BASES.demo;
  const igUrl = `${base}/${path}`;

  // Build headers for IG — inject server-side API key if configured
  const igHeaders = {
    'Content-Type': 'application/json',
    'X-IG-API-KEY': process.env.IG_API_KEY || req.headers['x-ig-api-key'] || '',
    'CST': req.headers['cst'] || '',
    'X-SECURITY-TOKEN': req.headers['x-security-token'] || '',
    'Version': req.headers['version'] || '1',
  };

  // Strip undefined / empty values
  Object.keys(igHeaders).forEach(k => { if (!igHeaders[k]) delete igHeaders[k]; });

  try {
    const igRes = await fetch(igUrl, {
      method: req.method,
      headers: igHeaders,
      body: ['POST', 'PUT', 'DELETE'].includes(req.method) && req.body
        ? JSON.stringify(req.body)
        : undefined,
    });

    // Forward IG auth tokens back to browser
    const cst = igRes.headers.get('CST');
    const xst = igRes.headers.get('X-SECURITY-TOKEN');
    if (cst) res.setHeader('CST', cst);
    if (xst) res.setHeader('X-SECURITY-TOKEN', xst);

    let body;
    const ct = igRes.headers.get('content-type') || '';
    if (ct.includes('application/json')) {
      body = await igRes.json();
      res.status(igRes.status).json(body);
    } else {
      body = await igRes.text();
      res.status(igRes.status).send(body);
    }
  } catch (err) {
    console.error('IG proxy error:', err.message);
    res.status(500).json({ error: 'Proxy error', detail: err.message });
  }
};

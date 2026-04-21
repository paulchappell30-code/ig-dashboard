// Local development server — serves the dashboard and proxies IG API calls
// Run: node server-local.js  (or: nodemon server-local.js)
// Then open: http://localhost:3000

const http = require('http');
const fs   = require('fs');
const path = require('path');
const fetch = require('node-fetch');

const PORT    = 3000;
const IG_BASE = process.env.IG_ENV === 'live'
  ? 'https://api.ig.com/gateway/deal'
  : 'https://demo-api.ig.com/gateway/deal';

const MIME = {
  '.html': 'text/html',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.ico':  'image/x-icon',
};

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  // ── IG PROXY ─────────────────────────────────────────────────────────────
  if (url.pathname.startsWith('/ig/')) {
    const igPath = url.pathname.replace('/ig/', '');
    const igUrl  = `${IG_BASE}/${igPath}`;

    // CORS preflight
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type,X-IG-API-KEY,CST,X-SECURITY-TOKEN,Version');
    res.setHeader('Access-Control-Expose-Headers', 'CST,X-SECURITY-TOKEN');
    if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

    // Read request body
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      try {
        const igRes = await fetch(igUrl, {
          method: req.method,
          headers: {
            'Content-Type': 'application/json',
            'X-IG-API-KEY': process.env.IG_API_KEY || req.headers['x-ig-api-key'] || '',
            'CST': req.headers['cst'] || '',
            'X-SECURITY-TOKEN': req.headers['x-security-token'] || '',
            'Version': req.headers['version'] || '1',
          },
          body: ['POST', 'PUT', 'DELETE'].includes(req.method) && body ? body : undefined,
        });

        // Forward auth tokens
        const cst = igRes.headers.get('CST');
        const xst = igRes.headers.get('X-SECURITY-TOKEN');
        if (cst) res.setHeader('CST', cst);
        if (xst) res.setHeader('X-SECURITY-TOKEN', xst);

        const data = await igRes.text();
        res.writeHead(igRes.status, { 'Content-Type': 'application/json' });
        res.end(data);
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // ── STATIC FILES ──────────────────────────────────────────────────────────
  let filePath = url.pathname === '/' ? '/public/index.html' : '/public' + url.pathname;
  filePath = path.join(__dirname, filePath);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    const ext  = path.extname(filePath);
    const mime = MIME[ext] || 'text/plain';
    res.writeHead(200, { 'Content-Type': mime });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`\n  IG Dashboard running at http://localhost:${PORT}`);
  console.log(`  Environment: ${process.env.IG_ENV || 'demo'}`);
  console.log(`  Press Ctrl+C to stop\n`);
});

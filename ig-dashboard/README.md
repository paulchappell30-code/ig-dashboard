# IG Automated Investment Programme

A full-featured automated investment dashboard for IG Index with DCA scheduling, signal-based rule engine, and portfolio rebalancer.

---

## Features

- **Live prices** — watchlist with real-time IG market data
- **DCA Programme** — recurring buy schedules (daily / weekly / monthly)
- **Rule Engine** — signal-based triggers (RSI, SMA Cross, EMA, MACD, Bollinger Bands)
- **Portfolio Rebalancer** — target allocation with drift detection
- **Order Management** — manual orders + full history log
- **Activity Log** — timestamped audit trail of all orders and rule events
- **Demo / Live modes** — paper trading by default, switch to live when ready

---

## Quick Start (Local)

### 1. Install dependencies

```bash
npm install
```

### 2. Run locally

```bash
node server-local.js
```

Then open `http://localhost:3000` in your browser.

---

## Deploy to Vercel

### 1. Install Vercel CLI

```bash
npm install -g vercel
```

### 2. Deploy

```bash
vercel
```

Follow the prompts. Your dashboard will be live at a `*.vercel.app` URL.

### 3. Add environment variables

In your Vercel project dashboard → **Settings** → **Environment Variables**, add:

| Variable | Value | Notes |
|---|---|---|
| `IG_API_KEY` | Your IG API key | From IG platform settings |
| `IG_ENV` | `demo` or `live` | Start with `demo` |

Once set, redeploy:

```bash
vercel --prod
```

---

## IG API Setup

1. Log in to **My IG** → Settings → API
2. Create an API key
3. Note your **username**, **password**, and **API key**
4. Enter these on the dashboard login screen

The proxy (`api/ig.js`) forwards all requests to IG's API server-side, so your credentials are never exposed in the browser.

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `IG_API_KEY` | IG API key (injected server-side) | Read from login |
| `IG_ENV` | `demo` \| `live` | `demo` |

---

## Project Structure

```
ig-dashboard/
├── public/
│   └── index.html      ← Dashboard (single-page app)
├── api/
│   └── ig.js           ← Vercel serverless proxy to IG API
├── package.json
├── vercel.json         ← Routing + security headers
└── README.md
```

---

## Switching to Live Trading

1. In Vercel environment variables, set `IG_ENV=live`
2. On the login screen, select **Live** from the environment dropdown
3. The red banner will confirm live mode is active

> **Warning:** Live mode places real orders. Always test thoroughly in demo mode first.

---

## IG API Epics (Instrument IDs)

| Instrument | Epic |
|---|---|
| FTSE 100 | `IX.D.FTSE.CFD.IP` |
| S&P 500 | `IX.D.SPTRD.CFD.IP` |
| DAX 40 | `IX.D.DAX.CFD.IP` |
| Gold | `CS.D.CFDGOLD.CFD.IP` |
| Crude Oil (Brent) | `CS.D.OILCFD.CFD.IP` |

To find epics for other instruments, use IG's market search API: `GET /markets?searchTerm=your+instrument`

---

## Local Development Server

For local testing without Vercel, use `server-local.js`:

```bash
node server-local.js
# Runs on http://localhost:3000
```

Install nodemon for auto-reload:

```bash
npm install -g nodemon
nodemon server-local.js
```

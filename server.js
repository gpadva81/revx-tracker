// RevX Tracker - Custom Postback & Click Tracking Server
// Hetzner VPS (aos-host-ash-1)

const express = require('express');
const { Pool } = require('pg');
const crypto = require('crypto');
const cors = require('cors');
const rateLimit = require('express-rate-limit');

const app = express();
app.use(express.json());
app.set('trust proxy', 1); // trust first proxy (caddy/nginx)

// --- CORS: only allow your landing page domains ---
const ALLOWED_ORIGINS = [
  'https://driverrates.com',
  'https://www.driverrates.com',
  'https://save-on-insurance.com',
  'https://www.save-on-insurance.com',
  'https://quoteshiftauto.com',
  'https://www.quoteshiftauto.com',
  // Allow localhost for testing
  'http://localhost:3000',
  'http://localhost:8080',
];

app.use(cors({
  origin: function (origin, callback) {
    // Allow requests with no origin (postbacks, curl, server-to-server)
    if (!origin) return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
    callback(null, false);
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  credentials: false,
}));

// --- Rate limiting ---
const clickLimiter = rateLimit({
  windowMs: 60 * 1000,   // 1 minute
  max: 30,                // 30 clicks per IP per minute
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests' },
});

const postbackLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,               // postbacks come in bursts from SmartFinancial
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests' },
});

// --- Database Setup ---
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://localhost:5432/revx_tracker',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS clicks (
        click_id VARCHAR(36) PRIMARY KEY,
        source VARCHAR(50),
        gclid VARCHAR(255),
        fbclid VARCHAR(255),
        campaign_id VARCHAR(100),
        adgroup_id VARCHAR(100),
        keyword VARCHAR(255),
        landing_page VARCHAR(500),
        offer_url VARCHAR(500),
        ip_address VARCHAR(45),
        user_agent TEXT,
        referer TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS conversions (
        id SERIAL PRIMARY KEY,
        click_id VARCHAR(36) REFERENCES clicks(click_id),
        event_type VARCHAR(50),
        payout DECIMAL(10,2) DEFAULT 0,
        uid VARCHAR(255),
        state VARCHAR(5),
        insured VARCHAR(10),
        own_home VARCHAR(10),
        multi_vehicle VARCHAR(10),
        raw_params JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_clicks_created ON clicks(created_at);
      CREATE INDEX IF NOT EXISTS idx_clicks_source ON clicks(source);
      CREATE INDEX IF NOT EXISTS idx_conversions_click_id ON conversions(click_id);
      CREATE INDEX IF NOT EXISTS idx_conversions_created ON conversions(created_at);
      CREATE INDEX IF NOT EXISTS idx_conversions_event_type ON conversions(event_type);
    `);
    console.log('[DB] Tables initialized');
  } finally {
    client.release();
  }
}

// --- Helpers ---
function generateClickId() {
  return crypto.randomUUID();
}

function sanitizeString(val, maxLen = 255) {
  if (!val || typeof val !== 'string') return null;
  return val.slice(0, maxLen).trim() || null;
}

function sanitizePayout(val) {
  const num = parseFloat(val);
  if (isNaN(num) || num < 0 || num > 99999) return 0;
  return Math.round(num * 100) / 100;
}

// --- Route: Register a click (called by tracker.js via POST) ---
app.post('/click', clickLimiter, async (req, res) => {
  try {
    const clickId = generateClickId();
    const {
      source, gclid, fbclid, campaign_id, adgroup_id,
      keyword, landing_page, offer_url
    } = req.body;

    await pool.query(
      `INSERT INTO clicks
        (click_id, source, gclid, fbclid, campaign_id, adgroup_id, keyword, landing_page, offer_url, ip_address, user_agent, referer)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [
        clickId,
        sanitizeString(source, 50) || 'unknown',
        sanitizeString(gclid),
        sanitizeString(fbclid),
        sanitizeString(campaign_id, 100),
        sanitizeString(adgroup_id, 100),
        sanitizeString(keyword),
        sanitizeString(landing_page, 500),
        sanitizeString(offer_url, 500),
        req.ip,
        sanitizeString(req.headers['user-agent'], 1000),
        sanitizeString(req.headers['referer'], 1000),
      ]
    );

    console.log(`[CLICK] ${clickId} src=${source || 'unknown'} ip=${req.ip}`);
    res.json({ click_id: clickId });
  } catch (err) {
    console.error('[CLICK] Error:', err.message);
    res.status(500).json({ error: 'Failed to register click' });
  }
});

// --- Route: Postback from SmartFinancial ---
// URL: https://track.revxglobal.com/postback?tid={tid}&payout={payout}&uid={uid}&state={state}&insured={insured}&own_home={own_home}&multi_vehicle={multi_vehicle}&event_type={event_type}
app.get('/postback', postbackLimiter, async (req, res) => {
  try {
    const { tid, payout, uid, state, insured, own_home, multi_vehicle, event_type } = req.query;

    if (!tid || typeof tid !== 'string' || tid.length > 36) {
      console.warn('[POSTBACK] Invalid/missing tid:', tid);
      return res.status(400).send('Missing or invalid tid');
    }

    const rawParams = JSON.stringify(req.query);
    const cleanPayout = sanitizePayout(payout);

    // Try to insert the conversion
    try {
      await pool.query(
        `INSERT INTO conversions
          (click_id, event_type, payout, uid, state, insured, own_home, multi_vehicle, raw_params)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [
          tid,
          sanitizeString(event_type, 50) || 'unknown',
          cleanPayout,
          sanitizeString(uid),
          sanitizeString(state, 5),
          sanitizeString(insured, 10),
          sanitizeString(own_home, 10),
          sanitizeString(multi_vehicle, 10),
          rawParams,
        ]
      );
    } catch (fkErr) {
      if (fkErr.code === '23503') {
        // FK violation: click_id not in clicks table
        // Create a placeholder click so we don't lose conversion data
        await pool.query(
          `INSERT INTO clicks (click_id, source) VALUES ($1, 'postback_only') ON CONFLICT DO NOTHING`,
          [tid]
        );
        await pool.query(
          `INSERT INTO conversions
            (click_id, event_type, payout, uid, state, insured, own_home, multi_vehicle, raw_params)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          [
            tid,
            sanitizeString(event_type, 50) || 'unknown',
            cleanPayout,
            sanitizeString(uid),
            sanitizeString(state, 5),
            sanitizeString(insured, 10),
            sanitizeString(own_home, 10),
            sanitizeString(multi_vehicle, 10),
            rawParams,
          ]
        );
        console.log(`[POSTBACK] Stored (orphan click): tid=${tid} payout=${cleanPayout} event=${event_type || 'unknown'}`);
        return res.status(200).send('OK');
      }
      throw fkErr;
    }

    console.log(`[POSTBACK] tid=${tid} payout=${cleanPayout} event=${event_type || 'unknown'} state=${state || '-'}`);
    res.status(200).send('OK');
  } catch (err) {
    console.error('[POSTBACK] Error:', err.message);
    res.status(500).send('Error');
  }
});

// --- Route: Revenue Report (parameterized queries) ---
app.get('/api/report', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 7, 1), 365);
    const interval = `${days} days`;

    const [dailyReport, sourceReport, stateReport, campaignReport, summary] = await Promise.all([
      // Daily breakdown
      pool.query(
        `SELECT
          DATE(c2.created_at) as date,
          COUNT(DISTINCT c2.click_id) as conversions,
          COUNT(c2.id) as total_events,
          COALESCE(SUM(c2.payout), 0) as revenue,
          COUNT(DISTINCT CASE WHEN c2.event_type = 'call' THEN c2.click_id END) as calls
        FROM conversions c2
        WHERE c2.created_at >= NOW() - $1::interval
        GROUP BY DATE(c2.created_at)
        ORDER BY date DESC`,
        [interval]
      ),

      // By source
      pool.query(
        `SELECT
          COALESCE(c.source, 'unknown') as source,
          COUNT(DISTINCT c2.click_id) as conversions,
          COALESCE(SUM(c2.payout), 0) as revenue,
          ROUND(COALESCE(AVG(c2.payout), 0), 2) as avg_payout
        FROM conversions c2
        LEFT JOIN clicks c ON c.click_id = c2.click_id
        WHERE c2.created_at >= NOW() - $1::interval
        GROUP BY c.source`,
        [interval]
      ),

      // By state (top 20)
      pool.query(
        `SELECT
          state,
          COUNT(*) as events,
          COALESCE(SUM(payout), 0) as revenue,
          ROUND(COALESCE(AVG(payout), 0), 2) as avg_payout
        FROM conversions
        WHERE created_at >= NOW() - $1::interval
          AND state IS NOT NULL AND state != ''
        GROUP BY state
        ORDER BY revenue DESC
        LIMIT 20`,
        [interval]
      ),

      // RPL by campaign
      pool.query(
        `SELECT
          COALESCE(c.campaign_id, 'unknown') as campaign_id,
          COUNT(DISTINCT c2.click_id) as leads,
          COALESCE(SUM(c2.payout), 0) as revenue,
          ROUND(COALESCE(SUM(c2.payout), 0) / NULLIF(COUNT(DISTINCT c2.click_id), 0), 2) as rpl
        FROM conversions c2
        LEFT JOIN clicks c ON c.click_id = c2.click_id
        WHERE c2.created_at >= NOW() - $1::interval
          AND c.campaign_id IS NOT NULL
        GROUP BY c.campaign_id
        ORDER BY revenue DESC`,
        [interval]
      ),

      // Overall summary
      pool.query(
        `SELECT
          COUNT(DISTINCT click_id) as total_clicks,
          (SELECT COUNT(*) FROM conversions WHERE created_at >= NOW() - $1::interval) as total_events,
          COALESCE(SUM(payout), 0) as total_revenue
        FROM conversions
        WHERE created_at >= NOW() - $1::interval`,
        [interval]
      ),
    ]);

    res.json({
      period: `Last ${days} days`,
      summary: summary.rows[0] || {},
      daily: dailyReport.rows,
      by_source: sourceReport.rows,
      by_state: stateReport.rows,
      by_campaign: campaignReport.rows,
    });
  } catch (err) {
    console.error('[REPORT] Error:', err.message);
    res.status(500).json({ error: 'Report failed' });
  }
});

// --- Route: Click detail (for debugging) ---
app.get('/api/click/:clickId', async (req, res) => {
  try {
    const { clickId } = req.params;
    if (!clickId || clickId.length > 36) return res.status(400).json({ error: 'Invalid click ID' });

    const [click, conversions] = await Promise.all([
      pool.query('SELECT * FROM clicks WHERE click_id = $1', [clickId]),
      pool.query('SELECT * FROM conversions WHERE click_id = $1 ORDER BY created_at', [clickId]),
    ]);

    if (click.rows.length === 0) return res.status(404).json({ error: 'Click not found' });

    res.json({
      click: click.rows[0],
      conversions: conversions.rows,
      total_revenue: conversions.rows.reduce((sum, c) => sum + parseFloat(c.payout || 0), 0),
    });
  } catch (err) {
    console.error('[API] Click detail error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// --- Route: Recent activity feed ---
app.get('/api/activity', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 7, 1), 365);
    const limit = Math.min(Math.max(parseInt(req.query.limit) || 20, 1), 100);
    const interval = `${days} days`;

    const result = await pool.query(
      `SELECT c2.click_id, c2.event_type, c2.payout, c2.state, c2.uid, c2.created_at,
              c.source, c.campaign_id, c.keyword
       FROM conversions c2
       LEFT JOIN clicks c ON c.click_id = c2.click_id
       WHERE c2.created_at >= NOW() - $1::interval
       ORDER BY c2.created_at DESC
       LIMIT $2`,
      [interval, limit]
    );

    res.json(result.rows);
  } catch (err) {
    console.error('[API] Activity error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// --- Route: Dashboard (password protected) ---
const DASHBOARD_PASSWORD = process.env.DASHBOARD_PASSWORD || 'Roas2026!';

// Auth check middleware for dashboard routes
function requireAuth(req, res, next) {
  // Check for session cookie
  const cookie = req.headers.cookie || '';
  const match = cookie.match(/revx_auth=([^;]+)/);
  if (match && match[1] === Buffer.from(DASHBOARD_PASSWORD).toString('base64')) {
    return next();
  }
  return res.redirect('/login');
}

app.get('/login', (req, res) => {
  const error = req.query.error ? '<div class="error">Wrong password</div>' : '';
  res.send(`<!DOCTYPE html>
<html><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Login — Active Funnel Tracker</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><path d='M24 8v20l-12 24a4 4 0 003.6 5.6h32.8A4 4 0 0052 52L40 28V8' fill='none' stroke='%236366f1' stroke-width='4' stroke-linecap='round' stroke-linejoin='round'/><path d='M20 8h24' fill='none' stroke='%236366f1' stroke-width='4' stroke-linecap='round'/><ellipse cx='32' cy='46' rx='10' ry='4' fill='%236366f180'/><circle cx='30' cy='40' r='3' fill='%2322c55e80'/><circle cx='36' cy='44' r='2' fill='%23eab30880'/></svg>">
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;background:#0f1117;color:#e4e4e7;display:flex;justify-content:center;align-items:center;min-height:100vh}
  .box{background:#1a1d27;border:1px solid #2a2d3a;border-radius:16px;padding:40px;width:100%;max-width:360px;text-align:center}
  h1{font-size:20px;margin-bottom:8px;letter-spacing:-0.5px}
  h1 span{color:#6366f1}
  .sub{color:#8b8d97;font-size:13px;margin-bottom:24px}
  input{width:100%;padding:12px 16px;background:#0f1117;border:1px solid #2a2d3a;border-radius:8px;color:#e4e4e7;font-size:14px;margin-bottom:12px;outline:none}
  input:focus{border-color:#6366f1}
  button{width:100%;padding:12px;background:#6366f1;border:none;border-radius:8px;color:#fff;font-size:14px;font-weight:600;cursor:pointer}
  button:hover{background:#5558e6}
  .error{background:rgba(239,68,68,0.15);color:#ef4444;padding:10px;border-radius:8px;font-size:13px;margin-bottom:16px}
</style>
</head><body>
<div class="box">
  <h1><span>●</span> Active Funnel</h1>
  <div class="sub">Enter password to view dashboard</div>
  ${error}
  <form method="POST" action="/login">
    <input type="password" name="password" placeholder="Password" autofocus>
    <button type="submit">Sign In</button>
  </form>
</div>
</body></html>`);
});

app.post('/login', express.urlencoded({ extended: false }), (req, res) => {
  if (req.body.password === DASHBOARD_PASSWORD) {
    const token = Buffer.from(DASHBOARD_PASSWORD).toString('base64');
    res.setHeader('Set-Cookie', `revx_auth=${token}; Path=/; HttpOnly; SameSite=Strict; Secure; Max-Age=604800`);
    return res.redirect('/');
  }
  res.redirect('/login?error=1');
});

app.get('/logout', (req, res) => {
  res.setHeader('Set-Cookie', 'revx_auth=; Path=/; HttpOnly; Max-Age=0');
  res.redirect('/login');
});

app.get('/', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'dashboard.html'));
});

// Protect API routes too (except postback and click which are public)
app.use('/api', (req, res, next) => {
  const cookie = req.headers.cookie || '';
  const match = cookie.match(/revx_auth=([^;]+)/);
  if (match && match[1] === Buffer.from(DASHBOARD_PASSWORD).toString('base64')) {
    return next();
  }
  res.status(401).json({ error: 'Unauthorized' });
});

// --- Route: Serve tracker.js ---
const path = require('path');
app.get('/tracker.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.setHeader('Cache-Control', 'public, max-age=3600');
  res.sendFile(path.join(__dirname, 'tracker.js'));
});

// --- Route: Health check ---
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  } catch (err) {
    res.status(503).json({ status: 'unhealthy', error: err.message });
  }
});

// --- Graceful shutdown ---
process.on('SIGTERM', async () => {
  console.log('[SERVER] SIGTERM received, shutting down...');
  await pool.end();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('[SERVER] SIGINT received, shutting down...');
  await pool.end();
  process.exit(0);
});

// --- Start Server ---
const PORT = process.env.PORT || 3456;
app.listen(PORT, async () => {
  await initDB();
  console.log(`[SERVER] RevX Tracker running on port ${PORT}`);
  console.log(`[SERVER] Postback URL: https://track.activefunnel.ai/postback?tid={tid}&payout={payout}&uid={uid}&state={state}&insured={insured}&own_home={own_home}&multi_vehicle={multi_vehicle}&event_type={event_type}`);
});

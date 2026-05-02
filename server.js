// RevX Tracker - Postback, Click & Funnel Tracking Server
// Hetzner VPS (aos-host-ash-1)

const express = require('express');
const { Pool } = require('pg');
const crypto = require('crypto');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const path = require('path');
const http = require('http');

const app = express();
app.use(express.json({ limit: '100kb' }));
app.use(express.urlencoded({ extended: false }));
app.set('trust proxy', 1);

// --- CORS: only allow your landing page domains ---
const ALLOWED_ORIGINS = [
  'https://driverrates.com',
  'https://www.driverrates.com',
  'https://save-on-insurance.com',
  'https://www.save-on-insurance.com',
  'https://quoteshiftauto.com',
  'https://www.quoteshiftauto.com',
  'http://localhost:3000',
  'http://localhost:8080',
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
    callback(null, false);
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  credentials: false,
}));

// --- Rate limiting ---
const clickLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests' },
});

const postbackLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests' },
});

const eventLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
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
    // Core tables
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
    `);

    // Add visitor_id column to clicks if missing (backward compat)
    await client.query(`
      ALTER TABLE clicks ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(36);
    `);

    // Funnel / visitor tables
    await client.query(`
      CREATE TABLE IF NOT EXISTS visitors (
        visitor_id VARCHAR(36) PRIMARY KEY,
        first_seen TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        device VARCHAR(50),
        browser VARCHAR(100),
        os VARCHAR(100),
        ip_address VARCHAR(45),
        country VARCHAR(100),
        region VARCHAR(100),
        city VARCHAR(100),
        raw_ua TEXT
      );

      CREATE TABLE IF NOT EXISTS page_events (
        id SERIAL PRIMARY KEY,
        visitor_id VARCHAR(36) REFERENCES visitors(visitor_id) ON DELETE CASCADE,
        click_id VARCHAR(36),
        event_type VARCHAR(50),
        page_url VARCHAR(1000),
        referrer TEXT,
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS funnel_steps (
        id SERIAL PRIMARY KEY,
        visitor_id VARCHAR(36) REFERENCES visitors(visitor_id) ON DELETE CASCADE,
        click_id VARCHAR(36),
        step_name VARCHAR(50),
        step_url VARCHAR(1000),
        entered_at TIMESTAMPTZ DEFAULT NOW(),
        exited_at TIMESTAMPTZ,
        time_spent_ms INTEGER
      );

      CREATE INDEX IF NOT EXISTS idx_clicks_created ON clicks(created_at);
      CREATE INDEX IF NOT EXISTS idx_clicks_source ON clicks(source);
      CREATE INDEX IF NOT EXISTS idx_clicks_visitor ON clicks(visitor_id);
      CREATE INDEX IF NOT EXISTS idx_conversions_click_id ON conversions(click_id);
      CREATE INDEX IF NOT EXISTS idx_conversions_created ON conversions(created_at);
      CREATE INDEX IF NOT EXISTS idx_conversions_event_type ON conversions(event_type);
      CREATE INDEX IF NOT EXISTS idx_visitors_last_seen ON visitors(last_seen);
      CREATE INDEX IF NOT EXISTS idx_page_events_visitor ON page_events(visitor_id);
      CREATE INDEX IF NOT EXISTS idx_page_events_created ON page_events(created_at);
      CREATE INDEX IF NOT EXISTS idx_page_events_type ON page_events(event_type);
      CREATE INDEX IF NOT EXISTS idx_funnel_steps_visitor ON funnel_steps(visitor_id);
      CREATE INDEX IF NOT EXISTS idx_funnel_steps_step ON funnel_steps(step_name);
      CREATE INDEX IF NOT EXISTS idx_funnel_steps_entered ON funnel_steps(entered_at);
    `);

    // Multi-campaign / multi-buyer tables
    await client.query(`
      CREATE TABLE IF NOT EXISTS buyers (
        id SERIAL PRIMARY KEY,
        slug VARCHAR(50) UNIQUE NOT NULL,
        name VARCHAR(100) NOT NULL,
        offer_url_template TEXT,
        postback_token VARCHAR(64),
        active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS campaigns (
        id SERIAL PRIMARY KEY,
        slug VARCHAR(50) UNIQUE NOT NULL,
        name VARCHAR(100) NOT NULL,
        traffic_source VARCHAR(50),
        landing_page VARCHAR(255),
        active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS campaign_buyers (
        id SERIAL PRIMARY KEY,
        campaign_id INTEGER REFERENCES campaigns(id),
        buyer_id INTEGER REFERENCES buyers(id),
        weight INTEGER DEFAULT 100,
        offer_url_override TEXT,
        active BOOLEAN DEFAULT true,
        UNIQUE(campaign_id, buyer_id)
      );
    `);

    // Add campaign/buyer tracking columns to existing tables (backward compat)
    await client.query(`
      ALTER TABLE clicks ADD COLUMN IF NOT EXISTS campaign_slug VARCHAR(50);
      ALTER TABLE clicks ADD COLUMN IF NOT EXISTS buyer_slug VARCHAR(50);
      ALTER TABLE clicks ADD COLUMN IF NOT EXISTS buyer_id INTEGER;
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS campaign_slug VARCHAR(50);
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS buyer_slug VARCHAR(50);
    `);

    // Indexes for new tables/columns
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_clicks_campaign_slug ON clicks(campaign_slug);
      CREATE INDEX IF NOT EXISTS idx_clicks_buyer_slug ON clicks(buyer_slug);
      CREATE INDEX IF NOT EXISTS idx_conversions_campaign_slug ON conversions(campaign_slug);
      CREATE INDEX IF NOT EXISTS idx_conversions_buyer_slug ON conversions(buyer_slug);
      CREATE INDEX IF NOT EXISTS idx_campaign_buyers_campaign ON campaign_buyers(campaign_id);
      CREATE INDEX IF NOT EXISTS idx_campaign_buyers_buyer ON campaign_buyers(buyer_id);
    `);

    // Seed default buyer (SmartFinancial)
    await client.query(`
      INSERT INTO buyers (slug, name, offer_url_template)
      VALUES ('smartfinancial', 'SmartFinancial',
        'https://midasrates.com/get-fast-quote.html?aid=115756&cid=10490&form_type=3&phone_cid=default&lead_type_id=1&tid={tid}')
      ON CONFLICT (slug) DO NOTHING;
    `);

    // Seed 3 existing campaigns
    await client.query(`
      INSERT INTO campaigns (slug, name, traffic_source, landing_page) VALUES
        ('sf-meta-10490',    'SF Meta - driverrates.com',            'meta',    'driverrates.com'),
        ('sf-youtube-10491', 'SF YouTube - save-on-insurance.com',   'youtube', 'save-on-insurance.com'),
        ('sf-search-10489',  'SF Search - quoteshiftauto.com',       'google',  'quoteshiftauto.com')
      ON CONFLICT (slug) DO NOTHING;
    `);

    // Link campaigns to SmartFinancial with CID-specific offer URL overrides
    await client.query(`
      INSERT INTO campaign_buyers (campaign_id, buyer_id, weight, offer_url_override)
      SELECT c.id, b.id, 100,
        CASE c.slug
          WHEN 'sf-meta-10490'    THEN 'https://midasrates.com/get-fast-quote.html?aid=115756&cid=10490&form_type=3&phone_cid=default&lead_type_id=1&tid={tid}'
          WHEN 'sf-youtube-10491' THEN 'https://midasrates.com/get-fast-quote.html?aid=115756&cid=10491&form_type=3&phone_cid=default&lead_type_id=1&tid={tid}'
          WHEN 'sf-search-10489'  THEN 'https://midasrates.com/get-fast-quote.html?aid=115756&cid=10489&form_type=3&phone_cid=default&lead_type_id=1&tid={tid}'
        END
      FROM campaigns c
      CROSS JOIN buyers b
      WHERE b.slug = 'smartfinancial'
        AND c.slug IN ('sf-meta-10490', 'sf-youtube-10491', 'sf-search-10489')
      ON CONFLICT (campaign_id, buyer_id) DO NOTHING;
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

function isValidUUID(str) {
  return typeof str === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(str);
}

function parseUserAgent(ua) {
  if (!ua) return { device: 'unknown', browser: 'unknown', os: 'unknown' };

  const device = /Mobile|Android(?!.*Tablet)|iPhone|iPod/i.test(ua) ? 'mobile'
    : /iPad|Android.*Tablet|Tablet/i.test(ua) ? 'tablet'
    : 'desktop';

  let browser = 'unknown';
  if (/Edg\//i.test(ua)) browser = 'Edge';
  else if (/OPR\/|Opera\//i.test(ua)) browser = 'Opera';
  else if (/SamsungBrowser/i.test(ua)) browser = 'Samsung';
  else if (/Chrome\/\d/i.test(ua) && !/Chromium/i.test(ua)) browser = 'Chrome';
  else if (/Firefox\/\d/i.test(ua)) browser = 'Firefox';
  else if (/Safari\/\d/i.test(ua) && !/Chrome/i.test(ua)) browser = 'Safari';
  else if (/MSIE|Trident/i.test(ua)) browser = 'IE';

  let os = 'unknown';
  if (/Windows NT/i.test(ua)) os = 'Windows';
  else if (/iPhone|iPod/i.test(ua)) os = 'iOS';
  else if (/iPad/i.test(ua)) os = 'iPadOS';
  else if (/Mac OS X/i.test(ua)) os = 'macOS';
  else if (/Android/i.test(ua)) os = 'Android';
  else if (/CrOS/i.test(ua)) os = 'ChromeOS';
  else if (/Linux/i.test(ua)) os = 'Linux';

  return { device, browser, os };
}

const BOT_PATTERN = /bot|crawler|spider|crawl|Googlebot|bingbot|Slurp|DuckDuck|Yahoo!|Baidu|yandex|facebookexternalhit|Twitterbot|LinkedInBot|curl|wget|python-requests|python\/|java\/|Go-http|okhttp|axios|libwww|HeadlessChrome|PhantomJS|Selenium|puppeteer|playwright|prerender|SiteChecker|AhrefsBot|MJ12bot|DotBot|SemrushBot|rogerbot/i;

function isBot(ua) {
  return BOT_PATTERN.test(ua || '');
}

// Weighted random selection from an array of objects with a `.weight` property
function pickWeightedBuyer(buyers) {
  if (!buyers || buyers.length === 0) return null;
  if (buyers.length === 1) return buyers[0];
  const total = buyers.reduce((sum, b) => sum + (parseInt(b.weight) || 100), 0);
  let rand = Math.random() * total;
  for (const b of buyers) {
    rand -= (parseInt(b.weight) || 100);
    if (rand <= 0) return b;
  }
  return buyers[buyers.length - 1];
}

function resolveGeo(ip, visitorId) {
  if (!ip) return;
  if (ip === '::1' || ip === '127.0.0.1') return;
  if (/^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)/.test(ip)) return;

  const reqUrl = `http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,country,regionName,city`;
  http.get(reqUrl, (res) => {
    let raw = '';
    res.on('data', chunk => raw += chunk);
    res.on('end', () => {
      try {
        const data = JSON.parse(raw);
        if (data.status === 'success') {
          pool.query(
            'UPDATE visitors SET country=$1, region=$2, city=$3 WHERE visitor_id=$4',
            [data.country || null, data.regionName || null, data.city || null, visitorId]
          ).catch(() => {});
        }
      } catch (e) {}
    });
  }).on('error', () => {});
}

// --- Route: Register a click (called by tracker.js via POST) ---
app.post('/click', clickLimiter, async (req, res) => {
  try {
    const clickId = generateClickId();
    const {
      source, gclid, fbclid, campaign_id, adgroup_id,
      keyword, landing_page, offer_url, visitor_id, campaign
    } = req.body;

    const cleanVisitorId = visitor_id && isValidUUID(visitor_id) ? visitor_id : null;

    // Smart routing: if a campaign slug is provided, pick a buyer
    let campaignSlug = sanitizeString(campaign, 50);
    let buyerSlug = null;
    let buyerId = null;
    let resolvedOfferUrl = sanitizeString(offer_url, 500);

    if (campaignSlug) {
      try {
        const campResult = await pool.query(
          `SELECT c.id as camp_id, c.slug as camp_slug,
                  cb.offer_url_override, cb.weight,
                  b.id as b_id, b.slug as b_slug, b.offer_url_template
           FROM campaigns c
           JOIN campaign_buyers cb ON cb.campaign_id = c.id
           JOIN buyers b ON b.id = cb.buyer_id
           WHERE c.slug = $1 AND c.active = true AND cb.active = true AND b.active = true`,
          [campaignSlug]
        );

        if (campResult.rows.length > 0) {
          const selected = pickWeightedBuyer(campResult.rows);
          buyerSlug = selected.b_slug;
          buyerId = selected.b_id;
          const urlTemplate = selected.offer_url_override || selected.offer_url_template;
          if (urlTemplate) {
            resolvedOfferUrl = urlTemplate.replace('{tid}', clickId);
          }
          console.log(`[CLICK] campaign=${campaignSlug} buyer=${buyerSlug} weight=${selected.weight}`);
        } else {
          console.warn(`[CLICK] Campaign not found or no buyers: ${campaignSlug}`);
          campaignSlug = null;
        }
      } catch (routeErr) {
        console.error('[CLICK] Campaign routing error:', routeErr.message);
        campaignSlug = null;
      }
    }

    await pool.query(
      `INSERT INTO clicks
        (click_id, source, gclid, fbclid, campaign_id, adgroup_id, keyword, landing_page, offer_url,
         ip_address, user_agent, referer, visitor_id, campaign_slug, buyer_slug, buyer_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        clickId,
        sanitizeString(source, 50) || 'unknown',
        sanitizeString(gclid),
        sanitizeString(fbclid),
        sanitizeString(campaign_id, 100),
        sanitizeString(adgroup_id, 100),
        sanitizeString(keyword),
        sanitizeString(landing_page, 500),
        resolvedOfferUrl,
        req.ip,
        sanitizeString(req.headers['user-agent'], 1000),
        sanitizeString(req.headers['referer'], 1000),
        cleanVisitorId,
        campaignSlug,
        buyerSlug,
        buyerId,
      ]
    );

    console.log(`[CLICK] ${clickId} src=${source || 'unknown'} campaign=${campaignSlug || '-'} buyer=${buyerSlug || '-'} ip=${req.ip}`);
    res.json({ click_id: clickId, offer_url: resolvedOfferUrl || '' });
  } catch (err) {
    console.error('[CLICK] Error:', err.message);
    res.status(500).json({ error: 'Failed to register click' });
  }
});

// --- Route: Postback (supports /postback and /postback/:campaignSlug) ---
async function handlePostback(req, res) {
  try {
    // Accept params from query string (GET) or body (POST) — merges both, query takes priority
    const params = { ...req.body, ...req.query };
    const { tid, payout, uid, state, insured, own_home, multi_vehicle, event_type } = params;
    const routeCampaignSlug = req.params.campaignSlug
      ? sanitizeString(req.params.campaignSlug, 50)
      : null;

    if (!tid || typeof tid !== 'string' || tid.length > 36) {
      console.warn('[POSTBACK] Invalid/missing tid:', tid);
      return res.status(400).send('Missing or invalid tid');
    }

    const rawParams = JSON.stringify(req.query);
    const cleanPayout = sanitizePayout(payout);

    // Look up campaign_slug and buyer_slug from the originating click
    let clickCampaignSlug = routeCampaignSlug;
    let clickBuyerSlug = null;
    try {
      const clickRec = await pool.query(
        'SELECT campaign_slug, buyer_slug FROM clicks WHERE click_id = $1',
        [tid]
      );
      if (clickRec.rows.length > 0) {
        clickCampaignSlug = clickRec.rows[0].campaign_slug || routeCampaignSlug;
        clickBuyerSlug = clickRec.rows[0].buyer_slug;
      }
    } catch (lookupErr) {
      console.warn('[POSTBACK] Click lookup failed:', lookupErr.message);
    }

    const insertConversion = async () => {
      await pool.query(
        `INSERT INTO conversions
          (click_id, event_type, payout, uid, state, insured, own_home, multi_vehicle, raw_params, campaign_slug, buyer_slug)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
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
          clickCampaignSlug,
          clickBuyerSlug,
        ]
      );
    };

    try {
      await insertConversion();
    } catch (fkErr) {
      if (fkErr.code === '23503') {
        await pool.query(
          `INSERT INTO clicks (click_id, source, campaign_slug, buyer_slug)
           VALUES ($1, 'postback_only', $2, $3) ON CONFLICT DO NOTHING`,
          [tid, clickCampaignSlug, clickBuyerSlug]
        );
        await insertConversion();
        console.log(`[POSTBACK] Stored (orphan click): tid=${tid} payout=${cleanPayout} event=${event_type || 'unknown'} campaign=${clickCampaignSlug || '-'}`);
        return res.status(200).send('OK');
      }
      throw fkErr;
    }

    console.log(`[POSTBACK] tid=${tid} payout=${cleanPayout} event=${event_type || 'unknown'} state=${state || '-'} campaign=${clickCampaignSlug || '-'} buyer=${clickBuyerSlug || '-'}`);
    res.status(200).send('OK');
  } catch (err) {
    console.error('[POSTBACK] Error:', err.message);
    res.status(500).send('Error');
  }
}

app.all('/postback', postbackLimiter, handlePostback);
app.all('/postback/:campaignSlug', postbackLimiter, handlePostback);

// --- Route: Funnel Event Ingestion (public, called by tracker.js) ---
// POST /event — receives visitor events, step info, pageviews, scroll, etc.
app.post('/event', eventLimiter, async (req, res) => {
  try {
    const { visitor_id, click_id, step, step_url, events } = req.body;

    // Validate visitor_id
    if (!visitor_id || !isValidUUID(visitor_id)) {
      return res.status(400).json({ error: 'Invalid visitor_id' });
    }

    // Bot check
    const ua = sanitizeString(req.headers['user-agent'], 1000) || '';
    if (isBot(ua)) {
      return res.status(204).send();
    }

    const { device, browser, os } = parseUserAgent(ua);
    const ip = req.ip;
    const cleanClickId = click_id && isValidUUID(click_id) ? click_id : null;

    // Upsert visitor — detect if new via xmax trick
    const upsertResult = await pool.query(
      `INSERT INTO visitors (visitor_id, device, browser, os, ip_address, raw_ua)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (visitor_id) DO UPDATE
         SET last_seen = NOW(),
             ip_address = EXCLUDED.ip_address,
             raw_ua = EXCLUDED.raw_ua
       RETURNING (xmax = 0) AS is_new`,
      [visitor_id, device, browser, os, ip, ua]
    );
    const isNew = upsertResult.rows[0]?.is_new === true;
    if (isNew) {
      // Kick off async geo resolution (no await — don't block response)
      resolveGeo(ip, visitor_id);
    }

    // Link visitor to click if not already linked
    if (cleanClickId) {
      pool.query(
        'UPDATE clicks SET visitor_id = $1 WHERE click_id = $2 AND visitor_id IS NULL',
        [visitor_id, cleanClickId]
      ).catch(() => {});
    }

    // Insert funnel step if provided (allow multiple visits, dedup within 30 min)
    if (step) {
      const validSteps = ['landing', 'presale', 'offer', 'conversion'];
      const cleanStep = validSteps.includes(step) ? step : 'landing';
      const cleanStepUrl = sanitizeString(step_url, 1000);

      await pool.query(
        `INSERT INTO funnel_steps (visitor_id, click_id, step_name, step_url)
         SELECT $1, $2, $3, $4
         WHERE NOT EXISTS (
           SELECT 1 FROM funnel_steps
           WHERE visitor_id = $1
             AND step_name = $3
             AND entered_at > NOW() - INTERVAL '30 minutes'
         )`,
        [visitor_id, cleanClickId, cleanStep, cleanStepUrl]
      );
    }

    // Insert page events (max 50 per request to prevent abuse)
    if (Array.isArray(events) && events.length > 0) {
      const batch = events.slice(0, 50);
      for (const ev of batch) {
        if (!ev || !ev.type) continue;

        const evType = sanitizeString(ev.type, 50);
        const evUrl = sanitizeString(ev.page_url, 1000);
        const evRef = sanitizeString(ev.referrer, 1000);
        const evMeta = ev.metadata && typeof ev.metadata === 'object'
          ? JSON.stringify(ev.metadata)
          : null;
        let evTs = new Date().toISOString();
        if (ev.timestamp) {
          try { evTs = new Date(ev.timestamp).toISOString(); } catch (e) {}
        }

        // Update funnel step time_spent when we get a time_on_page event
        if (evType === 'time_on_page' && ev.metadata?.duration_ms > 0) {
          const durationMs = Math.min(Math.round(ev.metadata.duration_ms), 3600000); // cap at 1h
          pool.query(
            `UPDATE funnel_steps
             SET time_spent_ms = $1, exited_at = NOW()
             WHERE id = (
               SELECT id FROM funnel_steps
               WHERE visitor_id = $2 AND exited_at IS NULL
               ORDER BY entered_at DESC LIMIT 1
             )`,
            [durationMs, visitor_id]
          ).catch(() => {});
        }

        await pool.query(
          `INSERT INTO page_events (visitor_id, click_id, event_type, page_url, referrer, metadata, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [visitor_id, cleanClickId, evType, evUrl, evRef, evMeta, evTs]
        );
      }
    }

    res.status(204).send();
  } catch (err) {
    console.error('[EVENT] Error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// --- Dashboard Auth ---
const DASHBOARD_PASSWORD = process.env.DASHBOARD_PASSWORD || 'Roas2026!';

function requireAuth(req, res, next) {
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

// Protect /api/* routes (except postback and click which are public)
app.use('/api', (req, res, next) => {
  const cookie = req.headers.cookie || '';
  const match = cookie.match(/revx_auth=([^;]+)/);
  if (match && match[1] === Buffer.from(DASHBOARD_PASSWORD).toString('base64')) {
    return next();
  }
  res.status(401).json({ error: 'Unauthorized' });
});

// --- Route: Revenue Report ---
app.get('/api/report', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 7, 1), 365);
    const interval = `${days} days`;
    const campaignFilter = req.query.campaign ? sanitizeString(req.query.campaign, 50) : null;

    const [dailyReport, sourceReport, stateReport, campaignReport, summary, visitorSummary] = await Promise.all([
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

      pool.query(
        `SELECT
          COUNT(DISTINCT click_id) as total_clicks,
          (SELECT COUNT(*) FROM conversions WHERE created_at >= NOW() - $1::interval) as total_events,
          COALESCE(SUM(payout), 0) as total_revenue
        FROM conversions
        WHERE created_at >= NOW() - $1::interval`,
        [interval]
      ),

      // Visitor counts & funnel conversion rate
      pool.query(
        `SELECT
          COUNT(DISTINCT v.visitor_id) as total_visitors,
          COUNT(DISTINCT CASE WHEN fs.step_name = 'conversion' THEN v.visitor_id END) as converted_visitors
        FROM visitors v
        LEFT JOIN funnel_steps fs ON fs.visitor_id = v.visitor_id
          AND fs.entered_at >= NOW() - $1::interval
        WHERE v.last_seen >= NOW() - $1::interval`,
        [interval]
      ),
    ]);

    res.json({
      period: `Last ${days} days`,
      summary: {
        ...summary.rows[0] || {},
        total_visitors: visitorSummary.rows[0]?.total_visitors || 0,
        converted_visitors: visitorSummary.rows[0]?.converted_visitors || 0,
      },
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

// --- Route: Click detail ---
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

// --- Route: Recent Visitors List ---
app.get('/api/visitors', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 7, 1), 365);
    const limit = Math.min(Math.max(parseInt(req.query.limit) || 50, 1), 100);
    const interval = `${days} days`;

    const result = await pool.query(
      `SELECT
        v.visitor_id,
        v.first_seen,
        v.last_seen,
        v.device,
        v.browser,
        v.os,
        v.country,
        v.city,
        COUNT(DISTINCT pe.id) as event_count,
        (
          SELECT step_name FROM funnel_steps fs2
          WHERE fs2.visitor_id = v.visitor_id
          ORDER BY entered_at DESC LIMIT 1
        ) as current_step,
        (
          SELECT STRING_AGG(DISTINCT step_name, ',' ORDER BY step_name)
          FROM funnel_steps fs3
          WHERE fs3.visitor_id = v.visitor_id
            AND fs3.entered_at >= NOW() - $1::interval
        ) as steps_visited
       FROM visitors v
       LEFT JOIN page_events pe ON pe.visitor_id = v.visitor_id
         AND pe.created_at >= NOW() - $1::interval
       WHERE v.last_seen >= NOW() - $1::interval
       GROUP BY v.visitor_id
       ORDER BY v.last_seen DESC
       LIMIT $2`,
      [interval, limit]
    );

    res.json(result.rows);
  } catch (err) {
    console.error('[API] Visitors error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// --- Route: Visitor Detail (full journey) ---
app.get('/api/visitor/:visitorId', async (req, res) => {
  try {
    const { visitorId } = req.params;
    if (!isValidUUID(visitorId)) return res.status(400).json({ error: 'Invalid visitor ID' });

    const [visitor, events, steps, clicks, conversions] = await Promise.all([
      pool.query('SELECT * FROM visitors WHERE visitor_id = $1', [visitorId]),
      pool.query(
        `SELECT id, event_type, page_url, referrer, metadata, created_at
         FROM page_events WHERE visitor_id = $1
         ORDER BY created_at ASC LIMIT 500`,
        [visitorId]
      ),
      pool.query(
        `SELECT step_name, step_url, entered_at, exited_at, time_spent_ms
         FROM funnel_steps WHERE visitor_id = $1
         ORDER BY entered_at ASC`,
        [visitorId]
      ),
      pool.query(
        `SELECT click_id, source, campaign_id, keyword, landing_page, created_at
         FROM clicks WHERE visitor_id = $1
         ORDER BY created_at ASC`,
        [visitorId]
      ),
      pool.query(
        `SELECT c2.event_type, c2.payout, c2.state, c2.created_at
         FROM conversions c2
         JOIN clicks cl ON cl.click_id = c2.click_id
         WHERE cl.visitor_id = $1
         ORDER BY c2.created_at ASC`,
        [visitorId]
      ),
    ]);

    if (visitor.rows.length === 0) return res.status(404).json({ error: 'Visitor not found' });

    res.json({
      visitor: visitor.rows[0],
      events: events.rows,
      funnel_steps: steps.rows,
      clicks: clicks.rows,
      conversions: conversions.rows,
      total_revenue: conversions.rows.reduce((sum, c) => sum + parseFloat(c.payout || 0), 0),
    });
  } catch (err) {
    console.error('[API] Visitor detail error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// --- Route: Funnel Report ---
app.get('/api/funnel', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 7, 1), 365);
    const interval = `${days} days`;

    const [steps, avgTimes] = await Promise.all([
      pool.query(
        `SELECT
          step_name,
          COUNT(DISTINCT visitor_id) as visitors
         FROM funnel_steps
         WHERE entered_at >= NOW() - $1::interval
         GROUP BY step_name
         ORDER BY CASE step_name
           WHEN 'landing' THEN 1
           WHEN 'presale' THEN 2
           WHEN 'offer' THEN 3
           WHEN 'conversion' THEN 4
           ELSE 5 END`,
        [interval]
      ),
      pool.query(
        `SELECT
          fs.step_name,
          ROUND(AVG(fs.time_spent_ms)) as avg_time_ms
         FROM funnel_steps fs
         WHERE fs.entered_at >= NOW() - $1::interval
           AND fs.time_spent_ms IS NOT NULL
         GROUP BY fs.step_name`,
        [interval]
      ),
    ]);

    // Merge avg times
    const avgMap = {};
    avgTimes.rows.forEach(r => { avgMap[r.step_name] = parseInt(r.avg_time_ms) || null; });

    const rows = steps.rows.map(r => ({
      step_name: r.step_name,
      visitors: parseInt(r.visitors) || 0,
      avg_time_ms: avgMap[r.step_name] || null,
    }));

    // Calculate drop-off rates
    const topStep = rows[0];
    const topCount = topStep ? topStep.visitors : 1;
    rows.forEach((r, i) => {
      r.pct_of_top = topCount > 0 ? Math.round((r.visitors / topCount) * 100) : 0;
      r.dropoff_pct = i > 0 ? Math.round(((rows[i-1].visitors - r.visitors) / Math.max(rows[i-1].visitors, 1)) * 100) : 0;
    });

    res.json({ period: `Last ${days} days`, steps: rows });
  } catch (err) {
    console.error('[API] Funnel error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// --- Route: Conversion Paths ---
app.get('/api/paths', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 7, 1), 365);
    const limit = Math.min(Math.max(parseInt(req.query.limit) || 20, 1), 50);
    const interval = `${days} days`;

    const result = await pool.query(
      `WITH visitor_paths AS (
         SELECT
           visitor_id,
           STRING_AGG(step_name, ' → ' ORDER BY entered_at) as path,
           COUNT(*) as steps_taken
         FROM funnel_steps
         WHERE entered_at >= NOW() - $1::interval
         GROUP BY visitor_id
       )
       SELECT
         path,
         COUNT(*) as visitor_count,
         ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 1) as pct
       FROM visitor_paths
       GROUP BY path
       ORDER BY visitor_count DESC
       LIMIT $2`,
      [interval, limit]
    );

    res.json({ period: `Last ${days} days`, paths: result.rows });
  } catch (err) {
    console.error('[API] Paths error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// ============================================================
// --- Campaign & Buyer Management API (all behind requireAuth) ---
// ============================================================

// GET /api/buyers — list all buyers
app.get('/api/buyers', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT b.*,
        COUNT(DISTINCT cb.campaign_id) as campaign_count
       FROM buyers b
       LEFT JOIN campaign_buyers cb ON cb.buyer_id = b.id AND cb.active = true
       GROUP BY b.id
       ORDER BY b.created_at DESC`
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[API/buyers] Error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// POST /api/buyers — create buyer
app.post('/api/buyers', async (req, res) => {
  try {
    const { slug, name, offer_url_template, postback_token } = req.body;
    if (!slug || !name) return res.status(400).json({ error: 'slug and name required' });
    const result = await pool.query(
      `INSERT INTO buyers (slug, name, offer_url_template, postback_token)
       VALUES ($1,$2,$3,$4) RETURNING *`,
      [
        sanitizeString(slug, 50),
        sanitizeString(name, 100),
        sanitizeString(offer_url_template, 2000),
        sanitizeString(postback_token, 64),
      ]
    );
    res.json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'Slug already exists' });
    console.error('[API/buyers] Create error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// PUT /api/buyers/:id — update buyer
app.put('/api/buyers/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!id) return res.status(400).json({ error: 'Invalid id' });
    const { name, offer_url_template, postback_token, active } = req.body;
    const result = await pool.query(
      `UPDATE buyers SET
        name = COALESCE($1, name),
        offer_url_template = COALESCE($2, offer_url_template),
        postback_token = COALESCE($3, postback_token),
        active = COALESCE($4, active)
       WHERE id = $5 RETURNING *`,
      [
        name ? sanitizeString(name, 100) : null,
        offer_url_template !== undefined ? sanitizeString(offer_url_template, 2000) : null,
        postback_token !== undefined ? sanitizeString(postback_token, 64) : null,
        active !== undefined ? !!active : null,
        id,
      ]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[API/buyers] Update error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// DELETE /api/buyers/:id — soft delete
app.delete('/api/buyers/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!id) return res.status(400).json({ error: 'Invalid id' });
    await pool.query('UPDATE buyers SET active = false WHERE id = $1', [id]);
    res.json({ ok: true });
  } catch (err) {
    console.error('[API/buyers] Delete error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// GET /api/campaigns — list all campaigns with buyers and basic stats
app.get('/api/campaigns', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 30, 1), 365);
    const interval = `${days} days`;

    const result = await pool.query(
      `SELECT c.*,
        COALESCE(
          json_agg(
            json_build_object(
              'cb_id', cb.id,
              'buyer_id', b.id,
              'buyer_slug', b.slug,
              'buyer_name', b.name,
              'weight', cb.weight,
              'offer_url_override', cb.offer_url_override,
              'active', cb.active
            ) ORDER BY cb.weight DESC
          ) FILTER (WHERE b.id IS NOT NULL),
          '[]'::json
        ) as buyers
       FROM campaigns c
       LEFT JOIN campaign_buyers cb ON cb.campaign_id = c.id
       LEFT JOIN buyers b ON b.id = cb.buyer_id
       GROUP BY c.id
       ORDER BY c.created_at DESC`
    );

    // Enrich with click + revenue stats
    const stats = await pool.query(
      `SELECT
        cl.campaign_slug,
        COUNT(DISTINCT cl.click_id) as clicks,
        COUNT(DISTINCT cv.click_id) as conversions,
        COALESCE(SUM(cv.payout), 0) as revenue
       FROM clicks cl
       LEFT JOIN conversions cv ON cv.click_id = cl.click_id
         AND cv.created_at >= NOW() - $1::interval
       WHERE cl.campaign_slug IS NOT NULL
         AND cl.created_at >= NOW() - $1::interval
       GROUP BY cl.campaign_slug`,
      [interval]
    );

    const statsMap = {};
    stats.rows.forEach(r => { statsMap[r.campaign_slug] = r; });

    const campaigns = result.rows.map(c => ({
      ...c,
      stats: statsMap[c.slug] || { clicks: 0, conversions: 0, revenue: 0 },
    }));

    res.json(campaigns);
  } catch (err) {
    console.error('[API/campaigns] Error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// POST /api/campaigns — create campaign
app.post('/api/campaigns', async (req, res) => {
  try {
    const { slug, name, traffic_source, landing_page } = req.body;
    if (!slug || !name) return res.status(400).json({ error: 'slug and name required' });
    const result = await pool.query(
      `INSERT INTO campaigns (slug, name, traffic_source, landing_page)
       VALUES ($1,$2,$3,$4) RETURNING *`,
      [
        sanitizeString(slug, 50),
        sanitizeString(name, 100),
        sanitizeString(traffic_source, 50),
        sanitizeString(landing_page, 255),
      ]
    );
    res.json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'Slug already exists' });
    console.error('[API/campaigns] Create error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// PUT /api/campaigns/:id — update campaign
app.put('/api/campaigns/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!id) return res.status(400).json({ error: 'Invalid id' });
    const { name, traffic_source, landing_page, active } = req.body;
    const result = await pool.query(
      `UPDATE campaigns SET
        name = COALESCE($1, name),
        traffic_source = COALESCE($2, traffic_source),
        landing_page = COALESCE($3, landing_page),
        active = COALESCE($4, active)
       WHERE id = $5 RETURNING *`,
      [
        name ? sanitizeString(name, 100) : null,
        traffic_source ? sanitizeString(traffic_source, 50) : null,
        landing_page !== undefined ? sanitizeString(landing_page, 255) : null,
        active !== undefined ? !!active : null,
        id,
      ]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[API/campaigns] Update error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// DELETE /api/campaigns/:id — soft delete
app.delete('/api/campaigns/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!id) return res.status(400).json({ error: 'Invalid id' });
    await pool.query('UPDATE campaigns SET active = false WHERE id = $1', [id]);
    res.json({ ok: true });
  } catch (err) {
    console.error('[API/campaigns] Delete error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// POST /api/campaigns/:id/buyers — add buyer to campaign
app.post('/api/campaigns/:id/buyers', async (req, res) => {
  try {
    const campaign_id = parseInt(req.params.id);
    if (!campaign_id) return res.status(400).json({ error: 'Invalid campaign id' });
    const { buyer_id, weight, offer_url_override } = req.body;
    if (!buyer_id) return res.status(400).json({ error: 'buyer_id required' });
    const result = await pool.query(
      `INSERT INTO campaign_buyers (campaign_id, buyer_id, weight, offer_url_override)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (campaign_id, buyer_id) DO UPDATE
         SET weight = EXCLUDED.weight,
             offer_url_override = EXCLUDED.offer_url_override,
             active = true
       RETURNING *`,
      [
        campaign_id,
        parseInt(buyer_id),
        Math.max(1, Math.min(9999, parseInt(weight) || 100)),
        sanitizeString(offer_url_override, 2000),
      ]
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[API/campaigns/buyers] Add error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// PUT /api/campaigns/:campId/buyers/:buyerId — update weight/override
app.put('/api/campaigns/:campId/buyers/:buyerId', async (req, res) => {
  try {
    const campaign_id = parseInt(req.params.campId);
    const buyer_id = parseInt(req.params.buyerId);
    if (!campaign_id || !buyer_id) return res.status(400).json({ error: 'Invalid ids' });
    const { weight, offer_url_override, active } = req.body;
    const result = await pool.query(
      `UPDATE campaign_buyers SET
        weight = COALESCE($1, weight),
        offer_url_override = COALESCE($2, offer_url_override),
        active = COALESCE($3, active)
       WHERE campaign_id = $4 AND buyer_id = $5 RETURNING *`,
      [
        weight !== undefined ? Math.max(1, Math.min(9999, parseInt(weight) || 100)) : null,
        offer_url_override !== undefined ? sanitizeString(offer_url_override, 2000) : null,
        active !== undefined ? !!active : null,
        campaign_id,
        buyer_id,
      ]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[API/campaigns/buyers] Update error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// DELETE /api/campaigns/:campId/buyers/:buyerId — remove buyer from campaign
app.delete('/api/campaigns/:campId/buyers/:buyerId', async (req, res) => {
  try {
    const campaign_id = parseInt(req.params.campId);
    const buyer_id = parseInt(req.params.buyerId);
    if (!campaign_id || !buyer_id) return res.status(400).json({ error: 'Invalid ids' });
    await pool.query(
      'UPDATE campaign_buyers SET active = false WHERE campaign_id = $1 AND buyer_id = $2',
      [campaign_id, buyer_id]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error('[API/campaigns/buyers] Remove error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// GET /api/campaigns/:id/report — per-buyer breakdown for a campaign
app.get('/api/campaigns/:id/report', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!id) return res.status(400).json({ error: 'Invalid id' });
    const days = Math.min(Math.max(parseInt(req.query.days) || 30, 1), 365);
    const interval = `${days} days`;

    const [campaign, buyerStats] = await Promise.all([
      pool.query('SELECT * FROM campaigns WHERE id = $1', [id]),
      pool.query(
        `SELECT
          cl.buyer_slug,
          b.name as buyer_name,
          COUNT(DISTINCT cl.click_id) as clicks,
          COUNT(DISTINCT cv.click_id) as conversions,
          COALESCE(SUM(cv.payout), 0) as revenue,
          ROUND(COALESCE(SUM(cv.payout), 0) / NULLIF(COUNT(DISTINCT cl.click_id), 0), 2) as rpl
         FROM clicks cl
         LEFT JOIN conversions cv ON cv.click_id = cl.click_id
           AND cv.created_at >= NOW() - $2::interval
         LEFT JOIN buyers b ON b.slug = cl.buyer_slug
         WHERE cl.campaign_slug = $1
           AND cl.created_at >= NOW() - $2::interval
         GROUP BY cl.buyer_slug, b.name
         ORDER BY revenue DESC`,
        [campaign.rows[0]?.slug, interval]
      ),
    ]);

    if (campaign.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ campaign: campaign.rows[0], buyers: buyerStats.rows, period: `Last ${days} days` });
  } catch (err) {
    console.error('[API/campaigns/report] Error:', err.message);
    res.status(500).json({ error: 'Failed' });
  }
});

// --- Route: Serve tracker.js ---
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

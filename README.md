# RevX Tracker

Lightweight affiliate click tracker and postback receiver. Built for performance marketing — track clicks, receive postbacks, and visualize revenue in real-time.

## Features

- **Click tracking** — Record clicks with source, campaign, and custom parameters
- **Postback receiver** — Receive server-to-server postbacks from affiliate networks (SmartFinancial, etc.)
- **Real-time dashboard** — Revenue, RPL, click volume, by-source/state/campaign breakdowns
- **Auto-refreshing** — Dashboard updates every 30 seconds
- **Docker-ready** — One-command deploy with Docker Compose (Express + PostgreSQL)
- **Rate limiting** — Built-in protection against abuse
- **Input sanitization** — SQL injection prevention, scoped CORS

## Quick Start

```bash
# Clone
git clone https://github.com/gpadva81/revx-tracker.git
cd revx-tracker

# Configure
cp .env.example .env
# Edit .env with your database password

# Deploy
docker compose up -d
```

The tracker runs on port 3000 by default. Put it behind a reverse proxy (Caddy, nginx) for HTTPS.

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/click` | POST | Record a click (returns click ID) |
| `/postback` | GET | Receive postback from affiliate network |
| `/api/report` | GET | Revenue/click report data |
| `/api/activity` | GET | Recent activity feed |
| `/api/click/:id` | GET | Look up a specific click |
| `/health` | GET | Health check |
| `/` | GET | Dashboard (password protected) |

## Postback URL Template

Give this to your affiliate network:

```
https://your-domain.com/postback?tid={tid}&payout={payout}&uid={uid}&state={state}&insured={insured}&own_home={own_home}&multi_vehicle={multi_vehicle}&event_type={event_type}
```

## Client-Side Tracker

Include `tracker.js` on your landing pages to automatically capture clicks and forward users to your offer URL with tracking.

## Stack

- **Runtime:** Node.js + Express
- **Database:** PostgreSQL 16
- **Deployment:** Docker Compose
- **Reverse Proxy:** Caddy (recommended) or nginx

## License

MIT

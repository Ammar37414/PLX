# PSX Stock Tracker — Railway Deployment Guide

## Set These Variables in Railway Dashboard → Variables Tab

### REQUIRED (app won't send alerts without these)
RESEND_API_KEY=re_YOUR_KEY_HERE
TARGET_EMAIL=ammarilyas343@gmail.com
SECRET_KEY=CHANGE_THIS_TO_SOMETHING_RANDOM_32_CHARS

### ALERT LOGIC (defaults shown — change if needed)
ALERT_THRESHOLD=4.0
MIN_VALUE_THRESHOLD=20000000
MONITOR_WINDOW=1200
MARKET_CLOSE_HOUR=15
MARKET_CLOSE_MINUTE=30

### OPTIONAL TUNING
REQUEST_DELAY=0.65
PRICE_CHECK_INTERVAL=60

### DATABASE (leave blank — Railway auto-injects if you add PostgreSQL plugin)
# DATABASE_URL=postgresql://...

---

## Deploy Steps

  1. git init
  2. git add .
  3. git commit -m "Initial deploy"
  4. Create private repo on GitHub
  5. git remote add origin https://github.com/YOUR_USERNAME/psx-stock-tracker.git
  6. git push -u origin main
  7. In Railway: New Project → Deploy from GitHub
  8. Add variables above in Railway Variables tab
  9. (Optional) Add PostgreSQL plugin for persistent database

## What to Check in Railway Logs
  ✅ Found 5XX symbols. Starting background cache warmer...
  ✅ WebSocket connected to PSX Terminal.
  ✅ Monitoring 5XX stocks from PSX Terminal

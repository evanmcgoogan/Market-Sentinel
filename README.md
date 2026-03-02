# Market Sentinel

A local-first early warning system that monitors prediction markets (Polymarket and Kalshi) for abnormal movements in serious domains like politics, geopolitics, AI, and markets.

**This is designed to be noisy and early.** You'll get alerts for movements that may not pan out. The goal is information advantage, not certainty.

---

## What This Does

Market Sentinel runs on your laptop and:

1. **Monitors** prediction markets every minute
2. **Filters** to focus only on serious topics (politics, wars, AI, economics - not sports or entertainment)
3. **Detects** unusual movements using simple rules:
   - Sudden price jumps
   - Volume spikes
   - Big moves on thin markets
   - One platform moving before the other
4. **Texts you** when something looks interesting

---

## Quick Start

### 1. Install Python 3.11+

If you don't have Python, download it from [python.org](https://www.python.org/downloads/)

### 2. Download the code

Put the `market-sentinel` folder somewhere on your computer (like your Desktop).

### 3. Install dependencies

Open Terminal and run:

```bash
cd ~/Desktop/market-sentinel
pip install -r requirements.txt
```

### 4. Set up Twilio (for SMS alerts)

1. Create a free account at [twilio.com](https://www.twilio.com)
2. Get your Account SID and Auth Token from the Twilio Console
3. Buy a phone number (starts around $1/month)
4. Copy `config.example.json` to `config.json`
5. Fill in your Twilio credentials

**Or** set environment variables:
```bash
export TWILIO_ACCOUNT_SID="your_account_sid"
export TWILIO_AUTH_TOKEN="your_auth_token"
export TWILIO_FROM_NUMBER="+1234567890"
export TWILIO_TO_NUMBER="+0987654321"
```

### 5. Run it

```bash
cd ~/Desktop/market-sentinel/src
python main.py
```

That's it. Leave it running and you'll get texts when markets move.

---

## How to Stop It

Press `Ctrl+C` in the Terminal window.

Or just close the Terminal window.

---

## How Alerts Work

When Market Sentinel detects unusual movement, you'll get a text like:

```
🚨 EARLY SIGNAL
POLYMARKET — 'China invades Taiwan by 2026'
21% → 34% in 9m
Thin market ($8,500 24h vol)
Sudden move + Volume spike
Kalshi @ 22%
Score: 65/100
```

### What the alert tells you:

- **Platform**: Where the movement happened
- **Market name**: What event it's tracking
- **Price change**: Old probability → new probability
- **Time**: How fast the move happened
- **Context**: Volume, liquidity, reasons the alert fired
- **Cross-platform**: What the other platform shows (if applicable)
- **Score**: 0-100 confidence the movement is meaningful

### Why you might get an alert:

1. **Price Velocity**: Probability changed 5+ points in under 30 minutes
2. **Volume Shock**: Trading volume spiked 3x above normal
3. **Thin Liquidity Jump**: Big price move on a market with little activity
4. **Cross-Market Divergence**: Polymarket and Kalshi disagree by 8+ points
5. **Late Stage**: Signals are weighted higher as resolution approaches

---

## Tuning Sensitivity

Edit `config.json` to adjust thresholds:

```json
{
  "signals": {
    "price_velocity_min_change": 5.0,      // Lower = more sensitive
    "volume_shock_multiplier": 3.0,         // Lower = more sensitive
    "alert_threshold": 40.0                 // Lower = more alerts
  }
}
```

**More alerts (noisier):**
- Lower `alert_threshold` to 25-30
- Lower `price_velocity_min_change` to 3

**Fewer alerts (quieter):**
- Raise `alert_threshold` to 60-70
- Raise `price_velocity_min_change` to 8-10

---

## What Markets It Watches

**Included:**
- Elections and politics (US, international)
- Geopolitics and international relations
- Wars, conflicts, military
- AI companies and policy
- Economic indicators, Fed, markets

**Excluded:**
- Sports (NFL, NBA, etc.)
- Entertainment (movies, music, celebrities)
- Pop culture and viral stuff
- Weather (unless geopolitically relevant)

You can customize the keyword lists in `config.json` under `filters`.

---

## Files Explained

```
market-sentinel/
├── README.md           # This file
├── requirements.txt    # Python dependencies
├── config.example.json # Example configuration
├── config.json         # Your configuration (create this)
└── src/
    ├── main.py         # Run this to start
    ├── config.py       # Settings management
    ├── database.py     # Stores market history
    ├── models.py       # Data structures
    ├── polymarket.py   # Polymarket API client
    ├── kalshi.py       # Kalshi API client
    ├── filters.py      # Market filtering rules
    ├── signals.py      # Detection heuristics
    └── alerts.py       # SMS sending
```

---

## Troubleshooting

### "No markets to monitor"
- Check your internet connection
- The APIs may be temporarily down
- Wait a minute and try again

### Not getting texts
- Check Twilio credentials in `config.json`
- Verify your Twilio account has credit
- Check the phone numbers are correct (include country code)
- Look at Terminal output - alerts print there even without Twilio

### Too many/few alerts
- Adjust `alert_threshold` in config
- See "Tuning Sensitivity" above

### Errors in Terminal
- Most errors are logged but don't crash the program
- It will keep trying on the next poll cycle
- If it crashes completely, just restart with `python main.py`

---

## Data Storage

Market Sentinel stores data locally in `market_sentinel.db` (SQLite):
- Market price/volume history (last 7 days)
- Alert history (for cooldowns)

This is automatically cleaned up. No data leaves your computer except SMS alerts.

---

## Limitations

- **Not financial advice**: This is an information tool, not trading signals
- **False positives**: You will get alerts that turn out to be noise
- **API dependent**: If Polymarket/Kalshi change their APIs, this may break
- **No guarantees**: This is v0 software optimized for speed, not reliability

---

## Making Changes

The code is designed to be hackable. Some ideas:

- Add new keywords in `config.py` → `MarketFilterConfig`
- Adjust signal weights in `signals.py` → `SignalDetector`
- Add new signal types by following the pattern in `_detect_*` methods
- Change alert formatting in `models.py` → `Alert.format_sms()`

---

## License

Do whatever you want with this. No warranty.

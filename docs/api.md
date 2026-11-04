# Cheapest hours API

A small HTTP API for use case 1 ([use_cases.md](plans/use_cases.md), section 5.1). A car charger, a home-automation system or any script can ask it when electricity is cheapest, instead of a person reading the dashboard. It answers in JSON from the same Gold tables as the dashboard's *Cheapest hours tomorrow* page. It is **open**: no key, read-only.

- **Code:** [api/main.py](../api/main.py) (the HTTP endpoints) and [api/gold.py](../api/gold.py) (reading Gold and building every answer).
- **Data:** the Gold tables `consumer_prices_hourly` and `cheapest_hours` ([gold_layer.md](plans/gold_layer.md), section 3).
- **Documentation for people:** the dashboard's **API for apps** page. It lists the endpoints and has a try-it panel that shows the request and the exact answer, because it calls the same functions as the API. The API itself has no generated docs page: `GET /` returns a short JSON list of its endpoints.

## 1. Start it

**On your laptop, with sample data** (the numbers are invented; `/health` says `"is_sample": true`):

```bash
pip install -r requirements.txt -r requirements-dashboard.txt
python scripts/make_sample_gold.py                       # sample Gold, with tomorrow's prices
uvicorn api.main:app --port 8000                         # http://localhost:8000/ lists the endpoints
```

**On the VM**, as a service next to the dashboard. It uses the dashboard's clone and Python environment:

```bash
cd ~/dat535-dashboard && git pull
source ~/dashboard-env/bin/activate && pip install -r requirements-dashboard.txt
sudo cp deploy/dat535-api.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now dat535-api
systemctl status dat535-api --no-pager                   # should say "active (running)"
```

It serves prod Gold (`DAT535_ENV=prod` in the unit). Logs: `journalctl -u dat535-api -f`. After a code update: `git pull`, then `sudo systemctl restart dat535-api`. New Gold data needs no restart: the API reads the tables again when a new Gold run has been published.

## 2. Reach it

The API listens only on the VM itself (`127.0.0.1:8000`), like the dashboard. From your laptop, open an SSH tunnel:

```bash
ssh -i ~/.ssh/id_ed25519 -N -L 8000:localhost:8000 ubuntu@<floating-ip>
```

Then use `http://localhost:8000` on your laptop. To open the dashboard and the API at the same time, forward both ports in one tunnel: `-L 8502:localhost:8501 -L 8000:localhost:8000`.

**A real device on another network** can't use a tunnel. For that, the API has to listen on the VM's public address:
1. Change `--host 127.0.0.1` to `--host 0.0.0.0` in `/etc/systemd/system/dat535-api.service`, then `sudo systemctl daemon-reload && sudo systemctl restart dat535-api`.
2. In OpenStack, add a security group rule that allows TCP port 8000. Limit it to the addresses that need it if you can (a device's IP, or the university network).
3. The device then uses `http://<floating-ip>:8000`.

The API is open and read-only: anyone who can reach the port can read prices, and nothing else. It has no HTTPS. That is fine for public price data, but put a reverse proxy with a certificate (e.g. Caddy or nginx) in front of it before anyone relies on it outside the course.

## 3. Endpoints

All paths start with `/api/v1`. `{zone}` is `NO1`–`NO5`. `day` is `today`, `tomorrow` (Norwegian dates, the default is `tomorrow`) or a date such as `2026-11-04`.

| Endpoint | What it answers |
|---|---|
| `GET /api/v1/cheapest-hours/{zone}/block?hours=6&day=tomorrow` | The cheapest block of 1, 3 or 6 hours (default 3): when to start and stop. **The one a charger needs.** |
| `GET /api/v1/cheapest-hours/{zone}?day=tomorrow` | Every hour of the day with its prices, and the cheapest 1-, 3- and 6-hour blocks |
| `GET /api/v1/health` | When Gold was built, how far the prices reach, and which zones already have tomorrow |
| `GET /api/v1/zones` | The zone codes and their names |

### The cheapest block

```bash
curl "http://localhost:8000/api/v1/cheapest-hours/NO1/block?hours=6&day=tomorrow"
```

```json
{
  "zone": "NO1",
  "date": "2026-11-04",
  "timezone": "Europe/Oslo",
  "block_hours": 6,
  "start_utc": "2026-11-04T00:00:00Z",
  "end_utc": "2026-11-04T06:00:00Z",
  "start_time": "01:00",
  "end_time": "07:00",
  "avg_price_nok_kwh": 0.68319,
  "evening_avg_price_nok_kwh": 0.7912,
  "saving_vs_17_nok_kwh": 0.10802
}
```

Read it as: charge from 01:00 to 07:00 Norwegian time. That costs 68.3 øre/kWh on average, against 79.1 øre if you start at 17:00. A 20 kWh charge saves 20 × 0.108 = 2.16 kr.

### The whole day

```bash
curl "http://localhost:8000/api/v1/cheapest-hours/NO1?day=tomorrow"
```

```json
{
  "zone": "NO1",
  "date": "2026-11-04",
  "timezone": "Europe/Oslo",
  "units": "NOK/kWh",
  "hours": [
    {
      "start_utc": "2026-11-03T23:00:00Z",
      "end_utc": "2026-11-04T00:00:00Z",
      "local_time": "00:00",
      "spot_nok_kwh": 0.57954,
      "price_with_vat_nok_kwh": 0.72443,
      "price_with_stromstotte_nok_kwh": 0.72443,
      "norgespris_nok_kwh": 0.5
    }
  ],
  "cheapest": [
    {
      "block_hours": 1,
      "start_utc": "2026-11-04T01:00:00Z",
      "end_utc": "2026-11-04T02:00:00Z",
      "start_time": "02:00",
      "end_time": "03:00",
      "avg_price_nok_kwh": 0.63911,
      "evening_avg_price_nok_kwh": 0.89349,
      "saving_vs_17_nok_kwh": 0.25438
    }
  ]
}
```

Shortened here: `hours` has every hour of the day (23, 24 or 25 with daylight saving), and `cheapest` has the 1-, 3- and 6-hour blocks.

### Health

```bash
curl "http://localhost:8000/api/v1/health"
```

```json
{
  "status": "ok",
  "environment": "prod",
  "gold_built_at_utc": "2026-11-03T12:41:07Z",
  "prices_through": "2026-11-04",
  "data_through": "2026-11-02",
  "tomorrow": {"date": "2026-11-04", "published_zones": ["NO1", "NO2", "NO3", "NO4", "NO5"]},
  "is_sample": false
}
```

### Fields

| Field | Meaning |
|---|---|
| `start_utc`, `end_utc` | The hour or block in UTC (ISO 8601). Use these to schedule a device: they are unambiguous, also on the daylight saving days. |
| `local_time`, `start_time`, `end_time` | The same on the Norwegian clock, for people. On the autumn daylight saving day, `02:00` appears twice. |
| `spot_nok_kwh` | The day-ahead spot price, without VAT |
| `price_with_vat_nok_kwh` | Spot plus VAT (25 %; NO4: no VAT): what a household with no support pays |
| `price_with_stromstotte_nok_kwh` | After strømstønad: what a household without Norgespris pays. **The blocks are ranked by this price.** Empty (`null`) in a year without a known threshold. |
| `norgespris_nok_kwh` | The fixed Norgespris price with VAT; `null` outside the scheme's dates. With Norgespris every hour costs the same, so there is nothing to gain from moving usage. |
| `block_hours` | The length of the block: 1, 3 or 6 hours. Blocks stay within one Norwegian day. |
| `avg_price_nok_kwh` | The block's average price after strømstønad |
| `evening_avg_price_nok_kwh` | The same length of block starting at 17:00, when people come home |
| `saving_vs_17_nok_kwh` | The evening block minus the cheapest block, per kWh. Multiply by the kWh you move. |

All prices are per kWh of energy. The grid tariff and the supplier's fees come on top.

## 4. Status codes

| Code | When | What to do |
|---|---|---|
| `200` | The day is there | Use it |
| `503` + `Retry-After: 1800` | `day=tomorrow` before the prices are published (they usually arrive around 13:00 Norwegian time and are here by 14:30). The body says `"published": false`, and says "late" after 14:30. Also when Gold isn't built yet, or is being swapped (`Retry-After: 60`). | Wait the number of seconds in `Retry-After`, then ask again |
| `404` | Any other day Gold doesn't have: before 2024, or further ahead than tomorrow | Don't retry |
| `422` | A wrong zone, `day` or `hours`. The body's `detail` says what's allowed. | Fix the request |

## 5. A smart charger, step by step

A charger that should charge 6 hours every night, in the cheapest hours of tomorrow:

1. After 13:00, ask `GET /api/v1/cheapest-hours/NO1/block?hours=6&day=tomorrow`.
2. On `503`, wait `Retry-After` seconds and ask again.
3. On `200`, schedule charging from `start_utc` to `end_utc`.

The same in Python, with only the `requests` package:

```python
import time
import requests

URL = "http://localhost:8000/api/v1/cheapest-hours/NO1/block"

while True:
    response = requests.get(URL, params={"hours": 6, "day": "tomorrow"}, timeout=10)
    if response.status_code == 503:  # not published yet
        time.sleep(int(response.headers.get("Retry-After", 1800)))
        continue
    response.raise_for_status()
    block = response.json()
    print(f"Charge {block['start_time']}-{block['end_time']} on {block['date']}, "
          f"saving {20 * block['saving_vs_17_nok_kwh']:.2f} kr on 20 kWh")
    break
```

**Home Assistant** can read the same endpoint as two sensors with its RESTful integration (`configuration.yaml`). The sensors refresh every 30 minutes and are unavailable while the API answers 503:

```yaml
rest:
  - resource: "http://<host>:8000/api/v1/cheapest-hours/NO1/block?hours=6&day=tomorrow"
    scan_interval: 1800
    sensor:
      - name: "Cheapest 6 hours start"
        value_template: "{{ value_json.start_utc }}"
        device_class: timestamp
      - name: "Cheapest 6 hours end"
        value_template: "{{ value_json.end_utc }}"
        device_class: timestamp
```

An automation can then turn the charger on at the first time and off at the second.

## 6. When the data changes

| Norwegian time | What happens | What the API answers for `day=tomorrow` |
|---|---|---|
| 00:00 | A new day begins: yesterday's "tomorrow" is now `today` | `503` until the new tomorrow arrives |
| ~03:20 | The nightly run brings in yesterday's weather; prices don't change | `503` |
| ~13:00 | Tomorrow's prices are published | `503` until the afternoon run has rebuilt Gold |
| By 14:30 | The day-ahead run has rebuilt Gold ([gold_layer.md](plans/gold_layer.md), G10) | `200` |

GitHub sometimes starts scheduled runs late. If tomorrow isn't there by 14:30, the `503` says "late", and `/health` shows which zones are missing.

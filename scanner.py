# Pump.fun Runner Scanner
# Finds tokens on PumpSwap with over 1M USD volume, launched in last 24h.
# Uses Dexscreener API only. Posts to Telegram. Responds to /dailyscan.

import asyncio
import csv
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

# ===========================================================================
# CONFIG
# ===========================================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip().encode('ascii', 'ignore').decode()
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip().encode('ascii', 'ignore').decode()

MIN_VOLUME_USD      = float(os.getenv("MIN_VOLUME_USD", "1000000"))
MAX_TOKEN_AGE_HOURS = int(os.getenv("MAX_TOKEN_AGE_HOURS", "24"))
MIN_LIQUIDITY_USD   = float(os.getenv("MIN_LIQUIDITY_USD", "5000"))
MIN_TRANSACTIONS    = int(os.getenv("MIN_TRANSACTIONS", "50"))
TOP_N               = int(os.getenv("TOP_N", "20"))
SCHEDULE_HOUR       = int(os.getenv("SCHEDULE_HOUR", "0"))
SCHEDULE_MINUTE     = int(os.getenv("SCHEDULE_MINUTE", "5"))

DEXSCREENER = "https://api.dexscreener.com"
DB_PATH     = "scanner.db"
OUTPUT_DIR  = Path("output")

# PumpSwap DEX IDs on Dexscreener
PUMPSWAP_DEX_IDS = {"pumpfun", "pumpswap", "pump-fun", "pump_fun"}

# ===========================================================================
# DATABASE
# ===========================================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT,
            rank INTEGER,
            mint TEXT,
            name TEXT,
            symbol TEXT,
            url TEXT,
            website TEXT,
            twitter TEXT,
            age_hours REAL,
            volume REAL,
            liquidity REAL,
            market_cap REAL,
            price_change REAL,
            buys INTEGER,
            sells INTEGER,
            score INTEGER,
            label TEXT,
            flags TEXT,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT UNIQUE,
            discovered INTEGER,
            passed INTEGER,
            winner_name TEXT,
            winner_symbol TEXT,
            winner_volume REAL,
            winner_score INTEGER
        )
    """)
    conn.commit()
    conn.close()


def save_results(scan_date, tokens):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM results WHERE scan_date=?", (scan_date,))
    for rank, t in enumerate(tokens, 1):
        conn.execute("""
            INSERT INTO results
            (scan_date,rank,mint,name,symbol,url,website,twitter,age_hours,volume,liquidity,
             market_cap,price_change,buys,sells,score,label,flags,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            scan_date, rank,
            t["mint"], t["name"], t["symbol"], t["url"],
            t.get("website",""), t.get("twitter",""),
            t["age_hours"], t["volume"], t["liquidity"],
            t.get("market_cap"), t["price_change"],
            t["buys"], t["sells"],
            t["score"], t["label"],
            json.dumps(t["flags"]),
            datetime.now(timezone.utc).isoformat()
        ))
    if tokens:
        w = tokens[0]
        conn.execute("""
            INSERT OR REPLACE INTO daily
            (scan_date,discovered,passed,winner_name,winner_symbol,winner_volume,winner_score)
            VALUES (?,?,?,?,?,?,?)
        """, (scan_date, t.get("discovered", 0), len(tokens),
              w["name"], w["symbol"], w["volume"], w["score"]))
    conn.commit()
    conn.close()


# ===========================================================================
# STEP 1 — DISCOVER via Dexscreener search
# Cast a wide net: multiple search queries targeting PumpSwap tokens.
# Each query returns up to 30 pairs. We deduplicate by mint address.
# ===========================================================================

SEARCH_QUERIES = [
    "pumpswap", "pump.fun", "pumpfun",
    "solana meme coin", "solana dog", "solana cat",
    "solana ai", "solana trump", "solana pepe",
    "solana based", "solana inu", "solana moon",
    "solana chad", "solana elon", "solana grok",
]

async def discover_tokens(client):
    cutoff_ms = int(
        (datetime.now(timezone.utc) - timedelta(hours=MAX_TOKEN_AGE_HOURS)).timestamp() * 1000
    )
    tokens = {}

    def ingest_pair(pair):
        if not isinstance(pair, dict):
            return
        if pair.get("chainId") != "solana":
            return
        dex_id = (pair.get("dexId") or "").lower()
        if dex_id not in PUMPSWAP_DEX_IDS:
            return
        created_ms = pair.get("pairCreatedAt") or 0
        if created_ms < cutoff_ms:
            return
        base = pair.get("baseToken") or {}
        mint = (base.get("address") or "").strip()
        if not mint:
            return
        vol = float((pair.get("volume") or {}).get("h24") or 0)
        liq = float((pair.get("liquidity") or {}).get("usd") or 0)
        txns_h24 = (pair.get("txns") or {}).get("h24") or {}
        buys = int(txns_h24.get("buys") or 0)
        sells = int(txns_h24.get("sells") or 0)
        info = pair.get("info") or {}
        websites = info.get("websites") or []
        socials = info.get("socials") or []
        website_url = websites[0].get("url", "") if websites else ""
        twitter_url = next((s.get("url","") for s in socials if s.get("type") == "twitter"), "")
        if mint not in tokens or vol > tokens[mint].get("volume", 0):
            tokens[mint] = {
                "mint": mint,
                "name": base.get("name") or "Unknown",
                "symbol": base.get("symbol") or "?",
                "created_ms": created_ms,
                "volume": vol,
                "liquidity": liq,
                "buys": buys,
                "sells": sells,
                "url": pair.get("url", ""),
                "dex": dex_id,
                "market_cap": pair.get("marketCap"),
                "price_change": float((pair.get("priceChange") or {}).get("h24") or 0),
                "pair_created_ms": created_ms,
                "website": website_url,
                "twitter": twitter_url,
            }

    for query in SEARCH_QUERIES:
        try:
            r = await client.get(
                f"{DEXSCREENER}/latest/dex/search",
                params={"q": query},
                timeout=15,
            )
            if r.status_code == 200:
                pairs = r.json().get("pairs") or []
                before = len(tokens)
                for pair in pairs:
                    ingest_pair(pair)
                new = len(tokens) - before
                if new > 0:
                    logger.info(f"Query '{query}': +{new} new tokens (total: {len(tokens)})")
        except Exception as e:
            logger.warning(f"Search error '{query}': {e}")
        await asyncio.sleep(0.3)

    result = list(tokens.values())
    logger.info(f"Discovery complete: {len(result)} PumpSwap tokens in last {MAX_TOKEN_AGE_HOURS}h")
    return result


# ===========================================================================
# STEP 2 — ENRICH (data already in discovery, just pass through)
# ===========================================================================

async def enrich_tokens(client, tokens):
    enriched = {}
    for t in tokens:
        if t.get("volume", 0) >= 0:
            enriched[t["mint"]] = {
                "url": t.get("url", ""),
                "dex": t.get("dex", ""),
                "volume": t.get("volume", 0),
                "liquidity": t.get("liquidity", 0),
                "market_cap": t.get("market_cap"),
                "price_change": t.get("price_change", 0),
                "buys": t.get("buys", 0),
                "sells": t.get("sells", 0),
                "pair_created_ms": t.get("pair_created_ms"),
                "website": t.get("website", ""),
                "twitter": t.get("twitter", ""),
            }
    return enriched


# ===========================================================================
# STEP 3 — FILTER
# ===========================================================================

def filter_tokens(tokens, enriched):
    passed = []
    stats = {"no_dex": 0, "low_vol": 0, "low_liq": 0, "few_txns": 0, "honeypot": 0, "wash": 0}

    for t in tokens:
        dex = enriched.get(t["mint"])
        if not dex:
            stats["no_dex"] += 1
            continue
        if dex["volume"] < MIN_VOLUME_USD:
            stats["low_vol"] += 1
            continue
        if dex["liquidity"] < MIN_LIQUIDITY_USD:
            stats["low_liq"] += 1
            continue
        if dex["buys"] + dex["sells"] < MIN_TRANSACTIONS:
            stats["few_txns"] += 1
            continue
        if dex["buys"] >= 20 and dex["sells"] == 0:
            stats["honeypot"] += 1
            logger.debug(f"Honeypot filtered: {t['symbol']}")
            continue
        if dex["liquidity"] > 0 and dex["volume"] / dex["liquidity"] > 1000:
            stats["wash"] += 1
            logger.debug(f"Wash trade filtered: {t['symbol']}")
            continue
        passed.append((t, dex))

    logger.info(
        f"Filters: no_dex={stats['no_dex']} low_vol={stats['low_vol']} "
        f"low_liq={stats['low_liq']} few_txns={stats['few_txns']} "
        f"honeypot={stats['honeypot']} wash={stats['wash']} "
        f"PASSED={len(passed)}"
    )
    return passed


# ===========================================================================
# STEP 4 — SCORE (manipulation risk 0-100)
# ===========================================================================

def score_token(dex):
    score = 0
    flags = []
    vol = dex["volume"]
    liq = max(dex["liquidity"], 1)
    buys = dex["buys"]
    sells = dex["sells"]
    total = buys + sells
    mcap = dex.get("market_cap") or 0

    ratio = vol / liq
    if ratio > 500:
        score += 30; flags.append(f"Extreme vol/liq ratio ({ratio:.0f}x)")
    elif ratio > 200:
        score += 20; flags.append(f"Very high vol/liq ratio ({ratio:.0f}x)")
    elif ratio > 100:
        score += 10; flags.append(f"Elevated vol/liq ratio ({ratio:.0f}x)")
    elif ratio > 50:
        score += 4

    if total > 0 and sells > 0:
        buy_ratio = buys / total
        if buy_ratio > 0.95:
            score += 20; flags.append(f"Extreme buy ratio ({buy_ratio:.0%})")
        elif buy_ratio > 0.90:
            score += 12; flags.append(f"High buy ratio ({buy_ratio:.0%})")
        elif buy_ratio > 0.85:
            score += 5

    if total > 0:
        avg = vol / total
        if avg > 100000:
            score += 20; flags.append(f"Huge avg txn (${avg:,.0f})")
        elif avg > 50000:
            score += 14; flags.append(f"Large avg txn (${avg:,.0f})")
        elif avg > 20000:
            score += 8; flags.append(f"Elevated avg txn (${avg:,.0f})")

    if mcap > 0:
        lm = liq / mcap
        if lm < 0.003:
            score += 15; flags.append(f"Dangerous liq/mcap ({lm:.2%})")
        elif lm < 0.01:
            score += 8; flags.append(f"Low liq/mcap ({lm:.2%})")

    score = min(score, 100)
    if score <= 20:   label = "Organic"
    elif score <= 40: label = "Likely Organic"
    elif score <= 65: label = "Suspicious"
    else:             label = "Likely Manipulated"
    return score, label, flags


def build_result(token, dex, discovered_count):
    now = datetime.now(timezone.utc)
    launch_ms = dex.get("pair_created_ms") or token.get("created_ms")
    age_hours = 0.0
    if launch_ms:
        launch_dt = datetime.fromtimestamp(launch_ms / 1000, tz=timezone.utc)
        age_hours = round((now - launch_dt).total_seconds() / 3600, 2)
    score, label, flags = score_token(dex)
    return {
        "mint": token["mint"],
        "name": token["name"],
        "symbol": token["symbol"],
        "url": dex["url"],
        "website": dex.get("website", ""),
        "twitter": dex.get("twitter", ""),
        "age_hours": age_hours,
        "volume": dex["volume"],
        "liquidity": dex["liquidity"],
        "market_cap": dex.get("market_cap"),
        "price_change": dex["price_change"],
        "buys": dex["buys"],
        "sells": dex["sells"],
        "score": score,
        "label": label,
        "flags": flags,
        "discovered": discovered_count,
    }


# ===========================================================================
# STEP 5 — TELEGRAM
# ===========================================================================

def fmt(v):
    if v >= 1000000: return f"${v/1000000:.1f}M"
    if v >= 1000:    return f"${v/1000:.0f}K"
    return f"${v:.0f}"

def risk_emoji(label):
    return {"Organic":"✅","Likely Organic":"🟢","Suspicious":"🟡","Likely Manipulated":"🔴"}.get(label,"❓")

async def send_telegram(client, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured")
        return
    try:
        r = await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        if r.json().get("ok"):
            logger.info("Telegram sent")
        else:
            logger.error(f"Telegram error: {r.json().get('description')}")
    except Exception as e:
        logger.error(f"Telegram failed: {e}")

def build_token_entry(t, rank):
    emoji = risk_emoji(t["label"])
    vol = fmt(t["volume"])
    mcap = fmt(t["market_cap"]) if t.get("market_cap") else "N/A"
    pc = f"{t['price_change']:+.0f}%"
    age = f"{t['age_hours']:.1f}h"
    rank_icon = {1:"🥇",2:"🥈",3:"🥉"}.get(rank, f"{rank}.")
    links = [f"<a href=\"{t['url']}\">Dexscreener</a>"]
    if t.get("twitter"):
        links.append(f"<a href=\"{t['twitter']}\">X</a>")
    if t.get("website"):
        links.append(f"<a href=\"{t['website']}\">Website</a>")
    return (
        f"{rank_icon} <b>{t['name']}</b> <code>${t['symbol']}</code>\n"
        f"Vol: <b>{vol}</b> | MCap: {mcap} | {pc} | {age} old\n"
        f"Risk: {emoji} {t['score']}/100\n"
        f"CA: <code>{t['mint']}</code>\n"
        f"{' | '.join(links)}"
    )

async def send_results(client, tokens, scan_date):
    if not tokens:
        await send_telegram(client,
            f"📭 <b>Potential Farms — {scan_date}</b>\n\n"
            f"No PumpSwap tokens found with $1M+ volume in the last 24h.")
        return
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    await send_telegram(client,
        f"🔥 <b>Potential Farms — {scan_date}</b>\n"
        f"<i>{len(tokens)} tokens | PumpSwap | $1M+ vol | {now_str}</i>")
    await asyncio.sleep(0.3)
    for i, t in enumerate(tokens, 1):
        await send_telegram(client, build_token_entry(t, i))
        await asyncio.sleep(0.3)


# ===========================================================================
# STEP 6 — EXPORT
# ===========================================================================

def export_files(tokens, scan_date):
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_DIR / f"{scan_date}_runners.json", "w") as f:
        json.dump({"scan_date": scan_date, "results": tokens}, f, indent=2, default=str)
    if tokens:
        fields = ["rank","name","symbol","mint","url","website","twitter","age_hours",
                  "volume","liquidity","market_cap","price_change","buys","sells","score","label"]
        with open(OUTPUT_DIR / f"{scan_date}_runners.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for rank, t in enumerate(tokens, 1):
                row = dict(t); row["rank"] = rank
                w.writerow({k: row.get(k,"") for k in fields})
    logger.info(f"Exported {len(tokens)} results")


# ===========================================================================
# MAIN SCAN PIPELINE
# ===========================================================================

async def run_scan(client=None):
    scan_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("=" * 50)
    logger.info(f"Starting scan — {scan_date}")
    logger.info("=" * 50)
    init_db()

    async def _scan(c):
        raw = await discover_tokens(c)
        if not raw:
            logger.warning("No tokens discovered")
            await send_telegram(c, f"No tokens found by Dexscreener scan ({scan_date})")
            return []
        enriched = await enrich_tokens(c, raw)
        passed = filter_tokens(raw, enriched)
        logger.info(f"Pipeline: {len(raw)} discovered -> {len(passed)} passed filters")
        if not passed:
            await send_results(c, [], scan_date)
            return []
        results = [build_result(t, d, len(raw)) for t, d in passed]
        results.sort(key=lambda x: x["volume"], reverse=True)
        top = results[:TOP_N]
        logger.info(f"Winner: {top[0]['symbol']} vol={fmt(top[0]['volume'])} score={top[0]['score']}/100")
        export_files(top, scan_date)
        save_results(scan_date, top)
        await send_results(c, top, scan_date)
        return top

    if client:
        return await _scan(client)
    else:
        async with httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0 (compatible; scanner/1.0)", "Accept": "application/json"},
            follow_redirects=True,
        ) as c:
            return await _scan(c)


def run():
    return asyncio.run(run_scan())


# ===========================================================================
# TELEGRAM BOT — /dailyscan command
# ===========================================================================

async def poll_telegram_commands():
    offset = 0
    scan_in_progress = False
    logger.info("Telegram command listener active — /dailyscan ready")

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; scanner/1.0)"},
        follow_redirects=True,
    ) as client:
        while True:
            try:
                r = await client.get(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                    params={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
                    timeout=40,
                )
                data = r.json()
                if not data.get("ok"):
                    await asyncio.sleep(5)
                    continue

                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = (msg.get("text") or "").strip()
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    user = msg.get("from", {}).get("first_name", "Someone")

                    if text.startswith("/dailyscan") and chat_id == TELEGRAM_CHAT_ID:
                        if scan_in_progress:
                            await send_telegram(client, "A scan is already running — please wait!")
                            continue
                        logger.info(f"/dailyscan by {user}")
                        scan_in_progress = True
                        await send_telegram(client,
                            f"🔍 <b>Scan started by {user}...</b>\n"
                            f"<i>Fetching for potential farms, this takes ~1 minute.</i>")
                        try:
                            await run_scan(client=client)
                        except Exception as e:
                            logger.error(f"/dailyscan error: {e}")
                            await send_telegram(client, f"Scan failed: {e}")
                        finally:
                            scan_in_progress = False

            except Exception as e:
                logger.warning(f"Telegram poll error: {e}")
                await asyncio.sleep(10)


# ===========================================================================
# MAIN
# ===========================================================================

async def main_async():
    init_db()
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(run, CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
                      id="daily_scan", replace_existing=True, misfire_grace_time=300, coalesce=True)
    t = threading.Thread(target=scheduler.start, daemon=True)
    t.start()
    logger.info(f"Scheduler started — daily at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC")
    await poll_telegram_commands()


if __name__ == "__main__":
    import sys
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add("scanner.log", level="DEBUG", rotation="1 day", retention="14 days")
    if len(sys.argv) > 1 and sys.argv[1] == "scan":
        run()
    else:
        asyncio.run(main_async())

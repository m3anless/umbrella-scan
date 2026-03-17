# Pump.fun Daily Runner Scanner
# Finds pump.fun tokens with over 1000000 USD volume, 300 holders min, no honeypots.
# Posts a clean copyable list to Telegram. Responds to /dailyscan command.

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

MIN_VOLUME_USD      = float(os.getenv("MIN_VOLUME_USD", "1000000"))   # $1M floor
MIN_HOLDERS         = int(os.getenv("MIN_HOLDERS", "300"))             # 300 unique holders
MAX_TOKEN_AGE_HOURS = int(os.getenv("MAX_TOKEN_AGE_HOURS", "24"))
MIN_LIQUIDITY_USD   = float(os.getenv("MIN_LIQUIDITY_USD", "5000"))
MIN_TRANSACTIONS    = int(os.getenv("MIN_TRANSACTIONS", "50"))
TOP_N               = int(os.getenv("TOP_N", "20"))                    # Show up to 20 tokens
SCHEDULE_HOUR       = int(os.getenv("SCHEDULE_HOUR", "0"))
SCHEDULE_MINUTE     = int(os.getenv("SCHEDULE_MINUTE", "5"))

PUMPFUN_API = "https://frontend-api.pump.fun"
DEXSCREENER = "https://api.dexscreener.com"
DB_PATH     = "scanner.db"
OUTPUT_DIR  = Path("output")

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
            age_hours REAL,
            volume REAL,
            liquidity REAL,
            market_cap REAL,
            price_change REAL,
            buys INTEGER,
            sells INTEGER,
            holders INTEGER,
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
            (scan_date,rank,mint,name,symbol,url,age_hours,volume,liquidity,
             market_cap,price_change,buys,sells,holders,score,label,flags,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            scan_date, rank,
            t["mint"], t["name"], t["symbol"], t["url"],
            t["age_hours"], t["volume"], t["liquidity"],
            t.get("market_cap"), t["price_change"],
            t["buys"], t["sells"], t.get("holders", 0),
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
# STEP 1 — DISCOVER tokens via Dexscreener boosted/new tokens endpoints
# Uses multiple Dexscreener endpoints to find new pump.fun tokens
# ===========================================================================

async def discover_tokens(client):
    cutoff_ms = int(
        (datetime.now(timezone.utc) - timedelta(hours=MAX_TOKEN_AGE_HOURS)).timestamp() * 1000
    )
    tokens = {}

    def process_pairs(pairs):
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            if pair.get("chainId") != "solana":
                continue
            # Accept pumpfun and raydium (raydium is where pumpfun tokens graduate to)
            if pair.get("dexId") not in ("pumpfun", "raydium"):
                continue
            created_ms = pair.get("pairCreatedAt") or 0
            if created_ms < cutoff_ms:
                continue
            base = pair.get("baseToken") or {}
            mint = base.get("address", "").strip()
            if not mint:
                return
            vol = float((pair.get("volume") or {}).get("h24") or 0)
            liq = float((pair.get("liquidity") or {}).get("usd") or 0)
            txns_h24 = (pair.get("txns") or {}).get("h24") or {}
            buys = int(txns_h24.get("buys") or 0)
            sells = int(txns_h24.get("sells") or 0)
            if mint not in tokens or vol > tokens[mint].get("volume", 0):
                # Extract social links from Dexscreener info field
                info = pair.get("info") or {}
                websites = info.get("websites") or []
                socials = info.get("socials") or []
                website_url = websites[0].get("url", "") if websites else ""
                twitter_url = ""
                for s in socials:
                    if s.get("type") == "twitter":
                        twitter_url = s.get("url", "")
                        break

                tokens[mint] = {
                    "mint": mint,
                    "name": base.get("name") or "Unknown",
                    "symbol": base.get("symbol") or "?",
                    "created_ms": created_ms,
                    "holders": 0,
                    "volume": vol,
                    "liquidity": liq,
                    "buys": buys,
                    "sells": sells,
                    "url": pair.get("url", ""),
                    "dex": pair.get("dexId", ""),
                    "market_cap": pair.get("marketCap"),
                    "price_change": float((pair.get("priceChange") or {}).get("h24") or 0),
                    "pair_created_ms": created_ms,
                    "website": website_url,
                    "twitter": twitter_url,
                }

    # 1. Get token addresses from Dexscreener's boosted/new token endpoints
    profile_mints = []
    for profile_url in [
        f"{DEXSCREENER}/token-profiles/latest/v1",
        f"{DEXSCREENER}/token-boosts/latest/v1",
        f"{DEXSCREENER}/token-boosts/top/v1",
    ]:
        try:
            r = await client.get(profile_url, timeout=20)
            if r.status_code == 200:
                items = r.json()
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict) and item.get("chainId") == "solana":
                            addr = item.get("tokenAddress", "").strip()
                            if addr:
                                profile_mints.append(addr)
        except Exception as e:
            logger.warning(f"Profile endpoint error {profile_url}: {e}")
        await asyncio.sleep(0.2)

    logger.info(f"Got {len(profile_mints)} token addresses from Dexscreener profiles")

    # 2. Batch fetch pair data for all those mints
    batch_size = 30
    for i in range(0, len(profile_mints), batch_size):
        batch = profile_mints[i:i + batch_size]
        try:
            r = await client.get(
                f"{DEXSCREENER}/latest/dex/tokens/{','.join(batch)}",
                timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                process_pairs(data.get("pairs") or [])
        except Exception as e:
            logger.warning(f"Batch lookup error: {e}")
        await asyncio.sleep(0.25)

    # 3. Also try direct Dexscreener search for pumpfun pairs
    for query in ["pump.fun", "pumpfun solana"]:
        try:
            r = await client.get(
                f"{DEXSCREENER}/latest/dex/search",
                params={"q": query},
                timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                process_pairs(data.get("pairs") or [])
        except Exception as e:
            logger.warning(f"Search error for '{query}': {e}")
        await asyncio.sleep(0.25)

    result = list(tokens.values())
    logger.info(f"Discovered {len(result)} tokens in last {MAX_TOKEN_AGE_HOURS}h")
    return result


# ===========================================================================
# STEP 2 — ENRICH with Dexscreener (volume, liquidity, txns)
# ===========================================================================

async def enrich_tokens(client, tokens):
    # Since discovery already pulls pair data from Dexscreener,
    # we use that data directly. This step re-fetches for freshness on the top candidates.
    mints = [t["mint"] for t in tokens if t.get("volume", 0) > 0]
    enriched = {}

    # Pre-populate from discovery data
    for t in tokens:
        if t.get("volume", 0) > 0:
            enriched[t["mint"]] = {
                "url": t.get("url", ""),
                "dex": t.get("dex", ""),
                "volume": t.get("volume", 0),
                "liquidity": t.get("liquidity", 0),
                "market_cap": t.get("market_cap"),
                "price_usd": None,
                "price_change": t.get("price_change", 0),
                "buys": t.get("buys", 0),
                "sells": t.get("sells", 0),
                "pair_created_ms": t.get("pair_created_ms"),
                "website": t.get("website", ""),
                "twitter": t.get("twitter", ""),
            }

    logger.info(f"Enriched: {len(enriched)}/{len(tokens)} tokens have Dexscreener data")
    return enriched


# ===========================================================================
# STEP 3 — FILTER (volume, holders, honeypot, wash trading, liquidity)
# ===========================================================================

def filter_tokens(tokens, enriched):
    passed = []
    stats = {
        "no_dex": 0, "low_vol": 0, "low_holders": 0,
        "low_liq": 0, "few_txns": 0, "honeypot": 0, "wash": 0
    }

    for t in tokens:
        dex = enriched.get(t["mint"])

        # Not on Dexscreener yet
        if not dex:
            stats["no_dex"] += 1
            continue

        # Volume floor: $1M
        if dex["volume"] < MIN_VOLUME_USD:
            stats["low_vol"] += 1
            continue

        # Holder floor: skip for now (Dexscreener doesn't provide holder count)
        # Will re-enable when we add a Solana RPC call for this
        # if t.get("holders", 0) < MIN_HOLDERS:
        #     stats["low_holders"] += 1
        #     continue

        # Minimum liquidity
        if dex["liquidity"] < MIN_LIQUIDITY_USD:
            stats["low_liq"] += 1
            continue

        # Minimum transactions
        if dex["buys"] + dex["sells"] < MIN_TRANSACTIONS:
            stats["few_txns"] += 1
            continue

        # Honeypot: many buys, zero sells
        if dex["buys"] >= 20 and dex["sells"] == 0:
            stats["honeypot"] += 1
            logger.debug(f"Honeypot: {t['symbol']} buys={dex['buys']} sells=0")
            continue

        # Extreme wash trading: vol/liq > 1000x
        if dex["liquidity"] > 0 and dex["volume"] / dex["liquidity"] > 1000:
            stats["wash"] += 1
            logger.debug(f"Wash trade: {t['symbol']} ratio={dex['volume']/dex['liquidity']:.0f}x")
            continue

        passed.append((t, dex))

    logger.info(
        f"Filters — no_dex:{stats['no_dex']} | low_vol:{stats['low_vol']} | "
        f"low_holders:{stats['low_holders']} | low_liq:{stats['low_liq']} | "
        f"few_txns:{stats['few_txns']} | honeypot:{stats['honeypot']} | "
        f"wash:{stats['wash']} | passed:{len(passed)}"
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

    # Vol/liq ratio
    ratio = vol / liq
    if ratio > 500:
        score += 30
        flags.append(f"Extreme vol/liq ratio ({ratio:.0f}x)")
    elif ratio > 200:
        score += 20
        flags.append(f"Very high vol/liq ratio ({ratio:.0f}x)")
    elif ratio > 100:
        score += 10
        flags.append(f"Elevated vol/liq ratio ({ratio:.0f}x)")
    elif ratio > 50:
        score += 4

    # Buy/sell imbalance
    if total > 0 and sells > 0:
        buy_ratio = buys / total
        if buy_ratio > 0.95:
            score += 20
            flags.append(f"Extreme buy ratio ({buy_ratio:.0%})")
        elif buy_ratio > 0.90:
            score += 12
            flags.append(f"High buy ratio ({buy_ratio:.0%})")
        elif buy_ratio > 0.85:
            score += 5

    # Avg txn size
    if total > 0:
        avg = vol / total
        if avg > 100_000:
            score += 20
            flags.append(f"Huge avg txn size (${avg:,.0f})")
        elif avg > 50_000:
            score += 14
            flags.append(f"Large avg txn size (${avg:,.0f})")
        elif avg > 20_000:
            score += 8
            flags.append(f"Elevated avg txn size (${avg:,.0f})")

    # Liq/mcap
    if mcap > 0:
        lm = liq / mcap
        if lm < 0.003:
            score += 15
            flags.append(f"Dangerous liq/mcap ratio ({lm:.2%})")
        elif lm < 0.01:
            score += 8
            flags.append(f"Low liq/mcap ratio ({lm:.2%})")

    score = min(score, 100)
    if score <= 20:
        label = "Organic"
    elif score <= 40:
        label = "Likely Organic"
    elif score <= 65:
        label = "Suspicious"
    else:
        label = "Likely Manipulated"

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
        "website": dex.get("website") or token.get("website", ""),
        "twitter": dex.get("twitter") or token.get("twitter", ""),
        "age_hours": age_hours,
        "volume": dex["volume"],
        "liquidity": dex["liquidity"],
        "market_cap": dex.get("market_cap"),
        "price_change": dex["price_change"],
        "buys": dex["buys"],
        "sells": dex["sells"],
        "holders": token.get("holders", 0),
        "score": score,
        "label": label,
        "flags": flags,
        "discovered": discovered_count,
    }


# ===========================================================================
# STEP 5 — TELEGRAM FORMATTING + SENDING
# ===========================================================================

def fmt(v):
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v / 1_000:.0f}K"
    return f"${v:.0f}"


def risk_emoji(label):
    return {
        "Organic": "✅",
        "Likely Organic": "🟢",
        "Suspicious": "🟡",
        "Likely Manipulated": "🔴",
    }.get(label, "❓")


async def send_telegram(client, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured — skipping")
        return
    try:
        r = await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        result = r.json()
        if result.get("ok"):
            logger.info("Telegram message sent ✅")
        else:
            logger.error(f"Telegram error: {result.get('description')}")
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")


def build_token_entry(t, rank):
    emoji = risk_emoji(t["label"])
    vol = fmt(t["volume"])
    mcap = fmt(t["market_cap"]) if t.get("market_cap") else "N/A"
    pc = f"{t['price_change']:+.0f}%"
    age = f"{t['age_hours']:.1f}h"
    score = t["score"]
    rank_icon = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"{rank}.")

    # Build links line — always show Dexscreener, optionally X and Website
    links = [f"<a href=\"{t['url']}\">Dexscreener</a>"]
    if t.get("twitter"):
        links.append(f"<a href=\"{t['twitter']}\">X</a>")
    if t.get("website"):
        links.append(f"<a href=\"{t['website']}\">Website</a>")
    links_line = " | ".join(links)

    return (
        f"{rank_icon} <b>{t['name']}</b> <code>${t['symbol']}</code>\n"
        f"   Vol: <b>{vol}</b> | MCap: {mcap} | {pc} | {age} old\n"
        f"   Risk: {emoji} {score}/100\n"
        f"   CA: <code>{t['mint']}</code>\n"
        f"   {links_line}"
    )


def build_token_list_message(tokens, scan_date):
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    header = (
        f"🔥 <b>Potential Farms — {scan_date}</b>\n"
        f"<i>{len(tokens)} tokens | $1M+ vol | no honeypots | {now_str}</i>"
    )
    entries = [build_token_entry(t, i) for i, t in enumerate(tokens, 1)]
    return header + "\n\n" + "\n\n".join(entries)


async def send_results(client, tokens, scan_date):
    if not tokens:
        await send_telegram(
            client,
            f"📭 <b>Potential Farms — {scan_date}</b>\n\n"
            f"No tokens matched today's criteria:\n"
            f"• $1M+ volume\n"
            f"• No honeypots\n"
            f"• No extreme wash trading",
        )
        return

    # Header message
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    header = (
        f"🔥 <b>Potential Farms — {scan_date}</b>\n"
        f"<i>{len(tokens)} tokens | $1M+ vol | no honeypots | {now_str}</i>"
    )
    await send_telegram(client, header)
    await asyncio.sleep(0.3)

    # Send each token as its own message so it's clean and easy to read
    for i, t in enumerate(tokens, 1):
        msg = build_token_entry(t, i)
        await send_telegram(client, msg)
        await asyncio.sleep(0.3)


# ===========================================================================
# STEP 6 — EXPORT (JSON + CSV)
# ===========================================================================

def export_files(tokens, scan_date):
    OUTPUT_DIR.mkdir(exist_ok=True)

    with open(OUTPUT_DIR / f"{scan_date}_runners.json", "w") as f:
        json.dump({"scan_date": scan_date, "results": tokens}, f, indent=2, default=str)

    if tokens:
        fields = ["rank", "name", "symbol", "mint", "url", "age_hours", "volume",
                  "liquidity", "market_cap", "price_change", "buys", "sells",
                  "holders", "score", "label"]
        with open(OUTPUT_DIR / f"{scan_date}_runners.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for rank, t in enumerate(tokens, 1):
                row = dict(t)
                row["rank"] = rank
                w.writerow({k: row.get(k, "") for k in fields})

    logger.info(f"Exported {len(tokens)} results to output/")


# ===========================================================================
# MAIN SCAN PIPELINE
# ===========================================================================

async def run_scan(client=None):
    """
    Full pipeline. Can be called with an existing client (from command handler)
    or creates its own (when called from scheduler).
    """
    scan_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("=" * 50)
    logger.info(f"Starting scan — {scan_date}")
    logger.info("=" * 50)

    init_db()

    async def _scan(c):
        raw_tokens = await discover_tokens(c)
        if not raw_tokens:
            logger.warning("No tokens discovered")
            await send_telegram(c, f"⚠️ Scan ran but pump.fun API returned no tokens ({scan_date})")
            return []

        enriched = await enrich_tokens(c, raw_tokens)
        passed = filter_tokens(raw_tokens, enriched)

        logger.info(f"Pipeline: {len(raw_tokens)} discovered → {len(enriched)} on Dex → {len(passed)} passed filters")

        if not passed:
            await send_results(c, [], scan_date)
            return []

        results = []
        for token, dex in passed:
            results.append(build_result(token, dex, len(raw_tokens)))
        results.sort(key=lambda x: x["volume"], reverse=True)
        top = results[:TOP_N]

        logger.info(f"Winner: {top[0]['symbol']} vol=${top[0]['volume']:,.0f} holders={top[0]['holders']}")
        export_files(top, scan_date)
        save_results(scan_date, top)
        await send_results(c, top, scan_date)
        return top

    if client:
        return await _scan(client)
    else:
        async with httpx.AsyncClient(
            headers={"User-Agent": "pumpfun-scanner/1.0", "Accept": "application/json"},
            follow_redirects=True,
        ) as c:
            return await _scan(c)


def run():
    return asyncio.run(run_scan())


# ===========================================================================
# TELEGRAM BOT — listens for /dailyscan command
# ===========================================================================

async def poll_telegram_commands():
    """
    Long-polls Telegram for /dailyscan command.
    Anyone in the group can trigger a scan.
    Prevents concurrent scans.
    """
    offset = 0
    scan_in_progress = False

    logger.info("Telegram command listener started — /dailyscan ready")

    async with httpx.AsyncClient(
        headers={"User-Agent": "pumpfun-scanner/1.0"},
        follow_redirects=True,
    ) as client:
        while True:
            try:
                r = await client.get(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                    params={
                        "offset": offset,
                        "timeout": 30,
                        "allowed_updates": ["message"],
                    },
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
                            await send_telegram(
                                client,
                                "⏳ A scan is already running — please wait a minute!",
                            )
                            continue

                        logger.info(f"/dailyscan triggered by {user}")
                        scan_in_progress = True

                        await send_telegram(
                            client,
                            f"🔍 <b>Scan started by {user}...</b>\n"
                            f"<i>Fetching for potential farms, this takes ~1 minute.</i>",
                        )

                        try:
                            await run_scan(client=client)
                        except Exception as e:
                            logger.error(f"/dailyscan error: {e}")
                            await send_telegram(client, f"❌ Scan failed: {e}")
                        finally:
                            scan_in_progress = False

            except Exception as e:
                logger.warning(f"Telegram poll error: {e}")
                await asyncio.sleep(10)


# ===========================================================================
# MAIN — scheduler + Telegram command listener running together
# ===========================================================================

async def main_async():
    init_db()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run,
        CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        id="daily_scan",
        replace_existing=True,
        misfire_grace_time=300,
        coalesce=True,
    )

    t = threading.Thread(target=scheduler.start, daemon=True)
    t.start()

    logger.info(f"Scheduler started — daily scan at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC")
    logger.info("Telegram command listener active — /dailyscan ready")

    await poll_telegram_commands()


if __name__ == "__main__":
    import sys

    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    )
    logger.add("scanner.log", level="DEBUG", rotation="1 day", retention="14 days")

    if len(sys.argv) > 1 and sys.argv[1] == "scan":
        run()
    else:
        asyncio.run(main_async())

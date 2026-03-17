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
# STEP 1 — DISCOVER tokens from pump.fun (includes holder_count)
# ===========================================================================

async def discover_tokens(client):
    """
    Fetches tokens from pump.fun sorted by creation time (newest first).
    pump.fun returns holder_count per token — we use this for the 300 holder filter.
    Stops once all tokens on a page are older than MAX_TOKEN_AGE_HOURS.
    """
    cutoff_ms = int(
        (datetime.now(timezone.utc) - timedelta(hours=MAX_TOKEN_AGE_HOURS)).timestamp() * 1000
    )
    tokens = []
    offset = 0

    for page in range(100):
        try:
            r = await client.get(
                f"{PUMPFUN_API}/coins",
                params={
                    "offset": offset,
                    "limit": 50,
                    "sort": "created_timestamp",
                    "order": "DESC",
                    "includeNsfw": "false",
                },
                timeout=20,
            )
            data = r.json()
        except Exception as e:
            logger.error(f"Pump.fun error page {page+1}: {e}")
            break

        if not data or not isinstance(data, list):
            break

        found_old = False
        page_tokens = []

        for item in data:
            ts = item.get("created_timestamp", 0)
            mint = item.get("mint", "").strip()
            if not mint:
                continue
            if ts < cutoff_ms:
                found_old = True
                continue

            # holder_count comes directly from pump.fun API
            holders = item.get("holder_count") or item.get("holders") or 0

            page_tokens.append({
                "mint": mint,
                "name": item.get("name") or "Unknown",
                "symbol": item.get("symbol") or "?",
                "created_ms": ts,
                "holders": int(holders),
            })

        tokens.extend(page_tokens)
        logger.info(f"Page {page+1}: {len(page_tokens)} tokens in window (total: {len(tokens)})")

        if found_old and not page_tokens:
            break
        if len(data) < 50:
            break
        offset += 50
        await asyncio.sleep(0.25)

    logger.info(f"Discovered {len(tokens)} pump.fun tokens in last {MAX_TOKEN_AGE_HOURS}h")
    return tokens


# ===========================================================================
# STEP 2 — ENRICH with Dexscreener (volume, liquidity, txns)
# ===========================================================================

async def enrich_tokens(client, tokens):
    mints = [t["mint"] for t in tokens]
    enriched = {}
    batch_size = 30

    for i in range(0, len(mints), batch_size):
        batch = mints[i:i + batch_size]
        try:
            r = await client.get(
                f"{DEXSCREENER}/latest/dex/tokens/{','.join(batch)}",
                timeout=20,
            )
            data = r.json()
            pairs = data.get("pairs") or []

            for pair in pairs:
                if pair.get("chainId") != "solana":
                    continue
                base = pair.get("baseToken", {})
                mint = base.get("address", "")
                vol = float((pair.get("volume") or {}).get("h24") or 0)

                if mint not in enriched or vol > enriched[mint]["volume"]:
                    liq = float((pair.get("liquidity") or {}).get("usd") or 0)
                    txns_h24 = pair.get("txns", {}).get("h24", {})
                    enriched[mint] = {
                        "url": pair.get("url", ""),
                        "dex": pair.get("dexId", ""),
                        "volume": vol,
                        "liquidity": liq,
                        "market_cap": pair.get("marketCap"),
                        "price_usd": pair.get("priceUsd"),
                        "price_change": float((pair.get("priceChange") or {}).get("h24") or 0),
                        "buys": int(txns_h24.get("buys") or 0),
                        "sells": int(txns_h24.get("sells") or 0),
                        "pair_created_ms": pair.get("pairCreatedAt"),
                    }
        except Exception as e:
            logger.warning(f"Dexscreener batch {i // batch_size + 1} error: {e}")
        await asyncio.sleep(0.25)

    logger.info(f"Enriched: {len(enriched)}/{len(mints)} tokens found on Dexscreener")
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

        # Holder floor: 300 unique holders (from pump.fun API)
        if t.get("holders", 0) < MIN_HOLDERS:
            stats["low_holders"] += 1
            continue

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


def build_token_list_message(tokens, scan_date):
    """
    Builds a clean Telegram message listing all runners.
    Token name is bold, contract address is in <code> tags
    so users can tap to copy it directly in Telegram.
    """
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [
        f"🔥 <b>Pump.fun Runners — {scan_date}</b>",
        f"<i>{len(tokens)} tokens | $1M+ vol | 300+ holders | no honeypots</i>",
        f"<i>Scan time: {now_str}</i>\n",
    ]

    for i, t in enumerate(tokens, 1):
        emoji = risk_emoji(t["label"])
        vol = fmt(t["volume"])
        mcap = fmt(t["market_cap"]) if t.get("market_cap") else "N/A"
        holders = f"{t.get('holders', 0):,}"
        pc = f"{t['price_change']:+.0f}%"
        age = f"{t['age_hours']:.1f}h"
        score = t["score"]

        # Medal for top 3
        rank_icon = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")

        lines.append(
            f"{rank_icon} <b>{t['name']}</b> <code>${t['symbol']}</code>\n"
            f"   Vol: <b>{vol}</b> | MCap: {mcap} | {pc} | {age} old\n"
            f"   Holders: {holders} | Risk: {emoji} {score}/100\n"
            f"   CA: <code>{t['mint']}</code>\n"
            f"   <a href=\"{t['url']}\">Dexscreener</a>"
        )

    return "\n".join(lines)


async def send_results(client, tokens, scan_date):
    """
    Sends results as a paginated list (Telegram max 4096 chars per message).
    Splits into multiple messages if needed.
    """
    if not tokens:
        await send_telegram(
            client,
            f"📭 <b>Pump.fun scan — {scan_date}</b>\n\n"
            f"No tokens matched today's criteria:\n"
            f"• $1M+ volume\n"
            f"• 300+ holders\n"
            f"• No honeypots\n"
            f"• No extreme wash trading",
        )
        return

    # Split into chunks of 5 tokens per message to stay under 4096 char limit
    chunk_size = 5
    chunks = [tokens[i:i + chunk_size] for i in range(0, len(tokens), chunk_size)]

    for idx, chunk in enumerate(chunks):
        if idx == 0:
            msg = build_token_list_message(chunk, scan_date)
            # Add summary header only to first message
        else:
            # Continuation messages
            lines = [f"<i>...continued ({idx * chunk_size + 1}–{min((idx + 1) * chunk_size, len(tokens))} of {len(tokens)})</i>\n"]
            for i, t in enumerate(chunk, idx * chunk_size + 1):
                emoji = risk_emoji(t["label"])
                vol = fmt(t["volume"])
                mcap = fmt(t["market_cap"]) if t.get("market_cap") else "N/A"
                holders = f"{t.get('holders', 0):,}"
                pc = f"{t['price_change']:+.0f}%"
                age = f"{t['age_hours']:.1f}h"
                score = t["score"]
                lines.append(
                    f"{i}. <b>{t['name']}</b> <code>${t['symbol']}</code>\n"
                    f"   Vol: <b>{vol}</b> | MCap: {mcap} | {pc} | {age} old\n"
                    f"   Holders: {holders} | Risk: {emoji} {score}/100\n"
                    f"   CA: <code>{t['mint']}</code>\n"
                    f"   <a href=\"{t['url']}\">Dexscreener</a>"
                )
            msg = "\n".join(lines)

        await send_telegram(client, msg)
        await asyncio.sleep(0.5)  # Small delay between messages


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
                            f"<i>Fetching pump.fun tokens, this takes ~1 minute.</i>",
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

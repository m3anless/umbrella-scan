# Pump.fun Runner Scanner
# Finds PumpSwap tokens with $1M+ vol in last 24h.
# Shows top 30 traders: avg entry, avg exit, total PnL extracted.
# Uses Dexscreener API only.

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

DEXSCREENER      = "https://api.dexscreener.com"
DEXSCREENER_IO   = "https://io.dexscreener.com"
DB_PATH          = "scanner.db"
OUTPUT_DIR       = Path("output")

PUMPSWAP_DEX_IDS = {"pumpfun", "pumpswap", "pump-fun", "pump_fun"}

SEARCH_QUERIES = [
    "pumpswap", "pump.fun", "pumpfun",
    "solana meme coin", "solana dog", "solana cat",
    "solana ai", "solana trump", "solana pepe",
    "solana based", "solana inu", "solana moon",
    "solana chad", "solana elon", "solana grok",
]

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
            total_extracted REAL,
            avg_entry REAL,
            avg_exit REAL,
            trader_count INTEGER,
            traders_json TEXT,
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
            winner_extracted REAL
        )
    """)
    conn.commit()
    conn.close()


def save_results(scan_date, tokens):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM results WHERE scan_date=?", (scan_date,))
    for rank, t in enumerate(tokens, 1):
        traders = t.get("traders") or {}
        conn.execute("""
            INSERT INTO results
            (scan_date,rank,mint,name,symbol,url,website,twitter,age_hours,volume,liquidity,
             market_cap,price_change,buys,sells,total_extracted,avg_entry,avg_exit,
             trader_count,traders_json,flags,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            scan_date, rank,
            t["mint"], t["name"], t["symbol"], t["url"],
            t.get("website",""), t.get("twitter",""),
            t["age_hours"], t["volume"], t["liquidity"],
            t.get("market_cap"), t["price_change"],
            t["buys"], t["sells"],
            traders.get("total_extracted", 0),
            traders.get("avg_entry", 0),
            traders.get("avg_exit", 0),
            traders.get("trader_count", 0),
            json.dumps(traders),
            json.dumps(t.get("flags", [])),
            datetime.now(timezone.utc).isoformat()
        ))
    if tokens:
        w = tokens[0]
        conn.execute("""
            INSERT OR REPLACE INTO daily
            (scan_date,discovered,passed,winner_name,winner_symbol,winner_volume,winner_extracted)
            VALUES (?,?,?,?,?,?,?)
        """, (scan_date, t.get("discovered", 0), len(tokens),
              w["name"], w["symbol"], w["volume"],
              (w.get("traders") or {}).get("total_extracted", 0)))
    conn.commit()
    conn.close()


# ===========================================================================
# STEP 1 — DISCOVER via Dexscreener search
# ===========================================================================

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
            # Dexscreener uses "pairAddress" in search results
            pair_addr = (pair.get("pairAddress") or pair.get("pair_address") or "").strip()
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
                "pair_address": pair_addr,
                "price_usd": pair.get("priceUsd", "0"),
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
                    logger.info(f"Query '{query}': +{new} (total: {len(tokens)})")
        except Exception as e:
            logger.warning(f"Search error '{query}': {e}")
        await asyncio.sleep(0.3)

    result = list(tokens.values())
    logger.info(f"Discovery: {len(result)} PumpSwap tokens in last {MAX_TOKEN_AGE_HOURS}h")
    return result


# ===========================================================================
# STEP 2 — ENRICH (pass-through)
# ===========================================================================

async def enrich_tokens(client, tokens):
    enriched = {}
    for t in tokens:
        enriched[t["mint"]] = {
            "url": t.get("url", ""),
            "pair_address": t.get("pair_address", ""),
            "dex": t.get("dex", ""),
            "volume": t.get("volume", 0),
            "liquidity": t.get("liquidity", 0),
            "market_cap": t.get("market_cap"),
            "price_usd": t.get("price_usd", "0"),
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
            stats["no_dex"] += 1; continue
        if dex["volume"] < MIN_VOLUME_USD:
            stats["low_vol"] += 1; continue
        if dex["liquidity"] < MIN_LIQUIDITY_USD:
            stats["low_liq"] += 1; continue
        if dex["buys"] + dex["sells"] < MIN_TRANSACTIONS:
            stats["few_txns"] += 1; continue
        if dex["buys"] >= 20 and dex["sells"] == 0:
            stats["honeypot"] += 1; continue
        if dex["liquidity"] > 0 and dex["volume"] / dex["liquidity"] > 1000:
            stats["wash"] += 1; continue
        passed.append((t, dex))
    logger.info(
        f"Filters: low_vol={stats['low_vol']} low_liq={stats['low_liq']} "
        f"few_txns={stats['few_txns']} honeypot={stats['honeypot']} "
        f"wash={stats['wash']} PASSED={len(passed)}"
    )
    return passed


# ===========================================================================
# STEP 4 — TOP TRADERS from Dexscreener
# Uses the same endpoint that powers the "Top Traders" tab on dexscreener.com
# Returns: total extracted, avg entry, avg exit for top 30 traders
# ===========================================================================

async def get_top_traders(client, pair_address, mint):
    if not pair_address:
        logger.warning(f"Top traders: no pair_address for {mint[:8]}")
        return {}

    logger.info(f"Fetching top traders for pair {pair_address[:12]}...")

    result = {
        "trader_count": 0,
        "total_extracted": 0.0,
        "avg_entry": 0.0,
        "avg_exit": 0.0,
        "top_traders": [],
    }

    # Try multiple endpoint formats since Dexscreener's internal API may vary
    endpoints = [
        f"{DEXSCREENER_IO}/dex/top-traders/v2/solana/{pair_address}",
        f"{DEXSCREENER_IO}/dex/top-traders/solana/{pair_address}",
        f"{DEXSCREENER}/latest/dex/pairs/solana/{pair_address}",
    ]

    traders_raw = []
    for endpoint in endpoints:
        try:
            r = await client.get(
                endpoint,
                params={"rankBy": "profitAndLoss", "order": "desc"},
                headers={
                    "Accept": "application/json",
                    "Origin": "https://dexscreener.com",
                    "Referer": f"https://dexscreener.com/solana/{pair_address}",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                },
                timeout=15,
            )
            logger.info(f"Top traders endpoint {endpoint.split('/')[-3]}: status {r.status_code}")

            if r.status_code == 200:
                data = r.json()
                # Try different response structures
                if isinstance(data, list):
                    traders_raw = data
                elif isinstance(data, dict):
                    traders_raw = (
                        data.get("topTraders") or
                        data.get("traders") or
                        data.get("data") or
                        []
                    )
                    # If it's pairs endpoint, log the keys so we can debug
                    if not traders_raw:
                        logger.info(f"Response keys: {list(data.keys())[:10]}")
                if traders_raw:
                    logger.info(f"Got {len(traders_raw)} traders from {endpoint.split('/')[-3]}")
                    break
        except Exception as e:
            logger.warning(f"Top traders endpoint error: {e}")
        await asyncio.sleep(0.3)

    if not traders_raw:
        logger.warning(f"Top traders: no data found for {pair_address[:12]}")
        return result

    traders_raw = traders_raw[:30]
    entries = []
    exits = []
    total_pnl = 0.0
    profitable_count = 0
    trader_summaries = []

    for trader in traders_raw:
        pnl = float(
            trader.get("profitAndLoss") or trader.get("pnl") or
            trader.get("realizedProfit") or trader.get("realizedPnl") or 0
        )
        bought_usd = float(
            trader.get("volumeBought") or trader.get("bought") or
            trader.get("boughtAmountUsd") or trader.get("buyAmountUsd") or 0
        )
        sold_usd = float(
            trader.get("volumeSold") or trader.get("sold") or
            trader.get("soldAmountUsd") or trader.get("sellAmountUsd") or 0
        )
        bought_tokens = float(
            trader.get("tokensBought") or trader.get("boughtAmount") or
            trader.get("buyAmount") or 0
        )
        sold_tokens = float(
            trader.get("tokensSold") or trader.get("soldAmount") or
            trader.get("sellAmount") or 0
        )
        wallet = trader.get("address") or trader.get("maker") or trader.get("wallet") or "?"

        if bought_tokens > 0 and bought_usd > 0:
            entries.append(bought_usd / bought_tokens)
        if sold_tokens > 0 and sold_usd > 0:
            exits.append(sold_usd / sold_tokens)
        if pnl > 0:
            total_pnl += pnl
            profitable_count += 1

        trader_summaries.append({
            "wallet": wallet[:8] + "..." if len(wallet) > 8 else wallet,
            "pnl_usd": round(pnl, 2),
            "bought_usd": round(bought_usd, 2),
            "sold_usd": round(sold_usd, 2),
        })

    result["trader_count"] = len(traders_raw)
    result["total_extracted"] = round(total_pnl, 2)
    result["avg_entry"] = round(sum(entries) / len(entries), 12) if entries else 0
    result["avg_exit"] = round(sum(exits) / len(exits), 12) if exits else 0
    result["profitable_count"] = profitable_count
    result["top_traders"] = trader_summaries[:5]

    logger.info(f"Top traders {mint[:8]}: {len(traders_raw)} traders, extracted=${total_pnl:,.0f}")
    return result


# ===========================================================================
# STEP 5 — BUILD RESULT
# ===========================================================================

def build_result(token, dex, traders, discovered_count):
    now = datetime.now(timezone.utc)
    launch_ms = dex.get("pair_created_ms") or token.get("created_ms")
    age_hours = 0.0
    if launch_ms:
        launch_dt = datetime.fromtimestamp(launch_ms / 1000, tz=timezone.utc)
        age_hours = round((now - launch_dt).total_seconds() / 3600, 2)

    flags = []
    vol = dex["volume"]
    liq = max(dex["liquidity"], 1)
    ratio = vol / liq
    if ratio > 200:
        flags.append(f"High vol/liq ratio ({ratio:.0f}x)")

    # Derive current price from mcap and supply estimate
    # current_price = mcap / total_supply
    # We can't get supply directly, but we store it so the display layer can do:
    # mcap_at_entry = (avg_entry_price / current_price) * current_mcap
    current_mcap = dex.get("market_cap") or 0
    current_price_str = token.get("price_usd") or dex.get("price_usd") or "0"
    try:
        current_price = float(current_price_str)
    except Exception:
        current_price = 0.0

    # Store current price in traders dict for the display layer
    if traders and current_price > 0:
        traders["current_price"] = current_price

    return {
        "mint": token["mint"],
        "name": token["name"],
        "symbol": token["symbol"],
        "url": dex["url"],
        "website": dex.get("website", ""),
        "twitter": dex.get("twitter", ""),
        "pair_address": dex.get("pair_address", ""),
        "age_hours": age_hours,
        "volume": dex["volume"],
        "liquidity": dex["liquidity"],
        "market_cap": current_mcap,
        "price_change": dex["price_change"],
        "buys": dex["buys"],
        "sells": dex["sells"],
        "traders": traders,
        "flags": flags,
        "discovered": discovered_count,
    }


# ===========================================================================
# STEP 6 — TELEGRAM FORMATTING
# ===========================================================================

def fmt(v):
    if not v:
        return "N/A"
    v = float(v)
    if v >= 1000000: return f"${v/1000000:.1f}M"
    if v >= 1000:    return f"${v/1000:.0f}K"
    return f"${v:.0f}"

def fmt_price(v):
    if not v or v == 0:
        return "N/A"
    v = float(v)
    if v >= 0.01:    return f"${v:.4f}"
    if v >= 0.0001:  return f"${v:.6f}"
    return f"${v:.10f}"

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

def price_to_mcap(price, current_price, current_mcap):
    """
    Convert a price point to the market cap at that price.
    Formula: mcap_at_price = (price / current_price) * current_mcap
    This tells you what the market cap was when traders entered/exited.
    """
    if not price or not current_price or not current_mcap:
        return None
    if current_price <= 0:
        return None
    return (price / current_price) * current_mcap


def build_token_entry(t, rank):
    vol = fmt(t["volume"])
    current_mcap = t.get("market_cap") or 0
    mcap = fmt(current_mcap) if current_mcap else "N/A"
    pc = f"{t['price_change']:+.0f}%"
    age = f"{t['age_hours']:.1f}h"
    rank_icon = {1:"🥇",2:"🥈",3:"🥉"}.get(rank, f"{rank}.")

    # Trader stats
    traders = t.get("traders") or {}
    extracted = traders.get("total_extracted", 0)
    avg_entry_price = traders.get("avg_entry", 0)
    avg_exit_price  = traders.get("avg_exit", 0)
    n_traders = traders.get("trader_count", 0)
    profitable = traders.get("profitable_count", 0)

    # Convert avg entry/exit prices to market cap values
    current_price = traders.get("current_price", 0)
    mcap_at_entry = price_to_mcap(avg_entry_price, current_price, current_mcap)
    mcap_at_exit  = price_to_mcap(avg_exit_price,  current_price, current_mcap)

    entry_str = fmt(mcap_at_entry) if mcap_at_entry else "N/A"
    exit_str  = fmt(mcap_at_exit)  if mcap_at_exit  else "N/A"

    if extracted > 0:
        trader_line = (
            f"💸 <b>Top {n_traders} traders extracted: {fmt(extracted)}</b>\n"
            f"   Avg entry mcap: {entry_str} | Avg exit mcap: {exit_str}\n"
            f"   {profitable}/{n_traders} traders profitable"
        )
    elif n_traders > 0:
        trader_line = (
            f"💸 Top {n_traders} traders — no profitable exits yet\n"
            f"   Avg entry mcap: {entry_str}"
        )
    else:
        trader_line = "💸 Trader data: not available"

    # Links
    links = [f"<a href=\"{t['url']}\">Dexscreener</a>"]
    if t.get("twitter"):
        links.append(f"<a href=\"{t['twitter']}\">X</a>")
    if t.get("website"):
        links.append(f"<a href=\"{t['website']}\">Website</a>")

    return (
        f"{rank_icon} <b>{t['name']}</b> <code>${t['symbol']}</code>\n"
        f"Vol: <b>{vol}</b> | MCap: {mcap} | {pc} | {age} old\n"
        f"{trader_line}\n"
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
# STEP 7 — EXPORT
# ===========================================================================

def export_files(tokens, scan_date):
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_DIR / f"{scan_date}_runners.json", "w") as f:
        json.dump({"scan_date": scan_date, "results": tokens}, f, indent=2, default=str)
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
        # 1. Discover
        raw = await discover_tokens(c)
        if not raw:
            await send_telegram(c, f"No tokens found ({scan_date})")
            return []

        # 2. Enrich
        enriched = await enrich_tokens(c, raw)

        # 3. Filter
        passed = filter_tokens(raw, enriched)
        logger.info(f"Pipeline: {len(raw)} discovered -> {len(passed)} passed filters")

        if not passed:
            await send_results(c, [], scan_date)
            return []

        # 4. Get top traders for each token
        results = []
        for token, dex in passed:
            pair_addr = dex.get("pair_address", "")
            traders = await get_top_traders(c, pair_addr, token["mint"])
            await asyncio.sleep(0.3)
            results.append(build_result(token, dex, traders, len(raw)))

        # 5. Sort by volume
        results.sort(key=lambda x: x["volume"], reverse=True)
        top = results[:TOP_N]

        logger.info(
            f"Winner: {top[0]['symbol']} "
            f"vol={fmt(top[0]['volume'])} "
            f"extracted={fmt((top[0].get('traders') or {}).get('total_extracted', 0))}"
        )

        export_files(top, scan_date)
        save_results(scan_date, top)
        await send_results(c, top, scan_date)
        return top

    if client:
        return await _scan(client)
    else:
        async with httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                     "Accept": "application/json"},
            follow_redirects=True,
        ) as c:
            return await _scan(c)


def run():
    return asyncio.run(run_scan())


# ===========================================================================
# TELEGRAM BOT
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
                      id="daily_scan", replace_existing=True,
                      misfire_grace_time=300, coalesce=True)
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

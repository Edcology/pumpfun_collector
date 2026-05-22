    # backfill.py
import asyncio
import aiohttp
import schedule
import time
import threading
from datetime import datetime, timezone
from database.db import engine
from sqlalchemy import text
from utils.logger import logger
BITQUERY_API_KEY = "ef87e84b-f641-4379-b8be-065aec1de1a1"
BITQUERY_URL     = "https://streaming.bitquery.io/graphql"
CONCURRENCY      = 3
TARGET_MULTIPLIER = 2.0
# ------------------------------------------------------------------
# Bitquery — fetch OHLCV for a token's first 24h
# ------------------------------------------------------------------
OHLCV_QUERY = """
query ($mint: String!, $from: ISO8601DateTime!, $till: ISO8601DateTime!) {
solana(network: solana) {
    dexTrades(
    baseCurrency: {is: $mint}
    date: {between: [$from, $till]}
    options: {asc: "timeInterval.minute", limit: 1440}
    ) {
    timeInterval {
        minute(count: 1)
    }
    high: quotePrice(calculate: maximum)
    low:  quotePrice(calculate: minimum)
    open: minimum(of: block, get: quote_price)
    close: maximum(of: block, get: quote_price)
    volume: quoteAmount
    }
}
}
"""
async def fetch_ohlcv(
    session: aiohttp.ClientSession,
    mint: str,
    created_at: datetime,
) -> dict | None:
    from_dt = created_at.strftime("%Y-%m-%dT%H:%M:%S")
    # 24h window
    import datetime as dt
    till_dt = (created_at + dt.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    headers = {
        "Content-Type":  "application/json",
        "X-API-KEY":     BITQUERY_API_KEY,
    }
    payload = {
        "query":     OHLCV_QUERY,
        "variables": {"mint": mint, "from": from_dt, "till": till_dt},
    }
    try:
        async with session.post(
            BITQUERY_URL,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                logger.debug(f"Bitquery HTTP {r.status} for {mint[:8]}")
                return None
            data = await r.json()
            trades = (
                data.get("data", {})
                    .get("solana", {})
                    .get("dexTrades", [])
            )
            return trades if trades else None
    except Exception as e:
        logger.debug(f"Bitquery fetch failed [{mint[:8]}]: {e}")
        return None
def compute_outcome(trades: list, entry_price: float) -> dict:
    if not trades or entry_price == 0:
        return {
            "hit_2x":          False,
            "rug_detected":    True,
            "peak_price_sol":  entry_price,
            "peak_multiplier": 0,
            "time_to_2x_secs": None,
        }
    peak_price     = entry_price
    time_to_2x     = None
    elapsed_secs   = 0
    for candle in trades:
        high = float(candle.get("high") or 0)
        elapsed_secs += 60  # 1 candle = 1 minute
        if high > peak_price:
            peak_price = high
        if time_to_2x is None and high >= entry_price * TARGET_MULTIPLIER:
            time_to_2x = elapsed_secs
    return {
        "hit_2x":          time_to_2x is not None,
        "time_to_2x_secs": time_to_2x,
        "rug_detected":    peak_price < entry_price * 0.1,  # dropped 90%+
        "peak_price_sol":  peak_price,
        "peak_multiplier": round(peak_price / entry_price, 4),
    }
# ------------------------------------------------------------------
# Backfill a single token
# ------------------------------------------------------------------
async def backfill_token(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    token: dict,
):
    async with semaphore:
        mint       = token["mint"]
        db_id      = token["id"]
        created_at = token["watch_started_at"]
        trades = await fetch_ohlcv(session, mint, created_at)
        # Get entry price from first candle open
        entry_price = 0
        if trades:
            entry_price = float(trades[0].get("open") or 0)
        outcome = compute_outcome(trades, entry_price)
        async with engine.begin() as conn:
            await conn.execute(text("""
                UPDATE token_snapshots SET
                    entry_price_sol  = :entry,
                    hit_2x           = :hit_2x,
                    time_to_2x_secs  = :time_to_2x_secs,
                    rug_detected     = :rug,
                    peak_price_sol   = :peak,
                    peak_multiplier  = :multiplier,
                    watch_ended_at   = :now
                WHERE id = :id
            """), {
                "id":        db_id,
                "entry":     entry_price,
                "hit_2x":    outcome["hit_2x"],
                "time_to_2x_secs": outcome["time_to_2x_secs"],
                "rug":       outcome["rug_detected"],
                "peak":      outcome["peak_price_sol"],
                "multiplier": outcome["peak_multiplier"],
                "now":       datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            })
        logger.info(
            f"[BACKFILLED] {mint[:8]} | "
            f"entry={entry_price:.8f} | "
            f"peak={outcome['peak_multiplier']}x | "
            f"2x={outcome['hit_2x']} | "
            f"rug={outcome['rug_detected']}"
        )
        await asyncio.sleep(0.3)  # stay under Bitquery rate limit
# ------------------------------------------------------------------
# Main backfill job — runs every 6 hours automatically
# ------------------------------------------------------------------
async def run_backfill():
    logger.info("[BACKFILL] Starting scheduled backfill run...")

    async with engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT id, mint, watch_started_at
            FROM token_snapshots
            WHERE watch_ended_at IS NULL
              AND watch_started_at < datetime('now', '-24 hours')
            ORDER BY watch_started_at ASC
        """))
        token_snapshots = [dict(row._mapping) for row in result]

    if not token_snapshots:
        logger.info("[BACKFILL] Nothing to backfill.")
        return

    logger.info(f"[BACKFILL] {len(token_snapshots)} tokens to process...")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        tasks = [
            backfill_token(semaphore, session, token)
            for token in token_snapshots
        ]
        await asyncio.gather(*tasks)

    logger.info("[BACKFILL] Run complete.")
def run_backfill_sync():
    """Called by schedule — bridges sync scheduler to async."""
    asyncio.run(run_backfill())
# ------------------------------------------------------------------
# Scheduler — runs in a background thread so it doesn't block main
# ------------------------------------------------------------------
def start_scheduler():
    schedule.every(6).hours.do(run_backfill_sync)
    # Also run immediately on startup
    run_backfill_sync()
    while True:
        schedule.run_pending()
        time.sleep(60)
if __name__ == "__main__":
    # Run standalone for manual backfill
    asyncio.run(run_backfill())

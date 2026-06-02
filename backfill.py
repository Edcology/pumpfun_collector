import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
from datetime import datetime as dt, timezone

from sqlalchemy import text

from database.db import engine
from utils.logger import logger

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

GT_BASE      = "https://api.geckoterminal.com/api/v2"
GT_HEADERS   = {"Accept": "application/json;version=20230302"}
GT_CALL_DELAY = 20.0

PEAK_WINDOW_SECS = 3600


# ─────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────

async def write_result(db_id, entry, peak, multiplier, hit_2x):
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                UPDATE token_snapshots
                SET
                    entry_price_sol = :entry,
                    peak_price_sol  = :peak,
                    peak_multiplier = :multiplier,
                    hit_2x          = :hit_2x,
                    watch_ended_at  = :ended
                WHERE id = :id
            """),
            {
                "id":         db_id,
                "entry":      entry,
                "peak":       peak,
                "multiplier": multiplier,
                "hit_2x":     hit_2x,
                "ended":      dt.now(timezone.utc),
            },
        )


# ─────────────────────────────────────────────────────────────
# GeckoTerminal
# ─────────────────────────────────────────────────────────────

async def get_pool(session, mint):
    try:
        async with session.get(
            f"{GT_BASE}/networks/solana/tokens/{mint}/pools",
            params={"page": 1},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return None, None
            data = (await r.json()).get("data", [])
            if not data:
                return None, None
            best        = max(data, key=lambda p: float(p["attributes"].get("reserve_in_usd") or 0))
            addr        = best["attributes"]["address"]
            base_id     = best.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "")
            token_param = "base" if mint in base_id else "quote"
            return addr, token_param
    except Exception as e:
        logger.warning(f"[GT pool] {mint[:8]} failed: {e}")
        return None, None


async def get_ohlcv(session, pool_address, token_param):
    try:
        async with session.get(
            f"{GT_BASE}/networks/solana/pools/{pool_address}/ohlcv/minute",
            params={
                "aggregate": 1,
                "limit":     100,
                "currency":  "token",
                "token":     token_param,
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return []
            return (await r.json()).get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    except Exception as e:
        logger.warning(f"[GT ohlcv] {pool_address[:8]} failed: {e}")
        return []


def compute_prices(candles):
    if not candles:
        return 0.0, 0.0
    candles = sorted(candles, key=lambda c: c[0])
    # Use first candle as entry, peak high across first hour
    launch_ts   = candles[0][0]
    window      = [c for c in candles if c[0] <= launch_ts + PEAK_WINDOW_SECS]
    entry_price = float(window[0][1])
    peak_price  = max(float(c[2]) for c in window)
    return entry_price, peak_price


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def run_backfill():
    logger.info("[BACKFILL] starting...")

    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT id, mint FROM token_snapshots WHERE watch_ended_at IS NULL")
        )
        tokens = [dict(row._mapping) for row in result]

    total = len(tokens)
    logger.info(f"[BACKFILL] {total} tokens to process")

    async with aiohttp.ClientSession(headers=GT_HEADERS) as session:
        for i, token in enumerate(tokens, 1):
            mint  = token["mint"]
            db_id = token["id"]

            logger.info(f"[{i}/{total}] processing {mint[:8]}")

            # 1. Pool
            pool_address, token_param = await get_pool(session, mint)
            await asyncio.sleep(GT_CALL_DELAY)

            if not pool_address:
                logger.warning(f"[{i}/{total}] {mint[:8]} — no pool, marking not hit")
                await write_result(db_id, 0, 0, 0, False)
                continue

            # 2. OHLCV
            candles = await get_ohlcv(session, pool_address, token_param)
            await asyncio.sleep(GT_CALL_DELAY)

            if not candles:
                logger.warning(f"[{i}/{total}] {mint[:8]} — no candles, marking not hit")
                await write_result(db_id, 0, 0, 0, False)
                continue

            # 3. Compute & write
            entry, peak = compute_prices(candles)
            if entry <= 0:
                logger.warning(f"[{i}/{total}] {mint[:8]} — zero entry, marking not hit")
                await write_result(db_id, 0, 0, 0, False)
                continue

            multiple = peak / entry if peak > 0 else 0.0
            hit_2x   = multiple >= 2

            await write_result(db_id, entry, peak, multiple, hit_2x)
            logger.info(f"[{i}/{total}] {mint[:8]} — entry={entry:.12f} peak={multiple:.2f}x hit_2x={hit_2x}")

    logger.info("[BACKFILL] complete")


if __name__ == "__main__":
    asyncio.run(run_backfill())
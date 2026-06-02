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

HELIUS_API_KEY = "73a77637-cda4-41af-9f54-70961b91d0ea"

RPC_URL = (
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
)

HELIUS_TX_URL = (
    f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_API_KEY}"
)

TARGET_SOL_ENTRY = 3
PEAK_WINDOW_SECS = 3600

CONCURRENCY = 5
BATCH_SIZE = 20


# ─────────────────────────────────────────────────────────────
# RPC
# ─────────────────────────────────────────────────────────────

async def rpc_post(
    session,
    method,
    params
):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    async with session.post(
        RPC_URL,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=30)
    ) as r:

        if r.status != 200:
            return None

        data = await r.json()

        if "error" in data:
            return None

        return data.get("result")


async def get_signatures(
    session,
    mint,
    limit=100
):
    result = await rpc_post(
        session,
        "getSignaturesForAddress",
        [
            mint,
            {
                "limit": limit,
                "commitment": "confirmed"
            }
        ]
    )

    return result or []


# ─────────────────────────────────────────────────────────────
# Helius Enhanced TX Parsing
# ─────────────────────────────────────────────────────────────

async def parse_transactions(
    session,
    signatures
):
    if not signatures:
        return []

    payload = {
        "transactions": signatures
    }

    async with session.post(
        HELIUS_TX_URL,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=60)
    ) as r:

        if r.status != 200:
            return []

        return await r.json()


# ─────────────────────────────────────────────────────────────
# Trade Extraction
# ─────────────────────────────────────────────────────────────

def extract_trades(
    txs,
    mint
):
    trades = []

    for tx in txs:

        try:
            token_transfers = (
                tx.get("tokenTransfers") or []
            )

            native_transfers = (
                tx.get("nativeTransfers") or []
            )

            token_transfer = None

            for t in token_transfers:
                if t.get("mint") == mint:
                    token_transfer = t
                    break

            if not token_transfer:
                continue

            token_amount = float(
                token_transfer
                .get("tokenAmount", 0)
            )

            if token_amount <= 0:
                continue

            lamports = sum(
                float(n.get("amount", 0))
                for n in native_transfers
            )

            sol_amount = lamports / 1e9

            if sol_amount <= 0:
                continue

            price = (
                sol_amount / token_amount
            )

            trades.append({
                "timestamp": tx.get("timestamp"),
                "price": price,
                "sol_amount": sol_amount,
                "token_amount": token_amount,
            })

        except Exception:
            continue

    trades.sort(
        key=lambda x: x["timestamp"]
    )

    return trades


# ─────────────────────────────────────────────────────────────
# Synthetic Entry
# ─────────────────────────────────────────────────────────────

def compute_entry(
    trades,
    target_sol=TARGET_SOL_ENTRY
):
    cumulative_sol = 0

    weighted_sol = 0
    weighted_tokens = 0

    for trade in trades:

        cumulative_sol += trade["sol_amount"]

        weighted_sol += trade["sol_amount"]

        weighted_tokens += (
            trade["token_amount"]
        )

        if cumulative_sol >= target_sol:
            break

    if weighted_tokens == 0:
        return 0

    return weighted_sol / weighted_tokens


# ─────────────────────────────────────────────────────────────
# Peak Calculation
# ─────────────────────────────────────────────────────────────

def compute_peak(
    trades,
    start_ts
):
    end_ts = (
        start_ts + PEAK_WINDOW_SECS
    )

    prices = [
        t["price"]
        for t in trades
        if start_ts <= t["timestamp"] <= end_ts
    ]

    if not prices:
        return 0

    return max(prices)


# ─────────────────────────────────────────────────────────────
# Backfill Token
# ─────────────────────────────────────────────────────────────

async def backfill_token(
    semaphore,
    session,
    token
):
    async with semaphore:

        mint = token["mint"]
        db_id = token["id"]

        try:
            signatures = await get_signatures(
                session,
                mint,
                limit=100
            )

            signatures = [
                s["signature"]
                for s in signatures
                if not s.get("err")
            ]

            if not signatures:
                return

            txs = await parse_transactions(
                session,
                signatures
            )

            trades = extract_trades(
                txs,
                mint
            )

            if not trades:
                return

            entry_price = compute_entry(
                trades
            )

            if entry_price <= 0:
                return

            peak_price = compute_peak(
                trades,
                trades[0]["timestamp"]
            )

            peak_multiple = (
                peak_price / entry_price
                if peak_price > 0
                else 0
            )

            hit_2x = (
                peak_multiple >= 2
            )

            async with engine.begin() as conn:

                await conn.execute(text("""
                    UPDATE token_snapshots
                    SET
                        entry_price_sol = :entry,
                        peak_price_sol = :peak,
                        peak_multiplier = :multiplier,
                        hit_2x = :hit_2x,
                        watch_ended_at = :ended
                    WHERE id = :id
                """), {
                    "id": db_id,
                    "entry": entry_price,
                    "peak": peak_price,
                    "multiplier": peak_multiple,
                    "hit_2x": hit_2x,
                    "ended": dt.now(
                        timezone.utc
                    )
                })

            logger.info(
                f"[DONE] "
                f"{mint[:8]} "
                f"entry={entry_price:.12f} "
                f"peak={peak_multiple:.2f}x"
            )

        except Exception as e:
            logger.error(
                f"[ERROR] {mint[:8]} {e}"
            )


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def run_backfill():

    logger.info(
        "[BACKFILL] starting..."
    )

    async with engine.connect() as conn:

        result = await conn.execute(text("""
            SELECT
                id,
                mint
            FROM token_snapshots
            WHERE watch_ended_at IS NULL
        """))

        tokens = [
            dict(row._mapping)
            for row in result
        ]

    logger.info(
        f"[BACKFILL] "
        f"{len(tokens)} tokens"
    )

    semaphore = asyncio.Semaphore(
        CONCURRENCY
    )

    async with aiohttp.ClientSession() as session:

        for i in range(
            0,
            len(tokens),
            BATCH_SIZE
        ):

            batch = tokens[
                i:i+BATCH_SIZE
            ]

            tasks = [
                backfill_token(
                    semaphore,
                    session,
                    token
                )
                for token in batch
            ]

            await asyncio.gather(
                *tasks,
                return_exceptions=True
            )

            await asyncio.sleep(1)

    logger.info(
        "[BACKFILL] complete"
    )


if __name__ == "__main__":
    asyncio.run(run_backfill())
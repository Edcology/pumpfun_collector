import asyncio
import sys
from datetime import datetime, timezone

from database.db import engine, Base
from services.websocket_service import PumpPortalWebsocket
from services.database_service import save_token, token_exists
from services.metadata_service import fetch as fetch_metadata
from services.creator_service import get_creator_age_days
from services.telegram_service import send as telegram
from utils.logger import logger


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

token_queue = asyncio.Queue()
tokens_saved = 0


def compute_name_quality(name: str, symbol: str) -> int:
    name   = (name or "").strip()
    symbol = (symbol or "").strip()
    if len(name) >= 3 and len(symbol) >= 2 and name.isascii():
        return 100
    elif len(name) >= 2:
        return 50
    return 0


async def worker():
    global tokens_saved
    while True:
        raw = await token_queue.get()
        try:
            mint = raw.get("mint")
            if not mint:
                continue

            if await token_exists(mint):
                logger.debug(f"Skipping duplicate: {mint[:8]}")
                continue

            metadata, creator_age = await asyncio.gather(
                fetch_metadata(raw.get("uri")),
                get_creator_age_days(raw.get("creator")),
            )

            token_data = {
                "mint":             mint,
                "name":             raw.get("name"),
                "symbol":           raw.get("symbol"),
                "creator":          raw.get("creator"),
                "uri":              raw.get("uri"),
                "liquidity_sol":    raw.get("liquidity_sol", 0),
                "market_cap_sol":   raw.get("market_cap_sol", 0),
                "creator_age_days": creator_age,
                "has_twitter":      bool(metadata.get("twitter")),
                "has_website":      bool(metadata.get("website")),
                "has_telegram":     bool(metadata.get("telegram")),
                "name_quality":     compute_name_quality(
                                        raw.get("name"),
                                        raw.get("symbol")
                                    ),
                "entry_price_sol":  None,   # backfilled later
                "watch_started_at": datetime.now(timezone.utc),
            }

            db_id = await save_token(token_data)

            if db_id == -1:
                logger.error(f"Failed to save {mint[:8]}")
                continue

            tokens_saved += 1
            logger.info(
                f"[SAVED] {raw.get('symbol', '?'):>6} | "
                f"{mint[:8]} | "
                f"liq={raw.get('liquidity_sol', 0):.3f} SOL | "
                f"id={db_id}"
            )

            # Alert every 100 tokens
            if tokens_saved % 100 == 0:
                await telegram(
                    f"📊 <b>Collector running</b>\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"Tokens saved: <b>{tokens_saved}</b>\n"
                    f"Latest: <b>{raw.get('symbol', '?')}</b> | "
                    f"<code>{mint[:8]}</code>\n"
                    f"Liquidity: {raw.get('liquidity_sol', 0):.3f} SOL"
                )

        except Exception as e:
            logger.error(f"Worker error: {e}")

        finally:
            token_queue.task_done()


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized")


async def main():
    await init_db()

    await telegram(
        f"🟢 <b>Collector started</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"Database initialized\n"
        f"Connecting to PumpPortal..."
    )

    websocket = PumpPortalWebsocket(token_queue, on_connect=telegram)

    workers = [
        asyncio.create_task(worker())
        for _ in range(5)  # increase workers since no price waiting
    ]

    producer = asyncio.create_task(websocket.connect())

    logger.info("Collector running — waiting for new tokens")
    await asyncio.gather(producer, *workers)


if __name__ == "__main__":
    asyncio.run(main())
import asyncio

import aiohttp
from utils.logger import logger


async def get_price_sol(mint: str) -> float | None:
    """
    Fetches current token price in SOL from DexScreener API.
    Returns None on any failure.
    """
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()

                pairs = data.get("pairs")
                if not pairs:
                    return None

                # Take the most liquid pair
                pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0))
                price_usd = pair.get("priceUsd")
                price_native = pair.get("priceNative")  # price in SOL

                return float(price_native) if price_native else None

    except Exception as e:
        logger.debug(f"Price fetch failed [{mint[:8]}]: {e}")
        return None

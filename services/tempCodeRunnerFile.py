import asyncio

import aiohttp
from utils.logger import logger


async def get_price_sol(mint: str) -> float | None:
    """
    Fetches current token price in SOL from PumpPortal REST API.
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
                price = data.get("price")
                return float(price) if price else None

    except Exception as e:
        logger.debug(f"Price fetch failed [{mint[:8]}]: {e}")
        return None
    
print(asyncio.run(get_price_sol("4vtu85AfEPbZ5aW5cCeo7xC7SaGyaSykjaiEfz7fpump")))
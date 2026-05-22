import aiohttp
from utils.logger import logger


async def fetch(uri: str) -> dict:
    """
    Fetches token metadata JSON from URI.
    Returns dict with twitter, website, telegram keys.
    All values default to None if missing or on error.
    """
    empty = {"twitter": None, "website": None, "telegram": None}

    if not uri or not uri.startswith("http"):
        return empty

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                uri,
                timeout=aiohttp.ClientTimeout(total=5),
                ssl=False,
            ) as r:
                if r.status != 200:
                    return empty
                data = await r.json(content_type=None)
                return {
                    "twitter":  data.get("twitter") or data.get("twitter_url"),
                    "website":  data.get("website"),
                    "telegram": data.get("telegram"),
                }
    except Exception as e:
        logger.debug(f"Metadata fetch failed [{uri}]: {e}")
        return empty
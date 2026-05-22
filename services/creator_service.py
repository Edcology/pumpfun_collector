import time
import aiohttp
from config import SOLANA_RPC_URL
from utils.logger import logger


async def get_creator_age_days(wallet: str) -> float:
    """
    Estimates wallet age by finding its oldest transaction.
    Uses free public Solana RPC — no API key needed.
    Returns 0.0 on any failure.
    """
    if not wallet:
        return 0.0

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [
            wallet,
            {"limit": 1000, "commitment": "confirmed"}
        ]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                SOLANA_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                if r.status == 429:
                    logger.debug("RPC rate limited — returning 0 for creator age")
                    return 0.0
                data = await r.json()

        sigs = data.get("result", [])
        if not sigs:
            return 0.0

        oldest_ts = sigs[-1].get("blockTime")
        if not oldest_ts:
            return 0.0

        age_days = (time.time() - oldest_ts) / 86400
        return round(age_days, 1)

    except Exception as e:
        logger.debug(f"Creator age failed [{wallet[:8]}]: {e}")
        return 0.0
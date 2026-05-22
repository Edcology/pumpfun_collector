import aiohttp
from utils.logger import logger
import os

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
BASE_URL  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


async def send(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(BASE_URL, json={
                "chat_id":    CHAT_ID,
                "text":       text,
                "parse_mode": "HTML",
            }, timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.debug(f"Telegram send failed: {e}")
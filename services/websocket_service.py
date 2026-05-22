import asyncio
import json
import websockets

from config import PUMPPORTAL_WS, PUMPPORTAL_API_KEY
from utils.logger import logger


class PumpPortalWebsocket:

    def __init__(self, token_queue: asyncio.Queue, on_connect: callable = None):
        self.uri         = f"{PUMPPORTAL_WS}?api-key={PUMPPORTAL_API_KEY}"
        self.token_queue = token_queue
        self.on_connect  = on_connect
        self._reconnects = 0

    async def connect(self):
        while True:
            try:
                async with websockets.connect(
                    self.uri,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._reconnects += 1
                    logger.info("Connected to PumpPortal WebSocket")

                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    logger.info("Subscribed to new token stream")

                    # Notify Telegram on connect/reconnect
                    if self.on_connect:
                        msg = (
                            "📡 <b>WebSocket connected</b>\n"
                            "Listening for new tokens..."
                            if self._reconnects == 1
                            else
                            f"🔄 <b>WebSocket reconnected</b>\n"
                            f"Reconnect #{self._reconnects}"
                        )
                        asyncio.create_task(self.on_connect(msg))

                    async for raw in ws:
                        try:
                            data = json.loads(raw)

                            if "errors" in data:
                                logger.warning(f"WS error: {data['errors']}")
                                continue

                            if data.get("txType") == "create":
                                await self._on_new_token(data)
                        except json.JSONDecodeError:
                            continue

            except websockets.ConnectionClosed as e:
                logger.warning(f"WS connection closed: {e} — reconnecting in 5s")
                if self.on_connect:
                    asyncio.create_task(self.on_connect(
                        f"⚠️ <b>WebSocket disconnected</b>\n"
                        f"Reconnecting in 5s...\n"
                        f"Reason: {str(e)[:100]}"
                    ))
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"WS error: {e} — reconnecting in 5s")
                await asyncio.sleep(5)

    async def _on_new_token(self, data: dict):
        mint = data.get("mint")
        if not mint:
            return

        await self.token_queue.put({
            "mint":           mint,
            "name":           data.get("name"),
            "symbol":         data.get("symbol"),
            "creator":        data.get("traderPublicKey"),
            "uri":            data.get("uri"),
            "liquidity_sol":  data.get("vSolInBondingCurve", 0) or 0,
            "market_cap_sol": data.get("marketCapSol", 0) or 0,
        })
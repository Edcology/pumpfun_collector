import os
from dotenv import load_dotenv

load_dotenv()

PUMPPORTAL_WS      = "wss://pumpportal.fun/api/data"
PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY", "")
SOLANA_RPC_URL     = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

WATCH_DURATION_HOURS = 24
PRICE_POLL_INTERVAL  = 60       # seconds between price checks
TARGET_MULTIPLIER    = 2.0      # 2x = hit_2x True
MAX_FAILED_FETCHES   = 10       # consecutive failures = rug
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.sql import func
from database.db import Base


class TokenSnapshot(Base):
    __tablename__ = "token_snapshots"

    # --- Identity ---
    id              = Column(Integer, primary_key=True, autoincrement=True)
    mint            = Column(String, unique=True, index=True, nullable=False)
    name            = Column(String)
    symbol          = Column(String)
    creator         = Column(String, index=True)
    uri             = Column(String)

    # --- Features captured at creation (ML inputs) ---
    liquidity_sol   = Column(Float, default=0.0)
    market_cap_sol  = Column(Float, default=0.0)
    creator_age_days= Column(Float, default=0.0)
    has_twitter     = Column(Boolean, default=False)
    has_website     = Column(Boolean, default=False)
    has_telegram    = Column(Boolean, default=False)
    name_quality    = Column(Integer, default=0)

    # --- Outcome (filled by watcher) ---
    entry_price_sol  = Column(Float,   nullable=True)
    peak_price_sol   = Column(Float,   nullable=True)
    peak_multiplier  = Column(Float,   nullable=True)
    hit_2x           = Column(Boolean, nullable=True)
    time_to_2x_secs  = Column(Integer, nullable=True)
    rug_detected     = Column(Boolean, default=False)

    # --- Timing ---
    detected_at     = Column(DateTime, server_default=func.now())
    watch_started_at= Column(DateTime)
    watch_ended_at  = Column(DateTime)
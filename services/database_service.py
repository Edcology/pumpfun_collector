from sqlalchemy import select
from database.db import AsyncSessionLocal
from database.models import TokenSnapshot
from utils.logger import logger


async def save_token(token_data: dict) -> int:
    """
    Inserts a new token snapshot.
    Returns the DB row id on success, -1 on failure.
    """
    async with AsyncSessionLocal() as session:
        try:
            snapshot = TokenSnapshot(**token_data)
            session.add(snapshot)
            await session.commit()
            await session.refresh(snapshot)
            return snapshot.id
        except Exception as e:
            await session.rollback()
            logger.error(f"save_token failed: {e}")
            return -1


async def update_outcome(db_id: int, outcome: dict):
    """
    Writes watcher results back to an existing row.
    """
    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(
                select(TokenSnapshot).where(TokenSnapshot.id == db_id)
            )
            token = result.scalar_one_or_none()
            if not token:
                logger.warning(f"update_outcome: no row found for id={db_id}")
                return
            for key, val in outcome.items():
                setattr(token, key, val)
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"update_outcome failed for id={db_id}: {e}")


async def token_exists(mint: str) -> bool:
    """
    Returns True if this mint is already in the DB.
    Prevents duplicate rows on reconnect.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TokenSnapshot.id).where(TokenSnapshot.mint == mint)
        )
        return result.scalar_one_or_none() is not None
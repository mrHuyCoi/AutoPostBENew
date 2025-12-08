"""
Script to make color_id nullable in user_devices table
"""
import asyncio
import sys
import os

# Add the parent directory to the path so we can import from app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.database import get_async_session


async def make_color_id_nullable():
    """
    Make color_id column nullable in user_devices table
    """
    async for session in get_async_session():
        try:
            # Make color_id nullable
            await session.execute(text("""
                ALTER TABLE user_devices 
                ALTER COLUMN color_id DROP NOT NULL;
            """))
            
            await session.commit()
            print("Successfully made color_id nullable in user_devices table")
            
        except Exception as e:
            await session.rollback()
            print(f"Error making color_id nullable: {e}")
            raise
        finally:
            await session.close()
            break


if __name__ == "__main__":
    asyncio.run(make_color_id_nullable())

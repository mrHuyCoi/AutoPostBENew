"""
Migration script to add applied_conditions column to services table
"""
import asyncio
import sys
import os

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.database import get_async_session

async def add_applied_conditions_column():
    """Add applied_conditions column to services table"""
    async for session in get_async_session():
        try:
            # Check if column already exists
            check_column_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'services' 
                AND column_name = 'applied_conditions'
            """)
            
            result = await session.execute(check_column_query)
            existing_column = result.fetchone()
            
            if existing_column:
                print("Column 'applied_conditions' already exists in services table")
                return
            
            # Add the new column
            alter_table_query = text("""
                ALTER TABLE services 
                ADD COLUMN applied_conditions TEXT[]
            """)
            
            await session.execute(alter_table_query)
            await session.commit()
            
            print("Successfully added 'applied_conditions' column to services table")
            
        except Exception as e:
            print(f"Error adding applied_conditions column: {e}")
            await session.rollback()
            raise
        finally:
            await session.close()
            break

if __name__ == "__main__":
    asyncio.run(add_applied_conditions_column())

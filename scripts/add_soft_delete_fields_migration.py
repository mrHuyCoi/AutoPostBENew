"""
Migration script to add soft delete fields (trashed_at, purge_after) 
to brands, services, user_devices, and product_components tables
"""
import asyncio
import asyncpg
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def run_migration():
    """Add soft delete fields to the specified tables"""
    
    # Database connection parameters
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:root@localhost:5432/dangbaitudongnew')
    
    # Parse the database URL to get connection parameters
    if DATABASE_URL.startswith('postgresql+psycopg2://'):
        DATABASE_URL = DATABASE_URL.replace('postgresql+psycopg2://', 'postgresql://')
    elif DATABASE_URL.startswith('postgresql+asyncpg://'):
        DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://')
    
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # SQL statements to add soft delete fields
        migrations = [
            # Add fields to brands table
            """
            ALTER TABLE brands 
            ADD COLUMN IF NOT EXISTS trashed_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS purge_after TIMESTAMP WITH TIME ZONE;
            """,
            
            # Add fields to services table
            """
            ALTER TABLE services 
            ADD COLUMN IF NOT EXISTS trashed_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS purge_after TIMESTAMP WITH TIME ZONE;
            """,
            
            # Add fields to user_devices table
            """
            ALTER TABLE user_devices 
            ADD COLUMN IF NOT EXISTS trashed_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS purge_after TIMESTAMP WITH TIME ZONE;
            """,
            
            # Add fields to product_components table
            """
            ALTER TABLE product_components 
            ADD COLUMN IF NOT EXISTS trashed_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS purge_after TIMESTAMP WITH TIME ZONE;
            """
        ]
        
        # Execute each migration
        for i, migration in enumerate(migrations, 1):
            print(f"Executing migration {i}/4...")
            await conn.execute(migration)
            print(f"Migration {i} completed successfully")
        
        print("All soft delete field migrations completed successfully!")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        raise
    finally:
        await conn.close()

if __name__ == "__main__":
    print("Starting soft delete fields migration...")
    asyncio.run(run_migration())

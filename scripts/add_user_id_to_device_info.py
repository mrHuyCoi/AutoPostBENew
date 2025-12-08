import asyncio
import sys
import os

# Thêm thư mục gốc của dự án vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.database import engine, get_db

async def add_user_id_column():
    async with engine.begin() as conn:
        # Thêm cột user_id vào bảng device_info nếu chưa tồn tại
        await conn.execute(
            text("ALTER TABLE device_info ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id)")
        )
        print("Đã thêm cột user_id vào bảng device_info")

if __name__ == "__main__":
    asyncio.run(add_user_id_column())
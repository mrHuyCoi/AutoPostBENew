import sys
import os
import asyncio
from sqlalchemy import text

# Thêm thư mục gốc vào đường dẫn để import các module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.database.database import engine

async def check_tables():
    """
    Kiểm tra cấu trúc các bảng trong database
    """
    async with engine.connect() as conn:
        # Kiểm tra bảng users
        print("\n=== BẢNG USERS ===")
        try:
            # Sử dụng truy vấn SQL trực tiếp để lấy thông tin cột trong bảng users
            result = await conn.execute(
                text("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'users'
                ORDER BY ordinal_position
                """)
            )
            for row in result:
                print(f"- {row[0]}: {row[1]}, nullable={'YES' if row[2] == 'YES' else 'NO'}")
        except Exception as e:
            print(f"Lỗi khi kiểm tra bảng users: {e}")
        
        # Kiểm tra bảng media_assets
        print("\n=== BẢNG MEDIA_ASSETS ===")
        try:
            # Sử dụng truy vấn SQL trực tiếp để lấy thông tin cột trong bảng media_assets
            result = await conn.execute(
                text("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'media_assets'
                ORDER BY ordinal_position
                """)
            )
            for row in result:
                print(f"- {row[0]}: {row[1]}, nullable={'YES' if row[2] == 'YES' else 'NO'}")
        except Exception as e:
            print(f"Lỗi khi kiểm tra bảng media_assets: {e}")

if __name__ == "__main__":
    asyncio.run(check_tables()) 
import sys
import os
import asyncio
from sqlalchemy import text

# Thêm thư mục gốc vào đường dẫn để import các module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.database.database import engine

async def manual_migration():
    """
    Thực hiện migration thủ công để thêm các cột mới
    """
    async with engine.begin() as conn:
        print("Bắt đầu migration thủ công...")

        # Thêm cột custom_system_prompt vào bảng users nếu chưa có
        try:
            print("Kiểm tra cột custom_system_prompt trong bảng users...")
            result = await conn.execute(
                text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'custom_system_prompt'
                """)
            )
            
            if result.rowcount == 0:
                print("Thêm cột custom_system_prompt vào bảng users...")
                await conn.execute(
                    text("""
                    ALTER TABLE users 
                    ADD COLUMN custom_system_prompt TEXT
                    """)
                )
                print("Đã thêm thành công!")
            else:
                print("Cột custom_system_prompt đã tồn tại trong bảng users.")
        except Exception as e:
            print(f"Lỗi khi thêm cột custom_system_prompt: {e}")

        # Thêm cột brand_name vào bảng media_assets nếu chưa có
        try:
            print("Kiểm tra cột brand_name trong bảng media_assets...")
            result = await conn.execute(
                text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'media_assets' AND column_name = 'brand_name'
                """)
            )
            
            if result.rowcount == 0:
                print("Thêm cột brand_name vào bảng media_assets...")
                await conn.execute(
                    text("""
                    ALTER TABLE media_assets 
                    ADD COLUMN brand_name VARCHAR
                    """)
                )
                print("Đã thêm thành công!")
            else:
                print("Cột brand_name đã tồn tại trong bảng media_assets.")
        except Exception as e:
            print(f"Lỗi khi thêm cột brand_name: {e}")

        # Thêm cột posting_purpose vào bảng media_assets nếu chưa có
        try:
            print("Kiểm tra cột posting_purpose trong bảng media_assets...")
            result = await conn.execute(
                text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'media_assets' AND column_name = 'posting_purpose'
                """)
            )
            
            if result.rowcount == 0:
                print("Thêm cột posting_purpose vào bảng media_assets...")
                await conn.execute(
                    text("""
                    ALTER TABLE media_assets 
                    ADD COLUMN posting_purpose TEXT
                    """)
                )
                print("Đã thêm thành công!")
            else:
                print("Cột posting_purpose đã tồn tại trong bảng media_assets.")
        except Exception as e:
            print(f"Lỗi khi thêm cột posting_purpose: {e}")

        print("Hoàn thành migration thủ công.")

if __name__ == "__main__":
    asyncio.run(manual_migration()) 
#!/usr/bin/env python3
"""
Script để thêm timestamps vào bảng chatbot_plans và chatbot_services
"""

import asyncio
import asyncpg
from datetime import datetime

async def add_timestamps_to_chatbot_tables():
    # Kết nối database
    conn = await asyncpg.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="your_password",  # Thay đổi password
        database="dangbaitudong"
    )
    
    try:
        # Thêm cột created_at và updated_at vào bảng chatbot_plans
        await conn.execute("""
            ALTER TABLE chatbot_plans 
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """)
        
        # Thêm cột created_at và updated_at vào bảng chatbot_services
        await conn.execute("""
            ALTER TABLE chatbot_services 
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """)
        
        # Cập nhật giá trị cho các bản ghi hiện có
        await conn.execute("""
            UPDATE chatbot_plans 
            SET created_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP 
            WHERE created_at IS NULL
        """)
        
        await conn.execute("""
            UPDATE chatbot_services 
            SET created_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP 
            WHERE created_at IS NULL
        """)
        
        print("✅ Đã thêm timestamps thành công!")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        await conn.rollback()
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(add_timestamps_to_chatbot_tables()) 
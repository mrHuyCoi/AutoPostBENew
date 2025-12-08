#!/usr/bin/env python3
"""
Script để thêm trường status vào bảng user_chatbot_subscriptions
"""

import asyncio
import sys
import os

# Thêm đường dẫn để import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.database import engine

async def add_status_column():
    """Thêm trường status vào bảng user_chatbot_subscriptions"""
    
    async with engine.begin() as conn:
        try:
            # Kiểm tra xem trường status đã tồn tại chưa
            check_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'user_chatbot_subscriptions' 
                AND column_name = 'status'
            """)
            
            result = await conn.execute(check_query)
            if result.fetchone():
                print("Trường 'status' đã tồn tại trong bảng user_chatbot_subscriptions")
                return
            
            # Thêm trường status
            add_status_query = text("""
                ALTER TABLE user_chatbot_subscriptions 
                ADD COLUMN status VARCHAR DEFAULT 'pending' NOT NULL
            """)
            
            await conn.execute(add_status_query)
            
            # Cập nhật các bản ghi hiện có
            update_existing_query = text("""
                UPDATE user_chatbot_subscriptions 
                SET status = CASE 
                    WHEN is_active = true THEN 'approved'
                    ELSE 'pending'
                END
            """)
            
            await conn.execute(update_existing_query)
            
            print("Đã thêm trường 'status' thành công!")
            print("Các bản ghi hiện có đã được cập nhật:")
            print("- is_active = true -> status = 'approved'")
            print("- is_active = false -> status = 'pending'")
            
        except Exception as e:
            print(f"Lỗi khi thêm trường status: {e}")
            raise

if __name__ == "__main__":
    print("Bắt đầu thêm trường status vào bảng user_chatbot_subscriptions...")
    asyncio.run(add_status_column())
    print("Hoàn thành!") 
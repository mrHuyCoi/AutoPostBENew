import asyncio
import sys
import os

# Thêm thư mục gốc vào đường dẫn để import các module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.database.database import engine
from app.models.user import Base
from app.models.media_asset import MediaAsset
from app.models.user import User
# Nhập thêm các model khác nếu cần

async def create_tables():
    """
    Tạo hoặc cập nhật tất cả các bảng trong database
    """
    async with engine.begin() as conn:
        # Tạo tất cả các bảng nếu chưa tồn tại
        await conn.run_sync(Base.metadata.create_all)
        print("Đã tạo/cập nhật các bảng trong database")

if __name__ == "__main__":
    asyncio.run(create_tables()) 
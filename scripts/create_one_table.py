#!/usr/bin/env python
import asyncio
import sys
import os

# Thêm thư mục gốc của dự án vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.ext.asyncio import create_async_engine
from app.database.database import Base
from app.configs.settings import settings

# ✅ Chỉ import YouTubeMetadata
from app.models.youtube_metadata import YouTubeMetadata

async def reset_youtube_metadata_table():
    print(f"Kết nối đến database: {settings.DATABASE_URL}")

    engine = create_async_engine(settings.DATABASE_URL, echo=True)

    async with engine.begin() as conn:
        # ❌ Xoá bảng nếu tồn tại
        print("🔁 Đang xoá bảng youtube_metadata nếu đã có...")
        await conn.run_sync(lambda sync_conn: YouTubeMetadata.__table__.drop(bind=sync_conn, checkfirst=True))

        # ✅ Tạo lại bảng
        print("🛠  Đang tạo lại bảng youtube_metadata...")
        await conn.run_sync(lambda sync_conn: YouTubeMetadata.__table__.create(bind=sync_conn, checkfirst=False))

    print("✅ Đã xoá và tạo lại bảng youtube_metadata thành công.")

    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(reset_youtube_metadata_table())

import sys
import os

# ✅ Đặt lên đầu để load được module `app`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
import uuid
from datetime import datetime

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database.database import engine
from app.models.device_info import DeviceInfo
from app.models.color import Color
from app.models.device_color import DeviceColor
from app.models.device_storage import DeviceStorage


async def import_colors_and_storage():
    # Đọc dữ liệu từ sheet DROPDOWN
    file_path = os.path.join(os.path.dirname(__file__), '..', 'iPhone.xlsm')
    df = pd.read_excel(file_path, sheet_name="DROPDOWN")

    df = df[["Model", "Mau sac", "Dung luong"]].dropna().drop_duplicates()

    async with AsyncSession(engine) as session:
        async with session.begin():
            for _, row in df.iterrows():
                model = row["Model"].strip()
                color_name = row["Mau sac"].strip()
                capacity_text = str(row["Dung luong"]).strip().upper()

                # ✅ Parse dung lượng về dạng số (VD: "64GB" → 64)
                capacity = None
                try:
                    if "TB" in capacity_text:
                        capacity = int(capacity_text.replace("TB", "").strip()) * 1024  # 1TB = 1024GB
                    elif "GB" in capacity_text:
                        capacity = int(capacity_text.replace("GB", "").strip())
                except ValueError:
                    print(f"❌ Không parse được dung lượng: {capacity_text}")
                    continue

                if capacity is None:
                    print(f"⚠️ Bỏ qua dung lượng không chuẩn: {capacity_text}")
                    continue

                # Tìm thiết bị
                result = await session.execute(
                    select(DeviceInfo).where(DeviceInfo.model == model)
                )
                device = result.scalars().first()
                if not device:
                    print(f"⚠️ Không tìm thấy model: {model}")
                    continue

                # =======================
                # 1. Thêm COLOR
                # =======================
                result = await session.execute(
                    select(Color).where(Color.name == color_name)
                )
                color = result.scalars().first()
                if not color:
                    color = Color(
                        id=uuid.uuid4(),
                        name=color_name,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    session.add(color)
                    await session.flush()

                # Liên kết màu với máy nếu chưa có
                result = await session.execute(
                    select(DeviceColor).where(
                        DeviceColor.device_info_id == device.id,
                        DeviceColor.color_id == color.id
                    )
                )
                if not result.scalars().first():
                    session.add(DeviceColor(
                        id=uuid.uuid4(),
                        device_info_id=device.id,
                        color_id=color.id,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    ))

                # =======================
                # 2. Thêm STORAGE
                # =======================
                result = await session.execute(
                    select(DeviceStorage).where(
                        DeviceStorage.device_info_id == device.id,
                        DeviceStorage.capacity == capacity
                    )
                )
                if not result.scalars().first():
                    session.add(DeviceStorage(
                        id=uuid.uuid4(),
                        device_info_id=device.id,
                        capacity=capacity,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    ))

        await session.commit()
        print("✅ Đã ánh xạ xong màu sắc và dung lượng cho thiết bị!")


async def main():
    await import_colors_and_storage()


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

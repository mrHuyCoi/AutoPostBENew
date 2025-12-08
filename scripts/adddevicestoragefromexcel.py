#!/usr/bin/env python
import asyncio
import sys
import os
import pandas as pd
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

# Thêm thư mục gốc của dự án vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.database import get_db, engine
from app.models.device_info import DeviceInfo
from app.models.device_storage import DeviceStorage
from sqlalchemy.orm import sessionmaker


async def add_device_storage_from_excel(file_path: str):
    """
    Thêm dung lượng thiết bị từ file Excel.
    
    Args:
        file_path: Đường dẫn đến file Excel
    """
    print(f"Đọc dữ liệu từ file: {file_path}")
    
    # Đọc file Excel
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Lỗi khi đọc file Excel: {str(e)}")
        return
    
    # Kiểm tra các cột cần thiết
    required_columns = ["model", "storage"]
    for col in required_columns:
        if col not in df.columns:
            print(f"Thiếu cột {col} trong file Excel")
            return
    
    # Tạo session
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    # Thêm dung lượng cho từng thiết bị
    async with async_session() as session:
        try:
            # Lấy tất cả các thiết bị hiện có
            result = await session.execute(select(DeviceInfo))
            device_infos = {device.model: device for device in result.scalars().all()}
            
            # Thêm dung lượng cho từng thiết bị
            for _, row in df.iterrows():
                model = row["model"]
                storage_str = str(row["storage"])
                
                # Chuyển đổi storage_str thành số nguyên (GB)
                try:
                    # Xử lý các định dạng như "128GB", "128 GB", "128"
                    storage_str = storage_str.upper().replace(" ", "").replace("GB", "")
                    capacity = int(storage_str)
                except ValueError:
                    print(f"Không thể chuyển đổi dung lượng '{storage_str}' thành số nguyên cho model '{model}'")
                    continue
                
                # Kiểm tra xem thiết bị có tồn tại không
                if model in device_infos:
                    device_info = device_infos[model]
                    
                    # Kiểm tra xem dung lượng đã tồn tại cho thiết bị này chưa
                    result = await session.execute(
                        select(DeviceStorage).where(
                            DeviceStorage.device_info_id == device_info.id,
                            DeviceStorage.capacity == capacity
                        )
                    )
                    existing_storage = result.scalars().first()
                    
                    if not existing_storage:
                        # Tạo dung lượng mới cho thiết bị
                        new_storage = DeviceStorage(
                            id=uuid.uuid4(),
                            device_info_id=device_info.id,
                            capacity=capacity
                        )
                        session.add(new_storage)
                        print(f"Đã thêm dung lượng {capacity}GB cho model '{model}'")
                    else:
                        print(f"Dung lượng {capacity}GB đã tồn tại cho model '{model}'")
                else:
                    print(f"Không tìm thấy thiết bị với model '{model}'")
            
            # Lưu các thay đổi
            await session.commit()
            print("Đã hoàn thành việc thêm dung lượng thiết bị từ file Excel!")
            
        except Exception as e:
            await session.rollback()
            print(f"Lỗi khi thêm dung lượng thiết bị: {str(e)}")


async def main():
    # Đường dẫn đến file Excel
    file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "iPhone.xlsm")
    
    # Thêm dung lượng thiết bị từ file Excel
    await add_device_storage_from_excel(file_path)


if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import uuid

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.database import get_db, engine
from app.models.device_info import DeviceInfo
from app.models.device_storage import DeviceStorage
from app.models.storage import Storage
from app.models.user_device import UserDevice
from app.dto.device_storage_dto import DeviceStorageCreate
from app.repositories.device_storage_repository import DeviceStorageRepository


async def migrate_storage_to_device_storage():
    """
    Chuyển đổi từ mô hình Storage sang DeviceStorage.
    
    1. Tạo bảng device_storage mới
    2. Chuyển dữ liệu từ bảng storage sang device_storage
    3. Cập nhật bảng user_devices để sử dụng device_storage_id thay vì storage_id
    """
    print("Bắt đầu quá trình chuyển đổi từ Storage sang DeviceStorage...")
    
    # Tạo session
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    async with async_session() as session:
        try:
            # 1. Lấy tất cả các storage hiện có
            query = text("SELECT id, capacity FROM storage")
            result = await session.execute(query)
            storages = result.fetchall()
            print(f"Đã tìm thấy {len(storages)} storage.")
            
            # 2. Lấy tất cả các user_device để biết thiết bị nào sử dụng storage nào
            query = text("SELECT id, device_info_id, storage_id FROM user_devices")
            result = await session.execute(query)
            user_devices = result.fetchall()
            print(f"Đã tìm thấy {len(user_devices)} user_device.")
            
            # 3. Tạo mapping từ storage_id sang device_info_id
            storage_to_device_info = {}
            for user_device in user_devices:
                device_info_id = user_device.device_info_id
                storage_id = user_device.storage_id
                if storage_id not in storage_to_device_info:
                    storage_to_device_info[storage_id] = []
                if device_info_id not in storage_to_device_info[storage_id]:
                    storage_to_device_info[storage_id].append(device_info_id)
            
            # 4. Tạo các device_storage mới
            device_storage_mapping = {}  # Mapping từ (device_info_id, capacity) sang device_storage_id
            for storage in storages:
                storage_id = storage.id
                capacity = storage.capacity
                
                # Nếu storage này được sử dụng bởi ít nhất một thiết bị
                if storage_id in storage_to_device_info:
                    for device_info_id in storage_to_device_info[storage_id]:
                        # Kiểm tra xem đã tạo device_storage cho (device_info_id, capacity) này chưa
                        key = (str(device_info_id), capacity)
                        if key not in device_storage_mapping:
                            # Tạo device_storage mới
                            query = text(f"""
                                INSERT INTO device_storage (id, device_info_id, capacity, created_at, updated_at)
                                VALUES (:id, :device_info_id, :capacity, NOW(), NOW())
                                RETURNING id
                            """)
                            new_id = uuid.uuid4()
                            params = {
                                "id": new_id,
                                "device_info_id": device_info_id,
                                "capacity": capacity
                            }
                            result = await session.execute(query, params)
                            device_storage_id = result.scalar()
                            device_storage_mapping[key] = new_id
                            print(f"Đã tạo device_storage mới: device_info_id={device_info_id}, capacity={capacity}, id={new_id}")
            
            # 5. Cập nhật bảng user_devices để sử dụng device_storage_id thay vì storage_id
            for user_device in user_devices:
                user_device_id = user_device.id
                device_info_id = user_device.device_info_id
                storage_id = user_device.storage_id
                
                # Lấy capacity của storage
                query = text("SELECT capacity FROM storage WHERE id = :storage_id")
                result = await session.execute(query, {"storage_id": storage_id})
                storage_capacity = result.scalar()
                
                # Lấy device_storage_id tương ứng
                key = (str(device_info_id), storage_capacity)
                if key in device_storage_mapping:
                    device_storage_id = device_storage_mapping[key]
                    
                    # Cập nhật user_device
                    query = text("""
                        ALTER TABLE user_devices 
                        ADD COLUMN IF NOT EXISTS device_storage_id UUID REFERENCES device_storage(id)
                    """)
                    await session.execute(query)
                    
                    query = text("""
                        UPDATE user_devices 
                        SET device_storage_id = :device_storage_id 
                        WHERE id = :user_device_id
                    """)
                    await session.execute(query, {
                        "device_storage_id": device_storage_id,
                        "user_device_id": user_device_id
                    })
                    print(f"Đã cập nhật user_device {user_device_id} với device_storage_id={device_storage_id}")
            
            # 6. Xóa cột storage_id khỏi bảng user_devices (chỉ sau khi đã cập nhật tất cả)
            # Kiểm tra xem tất cả các user_device đã được cập nhật chưa
            query = text("SELECT COUNT(*) FROM user_devices WHERE device_storage_id IS NULL")
            result = await session.execute(query)
            null_count = result.scalar()
            
            if null_count == 0:
                # Xóa ràng buộc khóa ngoại trước
                query = text("""
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conname = 'user_devices_storage_id_fkey'
                        ) THEN
                            ALTER TABLE user_devices DROP CONSTRAINT user_devices_storage_id_fkey;
                        END IF;
                    END $$;
                """)
                await session.execute(query)
                
                # Xóa cột storage_id
                query = text("ALTER TABLE user_devices DROP COLUMN IF EXISTS storage_id")
                await session.execute(query)
                print("Đã xóa cột storage_id khỏi bảng user_devices")
            else:
                print(f"CẢNH BÁO: Còn {null_count} user_device chưa được cập nhật device_storage_id")
            
            # Commit các thay đổi
            await session.commit()
            print("Đã hoàn thành quá trình chuyển đổi từ Storage sang DeviceStorage!")
            
        except Exception as e:
            await session.rollback()
            print(f"Lỗi trong quá trình chuyển đổi: {str(e)}")
            raise


async def main():
    await migrate_storage_to_device_storage()


if __name__ == "__main__":
    asyncio.run(main())
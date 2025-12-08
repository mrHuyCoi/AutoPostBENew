from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.dialects.postgresql import UUID
from app.database.database import Base

# Bảng trung gian cho quan hệ nhiều-nhiều giữa DeviceInfo và Material
device_material_association = Table(
    'device_material_association',
    Base.metadata,
    Column('device_info_id', UUID(as_uuid=True), ForeignKey('device_info.id'), primary_key=True),
    Column('material_id', UUID(as_uuid=True), ForeignKey('materials.id'), primary_key=True)
) 
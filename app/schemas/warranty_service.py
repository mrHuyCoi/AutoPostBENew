from typing import Optional
from uuid import UUID
from pydantic import BaseModel
from datetime import datetime


class WarrantyServiceBase(BaseModel):
    value: str


class WarrantyServiceCreate(WarrantyServiceBase):
    pass


class WarrantyServiceUpdate(WarrantyServiceBase):
    value: Optional[str] = None


class WarrantyServiceInDBBase(WarrantyServiceBase):
    id: UUID
    user_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class WarrantyService(WarrantyServiceInDBBase):
    pass 
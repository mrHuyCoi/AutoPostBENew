import uuid
from typing import Optional
from pydantic import BaseModel, HttpUrl


class UserSyncUrlCreate(BaseModel):
    url: str
    is_active: bool = True
    type_url: Optional[str] = None  # 'device' | 'component' | 'service'
    url_today: Optional[str] = None


class UserSyncUrlUpdate(BaseModel):
    url: Optional[str] = None
    is_active: Optional[bool] = None
    type_url: Optional[str] = None
    url_today: Optional[str] = None


class UserSyncUrlRead(BaseModel):
    user_id: uuid.UUID
    url: str
    is_active: bool
    type_url: Optional[str] = None
    url_today: Optional[str] = None

    class Config:
        from_attributes = True

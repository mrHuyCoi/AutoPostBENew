from datetime import datetime
from typing import List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, validator


class PlatformScheduleItem(BaseModel):
    """Item mô tả lịch đăng cho một tài khoản nền tảng."""
    social_account_id: UUID
    platform: str  # facebook, instagram, ... (redundant but convenient)
    scheduled_at: datetime
    platform_specific_data: Optional[dict] = None


class PostScheduleRequest(BaseModel):
    """Request body cho endpoint lập lịch đăng bài."""
    items: List[PlatformScheduleItem]


class MediaAssetResponse(BaseModel):
    id: UUID
    user_id: UUID
    storage_path: str
    url: Optional[Union[str, List[str]]] = None
    file_name: str
    file_type: str
    duration: Optional[int] = None
    brand_name: Optional[str] = None
    posting_purpose: Optional[str] = None
    uploaded_at: datetime
    updated_at: datetime
    prompt_for_content: Optional[str] = None

    @validator('url', pre=True)
    def validate_url(cls, v):
        if v is None:
            return None
        if isinstance(v, (str, list)):
            return v
        # If it's a dict or other type, return None
        return None

    class Config:
        from_attributes = True


class YouTubeMetadataResponse(BaseModel):
    platform_post_id: UUID
    content_type: str
    title: str
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    privacy_status: str
    shorts_hashtags: Optional[List[str]] = None
    shorts_music: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PlatformPostResponse(BaseModel):
    id: UUID
    user_id: UUID
    social_account_id: UUID
    platform: str
    status: str
    scheduled_at: Optional[datetime] = None
    generated_content: Optional[str] = None
    platform_type: Optional[str] = None
    post_url: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    media_assets: List[MediaAssetResponse] = []
    youtube_metadata: Optional[YouTubeMetadataResponse] = None

    class Config:
        from_attributes = True

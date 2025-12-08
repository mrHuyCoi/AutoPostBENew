from pydantic import BaseModel, Field, validator
from typing import Optional, List
import uuid
from app.configs.settings import settings

class YouTubeAuthRequest(BaseModel):
    """
    DTO cho request tạo URL xác thực YouTube OAuth2.
    """
    user_id: uuid.UUID = Field(..., description="ID của người dùng trong hệ thống")
    redirect_uri: Optional[str] = Field(None, description="Custom redirect URI (tùy chọn)")
    
    class Config:
        json_encoders = {
            uuid.UUID: str
        }
        json_schema_extra = {
            "example": {
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "redirect_uri": "{{SERVER_HOST}}/api/v1/youtube/oauth/callback"
            }
        }


class YouTubeCallbackRequest(BaseModel):
    """
    DTO cho request xử lý OAuth callback từ YouTube.
    """
    state: str = Field(..., description="State parameter để verify request")
    code: str = Field(..., description="Authorization code từ YouTube OAuth callback")
    # error: Optional[str] = Field(None, description="Error code nếu OAuth thất bại")
    # error_description: Optional[str] = Field(None, description="Mô tả lỗi nếu OAuth thất bại")
    
    @validator('code')
    def validate_code(cls, v, values):
        if not values.get('error') and not v:
            raise ValueError('Authorization code là bắt buộc khi không có lỗi')
        return v
    
    @validator('state')
    def validate_state(cls, v):
        if not v or len(v) < 10:
            raise ValueError('State parameter không hợp lệ')
        return v
    
    # class Config:
    #     schema_extra = {
    #         "example": {
    #             "code": "4/0AX4XfWjYxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    #             "state": "random_state_string_for_security",
    #             "error": None,
    #             "error_description": None
    #         }
    #     }

class YouTubeVideoUploadRequest(BaseModel):
    """
    DTO cho request upload video lên YouTube.
    """
    account_id: uuid.UUID = Field(..., description="ID của tài khoản YouTube trong hệ thống")
    title: str = Field(..., min_length=1, max_length=100, description="Tiêu đề video")
    description: Optional[str] = Field(None, max_length=5000, description="Mô tả video")
    tags: Optional[List[str]] = Field(None, description="Danh sách tags cho video")
    privacy_status: str = Field(default="private", description="Trạng thái riêng tư của video")
    category_id: Optional[str] = Field("22", description="Category ID của video (mặc định: People & Blogs)")
    thumbnail_url: Optional[str] = Field(None, description="URL thumbnail tùy chỉnh")
    scheduled_publish_time: Optional[str] = Field(None, description="Thời gian xuất bản theo lịch (ISO format)")
    
    @validator('privacy_status')
    def validate_privacy_status(cls, v):
        allowed_statuses = ['private', 'public', 'unlisted']
        if v not in allowed_statuses:
            raise ValueError(f'privacy_status phải là một trong: {", ".join(allowed_statuses)}')
        return v
    
    @validator('tags')
    def validate_tags(cls, v):
        if v is not None:
            if len(v) > 30:
                raise ValueError('Số lượng tags không được vượt quá 30')
            for tag in v:
                if len(tag) > 30:
                    raise ValueError('Mỗi tag không được vượt quá 30 ký tự')
        return v
    
    @validator('title')
    def validate_title(cls, v):
        if not v.strip():
            raise ValueError('Tiêu đề video không được để trống')
        return v.strip()
    
    class Config:
        json_encoders = {
            uuid.UUID: str
        }
        json_schema_extra = {
            "example": {
                "account_id": "123e4567-e89b-12d3-a456-426614174000",
                "title": "Video Demo Tuyệt Vời",
                "description": "Đây là video demo cho ứng dụng của chúng tôi. Video này sẽ hướng dẫn cách sử dụng các tính năng cơ bản.",
                "tags": ["demo", "tutorial", "hướng dẫn", "ứng dụng"],
                "privacy_status": "public",
                "category_id": "22",
                "thumbnail_url": "https://example.com/thumbnail.jpg",
                "scheduled_publish_time": "2024-12-25T10:00:00Z"
            }
        }



class YouTubeVideoUploadResponse(BaseModel):
    """
    DTO cho response sau khi upload video thành công.
    """
    video_id: str = Field(..., description="ID của video trên YouTube")
    video_url: str = Field(..., description="URL của video trên YouTube")
    title: str = Field(..., description="Tiêu đề video")
    description: Optional[str] = Field(None, description="Mô tả video")
    privacy_status: str = Field(..., description="Trạng thái riêng tư của video")
    channel_name: str = Field(..., description="Tên kênh YouTube")
    channel_id: str = Field(..., description="ID kênh YouTube")
    upload_status: str = Field(..., description="Trạng thái upload")
    thumbnail_url: Optional[str] = Field(None, description="URL thumbnail của video")
    duration: Optional[str] = Field(None, description="Thời lượng video")
    upload_time: str = Field(..., description="Thời gian upload")
    
    class Config:
        json_schema_extra = {
            "example": {
                "video_id": "dQw4w9WgXcQ",
                "video_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                "title": "Video Demo Tuyệt Vời",
                "description": "Đây là video demo cho ứng dụng của chúng tôi",
                "privacy_status": "public",
                "channel_name": "Kênh Demo",
                "channel_id": "UCxxxxxxxxxxxxxxxxxxxxxxx",
                "upload_status": "processed",
                "thumbnail_url": "https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg",
                "duration": "PT3M42S",
                "upload_time": "2024-12-20T14:30:00Z"
            }
        }
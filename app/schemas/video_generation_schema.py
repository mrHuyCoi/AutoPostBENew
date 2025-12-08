from typing import List, Optional
from pydantic import BaseModel, Field


class VideoMaterial(BaseModel):
    """Thông tin về nguồn video material"""
    provider: str = Field(..., description="Nhà cung cấp material (ví dụ: pexels, pixabay)")
    url: str = Field(default="", description="URL của material")
    duration: float = Field(default=0, ge=0, description="Độ dài material (giây)")


class VideoGenerationRequest(BaseModel):
    """Request schema cho API tạo video bằng AI"""
    # Thông tin cơ bản về video
    video_subject: str = Field(..., description="Chủ đề của video")
    video_script: str = Field(..., description="Kịch bản video")
    video_terms: Optional[List[str]] = Field(default=None, description="Từ khóa video")
    video_aspect: str = Field(default="9:16", description="Tỷ lệ khung hình (ví dụ: 9:16, 16:9, 1:1)")
    video_concat_mode: str = Field(default="random", description="Chế độ ghép video (random, sequential, etc.)")
    video_transition_mode: Optional[str] = Field(default=None, description="Chế độ chuyển cảnh")
    video_clip_duration: Optional[float] = Field(default=None, ge=0, description="Độ dài mỗi clip (giây)")
    video_count: int = Field(default=1, ge=1, description="Số lượng video cần tạo")
    video_source: str = Field(..., description="Nguồn video (pexels, pixabay, etc.)")
    video_materials: List[VideoMaterial] = Field(default_factory=list, description="Danh sách materials")
    
    # Ngôn ngữ và giọng nói
    video_language: str = Field(default="Vietnamese", description="Ngôn ngữ video")
    voice_name: Optional[str] = Field(default=None, description="Tên giọng nói TTS")
    voice_volume: Optional[float] = Field(default=1.0, ge=0, le=1, description="Âm lượng giọng nói (0-1)")
    tts_server: Optional[str] = Field(default=None, description="Server TTS sử dụng")
    voice_rate: Optional[float] = Field(default=1.0, ge=0.5, le=2.0, description="Tốc độ giọng nói")
    
    # Nhạc nền
    bgm_type: str = Field(default="random", description="Loại nhạc nền (random, specific, etc.)")
    bgm_file: Optional[str] = Field(default="", description="File nhạc nền (nếu có)")
    bgm_volume: Optional[float] = Field(default=0.5, ge=0, le=1, description="Âm lượng nhạc nền (0-1)")
    
    # Phụ đề
    subtitle_enabled: bool = Field(default=False, description="Bật/tắt phụ đề")
    type_subtitle: Optional[str] = Field(default=None, description="Loại phụ đề")
    subtitle_provider: Optional[str] = Field(default=None, description="Nhà cung cấp phụ đề")
    subtitle_position: Optional[str] = Field(default=None, description="Vị trí phụ đề")
    custom_position: Optional[float] = Field(default=None, description="Vị trí tùy chỉnh")
    font_name: Optional[str] = Field(default=None, description="Tên font chữ")
    text_fore_color: Optional[str] = Field(default="#FFFFFF", description="Màu chữ phụ đề")
    text_background_color: bool = Field(default=True, description="Có nền cho chữ không")
    font_size: Optional[int] = Field(default=24, ge=8, le=72, description="Kích thước font")
    stroke_color: Optional[str] = Field(default="#000000", description="Màu viền chữ")
    stroke_width: Optional[int] = Field(default=2, ge=0, le=10, description="Độ dày viền chữ")
    
    # Cấu hình kỹ thuật
    n_threads: int = Field(default=6, ge=1, le=16, description="Số luồng xử lý")
    paragraph_number: int = Field(default=1, ge=1, description="Số đoạn trong kịch bản")
    
    # API Keys
    gemini_key: Optional[str] = Field(default=None, description="API key Gemini")
    openai_key: Optional[str] = Field(default=None, description="API key OpenAI")
    speech_key: Optional[str] = Field(default=None, description="Azure Speech API key")
    speech_region: Optional[str] = Field(default=None, description="Azure Speech region")


class VideoGenerationResponse(BaseModel):
    """Response schema cho API tạo video"""
    video_id: str = Field(..., description="ID của video đã tạo")
    video_url: Optional[str] = Field(default=None, description="URL video (nếu có)")
    status: str = Field(..., description="Trạng thái (processing, completed, failed)")
    message: str = Field(..., description="Thông báo")
    estimated_duration: Optional[int] = Field(default=None, description="Thời gian ước tính hoàn thành (giây)")




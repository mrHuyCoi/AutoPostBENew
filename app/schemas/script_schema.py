from typing import List, Optional
from pydantic import BaseModel, Field


class ScriptGenerationRequest(BaseModel):
    video_subject: str = Field(..., description="Chủ đề của video cần viết kịch bản")
    video_language: str = Field(..., description="Ngôn ngữ đầu ra mong muốn, ví dụ: Vietnamese hoặc English")
    video_keywords: Optional[List[str]] = Field(
        default=None,
        description="Danh sách từ khóa quan trọng cho video",
    )
    script_style: Optional[str] = Field(
        default=None,
        description="Phong cách kịch bản mong muốn (ví dụ: thuyết phục, hài hước)",
    )
    paragraph_number: int = Field(
        default=1,
        ge=1,
        le=10,
        description="Số đoạn (phần) mong muốn trong kịch bản",
    )
    llm_provider: str = Field(
        ...,
        description="Nhà cung cấp LLM sử dụng: gemini hoặc openai",
        pattern="^(gemini|openai)$",
    )
    gemini_key: Optional[str] = Field(
        default=None,
        description="API key Gemini (chỉ bắt buộc khi llm_provider = gemini)",
    )
    openai_key: Optional[str] = Field(
        default=None,
        description="API key OpenAI (chỉ bắt buộc khi llm_provider = openai)",
    )


class ScriptGenerationResponse(BaseModel):
    script: str = Field(..., description="Kịch bản video được sinh bởi AI")




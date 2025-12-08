from pydantic import BaseModel
from typing import Optional, List

class ChatMessageInput(BaseModel):
    role: str
    message: str


class ChatRequest(BaseModel):
    query: str
    llm_provider: str
    api_key: Optional[str] = None
    stream: Optional[bool] = False
    thread_id: Optional[str] = None
    image_url: Optional[str] = None
    image_urls: Optional[List[str]] = None
    image_base64: Optional[str] = None
    platform: Optional[str] = None
    history: Optional[List[ChatMessageInput]] = None
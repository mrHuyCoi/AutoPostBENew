import uuid
from datetime import datetime
from pydantic import BaseModel
from typing import Optional

class ChatbotServiceBase(BaseModel):
    name: str
    description: Optional[str] = None
    base_price: int

class ChatbotServiceCreate(ChatbotServiceBase):
    pass

class ChatbotServiceUpdate(ChatbotServiceBase):
    pass

class ChatbotServiceRead(ChatbotServiceBase):
    id: uuid.UUID
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True 
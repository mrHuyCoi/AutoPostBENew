import uuid
from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List
from .chatbot_service_dto import ChatbotServiceRead

class ChatbotPlanBase(BaseModel):
    name: str
    description: Optional[str] = None
    monthly_price: int
    max_api_calls: Optional[int] = None

class ChatbotPlanCreate(ChatbotPlanBase):
    service_ids: List[uuid.UUID]

class ChatbotPlanUpdate(ChatbotPlanBase):
    service_ids: Optional[List[uuid.UUID]] = None

class ChatbotPlanRead(ChatbotPlanBase):
    id: uuid.UUID
    services: List[ChatbotServiceRead] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True 
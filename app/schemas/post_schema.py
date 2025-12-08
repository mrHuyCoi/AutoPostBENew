from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class ContentGenerationRequest(BaseModel):
    model: str = Field(default="gpt-4o-mini", description="OpenAI model to use")
    temperature: float = Field(default=0.8, ge=0.0, le=1.0, description="Controls randomness (0-1)")
    platform_specific_data: Optional[dict] = None

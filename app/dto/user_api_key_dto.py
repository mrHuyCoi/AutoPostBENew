import uuid
from pydantic import BaseModel
from typing import List

class UserApiKeyRead(BaseModel):
    api_key: str
    is_active: bool
    scopes: List[str]

    class Config:
        from_attributes = True 
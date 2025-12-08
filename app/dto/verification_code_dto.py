from pydantic import BaseModel, EmailStr
from datetime import datetime

class VerificationCodeBase(BaseModel):
    email: EmailStr
    code: str
    expires_at: datetime

class VerificationCodeCreate(VerificationCodeBase):
    pass

class VerificationCode(VerificationCodeBase):
    id: int

    class Config:
        from_attributes = True

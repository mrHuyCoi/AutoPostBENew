from pydantic import BaseModel
from typing import Optional

from .subscription_dto import SubscriptionRead
from .user_chatbot_subscription_dto import UserChatbotSubscriptionRead

class MySubscriptionsRead(BaseModel):
    video_subscription: Optional[SubscriptionRead] = None
    chatbot_subscription: Optional[UserChatbotSubscriptionRead] = None

    class Config:
        from_attributes = True 
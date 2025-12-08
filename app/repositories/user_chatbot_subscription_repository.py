from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Optional
import uuid

from app.models.user_chatbot_subscription import UserChatbotSubscription
from app.models.chatbot_plan import ChatbotPlan # Import ChatbotPlan

class UserChatbotSubscriptionRepository:
    @staticmethod
    async def create_subscription(db: AsyncSession, subscription: UserChatbotSubscription) -> UserChatbotSubscription:
        db.add(subscription)
        await db.commit()
        await db.refresh(subscription)
        return subscription

    @staticmethod
    async def get_subscription_by_id(db: AsyncSession, subscription_id: uuid.UUID) -> Optional[UserChatbotSubscription]:
        """Lấy subscription theo ID và tải kèm plan và services."""
        result = await db.execute(
            select(UserChatbotSubscription)
            .options(
                selectinload(UserChatbotSubscription.plan).selectinload(ChatbotPlan.services),
                selectinload(UserChatbotSubscription.user)
            )
            .filter(UserChatbotSubscription.id == subscription_id)
        )
        return result.scalars().first()

    @staticmethod
    async def get_active_subscription_by_user(db: AsyncSession, user_id: uuid.UUID) -> Optional[UserChatbotSubscription]:
        result = await db.execute(
            select(UserChatbotSubscription)
            .options(selectinload(UserChatbotSubscription.plan).selectinload(ChatbotPlan.services))
            .filter(UserChatbotSubscription.user_id == user_id)
            .order_by(UserChatbotSubscription.created_at.desc())
        )
        return result.scalars().first()
    
    @staticmethod
    async def deactivate_existing_subscriptions(db: AsyncSession, user_id: uuid.UUID):
        result = await db.execute(
            select(UserChatbotSubscription)
            .filter(UserChatbotSubscription.user_id == user_id, UserChatbotSubscription.is_active == True)
        )
        subscriptions = result.scalars().all()
        for sub in subscriptions:
            sub.is_active = False
        await db.commit()

    @staticmethod
    async def get_all_subscriptions(db: AsyncSession, skip: int = 0, limit: int = 100):
        """Lấy tất cả subscriptions với thông tin user và plan."""
        result = await db.execute(
            select(UserChatbotSubscription)
            .options(
                selectinload(UserChatbotSubscription.plan).selectinload(ChatbotPlan.services),
                selectinload(UserChatbotSubscription.user)
            )
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def get_pending_subscriptions(db: AsyncSession, skip: int = 0, limit: int = 100):
        """Lấy các subscription đang chờ phê duyệt."""
        result = await db.execute(
            select(UserChatbotSubscription)
            .options(
                selectinload(UserChatbotSubscription.plan).selectinload(ChatbotPlan.services),
                selectinload(UserChatbotSubscription.user)
            )
            .filter(UserChatbotSubscription.status == 'pending')
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def create_subscription(db: AsyncSession, subscription):
        """Tạo subscription mới từ subscription object."""
        db.add(subscription)
        await db.commit()
        await db.refresh(subscription)
        return subscription

    @staticmethod
    async def update_subscription(db: AsyncSession, subscription_id: uuid.UUID, data: dict):
        """Cập nhật subscription."""
        result = await db.execute(
            select(UserChatbotSubscription).filter(UserChatbotSubscription.id == subscription_id)
        )
        subscription = result.scalars().first()
        
        if not subscription:
            return None
            
        for key, value in data.items():
            if hasattr(subscription, key):
                setattr(subscription, key, value)
                
        await db.commit()
        await db.refresh(subscription)
        return subscription

    @staticmethod
    async def delete_subscription(db: AsyncSession, subscription_id: uuid.UUID):
        """Xóa subscription."""
        result = await db.execute(
            select(UserChatbotSubscription).filter(UserChatbotSubscription.id == subscription_id)
        )
        subscription = result.scalars().first()
        
        if subscription:
            await db.delete(subscription)
            await db.commit()
            return True
        return False 
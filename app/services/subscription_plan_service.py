from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid

from app.models.subscription import Subscription
from app.repositories.subscription_repository import SubscriptionRepository
from app.exceptions.api_exceptions import NotFoundException
from app.dto.subscription_dto import SubscriptionPlanCreate, SubscriptionPlanUpdate


class SubscriptionPlanService:
    """Service xử lý các thao tác liên quan đến gói cước (Subscription plans)."""
    
    @staticmethod
    async def get_subscription_plan(db: AsyncSession, subscription_id: uuid.UUID) -> Subscription:
        """Lấy thông tin gói cước bằng ID."""
        subscription = await SubscriptionRepository.get_plan_by_id(db, subscription_id)
        if not subscription:
            raise NotFoundException("Không tìm thấy gói cước")
        
        return subscription
    
    @staticmethod
    async def get_all_subscription_plans(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Subscription]:
        """Lấy danh sách tất cả gói cước."""
        return await SubscriptionRepository.get_all_plans(db, skip, limit)
    
    @staticmethod
    async def create_subscription_plan(db: AsyncSession, data: SubscriptionPlanCreate) -> Subscription:
        """Tạo một gói cước mới."""
        return await SubscriptionRepository.create_plan(db, data)
    
    @staticmethod
    async def update_subscription_plan(db: AsyncSession, plan_id: uuid.UUID, data: SubscriptionPlanUpdate) -> Subscription:
        """Cập nhật thông tin gói cước."""
        plan = await SubscriptionRepository.update_plan(db, plan_id, data)
        if not plan:
            raise NotFoundException("Không tìm thấy gói cước")
        return plan
    
    @staticmethod
    async def delete_subscription_plan(db: AsyncSession, plan_id: uuid.UUID) -> bool:
        """Xóa một gói cước."""
        result = await SubscriptionRepository.delete_plan(db, plan_id)
        if not result:
            raise NotFoundException("Không tìm thấy gói cước")
        return result
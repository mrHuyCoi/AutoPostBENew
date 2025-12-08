from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import selectinload

from app.dto.subscription_dto import SubscriptionCreate, SubscriptionUpdate
from app.models.user_subscription import UserSubscription
from app.models.subscription import Subscription
from app.repositories.subscription_repository import SubscriptionRepository
from app.exceptions.api_exceptions import BadRequestException, NotFoundException


class UserSubscriptionService:
    """Service xử lý các thao tác liên quan đến gói đăng ký người dùng."""
    
    @staticmethod
    async def create_subscription(db: AsyncSession, data: SubscriptionCreate) -> UserSubscription:
        """Tạo gói đăng ký mới cho người dùng."""
        # Kiểm tra người dùng đã có gói đăng ký đang hoạt động chưa
        # existing_subscription = await SubscriptionRepository.get_by_user_id(db, data.user_id)
        # if existing_subscription and existing_subscription.is_active:
        #     raise BadRequestException("Người dùng đã có gói đăng ký đang hoạt động")
        
        # Lấy thông tin gói đăng ký
        subscription_plan = await db.get(Subscription, data.subscription_id)
        if not subscription_plan:
            raise NotFoundException("Không tìm thấy gói đăng ký")
        
        # Tạo gói đăng ký mới
        # Nếu giá tiền > 0, đặt is_active = False để chờ xác nhận thanh toán
        # Nếu giá tiền = 0, đặt is_active = True (gói miễn phí, chỉ admin mới gán được)
        if subscription_plan.price > 0:
            data.is_active = False  # Chờ xác nhận thanh toán
        
        return await SubscriptionRepository.create(db, data)
    
    @staticmethod
    async def get_subscription(db: AsyncSession, subscription_id: uuid.UUID) -> UserSubscription:
        """Lấy thông tin gói đăng ký người dùng."""
        subscription = await SubscriptionRepository.get_user_subscription_by_id(db, subscription_id)
        if not subscription:
            raise NotFoundException("Không tìm thấy gói đăng ký")
        
        return subscription
    
    @staticmethod
    async def get_user_subscription(db: AsyncSession, user_id: uuid.UUID) -> UserSubscription:
        """Lấy thông tin gói đăng ký của người dùng."""
        subscription = await SubscriptionRepository.get_by_user_id(db, user_id)
        if not subscription:
            raise NotFoundException("Không tìm thấy gói đăng ký của người dùng")
        
        return subscription
    
    @staticmethod
    async def update_subscription(db: AsyncSession, subscription_id: uuid.UUID, data: SubscriptionUpdate) -> UserSubscription:
        """Cập nhật thông tin gói đăng ký người dùng."""
        updated_subscription = await SubscriptionRepository.update(db, subscription_id, data)
        if not updated_subscription:
            raise NotFoundException("Không tìm thấy gói đăng ký")
        
        return updated_subscription
    
    @staticmethod
    async def approve_subscription(db: AsyncSession, user_subscription_id: uuid.UUID) -> UserSubscription:
        subscription = await SubscriptionRepository.get_by_id(db, user_subscription_id)
        if not subscription:
            raise NotFoundException("Không tìm thấy gói đăng ký người dùng.")
        if subscription.is_active:
            raise BadRequestException("Gói đăng ký đã được kích hoạt.")
        
        subscription_plan = subscription.subscription_plan
        if not subscription_plan:
            # Điều này không nên xảy ra nếu dữ liệu toàn vẹn
            raise BadRequestException("Không tìm thấy thông tin gói cước của đăng ký này.")

        # Kích hoạt và tính toán lại ngày bắt đầu/kết thúc
        subscription.is_active = True
        subscription.start_date = datetime.now(timezone.utc)
        subscription.end_date = subscription.start_date + timedelta(days=subscription_plan.duration_days)
        subscription.updated_at = datetime.now(timezone.utc)
        
        await db.commit()
        await db.refresh(subscription)
        return subscription

    @staticmethod
    async def delete_subscription(db: AsyncSession, subscription_id: uuid.UUID) -> bool:
        """Xóa gói đăng ký người dùng."""
        success = await SubscriptionRepository.delete(db, subscription_id)
        if not success:
            raise NotFoundException("Không tìm thấy gói đăng ký")
        return success
    
    @staticmethod
    async def get_all_subscriptions(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[UserSubscription]:
        """Lấy danh sách gói đăng ký người dùng."""
        return await SubscriptionRepository.get_all(db, skip, limit)
    
    @staticmethod
    async def check_subscription_valid(db: AsyncSession, user_id: uuid.UUID) -> bool:
        """Kiểm tra gói đăng ký của người dùng có còn hiệu lực không."""
        subscription = await SubscriptionRepository.get_by_user_id(db, user_id)
        
        if not subscription or not subscription.is_active:
            return False
        
        # Kiểm tra ngày hết hạn
        if subscription.end_date and subscription.end_date.replace(tzinfo=None) < datetime.now(timezone.utc).replace(tzinfo=None):
            # Tự động cập nhật trạng thái nếu đã hết hạn
            subscription.is_active = False
            await db.commit()
            return False
        
        return True
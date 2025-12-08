from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List
import uuid
from sqlalchemy.orm import selectinload

from app.models.user_subscription import UserSubscription
from app.models.subscription import Subscription
from app.dto.subscription_dto import SubscriptionCreate, SubscriptionUpdate, SubscriptionPlanCreate, SubscriptionPlanUpdate


class SubscriptionRepository:
    """
    Repository xử lý các thao tác CRUD cho đối tượng UserSubscription.
    """
    
    @staticmethod
    async def create(db: AsyncSession, data: SubscriptionCreate) -> UserSubscription:
        """
        Tạo một gói đăng ký mới cho người dùng.
        
        Args:
            db: Database session
            data: Dữ liệu tạo gói đăng ký người dùng
            
        Returns:
            Đối tượng UserSubscription đã tạo
        """
        try:
            # Tạo đối tượng UserSubscription
            db_subscription = UserSubscription(
                user_id=data.user_id,
                subscription_id=data.subscription_id,
                start_date=data.start_date,
                end_date=data.end_date,
                is_active=data.is_active
            )
            
            # Lưu vào database
            db.add(db_subscription)
            await db.commit()
            await db.refresh(db_subscription)
            
            return db_subscription
        except Exception as e:
            print(f"Error creating user subscription: {str(e)}")
            await db.rollback()
            raise e
    
    @staticmethod
    async def get_by_id(db: AsyncSession, user_subscription_id: uuid.UUID) -> Optional[UserSubscription]:
        """
        Lấy thông tin gói đăng ký người dùng bằng ID.
        
        Args:
            db: Database session
            user_subscription_id: ID của gói đăng ký người dùng
            
        Returns:
            Đối tượng UserSubscription hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(UserSubscription)
            .options(
                selectinload(UserSubscription.subscription_plan),
                selectinload(UserSubscription.user)
            )
            .where(UserSubscription.id == user_subscription_id)
        )
        return result.scalars().first()
    
    @staticmethod
    async def get_by_user_id(db: AsyncSession, user_id: uuid.UUID) -> Optional[UserSubscription]:
        """
        Lấy thông tin gói đăng ký của người dùng.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            
        Returns:
            Đối tượng UserSubscription hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(UserSubscription)
            .options(
                selectinload(UserSubscription.subscription_plan),
                selectinload(UserSubscription.user)
            )
            .where(UserSubscription.user_id == user_id)
        )
        return result.scalars().first()
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[UserSubscription]:
        """
        Lấy danh sách gói đăng ký người dùng với phân trang.
        Bao gồm thông tin chi tiết về người dùng và gói đăng ký.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa
            
        Returns:
            Danh sách các đối tượng UserSubscription với thông tin liên quan
        """
        result = await db.execute(
            select(UserSubscription)
            .options(
                selectinload(UserSubscription.subscription_plan),
                selectinload(UserSubscription.user)
            )
            .offset(skip).limit(limit)
        )
        return result.scalars().all()
    
    @staticmethod
    async def update(db: AsyncSession, user_subscription_id: uuid.UUID, data: SubscriptionUpdate) -> Optional[UserSubscription]:
        """
        Cập nhật thông tin gói đăng ký người dùng.
        
        Args:
            db: Database session
            user_subscription_id: ID của gói đăng ký người dùng
            data: Dữ liệu cập nhật
            
        Returns:
            Đối tượng UserSubscription đã cập nhật hoặc None nếu không tìm thấy
        """
        try:
            db_subscription = await SubscriptionRepository.get_user_subscription_by_id(db, user_subscription_id)
            
            if not db_subscription:
                return None
            
            # Cập nhật các trường
            if data.subscription_id is not None:
                db_subscription.subscription_id = data.subscription_id
            if data.start_date is not None:
                db_subscription.start_date = data.start_date
            if data.end_date is not None:
                db_subscription.end_date = data.end_date
            if data.is_active is not None:
                db_subscription.is_active = data.is_active
            
            # Lưu thay đổi
            await db.commit()
            await db.refresh(db_subscription)
            
            return db_subscription
        except Exception as e:
            print(f"Error updating user subscription: {str(e)}")
            await db.rollback()
            raise e
    
    @staticmethod
    async def delete(db: AsyncSession, user_subscription_id: uuid.UUID) -> bool:
        """
        Xóa gói đăng ký người dùng.
        
        Args:
            db: Database session
            user_subscription_id: ID của gói đăng ký người dùng
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        try:
            db_subscription = await SubscriptionRepository.get_user_subscription_by_id(db, user_subscription_id)
            
            if not db_subscription:
                return False
            
            await db.delete(db_subscription)
            await db.commit()
            
            return True
        except Exception as e:
            print(f"Error deleting user subscription: {str(e)}")
            await db.rollback()
            raise e

    @staticmethod
    async def get_all_plans(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Subscription]:
        """
        Lấy danh sách tất cả gói cước với phân trang.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa
            
        Returns:
            Danh sách các đối tượng Subscription
        """
        result = await db.execute(
            select(Subscription)
            .offset(skip).limit(limit)
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_plan_by_id(db: AsyncSession, plan_id: uuid.UUID) -> Optional[Subscription]:
        """
        Lấy thông tin gói cước bằng ID.
        
        Args:
            db: Database session
            plan_id: ID của gói cước
            
        Returns:
            Đối tượng Subscription hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(Subscription)
            .where(Subscription.id == plan_id)
        )
        return result.scalars().first()
    
    @staticmethod
    async def create_plan(db: AsyncSession, data: SubscriptionPlanCreate) -> Subscription:
        """
        Tạo một gói cước mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo gói cước
            
        Returns:
            Đối tượng Subscription đã tạo
        """
        try:
            # Tạo đối tượng Subscription
            db_plan = Subscription(
                name=data.name,
                description=data.description,
                price=data.price,
                duration_days=data.duration_days,
                max_videos_per_day=data.max_videos_per_day,
                max_scheduled_days=data.max_scheduled_days,
                max_stored_videos=data.max_stored_videos,
                storage_limit_gb=data.storage_limit_gb,
                max_social_accounts=data.max_social_accounts,
                ai_content_generation=data.ai_content_generation,
                is_active=data.is_active
            )
            
            # Lưu vào database
            db.add(db_plan)
            await db.commit()
            await db.refresh(db_plan)
            
            return db_plan
        except Exception as e:
            print(f"Error creating subscription plan: {str(e)}")
            await db.rollback()
            raise e
    
    @staticmethod
    async def update_plan(db: AsyncSession, plan_id: uuid.UUID, data: SubscriptionPlanUpdate) -> Optional[Subscription]:
        """
        Cập nhật thông tin gói cước.
        
        Args:
            db: Database session
            plan_id: ID của gói cước
            data: Dữ liệu cập nhật
            
        Returns:
            Đối tượng Subscription đã cập nhật hoặc None nếu không tìm thấy
        """
        db_plan = await SubscriptionRepository.get_plan_by_id(db, plan_id)
        
        if not db_plan:
            return None
        
        # Cập nhật các trường
        update_data = data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_plan, key, value)
        
        # Lưu thay đổi
        await db.commit()
        await db.refresh(db_plan)
        
        return db_plan
    
    @staticmethod
    async def delete_plan(db: AsyncSession, plan_id: uuid.UUID) -> bool:
        """
        Xóa gói cước.
        
        Args:
            db: Database session
            plan_id: ID của gói cước
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        db_plan = await SubscriptionRepository.get_plan_by_id(db, plan_id)
        
        if not db_plan:
            return False
        
        await db.delete(db_plan)
        await db.commit()
        
        return True

    @staticmethod
    async def get_user_subscription_by_id(db: AsyncSession, user_subscription_id: uuid.UUID) -> Optional[UserSubscription]:
        """
        Lấy thông tin gói đăng ký người dùng bằng ID.
        
        Args:
            db: Database session
            user_subscription_id: ID của gói đăng ký người dùng
            
        Returns:
            Đối tượng UserSubscription hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(UserSubscription)
            .options(
                selectinload(UserSubscription.subscription_plan),
                selectinload(UserSubscription.user)
            )
            .where(UserSubscription.id == user_subscription_id)
        )
        return result.scalars().first()
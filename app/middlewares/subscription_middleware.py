from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from zoneinfo import ZoneInfo

from app.database.database import get_db
from app.models.user import User
from app.models.user_subscription import UserSubscription
from app.middlewares.auth_middleware import get_current_user
from app.repositories.subscription_repository import SubscriptionRepository

# ✅ Hàm chuẩn hóa thời gian giờ Việt không timezone
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


def check_active_subscription(
    required_max_videos_per_day: int = 0,
    required_max_scheduled_days: int = 0,
    required_max_stored_videos: int = 0,
    required_storage_limit_gb: int = 0,
    required_max_social_accounts: int = 0
):
    """
    Dependency để kiểm tra xem người dùng có gói đăng ký đang hoạt động và đủ quyền lợi không.
    """
    async def dependency(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)) -> User:
        if current_user.is_superuser:
            return current_user

        user_subscription = await SubscriptionRepository.get_by_user_id(db, current_user.id)

        if not user_subscription or not user_subscription.is_active or (user_subscription.end_date and user_subscription.end_date.replace(tzinfo=None) < now_vn_naive()):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Bạn cần có gói đăng ký đang hoạt động để sử dụng tính năng này."
            )

        subscription_plan = user_subscription.subscription_plan

        if not subscription_plan:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Lỗi hệ thống: Không tìm thấy thông tin gói đăng ký."
            )

        # Kiểm tra các quyền lợi cụ thể
        if required_max_videos_per_day > 0 and subscription_plan.max_videos_per_day < required_max_videos_per_day:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Gói của bạn chỉ cho phép {subscription_plan.max_videos_per_day} video/ngày. Yêu cầu: {required_max_videos_per_day}."
            )
        if required_max_scheduled_days > 0 and subscription_plan.max_scheduled_days < required_max_scheduled_days:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Gói của bạn chỉ cho phép lên lịch trước {subscription_plan.max_scheduled_days} ngày. Yêu cầu: {required_max_scheduled_days}."
            )
        if required_max_stored_videos > 0 and subscription_plan.max_stored_videos < required_max_stored_videos:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Gói của bạn chỉ cho phép lưu trữ {subscription_plan.max_stored_videos} video. Yêu cầu: {required_max_stored_videos}."
            )
        if required_storage_limit_gb > 0 and subscription_plan.storage_limit_gb < required_storage_limit_gb:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Gói của bạn chỉ cho phép {subscription_plan.storage_limit_gb}GB lưu trữ. Yêu cầu: {required_storage_limit_gb}GB."
            )
        if required_max_social_accounts > 0 and subscription_plan.max_social_accounts < required_max_social_accounts:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Gói của bạn chỉ cho phép kết nối {subscription_plan.max_social_accounts} tài khoản MXH. Yêu cầu: {required_max_social_accounts}."
            )

        return current_user
    return dependency


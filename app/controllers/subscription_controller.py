from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
import uuid
from datetime import datetime, timezone, timedelta

from app.database.database import get_db
from app.dto.subscription_dto import SubscriptionCreate, SubscriptionCreateSimple, SubscriptionRead, SubscriptionUpdate, SubscriptionPlanRead, SubscriptionPlanCreate, SubscriptionPlanUpdate
from app.dto.my_subscription_dto import MySubscriptionsRead # Import DTO mới
from app.dto.user_chatbot_subscription_dto import UserChatbotSubscriptionRead # Import DTO
from app.services.user_subscription_service import UserSubscriptionService
from app.services.subscription_plan_service import SubscriptionPlanService
from app.middlewares.auth_middleware import get_current_active_superuser, get_current_user
from app.middlewares.subscription_middleware import check_active_subscription
from app.models.user import User
from app.models.subscription import Subscription
from app.repositories.subscription_repository import SubscriptionRepository
from app.repositories.user_chatbot_subscription_repository import UserChatbotSubscriptionRepository # Import repo chatbot
from app.logging.decorators import with_log_context

router = APIRouter()


@router.get("/plans", response_model=List[SubscriptionPlanRead])
@with_log_context(
    event_action="subscription.plans.list",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def get_all_subscription_plans(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """
    [PUBLIC] Lấy danh sách tất cả các gói cước (plans).
    
    **Chức năng:**
    - API này cho phép tất cả mọi người truy cập, không yêu cầu đăng nhập.
    - Trả về danh sách tất cả các gói cước có sẵn trong hệ thống.
    - Giúp người dùng xem và so sánh các gói cước trước khi đăng ký.
    """
    return await SubscriptionPlanService.get_all_subscription_plans(db, skip, limit)


@router.post("/plans", response_model=SubscriptionPlanRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="subscription.plans.create",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def create_subscription_plan(
    data: SubscriptionPlanCreate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Tạo một gói cước mới.
    
    **Chức năng:**
    - API này dành riêng cho quản trị viên (superuser).
    - Cho phép admin tạo một gói cước mới với các thông tin như tên, giá, thời hạn, v.v.
    - Gói cước này sau đó có thể được gán cho người dùng thông qua API tạo subscription.
    """
    return await SubscriptionPlanService.create_subscription_plan(db, data)


@router.get("/plans/{plan_id}", response_model=SubscriptionPlanRead)
@with_log_context(
    event_action="subscription.plans.get",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def get_subscription_plan(
    plan_id: uuid.UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    [PUBLIC] Lấy thông tin chi tiết của một gói cước.
    
    **Chức năng:**
    - API này cho phép tất cả mọi người truy cập, không yêu cầu đăng nhập.
    - Trả về thông tin chi tiết của một gói cước cụ thể.
    - Giúp người dùng xem chi tiết về một gói cước trước khi đăng ký.
    """
    return await SubscriptionPlanService.get_subscription_plan(db, plan_id)


@router.put("/plans/{plan_id}", response_model=SubscriptionPlanRead)
@with_log_context(
    event_action="subscription.plans.update",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def update_subscription_plan(
    plan_id: uuid.UUID,
    data: SubscriptionPlanUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Cập nhật thông tin của một gói cước.
    
    **Chức năng:**
    - API này dành riêng cho quản trị viên (superuser).
    - Cho phép admin cập nhật thông tin của một gói cước đã tồn tại.
    """
    return await SubscriptionPlanService.update_subscription_plan(db, plan_id, data)


@router.delete("/plans/{plan_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="subscription.plans.delete",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def delete_subscription_plan(
    plan_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Xóa một gói cước.
    
    **Chức năng:**
    - API này dành riêng cho quản trị viên (superuser).
    - Xóa một gói cước khỏi hệ thống.
    - Lưu ý: Chỉ nên xóa các gói cước chưa có người dùng đăng ký.
    """
    await SubscriptionPlanService.delete_subscription_plan(db, plan_id)
    return None


@router.post("/", response_model=SubscriptionRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="subscription.create",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def create_subscription(
    data: SubscriptionCreateSimple,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    [USER] Tạo một gói đăng ký cho người dùng.

    **Chức năng:**
    - API này cho phép người dùng đã đăng nhập đăng ký gói dịch vụ có giá tiền lớn hơn 0.
    - Cho phép người dùng tạo một bản ghi `UserSubscription` mới và liên kết nó với `subscription_id` (gói cước) cụ thể.
    - Người dùng chỉ có thể đăng ký cho chính mình.
    - Người dùng chỉ có thể đăng ký gói có giá tiền lớn hơn gói hiện tại hoặc khi gói hiện tại đã hết hạn.
    - Hữu ích trong luồng đăng ký thông thường của người dùng.

    **Tham số:**
    - `data`: Chứa `subscription_id` cần gán.
    """
    # Kiểm tra xem gói đăng ký mới có tồn tại không
    new_subscription_plan = await db.get(Subscription, data.subscription_id)
    if not new_subscription_plan:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Không tìm thấy gói đăng ký"
        )
    
    # Không cho đăng ký gói có giá nhỏ hơn 0
    if new_subscription_plan.price <= 0:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Gói đăng ký miễn phí chỉ có thể được gán bởi quản trị viên"
        )
    
    # Kiểm tra gói đăng ký hiện tại của người dùng
    existing_subscription = await SubscriptionRepository.get_by_user_id(db, current_user.id)
    
    # Nếu người dùng đã có gói đăng ký
    if existing_subscription:
        # Lấy thông tin gói cước hiện tại
        current_plan = existing_subscription.subscription_plan
        
        # Kiểm tra xem gói hiện tại có hết hạn chưa
        is_expired = False
        if existing_subscription.end_date and existing_subscription.end_date.replace(tzinfo=None) < datetime.now(timezone.utc).replace(tzinfo=None):
            is_expired = True
        
        # Nếu gói chưa hết hạn và đang hoạt động, kiểm tra giá tiền
        if not is_expired and existing_subscription.is_active and new_subscription_plan.price <= current_plan.price:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Bạn chỉ có thể đăng ký gói có giá tiền cao hơn gói hiện tại hoặc khi gói hiện tại đã hết hạn"
            )
        
        # Cập nhật gói đăng ký hiện có thay vì tạo mới
        # Tính toán end_date dựa trên duration_days của gói mới
        start_date = datetime.now(timezone.utc).replace(tzinfo=None)
        end_date = start_date + timedelta(days=new_subscription_plan.duration_days)
        
        update_data = SubscriptionUpdate(
            subscription_id=data.subscription_id,
            start_date=start_date,
            end_date=end_date,
            is_active=False  # Đặt là False để chờ xác nhận thanh toán
        )
        
        return await UserSubscriptionService.update_subscription(db, existing_subscription.id, update_data)
    
    # Nếu người dùng chưa có gói đăng ký, tạo mới
    # Tính toán end_date dựa trên duration_days của gói
    start_date = datetime.now(timezone.utc).replace(tzinfo=None)
    end_date = start_date + timedelta(days=new_subscription_plan.duration_days)
    
    full_data = SubscriptionCreate(
        user_id=current_user.id,
        subscription_id=data.subscription_id,
        start_date=start_date,
        end_date=end_date,
        is_active=False  # Đặt là False để chờ xác nhận thanh toán
    )

    return await UserSubscriptionService.create_subscription(db, full_data)


@router.get("/", response_model=List[SubscriptionRead])
@with_log_context(
    event_action="subscription.list",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def get_subscriptions(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser) # Thay đổi: Chỉ admin mới có quyền xem tất cả
):
    """
    [ADMIN] Lấy danh sách tất cả các gói đăng ký của người dùng trong hệ thống.

    **Chức năng:**
    - API này dành riêng cho quản trị viên (superuser).
    - Trả về một danh sách các bản ghi `UserSubscription` có phân trang.
    - Bao gồm thông tin chi tiết về người dùng và gói đăng ký.
    - Giúp admin có cái nhìn tổng quan về tình hình đăng ký của tất cả người dùng.
    """
    return await UserSubscriptionService.get_all_subscriptions(db, skip, limit)


@router.get("/me", response_model=MySubscriptionsRead)
@with_log_context(
    event_action="subscription.me.get",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def get_my_subscription(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    [USER] Lấy thông tin tất cả các gói đăng ký của chính người dùng đang đăng nhập.
    """
    video_sub_orm = await UserSubscriptionService.get_user_subscription(db, current_user.id)
    chatbot_sub_orm = await UserChatbotSubscriptionRepository.get_active_subscription_by_user(db, current_user.id)
    
    # Chuyển đổi tường minh từ SQLAlchemy model sang Pydantic DTO
    video_sub_dto = SubscriptionRead.model_validate(video_sub_orm) if video_sub_orm else None
    chatbot_sub_dto = UserChatbotSubscriptionRead.model_validate(chatbot_sub_orm) if chatbot_sub_orm else None

    return MySubscriptionsRead(
        video_subscription=video_sub_dto,
        chatbot_subscription=chatbot_sub_dto
    )


@router.get("/{subscription_id}", response_model=SubscriptionPlanRead)
@with_log_context(
    event_action="subscription.plan_details.get",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def get_subscription_plan_details(
    subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(check_active_subscription())
):
    """
    [USER] Lấy thông tin chi tiết của một GÓI CƯỚC (plan) cụ thể.

    **Chức năng:**
    - Cho phép người dùng xem chi tiết về một gói cước (ví dụ: "Gói Cơ bản", "Gói Chuyên nghiệp").
    - Trả về các thông tin như giá, thời hạn, các quyền lợi (số video, dung lượng, v.v.).
    - `subscription_id` ở đây là ID của gói cước trong bảng `subscriptions`.
    """
    return await SubscriptionPlanService.get_subscription_plan(db, subscription_id)


@router.put("/{user_subscription_id}", response_model=SubscriptionRead)
@with_log_context(
    event_action="subscription.update",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def update_subscription(
    user_subscription_id: uuid.UUID,
    data: SubscriptionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Cập nhật thông tin gói đăng ký của một người dùng.

    **Chức năng:**
    - API này dành riêng cho quản trị viên (superuser).
    - Cho phép admin thay đổi thông tin của một bản ghi `UserSubscription` đã tồn tại.
    - Ví dụ: thay đổi ngày hết hạn, hủy kích hoạt gói.
    - `user_subscription_id` ở đây là ID của bản ghi trong bảng `user_subscriptions`.
    """
    return await UserSubscriptionService.update_subscription(db, user_subscription_id, data)



@router.post("/approve/{user_subscription_id}", response_model=SubscriptionRead)
@with_log_context(
    event_action="subscription.approve",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def approve_user_subscription(
    user_subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Phê duyệt và kích hoạt gói đăng ký cho người dùng.

    **Chức năng:**
    - Đây là API chính trong luồng "người dùng đăng ký -> admin phê duyệt".
    - Sau khi người dùng đăng ký, một bản ghi `UserSubscription` được tạo với `is_active=False`.
    - Admin sẽ gọi API này với `user_subscription_id` tương ứng để chuyển `is_active` thành `True`, chính thức kích hoạt gói cho người dùng.
    """
    return await UserSubscriptionService.approve_subscription(db, user_subscription_id)

@router.delete("/{user_subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="subscription.delete",
    event_category="subscription",
    source_layer="controller",
    source_controller="subscription_controller",
)
async def delete_subscription(
    user_subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Xóa bản ghi đăng ký của một người dùng.

    **Chức năng:**
    - API này dành riêng cho quản trị viên (superuser).
    - Xóa một bản ghi `UserSubscription` khỏi cơ sở dữ liệu.
    - Đây là một hành động quản trị, ví dụ như khi người dùng yêu cầu xóa tài khoản hoặc admin cần dọn dẹp dữ liệu.
    - `user_subscription_id` ở đây là ID của bản ghi trong bảng `user_subscriptions`.
    """
    await UserSubscriptionService.delete_subscription(db, user_subscription_id)
    # Không trả về body với status code 204 NO_CONTENT
    return None

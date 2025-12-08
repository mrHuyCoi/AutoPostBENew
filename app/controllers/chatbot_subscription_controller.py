from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
import uuid

from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user, get_current_active_superuser
from app.models.user import User
from app.services.chatbot_subscription_service import ChatbotSubscriptionService
from app.dto.chatbot_service_dto import ChatbotServiceCreate, ChatbotServiceRead
from app.dto.chatbot_plan_dto import ChatbotPlanCreate, ChatbotPlanUpdate, ChatbotPlanRead
from app.dto.user_chatbot_subscription_dto import UserChatbotSubscriptionCreate, UserChatbotSubscriptionRead, UserChatbotSubscriptionApproval, UserChatbotSubscriptionUpdate
from app.dto.user_api_key_dto import UserApiKeyRead
from app.logging.decorators import with_log_context

router = APIRouter(prefix="/chatbot-subscriptions", tags=["Chatbot Subscriptions"])

# --- Public Route ---

@router.get("/plans", response_model=List[ChatbotPlanRead])
@with_log_context(
    event_action="chatbot_subscriptions.plans.list",
    event_category="chatbot_subscription",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def list_available_chatbot_plans(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """(Public) Lấy danh sách các gói cước chatbot hiện có."""
    return await ChatbotSubscriptionService.list_available_plans(db, skip, limit)

@router.post("/subscribe", response_model=UserChatbotSubscriptionRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="chatbot_subscriptions.subscribe",
    event_category="chatbot_subscription",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def subscribe_to_chatbot_plan(
    data: UserChatbotSubscriptionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """(User) Đăng ký gói cước chatbot."""
    return await ChatbotSubscriptionService.subscribe_to_plan(db, current_user, data)

@router.get("/me", response_model=UserChatbotSubscriptionRead)
@with_log_context(
    event_action="chatbot_subscriptions.me.get",
    event_category="chatbot_subscription",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def get_my_chatbot_subscription(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """(User) Lấy thông tin subscription chatbot của user hiện tại."""
    subscription = await ChatbotSubscriptionService.get_my_active_subscription(db, current_user)
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bạn chưa có subscription chatbot nào"
        )
    return subscription

@router.get("/my-api-key", response_model=UserApiKeyRead)
@with_log_context(
    event_action="chatbot_subscriptions.my_api_key.get",
    event_category="chatbot_subscription",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def get_my_api_key(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """(User) Lấy API key chatbot của user hiện tại."""
    api_key = await ChatbotSubscriptionService.get_my_api_key(db, current_user)
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Không tìm thấy API key. Vui lòng kiểm tra lại gói đăng ký."
        )
    return api_key

@router.post("/my-api-key/regenerate", response_model=UserApiKeyRead)
@with_log_context(
    event_action="chatbot_subscriptions.my_api_key.regenerate",
    event_category="chatbot_subscription",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def regenerate_my_api_key(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """(User) Tái tạo API key chatbot."""
    api_key = await ChatbotSubscriptionService.regenerate_my_api_key(db, current_user)
    return api_key

# --- Admin Routes ---

@router.post("/admin/services", response_model=ChatbotServiceRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="chatbot_subscriptions.admin.services.create",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def create_chatbot_service(
    data: ChatbotServiceCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Tạo một dịch vụ chatbot mới."""
    return await ChatbotSubscriptionService.create_service(db, data)

@router.get("/admin/services", response_model=List[ChatbotServiceRead])
@with_log_context(
    event_action="chatbot_subscriptions.admin.services.list",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def list_chatbot_services(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Lấy danh sách tất cả dịch vụ chatbot."""
    return await ChatbotSubscriptionService.list_services(db, skip, limit)

@router.put("/admin/services/{service_id}", response_model=ChatbotServiceRead)
@with_log_context(
    event_action="chatbot_subscriptions.admin.services.update",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def update_chatbot_service(
    service_id: uuid.UUID,
    data: ChatbotServiceCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Cập nhật dịch vụ chatbot."""
    return await ChatbotSubscriptionService.update_service(db, service_id, data)

@router.delete("/admin/services/{service_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="chatbot_subscriptions.admin.services.delete",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def delete_chatbot_service(
    service_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Xóa dịch vụ chatbot."""
    await ChatbotSubscriptionService.delete_service(db, service_id)
    return {"message": "Service deleted successfully"}

@router.post("/admin/plans", response_model=ChatbotPlanRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="chatbot_subscriptions.admin.plans.create",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def create_chatbot_plan(
    data: ChatbotPlanCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Tạo một gói cước chatbot mới."""
    return await ChatbotSubscriptionService.create_plan(db, data)

@router.get("/admin/plans", response_model=List[ChatbotPlanRead])
@with_log_context(
    event_action="chatbot_subscriptions.admin.plans.list",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def list_chatbot_plans(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Lấy danh sách tất cả gói cước chatbot."""
    return await ChatbotSubscriptionService.list_plans(db, skip, limit)

@router.put("/admin/plans/{plan_id}", response_model=ChatbotPlanRead)
@with_log_context(
    event_action="chatbot_subscriptions.admin.plans.update",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def update_chatbot_plan(
    plan_id: uuid.UUID,
    data: ChatbotPlanUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Cập nhật gói cước chatbot."""
    return await ChatbotSubscriptionService.update_plan(db, plan_id, data)

@router.delete("/admin/plans/{plan_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="chatbot_subscriptions.admin.plans.delete",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def delete_chatbot_plan(
    plan_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Xóa gói cước chatbot."""
    await ChatbotSubscriptionService.delete_plan(db, plan_id)
    return {"message": "Plan deleted successfully"}

@router.get("/admin/subscriptions", response_model=List[UserChatbotSubscriptionRead])
@with_log_context(
    event_action="chatbot_subscriptions.admin.subscriptions.list",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def list_user_subscriptions(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Lấy danh sách tất cả đăng ký chatbot của người dùng."""
    return await ChatbotSubscriptionService.list_user_subscriptions(db, skip, limit)

@router.get("/admin/subscriptions/pending", response_model=List[UserChatbotSubscriptionRead])
@with_log_context(
    event_action="chatbot_subscriptions.admin.subscriptions.pending.list",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def list_pending_subscriptions(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Lấy danh sách các subscription đang chờ phê duyệt."""
    return await ChatbotSubscriptionService.list_pending_subscriptions(db, skip, limit)

@router.post("/admin/subscriptions/{subscription_id}/approve", response_model=UserChatbotSubscriptionRead)
@with_log_context(
    event_action="chatbot_subscriptions.admin.subscriptions.approve",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def approve_subscription(
    subscription_id: uuid.UUID,
    approval_data: UserChatbotSubscriptionApproval,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Phê duyệt subscription và xóa gói cũ."""
    return await ChatbotSubscriptionService.approve_subscription(db, subscription_id, approval_data.notes)

@router.post("/admin/subscriptions/{subscription_id}/reject", response_model=UserChatbotSubscriptionRead)
@with_log_context(
    event_action="chatbot_subscriptions.admin.subscriptions.reject",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def reject_subscription(
    subscription_id: uuid.UUID,
    approval_data: UserChatbotSubscriptionApproval,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Từ chối subscription."""
    return await ChatbotSubscriptionService.reject_subscription(db, subscription_id, approval_data.notes)

@router.post("/admin/subscriptions", response_model=UserChatbotSubscriptionRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="chatbot_subscriptions.admin.subscriptions.create",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def create_user_subscription(
    data: UserChatbotSubscriptionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Tạo đăng ký chatbot cho người dùng."""
    return await ChatbotSubscriptionService.create_user_subscription(db, data)

@router.put("/admin/subscriptions/{subscription_id}", response_model=UserChatbotSubscriptionRead)
@with_log_context(
    event_action="chatbot_subscriptions.admin.subscriptions.update",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def update_user_subscription(
    subscription_id: uuid.UUID,
    data: UserChatbotSubscriptionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Cập nhật đăng ký chatbot của người dùng."""
    return await ChatbotSubscriptionService.update_user_subscription(db, subscription_id, data)

@router.delete("/admin/subscriptions/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="chatbot_subscriptions.admin.subscriptions.delete",
    event_category="chatbot_subscription_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def delete_user_subscription(
    subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Xóa đăng ký chatbot của người dùng."""
    await ChatbotSubscriptionService.delete_user_subscription(db, subscription_id)
    return {"message": "Subscription deleted successfully"}

# --- Permissions Routes ---

@router.get("/admin/permissions", response_model=List[dict])
@with_log_context(
    event_action="chatbot_subscriptions.admin.permissions.list",
    event_category="chatbot_permission_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def list_chatbot_permissions(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Lấy danh sách phân quyền chatbot."""
    # Tạm thời trả về empty list cho đến khi implement permissions
    return []

@router.post("/admin/permissions", response_model=dict)
@with_log_context(
    event_action="chatbot_subscriptions.admin.permissions.create",
    event_category="chatbot_permission_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def create_chatbot_permission(
    data: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Tạo phân quyền chatbot mới."""
    # Tạm thời trả về success cho đến khi implement permissions
    return {"message": "Permission created successfully"}

@router.put("/admin/permissions/{permission_id}", response_model=dict)
@with_log_context(
    event_action="chatbot_subscriptions.admin.permissions.update",
    event_category="chatbot_permission_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def update_chatbot_permission(
    permission_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Cập nhật phân quyền chatbot."""
    # Tạm thời trả về success cho đến khi implement permissions
    return {"message": "Permission updated successfully"}

@router.delete("/admin/permissions/{permission_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="chatbot_subscriptions.admin.permissions.delete",
    event_category="chatbot_permission_admin",
    source_layer="controller",
    source_controller="chatbot_subscription_controller",
)
async def delete_chatbot_permission(
    permission_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """(Admin) Xóa phân quyền chatbot."""
    # Tạm thời trả về success cho đến khi implement permissions
    return {"message": "Permission deleted successfully"} 
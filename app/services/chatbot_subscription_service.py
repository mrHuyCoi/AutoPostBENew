from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException, status

from app.models.user import User
from app.models.user_chatbot_subscription import UserChatbotSubscription
from app.dto.chatbot_service_dto import ChatbotServiceCreate, ChatbotServiceUpdate
from app.dto.chatbot_plan_dto import ChatbotPlanCreate, ChatbotPlanUpdate
from app.dto.user_chatbot_subscription_dto import UserChatbotSubscriptionCreate, UserChatbotSubscriptionUpdate
from app.repositories.chatbot_service_repository import ChatbotServiceRepository
from app.repositories.chatbot_plan_repository import ChatbotPlanRepository
from app.repositories.user_chatbot_subscription_repository import UserChatbotSubscriptionRepository
from app.repositories.user_api_key_repository import UserApiKeyRepository
from app.utils.time import get_vn_now


class ChatbotSubscriptionService:

    # --- Admin Services for ChatbotService ---
    @staticmethod
    async def create_service(db: AsyncSession, data: ChatbotServiceCreate):
        try:
            return await ChatbotServiceRepository.create_service(db, data)
        except Exception as e:
            # Kiểm tra xem có phải lỗi duplicate name không
            if "duplicate key value violates unique constraint" in str(e) and "chatbot_services_name_key" in str(e):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Dịch vụ với tên '{data.name}' đã tồn tại. Vui lòng chọn tên khác."
                )
            # Lỗi database khác
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi tạo dịch vụ: {str(e)}"
            )

    @staticmethod
    async def list_services(db: AsyncSession, skip: int, limit: int):
        return await ChatbotServiceRepository.get_all_services(db, skip, limit)

    @staticmethod
    async def update_service(db: AsyncSession, service_id: uuid.UUID, data: ChatbotServiceCreate):
        try:
            result = await ChatbotServiceRepository.update_service(db, service_id, data)
            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Không tìm thấy dịch vụ"
                )
            return result
        except Exception as e:
            # Kiểm tra xem có phải lỗi duplicate name không
            if "duplicate key value violates unique constraint" in str(e) and "chatbot_services_name_key" in str(e):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Dịch vụ với tên '{data.name}' đã tồn tại. Vui lòng chọn tên khác."
                )
            # Lỗi database khác
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi cập nhật dịch vụ: {str(e)}"
            )

    @staticmethod
    async def delete_service(db: AsyncSession, service_id: uuid.UUID):
        return await ChatbotServiceRepository.delete_service(db, service_id)

    # --- Admin Services for ChatbotPlan ---
    @staticmethod
    async def create_plan(db: AsyncSession, data: ChatbotPlanCreate):
        try:
            return await ChatbotPlanRepository.create_plan(db, data)
        except ValueError as e:
            # Lỗi validation (duplicate name, etc.)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except Exception as e:
            # Lỗi database khác
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi tạo gói cước: {str(e)}"
            )
    
    @staticmethod
    async def list_plans(db: AsyncSession, skip: int, limit: int):
        return await ChatbotPlanRepository.get_all_plans(db, skip, limit)

    @staticmethod
    async def update_plan(db: AsyncSession, plan_id: uuid.UUID, data: ChatbotPlanCreate):
        try:
            result = await ChatbotPlanRepository.update_plan(db, plan_id, data)
            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Không tìm thấy gói cước"
                )
            return result
        except ValueError as e:
            # Lỗi validation (duplicate name, etc.)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except Exception as e:
            # Lỗi database khác
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi cập nhật gói cước: {str(e)}"
            )

    @staticmethod
    async def delete_plan(db: AsyncSession, plan_id: uuid.UUID):
        return await ChatbotPlanRepository.delete_plan(db, plan_id)

    # --- Admin Services for User Subscriptions ---
    @staticmethod
    async def list_user_subscriptions(db: AsyncSession, skip: int, limit: int):
        return await UserChatbotSubscriptionRepository.get_all_subscriptions(db, skip, limit)

    @staticmethod
    async def list_pending_subscriptions(db: AsyncSession, skip: int, limit: int):
        """Lấy danh sách các subscription đang chờ phê duyệt"""
        return await UserChatbotSubscriptionRepository.get_pending_subscriptions(db, skip, limit)

    @staticmethod
    async def approve_subscription(db: AsyncSession, subscription_id: uuid.UUID, admin_notes: str = None):
        """Admin phê duyệt subscription và xóa các gói cũ"""
        try:
            # Lấy subscription cần phê duyệt
            subscription = await UserChatbotSubscriptionRepository.get_subscription_by_id(db, subscription_id)
            if not subscription:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Không tìm thấy subscription"
                )
            
            if subscription.status != 'pending':
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Subscription này không ở trạng thái chờ phê duyệt"
                )

            # Xóa các gói cũ của user này
            await UserChatbotSubscriptionRepository.deactivate_existing_subscriptions(db, subscription.user_id)
            
            # Phê duyệt gói mới
            subscription.status = 'approved'
            subscription.is_active = True
            subscription.updated_at = get_vn_now()
            
            # Tạo hoặc cập nhật API Key với scopes từ plan
            scopes = [service.name for service in subscription.plan.services]
            await UserApiKeyRepository.create_or_update_api_key(db, user_id=subscription.user_id, scopes=scopes)
            
            await db.commit()
            await db.refresh(subscription)
            
            return subscription
            
        except Exception as e:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi phê duyệt subscription: {str(e)}"
            )

    @staticmethod
    async def reject_subscription(db: AsyncSession, subscription_id: uuid.UUID, admin_notes: str = None):
        """Admin từ chối subscription"""
        try:
            subscription = await UserChatbotSubscriptionRepository.get_subscription_by_id(db, subscription_id)
            if not subscription:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Không tìm thấy subscription"
                )
            
            if subscription.status != 'pending':
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Subscription này không ở trạng thái chờ phê duyệt"
                )

            subscription.status = 'rejected'
            subscription.is_active = False
            subscription.updated_at = get_vn_now()
            
            await db.commit()
            await db.refresh(subscription)
            
            return subscription
            
        except Exception as e:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi từ chối subscription: {str(e)}"
            )

    @staticmethod
    async def create_user_subscription(db: AsyncSession, data: UserChatbotSubscriptionCreate):
        # This is business logic, should stay in the service
        plan = await ChatbotPlanRepository.get_plan(db, data.plan_id)
        if not plan:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")

        # You would also fetch user info if needed
        # user = await UserRepository.get_by_id(db, data.user_id) ...

        subscription = UserChatbotSubscription(
            user_id=data.user_id,
            plan_id=data.plan_id,
            start_date=datetime.utcnow(),
            end_date=datetime.utcnow() + relativedelta(months=data.months_subscribed),
            months_subscribed=data.months_subscribed,
            total_price=plan.monthly_price * data.months_subscribed,  # Example price calculation
            max_api_calls=plan.max_api_calls,
            is_active=False,  # Mặc định không active, cần admin phê duyệt
            status='pending'  # Mặc định chờ phê duyệt
        )
        return await UserChatbotSubscriptionRepository.create_subscription(db, subscription)

    @staticmethod
    async def update_user_subscription(db: AsyncSession, subscription_id: uuid.UUID, data: UserChatbotSubscriptionUpdate):
        """Admin cập nhật subscription của người dùng với các xử lý nghiệp vụ cần thiết.

        - Cho phép đổi plan và/hoặc số tháng: tự động tính lại end_date và total_price.
        - Thay đổi status:
            + 'approved' => deactivate các gói cũ, activate gói hiện tại, và cập nhật API key scopes theo plan.
            + 'rejected' hoặc 'pending' => set inactive và vô hiệu hoá API key.
        - Thay đổi is_active trực tiếp: nếu False => vô hiệu hoá API key.
        """
        try:
            # Lấy subscription hiện tại
            subscription = await UserChatbotSubscriptionRepository.get_subscription_by_id(db, subscription_id)
            if not subscription:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Không tìm thấy subscription")

            update_data = data.dict(exclude_unset=True)

            # Theo dõi thay đổi ảnh hưởng đến giá và ngày kết thúc
            plan_changed = 'plan_id' in update_data and update_data['plan_id'] != subscription.plan_id
            months_changed = 'months_subscribed' in update_data and update_data['months_subscribed'] is not None and update_data['months_subscribed'] != subscription.months_subscribed

            # Nếu đổi plan, tải plan mới và gán
            if plan_changed:
                new_plan = await ChatbotPlanRepository.get_plan(db, update_data['plan_id'])
                if not new_plan:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Gói không tồn tại")
                subscription.plan_id = update_data['plan_id']
                subscription.max_api_calls = new_plan.max_api_calls

            # Nếu đổi số tháng, cập nhật
            if months_changed:
                subscription.months_subscribed = update_data['months_subscribed']

            # Nếu có thay đổi plan hoặc months => tính lại end_date và total_price dựa vào plan hiện tại
            if plan_changed or months_changed:
                # Bảo đảm có plan hiện tại đã load
                current_plan = await ChatbotPlanRepository.get_plan(db, subscription.plan_id)
                if not current_plan:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Gói không tồn tại")
                # Giữ nguyên start_date, chỉ tính lại end_date theo months_subscribed
                start_date = subscription.start_date or get_vn_now()
                subscription.end_date = start_date + relativedelta(months=subscription.months_subscribed)
                # Tính giá không áp dụng thêm chiết khấu phức tạp ở đây (admin)
                subscription.total_price = current_plan.monthly_price * subscription.months_subscribed
                # Nếu chưa bị override bởi plan_changed ở trên, đảm bảo max_api_calls đồng bộ với plan hiện tại
                if not plan_changed:
                    subscription.max_api_calls = current_plan.max_api_calls

            # Xử lý status
            if 'status' in update_data and update_data['status']:
                new_status = update_data['status']
                if new_status not in ['pending', 'approved', 'rejected']:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Trạng thái không hợp lệ")

                subscription.status = new_status
                if new_status == 'approved':
                    # Deactivate các gói active khác của user
                    await UserChatbotSubscriptionRepository.deactivate_existing_subscriptions(db, subscription.user_id)
                    subscription.is_active = True
                    # Cập nhật API key theo services trong plan
                    current_plan = await ChatbotPlanRepository.get_plan(db, subscription.plan_id)
                    scopes = [svc.name for svc in (current_plan.services or [])]
                    await UserApiKeyRepository.create_or_update_api_key(db, user_id=subscription.user_id, scopes=scopes)
                else:
                    # pending hoặc rejected => không active
                    subscription.is_active = False
                    await UserApiKeyRepository.deactivate_api_key(db, subscription.user_id)

            # Nếu admin đặt is_active trực tiếp
            if 'is_active' in update_data and update_data['is_active'] is not None:
                subscription.is_active = update_data['is_active']
                if not subscription.is_active:
                    await UserApiKeyRepository.deactivate_api_key(db, subscription.user_id)

            subscription.updated_at = get_vn_now()

            await db.commit()
            # Refresh với đầy đủ quan hệ
            updated = await UserChatbotSubscriptionRepository.get_subscription_by_id(db, subscription.id)
            return updated
        except HTTPException:
            # Các HTTPException giữ nguyên
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Lỗi khi cập nhật subscription: {str(e)}")

    @staticmethod
    async def delete_user_subscription(db: AsyncSession, subscription_id: uuid.UUID):
        # Trước khi xoá, cố gắng vô hiệu hoá API key nếu đây là gói đang active
        try:
            subscription = await UserChatbotSubscriptionRepository.get_subscription_by_id(db, subscription_id)
            user_id = subscription.user_id if subscription else None
            result = await UserChatbotSubscriptionRepository.delete_subscription(db, subscription_id)
            if result and user_id:
                # Vô hiệu hoá API key sau khi xoá subscription (an toàn hơn),
                # Hoặc có thể mở rộng: kiểm tra còn gói approved/active nào khác không.
                await UserApiKeyRepository.deactivate_api_key(db, user_id)
            return result
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Lỗi khi xoá subscription: {str(e)}")

    # --- Permissions Services ---
    @staticmethod
    async def list_permissions(db: AsyncSession, skip: int, limit: int):
        # Tạm thời trả về empty list cho đến khi implement permissions
        return []

    @staticmethod
    async def create_permission(db: AsyncSession, data: dict):
        # Tạm thời trả về success cho đến khi implement permissions
        return {"message": "Permission created successfully"}

    @staticmethod
    async def update_permission(db: AsyncSession, permission_id: uuid.UUID, data: dict):
        # Tạm thời trả về success cho đến khi implement permissions
        return {"message": "Permission updated successfully"}

    @staticmethod
    async def delete_permission(db: AsyncSession, permission_id: uuid.UUID):
        # Tạm thời trả về success cho đến khi implement permissions
        return {"message": "Permission deleted successfully"}


    # --- User-facing Services ---
    @staticmethod
    async def list_available_plans(db: AsyncSession, skip: int, limit: int):
        # Wrapper có thể thêm logic filter các plan active sau này
        return await ChatbotPlanRepository.get_all_plans(db, skip, limit)

    @staticmethod
    async def get_my_active_subscription(db: AsyncSession, current_user: User):
        return await UserChatbotSubscriptionRepository.get_active_subscription_by_user(db, current_user.id)

    @staticmethod
    async def get_my_api_key(db: AsyncSession, current_user: User):
        return await UserApiKeyRepository.get_by_user_id(db, current_user.id)

    @staticmethod
    async def regenerate_my_api_key(db: AsyncSession, current_user: User):
        api_key = await UserApiKeyRepository.regenerate_api_key(db, current_user.id)
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="Không tìm thấy API key để tái tạo. Vui lòng đăng ký gói cước trước."
            )
        return api_key

    @staticmethod
    async def subscribe_to_plan(db: AsyncSession, current_user: User, data: UserChatbotSubscriptionCreate):
        plan = await ChatbotPlanRepository.get_plan(db, data.plan_id)
        if not plan:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Gói không tồn tại")

        # 1. Tính toán giá cuối cùng với chiết khấu
        # Logic chiết khấu: 3-5 tháng: 5%, 6-11 tháng: 10%, >=12 tháng: 15%
        discount_rate = 0
        if 3 <= data.months_subscribed <= 5:
            discount_rate = 0.05
        elif 6 <= data.months_subscribed <= 11:
            discount_rate = 0.10
        elif data.months_subscribed >= 12:
            discount_rate = 0.15
        
        total_price_before_discount = plan.monthly_price * data.months_subscribed
        total_discount = total_price_before_discount * discount_rate
        final_price = total_price_before_discount - total_discount

        # 2. Tạo subscription mới (chờ admin phê duyệt)
        start_date = get_vn_now()
        end_date = start_date + relativedelta(months=data.months_subscribed)
        
        new_subscription = UserChatbotSubscription(
            user_id=current_user.id,
            plan_id=data.plan_id,
            start_date=start_date,
            end_date=end_date,
            months_subscribed=data.months_subscribed,
            total_price=final_price,
            max_api_calls=plan.max_api_calls,
            is_active=False,  # Chỉ active khi admin phê duyệt
            status='pending'  # Chờ admin phê duyệt
        )
        created_subscription = await UserChatbotSubscriptionRepository.create_subscription(db, new_subscription)

        # 3. Lấy lại subscription với đầy đủ thông tin để trả về response
        full_subscription = await UserChatbotSubscriptionRepository.get_subscription_by_id(db, created_subscription.id)

        return full_subscription 

    @staticmethod
    async def check_and_increment_usage(db: AsyncSession, current_user: User):
        """Kiểm tra subscription chatbot của user và tăng bộ đếm lượt gọi.

        - Chỉ áp dụng cho gói chatbot.
        - Nếu max_api_calls là None hoặc <= 0: hiểu là không giới hạn, nhưng vẫn tăng api_calls_used để theo dõi.
        - Nếu api_calls_used >= max_api_calls: raise HTTP 403.
        """
        try:
            subscription = await UserChatbotSubscriptionRepository.get_active_subscription_by_user(db, current_user.id)
            if not subscription:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Bạn cần đăng ký gói chatbot để sử dụng API này."
                )

            now = get_vn_now()
            if (
                (hasattr(subscription, "is_active") and not subscription.is_active)
                or (hasattr(subscription, "status") and subscription.status != "approved")
                or (hasattr(subscription, "end_date") and subscription.end_date is not None and subscription.end_date < now)
            ):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Gói chatbot của bạn đã hết hạn hoặc chưa được phê duyệt."
                )

            max_calls = subscription.max_api_calls
            if max_calls is not None and max_calls > 0 and subscription.api_calls_used >= max_calls:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Bạn đã sử dụng hết số lượt gọi API chatbot trong gói hiện tại."
                )

            subscription.api_calls_used = (subscription.api_calls_used or 0) + 1
            subscription.updated_at = get_vn_now()
            await db.commit()
            await db.refresh(subscription)
        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi cập nhật số lượt sử dụng chatbot: {str(e)}"
            )

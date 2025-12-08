from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import List, Optional
import uuid

from app.models.chatbot_plan import ChatbotPlan
from app.models.chatbot_service import ChatbotService
from app.dto.chatbot_plan_dto import ChatbotPlanCreate, ChatbotPlanUpdate

class ChatbotPlanRepository:
    @staticmethod
    async def create_plan(db: AsyncSession, data: ChatbotPlanCreate) -> ChatbotPlan:
        # Kiểm tra tên gói cước đã tồn tại chưa
        existing_plan_result = await db.execute(
            select(ChatbotPlan).filter(ChatbotPlan.name == data.name)
        )
        if existing_plan_result.scalars().first():
            raise ValueError(f"Gói cước với tên '{data.name}' đã tồn tại. Vui lòng chọn tên khác.")
        
        service_ids = data.service_ids
        plan_data = data.dict(exclude={'service_ids'})
        
        plan = ChatbotPlan(**plan_data)
        
        if service_ids:
            services_result = await db.execute(select(ChatbotService).filter(ChatbotService.id.in_(service_ids)))
            services = services_result.scalars().all()
            plan.services = services

        db.add(plan)
        await db.commit()
        # await db.refresh(plan) # db.refresh() không tải lại relationships

        # Tải lại plan với relationships để đảm bảo response model nhận đủ dữ liệu
        created_plan_result = await db.execute(
            select(ChatbotPlan)
            .options(selectinload(ChatbotPlan.services))
            .filter(ChatbotPlan.id == plan.id)
        )
        created_plan = created_plan_result.scalars().first()
        
        return created_plan

    @staticmethod
    async def get_plan(db: AsyncSession, plan_id: uuid.UUID) -> Optional[ChatbotPlan]:
        result = await db.execute(
            select(ChatbotPlan)
            .options(selectinload(ChatbotPlan.services))
            .filter(ChatbotPlan.id == plan_id)
        )
        return result.scalars().first()

    @staticmethod
    async def get_all_plans(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[ChatbotPlan]:
        result = await db.execute(
            select(ChatbotPlan)
            .options(selectinload(ChatbotPlan.services))
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def update_plan(db: AsyncSession, plan_id: uuid.UUID, data: ChatbotPlanUpdate) -> Optional[ChatbotPlan]:
        result = await db.execute(
            select(ChatbotPlan)
            .options(selectinload(ChatbotPlan.services)) # Eager load services for the object to be updated
            .filter(ChatbotPlan.id == plan_id)
        )
        plan = result.scalars().first()

        if not plan:
            return None

        update_data = data.dict(exclude_unset=True)
        
        # Kiểm tra tên gói cước đã tồn tại chưa (nếu có thay đổi tên)
        if 'name' in update_data and update_data['name'] != plan.name:
            existing_plan_result = await db.execute(
                select(ChatbotPlan).filter(
                    ChatbotPlan.name == update_data['name'],
                    ChatbotPlan.id != plan_id
                )
            )
            if existing_plan_result.scalars().first():
                raise ValueError(f"Gói cước với tên '{update_data['name']}' đã tồn tại. Vui lòng chọn tên khác.")
        
        if 'service_ids' in update_data:
            service_ids = update_data.pop('service_ids')
            # Fetch the service objects to be associated
            if service_ids:
                services_result = await db.execute(select(ChatbotService).filter(ChatbotService.id.in_(service_ids)))
                plan.services = services_result.scalars().all()
            else:
                plan.services = [] # Clear services if an empty list is provided

        for key, value in update_data.items():
            setattr(plan, key, value)
            
        await db.commit()
        
        # After commit, the object 'plan' is often expired. 
        # We re-fetch it to get the clean, updated state with all relationships loaded.
        refreshed_plan_result = await db.execute(
            select(ChatbotPlan)
            .options(selectinload(ChatbotPlan.services))
            .filter(ChatbotPlan.id == plan.id)
        )
        
        return refreshed_plan_result.scalars().first()

    @staticmethod
    async def delete_plan(db: AsyncSession, plan_id: uuid.UUID) -> bool:
        result = await db.execute(select(ChatbotPlan).filter(ChatbotPlan.id == plan_id))
        plan = result.scalars().first()
        if plan:
            await db.delete(plan)
            await db.commit()
            return True
        return False 
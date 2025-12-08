from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Optional
import uuid

from app.models.chatbot_service import ChatbotService
from app.dto.chatbot_service_dto import ChatbotServiceCreate, ChatbotServiceUpdate

class ChatbotServiceRepository:
    @staticmethod
    async def create_service(db: AsyncSession, data: ChatbotServiceCreate) -> ChatbotService:
        # Kiểm tra tên dịch vụ đã tồn tại chưa
        existing_service = await db.execute(
            select(ChatbotService).filter(ChatbotService.name == data.name)
        )
        if existing_service.scalars().first():
            raise ValueError(f"Dịch vụ với tên '{data.name}' đã tồn tại. Vui lòng chọn tên khác.")
        
        service = ChatbotService(**data.dict())
        db.add(service)
        await db.commit()
        await db.refresh(service)
        return service

    @staticmethod
    async def get_service(db: AsyncSession, service_id: uuid.UUID) -> Optional[ChatbotService]:
        result = await db.execute(select(ChatbotService).filter(ChatbotService.id == service_id))
        return result.scalars().first()

    @staticmethod
    async def get_all_services(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[ChatbotService]:
        result = await db.execute(select(ChatbotService).offset(skip).limit(limit))
        return result.scalars().all()

    @staticmethod
    async def update_service(db: AsyncSession, service_id: uuid.UUID, data: ChatbotServiceUpdate) -> Optional[ChatbotService]:
        result = await db.execute(select(ChatbotService).filter(ChatbotService.id == service_id))
        service = result.scalars().first()
        if service:
            update_data = data.dict(exclude_unset=True)
            
            # Kiểm tra tên dịch vụ đã tồn tại chưa (nếu có thay đổi tên)
            if 'name' in update_data and update_data['name'] != service.name:
                existing_service = await db.execute(
                    select(ChatbotService).filter(
                        ChatbotService.name == update_data['name'],
                        ChatbotService.id != service_id
                    )
                )
                if existing_service.scalars().first():
                    raise ValueError(f"Dịch vụ với tên '{update_data['name']}' đã tồn tại. Vui lòng chọn tên khác.")
            
            for key, value in update_data.items():
                setattr(service, key, value)
            await db.commit()
            await db.refresh(service)
        return service

    @staticmethod
    async def delete_service(db: AsyncSession, service_id: uuid.UUID) -> bool:
        result = await db.execute(select(ChatbotService).filter(ChatbotService.id == service_id))
        service = result.scalars().first()
        if service:
            await db.delete(service)
            await db.commit()
            return True
        return False 
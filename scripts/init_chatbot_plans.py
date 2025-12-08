import asyncio
import sys
import os

# Thêm thư mục gốc vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database.database import engine
from app.models.chatbot_service import ChatbotService
from app.models.chatbot_plan import ChatbotPlan

async def init_chatbot_data():
    """Khởi tạo các dịch vụ và gói cước (plan) cho chatbot."""
    
    # 1. Định nghĩa các dịch vụ cơ bản
    services_data = [
        {"name": "Dịch vụ sửa chữa", "description": "Tích hợp chatbot chuyên về tư vấn dịch vụ sửa chữa.", "base_price": 1000000},
        {"name": "Bán điện thoại", "description": "Tích hợp chatbot chuyên về tư vấn và bán điện thoại.", "base_price": 1000000},
        {"name": "Bán linh kiện", "description": "Tích hợp chatbot chuyên về tư vấn và bán linh kiện.", "base_price": 1000000},
        {"name": "Bán dụng cụ sửa chữa", "description": "Tích hợp chatbot chuyên về tư vấn và bán dụng cụ.", "base_price": 1000000},
    ]

    # 2. Định nghĩa các gói cước (combo)
    plans_data = [
        {
            "name": "Combo Tiết kiệm 2",
            "description": "Kết hợp 2 dịch vụ: Sửa chữa & Bán điện thoại.",
            "monthly_price": 1500000,
            "service_names": ["Dịch vụ sửa chữa", "Bán điện thoại"]
        },
        {
            "name": "Combo Nâng cao 3",
            "description": "Kết hợp 3 dịch vụ: Sửa chữa, Bán điện thoại, Bán linh kiện.",
            "monthly_price": 2000000,
            "service_names": ["Dịch vụ sửa chữa", "Bán điện thoại", "Bán linh kiện"]
        },
        {
            "name": "Combo Toàn diện 4",
            "description": "Tích hợp toàn bộ 4 dịch vụ chatbot.",
            "monthly_price": 2500000,
            "service_names": ["Dịch vụ sửa chữa", "Bán điện thoại", "Bán linh kiện", "Bán dụng cụ sửa chữa"]
        },
    ]

    async with AsyncSession(engine) as session:
        async with session.begin():
            # 3. Tạo các dịch vụ nếu chưa tồn tại
            created_services = {}
            for service_data in services_data:
                result = await session.execute(
                    select(ChatbotService).where(ChatbotService.name == service_data["name"])
                )
                existing_service = result.scalars().first()
                if not existing_service:
                    new_service = ChatbotService(**service_data)
                    session.add(new_service)
                    print(f"Đã tạo dịch vụ: {new_service.name}")
                    created_services[new_service.name] = new_service
                else:
                    created_services[existing_service.name] = existing_service

            await session.flush() # Đảm bảo services có ID trước khi gán cho plan

            # 4. Tạo các gói cước nếu chưa tồn tại và liên kết với dịch vụ
            for plan_data in plans_data:
                result = await session.execute(
                    select(ChatbotPlan).where(ChatbotPlan.name == plan_data["name"])
                )
                existing_plan = result.scalars().first()

                if not existing_plan:
                    services_to_link = []
                    for service_name in plan_data["service_names"]:
                        if service_name in created_services:
                            services_to_link.append(created_services[service_name])
                    
                    plan_create_data = {k: v for k, v in plan_data.items() if k != 'service_names'}
                    new_plan = ChatbotPlan(**plan_create_data)
                    new_plan.services = services_to_link
                    session.add(new_plan)
                    print(f"Đã tạo gói: {new_plan.name} với {len(new_plan.services)} dịch vụ.")
            
            await session.commit()
    print("Hoàn tất khởi tạo dữ liệu chatbot.")

async def main():
    await init_chatbot_data()

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 
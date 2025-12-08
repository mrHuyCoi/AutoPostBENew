import asyncio
import sys
import os

# Thêm thư mục gốc vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database.database import engine
from app.models.subscription import Subscription


async def init_subscription_plans():
    """Khởi tạo các gói đăng ký cơ bản."""
    plans = [
        {
            "name": "Cơ bản",
            "description": "Gói hàng tháng",
            "price": 199000,
            "duration_days": 30,
            "max_videos_per_day": 3,
            "max_scheduled_days": 7,
            "max_stored_videos": 30,
            "storage_limit_gb": 5,
            "max_social_accounts": 5,
        },
        {
            "name": "Tiết kiệm",
            "description": "Gói 3 tháng, giảm 16%",
            "price": 499000,
            "duration_days": 90,
            "max_videos_per_day": 6,
            "max_scheduled_days": 14,
            "max_stored_videos": 60,
            "storage_limit_gb": 10,
            "max_social_accounts": 8,
        },
        {
            "name": "Chuyên nghiệp",
            "description": "Gói 1 năm, tặng 6 tháng",
            "price": 1699000,
            "duration_days": 545,  # 365 + 180
            "max_videos_per_day": 9,
            "max_scheduled_days": 21,
            "max_stored_videos": 90,
            "storage_limit_gb": 15,
            "max_social_accounts": 12,
        },
        {
            "name": "miễn phí",
            "description": "Gói 1 tuần",
            "price": 0,
            "duration_days": 7,  # 365 + 180
            "max_videos_per_day": 9,
            "max_scheduled_days": 21,
            "max_stored_videos": 90,
            "storage_limit_gb": 15,
            "max_social_accounts": 12,
        },
    ]

    async with AsyncSession(engine) as session:
        async with session.begin():
            for plan_data in plans:
                # Kiểm tra xem gói đã tồn tại chưa
                result = await session.execute(
                    select(Subscription).where(Subscription.name == plan_data["name"])
                )
                existing_plan = result.scalars().first()

                if not existing_plan:
                    new_plan = Subscription(**plan_data)
                    session.add(new_plan)
            
            await session.commit()

async def main():
    await init_subscription_plans()

if __name__ == "__main__":
    import sys
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

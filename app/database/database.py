from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import AsyncGenerator
import asyncio

from app.configs.settings import settings

# Tạo async engine
engine = create_async_engine(settings.DATABASE_URL, echo=False)

# Tạo async session
async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Base class cho các ORM models
Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function để cung cấp database session cho các API endpoints
    """
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

async def create_tables():
    """
    Tạo hoặc cập nhật bảng trong database
    """
    async with engine.begin() as conn:
        # Tạo bảng nếu chưa tồn tại và cập nhật schema hiện tại
        await conn.run_sync(Base.metadata.create_all)

# Hàm này có thể chạy trực tiếp để tạo bảng
if __name__ == "__main__":
    asyncio.run(create_tables())

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from datetime import datetime
import uuid

from app.models.platform_post import PlatformPost
from app.models.social_account import SocialAccount
from app.models.platform_post_media_asset import platform_post_media_asset
from app.models.youtube_metadata import YouTubeMetadata


class PlatformPostRepository:
    """
    Repository xử lý các thao tác liên quan đến PlatformPost.
    """

    @staticmethod
    async def count_user_posts_today(db: AsyncSession, user_id: uuid.UUID, today_start: datetime) -> int:
        """
        Đếm số lượng PlatformPost mà người dùng đã tạo trong ngày hôm nay.
        """
        count_posts_today_stmt = select(func.count(PlatformPost.id)).where(
            PlatformPost.user_id == user_id,
            PlatformPost.created_at >= today_start
        )
        return (await db.execute(count_posts_today_stmt)).scalar_one()

    @staticmethod
    async def create_platform_post(db: AsyncSession, platform_post_data: dict) -> PlatformPost:
        """
        Tạo một bản ghi PlatformPost mới.
        """
        new_platform_post = PlatformPost(**platform_post_data)
        db.add(new_platform_post)
        await db.flush() # Flush để có ID
        return new_platform_post

    @staticmethod
    async def link_platform_post_with_media_asset(db: AsyncSession, platform_post_id: uuid.UUID, media_asset_id: uuid.UUID):
        """
        Liên kết PlatformPost với MediaAsset.
        """
        await db.execute(
            platform_post_media_asset.insert().values(
                platform_post_id=platform_post_id,
                media_asset_id=media_asset_id
            )
        )

    @staticmethod
    async def create_youtube_metadata(db: AsyncSession, youtube_metadata_data: dict) -> YouTubeMetadata:
        """
        Tạo một bản ghi YouTubeMetadata mới.
        """
        new_youtube_metadata = YouTubeMetadata(**youtube_metadata_data)
        db.add(new_youtube_metadata)
        return new_youtube_metadata
        
    @staticmethod
    async def get_platform_post_by_id(db: AsyncSession, post_id: uuid.UUID, user_id: uuid.UUID) -> PlatformPost:
        """
        Lấy thông tin một PlatformPost theo ID và user_id.
        """
        query = select(PlatformPost).where(
            PlatformPost.id == post_id,
            PlatformPost.user_id == user_id
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()
        
    @staticmethod
    async def update_platform_post(db: AsyncSession, post_id: uuid.UUID, user_id: uuid.UUID, update_data: dict) -> bool:
        """
        Cập nhật thông tin một PlatformPost.
        Trả về True nếu cập nhật thành công, False nếu không tìm thấy bản ghi.
        """
        stmt = update(PlatformPost).where(
            PlatformPost.id == post_id,
            PlatformPost.user_id == user_id
        ).values(**update_data)
        
        result = await db.execute(stmt)
        return result.rowcount > 0
        
    @staticmethod
    async def delete_platform_post(db: AsyncSession, post_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """
        Xóa một PlatformPost.
        Trả về True nếu xóa thành công, False nếu không tìm thấy bản ghi.
        """
        stmt = delete(PlatformPost).where(
            PlatformPost.id == post_id,
            PlatformPost.user_id == user_id
        )
        
        result = await db.execute(stmt)
        return result.rowcount > 0
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from typing import Tuple, Optional
import uuid

from app.models.media_asset import MediaAsset


class MediaAssetRepository:
    """
    Repository xử lý các thao tác liên quan đến MediaAsset.
    """

    @staticmethod
    async def get_user_media_asset_counts_and_size(db: AsyncSession, user_id: uuid.UUID) -> Tuple[int, int]:
        """
        Đếm số lượng media asset và tổng kích thước (bytes) của người dùng.
        """
        result = await db.execute(
            select(func.count(MediaAsset.id), func.sum(MediaAsset.size_bytes)).where(
                MediaAsset.user_id == user_id
            )
        )
        count, total_size = result.one()
        return count or 0, total_size or 0

    @staticmethod
    async def create_media_asset(db: AsyncSession, media_asset_data: dict) -> MediaAsset:
        """
        Tạo một bản ghi MediaAsset mới.
        """
        new_media_asset = MediaAsset(**media_asset_data)
        db.add(new_media_asset)
        await db.flush() # Flush để có ID
        return new_media_asset
        
    @staticmethod
    async def mark_media_asset_deleted(db: AsyncSession, media_asset_id: uuid.UUID, deleted_at: str) -> bool:
        """
        Đánh dấu MediaAsset đã bị xóa và cập nhật kích thước về 0 để giảm dung lượng lưu trữ.
        """
        stmt = update(MediaAsset).where(
            MediaAsset.id == media_asset_id
        ).values(
            url={"deleted": True, "deleted_at": deleted_at},
            size_bytes=0  # Đặt kích thước về 0 để giảm dung lượng lưu trữ
        )
        
        result = await db.execute(stmt)
        return result.rowcount > 0
from app.models.platform_post import PlatformPost
from app.models.media_asset import MediaAsset
from app.models.platform_post_media_asset import platform_post_media_asset
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert
from datetime import datetime
from sqlalchemy.future import select
from sqlalchemy.exc import NoResultFound
from zoneinfo import ZoneInfo

# ✅ Hàm chuẩn hóa thời gian giờ Việt không timezone
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)

async def schedule_platform_post(
    db: AsyncSession,
    media_asset_id: str,
    social_account_id: str,
    platform: str,
    scheduled_at: datetime,
    platform_specific_data: dict,
    current_user_id: str
):
    # Kiểm tra media asset thuộc về user
    result = await db.execute(select(MediaAsset).where(MediaAsset.id == media_asset_id, MediaAsset.user_id == current_user_id))
    media_asset = result.scalar_one_or_none()
    if not media_asset:
        raise ValueError("Media asset not found or not owned by user")

    # Tạo platform_post
    platform_post = PlatformPost(
        user_id=current_user_id,
        social_account_id=social_account_id,
        platform=platform,
        scheduled_at=scheduled_at,
        status="scheduled",
        platform_specific_data=platform_specific_data,
        created_at=now_vn_naive(),
        updated_at=now_vn_naive(),
    )
    db.add(platform_post)
    await db.flush()  # Để lấy id

    # Liên kết với media_asset
    await db.execute(
        insert(platform_post_media_asset).values(
            platform_post_id=platform_post.id,
            media_asset_id=media_asset_id
        )
    )
    await db.commit()
    await db.refresh(platform_post)
    return platform_post

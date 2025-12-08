"""
content_publisher_tasks.py
Quản lý pipeline đăng bài đa nền tảng bằng Celery + SQLAlchemy (async).
Đảm bảo:
- Không còn lỗi “Future attached to a different loop”.
- Lỗi publish được commit vào DB, task hiển thị FAILED trên Flower.
- Thiếu MediaAsset → fallback text-only, không crash.
"""

import asyncio
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

from celery import chain
from sqlalchemy import select, update, or_, and_
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

from app.celery_app import celery_app
from app.database.database import async_session
from app.models.platform_post import PlatformPost
from app.models.media_asset import MediaAsset
from app.models.platform_post_media_asset import platform_post_media_asset
from app.services.file_storage_service import FileStorageService
from app.repositories.media_asset_repository import MediaAssetRepository
from app.services.facebook_acc_service import FacebookService
from app.services.instagram_acc_service import InstagramService
from app.services.youtube_acc_service import YouTubeService

# ============================================================
# Helper
# ============================================================

def _get_loop() -> asyncio.AbstractEventLoop:
    """Luôn dùng cùng event-loop mặc định của worker."""
    try:
        return asyncio.get_event_loop()
    except RuntimeError:  # no loop yet
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


# ============================================================
# 1. Quét & xếp bài đã schedule
# ============================================================

@celery_app.task(name="publish_scheduled_posts")
def publish_scheduled_posts():
    """
    Celery task: tìm các post đã đến giờ & xếp chain xử lý chúng.
    """
    loop = _get_loop()
    try:
        return loop.run_until_complete(_publish_scheduled_posts())
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


async def _publish_scheduled_posts():
    published, errors = 0, 0

    async with async_session() as db:
        now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)
        stmt = select(PlatformPost).where(
            PlatformPost.status == "ready",
            PlatformPost.scheduled_at != None,
            PlatformPost.scheduled_at <= now,
        )
        posts = (await db.execute(stmt)).scalars().all()

        if not posts:
            return {"status": "success", "message": "No posts", "published": 0, "errors": 0}

        logger.info(f"[Scheduler] Found {len(posts)} posts to publish")

        for pp in posts:
            try:
                chain(
                    prepare_media_for_post.s(str(pp.id), str(pp.user_id)),
                    call_platform_api.s(),
                    update_post_status.s(),
                ).apply_async(queue="default")
                published += 1
            except Exception as e:
                logger.error(f"[Scheduler] Error scheduling {pp.id}: {e}")
                errors += 1

        return {
            "status": "success",
            "message": f"Scheduled {published} posts with {errors} errors",
            "published": published,
            "errors": errors,
        }


# ============================================================
# 2. Đăng 1 bài lẻ
# ============================================================

@celery_app.task(name="publish_single_post")
def publish_single_post(post_id: str, user_id: str):
    """
    Xếp chain publish cho 1 post cụ thể.
    """
    try:
        res = chain(
            prepare_media_for_post.s(post_id, user_id),
            call_platform_api.s(),
            update_post_status.s(),
        ).apply_async(queue="default")
        return {"status": "success", "task_id": res.id}
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ============================================================
# 3. Prepare media
# ============================================================

@celery_app.task(name="prepare_media_for_post", bind=True)
def prepare_media_for_post(self, post_id: str, user_id: str):
    loop = _get_loop()
    tid = self.request.id
    logger.info(f"[{tid}] Preparing media for post {post_id}")

    try:
        result = loop.run_until_complete(_prepare_media_for_post(post_id, user_id))
        return {"post_id": post_id, "user_id": user_id, "media_info": result}
    except Exception as e:
        loop.run_until_complete(_update_post_status_failed(post_id, user_id, str(e)))
        raise


async def _prepare_media_for_post(post_id: str, user_id: str):
    """
    - Retry 1 lần nếu MediaAsset chưa commit.
    - Thiếu asset ⇒ fallback text-only, không raise.
    """
    async with async_session() as db:
        # Lấy PlatformPost
        stmt = select(PlatformPost).where(PlatformPost.id == post_id)
        platform_post = (await db.execute(stmt)).scalars().first()
        if not platform_post:
            raise ValueError(f"Platform post {post_id} not found")

        # Lấy media_asset_id
        stmt_link = select(platform_post_media_asset.c.media_asset_id).where(
            platform_post_media_asset.c.platform_post_id == post_id
        )
        media_asset_id = (await db.execute(stmt_link)).scalar_one_or_none()

        # Không có media → text-only
        if not media_asset_id:
            return {"has_media": False, "content": platform_post.generated_content}

        # Thử lấy MediaAsset + retry
        media_asset = await db.get(MediaAsset, media_asset_id)
        if not media_asset:
            await asyncio.sleep(0.4)
            await db.expire_all()
            media_asset = await db.get(MediaAsset, media_asset_id)

        if not media_asset:
            logger.warning(f"[WARN] Media asset {media_asset_id} missing → post {post_id} text-only")
            return {"has_media": False, "content": platform_post.generated_content}

        return {
            "has_media": True,
            "media_asset_id": str(media_asset_id),
            "file_type": media_asset.file_type,
            "urls": InstagramService._extract_urls(media_asset.url),
            "content": platform_post.generated_content,
        }


# ============================================================
# 4. Call platform API
# ============================================================

@celery_app.task(name="call_platform_api", bind=True, max_retries=0)
def call_platform_api(self, media_info):
    loop = _get_loop()
    tid = self.request.id
    post_id = media_info["post_id"]
    user_id = media_info["user_id"]
    logger.info(f"[{tid}] Calling platform API for post {post_id}")

    try:
        res = loop.run_until_complete(_call_platform_api(post_id, user_id))

        # Nếu API báo lỗi → raise để Celery mark FAILED
        if res.get("status") == "error":
            raise RuntimeError(res.get("message", "Unknown error"))

        return {"post_id": post_id, "user_id": user_id, "api_result": res}

    except Exception as e:
        err_msg = str(e)
        logger.error(f"[{tid}] API error {post_id}: {err_msg}")
        loop.run_until_complete(_update_post_status_failed(post_id, user_id, err_msg))
        raise


async def _call_platform_api(post_id: str, user_id: str):
    async with async_session() as db:
        try:
            # Lấy PlatformPost
            stmt = select(PlatformPost).where(PlatformPost.id == post_id)
            platform_post = (await db.execute(stmt)).scalars().first()
            if not platform_post:
                raise ValueError(f"Platform post {post_id} not found")

            # Gọi API tương ứng
            if platform_post.platform == "facebook":
                api_res = await FacebookService.publish_platform_post(db, post_id)
            elif platform_post.platform == "instagram":
                api_res = await InstagramService.publish_platform_post(db, post_id)
            elif platform_post.platform == "youtube":
                api_res = await YouTubeService.publish_platform_post(db, post_id)
            else:
                raise ValueError(f"Unsupported platform: {platform_post.platform}")

            await db.commit()
            return {"status": "ok", "result": api_res}

        except Exception as e:
            err = str(e)
            platform_post.status = "failed"
            platform_post.post_url = f"Error: {err}"
            platform_post.platform_specific_data = (platform_post.platform_specific_data or {}) | {"error": err}
            platform_post.updated_at = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)
            await db.commit()
            return {"status": "error", "message": err}


# ============================================================
# 5. Update post status (log-only)
# ============================================================

@celery_app.task(name="update_post_status", bind=True)
def update_post_status(self, api_result):
    loop = _get_loop()
    tid = self.request.id
    post_id = api_result["post_id"]
    logger.info(f"[{tid}] Updating status for {post_id}")

    try:
        return loop.run_until_complete(_update_post_status(post_id, api_result))
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


async def _update_post_status(post_id: str, api_result: dict):
    return {
        "status": "success",
        "message": f"Post {post_id} published successfully",
        "platform_result": api_result.get("api_result"),
    }


# ============================================================
# 6. Update FAILED helper
# ============================================================

async def _update_post_status_failed(post_id: str, user_id: str, error_message: str):
    async with async_session() as db:
        stmt = select(PlatformPost).where(PlatformPost.id == post_id)
        platform_post = (await db.execute(stmt)).scalars().first()
        if not platform_post:
            return

        platform_post.status = "failed"
        platform_post.post_url = f"Error: {error_message}"
        platform_post.platform_specific_data = (platform_post.platform_specific_data or {}) | {"error": error_message}
        platform_post.updated_at = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)
        await db.commit()


# ============================================================
# 7. API tiện ích (nếu cần)
# ============================================================

def _process_post_publishing(post_id: str, user_id: str):
    """
    Gọi hàm này từ API HTTP nếu muốn trigger publish ngay.
    """
    return chain(
        prepare_media_for_post.s(post_id, user_id),
        call_platform_api.s(),
        update_post_status.s(),
    ).apply_async(queue="default")


# ============================================================
# 8. Xóa video đã đăng sau 3 ngày
# ============================================================

@celery_app.task(name="cleanup_old_media")
def cleanup_old_media():
    """Celery task: tìm và xóa media (video và hình ảnh) đã đăng quá 3 ngày.
    
    Sau khi xóa, cập nhật size_bytes = 0 để giảm dung lượng lưu trữ của người dùng.
    """
    loop = _get_loop()
    try:
        return loop.run_until_complete(_cleanup_old_media())
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


async def _cleanup_old_media():
    """Tìm và xóa media (video và hình ảnh) đã đăng sau 3 ngày."""
    deleted_count = 0
    error_count = 0
    
    async with async_session() as db:
        # Tính thời điểm 3 ngày trước
        three_days_ago = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None) - timedelta(days=3)
        
        # Tìm tất cả các bài đăng đã published trước 3 ngày
        stmt = select(PlatformPost).where(
            PlatformPost.status == "published",
            PlatformPost.published_at != None,
            PlatformPost.published_at <= three_days_ago
        )
        old_posts = (await db.execute(stmt)).scalars().all()
        
        if not old_posts:
            logger.info("[Cleanup] No old posts found")
            return {"status": "success", "message": "No old media files to delete", "deleted": 0, "errors": 0}
        
        logger.info(f"[Cleanup] Found {len(old_posts)} old posts to process")
        
        for post in old_posts:
            # Lấy media_asset_id liên kết với post
            stmt_link = select(platform_post_media_asset.c.media_asset_id).where(
                platform_post_media_asset.c.platform_post_id == post.id
            )
            media_asset_id = (await db.execute(stmt_link)).scalar_one_or_none()
            
            if not media_asset_id:
                continue  # Không có media, bỏ qua
            
            # Lấy thông tin MediaAsset
            media_asset = await db.get(MediaAsset, media_asset_id)
            if not media_asset:
                continue  # Không tìm thấy media, bỏ qua
                
            # Kiểm tra xem media đã bị xóa chưa
            if media_asset.size_bytes == 0 or (isinstance(media_asset.url, dict) and media_asset.url.get("deleted")):
                logger.info(f"[Cleanup] Media {media_asset_id} already deleted, skipping")
                continue  # Media đã bị xóa trước đó, bỏ qua
            
            # Xóa file từ S3
            success = await FileStorageService.delete_file_from_s3(media_asset.storage_path)
            
            if success:
                # Cập nhật thông tin trong database và đặt size_bytes = 0
                deleted_at = datetime.now().isoformat()
                update_success = await MediaAssetRepository.mark_media_asset_deleted(db, media_asset_id, deleted_at)
                
                if update_success:
                    await db.commit()
                    deleted_count += 1
                    logger.info(f"[Cleanup] Deleted {media_asset.file_type} for post {post.id}, media {media_asset_id}, path {media_asset.storage_path}")
                else:
                    await db.rollback()
                    error_count += 1
                    logger.error(f"[Cleanup] Failed to update database for video {media_asset_id}")
            else:
                error_count += 1
                logger.error(f"[Cleanup] Failed to delete {media_asset.file_type} for post {post.id}, media {media_asset_id}, path {media_asset.storage_path}")
    
    result = {
        "status": "success",
        "message": f"Deleted {deleted_count} media files with {error_count} errors",
        "deleted": deleted_count,
        "errors": error_count
    }
    logger.info(f"[Cleanup] Task completed: {result['message']}")
    return result

import uuid
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.platform_post import PlatformPost
from app.models.social_account import SocialAccount
# from app.models.post import Post
from app.models.media_asset import MediaAsset
# from app.models.post_media import PostMedia
from app.utils.crypto import token_encryption
import httpx
import asyncio
import json
from fastapi import HTTPException
from app.models.platform_post_media_asset import platform_post_media_asset
from app.exceptions.api_exceptions import BadRequestException
from sqlalchemy.orm import selectinload
from typing import List
from sqlalchemy import join
from zoneinfo import ZoneInfo

INSTAGRAM_GRAPH_API_URL = "https://graph.facebook.com/v23.0"

# ✅ Hàm chuẩn hóa thời gian giờ Việt không timezone
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)

class InstagramService:
    @staticmethod
    def _extract_urls(url_field):
        """
        Trả về danh sách URL từ trường url của MediaAsset.
        - Nếu đã là list, trả về nguyên vẹn.
        - Nếu là chuỗi JSON của list, parse rồi trả về.
        - Ngược lại, trả về list chứa chính chuỗi đó.
        """
        if url_field is None:
            return []
        # Nếu đã là list (được ORM cast thành list) -> trả về ngay
        if isinstance(url_field, list):
            return url_field
        # Thử parse JSON list
        try:
            parsed = json.loads(url_field)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        # Mặc định coi là một URL đơn
        return [url_field]

    @staticmethod
    async def publish_platform_post(db: AsyncSession, platform_post_id: str) -> dict:
        """Publish a queued platform_post to Instagram with async polling and timeout-safe publish."""

        stmt_pp = (
            select(PlatformPost)
            .where(PlatformPost.id == platform_post_id)
            .options(selectinload(PlatformPost.social_account))
        )
        platform_post: PlatformPost | None = (await db.execute(stmt_pp)).scalars().first()
        if not platform_post:
            raise BadRequestException("Không tìm thấy platform_post.")

        social_account: SocialAccount | None = platform_post.social_account
        if not social_account or social_account.platform != platform_post.platform:
            raise BadRequestException("Account IG mismatch.")

        stmt_media = (
            select(MediaAsset)
            .select_from(
                join(
                    platform_post_media_asset,
                    MediaAsset,
                    platform_post_media_asset.c.media_asset_id == MediaAsset.id,
                )
            )
            .where(platform_post_media_asset.c.platform_post_id == platform_post_id)
        )
        media_asset: MediaAsset | None = (await db.execute(stmt_media)).scalars().first()
        if not media_asset:
            raise BadRequestException("Không tìm thấy media asset.")

        access_token: str = token_encryption.decrypt(social_account.access_token)
        ig_user_id: str = social_account.account_id
        caption: str = platform_post.generated_content or ""

        urls = InstagramService._extract_urls(media_asset.url)
        image_urls = [u for u in urls if media_asset.file_type.startswith("image")]
        video_urls = [u for u in urls if media_asset.file_type.startswith("video")]
        total_media = len(image_urls) + len(video_urls)

        psd = platform_post.platform_specific_data or {}
        platform_type = (psd.get("platform_type") or "").strip().lower()

        if platform_type == "instagram-reels":
            media_type = "REELS"
        elif platform_type == "instagram-feed":
            if total_media > 1:
                media_type = "CAROUSEL"
            elif image_urls:
                media_type = "PHOTO"
            else:
                raise BadRequestException("Không có ảnh để đăng IG Feed.")
        else:
            media_type = "CAROUSEL" if total_media > 1 else ("REELS" if video_urls else "PHOTO")

        timeout = httpx.Timeout(timeout=60.0, connect=10.0, read=60.0, write=30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                if media_type == "CAROUSEL":
                    async def create_child(url: str, is_video: bool, is_first: bool) -> str:
                        payload = {
                            "access_token": access_token,
                            "is_carousel_item": True,
                            "caption": caption if is_first else "",
                            "media_type": "VIDEO" if is_video else "IMAGE",
                            ("video_url" if is_video else "image_url"): url,
                        }
                        r = await client.post(f"{INSTAGRAM_GRAPH_API_URL}/{ig_user_id}/media", json=payload)
                        r.raise_for_status()
                        return r.json()["id"]

                    tasks = [
                        create_child(u, u in video_urls, i == 0)
                        for i, u in enumerate(image_urls + video_urls)
                    ]
                    children_ids: List[str] = await asyncio.gather(*tasks)

                    # Wait for each child to be ready
                    for media_id in children_ids:
                        for _ in range(10):
                            status_resp = await client.get(
                                f"{INSTAGRAM_GRAPH_API_URL}/{media_id}",
                                params={"fields": "status_code", "access_token": access_token},
                            )
                            status_resp.raise_for_status()
                            status = status_resp.json().get("status_code")
                            if status in ["FINISHED", "PUBLISHED"]:
                                break
                            await asyncio.sleep(2)

                    create_media_payload = {
                        "access_token": access_token,
                        "media_type": "CAROUSEL",
                        "children": ",".join(children_ids),
                        "caption": caption,
                    }
                    resp = await client.post(
                        f"{INSTAGRAM_GRAPH_API_URL}/{ig_user_id}/media", json=create_media_payload
                    )
                    resp.raise_for_status()
                    container_id = resp.json()["id"]
                else:
                    create_media_payload = {
                        "access_token": access_token,
                        "caption": caption,
                    }
                    if media_type == "PHOTO":
                        create_media_payload.update({
                            "media_type": "IMAGE",
                            "image_url": image_urls[0],
                        })
                    elif media_type in ("REELS", "VIDEO"):
                        create_media_payload.update({
                            "media_type": "REELS",
                            "video_url": video_urls[0],
                        })
                    resp = await client.post(
                        f"{INSTAGRAM_GRAPH_API_URL}/{ig_user_id}/media", json=create_media_payload
                    )
                    resp.raise_for_status()
                    container_id = resp.json()["id"]

                # Wait until container ready
                for _ in range(20):
                    status_resp = await client.get(
                        f"{INSTAGRAM_GRAPH_API_URL}/{container_id}",
                        params={"fields": "status_code", "access_token": access_token},
                    )
                    status_resp.raise_for_status()
                    status_code = status_resp.json().get("status_code")
                    if status_code in {"FINISHED", "PUBLISHED", "ERROR", "EXPIRED"}:
                        break
                    await asyncio.sleep(3)

                # Try publishing
                publish_payload = {
                    "access_token": access_token,
                    "creation_id": container_id,
                }
                try:
                    resp = await client.post(
                        f"{INSTAGRAM_GRAPH_API_URL}/{ig_user_id}/media_publish", json=publish_payload
                    )
                    resp.raise_for_status()
                    media_id = resp.json().get("id")
                except httpx.ReadTimeout:
                    # Fallback: check if IG already published
                    status_check = await client.get(
                        f"{INSTAGRAM_GRAPH_API_URL}/{container_id}",
                        params={"fields": "status_code", "access_token": access_token},
                    )
                    status_check.raise_for_status()
                    status_code = status_check.json().get("status_code")
                    if status_code != "PUBLISHED":
                        raise HTTPException(status_code=504, detail="IG không phản hồi kịp khi publish.")
                    media_id = container_id

                permalink = None
                if status_code == "FINISHED":
                    try:
                        pp_resp = await client.get(f"{INSTAGRAM_GRAPH_API_URL}/{media_id}?fields=permalink&access_token={access_token}")
                        pp_resp.raise_for_status()
                        permalink = pp_resp.json().get("permalink")
                    except Exception:
                        permalink = None

                platform_post.platform_post_id = media_id
                platform_post.status = "published" if status_code == "FINISHED" else "failed"
                platform_post.post_url = permalink if status_code == "FINISHED" else None
                psd["status_code"] = status_code
                if permalink:
                    psd["permalink"] = permalink
                platform_post.platform_specific_data = psd
                platform_post.published_at = now_vn_naive() if status_code == "FINISHED" else None
                platform_post.updated_at = now_vn_naive()

                await db.commit()
                await db.refresh(platform_post)

                return {
                    "media_id": media_id,
                    "status_code": status_code,
                    "post_url": permalink,
                }

            except Exception as e:
                await db.rollback()
                error_text = None
                if isinstance(e, httpx.HTTPStatusError) and e.response is not None:
                    try:
                        error_text = e.response.text
                    except Exception:
                        error_text = str(e.response)

                psd = platform_post.platform_specific_data or {}
                psd.update({"error": str(e), "error_response": error_text})
                platform_post.status = "failed"
                platform_post.platform_specific_data = psd
                platform_post.updated_at = now_vn_naive()
                await db.commit()
                await db.refresh(platform_post)

                raise HTTPException(status_code=400, detail=error_text or str(e)) from e


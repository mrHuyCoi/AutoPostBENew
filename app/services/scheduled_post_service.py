from fastapi import HTTPException, status, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from zoneinfo import ZoneInfo
import json
import traceback
import uuid

# Import models and repositories
from app.models.youtube_metadata import YouTubeContentType, YouTubeMetadata
from app.repositories.subscription_repository import SubscriptionRepository
from app.repositories.media_asset_repository import MediaAssetRepository
from app.repositories.platform_post_repository import PlatformPostRepository

# Import services
from app.services.file_storage_service import FileStorageService
from app.services.openai_service import AIService
from app.utils.crypto import token_encryption
from app.services.publishing_service import PublishingService

# Import schemas for response models
from app.schemas.schedule_schema import PlatformPostResponse, MediaAssetResponse, YouTubeMetadataResponse
from sqlalchemy.orm import selectinload
from sqlalchemy.future import select
from app.models.platform_post import PlatformPost
from app.models.social_account import SocialAccount

# ✅ Hàm chuẩn hóa thời gian giờ Việt không timezone
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class ScheduledPostService:
    @staticmethod
    async def generate_preview_content(
        prompt: str,
        platform_type: List[str],
        hashtags: Optional[List[str]],
        brand_name: Optional[str],
        posting_purpose: Optional[str],
        ai_platform: str,
        call_to_action: Optional[str],
        current_user_id: str,
        current_user_custom_prompt: Optional[str],
        current_user_openai_api_key: Optional[str],
        current_user_gemini_api_key: Optional[str]
    ) -> Dict:

        # Lấy custom system prompt nếu có
        user_prompt = current_user_custom_prompt or None

        # Xác thực và giải mã API key
        if ai_platform == "openai":
            if not current_user_openai_api_key:
                raise HTTPException(status_code=400, detail="Bạn cần nhập OpenAI API key.")
            user_api_key = token_encryption.decrypt(current_user_openai_api_key)
        elif ai_platform == "gemini":
            if not current_user_gemini_api_key:
                raise HTTPException(status_code=400, detail="Bạn cần nhập Gemini API key.")
            user_api_key = token_encryption.decrypt(current_user_gemini_api_key)
        else:
            raise HTTPException(status_code=400, detail="ai_platform không hợp lệ. Chỉ hỗ trợ: openai, gemini")

        supported_platforms = {
            "facebook-reels": "facebook-reels",
            "instagram-reels": "instagram-reels",
            "facebook-page": "facebook-page",
            "instagram-feed": "instagram-feed",
            "youtube": "youtube"
        }

        result = {}

        for pf in platform_type:
            mapped_platform = supported_platforms.get(pf)
            if not mapped_platform:
                continue 

            content = await AIService.generate_content(
                prompt=prompt,
                platform=mapped_platform,
                brand_name=brand_name,
                posting_purpose=posting_purpose,
                call_to_action=call_to_action,
                hashtags=hashtags,
                custom_system_prompt=user_prompt,
                ai_platform=ai_platform,
                api_key=user_api_key
            )

            result[mapped_platform] = {"content": content}

        return result

    @staticmethod
    async def publish_post_immediately(db: AsyncSession, post_id: uuid.UUID, current_user_id: uuid.UUID):
        """
        Ngay lập tức đăng một bài đăng đã sẵn sàng sử dụng Celery task.
        """
        try:
            # Sử dụng Celery task để đăng bài thay vì gọi trực tiếp
            from app.tasks.content_publisher_tasks import publish_single_post
            
            # Gọi task Celery để xử lý bất đồng bộ
            task_result = publish_single_post.delay(str(post_id), str(current_user_id))
            
            # Trả về task_id để có thể theo dõi trạng thái task nếu cần
            return {"task_id": task_result.id, "status": "processing"}
        except Exception as e:
            # Xử lý lỗi khi gọi Celery task
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Không thể đăng bài ngay lập tức: {str(e)}"
            )

    @staticmethod
    async def schedule_new_post(
        db: AsyncSession,
        current_user_id: uuid.UUID,
        prompt: str,
        preview_content: str,
        scheduled_at: str,
        platform_specific_data: str,
        media_files: Optional[List[UploadFile]],
        brand_name: Optional[str],
        posting_purpose: Optional[str],
        publish_immediately
    ):
        # Lấy thông tin gói đăng ký của người dùng
        user_subscription = await SubscriptionRepository.get_by_user_id(db, current_user_id)
        if not user_subscription or not user_subscription.subscription_plan:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Lỗi hệ thống: Không tìm thấy thông tin gói đăng ký của bạn.")
        subscription_plan = user_subscription.subscription_plan

        # --- Kiểm tra max_scheduled_days ---
        try:
            scheduled_at_dt = datetime.fromisoformat(scheduled_at.strip().replace("Z", "+00:00"))
            scheduled_at_dt = scheduled_at_dt.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)
        except Exception as dt_err:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Lỗi định dạng thời gian: {dt_err}")

        days_diff = (scheduled_at_dt.date() - datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()).days
        if days_diff > subscription_plan.max_scheduled_days:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Gói của bạn chỉ cho phép lên lịch trước tối đa {subscription_plan.max_scheduled_days} ngày."
            )

        # --- Kiểm tra max_videos_per_day (số lượng PlatformPost tạo trong ngày) ---
        today_start = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None)
        posts_created_today = await PlatformPostRepository.count_user_posts_today(db, current_user_id, today_start)

        if posts_created_today >= subscription_plan.max_videos_per_day:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Bạn đã đạt giới hạn {subscription_plan.max_videos_per_day} bài đăng/ngày của gói đăng ký."
            )

        # --- Kiểm tra max_stored_videos và storage_limit_gb (khi tải file) ---
        asset = None
        if media_files:
            new_files_total_size = sum(file.size for file in media_files)
            current_assets_count, current_storage_bytes = await MediaAssetRepository.get_user_media_asset_counts_and_size(db, current_user_id)

            if current_assets_count + len(media_files) > subscription_plan.max_stored_videos:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Bạn đã đạt giới hạn {subscription_plan.max_stored_videos} video được lưu trữ của gói đăng ký."
                )

            total_storage_bytes_after_upload = current_storage_bytes + new_files_total_size
            if total_storage_bytes_after_upload > subscription_plan.storage_limit_gb * 1024 * 1024 * 1024:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Bạn đã vượt quá giới hạn {subscription_plan.storage_limit_gb}GB lưu trữ của gói đăng ký."
                )

            uploaded_urls, storage_paths, file_names = [], [], []
            file_type = "image"
            for file in media_files:
                meta = await FileStorageService.upload_file_to_cms(file, str(current_user_id))
                uploaded_urls.append(meta["public_url"])
                storage_paths.append(meta["storage_path"])
                file_names.append(meta["file_name"])
                if "video" in meta["file_type"]:
                    file_type = "video"

            asset = await MediaAssetRepository.create_media_asset(db, {
                "user_id": current_user_id,
                "storage_path": ", ".join(storage_paths),
                "file_name": ", ".join(file_names),
                "file_type": file_type,
                "url": uploaded_urls,
                "size_bytes": new_files_total_size,
                "uploaded_at": now_vn_naive(),
                "updated_at": now_vn_naive(),
                "prompt_for_content": prompt,
                "brand_name": brand_name,
                "posting_purpose": posting_purpose
            })

        try:
            data_list = json.loads(platform_specific_data)
        except json.JSONDecodeError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="platform_specific_data phải là một chuỗi JSON hợp lệ.")

        try:
            parsed_preview = json.loads(preview_content)
        except json.JSONDecodeError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="preview_content phải là một chuỗi JSON hợp lệ.")

        created_post_ids = []

        for data in data_list:
            platform_type = data.get("platform_type")
            if platform_type == "facebook-reels" or platform_type == "facebook-page":
                platform = "facebook"
            elif platform_type == "instagram-reels" or platform_type == "instagram-feed":
                platform = "instagram"
            else:
                platform = "youtube"
            social_account_id = data.get("social_account_id")

            if not platform or not social_account_id:
                raise HTTPException(status_code=400, detail="Mỗi item trong platform_specific_data phải chứa 'platform' và 'social_account_id'.")

            # --- Kiểm tra media asset khi publish_immediately ---
            if publish_immediately:
                # Facebook Page KHÔNG cần media, các loại khác thì cần
                is_facebook_page = (platform == "facebook" and platform_type == "facebook-page")
                if not is_facebook_page and not asset:
                    raise HTTPException(
                        status_code=400,
                        detail="Bắt buộc phải đính kèm ảnh hoặc video khi đăng ngay với nền tảng này."
                    )

            generated_content = ""
            if platform == "youtube":
                generated_content = parsed_preview.get("youtube", {}).get("content").get("description", "")
            elif platform == "facebook" and platform_type == "facebook-reels":
                generated_content = parsed_preview.get("facebook-reels", {}).get("content")
            elif platform == "instagram" and platform_type == "instagram-reels":
                generated_content = parsed_preview.get("instagram-reels", {}).get("content")
            elif platform == "facebook" and platform_type == "facebook-page":
                generated_content = parsed_preview.get("facebook-page", {}).get("content")
            elif platform == "instagram" and platform_type == "instagram-feed":
                generated_content = parsed_preview.get("instagram-feed", {}).get("content")

            platform_post = await PlatformPostRepository.create_platform_post(db, {
                "user_id": current_user_id,
                "social_account_id": social_account_id,
                "platform": platform,
                "scheduled_at": scheduled_at_dt,
                "platform_specific_data": data,
                "generated_content": generated_content,
                "status": "ready"
            })
            created_post_ids.append(platform_post.id)

            if asset:
                await PlatformPostRepository.link_platform_post_with_media_asset(db, platform_post.id, asset.id)

            if platform == "youtube":
                youtube_data = parsed_preview.get("youtube", {}).get("content")
                title = youtube_data.get("title", "Untitled")
                if not title or not str(title).strip():
                    title = "Untitled"
                await PlatformPostRepository.create_youtube_metadata(db, {
                    "platform_post_id": platform_post.id,
                    "title": title,
                    "description": youtube_data.get("description", ""),
                    "tags": youtube_data.get("tags", []),
                    "privacy_status": "public",
                    "content_type": YouTubeContentType("regular"),
                    "shorts_hashtags": []
                })

        return {"message": "Đã đặt lịch thành công", "post_ids": created_post_ids}

    @staticmethod
    async def get_user_platform_posts(
        db: AsyncSession,
        current_user_id: uuid.UUID,
        skip: int = 0,
        limit: int = 100,
        status_filter: Optional[str] = None
    ) -> List[PlatformPostResponse]:
        query = select(PlatformPost).options(
            selectinload(PlatformPost.media_assets),
            selectinload(PlatformPost.youtube_metadata)
        ).where(
            PlatformPost.user_id == current_user_id
        )

        if status_filter:
            if status_filter == "unpublished":
                query = query.where(PlatformPost.status != 'published')
            else:
                query = query.where(PlatformPost.status == status_filter)

        query = query.order_by(PlatformPost.created_at.desc()).offset(skip).limit(limit)

        posts = (await db.execute(query)).scalars().all()

        response = []
        for post in posts:
            # Handle media assets with proper validation
            media_assets = []
            for asset in post.media_assets:
                try:
                    # Handle case where url might be a dict instead of string/list
                    asset_data = {
                        "id": asset.id,
                        "user_id": asset.user_id,
                        "storage_path": asset.storage_path,
                        "file_name": asset.file_name,
                        "file_type": asset.file_type,
                        "duration": asset.duration,
                        "brand_name": asset.brand_name,
                        "posting_purpose": asset.posting_purpose,
                        "uploaded_at": asset.uploaded_at,
                        "updated_at": asset.updated_at,
                        "prompt_for_content": asset.prompt_for_content
                    }
                    
                    # Handle url field - convert dict to None if it's not a string/list
                    if asset.url is not None:
                        if isinstance(asset.url, (str, list)):
                            asset_data["url"] = asset.url
                        else:
                            # If url is a dict (like {'deleted': True}), set to None
                            asset_data["url"] = None
                    else:
                        asset_data["url"] = None
                    
                    media_assets.append(MediaAssetResponse.model_validate(asset_data))
                except Exception as e:
                    print(f"Error validating media asset {asset.id}: {e}")
                    continue
            
            # Handle youtube metadata with proper validation
            youtube_metadata = None
            if post.youtube_metadata is not None:
                try:
                    youtube_metadata = YouTubeMetadataResponse.model_validate(post.youtube_metadata)
                except Exception as e:
                    print(f"Error validating youtube metadata for post {post.id}: {e}")
                    youtube_metadata = None
            
            # # Lấy thông tin profile picture từ social account
            # profile_picture = None
            # if post.social_account:
            #     try:
            #         profile_picture = await ScheduledPostService._get_profile_picture(
            #             post.social_account.platform,
            #             post.social_account.account_id,
            #             post.social_account.access_token
            #         )
            #     except Exception as e:
            #         print(f"Lỗi khi lấy profile picture cho {post.social_account.platform}: {e}")
            #         profile_picture = None
            
            response.append(
                PlatformPostResponse(
                    id=post.id,
                    user_id=post.user_id,
                    social_account_id=post.social_account_id,
                    platform=post.platform,
                    status=post.status,
                    scheduled_at=post.scheduled_at,
                    platform_type=post.platform_specific_data.get("platform_type", ""),
                    generated_content=post.generated_content,
                    post_url=post.post_url,
                    created_at=post.created_at,
                    updated_at=post.updated_at,
                    media_assets=media_assets,
                    youtube_metadata=youtube_metadata
                )
            )
        return response


    @staticmethod
    async def update_scheduled_post(
        db: AsyncSession,
        post_id: uuid.UUID,
        current_user_id: uuid.UUID,
        preview_content: Optional[str] = None,
        scheduled_at: Optional[str] = None,
        platform_specific_data: Optional[str] = None,
        media_files: Optional[List[UploadFile]] = None,
        brand_name: Optional[str] = None,
        posting_purpose: Optional[str] = None
    ):
        """Chỉ cho phép cập nhật trường preview_content và scheduled_at của platform post đã lên lịch"""
        # Kiểm tra xem post có tồn tại và thuộc về user hiện tại không
        platform_post = await PlatformPostRepository.get_platform_post_by_id(db, post_id, current_user_id)
        if not platform_post:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Không tìm thấy bài đăng đã lên lịch")
        # Kiểm tra trạng thái của post, chỉ cho phép cập nhật nếu chưa được đăng
        if platform_post.status == "published":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Không thể cập nhật bài đăng đã được xuất bản"
            )
        update_data = {}
        # Xử lý scheduled_at nếu có
        if scheduled_at is not None and scheduled_at != "":
            try:
                scheduled_at_dt = datetime.fromisoformat(scheduled_at.strip().replace("Z", "+00:00"))
                scheduled_at_dt = scheduled_at_dt.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)
                update_data["scheduled_at"] = scheduled_at_dt
            except Exception as dt_err:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Lỗi định dạng thời gian: {dt_err}")
        # Xử lý preview_content nếu có
        if preview_content is not None and preview_content != "":
            try:
                parsed_preview = json.loads(preview_content)
                platform = platform_post.platform
                platform_type = platform_post.platform_specific_data.get("platform_type", "")
                generated_content = ""
                if platform == "youtube":
                    generated_content = parsed_preview.get("youtube", {}).get("content", {}).get("description", "")
                elif platform == "facebook" and platform_type == "facebook-reels":
                    generated_content = parsed_preview.get("facebook-reels", {}).get("content", "")
                elif platform == "instagram" and platform_type == "instagram-reels":
                    generated_content = parsed_preview.get("instagram-reels", {}).get("content", "")
                elif platform == "facebook" and platform_type == "facebook-page":
                    generated_content = parsed_preview.get("facebook-page", {}).get("content", "")
                elif platform == "instagram" and platform_type == "instagram-feed":
                    generated_content = parsed_preview.get("instagram-feed", {}).get("content", "")
                update_data["generated_content"] = generated_content
                # Cập nhật YouTube metadata nếu có
                if platform == "youtube" and platform_post.youtube_metadata:
                    youtube_data = parsed_preview.get("youtube", {}).get("content", {})
                    youtube_metadata_update = {}
                    if "title" in youtube_data:
                        title = youtube_data.get("title", "Untitled")
                        if not title or not str(title).strip():
                            title = "Untitled"
                        youtube_metadata_update["title"] = title
                    if "description" in youtube_data:
                        youtube_metadata_update["description"] = youtube_data.get("description", "")
                    if "tags" in youtube_data:
                        youtube_metadata_update["tags"] = youtube_data.get("tags", [])
                    if youtube_metadata_update:
                        # Cập nhật YouTube metadata
                        await db.execute(
                            update(YouTubeMetadata)
                            .where(YouTubeMetadata.platform_post_id == post_id)
                            .values(**youtube_metadata_update)
                        )
            except json.JSONDecodeError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="preview_content phải là một chuỗi JSON hợp lệ.")
        # Cập nhật platform post
        if update_data:
            success = await PlatformPostRepository.update_platform_post(db, post_id, current_user_id, update_data)
            if not success:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Không thể cập nhật bài đăng")
        return {"message": "Đã cập nhật bài đăng thành công", "post_id": post_id}
    
    @staticmethod
    async def delete_scheduled_post(
        db: AsyncSession,
        post_id: uuid.UUID,
        current_user_id: uuid.UUID
    ):
        """Xóa một platform post đã lên lịch"""
        # Kiểm tra xem post có tồn tại và thuộc về user hiện tại không
        platform_post = await PlatformPostRepository.get_platform_post_by_id(db, post_id, current_user_id)
        if not platform_post:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Không tìm thấy bài đăng đã lên lịch")
        
        # Kiểm tra trạng thái của post, chỉ cho phép xóa nếu chưa được đăng
        if platform_post.status == "published":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Không thể xóa bài đăng đã được xuất bản"
            )
        
        # Xóa platform post
        success = await PlatformPostRepository.delete_platform_post(db, post_id, current_user_id)
        if not success:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Không thể xóa bài đăng")
        
        return {"message": "Đã xóa bài đăng thành công", "post_id": post_id}

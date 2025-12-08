from fastapi import APIRouter, Depends, HTTPException, status, Body, UploadFile, File, Form
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List, Dict
import traceback
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)

from app.database.database import get_db
from app.dto.response import ResponseModel
from app.middlewares.subscription_middleware import check_active_subscription
from app.repositories.platform_post_repository import PlatformPostRepository
from app.schemas.schedule_schema import PlatformPostResponse
from app.schemas.script_schema import ScriptGenerationRequest, ScriptGenerationResponse
from app.schemas.video_generation_schema import VideoGenerationRequest, VideoGenerationResponse
from app.services.scheduled_post_service import ScheduledPostService
from app.services.script_generation_service import ScriptGenerationService
from app.services.video_generation_service import VideoGenerationService
from app.utils.crypto import token_encryption
from app.logging.decorators import with_log_context
router = APIRouter()


@router.post("/generate-script", response_model=ResponseModel[ScriptGenerationResponse])
async def generate_video_script(
    payload: ScriptGenerationRequest,
    current_user=Depends(check_active_subscription(required_max_videos_per_day=1)),
):
    """
    Endpoint tạo kịch bản video bằng AI dựa trên các thông tin đầu vào.
    """
    try:
        provider = payload.llm_provider.lower()

        # Ưu tiên khóa API gửi lên từ client; nếu không có thì lấy theo user đang đăng nhập
        openai_key = payload.openai_key
        gemini_key = payload.gemini_key

        if provider == "openai" and not openai_key and getattr(current_user, "openai_api_key", None):
            openai_key = token_encryption.decrypt(current_user.openai_api_key)
        if provider == "gemini" and not gemini_key and getattr(current_user, "gemini_api_key", None):
            gemini_key = token_encryption.decrypt(current_user.gemini_api_key)

        script = await ScriptGenerationService.generate_script(
            video_subject=payload.video_subject,
            video_language=payload.video_language,
            video_keywords=payload.video_keywords,
            script_style=payload.script_style,
            paragraph_number=payload.paragraph_number,
            llm_provider=payload.llm_provider,
            openai_key=openai_key,
            gemini_key=gemini_key,
        )

        return ResponseModel.success(
            data=ScriptGenerationResponse(script=script),
            message="Sinh kịch bản thành công",
        )
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi khi sinh kịch bản: {str(e)}")


@router.post("/generate-video", response_model=ResponseModel[VideoGenerationResponse])
async def generate_video_with_ai(
    payload: VideoGenerationRequest,
    current_user=Depends(check_active_subscription(required_max_videos_per_day=1)),
    db: AsyncSession = Depends(get_db),
):
    """
    Endpoint tạo video bằng AI dựa trên kịch bản và các thông số cấu hình.
    
    API này sẽ:
    1. Nhận kịch bản video và các thông số cấu hình
    2. Tạo video với TTS, materials, BGM, subtitles
    3. Trả về video_id để theo dõi tiến trình
    
    Video generation là một process async, client cần poll status endpoint
    để kiểm tra tiến trình.
    """
    try:
        # Ưu tiên API keys từ request, nếu không có thì lấy từ user
        openai_key = payload.openai_key
        gemini_key = payload.gemini_key
        
        if not openai_key and getattr(current_user, "openai_api_key", None):
            try:
                openai_key = token_encryption.decrypt(current_user.openai_api_key)
            except Exception:
                logger.warning(f"Không thể decrypt OpenAI key cho user {current_user.id}")
        
        if not gemini_key and getattr(current_user, "gemini_api_key", None):
            try:
                gemini_key = token_encryption.decrypt(current_user.gemini_api_key)
            except Exception:
                logger.warning(f"Không thể decrypt Gemini key cho user {current_user.id}")
        
        # Gọi service để tạo video
        result = await VideoGenerationService.generate_video(
            db=db,
            user_id=current_user.id,
            request=payload,
            openai_key=openai_key,
            gemini_key=gemini_key,
        )
        
        return ResponseModel.success(
            data=result,
            message="Đã khởi tạo quá trình tạo video. Vui lòng sử dụng video_id để theo dõi tiến trình.",
        )
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi tạo video: {str(e)}",
        )


@router.get("/video-status/{video_id}", response_model=ResponseModel[VideoGenerationResponse])
async def get_video_generation_status(
    video_id: str,
    current_user=Depends(check_active_subscription()),
    db: AsyncSession = Depends(get_db),
):
    """
    Lấy trạng thái của video generation job.
    
    Args:
        video_id: ID của video cần kiểm tra
        
    Returns:
        VideoGenerationResponse với trạng thái hiện tại
    """
    try:
        result = await VideoGenerationService.get_video_status(
            db=db,
            video_id=video_id,
            user_id=current_user.id,
        )
        return ResponseModel.success(data=result, message="Lấy trạng thái video thành công")
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy trạng thái video: {str(e)}",
        )


@router.post("/generate-review")
async def generate_review_content(
    prompt: str = Body(..., embed=True),
    platform_type: List[str] = Body(..., embed=True),
    hashtags: Optional[List[str]] = Body(None, embed=True),
    brand_name: Optional[str] = Body(None, embed=True),
    call_to_action: Optional[str] = Body(None, embed=True),
    posting_purpose: Optional[str] = Body(None, embed=True),
    ai_platform: str = Body("gemini", embed=True),
    current_user=Depends(check_active_subscription(required_max_videos_per_day=1)),
):
    """
    Endpoint để sinh trước nội dung review cho bài đăng.
    - Logic được xử lý trong ScheduledPostService.
    """
    try:
        result = await ScheduledPostService.generate_preview_content(
            prompt=prompt,
            platform_type=platform_type,
            hashtags=hashtags,
            brand_name=brand_name,
            posting_purpose=posting_purpose,
            ai_platform=ai_platform,
            call_to_action=call_to_action,
            current_user_id=current_user.id,
            current_user_custom_prompt=current_user.custom_system_prompt,
            current_user_openai_api_key=current_user.openai_api_key,
            current_user_gemini_api_key=current_user.gemini_api_key
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi khi sinh nội dung: {str(e)}")


@router.post("/schedule-post")
@with_log_context(
    event_action="scheduled_video.schedule_post",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def schedule_post(
    prompt: str = Form(...),
    preview_content: str = Form(...),
    scheduled_at: str = Form(...),
    platform_specific_data: str = Form(...),
    media_files: Optional[List[UploadFile]] = File(None),
    brand_name: Optional[str] = Form(None),
    posting_purpose: Optional[str] = Form(None),
    publish_immediately: bool = Form(False),
    current_user=Depends(check_active_subscription()),
    db: AsyncSession = Depends(get_db)
):
    """
    Endpoint để lên lịch đăng bài mới hoặc đăng bài ngay lập tức.
    - Nếu publish_immediately=True: Bài viết sẽ được đăng ngay lập tức.
    - Nếu publish_immediately=False: Bài viết sẽ được lên lịch đăng vào thời điểm scheduled_at.
    - Logic nghiệp vụ được chuyển sang ScheduledPostService.
    - Controller chỉ chịu trách nhiệm nhận request và gọi service.
    """
    try:
        effective_scheduled_at = scheduled_at
        if publish_immediately:
            # Nếu đăng ngay, ghi đè thời gian hẹn lịch bằng thời gian hiện tại
            now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
            effective_scheduled_at = now.isoformat()

        # 1. Luôn tạo bản ghi bài đăng trong DB trước
        result = await ScheduledPostService.schedule_new_post(
            db=db,
            current_user_id=current_user.id,
            prompt=prompt,
            preview_content=preview_content,
            scheduled_at=effective_scheduled_at,
            platform_specific_data=platform_specific_data,
            media_files=media_files,
            brand_name=brand_name,
            posting_purpose=posting_purpose,
            publish_immediately=publish_immediately
        )

        # 2. Nếu yêu cầu đăng ngay, gọi service để đăng
        if publish_immediately:
            post_ids = result.get("post_ids", [])
            for post_id in post_ids:
                await ScheduledPostService.publish_post_immediately(
                    db=db,
                    post_id=post_id,
                    current_user_id=current_user.id
                )
            return {"message": "Đã đăng bài thành công", "post_ids": post_ids}

        # 3. Nếu chỉ lên lịch, trả về kết quả từ schedule_new_post
        return result
    except HTTPException as e:
        # Ném lại các lỗi HTTP đã được xử lý từ service
        raise e
    except Exception as e:
        # Bắt các lỗi không mong muốn khác
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Lỗi máy chủ nội bộ khi xử lý bài đăng: {str(e)}")


@router.get("/platform-posts", response_model=List[PlatformPostResponse])
@with_log_context(
    event_action="scheduled_video.platform_posts.list",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def get_platform_posts_by_user(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription()),
    skip: int = 0,
    limit: int = 100
):
    """
    Lấy danh sách platform_post theo user hiện tại.
    """
    try:
        return await ScheduledPostService.get_user_platform_posts(
            db=db,
            current_user_id=current_user.id,
            skip=skip,
            limit=limit
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy danh sách platform_post: {e}")

@router.get("/platform-posts/published", response_model=List[PlatformPostResponse])
@with_log_context(
    event_action="scheduled_video.platform_posts.list_published",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def get_published_platform_posts_by_user(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription()),
    skip: int = 0,
    limit: int = 100
):
    """
    Lấy danh sách platform_post đã đăng (status='published') theo user hiện tại.
    """
    try:
        return await ScheduledPostService.get_user_platform_posts(
            db=db,
            current_user_id=current_user.id,
            skip=skip,
            limit=limit,
            status_filter="published"
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy danh sách platform_post đã đăng: {e}")

@router.get("/platform-posts/unpublished", response_model=List[PlatformPostResponse])
@with_log_context(
    event_action="scheduled_video.platform_posts.list_unpublished",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def get_unpublished_platform_posts_by_user(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription()),
    skip: int = 0,
    limit: int = 100
):
    """
    Lấy danh sách platform_post chưa đăng (status khác 'published') theo user hiện tại.
    """
    try:
        return await ScheduledPostService.get_user_platform_posts(
            db=db,
            current_user_id=current_user.id,
            skip=skip,
            limit=limit,
            status_filter="unpublished" # Hoặc một giá trị khác để lọc các trạng thái chưa đăng
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy danh sách platform_post chưa đăng: {e}")


@router.put("/platform-posts/{post_id}", response_model=dict)
@with_log_context(
    event_action="scheduled_video.platform_post.update",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def update_scheduled_post(
    post_id: uuid.UUID,
    preview_content: Optional[str] = Form(None),
    scheduled_at: Optional[str] = Form(None),
    current_user=Depends(check_active_subscription()),
    db: AsyncSession = Depends(get_db)
):
    """
    Endpoint cho phép cập nhật trường preview_content và scheduled_at của bài đăng đã lên lịch.
    """
    try:
        result = await ScheduledPostService.update_scheduled_post(
            db=db,
            post_id=post_id,
            current_user_id=current_user.id,
            preview_content=preview_content,
            scheduled_at=scheduled_at
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Lỗi máy chủ nội bộ khi cập nhật bài đăng: {str(e)}")


@router.patch("/platform-posts/{post_id}/retry", response_model=dict)
@with_log_context(
    event_action="scheduled_video.platform_post.retry",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def retry_failed_post(
    post_id: uuid.UUID,
    current_user=Depends(check_active_subscription()),
    db: AsyncSession = Depends(get_db)
):
    """
    Chuyển đổi trạng thái bài đăng từ 'failed' thành 'ready' để có thể thử đăng lại.
    """
    try:
        # Lấy thông tin bài đăng
        platform_post = await PlatformPostRepository.get_platform_post_by_id(db, post_id, current_user.id)
        if not platform_post:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Không tìm thấy bài đăng")
        
        # Kiểm tra trạng thái hiện tại
        if platform_post.status != "failed":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Chỉ có thể thử lại các bài đăng có trạng thái 'failed'"
            )
        
        # Cập nhật trạng thái thành 'ready'
        update_data = {
            "status": "ready",
            "updated_at": datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)
        }
        
        success = await PlatformPostRepository.update_platform_post(db, post_id, current_user.id, update_data)
        if not success:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Không thể cập nhật trạng thái bài đăng")
        
        return {"message": "Đã chuyển trạng thái bài đăng thành 'ready' để thử lại", "post_id": post_id}
    except HTTPException as e:
        raise e
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Lỗi máy chủ nội bộ: {str(e)}")

@router.post("/platform-posts/{post_id}/publish", response_model=dict)
@with_log_context(
    event_action="scheduled_video.platform_post.publish",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def publish_post(
    post_id: uuid.UUID,
    current_user=Depends(check_active_subscription()),
    db: AsyncSession = Depends(get_db)
):
    """
    Đăng lại bài đăng có trạng thái 'ready'.
    """
    try:
        # Lấy thông tin bài đăng
        platform_post = await PlatformPostRepository.get_platform_post_by_id(db, post_id, current_user.id)
        if not platform_post:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Không tìm thấy bài đăng")
        
        # Kiểm tra trạng thái hiện tại
        if platform_post.status != "ready":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Chỉ có thể đăng các bài đăng có trạng thái 'ready'"
            )
        
        # Gọi service để đăng bài
        await ScheduledPostService.publish_post_immediately(db, post_id, current_user.id)
        
        return {"message": "Đã gửi yêu cầu đăng bài", "post_id": post_id}
    except HTTPException as e:
        raise e
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Lỗi máy chủ nội bộ: {str(e)}")

@router.delete("/platform-posts/{post_id}", response_model=dict)
@with_log_context(
    event_action="scheduled_video.platform_post.delete",
    event_category="scheduled_video",
    source_layer="controller",
    source_controller="scheduled_video_controller",
)
async def delete_scheduled_post(
    post_id: uuid.UUID,
    current_user=Depends(check_active_subscription()),
    db: AsyncSession = Depends(get_db)
):
    """
    Endpoint để xóa bài đăng đã lên lịch.
    - Chỉ có thể xóa bài đăng chưa được xuất bản.
    """
    try:
        result = await ScheduledPostService.delete_scheduled_post(
            db=db,
            post_id=post_id,
            current_user_id=current_user.id
        )
        return result
    except HTTPException as e:
        # Ném lại các lỗi HTTP đã được xử lý từ service
        raise e
    except Exception as e:
        # Bắt các lỗi không mong muốn khác
        traceback.print_exc()
        # get_db dependency sẽ tự động rollback
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Lỗi máy chủ nội bộ khi xóa bài đăng: {str(e)}")

from fastapi import APIRouter, Depends, HTTPException, status, Body
import uuid
from app.database.database import get_db
from app.services.instagram_acc_service import InstagramService
from app.middlewares.subscription_middleware import check_active_subscription
from app.logging.decorators import with_log_context
from sqlalchemy.ext.asyncio import AsyncSession
import traceback

router = APIRouter()

@router.post("/publish", status_code=200)
@with_log_context(
    event_action="instagram.publish_platform_post",
    event_category="instagram",
    source_layer="controller",
    source_controller="instagram_controller",
)
async def publish_platform_post_instagram(
    platform_post_id: uuid.UUID = Body(..., embed=True, description="ID của bản ghi platform_posts muốn đăng lên Instagram"),
    db: AsyncSession = Depends(get_db),
    current_user = Depends(check_active_subscription(required_max_social_accounts=1))
):
    """
    Đăng bài lên Instagram dựa trên bản ghi platform_posts.
    Chỉ cho phép user sở hữu platform_post này.
    """
    try:
        from app.models.platform_post import PlatformPost
        from app.models.post import Post
        # Lấy platform_post và kiểm tra quyền
        platform_post = await db.get(PlatformPost, platform_post_id)
        if not platform_post:
            raise HTTPException(status_code=404, detail="Không tìm thấy bản ghi platform_post.")
        post = await db.get(Post, platform_post.post_id)
        if not post or post.user_id != current_user.id:
            raise HTTPException(status_code=403, detail="Bạn không có quyền đăng bài này.")
        # Gọi service đăng bài
        result = await InstagramService.publish_platform_post(
            db=db,
            platform_post_id=platform_post_id
        )
        return {"success": True, "data": result}
    except HTTPException as e:
        raise e
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi khi đăng bài lên Instagram: {str(e)}") 
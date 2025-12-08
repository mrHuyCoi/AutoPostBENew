import traceback
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.platform_post import PlatformPost
from app.services.facebook_acc_service import FacebookService
from app.services.instagram_acc_service import InstagramService
from app.services.youtube_acc_service import YouTubeService
from app.repositories.platform_post_repository import PlatformPostRepository


class PublishingService:
    @staticmethod
    async def publish_post(db: AsyncSession, post_id: str, user_id: str):
        """
        Lấy một PlatformPost theo ID và đăng nó lên nền tảng tương ứng.
        Cập nhật trạng thái bài đăng (published hoặc failed).
        """
        post_to_publish = await PlatformPostRepository.get_platform_post_by_id(db, post_id, user_id)

        if not post_to_publish:
            # Nếu không tìm thấy post, có thể log lại hoặc bỏ qua
            print(f"Post with id {post_id} not found for user {user_id}. Skipping publish.")
            return

        try:
            print(f"Publishing post {post_to_publish.id} to {post_to_publish.platform}...")
            if post_to_publish.platform == "facebook":
                await FacebookService.publish_platform_post(db, str(post_to_publish.id))
            elif post_to_publish.platform == "instagram":
                await InstagramService.publish_platform_post(db, str(post_to_publish.id))
            elif post_to_publish.platform == "youtube":
                await YouTubeService.publish_platform_post(db, str(post_to_publish.id))
            # Cập nhật trạng thái sau khi đăng thành công (logic này đã nằm trong các service con)
            print(f"Successfully published post {post_to_publish.id}")

        except Exception as e:
            # Xử lý lỗi nếu đăng bài thất bại
            print(f"Failed to publish post {post_to_publish.id}. Error: {e}")
            traceback.print_exc()
            
            post_to_publish.status = "failed"
            # Lưu thông tin lỗi để debug
            error_info = {
                "publish_error": str(e),
                "traceback": traceback.format_exc()
            }
            # Cập nhật platform_specific_data một cách an toàn
            if isinstance(post_to_publish.platform_specific_data, dict):
                post_to_publish.platform_specific_data.update(error_info)
            else:
                post_to_publish.platform_specific_data = error_info
                
            post_to_publish.updated_at = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)
            
            # Commit thay đổi trạng thái 'failed'
            await db.commit()
            await db.refresh(post_to_publish)
            # Ném lại lỗi để controller có thể bắt và trả về response phù hợp
            raise e 
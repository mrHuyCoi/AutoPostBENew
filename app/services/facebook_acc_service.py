import uuid
import requests
import httpx  # Thêm thư viện httpx
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo
from app.models.social_account import SocialAccount
from app.utils.crypto import token_encryption
from app.dto.facebook_dto import FacebookTokenRequest, PlatformEnum
from app.exceptions.api_exceptions import BadRequestException, UnauthorizedException
# from app.models.post import Post
from app.models.platform_post import PlatformPost
from app.models.media_asset import MediaAsset
# from app.models.post_media import PostMedia
from app.services.facebook_service import (
    post_text_to_facebook_page,
    post_photo_to_facebook_page,
    post_video_to_facebook_page,
    post_reel_to_facebook_page,
    poll_facebook_video_status,
    post_multiple_photos_to_facebook_page,
)
from sqlalchemy.exc import NoResultFound
from app.services.instagram_acc_service import InstagramService
from app.models.platform_post_media_asset import platform_post_media_asset
from app.configs.settings import settings

def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class FacebookService:
    """
    Service xử lý các thao tác liên quan đến tài khoản mạng xã hội.
    """
    
    @staticmethod
    async def add_facebook_account(
        db: AsyncSession,
        user_id: uuid.UUID,
        data: FacebookTokenRequest  # DTO giờ chứa user_access_token và platform
    ) -> SocialAccount:
        """
        Thêm tài khoản Facebook Page hoặc Instagram Business Account vào hệ thống.
        1. Lấy User ID từ API /me.
        2. Lấy danh sách Page từ API /accounts.
        3. Chọn Facebook Page đầu tiên.
        4. Nếu platform là 'instagram', lấy Instagram Business Account liên kết với Page đó.
        5. Lưu thông tin tài khoản (Facebook Page hoặc Instagram Account).
        
        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng (trong hệ thống của chúng ta, không phải Facebook User ID)
            data: Dữ liệu chứa User Access Token và platform (facebook/instagram)
            
        Returns:
            Đối tượng SocialAccount đã được tạo hoặc cập nhật
            
        Raises:
            BadRequestException: Nếu thông tin không hợp lệ, không tìm thấy tài khoản (Page/Instagram), hoặc token không hợp lệ.
            UnauthorizedException: Nếu token không có quyền truy cập cần thiết.
        """
        fb_user_id_from_token = None
        page_id_to_add = None
        page_name_to_add = None
        page_access_token_to_add = None

        try:
            async with httpx.AsyncClient() as client:
                fb_api_version = "v23.0"  # Hoặc phiên bản API bạn muốn dùng

                # Bước 1: Gọi API /me để lấy Facebook User ID từ User Access Token
                me_url = f"https://graph.facebook.com/{fb_api_version}/me"
                me_params = {"access_token": data.user_access_token, "fields": "id,name"}
                
                print(f"Đang gọi API Facebook: GET {me_url} với token")
                me_response = await client.get(me_url, params=me_params)
                me_response.raise_for_status() # Ném lỗi nếu HTTP status là 4xx hoặc 5xx
                me_data = me_response.json()
                
                fb_user_id_from_token = me_data.get("id")
                user_name_from_token = me_data.get("name") # Có thể dùng để log hoặc kiểm tra

                if not fb_user_id_from_token:
                    raise BadRequestException("Không thể lấy được Facebook User ID từ User Access Token cung cấp. Token có thể không hợp lệ hoặc thiếu quyền truy cập thông tin cơ bản.")
                
                print(f"Lấy thành công Facebook User ID: {fb_user_id_from_token} (Tên: {user_name_from_token}) từ token.")

                # Bước 2: Gọi API /accounts để lấy danh sách Page người dùng quản lý
                accounts_url = f"https://graph.facebook.com/{fb_api_version}/{fb_user_id_from_token}/accounts"
                # Yêu cầu các trường: id (Page ID), name (tên Page), access_token (Page Access Token)
                accounts_params = {"access_token": data.user_access_token, "fields": "id,name,access_token,category,tasks"} 
                
                print(f"Đang gọi API Facebook: GET {accounts_url} với token")
                accounts_response = await client.get(accounts_url, params=accounts_params)
                accounts_response.raise_for_status() # Ném lỗi nếu HTTP status là 4xx hoặc 5xx
                pages_data = accounts_response.json()

                if "data" not in pages_data or not pages_data["data"]:
                    raise BadRequestException(f"Người dùng Facebook (ID: {fb_user_id_from_token}) không quản lý Page nào, hoặc User Token không có quyền 'pages_show_list' cho User này.")

                # Tự động chọn Page đầu tiên từ danh sách
                first_page_info = pages_data["data"][0]
                
                page_id_to_add = first_page_info.get("id") # Đây là Facebook Page ID
                page_name_to_add = first_page_info.get("name")
                page_access_token_to_add = first_page_info.get("access_token") # Đây là Page Access Token

                if not page_id_to_add or not page_name_to_add or not page_access_token_to_add:
                    # Trường hợp này ít khi xảy ra nếu API trả về dữ liệu page hợp lệ
                    raise BadRequestException(f"Không thể lấy đủ thông tin (ID, tên, page access token) từ Page đầu tiên (Facebook Page ID: {page_id_to_add}) được Facebook trả về. Dữ liệu API có thể không đầy đủ.")
                
                print(f"Đã chọn Facebook Page đầu tiên: ID='{page_id_to_add}', Name='{page_name_to_add}'. Sẽ xử lý dựa trên platform: {data.platform.value}")

                # Bước 3: Xử lý dựa trên platform
                if data.platform == PlatformEnum.INSTAGRAM:
                    if not page_access_token_to_add: # Cần Page Access Token để lấy IG account
                        raise BadRequestException(f"Không có Page Access Token cho Facebook Page ID: {first_page_info.get('id')}, không thể lấy thông tin Instagram Account.")

                    print(f"Platform là Instagram. Đang lấy Instagram Business Account liên kết với Facebook Page ID: {first_page_info.get('id')}")
                    ig_account_url = f"https://graph.facebook.com/{fb_api_version}/{first_page_info.get('id')}"
                    # Yêu cầu trường instagram_business_account và các sub-fields của nó
                    ig_account_params = {
                        "access_token": page_access_token_to_add, # Sử dụng Page Access Token
                        "fields": "instagram_business_account{id,username,name,profile_picture_url}" 
                    }
                    
                    print(f"Đang gọi API Facebook: GET {ig_account_url} với Page token để lấy IG account")
                    ig_response = await client.get(ig_account_url, params=ig_account_params)
                    ig_response.raise_for_status() # Ném lỗi nếu HTTP status là 4xx hoặc 5xx
                    ig_data = ig_response.json()

                    if "instagram_business_account" not in ig_data or not ig_data["instagram_business_account"]:
                        raise BadRequestException(f"Facebook Page (ID: {first_page_info.get('id')}, Tên: '{first_page_info.get('name')}') không có Instagram Business Account nào được liên kết, hoặc token thiếu quyền (cần instagram_basic, pages_show_list). Hãy đảm bảo tài khoản Instagram là tài khoản doanh nghiệp và đã kết nối đúng với Trang Facebook.")
                    
                    ig_business_account = ig_data["instagram_business_account"]
                    ig_account_id_val = ig_business_account.get("id")
                    ig_username_val = ig_business_account.get("username") # Ưu tiên username
                    ig_name_val = ig_business_account.get("name") # Tên hiển thị, có thể dùng nếu username không có
                    ig_profile_picture_url = ig_business_account.get("profile_picture_url") # URL ảnh đại diện

                    if not ig_account_id_val:
                        raise BadRequestException(f"Không thể lấy được ID của Instagram Business Account từ Page (ID: {first_page_info.get('id')}). Dữ liệu API có thể không đầy đủ.")
                    
                    # Cập nhật thông tin để lưu là của Instagram
                    page_id_to_add = ig_account_id_val       # Đây sẽ là Instagram Business Account ID
                    page_name_to_add = ig_username_val if ig_username_val else ig_name_val # Ưu tiên username
                    # page_access_token_to_add vẫn là của Facebook Page, vì nó dùng để quản lý IG

                    print(f"Đã lấy thông tin Instagram Account: ID={page_id_to_add}, Username/Name='{page_name_to_add}'")
                else: # Platform là Facebook
                    print(f"Platform là Facebook. Sử dụng thông tin Facebook Page: ID={page_id_to_add}, Name='{page_name_to_add}'")

        except httpx.HTTPStatusError as e:
            error_detail = "Unknown error"
            try:
                error_detail = e.response.json().get("error", {}).get("message", str(e))
            except Exception:
                error_detail = str(e)
            if e.response.status_code == 400:
                 raise BadRequestException(f"Lỗi từ Facebook API khi lấy danh sách Page: {error_detail}")
            elif e.response.status_code in [401, 403]:
                 raise UnauthorizedException(f"User Token không hợp lệ hoặc không có quyền truy cập Facebook API: {error_detail}")
            else:
                 raise BadRequestException(f"Lỗi kết nối đến Facebook API ({e.response.status_code}): {error_detail}")
        except httpx.RequestError as e:
            raise BadRequestException(f"Lỗi kết nối mạng khi gọi Facebook API: {str(e)}")
        except Exception as e: # Bắt các lỗi khác như JSON decode error
            raise BadRequestException(f"Lỗi không xác định khi xử lý phản hồi từ Facebook: {str(e)}")

        # Kiểm tra xem tài khoản đã tồn tại trong hệ thống của bạn chưa
        stmt = select(SocialAccount).filter(
            SocialAccount.user_id == user_id,
            SocialAccount.platform == data.platform.value,
            SocialAccount.account_id == page_id_to_add
        )
        result = await db.execute(stmt)
        existing_account = result.scalars().first()
        
        # Mã hóa Page Access Token trước khi lưu
        encrypted_page_token = token_encryption.encrypt(page_access_token_to_add)
        
        # Lấy thumbnail từ API hoặc từ dữ liệu đã có
        thumbnail = None
        if data.platform == PlatformEnum.INSTAGRAM and 'ig_profile_picture_url' in locals():
            thumbnail = locals()['ig_profile_picture_url']
        
        if not thumbnail:
            try:
                thumbnail = await FacebookService._get_account_thumbnail(
                    data.platform.value,
                    page_id_to_add,
                    encrypted_page_token
                )
            except Exception as e:
                print(f"Lỗi khi lấy thumbnail cho {data.platform.value}: {e}")
                thumbnail = None
        
        if existing_account:
            # Cập nhật token, tên và thumbnail nếu tài khoản đã tồn tại
            existing_account.access_token = encrypted_page_token
            existing_account.account_name = page_name_to_add
            existing_account.is_active = True
            if thumbnail:
                existing_account.thumbnail = thumbnail
            existing_account.updated_at = now_vn_naive()
            await db.commit()
            await db.refresh(existing_account)
            return existing_account
        else:
            # Tạo mới tài khoản nếu chưa tồn tại
            social_account = SocialAccount(
                user_id=user_id,
                platform=data.platform.value,
                account_name=page_name_to_add,
                account_id=page_id_to_add,
                access_token=encrypted_page_token,
                thumbnail=thumbnail,
                is_active=True
            )
            db.add(social_account)
            await db.commit()
            await db.refresh(social_account)
            return social_account
    
    @staticmethod
    def _verify_facebook_token(access_token: str, page_id: str) -> Dict[str, Any]:
        """
        Xác thực Facebook Page Access Token bằng cách gọi API Facebook.
        
        Args:
            access_token: Facebook Page Access Token
            page_id: ID của Facebook Page
            
        Returns:
            Thông tin của Facebook Page
            
        Raises:
            BadRequestException: Nếu token không hợp lệ
            UnauthorizedException: Nếu không có quyền truy cập
        """
        try:
            # Gọi API Facebook để lấy thông tin Page
            url = f"https://graph.facebook.com/v23.0/{page_id}"
            params = {
                "fields": "name,id",
                "access_token": access_token
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            # Kiểm tra lỗi từ Facebook API
            if "error" in data:
                error_message = data["error"].get("message", "Invalid Facebook token")
                error_code = data["error"].get("code", 400)
                
                if error_code in [190, 102, 104]:
                    # Mã lỗi liên quan đến token không hợp lệ hoặc hết hạn
                    raise UnauthorizedException(error_message)
                else:
                    raise BadRequestException(error_message)
            
            return data
        except requests.RequestException as e:
            raise BadRequestException(f"Không thể kết nối đến Facebook API: {str(e)}")
    
    @staticmethod
    async def get_user_social_accounts(db: AsyncSession, user_id: uuid.UUID) -> list[dict]:
        """
        Lấy danh sách tất cả tài khoản mạng xã hội của người dùng kèm thumbnail.
        
        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng
            
        Returns:
            Danh sách các dictionary chứa thông tin SocialAccount và thumbnail
        """
        stmt = select(SocialAccount).filter(SocialAccount.user_id == user_id)
        result = await db.execute(stmt)
        accounts = result.scalars().all()
        
        accounts_with_thumbnails = []
        for account in accounts:
            # Chỉ sử dụng thumbnail đã lưu trong cơ sở dữ liệu
            thumbnail = account.thumbnail
            
            # Tạo dictionary với thông tin account và thumbnail
            account_data = {
                "id": account.id,
                "user_id": account.user_id,
                "platform": account.platform,
                "account_name": account.account_name,
                "account_id": account.account_id,
                "is_active": account.is_active,
                "created_at": account.created_at,
                "updated_at": account.updated_at,
                "thumbnail": thumbnail
            }
            accounts_with_thumbnails.append(account_data)
        
        return accounts_with_thumbnails
    
    @staticmethod
    async def get_social_account(db: AsyncSession, account_id: uuid.UUID, user_id: uuid.UUID) -> Optional[dict]:
        """
        Lấy thông tin một tài khoản mạng xã hội cụ thể kèm thumbnail.
        
        Args:
            db: Database session (AsyncSession)
            account_id: ID của tài khoản mạng xã hội
            user_id: ID của người dùng
            
        Returns:
            Dictionary chứa thông tin SocialAccount và thumbnail hoặc None nếu không tìm thấy
        """
        stmt = select(SocialAccount).filter(
            SocialAccount.id == account_id,
            SocialAccount.user_id == user_id
        )
        result = await db.execute(stmt)
        account = result.scalars().first()
        
        if not account:
            return None
        
        # Chỉ sử dụng thumbnail đã lưu trong cơ sở dữ liệu
        thumbnail = account.thumbnail
        
        # Trả về dictionary với thông tin account và thumbnail
        return {
            "id": account.id,
            "user_id": account.user_id,
            "platform": account.platform,
            "account_name": account.account_name,
            "account_id": account.account_id,
            "is_active": account.is_active,
            "created_at": account.created_at,
            "updated_at": account.updated_at,
            "thumbnail": thumbnail
        }

    @staticmethod
    async def _get_account_thumbnail(platform: str, account_id: str, access_token: str) -> Optional[str]:
        """
        Lấy thumbnail từ API của các nền tảng mạng xã hội
        """
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                if platform == "facebook":
                    # Facebook API để lấy profile picture
                    decrypted_token = token_encryption.decrypt(access_token)
                    url = f"https://graph.facebook.com/v23.0/{account_id}"
                    params = {
                        "access_token": decrypted_token,
                        "fields": "picture{url}"
                    }
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    data = response.json()

                    if "picture" in data and "data" in data["picture"]:
                        return data["picture"]["data"]["url"]

                elif platform == "instagram":
                    decrypted_token = token_encryption.decrypt(access_token)

                    async def get_pic(ig_id: str) -> Optional[str]:
                        url = f"https://graph.facebook.com/v18.0/{ig_id}"
                        params = {"fields": "profile_picture_url", "access_token": decrypted_token}
                        r = await client.get(url, params=params)
                        r.raise_for_status()
                        return r.json().get("profile_picture_url")

                    try:
                        # Nếu ID trông như IG (bắt đầu 178) → hỏi thẳng ảnh
                        if str(account_id).startswith("178"):
                            return await get_pic(account_id)

                        # Còn lại coi là Page ID
                        page_url = f"https://graph.facebook.com/v18.0/{account_id}"
                        page_params = {
                            "fields": "instagram_business_account",
                            "access_token": decrypted_token
                        }
                        page_resp = await client.get(page_url, params=page_params)
                        page_resp.raise_for_status()

                        ig_id = page_resp.json().get("instagram_business_account", {}).get("id")
                        if not ig_id:
                            print("⚠️ Page chưa liên kết IG Business.")
                            return None

                        return await get_pic(ig_id)

                    except httpx.HTTPStatusError as e:
                        print(f"❌ Facebook trả {e.response.status_code}: {e.response.text}")
                        return None
                    except Exception as e:
                        print(f"❌ Lỗi bất ngờ: {e}")
                        return None




                elif platform == "youtube":
                    # YouTube API để lấy thumbnail channel qua OAuth access token (ya29...)
                    decrypted_token = token_encryption.decrypt(access_token)
                    url = "https://www.googleapis.com/youtube/v3/channels"
                    headers = {
                        "Authorization": f"Bearer {decrypted_token}"
                    }
                    params = {
                        "part": "snippet",
                        "mine": "true"  # lấy kênh chính của người dùng
                    }
                    response = await client.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    data = response.json()

                    if "items" in data and len(data["items"]) > 0:
                        thumbnails = data["items"][0]["snippet"]["thumbnails"]
                        # Ưu tiên ảnh chất lượng cao
                        return thumbnails.get("high", {}).get("url") or thumbnails.get("default", {}).get("url")

        except Exception as e:
            print(f"Lỗi khi lấy thumbnail cho {platform}: {e}")
            return None

        return None

    
    @classmethod
    async def delete_social_account(cls, db: AsyncSession, account_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """
        Xóa một tài khoản mạng xã hội và tất cả các platform_post liên quan.
        """
        # Xóa tất cả platform_post liên quan trước
        await db.execute(
            delete(PlatformPost).where(PlatformPost.social_account_id == account_id)
        )
        await db.commit()
        
        # Lấy SocialAccount object để xóa
        stmt = select(SocialAccount).filter(
            SocialAccount.id == account_id,
            SocialAccount.user_id == user_id
        )
        result = await db.execute(stmt)
        account = result.scalars().first()
        
        if not account:
            return False
            
        await db.delete(account)
        await db.commit()
        return True
    
    # @staticmethod
    # async def publish_post_to_facebook(
    #     db: AsyncSession,
    #     post_id: uuid.UUID,
    #     social_account_id: uuid.UUID,
    # ) -> dict:
    #     """
    #     Đăng bài lên Facebook Page (text, ảnh, video) dựa trên thông tin post và social_account.
    #     Lưu lại trạng thái vào PlatformPost.
    #     """
    #     # 1. Lấy thông tin post
    #     stmt_post = select(Post).where(Post.id == post_id)
    #     result_post = await db.execute(stmt_post)
    #     post: Post = result_post.scalars().first()
    #     if not post:
    #         raise BadRequestException("Không tìm thấy bài đăng (post) trong hệ thống.")

    #     # 2. Lấy thông tin social_account (Facebook Page)
    #     stmt_acc = select(SocialAccount).where(SocialAccount.id == social_account_id)
    #     result_acc = await db.execute(stmt_acc)
    #     social_account: SocialAccount = result_acc.scalars().first()
    #     if not social_account or social_account.platform != "facebook":
    #         raise BadRequestException("Không tìm thấy tài khoản Facebook Page hợp lệ.")

    #     # 3. Giải mã access_token và lấy page_id
    #     access_token = token_encryption.decrypt(social_account.access_token)
    #     page_id = social_account.account_id

    #     # 4. Lấy media liên quan (nếu có)
    #     stmt_media = select(PostMedia, MediaAsset).join(MediaAsset, PostMedia.media_id == MediaAsset.id).where(PostMedia.post_id == post_id)
    #     result_media = await db.execute(stmt_media)
    #     media_list = result_media.all()
    #     image_urls = []
    #     video_url = None
    #     for pm, media in media_list:
    #         urls = InstagramService._extract_urls(media.url) if hasattr(InstagramService, "_extract_urls") else ([media.url] if media.url else [])
    #         if pm.media_type == "image":
    #             image_urls.extend(urls)
    #         if pm.media_type == "video" and not video_url:
    #             video_url = urls[0] if urls else None

    #     # 5. Đăng bài lên Facebook
    #     fb_response = None
    #     fb_post_type = None
    #     try:
    #         # Ưu tiên đăng Reels nếu post có facebook_post_format == 'reel'
    #         facebook_post_format = None
    #         if hasattr(post, 'facebook_post_format'):
    #             facebook_post_format = getattr(post, 'facebook_post_format', None)
    #         if video_url and (facebook_post_format == 'facebook_reels' or facebook_post_format == 'reel'):
    #             fb_response = await post_reel_to_facebook_page(
    #                 page_id, access_token, video_url, description=post.content
    #             )
    #             fb_post_type = "REELS"
    #         elif video_url:
    #             fb_response = await post_video_to_facebook_page(page_id, access_token, video_url, description=post.content)
    #             fb_post_type = "VIDEO"
    #         elif image_urls:
    #             if len(image_urls) == 1:
    #                 fb_response = await post_photo_to_facebook_page(page_id, access_token, image_urls[0], caption=post.content)
    #             else:
    #                 fb_response = await post_multiple_photos_to_facebook_page(page_id, access_token, image_urls, caption=post.content)
    #             fb_post_type = "PHOTO"
    #         elif post.content:
    #             fb_response = await post_text_to_facebook_page(page_id, access_token, post.content)
    #             fb_post_type = "TEXT"
    #         else:
    #             raise BadRequestException("Bài đăng không có nội dung hoặc media để đăng lên Facebook.")
    #     except Exception as e:
    #         # Lưu trạng thái failed vào PlatformPost
    #         platform_post = PlatformPost(
    #             post_id=post_id,
    #             social_account_id=social_account_id,
    #             platform="facebook",
    #             status="failed",
    #             platform_post_id=None,
    #             post_url=None,
    #             platform_specific_data={"error": str(e)},
    #             published_at=None,
    #             created_at=datetime.utcnow(),
    #             updated_at=datetime.utcnow()
    #         )
    #         db.add(platform_post)
    #         await db.commit()
    #         await db.refresh(platform_post)
    #         raise BadRequestException(f"Đăng bài lên Facebook thất bại: {str(e)}")

    #     # 6. Lưu trạng thái thành công vào PlatformPost
    #     platform_post_id = None
    #     post_url = None
    #     if fb_post_type == "REELS":
    #         # Lấy id từ publish_data nếu có
    #         publish_data = fb_response.get("publish_data", {})
    #         platform_post_id = publish_data.get("id") or fb_response.get("video_id")
    #         post_url = f"https://www.facebook.com/reel/{platform_post_id}" if platform_post_id else None
    #     else:
    #         platform_post_id = fb_response.get("id") or fb_response.get("post_id")
    #         post_url = f"https://www.facebook.com/{platform_post_id}" if platform_post_id else None
    #     platform_post = PlatformPost(
    #         post_id=post_id,
    #         social_account_id=social_account_id,
    #         platform="facebook",
    #         status="published",
    #         platform_post_id=platform_post_id,
    #         post_url=post_url,
    #         platform_specific_data={"fb_type": fb_post_type},
    #         published_at=datetime.utcnow(),
    #         created_at=datetime.utcnow(),
    #         updated_at=datetime.utcnow()
    #     )
    #     db.add(platform_post)
    #     await db.commit()
    #     await db.refresh(platform_post)
    #     return {
    #         "platform_post_id": platform_post_id,
    #         "post_url": post_url,
    #         "fb_type": fb_post_type,
    #         "fb_response": fb_response
    #     }

    @staticmethod
    async def publish_platform_post(
        db: AsyncSession,
        platform_post_id: str
    ) -> dict:
        # 1. Lấy thông tin platform_post
        stmt_pp = select(PlatformPost).where(PlatformPost.id == platform_post_id)
        result_pp = await db.execute(stmt_pp)
        platform_post: PlatformPost = result_pp.scalars().first()
        if not platform_post:
            raise BadRequestException("Không tìm thấy bản ghi platform_post.")

        # 2. Lấy media_asset_id từ bảng liên kết
        stmt_link = select(platform_post_media_asset.c.media_asset_id).where(
            platform_post_media_asset.c.platform_post_id == platform_post_id
        )
        result_link = await db.execute(stmt_link)
        media_asset_id = result_link.scalar_one_or_none()
        # Lấy platform_type để kiểm tra loại post
        platform_specific_data = platform_post.platform_specific_data or {}
        platform_type = platform_specific_data.get("platform_type", "").lower()
        is_facebook_page = platform_type == "facebook-page"
        if not media_asset_id and not is_facebook_page:
            raise BadRequestException("Không tìm thấy media asset liên kết.")

        # 3. Lấy media asset
        media_asset = None
        if media_asset_id:
            media_asset = await db.get(MediaAsset, media_asset_id)
            if not media_asset:
                raise BadRequestException("Không tìm thấy media asset.")

        # 4. Lấy thông tin social_account
        stmt_acc = select(SocialAccount).where(SocialAccount.id == platform_post.social_account_id)
        result_acc = await db.execute(stmt_acc)
        social_account: SocialAccount = result_acc.scalars().first()
        if not social_account or social_account.platform != platform_post.platform:
            raise BadRequestException("Không tìm thấy tài khoản mạng xã hội hợp lệ.")

        # 5. Giải mã access_token và lấy page_id
        access_token = token_encryption.decrypt(social_account.access_token)
        page_id = social_account.account_id

        # 6. Lấy nội dung từ platform_post.generated_content
        content = platform_post.generated_content

        # 7. Lấy url media từ media_asset (chỉ khi có media asset)
        urls = []
        image_urls = []
        video_urls = []
        if media_asset:
            urls = InstagramService._extract_urls(media_asset.url)
            image_urls = [u for u in urls if media_asset.file_type.startswith("image")]
            video_urls = [u for u in urls if media_asset.file_type.startswith("video")]

        # 8. Lấy định dạng post đặc thù từ platform_specific_data
        platform_specific_data = platform_post.platform_specific_data or {}
        facebook_post_format = platform_specific_data.get("platform_type", "").lower()  # ví dụ: "reel", "newsfeed", ...
        if facebook_post_format == "facebook-reels":
            facebook_post_format = "reel"
        # 9. Đăng bài lên Facebook
        fb_response = None
        fb_post_type = None
        try:
            if video_urls and facebook_post_format == "reel":
                fb_response = await post_reel_to_facebook_page(
                    page_id, access_token, video_urls[0], description=content
                )
                fb_post_type = "reel"
            elif video_urls:
                fb_response = await post_video_to_facebook_page(page_id, access_token, video_urls[0], description=content)
                fb_post_type = "video"
            elif image_urls:
                if len(image_urls) == 1:
                    fb_response = await post_photo_to_facebook_page(page_id, access_token, image_urls[0], caption=content)
                else:
                    fb_response = await post_multiple_photos_to_facebook_page(page_id, access_token, image_urls, caption=content)
                fb_post_type = "photo"
            elif content:
                fb_response = await post_text_to_facebook_page(page_id, access_token, content)
                fb_post_type = "text"
            else:
                raise BadRequestException("Không có nội dung hoặc media để đăng lên Facebook.")
        except Exception as e:
            platform_post.status = "failed"
            platform_post.platform_post_id = None
            platform_post.post_url = None
            platform_post.platform_specific_data = platform_post.platform_specific_data or {}
            platform_post.platform_specific_data["error"] = str(e)
            platform_post.published_at = None
            platform_post.updated_at = now_vn_naive()
            await db.commit()
            await db.refresh(platform_post)
            raise BadRequestException(f"Đăng bài lên Facebook thất bại: {str(e)}")

        # 10. Lưu trạng thái thành công vào PlatformPost
        platform_post_id = fb_response.get("id") or fb_response.get("post_id") or fb_response.get("video_id")
        post_url = f"https://www.facebook.com/{platform_post_id}" if platform_post_id else None
        platform_post.status = "published"
        platform_post.platform_post_id = platform_post_id
        platform_post.post_url = post_url
        platform_post.platform_specific_data = platform_post.platform_specific_data or {}
        platform_post.platform_specific_data["fb_type"] = fb_post_type
        platform_post.published_at = now_vn_naive()
        platform_post.updated_at = now_vn_naive()
        await db.commit()
        await db.refresh(platform_post)
        return {
            "platform_post_id": platform_post_id,
            "post_url": post_url,
            "fb_type": fb_post_type,
            "fb_response": fb_response
        }
 
    @staticmethod
    async def _subscribe_page_webhooks(page_id: str, page_access_token: str, fields: list[str] | None = None) -> bool:
        """
        Đăng ký nhận Webhooks cho một Facebook Page qua /{PAGE-ID}/subscribed_apps.
        Trả về True nếu thành công, False nếu thất bại. Không raise để tránh chặn luồng chính.
        """
        try:
            subscribed_fields = fields or [
                "messages",
                "message_reactions",
                "message_reads",
                "message_deliveries",
                "message_echoes",
                "messaging_postbacks",
                "messaging_optins",
                "messaging_handovers",
                "standby",
            ]
            url = f"{settings.FACEBOOK_API_BASE_URL}/{page_id}/subscribed_apps"
            params = {
                "access_token": page_access_token,
                "subscribed_fields": ",".join(subscribed_fields),
            }
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.post(url, params=params)
            if resp.status_code == 200:
                print(f"✅ Subscribed webhooks cho Page {page_id}: {params['subscribed_fields']}")
                return True
            else:
                print(f"⚠️ Không thể subscribe webhooks cho Page {page_id}: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            print(f"❌ Lỗi subscribe webhooks cho Page {page_id}: {e}")
            return False

    @staticmethod
    async def add_all_facebook_pages(
        db: AsyncSession,
        user_id: uuid.UUID,
        data: FacebookTokenRequest
    ) -> list[SocialAccount]:
        """
        Thêm tất cả Facebook Pages và Instagram Business Accounts liên kết vào hệ thống.
        1. Lấy User ID từ API /me.
        2. Lấy danh sách tất cả Pages từ API /accounts.
        3. Lưu tất cả Facebook Pages.
        4. Kiểm tra và lưu Instagram Business Accounts liên kết với các Pages.
        
        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng (trong hệ thống của chúng ta, không phải Facebook User ID)
            data: Dữ liệu chứa User Access Token
            
        Returns:
            Danh sách các đối tượng SocialAccount đã được tạo hoặc cập nhật (Facebook Pages + Instagram Accounts)
            
        Raises:
            BadRequestException: Nếu thông tin không hợp lệ, không tìm thấy tài khoản, hoặc token không hợp lệ.
            UnauthorizedException: Nếu token không có quyền truy cập cần thiết.
        """
        fb_user_id_from_token = None
        saved_accounts = []

        try:
            async with httpx.AsyncClient() as client:
                fb_api_version = "v23.0"

                # Bước 1: Gọi API /me để lấy Facebook User ID từ User Access Token
                me_url = f"https://graph.facebook.com/{fb_api_version}/me"
                me_params = {"access_token": data.user_access_token, "fields": "id,name"}
                
                print(f"Đang gọi API Facebook: GET {me_url} với token")
                me_response = await client.get(me_url, params=me_params)
                me_response.raise_for_status()
                me_data = me_response.json()
                
                fb_user_id_from_token = me_data.get("id")
                user_name_from_token = me_data.get("name")

                if not fb_user_id_from_token:
                    raise BadRequestException("Không thể lấy được Facebook User ID từ User Access Token cung cấp. Token có thể không hợp lệ hoặc thiếu quyền truy cập thông tin cơ bản.")
                
                print(f"Lấy thành công Facebook User ID: {fb_user_id_from_token} (Tên: {user_name_from_token}) từ token.")

                # Bước 2: Gọi API /accounts để lấy danh sách tất cả Pages
                accounts_url = f"https://graph.facebook.com/{fb_api_version}/{fb_user_id_from_token}/accounts"
                accounts_params = {"access_token": data.user_access_token, "fields": "id,name,access_token,category,tasks"} 
                
                print(f"Đang gọi API Facebook: GET {accounts_url} với token")
                accounts_response = await client.get(accounts_url, params=accounts_params)
                accounts_response.raise_for_status()
                pages_data = accounts_response.json()

                if "data" not in pages_data or not pages_data["data"]:
                    raise BadRequestException(f"Người dùng Facebook (ID: {fb_user_id_from_token}) không quản lý Page nào, hoặc User Token không có quyền 'pages_show_list' cho User này.")

                print(f"Tìm thấy {len(pages_data['data'])} Facebook Pages. Đang lưu tất cả...")

                # --- Kiểm tra giới hạn max_social_accounts ---
                from app.repositories.subscription_repository import SubscriptionRepository
                from sqlalchemy import func

                user_subscription = await SubscriptionRepository.get_by_user_id(db, user_id)
                if not user_subscription or not user_subscription.subscription_plan:
                    raise BadRequestException("Không tìm thấy gói đăng ký của người dùng.")

                subscription_plan = user_subscription.subscription_plan

                # Đếm số tài khoản MXH hiện tại của người dùng
                count_stmt = select(func.count(SocialAccount.id)).where(
                    SocialAccount.user_id == user_id
                )
                current_social_accounts_count = (await db.execute(count_stmt)).scalar_one()

                if current_social_accounts_count >= subscription_plan.max_social_accounts:
                    raise BadRequestException(
                        f"Bạn đã đạt giới hạn {subscription_plan.max_social_accounts} tài khoản mạng xã hội được kết nối của gói đăng ký."
                    )
                # --- Kết thúc kiểm tra giới hạn ---

                # Bước 3: Lưu tất cả Pages và kiểm tra Instagram liên kết
                for page_info in pages_data["data"]:
                    page_id = page_info.get("id")
                    page_name = page_info.get("name")
                    page_access_token = page_info.get("access_token")

                    if not page_id or not page_name or not page_access_token:
                        print(f"Bỏ qua Page thiếu thông tin: ID={page_id}, Name={page_name}")
                        continue

                    # Lưu Facebook Page
                    fb_account = await FacebookService._save_or_update_account(
                        db, user_id, "facebook", page_id, page_name, page_access_token
                    )
                    if fb_account:
                        saved_accounts.append(fb_account)
                        print(f"Lưu Facebook Page: {page_name} (ID: {page_id})")
                        # Đăng ký Webhooks để nhận sự kiện từ Messenger cho Page
                        try:
                            ok = await FacebookService._subscribe_page_webhooks(page_id, page_access_token)
                            if not ok:
                                print(f"⚠️ Subscribe webhooks không thành công cho Page {page_name} ({page_id})")
                        except Exception as sub_err:
                            print(f"⚠️ Lỗi khi subscribe webhooks cho Page {page_name} ({page_id}): {sub_err}")

                    # Kiểm tra Instagram Business Account liên kết
                    try:
                        ig_account_url = f"https://graph.facebook.com/{fb_api_version}/{page_id}"
                        ig_account_params = {
                            "access_token": page_access_token,
                            "fields": "instagram_business_account{id,username,name,profile_picture_url}" 
                        }
                        
                        print(f"Kiểm tra Instagram Business Account cho Page: {page_name}")
                        ig_response = await client.get(ig_account_url, params=ig_account_params)
                        ig_response.raise_for_status()
                        ig_data = ig_response.json()

                        if "instagram_business_account" in ig_data and ig_data["instagram_business_account"]:
                            ig_business_account = ig_data["instagram_business_account"]
                            ig_account_id = ig_business_account.get("id")
                            ig_username = ig_business_account.get("username")
                            ig_name = ig_business_account.get("name")

                            if ig_account_id:
                                # Lưu Instagram Business Account
                                ig_account_name = ig_username if ig_username else ig_name
                                
                                # Lấy thumbnail từ profile_picture_url nếu có
                                ig_thumbnail = ig_business_account.get("profile_picture_url")
                                
                                # Tạo hoặc cập nhật tài khoản Instagram
                                ig_account = await FacebookService._save_or_update_account(
                                    db, user_id, "instagram", ig_account_id, ig_account_name, page_access_token
                                )
                                
                                # Cập nhật thumbnail nếu có và chưa được lưu
                                if ig_account and ig_thumbnail and not ig_account.thumbnail:
                                    ig_account.thumbnail = ig_thumbnail
                                    await db.commit()
                                    await db.refresh(ig_account)
                                
                                if ig_account:
                                    saved_accounts.append(ig_account)
                                    print(f"Lưu Instagram Account: {ig_account_name} (ID: {ig_account_id}) liên kết với Page: {page_name}")
                        else:
                            print(f"Page {page_name} không có Instagram Business Account liên kết")

                    except Exception as ig_error:
                        print(f"Lỗi khi kiểm tra Instagram cho Page {page_name}: {str(ig_error)}")
                        # Tiếp tục với Page tiếp theo, không dừng toàn bộ quá trình

        except httpx.HTTPStatusError as e:
            error_detail = "Unknown error"
            try:
                error_detail = e.response.json().get("error", {}).get("message", str(e))
            except Exception:
                error_detail = str(e)
            if e.response.status_code == 400:
                 raise BadRequestException(f"Lỗi từ Facebook API khi lấy danh sách Page: {error_detail}")
            elif e.response.status_code in [401, 403]:
                 raise UnauthorizedException(f"User Token không hợp lệ hoặc không có quyền truy cập Facebook API: {error_detail}")
            else:
                 raise BadRequestException(f"Lỗi kết nối đến Facebook API ({e.response.status_code}): {error_detail}")
        except httpx.RequestError as e:
            raise BadRequestException(f"Lỗi kết nối mạng khi gọi Facebook API: {str(e)}")
        except Exception as e:
            raise BadRequestException(f"Lỗi không xác định khi xử lý phản hồi từ Facebook: {str(e)}")

        return saved_accounts

    @staticmethod
    async def _save_or_update_account(
        db: AsyncSession,
        user_id: uuid.UUID,
        platform: str,
        account_id: str,
        account_name: str,
        access_token: str
    ) -> SocialAccount:
        """
        Helper method để lưu hoặc cập nhật một tài khoản mạng xã hội.
        
        Args:
            db: Database session
            user_id: ID của user
            platform: "facebook" hoặc "instagram"
            account_id: ID của tài khoản trên platform
            account_name: Tên hiển thị của tài khoản
            access_token: Access token của tài khoản
            
        Returns:
            SocialAccount object đã được lưu/cập nhật
        """
        # Kiểm tra xem tài khoản đã tồn tại trong hệ thống chưa
        stmt = select(SocialAccount).filter(
            SocialAccount.user_id == user_id,
            SocialAccount.platform == platform,
            SocialAccount.account_id == account_id
        )
        result = await db.execute(stmt)
        existing_account = result.scalars().first()
        
        # Mã hóa Access Token trước khi lưu
        encrypted_token = token_encryption.encrypt(access_token)
        
        # Lấy thumbnail từ API
        thumbnail = await FacebookService._get_account_thumbnail(platform, account_id, encrypted_token)
        
        if existing_account:
            # Cập nhật token, tên và thumbnail nếu tài khoản đã tồn tại
            existing_account.access_token = encrypted_token
            existing_account.account_name = account_name
            existing_account.is_active = True
            existing_account.updated_at = now_vn_naive()
            if thumbnail:
                existing_account.thumbnail = thumbnail
            await db.commit()
            await db.refresh(existing_account)
            return existing_account
        else:
            # Tạo mới tài khoản nếu chưa tồn tại
            social_account = SocialAccount(
                user_id=user_id,
                platform=platform,
                account_name=account_name,
                account_id=account_id,
                access_token=encrypted_token,
                thumbnail=thumbnail,
                is_active=True
            )
            db.add(social_account)
            await db.commit()
            await db.refresh(social_account)
            return social_account

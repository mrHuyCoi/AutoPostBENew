import uuid
import secrets
import httpx
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urlencode
import os
from dotenv import load_dotenv
# from app.models.user import User
# from app.middlewares.auth_middleware import get_current_user
import sys
from fastapi import HTTPException
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from app.models.social_account import SocialAccount
from googleapiclient.http import MediaFileUpload
from app.utils.crypto import token_encryption
from app.dto.youtube_dto import YouTubeAuthRequest, YouTubeCallbackRequest
from app.exceptions.api_exceptions import BadRequestException, UnauthorizedException
from app.services.social_account_service import ISocialAccountService
from loguru import logger
from app.models.platform_post import PlatformPost
from app.models.platform_post_media_asset import platform_post_media_asset
from app.models.media_asset import MediaAsset
from sqlalchemy import select
import tempfile
from app.models.youtube_metadata import YouTubeMetadata
load_dotenv()

class YouTubeService(ISocialAccountService):
    """
    Service xử lý các thao tác liên quan đến tài khoản mạng xã hội YouTube.
    """
    
    # Configuration
    YOUTUBE_CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REDIRECT_URI = "https://test.autodangbai.doiquanai.vn/api/v1/youtube/oauth/callback"
    YOUTUBE_SCOPES = [
        'https://www.googleapis.com/auth/youtube.upload',
        'https://www.googleapis.com/auth/youtube.readonly'
    ]
    
    async def create_auth_url(db) -> Tuple[str, str]:
        """
        Tạo URL xác thực OAuth2 cho YouTube.
        
        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng trong hệ thống
            
        Returns:
            Tuple chứa (auth_url, state)
            
        Raises:
            BadRequestException: Nếu có lỗi trong quá trình tạo URL
        """
        try:
            state = secrets.token_urlsafe(32)
            
            # Lưu state vào database (có thể tạo bảng oauth_states riêng hoặc lưu tạm thời)
            # Ở đây giả sử bạn có một cách lưu trữ state, ví dụ Redis hoặc bảng tạm
            # await YouTubeService._save_oauth_state(db, state, user_id)
            
            params = {
                'client_id': YouTubeService.YOUTUBE_CLIENT_ID,
                'redirect_uri': YouTubeService.YOUTUBE_REDIRECT_URI,
                'scope': ' '.join(YouTubeService.YOUTUBE_SCOPES),
                'response_type': 'code',
                'access_type': 'offline',
                'prompt': 'consent',
                'state': state
            }
            
            auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
            return auth_url, state
            
        except Exception as e:
            raise BadRequestException(f"Lỗi khi tạo URL xác thực YouTube: {str(e)}")
    
    @staticmethod
    async def create_auth_url_with_state(db, state: str) -> tuple[str, str]:
        """
        Tạo URL xác thực OAuth2 cho YouTube với state tuỳ ý.
        """
        try:
            params = {
                'client_id': YouTubeService.YOUTUBE_CLIENT_ID,
                'redirect_uri': YouTubeService.YOUTUBE_REDIRECT_URI,
                'scope': ' '.join(YouTubeService.YOUTUBE_SCOPES),
                'response_type': 'code',
                'access_type': 'offline',
                'prompt': 'consent',
                'state': state
            }
            auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
            return auth_url, state
        except Exception as e:
            raise BadRequestException(f"Lỗi khi tạo URL xác thực YouTube: {str(e)}")
    
    @staticmethod
    async def add_youtube_account(
        db: AsyncSession,
        data: dict,
        user_id: uuid.UUID
    ) -> SocialAccount:
        """
        Thêm tài khoản YouTube vào hệ thống sau khi OAuth thành công.
        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng trong hệ thống
            data: Dữ liệu chứa authorization code và state từ OAuth callback
        Returns:
            Đối tượng SocialAccount đã được tạo hoặc cập nhật
        Raises:
            BadRequestException: Nếu thông tin không hợp lệ hoặc token không hợp lệ
            UnauthorizedException: Nếu không có quyền truy cập
        """
        try:
            # Verify state (có thể implement logic verify state ở đây)
            # await YouTubeService._verify_oauth_state(db, data.state, user_id)
            
            # Exchange authorization code for tokens
            token_info = await YouTubeService._exchange_code_for_tokens(data['code'])
            
            # Get channel info using access token
            channel_info = await YouTubeService._get_channel_info(token_info['access_token'])
            
            # Kiểm tra xem tài khoản đã tồn tại trong hệ thống chưa
            stmt = select(SocialAccount).filter(
                SocialAccount.user_id == user_id,
                SocialAccount.platform == "youtube",
                SocialAccount.account_id == channel_info['channel_id']
            )
            result = await db.execute(stmt)
            existing_account = result.scalars().first()
            logger.info(f"Existing account: {existing_account}")
            
            # Mã hóa tokens trước khi lưu
            encrypted_access_token = token_encryption.encrypt(token_info['access_token'])
            encrypted_refresh_token = None
            if token_info.get('refresh_token'):
                encrypted_refresh_token = token_encryption.encrypt(token_info['refresh_token'])
            
            # Tính thời gian hết hạn token
            expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=token_info.get('expires_in', 3600))
            
            if existing_account:
                # Cập nhật token và thông tin nếu tài khoản đã tồn tại
                # Lấy thumbnail từ YouTube API
                thumbnail_url = await YouTubeService._get_account_thumbnail(access_token=token_info['access_token'])
                
                # Cập nhật thông tin tài khoản
                setattr(existing_account, 'access_token', encrypted_access_token)
                setattr(existing_account, 'refresh_token', encrypted_refresh_token)
                setattr(existing_account, 'token_expires_at', expires_at)
                setattr(existing_account, 'account_name', channel_info['channel_name'])
                setattr(existing_account, 'is_active', True)
                setattr(existing_account, 'updated_at', datetime.now(timezone.utc).replace(tzinfo=None))
                
                # Cập nhật thumbnail nếu có
                if thumbnail_url:
                    setattr(existing_account, 'thumbnail', thumbnail_url)
                await db.commit()
                await db.refresh(existing_account)
                return existing_account
            else:
                # Lấy thumbnail từ YouTube API
                thumbnail_url = await YouTubeService._get_account_thumbnail(access_token=token_info['access_token'])
                
                # Tạo mới tài khoản nếu chưa tồn tại
                social_account = SocialAccount(
                    user_id=user_id,
                    platform="youtube", 
                    account_name=channel_info['channel_name'],
                    account_id=channel_info['channel_id'],
                    access_token=encrypted_access_token,
                    refresh_token=encrypted_refresh_token,
                    token_expires_at=expires_at,
                    thumbnail=thumbnail_url,
                    is_active=True
                )
                db.add(social_account)
                await db.commit()
                await db.refresh(social_account)
                return social_account
                
        except httpx.HTTPStatusError as e:
            error_detail = "Unknown error"
            try:
                error_detail = e.response.json().get("error_description", str(e))
            except Exception:
                error_detail = str(e)
            if e.response.status_code == 400:
                raise BadRequestException(f"Lỗi từ Google API khi xác thực YouTube: {error_detail}")
            elif e.response.status_code in [401, 403]:
                raise UnauthorizedException(f"Không có quyền truy cập YouTube API: {error_detail}")
            else:
                raise BadRequestException(f"Lỗi kết nối đến Google API ({e.response.status_code}): {error_detail}")
        except httpx.RequestError as e:
            raise BadRequestException(f"Lỗi kết nối mạng khi gọi Google API: {str(e)}")
        except Exception as e:
            raise BadRequestException(f"Lỗi không xác định khi xử lý phản hồi từ Google: {str(e)}")
    
    @staticmethod
    async def _exchange_code_for_tokens(code: str) -> Dict[str, Any]:
        """
        Đổi authorization code lấy access token và refresh token.
        
        Args:
            code: Authorization code từ OAuth callback
            
        Returns:
            Dictionary chứa token info
            
        Raises:
            httpx.HTTPStatusError: Nếu có lỗi từ Google API
        """
        token_data = {
            'code': code,
            'client_id': YouTubeService.YOUTUBE_CLIENT_ID,
            'client_secret': YouTubeService.YOUTUBE_CLIENT_SECRET,
            'redirect_uri': YouTubeService.YOUTUBE_REDIRECT_URI,
            'grant_type': 'authorization_code'
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post('https://oauth2.googleapis.com/token', data=token_data)
            response.raise_for_status()
            return response.json()
    
    @staticmethod
    async def _get_channel_info(access_token: str) -> Dict[str, str]:
        """
        Lấy thông tin kênh YouTube từ access token.
        
        Args:
            access_token: YouTube access token
            
        Returns:
            Dictionary chứa channel_id và channel_name
            
        Raises:
            BadRequestException: Nếu không tìm thấy kênh hoặc token không hợp lệ
        """
        headers = {'Authorization': f'Bearer {access_token}'}
        url = 'https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true'
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('items'):
                raise BadRequestException("Không tìm thấy kênh YouTube nào")
            
            channel = data['items'][0]
            return {
                'channel_id': channel['id'],
                'channel_name': channel['snippet']['title']
            }
    
    @staticmethod
    async def get_valid_token(
        db: AsyncSession, 
        user_id: uuid.UUID, 
        account_id: uuid.UUID
    ) -> Optional[Dict[str, Any]]:
        """
        Lấy access token hợp lệ, tự động refresh nếu cần.
        
        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng
            account_id: ID của tài khoản YouTube
            
        Returns:
            Dictionary chứa token info hoặc None nếu không tìm thấy
        """
        account = await YouTubeService.get_social_account(db=db, user_id=user_id, account_id=account_id)
        logger.info(f"Account info: {account}")
        
        if not account:
            return None
            
        # Kiểm tra xem access_token có tồn tại không
        if not getattr(account, 'access_token', None):
            return None
        
        # Giải mã token
        try:
            access_token_value = getattr(account, 'access_token', None)
            if not access_token_value:
                return None
            access_token = token_encryption.decrypt(access_token_value)
        except Exception:
            return None
        
        # Kiểm tra token còn hạn không
        token_expires_at = getattr(account, 'token_expires_at', None)
        if token_expires_at and datetime.now(timezone.utc).replace(tzinfo=None) < token_expires_at.replace(tzinfo=None):
            return {
                'access_token': access_token,
                'channel_name': account.account_name,
                'channel_id': account.account_id,
                'thumbnail': getattr(account, 'thumbnail', None)
            }
        
        # Thử refresh token nếu có
        refresh_token_value = getattr(account, 'refresh_token', None)
        if refresh_token_value:
            try:
                refresh_token = token_encryption.decrypt(refresh_token_value)
                new_token_info = await YouTubeService._refresh_access_token(refresh_token)
                
                if new_token_info:
                    # Cập nhật token mới vào database
                    encrypted_access_token = token_encryption.encrypt(new_token_info['access_token'])
                    expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=new_token_info.get('expires_in', 3600))
                    
                    # Cập nhật các thuộc tính của account
                    setattr(account, 'access_token', encrypted_access_token)
                    setattr(account, 'token_expires_at', expires_at)
                    setattr(account, 'updated_at', datetime.now(timezone.utc).replace(tzinfo=None))
                    await db.commit()
                    
                    return {
                        'access_token': new_token_info['access_token'],
                        'channel_name': account.account_name,
                        'channel_id': account.account_id,
                        'thumbnail': getattr(account, 'thumbnail', None)
                    }
            except Exception:
                pass
        
        return None
    
    @staticmethod
    async def _get_account_thumbnail(access_token: str) -> Optional[str]:
        """
        Lấy thumbnail từ YouTube API sử dụng access token.
        
        Args:
            access_token: YouTube access token
            
        Returns:
            URL của thumbnail hoặc None nếu không lấy được
        """
        try:
            async with httpx.AsyncClient() as client:
                url = "https://www.googleapis.com/youtube/v3/channels"
                headers = {
                    "Authorization": f"Bearer {access_token}"
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
                return None
        except Exception as e:
            print(f"Lỗi khi lấy thumbnail cho YouTube: {e}")
            return None
    
    @staticmethod
    async def _refresh_access_token(refresh_token: str) -> Optional[Dict[str, Any]]:
        """
        Refresh access token sử dụng refresh token.
        
        Args:
            refresh_token: Refresh token
            
        Returns:
            Dictionary chứa token info mới hoặc None nếu thất bại
        """
        token_data = {
            'refresh_token': refresh_token,
            'client_id': YouTubeService.YOUTUBE_CLIENT_ID,
            'client_secret': YouTubeService.YOUTUBE_CLIENT_SECRET,
            'grant_type': 'refresh_token'
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post('https://oauth2.googleapis.com/token', data=token_data)
                response.raise_for_status()
                return response.json()
        except Exception:
            return None
    
    @staticmethod
    async def get_user_social_accounts(db: AsyncSession, user_id: uuid.UUID) -> list[SocialAccount]:
        """
        Lấy danh sách tài khoản YouTube của người dùng.
        Chỉ lấy thumbnail từ cơ sở dữ liệu, không gọi API.
        
        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng
            
        Returns:
            Danh sách các tài khoản YouTube
        """
        stmt = select(SocialAccount).filter(
            SocialAccount.user_id == user_id,
            SocialAccount.platform == "youtube"
        )
        result = await db.execute(stmt)
        accounts = result.scalars().all()
        
        return accounts
    
    @staticmethod
    async def get_social_account(db: AsyncSession, 
                                 user_id: uuid.UUID, 
                                 account_id: uuid.UUID=None) -> Optional[SocialAccount]:
        """
        Lấy thông tin một tài khoản YouTube cụ thể.
        Chỉ lấy thumbnail từ cơ sở dữ liệu, không gọi API.
        
        Args:
            db: Database session (AsyncSession)
            account_id: ID của tài khoản YouTube
            user_id: ID của người dùng
            
        Returns:
            Đối tượng SocialAccount hoặc None nếu không tìm thấy
        """
        stmt = select(SocialAccount).filter(
            SocialAccount.id == account_id,
            SocialAccount.user_id == user_id,
            SocialAccount.platform == "youtube"
        )
        result = await db.execute(stmt)
        account = result.scalars().first()
        
        return account
    
    @staticmethod
    async def delete_social_account(db: AsyncSession, user_id: uuid.UUID, account_id: uuid.UUID=None) -> bool:
        """
        Xóa một tài khoản YouTube.
        
        Args:
            db: Database session (AsyncSession)
            account_id: ID của tài khoản YouTube
            user_id: ID của người dùng
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        account = await YouTubeService.get_social_account(db, user_id, account_id)
        
        if not account:
            return False
        
        await db.delete(account)
        await db.commit()
        return True
    
    @staticmethod
    async def publish_platform_post(
        db: AsyncSession,
        platform_post_id: str
    ) -> dict:
        """
        Đăng bài lên YouTube dựa trên bản ghi platform_posts.
        """
        from app.models.platform_post import PlatformPost
        from app.models.platform_post_media_asset import platform_post_media_asset
        from app.models.media_asset import MediaAsset
        from app.models.social_account import SocialAccount
        from app.models.youtube_metadata import YouTubeMetadata
        from sqlalchemy import select
        import os
        import tempfile
        import httpx
        import mimetypes
        from datetime import datetime

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
        if not media_asset_id:
            raise BadRequestException("Không tìm thấy media asset liên kết.")

        # 3. Lấy media asset
        media_asset = await db.get(MediaAsset, media_asset_id)
        if not media_asset:
            raise BadRequestException("Không tìm thấy media asset.")

        # 4. Lấy thông tin social_account
        stmt_acc = select(SocialAccount).where(SocialAccount.id == platform_post.social_account_id)
        result_acc = await db.execute(stmt_acc)
        social_account: SocialAccount = result_acc.scalars().first()
        if not social_account or social_account.platform != platform_post.platform:
            raise BadRequestException("Không tìm thấy tài khoản YouTube hợp lệ.")

        # 5. Lấy metadata
        metadata = await db.get(YouTubeMetadata, platform_post.id)
        if metadata:
            title = metadata.title or "Untitled"
            description = metadata.description or ""
            tags = metadata.tags or []
            privacy_status = metadata.privacy_status or "public"
        else:
            title = platform_post.generated_content.get("title") or "Untitled"
            description = platform_post.generated_content.get("description") or ""
            tags = platform_post.generated_content.get("tags", [])
            privacy_status = platform_post.platform_specific_data.get("privacy_status") or "public"

        if not title or not str(title).strip():
            title = "Untitled"


        # 6. Tải video về máy tạm nếu cần
        video_path = None
        try:
            urls = media_asset.url if isinstance(media_asset.url, list) else [media_asset.url]
            video_url = urls[0] if urls else None
            if not video_url:
                raise BadRequestException("Không tìm thấy URL video trong media asset.")

            ext = os.path.splitext(media_asset.file_name or video_url)[-1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as temp_file:
                video_path = temp_file.name
                print(f"[DEBUG] Tải video từ {video_url} vào {video_path}")
                async with httpx.AsyncClient() as client:
                    resp = await client.get(video_url)
                    resp.raise_for_status()
                    temp_file.write(resp.content)

            # 7. Upload video
            result = await YouTubeService.upload_video(
                db=db,
                user_id=social_account.user_id,
                account_id=social_account.id,
                video_path=video_path,
                title=title,
                description=description,
                tags=tags,
                privacy_status=privacy_status
            )

            # 8. Cập nhật trạng thái
            platform_post.status = "published"
            platform_post.platform_post_id = result.get("video_id")
            platform_post.post_url = result.get("video_url")
            platform_post.published_at = datetime.now(timezone.utc).replace(tzinfo=None)
            platform_post.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
            await db.commit()
            await db.refresh(platform_post)
            return result

        except Exception as e:
            await db.rollback()
            platform_post.status = "failed"
            platform_post.platform_specific_data = platform_post.platform_specific_data or {}
            platform_post.platform_specific_data["publish_error"] = str(e)
            platform_post.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
            await db.commit()
            await db.refresh(platform_post)
            raise BadRequestException(f"Đăng video lên YouTube thất bại: {str(e)}")

        finally:
            if video_path and os.path.exists(video_path):
                try:
                    os.remove(video_path)
                except Exception:
                    pass


    @staticmethod
    async def upload_video(
        db: AsyncSession,
        user_id: uuid.UUID,
        account_id: uuid.UUID,
        video_path: str,
        title: str,
        description: str,
        tags: list,
        privacy_status: str = "private"
    ) -> Dict[str, Any]:
        """
        Upload video lên YouTube.

        Args:
            db: Database session (AsyncSession)
            user_id: ID của người dùng
            account_id: ID của tài khoản YouTube
            video_path: Đường dẫn file video
            title: Tiêu đề video
            description: Mô tả video
            tags: Danh sách tags
            privacy_status: Trạng thái riêng tư (private, public, unlisted)

        Returns:
            Dictionary chứa thông tin video đã upload

        Raises:
            UnauthorizedException: Nếu không có quyền truy cập
            BadRequestException: Nếu upload thất bại
        """
        try:
            # 1. Lấy access token
            token_info = await YouTubeService.get_valid_token(db=db, user_id=user_id, account_id=account_id)
            if not token_info:
                raise UnauthorizedException("YouTube không được kết nối hoặc token đã hết hạn")

            credentials = Credentials(token=token_info['access_token'])
            youtube = build('youtube', 'v3', credentials=credentials)

            # 2. Xử lý an toàn các giá trị đầu vào
            title = title or "Untitled"
            title = str(title).strip()
            if not title:
                title = "Untitled"

            description = description or ""
            tags = tags or []
            privacy_status = privacy_status or "private"

            print("[DEBUG] Final video title:", repr(title))

            # 3. Lấy mimetype chính xác từ file video
            import mimetypes
            mimetype, _ = mimetypes.guess_type(video_path)
            mimetype = mimetypes.guess_type(video_path)[0] or 'video/mp4'

            from googleapiclient.http import MediaFileUpload
            media = MediaFileUpload(
                video_path,
                chunksize=-1,
                resumable=True,
                mimetype=mimetype
            )

            # 4. Tạo metadata video
            body = {
                'snippet': {
                    'title': title,
                    'description': description,
                    'tags': tags,
                    'categoryId': '22'  # People & Blogs
                },
                'status': {
                    'privacyStatus': privacy_status
                }
            }

            print("[DEBUG] Uploading video:", video_path)
            print("[DEBUG] Metadata:", body)

            # 5. Tạo request upload
            insert_request = youtube.videos().insert(
                part="snippet,status",
                body=body,
                media_body=media
            )

            response = None
            while response is None:
                status, response = insert_request.next_chunk()
                if status:
                    print(f"[DEBUG] Upload tiến trình: {int(status.progress() * 100)}%")

            if 'id' not in response:
                raise HTTPException(status_code=500, detail=f"Upload failed: {response}")

            print("[DEBUG] Upload thành công:", response['id'])

            return {
                'video_id': response['id'],
                'video_url': f"https://www.youtube.com/watch?v={response['id']}",
                'channel_name': token_info.get('channel_name', 'Unknown'),
                'title': title,
                'status': 'uploaded'
            }

        except Exception as e:
            print("[ERROR] Lỗi khi upload video:", str(e))
            raise BadRequestException(f"Lỗi khi upload video lên YouTube: {str(e)}")

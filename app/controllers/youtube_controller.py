from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid
import os
import tempfile
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from app.database.database import get_db
from app.services.youtube_acc_service import YouTubeService
from app.exceptions.api_exceptions import BadRequestException, UnauthorizedException, NotFoundException
from app.middlewares.subscription_middleware import check_active_subscription
from app.models.user import User
from app.dto.response import SuccessResponse, ErrorResponse
from app.logging.decorators import with_log_context


router = APIRouter()


@router.post("/connect", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.connect",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def create_youtube_auth_url(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(check_active_subscription(required_max_videos_per_day=1))
):
    """
    Tạo URL xác thực OAuth2 cho YouTube, truyền state là user_id.
    """
    try:
        state = str(current_user.id)
        auth_url, _ = await YouTubeService.create_auth_url_with_state(db, state)
        return SuccessResponse(
            data={
                "auth_url": auth_url,
                "state": state,
            },
            message="URL xác thực YouTube đã được tạo thành công"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/oauth/callback", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.oauth.callback",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def handle_youtube_oauth_callback(
    state: str, code: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Callback nhận code và state (chính là user_id), lưu tài khoản YouTube cho user đó.
    """
    try:
        user_id = state  # state chính là user_id
        request = {
            "state": state,
            "code": code,
        }
        social_account = await YouTubeService.add_youtube_account(db, request, user_id)
        return SuccessResponse(
            data={
                "account_id": str(social_account.id),
                "channel_id": social_account.account_id,
                "channel_name": social_account.account_name,
                "platform": social_account.platform,
                "is_active": social_account.is_active,
                "connected_at": social_account.created_at.isoformat()
            },
            message="Tài khoản YouTube đã được kết nối thành công"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xác thực YouTube: {str(e)}")


@router.post("/upload", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.upload_video",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def upload_video_to_youtube(
    account_id: uuid.UUID = Form(...),
    title: str = Form(...),
    description: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),  # Comma-separated tags
    privacy_status: str = Form("public"),
    video_file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(check_active_subscription(required_max_videos_per_day=1))
):
    """
    Upload video lên YouTube.
    
    - **account_id**: ID tài khoản YouTube trong hệ thống
    - **title**: Tiêu đề video (bắt buộc)
    - **description**: Mô tả video
    - **tags**: Tags cho video (phân cách bằng dấu phẩy)
    - **privacy_status**: Trạng thái riêng tư (private, public, unlisted)
    - **category_id**: Category ID (mặc định: 22 - People & Blogs)
    - **scheduled_publish_time**: Thời gian xuất bản theo lịch
    - **video_file**: File video để upload
    
    Returns:
        Thông tin video đã upload thành công
    """
    try:
        # Validate video file
        if not video_file.content_type or not video_file.content_type.startswith('video/'):
            raise BadRequestException("File upload phải là video")
        
        # Check file size (ví dụ: max 2GB)
        if video_file.size and video_file.size > 2 * 1024 * 1024 * 1024:
            raise BadRequestException("File video không được vượt quá 2GB")
        
        # Parse tags
        tags_list = []
        if tags:
            tags_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
            if len(tags_list) > 30:
                raise BadRequestException("Số lượng tags không được vượt quá 30")
        
        # Validate privacy status
        if privacy_status not in ['private', 'public', 'unlisted']:
            raise BadRequestException("privacy_status phải là private, public, hoặc unlisted")
        
        # Create temporary file to save uploaded video
        temp_file_path = None
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(video_file.filename)[1]) as temp_file:
                temp_file_path = temp_file.name
                # Save uploaded file to temporary location
                content = await video_file.read()
                temp_file.write(content)
            
            # Upload video to YouTube
            upload_result = await YouTubeService.upload_video(
                db=db,
                user_id=current_user.id,
                account_id=account_id,
                video_path=temp_file_path,
                title=title,
                description=description or "",
                tags=tags_list,
                privacy_status=privacy_status
            )
            
            # Schedule cleanup of temporary file
            background_tasks.add_task(cleanup_temp_file, temp_file_path)
            
            return SuccessResponse(
                data=upload_result,
                message="Video đã được upload lên YouTube thành công"
            )
            
        except Exception as e:
            # Cleanup temp file if upload failed
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            raise e
            
    except (BadRequestException, UnauthorizedException) as e:
        raise HTTPException(status_code=400 if isinstance(e, BadRequestException) else 401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi upload video: {str(e)}")


@router.get("/accounts", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.accounts.list",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def get_youtube_accounts(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(check_active_subscription(required_max_videos_per_day=1))
):
    """
    Lấy danh sách tài khoản YouTube của người dùng.
    
    Returns:
        Danh sách các tài khoản YouTube đã kết nối
    """
    try:
        accounts = await YouTubeService.get_user_social_accounts(db=db,user_id=current_user.id)
        
        accounts_data = []
        for account in accounts:
            # Kiểm tra trạng thái token
            token_info = await YouTubeService.get_valid_token(db=db, user_id=current_user.id, account_id=account.id)
            is_token_valid = token_info is not None
            
            accounts_data.append({
                "account_id": str(account.id),
                "channel_id": account.account_id,
                "channel_name": account.account_name,
                "is_active": account.is_active,
                "is_token_valid": is_token_valid,
                "token_expires_at": account.token_expires_at.isoformat() if account.token_expires_at else None,
                "connected_at": account.created_at.isoformat(),
                "last_updated": account.updated_at.isoformat()
            })
        
        return SuccessResponse(
            data={
                "accounts": accounts_data,
                "total_count": len(accounts_data)
            },
            message="Lấy danh sách tài khoản YouTube thành công"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy danh sách tài khoản: {str(e)}")


@router.get("/accounts/{account_id}", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.accounts.get",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def get_youtube_account_detail(
    account_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(check_active_subscription(required_max_videos_per_day=1))
):
    """
    Lấy thông tin chi tiết một tài khoản YouTube.
    
    - **account_id**: ID của tài khoản YouTube trong hệ thống
    
    Returns:
        Thông tin chi tiết tài khoản YouTube
    """
    try:
        account = await YouTubeService.get_social_account(db=db, account_id=account_id, user_id=current_user.id)
        
        if not account:
            raise NotFoundException("Không tìm thấy tài khoản YouTube")
        
        # Kiểm tra trạng thái token và lấy thông tin kênh
        token_info = await YouTubeService.get_valid_token(db=db,user_id=current_user.id,account_id=account_id)
        is_token_valid = token_info is not None
        
        channel_info = None
        if is_token_valid:
            try:
                # Lấy thông tin chi tiết kênh từ YouTube API
                channel_info = await YouTubeService._get_channel_info(access_token=token_info['access_token'])
            except Exception:
                pass  # Nếu không lấy được thông tin kênh, vẫn trả về thông tin cơ bản
        
        account_data = {
            "account_id": str(account.id),
            "channel_id": account.account_id,
            "channel_name": account.account_name,
            "is_active": account.is_active,
            "is_token_valid": is_token_valid,
            "token_expires_at": account.token_expires_at.isoformat() if account.token_expires_at else None,
            "connected_at": account.created_at.isoformat(),
            "last_updated": account.updated_at.isoformat(),
            "channel_info": channel_info
        }
        
        return SuccessResponse(
            data=account_data,
            message="Lấy thông tin tài khoản YouTube thành công"
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy thông tin tài khoản: {str(e)}")


@router.delete("/accounts/{account_id}", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.accounts.delete",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def delete_youtube_account(
    account_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(check_active_subscription(required_max_videos_per_day=1))
):
    """
    Xóa kết nối tài khoản YouTube.
    
    - **account_id**: ID của tài khoản YouTube trong hệ thống
    
    Returns:
        Xác nhận xóa thành công
    """
    try:
        success = await YouTubeService.delete_social_account(db=db,account_id=account_id, user_id=current_user.id)
        
        if not success:
            raise NotFoundException("Không tìm thấy tài khoản YouTube để xóa")
        
        return SuccessResponse(
            data={"deleted_account_id": str(account_id)},
            message="Tài khoản YouTube đã được xóa thành công"
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa tài khoản: {str(e)}")


@router.post("/refresh-token/{account_id}", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.token.refresh",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def refresh_youtube_token(
    account_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(check_active_subscription(required_max_videos_per_day=1))
):
    """
    Refresh token của tài khoản YouTube.
    
    - **account_id**: ID của tài khoản YouTube trong hệ thống
    
    Returns:
        Thông tin token mới
    """
    try:
        account = await YouTubeService.get_social_account(db=db, account_id=account_id, user_id=current_user.id)
        
        if not account:
            raise NotFoundException("Không tìm thấy tài khoản YouTube")
        
        # Try to get valid token (this will automatically refresh if needed)
        token_info = await YouTubeService.get_valid_token(db=db, user_id=current_user.id, account_id=account_id)
        
        if not token_info:
            raise UnauthorizedException("Không thể refresh token. Vui lòng kết nối lại tài khoản YouTube")
        
        return SuccessResponse(
            data={
                "account_id": str(account_id),
                "channel_name": token_info['channel_name'],
                "token_refreshed": True,
                "expires_at": account.token_expires_at.isoformat() if account.token_expires_at else None
            },
            message="Token YouTube đã được refresh thành công"
        )
    except (NotFoundException, UnauthorizedException) as e:
        status_code = 404 if isinstance(e, NotFoundException) else 401
        raise HTTPException(status_code=status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi refresh token: {str(e)}")


@router.get("/channel-info/{account_id}", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.channel.info",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def get_youtube_channel_info(
    account_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(check_active_subscription(required_max_videos_per_day=1))
):
    """
    Lấy thông tin chi tiết kênh YouTube từ API.
    
    - **account_id**: ID của tài khoản YouTube trong hệ thống
    
    Returns:
        Thông tin chi tiết kênh YouTube
    """
    try:
        token_info = await YouTubeService.get_valid_token(db=db, user_id=current_user.id, account_id=account_id)
        
        if not token_info:
            raise UnauthorizedException("Token YouTube không hợp lệ hoặc đã hết hạn")
        
        # Get detailed channel info from YouTube API
        channel_info = await YouTubeService._get_channel_info(access_token=token_info['access_token'])
        
        return SuccessResponse(
            data=channel_info,
            message="Lấy thông tin kênh YouTube thành công"
        )
    except UnauthorizedException as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy thông tin kênh: {str(e)}")


# Utility functions
def cleanup_temp_file(file_path: str):
    """Background task to cleanup temporary files."""
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except Exception:
        pass  # Ignore cleanup errors


# Health check endpoint
@router.get("/health", response_model=SuccessResponse)
@with_log_context(
    event_action="youtube.health",
    event_category="youtube",
    source_layer="controller",
    source_controller="youtube_controller",
)
async def youtube_service_health():
    """
    Kiểm tra trạng thái service YouTube.
    
    Returns:
        Trạng thái service
    """
    return SuccessResponse(
        data={
            "service": "youtube",
            "status": "healthy",
            "timestamp": now_vn_naive().isoformat(),
            "features": [
                "oauth_authentication",
                "video_upload",
                "channel_management",
                "token_refresh"
            ]
        },
        message="YouTube service đang hoạt động bình thường"
    )
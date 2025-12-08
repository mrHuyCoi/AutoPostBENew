from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from sqlalchemy.orm import Session
from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.services.file_storage_service import FileStorageService
from app.dto.response import SuccessResponse
from typing import Dict, Any
from app.logging.decorators import with_log_context

router = APIRouter()

@router.post("/upload-file")
@with_log_context(
    event_action="file_upload.upload",
    event_category="file_upload",
    source_layer="controller",
    source_controller="file_upload_controller",
)
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Upload file lên S3 storage và trả về URL public
    Hỗ trợ: image (png, jpeg, webp) và video (mp4, quicktime, x-matroska)
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Upload file sử dụng FileStorageService
        result = await FileStorageService.upload_file_to_cms(file, user_id)
        
        return SuccessResponse(
            message="Upload file thành công",
            data=result
        )
        
    except HTTPException:
        # Re-raise HTTPException để giữ nguyên status code và detail
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi upload file: {str(e)}")

@router.delete("/delete-file")
@with_log_context(
    event_action="file_upload.delete",
    event_category="file_upload",
    source_layer="controller",
    source_controller="file_upload_controller",
)
async def delete_file(
    storage_path: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa file từ S3 storage
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Kiểm tra storage_path có thuộc về user này không (bảo mật)
        if not storage_path.startswith(f"{user_id}/"):
            raise HTTPException(status_code=403, detail="Không có quyền xóa file này")
        
        # Xóa file sử dụng FileStorageService
        success = await FileStorageService.delete_file_from_s3(storage_path)
        
        if success:
            return SuccessResponse(
                message="Xóa file thành công",
                data={"storage_path": storage_path}
            )
        else:
            raise HTTPException(status_code=500, detail="Không thể xóa file")
            
    except HTTPException:
        # Re-raise HTTPException để giữ nguyên status code và detail
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa file: {str(e)}")

@router.get("/file-info")
@with_log_context(
    event_action="file_upload.info.get",
    event_category="file_upload",
    source_layer="controller",
    source_controller="file_upload_controller",
)
async def get_file_info(
    storage_path: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin file từ storage_path
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Kiểm tra storage_path có thuộc về user này không (bảo mật)
        if not storage_path.startswith(f"{user_id}/"):
            raise HTTPException(status_code=403, detail="Không có quyền truy cập file này")
        
        # Tạo public URL
        from app.services.file_storage_service import AWS_S3_ENDPOINT_URL, BUCKET_NAME
        public_url = f"{AWS_S3_ENDPOINT_URL}/{BUCKET_NAME}/{storage_path}"
        
        return SuccessResponse(
            message="Lấy thông tin file thành công",
            data={
                "storage_path": storage_path,
                "public_url": public_url,
                "user_id": user_id
            }
        )
        
    except HTTPException:
        # Re-raise HTTPException để giữ nguyên status code và detail
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy thông tin file: {str(e)}")

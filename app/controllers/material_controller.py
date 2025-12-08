from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid

from app.database.database import get_db
from app.dto.material_dto import MaterialCreate, MaterialUpdate, MaterialRead, MaterialInfo
from app.services.material_service import MaterialService
from app.middlewares.auth_middleware import get_current_user, get_current_active_superuser
from app.models.user import User

router = APIRouter(prefix="/materials", tags=["Materials"])


@router.post("/", response_model=MaterialRead, status_code=status.HTTP_201_CREATED)
async def create_material(
    material_data: MaterialCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tạo vật liệu mới.
    
    - **name**: Tên vật liệu (bắt buộc)
    - **description**: Mô tả chi tiết về vật liệu (tùy chọn)
    - **user_id**: ID người dùng tạo (chỉ admin có thể truyền, mặc định là current_user)
    """
    try:
        return await MaterialService.create_material(
            db, material_data, current_user.id
        )
    except Exception as e:
        raise HTTPException(
            status_code=getattr(e, 'status_code', 500),
            detail=str(e)
        )


@router.get("/", response_model=List[MaterialRead])
async def get_materials(
    skip: int = Query(0, ge=0, description="Số bản ghi bỏ qua"),
    limit: int = Query(100, ge=1, le=1000, description="Số bản ghi trả về"),
    user_id: Optional[uuid.UUID] = Query(None, description="Lọc theo user ID"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách vật liệu với phân trang.
    
    - **skip**: Số bản ghi bỏ qua (mặc định: 0)
    - **limit**: Số bản ghi trả về (mặc định: 100, tối đa: 1000)
    - **user_id**: Lọc theo user ID (tùy chọn)
    """
    try:
        # Chỉ admin có thể lọc theo user_id khác
        if user_id and not current_user.is_admin:
            user_id = current_user.id
            
        return await MaterialService.get_all_materials(db, skip, limit, user_id)
    except Exception as e:
        raise HTTPException(
            status_code=getattr(e, 'status_code', 500),
            detail=str(e)
        )


@router.get("/{material_id}", response_model=MaterialRead)
async def get_material(
    material_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin chi tiết của vật liệu theo ID.
    """
    try:
        return await MaterialService.get_material(db, material_id)
    except Exception as e:
        raise HTTPException(
            status_code=getattr(e, 'status_code', 500),
            detail=str(e)
        )


@router.put("/{material_id}", response_model=MaterialRead)
async def update_material(
    material_id: uuid.UUID,
    material_data: MaterialUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cập nhật thông tin vật liệu.
    
    - **name**: Tên vật liệu (tùy chọn)
    - **description**: Mô tả chi tiết về vật liệu (tùy chọn)
    """
    try:
        return await MaterialService.update_material(db, material_id, material_data)
    except Exception as e:
        raise HTTPException(
            status_code=getattr(e, 'status_code', 500),
            detail=str(e)
        )


@router.delete("/{material_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_material(
    material_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)  # Chỉ admin có thể xóa
):
    """
    Xóa vật liệu (chỉ dành cho admin).
    """
    try:
        success = await MaterialService.delete_material(db, material_id)
        if not success:
            raise HTTPException(
                status_code=404,
                detail="Vật liệu không tồn tại"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=getattr(e, 'status_code', 500),
            detail=str(e)
        )


@router.get("/search/name", response_model=List[MaterialRead])
async def search_materials_by_name(
    name: str = Query(..., description="Tên vật liệu cần tìm kiếm"),
    skip: int = Query(0, ge=0, description="Số bản ghi bỏ qua"),
    limit: int = Query(100, ge=1, le=1000, description="Số bản ghi trả về"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tìm kiếm vật liệu theo tên (case-insensitive).
    
    - **name**: Tên vật liệu cần tìm kiếm (bắt buộc)
    - **skip**: Số bản ghi bỏ qua (mặc định: 0)
    - **limit**: Số bản ghi trả về (mặc định: 100, tối đa: 1000)
    """
    try:
        return await MaterialService.search_materials(db, name, skip, limit)
    except Exception as e:
        raise HTTPException(
            status_code=getattr(e, 'status_code', 500),
            detail=str(e)
        )


@router.post("/batch/info", response_model=List[MaterialInfo])
async def get_materials_info_batch(
    material_ids: List[uuid.UUID],
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin đơn giản của nhiều vật liệu theo danh sách IDs.
    
    - **material_ids**: Danh sách ID vật liệu cần lấy thông tin
    """
    try:
        return await MaterialService.get_materials_info_by_ids(db, material_ids)
    except Exception as e:
        raise HTTPException(
            status_code=getattr(e, 'status_code', 500),
            detail=str(e)
        )
import json
import os
from fastapi import APIRouter, Depends, Form, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
import httpx
import logging
from pydantic import BaseModel

from app.database.database import get_db
from app.middlewares.api_key_middleware import get_user_for_chatbot
from app.models.user import User
from app.dto.response import ResponseModel
from app.configs.settings import settings
from app.logging.decorators import with_log_context

logger = logging.getLogger(__name__)

router = APIRouter(tags=["FAQ Mobile"])

# Pydantic models for FAQ data
class FaqCreate(BaseModel):
    classification: str
    question: str
    answer: str

class FaqRow(BaseModel):
    faq_id: str
    classification: str
    question: str
    answer: str
    customer_id: str

@router.get("/mobile-faqs")
@with_log_context(
    event_action="faq_mobile.list",
    event_category="faq_mobile",
    source_layer="controller",
    source_controller="faq_mobile_controller",
)
async def get_all_faqs(
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    """Lấy tất cả các cặp FAQ của một khách hàng từ ChatbotMobile API."""
    try:
        current_user, scopes = auth_result
        customer_id = str(current_user.id)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{settings.CHATBOT_API_BASE_URL}/faqs/{customer_id}")
            
            if response.status_code == 200:
                faqs = response.json()
                return ResponseModel.success(
                    data=faqs,
                    message=f"Lấy thành công {len(faqs)} FAQs cho khách hàng '{customer_id}'"
                )
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Lỗi từ ChatbotMobile API: {response.text}"
                )
                
    except httpx.RequestError as e:
        logger.error(f"Request error when calling ChatbotMobile API: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến ChatbotMobile API"
        )
    except Exception as e:
        logger.error(f"Error in get_all_faqs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# @router.post("/mobile-faq")
# async def add_faq(
#     faq_data: FaqCreate,
#     db: AsyncSession = Depends(get_db),
#     auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
# ):
#     """Thêm mới một cặp FAQ thông qua ChatbotMobile API."""
#     try:
#         current_user, scopes = auth_result
#         customer_id = str(current_user.id)
        
#         async with httpx.AsyncClient() as client:
#             response = await client.post(
#                 f"{settings.CHATBOT_API_BASE_URL}/faq/{customer_id}",
#                 json=faq_data.model_dump()
#             )
            
#             if response.status_code == 200:
#                 result = response.json()
#                 return ResponseModel.success(
#                     data=result,
#                     message="FAQ đã được thêm thành công"
#                 )
#             else:
#                 raise HTTPException(
#                     status_code=response.status_code,
#                     detail=f"Lỗi từ ChatbotMobile API: {response.text}"
#                 )
                
#     except httpx.RequestError as e:
#         logger.error(f"Request error when calling ChatbotMobile API: {e}")
#         raise HTTPException(
#             status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
#             detail="Không thể kết nối đến ChatbotMobile API"
#         )
#     except Exception as e:
#         logger.error(f"Error in add_faq: {e}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=str(e)
#         )

@router.post("/mobile-faq")
@with_log_context(
    event_action="faq_mobile.create_or_update",
    event_category="faq_mobile",
    source_layer="controller",
    source_controller="faq_mobile_controller",
)
async def add_faq(
    faq_data: str = Form(...),
    files: List[UploadFile] = File(None),
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    """
    Tạo mới hoặc cập nhật thông tin cặp FAQ.
    Nếu có files, upload lên cloud và ghi đè URLs vào faq_data.images.
    Gọi API từ ChatbotMobile.
    """
    try:
        # Lấy user_id từ current_user
        current_user, scopes = auth_result
        customer_id = str(current_user.id)
        if not customer_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Parse JSON string thành dictionary
        try:
            faq_data_dict = json.loads(faq_data)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"faq_data không phải là JSON hợp lệ: {str(e)}")
        
        # Upload files nếu có files được gửi lên
        if files and len(files) > 0:
            from app.services.file_storage_service import FileStorageService
            uploaded_urls = []
            
            for file in files:
                if file and file.filename:
                    upload_result = await FileStorageService.upload_file_to_cms(file, customer_id)
                    uploaded_urls.append(upload_result["public_url"])
            
            # Ghi đè URLs public vào faq_data.images
            if uploaded_urls:
                faq_data_dict["images"] = uploaded_urls
                # Giữ lại image cũ nếu có (backward compatibility)
                if len(uploaded_urls) == 1:
                    faq_data_dict["image"] = uploaded_urls[0]
        
        # Gọi API ChatbotMobile để lưu thông tin cửa hàng
        chatbot_mobile_url = os.getenv("CHATBOT_API_BASE_URL", "http://localhost:8001")
        
        async with httpx.AsyncClient() as client:
            url = f"{chatbot_mobile_url}/faq/{customer_id}"
            headers = {"Content-Type": "application/json"}
            response = await client.post(url, json=faq_data_dict, headers=headers, timeout=None)
            
            if response.status_code in [200, 201]:
                return response.json()
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotMobile: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobile: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lưu thông tin cặp FAQ: {str(e)}")

@router.put("/mobile-faq/{faq_id}")
@with_log_context(
    event_action="faq_mobile.update",
    event_category="faq_mobile",
    source_layer="controller",
    source_controller="faq_mobile_controller",
)
async def update_faq(
    faq_id: str,
    faq_data: str = Form(...),
    files: List[UploadFile] = File(None),
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    """Cập nhật một cặp FAQ thông qua ChatbotMobile API."""
    try:
        current_user, scopes = auth_result
        customer_id = str(current_user.id)
        
        # Parse JSON string thành dictionary
        try:
            faq_data_dict = json.loads(faq_data)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"faq_data không phải là JSON hợp lệ: {str(e)}")
        
        # Upload files nếu có files được gửi lên
        if files and len(files) > 0:
            from app.services.file_storage_service import FileStorageService
            uploaded_urls = []
            
            for file in files:
                if file and file.filename:
                    upload_result = await FileStorageService.upload_file_to_cms(file, customer_id)
                    uploaded_urls.append(upload_result["public_url"])
            
            # Ghi đè URLs public vào faq_data.images
            if uploaded_urls:
                faq_data_dict["images"] = uploaded_urls
                # Giữ lại image cũ nếu có (backward compatibility)
                if len(uploaded_urls) == 1:
                    faq_data_dict["image"] = uploaded_urls[0]
        
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{settings.CHATBOT_API_BASE_URL}/faq/{customer_id}/{faq_id}",
                json=faq_data_dict
            )
            
            if response.status_code == 200:
                result = response.json()
                return ResponseModel.success(
                    data=result,
                    message="FAQ đã được cập nhật thành công"
                )
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Lỗi từ ChatbotMobile: {response.text}"
                )
                
    except httpx.RequestError as e:
        logger.error(f"Request error when calling ChatbotMobile: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến ChatbotMobile"
        )
    except Exception as e:
        logger.error(f"Error in update_faq: {e}")
        raise HTTPException(status_code=500, detail=f"Lỗi khi cập nhật cặp FAQ: {str(e)}")

@router.delete("/mobile-faq/{faq_id}")
@with_log_context(
    event_action="faq_mobile.delete",
    event_category="faq_mobile",
    source_layer="controller",
    source_controller="faq_mobile_controller",
)
async def delete_faq(
    faq_id: str,
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    """Xóa một cặp FAQ thông qua ChatbotMobile API."""
    try:
        current_user, scopes = auth_result
        customer_id = str(current_user.id)
        
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{settings.CHATBOT_API_BASE_URL}/faq/{customer_id}/{faq_id}")
            
            if response.status_code == 200:
                result = response.json()
                return ResponseModel.success(
                    data=result,
                    message="FAQ đã được xóa thành công"
                )
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Lỗi từ ChatbotMobile API: {response.text}"
                )
                
    except httpx.RequestError as e:
        logger.error(f"Request error when calling ChatbotMobile API: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến ChatbotMobile API"
        )
    except Exception as e:
        logger.error(f"Error in delete_faq: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.delete("/mobile-faqs")
@with_log_context(
    event_action="faq_mobile.delete_all",
    event_category="faq_mobile",
    source_layer="controller",
    source_controller="faq_mobile_controller",
)
async def delete_all_faqs(
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    """Xóa TẤT CẢ các cặp FAQ của một khách hàng thông qua ChatbotMobile API."""
    try:
        current_user, scopes = auth_result
        customer_id = str(current_user.id)
        
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{settings.CHATBOT_API_BASE_URL}/faqs/{customer_id}")
            
            if response.status_code == 200:
                result = response.json()
                return ResponseModel.success(
                    data=result,
                    message=f"Đã xóa thành công tất cả FAQs cho khách hàng '{customer_id}'"
                )
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Lỗi từ ChatbotMobile API: {response.text}"
                )
                
    except httpx.RequestError as e:
        logger.error(f"Request error when calling ChatbotMobile API: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến ChatbotMobile API"
        )
    except Exception as e:
        logger.error(f"Error in delete_all_faqs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/mobile-faq/import")
@with_log_context(
    event_action="faq_mobile.import",
    event_category="faq_mobile",
    source_layer="controller",
    source_controller="faq_mobile_controller",
)
async def import_faq_from_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    """Import FAQ từ file Excel thông qua ChatbotMobile API."""
    try:
        current_user, scopes = auth_result
        customer_id = str(current_user.id)
        
        async with httpx.AsyncClient() as client:
            files = {"file": (file.filename, file.file, file.content_type)}
            response = await client.post(
                f"{settings.CHATBOT_API_BASE_URL}/insert-faq/{customer_id}",
                files=files
            )
            
            if response.status_code == 200:
                result = response.json()
                return ResponseModel.success(
                    data=result,
                    message="Import FAQ thành công"
                )
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Lỗi từ ChatbotMobile API: {response.text}"
                )
                
    except httpx.RequestError as e:
        logger.error(f"Request error when calling ChatbotMobile API: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến ChatbotMobile API"
        )
    except Exception as e:
        logger.error(f"Error in import_faq_from_file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/mobile-faq/export")
@with_log_context(
    event_action="faq_mobile.export",
    event_category="faq_mobile",
    source_layer="controller",
    source_controller="faq_mobile_controller",
)
async def export_faq_to_excel(
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    """Export FAQ ra file Excel thông qua ChatbotMobile API."""
    try:
        current_user, scopes = auth_result
        customer_id = str(current_user.id)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{settings.CHATBOT_API_BASE_URL}/faq-export/{customer_id}")
            
            if response.status_code == 200:
                # Trả về file Excel
                from fastapi.responses import StreamingResponse
                import io
                
                content = response.content
                headers = {
                    'Content-Disposition': 'attachment; filename="faq_export.xlsx"',
                    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                }
                
                return StreamingResponse(
                    io.BytesIO(content), 
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers=headers
                )
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Lỗi từ ChatbotMobile API: {response.text}"
                )
                
    except httpx.RequestError as e:
        logger.error(f"Request error when calling ChatbotMobile API: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến ChatbotMobile API"
        )
    except Exception as e:
        logger.error(f"Error in export_faq_to_excel: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

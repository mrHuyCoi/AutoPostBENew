from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Body, Response, Form
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session
from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user

from app.models.user import User
import httpx
import os
import io
import base64
import json
import asyncio
from typing import Dict, Any, Optional
from app.utils.crypto import token_encryption
from app.logging.decorators import with_log_context

router = APIRouter()

# URL của ChatbotMobileStore API
CHATBOT_API_BASE_URL = os.getenv("CHATBOT_API_BASE_URL", "http://localhost:8001")

def extract_text_from_word(content: bytes) -> str:
    """
    Extract text từ file Word (.docx)
    """
    try:
        from docx import Document
        doc = Document(io.BytesIO(content))
        text = []
        for paragraph in doc.paragraphs:
            text.append(paragraph.text)
        return "\n".join(text)
    except ImportError:
        # Nếu chưa cài python-docx, trả về thông báo lỗi
        return "Lỗi: Cần cài đặt thư viện python-docx để đọc file Word"
    except Exception as e:
        return f"Lỗi khi đọc file Word: {str(e)}"

def extract_text_from_pdf(content: bytes) -> str:
    """
    Extract text từ file PDF
    """
    try:
        import PyPDF2
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(content))
        text = []
        for page in pdf_reader.pages:
            text.append(page.extract_text())
        return "\n".join(text)
    except ImportError:
        # Nếu chưa cài PyPDF2, trả về thông báo lỗi
        return "Lỗi: Cần cài đặt thư viện PyPDF2 để đọc file PDF"
    except Exception as e:
        return f"Lỗi khi đọc file PDF: {str(e)}"

async def call_chatbot_api(
    endpoint: str, 
    method: str = "GET", 
    data: Dict[str, Any] = None,
    file: UploadFile = None,
    user_id: str = None
) -> Dict[str, Any]:
    """
    Gọi API từ ChatbotMobileStore
    """
    try:
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}{endpoint}"
            headers = {}
            
            if method == "GET":
                response = await client.get(url, headers=headers, timeout=None)
            elif method == "POST":
                if file:
                    # Upload file
                    files = {"file": (file.filename, file.file, file.content_type)}
                    response = await client.post(url, files=files, timeout=None)
                else:
                    # Upload text
                    headers["Content-Type"] = "application/json"
                    response = await client.post(url, json=data, headers=headers, timeout=None)
            elif method == "PUT":
                if file:
                    # Replace file
                    files = {"file": (file.filename, file.file, file.content_type)}
                    response = await client.put(url, files=files, timeout=None)
                else:
                    # Replace text
                    headers["Content-Type"] = "application/json"
                    response = await client.put(url, json=data, headers=headers, timeout=None)
            elif method == "DELETE":
                response = await client.delete(url, headers=headers, timeout=None)
            else:
                raise HTTPException(status_code=400, detail="Method không được hỗ trợ")
            
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "")
                if "application/json" in content_type:
                    return response.json()
                elif content_type.startswith("text/"):  # Nếu là plain text
                    return {"content": response.text}
                elif "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in content_type:
                    # File Word (.docx) - extract text
                    text_content = extract_text_from_word(response.content)
                    return {"content": text_content}
                elif content_type == "application/pdf":
                    # File PDF - extract text
                    text_content = extract_text_from_pdf(response.content)
                    return {"content": text_content}
                else:
                    # Các loại file khác - trả về base64 hoặc thông báo
                    try:
                        # Thử decode như text trước
                        text_content = response.content.decode("utf-8", errors="replace")
                        return {"content": text_content}
                    except:
                        # Nếu không decode được, trả về base64
                        base64_content = base64.b64encode(response.content).decode("utf-8")
                        return {"content": f"[Binary file - Base64]: {base64_content}"}
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotMobileStore: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi gọi ChatbotMobileStore API: {str(e)}")


async def _trigger_graphrag_reindex(
    user_id: str,
    current_user: User,
    *,
    provider: Optional[str] = None,
    method: Optional[str] = "fast",
    persist_to_db: Optional[bool] = True,
    overwrite: Optional[bool] = True,
    chat_model: Optional[str] = None,
    embedding_model: Optional[str] = None,
    ) -> Dict[str, Any]:
    openai_key = token_encryption.decrypt(current_user.openai_api_key) if getattr(current_user, "openai_api_key", None) else None
    gemini_key = token_encryption.decrypt(current_user.gemini_api_key) if getattr(current_user, "gemini_api_key", None) else None

    prov = (provider or "").strip().lower() or None
    if prov not in ("openai", "gemini", None):
        raise HTTPException(status_code=400, detail="Provider không hợp lệ. Hỗ trợ: openai | gemini")

    if prov is None:
        if openai_key:
            prov = "openai"
        elif gemini_key:
            prov = "gemini"
        else:
            raise HTTPException(status_code=400, detail="Không tìm thấy API key. Vui lòng cấu hình OpenAI hoặc Gemini API key cho tài khoản.")

    api_key = openai_key if prov == "openai" else gemini_key
    if not api_key:
        raise HTTPException(status_code=400, detail=f"Không có API key cho provider '{prov}'.")

    payload: Dict[str, Any] = {
        "api_key": api_key,
        "method": method or "fast",
        "persist_to_db": True if persist_to_db is None else persist_to_db,
        "overwrite": True if overwrite is None else overwrite,
        "provider": prov,
    }
    if prov == "gemini":
        if chat_model:
            payload["chat_model"] = chat_model
        if embedding_model:
            payload["embedding_model"] = embedding_model

    try:
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}/graphrag/reindex/{user_id}"
            resp = await client.post(url, json=payload, timeout=None)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=f"Lỗi từ ChatbotMobileStore: {resp.text}")
            return resp.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi gọi GraphRAG reindex: {str(e)}")

@router.post("/documents/upload-text")
@with_log_context(
    event_action="document.upload_text",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def upload_text(
    document_input: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Thêm dữ liệu văn bản thô vào cơ sở dữ liệu vector của người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được xác thực qua JWT token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/upload-text/{user_id}",
            method="POST",
            data=document_input,
            user_id=user_id
        )
        try:
            await _trigger_graphrag_reindex(user_id, current_user)
        except Exception:
            pass
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/documents/upload-file")
@with_log_context(
    event_action="document.upload_file",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Thêm dữ liệu từ một tệp vào cơ sở dữ liệu vector của người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được xác thực qua JWT token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/upload-file/{user_id}",
            method="POST",
            file=file,
            user_id=user_id
        )
        try:
            await _trigger_graphrag_reindex(user_id, current_user)
        except Exception:
            pass
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/documents/upload-url")
@with_log_context(
    event_action="document.upload_url",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def upload_url(
    document_input: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Thêm dữ liệu từ một URL vào cơ sở dữ liệu vector của người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được xác thực qua JWT token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/upload-url/{user_id}",
            method="POST",
            data=document_input,
            user_id=user_id
        )
        try:
            await _trigger_graphrag_reindex(user_id, current_user)
        except Exception:
            pass
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/documents/reindex")
@with_log_context(
    event_action="document.reindex",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def reindex_documents(
    provider: Optional[str] = Body(None),
    method: Optional[str] = Body("fast"),
    persist_to_db: Optional[bool] = Body(True),
    overwrite: Optional[bool] = Body(True),
    chat_model: Optional[str] = Body(None),
    embedding_model: Optional[str] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        return await _trigger_graphrag_reindex(
            user_id,
            current_user,
            provider=provider,
            method=method,
            persist_to_db=persist_to_db,
            overwrite=overwrite,
            chat_model=chat_model,
            embedding_model=embedding_model,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/documents/list")
@with_log_context(
    event_action="document.list",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def list_documents(
    limit: int = 100, 
    offset: int = 0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Liệt kê tất cả các document trong class `document_{user_id}`.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/documents/{user_id}?limit={limit}&offset={offset}",
            method="GET",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/documents-original/list")
@with_log_context(
    event_action="document.original.list",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def list_documents_original(
    source: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Liệt kê tất cả các document trong class `document_{user_id}`.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/document-original/{user_id}?source={source}",
            method="GET",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/documents/delete-all")
@with_log_context(
    event_action="document.delete_all",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def delete_all_documents(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa tất cả dữ liệu cho người dùng (xóa collection `document_{user_id}`).
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/documents/{user_id}",
            method="DELETE",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/documents/delete-by-source")
@with_log_context(
    event_action="document.delete_by_source",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def delete_documents_by_source(
    source: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa tất cả documents theo source cụ thể.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/sources/{user_id}?source={source}",
            method="DELETE",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/documents/sources")
@with_log_context(
    event_action="document.sources.list",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def list_document_sources(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách các source của documents.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/sources/{user_id}",
            method="GET",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Store Info API endpoints for ChatbotCustom integration

@router.post("/documents/store-info")
@with_log_context(
    event_action="document.store_info.create_or_update",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def create_or_update_store_info(
    store_info: str = Form(...),
    file: UploadFile = File(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tạo mới hoặc cập nhật thông tin cửa hàng.
    Nếu có file, upload lên cloud và ghi đè URL vào store_info.store_image.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Parse JSON string thành dictionary
        try:
            store_info_dict = json.loads(store_info)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"store_info không phải là JSON hợp lệ: {str(e)}")
        
        # Chỉ upload file nếu có file được gửi lên
        if file and file.filename:
            from app.services.file_storage_service import FileStorageService
            upload_result = await FileStorageService.upload_file_to_cms(file, user_id)
            
            # Ghi đè URL public vào store_info.store_image
            store_info_dict["store_image"] = upload_result["public_url"]
        
        # Gọi API ChatbotCustom để lưu thông tin cửa hàng
        chatbot_custom_url = os.getenv("CHATBOT_CUSTOM_API_BASE_URL", "http://localhost:8002")
        
        async with httpx.AsyncClient() as client:
            url = f"{chatbot_custom_url}/{user_id}"
            headers = {"Content-Type": "application/json"}
            response = await client.post(url, json=store_info_dict, headers=headers, timeout=None)
            
            if response.status_code in [200, 201]:
                return response.json()
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lưu thông tin cửa hàng: {str(e)}")

@router.get("/documents/store-info")
@with_log_context(
    event_action="document.store_info.get",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def get_store_info(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin cửa hàng của người dùng hiện tại.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        # Gọi API ChatbotCustom để lấy thông tin cửa hàng
        chatbot_custom_url = os.getenv("CHATBOT_CUSTOM_API_BASE_URL", "http://localhost:8002")
        
        async with httpx.AsyncClient() as client:
            url = f"{chatbot_custom_url}/{user_id}"
            response = await client.get(url, timeout=None)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # Trả về thông tin mặc định nếu chưa có
                return {
                    "store_name": "",
                    "store_address": "", 
                    "store_phone": "",
                    "store_email": "",
                    "store_website": "",
                    "store_facebook": "",
                    "store_address_map": "",
                    "store_image": "",
                    "info_more": ""
                }
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy thông tin cửa hàng: {str(e)}")

@router.delete("/documents/store-info")
@with_log_context(
    event_action="document.store_info.delete",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def delete_store_info(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa thông tin cửa hàng của người dùng hiện tại.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        # Gọi API ChatbotCustom để xóa thông tin cửa hàng
        chatbot_custom_url = os.getenv("CHATBOT_CUSTOM_API_BASE_URL", "http://localhost:8002")
        
        async with httpx.AsyncClient() as client:
            url = f"{chatbot_custom_url}/{user_id}"
            response = await client.delete(url, timeout=None)
            
            if response.status_code in [200, 204]:
                return {"message": "Đã xóa thông tin cửa hàng thành công"}
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa thông tin cửa hàng: {str(e)}")

# Store Info API endpoints for ChatbotMobile integration

@router.post("/documents-mobile/store-info")
@with_log_context(
    event_action="document_mobile.store_info.create_or_update",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def create_or_update_store_info_mobile(
    store_info: str = Form(...),
    file: UploadFile = File(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tạo mới hoặc cập nhật thông tin cửa hàng.
    Nếu có file, upload lên cloud và ghi đè URL vào store_info.store_image.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Parse JSON string thành dictionary
        try:
            store_info_dict = json.loads(store_info)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"store_info không phải là JSON hợp lệ: {str(e)}")
        
        # Chỉ upload file nếu có file được gửi lên
        if file and file.filename:
            from app.services.file_storage_service import FileStorageService
            upload_result = await FileStorageService.upload_file_to_cms(file, user_id)
            
            # Ghi đè URL public vào store_info.store_image
            store_info_dict["store_image"] = upload_result["public_url"]
        
        # Gọi API ChatbotCustom để lưu thông tin cửa hàng
        chatbot_mobile_url = CHATBOT_API_BASE_URL
        
        async with httpx.AsyncClient() as client:
            url = f"{chatbot_mobile_url}/store-info/{user_id}"
            headers = {"Content-Type": "application/json"}
            response = await client.put(url, json=store_info_dict, headers=headers, timeout=None)
            
            if response.status_code in [200, 201]:
                return response.json()
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lưu thông tin cửa hàng: {str(e)}")

@router.get("/documents-mobile/store-info")
@with_log_context(
    event_action="document_mobile.store_info.get",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def get_store_info_mobile(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin cửa hàng của người dùng hiện tại.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        # Gọi API ChatbotCustom để lấy thông tin cửa hàng
        chatbot_mobile_url = CHATBOT_API_BASE_URL
        
        async with httpx.AsyncClient() as client:
            url = f"{chatbot_mobile_url}/store-info/{user_id}"
            response = await client.get(url, timeout=None)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # Trả về thông tin mặc định nếu chưa có
                return {
                    "store_name": "",
                    "store_address": "", 
                    "store_phone": "",
                    "store_email": "",
                    "store_website": "",
                    "store_facebook": "",
                    "store_address_map": "",
                    "store_image": "",
                    "info_more": ""
                }
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy thông tin cửa hàng: {str(e)}")

@router.delete("/documents-mobile/store-info")
@with_log_context(
    event_action="document_mobile.store_info.delete",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def delete_store_info_mobile(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa thông tin cửa hàng của người dùng hiện tại.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        # Gọi API ChatbotCustom để xóa thông tin cửa hàng
        chatbot_mobile_url = CHATBOT_API_BASE_URL
        
        async with httpx.AsyncClient() as client:
            url = f"{chatbot_mobile_url}/store-info/{user_id}"
            response = await client.delete(url, timeout=None)
            
            if response.status_code in [200, 204]:
                return {"message": "Đã xóa thông tin cửa hàng thành công"}
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa thông tin cửa hàng: {str(e)}")

@router.post("/documents/upload-website")
@with_log_context(
    event_action="document.upload_website",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def upload_website(
    website_url: str = Form(...),
    source: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Start a sitemap crawl task and return task_id immediately.
    Use the task_id to get progress via /documents/sitemap-progress/{task_id} or cancel via /documents/cancel-crawl/{task_id}
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Prepare form data for the external API
        form_data = {
            "website_url": website_url
        }
        if source:
            form_data["source"] = source
        
        # Call the external API to start crawl task
        async with httpx.AsyncClient(timeout=30) as client:
            url = f"{CHATBOT_API_BASE_URL}/start-sitemap-crawl/{user_id}"
            
            response = await client.post(
                url,
                data=form_data,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Lỗi từ ChatbotMobileStore: {response.text}"
                )
            
            result = response.json()
            
            # Add our own endpoints to the response
            result["progress_url"] = f"/documents/sitemap-progress/{result['task_id']}"
            result["cancel_url"] = f"/documents/cancel-crawl/{result['task_id']}"
            
            return result
            
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi hệ thống: {str(e)}")

@router.get("/documents/sitemap-progress/{task_id}")
@with_log_context(
    event_action="document.sitemap_progress.get",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def get_sitemap_progress(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Stream progress for a specific crawl task.
    """
    async def generate_progress():
        try:
            # Lấy user_id từ current_user
            user_id = str(current_user.id)
            if not user_id:
                yield f"data: {json.dumps({'status': 'error', 'message': '❌ Không thể xác định người dùng'})}\n\n"
                return
            
            # Call the external streaming API
            async with httpx.AsyncClient(timeout=None) as client:
                url = f"{CHATBOT_API_BASE_URL}/sitemap-progress/{task_id}"
                
                async with client.stream(
                    "GET",
                    url,
                    headers={
                        "Accept": "text/event-stream",
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive"
                    }
                ) as response:
                    if response.status_code == 404:
                        yield f"data: {json.dumps({'status': 'error', 'message': f'❌ Task {task_id} không tìm thấy'})}\n\n"
                        return
                    elif response.status_code != 200:
                        yield f"data: {json.dumps({'status': 'error', 'message': f'❌ Lỗi từ ChatbotMobileStore: {response.status_code}'})}\n\n"
                        return
                    
                    # Stream the response from the external API
                    async for chunk in response.aiter_text():
                        if chunk.strip():
                            # Forward the chunk as-is since it's already in SSE format
                            yield chunk
                            # Small delay to prevent overwhelming
                            await asyncio.sleep(0.01)
                            
        except httpx.RequestError as e:
            yield f"data: {json.dumps({'status': 'error', 'message': f'❌ Không thể kết nối đến ChatbotMobileStore: {str(e)}'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'status': 'error', 'message': f'❌ Lỗi hệ thống: {str(e)}'})}\n\n"
    
    return StreamingResponse(
        generate_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "X-Task-ID": task_id,  # Return task ID in header
        }
    )

@router.post("/documents/cancel-crawl/{task_id}")
@with_log_context(
    event_action="document.cancel_crawl",
    event_category="document",
    source_layer="controller",
    source_controller="document_controller",
)
async def cancel_crawl(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cancel an active crawl task.
    """
    try:
        # Lấy user_id từ current_user
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        # Call the external API to cancel crawl task
        async with httpx.AsyncClient(timeout=30) as client:
            url = f"{CHATBOT_API_BASE_URL}/cancel-crawl/{task_id}"
            
            response = await client.post(
                url,
                headers={
                    "Accept": "application/json"
                }
            )
            
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Task {task_id} không tìm thấy")
            elif response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Lỗi từ ChatbotMobileStore: {response.text}"
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi hệ thống: {str(e)}")

from fastapi import APIRouter, Depends, HTTPException, status
from app.celery_app import celery_app
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from typing import Dict, Any, Optional
from app.logging.decorators import with_log_context

task_router = APIRouter()

@task_router.get("/status/{task_id}", response_model=Dict[str, Any])
@with_log_context(
    event_action="task.status.get",
    event_category="task",
    source_layer="controller",
    source_controller="task_controller",
)
async def get_task_status(task_id: str, current_user: User = Depends(get_current_user)):
    """
    Lấy trạng thái của một Celery task theo ID
    """
    try:
        task = celery_app.AsyncResult(task_id)
        response = {
            "task_id": task_id,
            "status": task.status,
            "result": task.result if task.status == "SUCCESS" else None,
        }
        
        # Thêm thông tin lỗi nếu task thất bại
        if task.status == "FAILURE":
            response["error"] = str(task.result)
            
        return response
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi kiểm tra trạng thái task: {str(e)}"
        )

@task_router.post("/retry/{task_id}", response_model=Dict[str, Any])
@with_log_context(
    event_action="task.retry",
    event_category="task",
    source_layer="controller",
    source_controller="task_controller",
)
async def retry_task(task_id: str, current_user: User = Depends(get_current_user)):
    """
    Thử lại một task đã thất bại
    """
    try:
        task = celery_app.AsyncResult(task_id)
        
        if task.status != "FAILURE":
            return {
                "task_id": task_id,
                "status": task.status,
                "message": "Chỉ có thể thử lại các task đã thất bại"
            }
            
        # Lấy thông tin task gốc
        task_info = task.info
        if not task_info or not hasattr(task_info, "name"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Không thể lấy thông tin task để thử lại"
            )
            
        # Thử lại task với cùng tham số
        new_task = celery_app.send_task(
            task_info.name,
            args=task_info.args,
            kwargs=task_info.kwargs
        )
        
        return {
            "original_task_id": task_id,
            "new_task_id": new_task.id,
            "status": "PENDING",
            "message": "Đã gửi lại task"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi thử lại task: {str(e)}"
        )
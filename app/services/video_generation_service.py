import logging
import uuid
from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.video_generation_schema import VideoGenerationRequest, VideoGenerationResponse

logger = logging.getLogger(__name__)

# In-memory storage cho video jobs (tạm thời, nên migrate sang database sau)
# Format: {video_id: {user_id, status, video_url, message, created_at, updated_at}}
_video_jobs_storage: Dict[str, Dict[str, Any]] = {}


class VideoGenerationService:
    """
    Service xử lý logic tạo video bằng AI.
    Service này có thể tích hợp với các dịch vụ video generation bên ngoài
    hoặc xử lý nội bộ tùy theo yêu cầu.
    """
    
    @staticmethod
    async def generate_video(
        db: AsyncSession,
        user_id: uuid.UUID,
        request: VideoGenerationRequest,
        openai_key: Optional[str] = None,
        gemini_key: Optional[str] = None,
    ) -> VideoGenerationResponse:
        """
        Tạo video bằng AI dựa trên request.
        
        Args:
            db: Database session
            user_id: ID của user
            request: Thông tin request tạo video
            openai_key: OpenAI API key (nếu có)
            gemini_key: Gemini API key (nếu có)
            
        Returns:
            VideoGenerationResponse với thông tin video đã tạo
            
        Raises:
            HTTPException: Nếu có lỗi xảy ra
        """
        try:
            # Validate request
            VideoGenerationService._validate_request(request)
            
            # Tạo video ID duy nhất
            video_id = str(uuid.uuid4())
            
            logger.info(f"Bắt đầu tạo video {video_id} cho user {user_id}")
            
            # TODO: Tích hợp với service tạo video thực tế
            # Có thể là:
            # 1. Gọi API bên ngoài (ví dụ: D-ID, Synthesia, hoặc service custom)
            # 2. Xử lý nội bộ với FFmpeg, TTS, etc.
            # 3. Queue job để xử lý async
            
            # Hiện tại trả về response với status "processing"
            # Trong thực tế, bạn sẽ:
            # - Tạo task/job để xử lý video generation
            # - Lưu thông tin vào database
            # - Trả về video_id để client có thể theo dõi tiến trình
            
            response = VideoGenerationResponse(
                video_id=video_id,
                video_url=None,
                status="processing",
                message="Video đang được tạo. Vui lòng theo dõi tiến trình qua video_id.",
                estimated_duration=VideoGenerationService._estimate_duration(request),
            )
            
            # Lưu job vào in-memory storage (tạm thời)
            _video_jobs_storage[video_id] = {
                "user_id": str(user_id),
                "status": "processing",
                "video_url": None,
                "message": response.message,
                "estimated_duration": response.estimated_duration,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            
            # TODO: Lưu job vào database hoặc queue (thay thế in-memory storage)
            # await VideoGenerationJobRepository.create(db, {
            #     "video_id": video_id,
            #     "user_id": user_id,
            #     "request_data": request.dict(),
            #     "status": "processing",
            #     "created_at": datetime.now(),
            # })
            
            # TODO: Trigger video generation process (async background task)
            # await VideoGenerationService._process_video_generation(video_id, request, openai_key, gemini_key)
            
            logger.info(f"Đã khởi tạo video generation job {video_id}")
            
            return response
            
        except HTTPException:
            raise
        except Exception as e:
            logger.exception(f"Lỗi khi tạo video: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi khi tạo video: {str(e)}",
            ) from e
    
    @staticmethod
    def _validate_request(request: VideoGenerationRequest) -> None:
        """Validate request data"""
        if not request.video_subject or not request.video_script:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="video_subject và video_script là bắt buộc",
            )
        
        if request.video_count < 1 or request.video_count > 10:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="video_count phải từ 1 đến 10",
            )
        
        # Validate aspect ratio
        valid_aspects = ["9:16", "16:9", "1:1", "4:3", "21:9"]
        if request.video_aspect not in valid_aspects:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"video_aspect không hợp lệ. Chỉ hỗ trợ: {', '.join(valid_aspects)}",
            )
        
        # Validate video source
        valid_sources = ["pexels", "pixabay", "unsplash", "custom"]
        if request.video_source.lower() not in valid_sources:
            logger.warning(f"Video source '{request.video_source}' không nằm trong danh sách chuẩn")
    
    @staticmethod
    def _estimate_duration(request: VideoGenerationRequest) -> int:
        """
        Ước tính thời gian hoàn thành video generation (giây).
        Logic này có thể được cải thiện dựa trên:
        - Độ dài script
        - Số lượng video materials
        - video_count
        - Độ phức tạp của effects
        """
        base_duration = 60  # 1 phút cơ bản
        
        # Thêm thời gian dựa trên độ dài script (ước tính 1 từ = 0.5 giây)
        script_words = len(request.video_script.split())
        script_duration = script_words * 0.5
        
        # Thêm thời gian cho mỗi video
        video_duration = request.video_count * 30
        
        # Thêm thời gian nếu có subtitle processing
        if request.subtitle_enabled:
            script_duration += 20
        
        total_estimated = int(base_duration + script_duration + video_duration)
        
        return min(total_estimated, 600)  # Max 10 phút
    
    @staticmethod
    async def _process_video_generation(
        video_id: str,
        request: VideoGenerationRequest,
        openai_key: Optional[str],
        gemini_key: Optional[str],
    ) -> None:
        """
        Xử lý video generation thực tế.
        Method này sẽ được gọi async trong background task hoặc worker.
        
        TODO: Implement logic thực tế:
        1. Tạo TTS từ script
        2. Tìm/download video materials từ source
        3. Ghép video theo concat_mode và transition_mode
        4. Thêm BGM
        5. Thêm subtitle nếu enabled
        6. Render video với aspect ratio
        7. Upload lên storage
        8. Cập nhật status trong database
        """
        logger.info(f"Bắt đầu xử lý video generation cho {video_id}")
        
        # Placeholder - implement actual logic here
        # Example flow:
        # 1. Generate TTS audio
        # 2. Fetch video materials
        # 3. Process and combine videos
        # 4. Add effects, BGM, subtitles
        # 5. Render final video
        # 6. Upload to storage
        # 7. Update status
        
        pass
    
    @staticmethod
    async def get_video_status(
        db: AsyncSession,
        video_id: str,
        user_id: uuid.UUID,
    ) -> VideoGenerationResponse:
        """
        Lấy trạng thái của video generation job.
        
        Args:
            db: Database session
            video_id: ID của video
            user_id: ID của user
            
        Returns:
            VideoGenerationResponse với trạng thái hiện tại
        """
        # Lấy từ in-memory storage (tạm thời)
        job = _video_jobs_storage.get(video_id)
        
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Không tìm thấy video với ID này",
            )
        
        # Kiểm tra quyền truy cập
        if job["user_id"] != str(user_id):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Bạn không có quyền truy cập video này",
            )
        
        return VideoGenerationResponse(
            video_id=video_id,
            video_url=job.get("video_url"),
            status=job.get("status", "unknown"),
            message=job.get("message", ""),
            estimated_duration=job.get("estimated_duration"),
        )
        
        # TODO: Query từ database (thay thế in-memory storage)
        # job = await VideoGenerationJobRepository.get_by_id(db, video_id, user_id)
        # if not job:
        #     raise HTTPException(status_code=404, detail="Không tìm thấy video")
        
        # return VideoGenerationResponse(
        #     video_id=video_id,
        #     video_url=job.video_url,
        #     status=job.status,
        #     message=job.message,
        #     estimated_duration=job.estimated_duration,
        # )
    
    @staticmethod
    def _update_video_status(
        video_id: str,
        status: str,
        video_url: Optional[str] = None,
        message: Optional[str] = None,
    ) -> None:
        """
        Cập nhật trạng thái video (internal method).
        Sử dụng khi video generation hoàn thành hoặc thất bại.
        """
        if video_id in _video_jobs_storage:
            _video_jobs_storage[video_id]["status"] = status
            _video_jobs_storage[video_id]["updated_at"] = datetime.now().isoformat()
            if video_url:
                _video_jobs_storage[video_id]["video_url"] = video_url
            if message:
                _video_jobs_storage[video_id]["message"] = message
            logger.info(f"Đã cập nhật status video {video_id}: {status}")


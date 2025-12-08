import asyncio
import tempfile
import os
import httpx
from datetime import datetime
from loguru import logger
from app.services.scheduled_video_service import ScheduledVideoService
from app.services.youtube_acc_service import YouTubeService
from app.database.database import async_session

class ScheduledUploadTask:
    def __init__(self):
        self.scheduled_service = ScheduledVideoService()
        self.youtube_service = YouTubeService()
    
    async def process_pending_videos(self):
        """Process all pending videos that are due for upload"""
        while True:
            try:
                async with async_session() as db:
                    # Get pending videos
                    pending_videos = await self.scheduled_service.get_pending_videos(db)
                    
                    for video in pending_videos:
                        try:
                            # Update status to processing
                            await self.scheduled_service.update_video_status(
                                db, video.id, "processing"
                            )
                            
                            # Get YouTube account token
                            token_info = await self.youtube_service.get_valid_token(
                                db, video.user_id, video.account_id
                            )
                            
                            if not token_info:
                                raise Exception("Invalid YouTube token")
                            
                            # Download video from Supabase
                            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(video.video_filename)[1])
                            try:
                                async with httpx.AsyncClient() as client:
                                    response = await client.get(video.video_url)
                                    temp_file.write(response.content)
                                temp_file.close()
                                
                                # Upload to YouTube
                                upload_result = await self.youtube_service.upload_video(
                                    db=db,
                                    user_id=video.user_id,
                                    account_id=video.account_id,
                                    video_path=temp_file.name,
                                    title=video.title,
                                    description=video.description,
                                    tags=video.tags.split(',') if video.tags else [],
                                    privacy_status=video.privacy_status
                                )
                                
                                # Update video status with YouTube information
                                await self.scheduled_service.update_video_status(
                                    db,
                                    video.id,
                                    "completed",
                                    youtube_video_id=upload_result['video_id'],
                                    youtube_url=upload_result['video_url']
                                )
                                
                                logger.info(f"Successfully uploaded scheduled video: {video.id}")
                                
                            finally:
                                # Cleanup temporary file
                                if os.path.exists(temp_file.name):
                                    os.unlink(temp_file.name)
                                    
                        except Exception as e:
                            logger.error(f"Error processing video {video.id}: {str(e)}")
                            await self.scheduled_service.update_video_status(
                                db, video.id, "failed"
                            )
                    
                    # Wait for 1 minute before next check
                    await asyncio.sleep(60)
                    
            except Exception as e:
                logger.error(f"Error in scheduled upload task: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying

async def start_scheduled_upload_task():
    """Start the scheduled upload task"""
    task = ScheduledUploadTask()
    await task.process_pending_videos() 
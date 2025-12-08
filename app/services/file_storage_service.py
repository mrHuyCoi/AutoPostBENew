from fastapi import UploadFile, HTTPException, status
from uuid import uuid4
import boto3
import os
import requests
from datetime import datetime, timedelta

# Cấu hình S3 (lấy từ biến môi trường hoặc settings)
# Nên chuyển các giá trị này vào file settings.py để quản lý tập trung
from app.configs.settings import settings
import tempfile
AWS_ACCESS_KEY_ID = settings.S3_ACCESS_KEY
AWS_SECRET_ACCESS_KEY = settings.S3_SECRET_KEY
AWS_S3_ENDPOINT_URL = settings.S3_ENDPOINT_URL
AWS_REGION_NAME = settings.S3_REGION  # hoặc "ap-southeast-1", không quan trọng với CMC
BUCKET_NAME = settings.S3_BUCKET_NAME

ALLOWED_IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp"}
ALLOWED_VIDEO_TYPES = {"video/mp4", "video/quicktime", "video/x-matroska"}

MAX_VIDEO_SIZE = 2 * 1024 * 1024 * 1024  # 5GB

class FileStorageService:
    @staticmethod
    def get_s3_client():
        """Khởi tạo và trả về S3 client"""
        return boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION_NAME,
            endpoint_url=AWS_S3_ENDPOINT_URL
        )
    
    @staticmethod
    async def delete_file_from_s3(storage_path: str):
        """Xóa file từ S3 storage"""
        try:
            s3 = FileStorageService.get_s3_client()
            s3.delete_object(Bucket=BUCKET_NAME, Key=storage_path)
            return True
        except Exception as e:
            print(f"Error deleting file {storage_path} from S3: {e}")
            return False
    
    @staticmethod
    async def upload_file_to_cms(file: UploadFile, user_id: str):
        # 1. Kiểm tra content_type hợp lệ
        if file.content_type not in ALLOWED_IMAGE_TYPES.union(ALLOWED_VIDEO_TYPES):
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file.content_type}")

        # 2. Kiểm tra tên file có hợp lệ
        if not file.filename or "." not in file.filename:
            raise HTTPException(status_code=400, detail="Invalid file name.")

        # 3. Đọc dữ liệu file
        raw_data = await file.read()
        if not isinstance(raw_data, (bytes, bytearray)):
            raise HTTPException(status_code=500, detail="File content is not bytes-like object.")

        # 4. Kiểm tra kích thước nếu là video
        if file.content_type in ALLOWED_VIDEO_TYPES and len(raw_data) > MAX_VIDEO_SIZE:
            raise HTTPException(status_code=400, detail="Kích thước video vượt quá giới hạn 5GB.")

        # 5. Tạo tên file mới
        ext = file.filename.split(".")[-1]
        new_filename = f"{uuid4()}.{ext}"
        storage_path = f"{user_id}/{new_filename}"

        # 6. Khởi tạo S3 client
        s3 = FileStorageService.get_s3_client()

        try:
            # 7. Tạo presigned PUT URL
            presigned_url = s3.generate_presigned_url(
                ClientMethod="put_object",
                Params={
                    "Bucket": BUCKET_NAME,
                    "Key": storage_path,
                    "ContentType": file.content_type
                },
                ExpiresIn=3600,
                HttpMethod="PUT"
            )

            # 8. Upload file qua presigned URL
            headers = {
                "Content-Type": file.content_type,
                "Content-Length": str(len(raw_data))
            }

            response = requests.put(presigned_url, data=raw_data, headers=headers)

            if not response.ok:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to upload {storage_path}: {e}")

        # 9. Trả về đường dẫn công khai
        public_url = f"{AWS_S3_ENDPOINT_URL}/{BUCKET_NAME}/{storage_path}"

        return {
            "storage_path": storage_path,
            "public_url": public_url,
            "file_name": file.filename,
            "file_type": "video" if "video" in file.content_type else "image",
            "size_bytes": len(raw_data)
        }
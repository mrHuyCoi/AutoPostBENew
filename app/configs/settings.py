import os
from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

# Xác định đường dẫn đến thư mục gốc của dự án
# Điều này đảm bảo file .env luôn được tìm thấy, bất kể bạn chạy ứng dụng từ đâu
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class Settings(BaseSettings):
    # Application settings
    APP_NAME: str = "Đăng Bài Tự Động"
    API_V1_PREFIX: str = "/v1"
    # SERVER_HOST: str = "http://localhost:8000"
    SERVER_HOST: str = "https://backend.doiquanai.com"
    
    # Celery settings
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    CELERY_TASK_ALWAYS_EAGER: bool = False
    
    # Security settings
    SECRET_KEY: str
    ENCRYPTION_KEY: str 
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days
    
    # Database settings
    DATABASE_URL: str
    
    # CORS settings
    CORS_ORIGINS: List[str] = ["*"]
    
    # Social media API settings
    FACEBOOK_API_VERSION: str = "v23.0"
    FACEBOOK_API_BASE_URL: str = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"
    FACEBOOK_APP_ID: str
    FACEBOOK_APP_SECRET: str
    FACEBOOK_VERIFY_TOKEN: str | None = None
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = ""
    
    @property
    def google_redirect_uri(self) -> str:
        return f"{self.SERVER_HOST}/api/v1/auth/google/callback"
    
    # Mail Settings
    MAIL_USERNAME: str
    MAIL_PASSWORD: str
    MAIL_FROM: str
    MAIL_PORT: int
    MAIL_SERVER: str
    MAIL_FROM_NAME: str

    YOUTUBE_API_VERSION: str = "v3"
    YOUTUBE_API_BASE_URL: str = "https://www.googleapis.com/youtube/v3"
    YOUTUBE_CLIENT_ID: str
    YOUTUBE_CLIENT_SECRET: str

    # OpenAI settings
    OPENAI_API_KEY: Optional[str] = None
    
    # Cloud storage settings
    CLOUD_STORAGE_PROVIDER: str = "local"
    S3_ACCESS_KEY: Optional[str] = None
    S3_SECRET_KEY: Optional[str] = None
    S3_BUCKET_NAME: Optional[str] = None
    S3_REGION: Optional[str] = None
    S3_ENDPOINT_URL: Optional[str] = None
    
    # AWS S3 settings (alternative naming)
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_S3_ENDPOINT_URL: Optional[str] = None
    AWS_REGION_NAME: Optional[str] = None
    BUCKET_NAME: Optional[str] = None
    GCS_PROJECT_ID: Optional[str] = None
    GCS_BUCKET_NAME: Optional[str] = None
    
    # Local file storage settings
    LOCAL_STORAGE_PATH: str = "./uploads"
    CLIENT_ORIGIN: str
    # Supabase settings
    SUPABASE_URL: Optional[str] = None
    SUPABASE_SERVICE_KEY: Optional[str] = None
    SUPABASE_ANON_KEY: Optional[str] = None
    SUPABASE_STORAGE_BUCKET: str = "media"
    SUPABASE_SERVICE_ROLE_KEY: Optional[str] = None
    
    # Chatbot API settings
    CHATBOT_API_BASE_URL: str = "http://localhost:8010"
    CHATBOT_CUSTOM_API_BASE_URL:str = "http://192.168.1.161:8010"
    
    # Elasticsearch settings (direct connection, same network as ChatbotMobileStore)
    ELASTICSEARCH_URL: str = "http://elasticsearch:9200"
    GEMINI_API_KEY: Optional[str] = None
    
    # Zalo API settings
    ZALO_API_BASE_URL: str = "http://localhost:3000"
    # Zalo OA (Official Account) settings
    ZALO_OA_APP_ID: str | None = None
    ZALO_OA_SECRET_KEY: str | None = None
    ZALO_OAUTH_BASE_URL: str = "https://oauth.zaloapp.com"
    ZALO_GRAPH_BASE_URL: str = "https://graph.zalo.me/v2.0"
    ZALO_OA_OPENAPI_BASE_URL: str = "https://openapi.zalo.me"
    # Callback/Webhook paths (joined with SERVER_HOST)
    ZALO_OA_CALLBACK_PATH: str = "/api/v1/zalo-oa/auth/callback"
    ZALO_OA_WEBHOOK_PATH: str = "/api/v1/zalo-oa/webhook"
    # Webhook verification (either verify token or signature secret depending on OA config)
    ZALO_OA_WEBHOOK_VERIFY_TOKEN: str | None = None
    ZALO_OA_WEBHOOK_SECRET: str | None = None

    @property
    def zalo_oa_callback_url(self) -> str:
        """Derived absolute callback URL for OA OAuth flow."""
        return f"{self.SERVER_HOST}{self.ZALO_OA_CALLBACK_PATH}"

    class Config:
        env_file = BASE_DIR / ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = True
    API_DATA: str = "https://hoangmaimobile.vn/api/product/getlist"
settings = Settings()
import os
from google_auth_oauthlib.flow import Flow
from app.configs.settings import settings

# Sử dụng cấu hình từ settings thay vì biến môi trường
GOOGLE_CLIENT_ID = settings.GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET = settings.GOOGLE_CLIENT_SECRET
# Cần phải thêm URI này vào trong Google Cloud Console
GOOGLE_REDIRECT_URI = f"{settings.SERVER_HOST}/api/v1/auth/google/callback"

# Scopes cho các mục đích khác nhau
LOGIN_SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid"
]

# Scopes kết hợp (ví dụ)
ALL_SCOPES = LOGIN_SCOPES + [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly"
]

# Scopes cần thiết để lấy thông tin hồ sơ người dùng
SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid"
]

# Tạo một client_config dictionary
client_config = {
    "web": {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": [GOOGLE_REDIRECT_URI]
    }
}

def get_google_auth_flow():
    """Tạo và trả về một flow object của Google OAuth2."""
    return Flow.from_client_config(
        client_config=client_config,
        scopes=LOGIN_SCOPES,
        redirect_uri=GOOGLE_REDIRECT_URI
    )

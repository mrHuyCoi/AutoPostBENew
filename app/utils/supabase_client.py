from supabase import create_client
from app.configs.settings import settings

# Kiểm tra biến môi trường bắt buộc
if not settings.SUPABASE_URL or not settings.SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not configured in environment variables")

# Khởi tạo Supabase client với SERVICE ROLE KEY (có quyền ghi vượt RLS)
supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

def get_client():
    """Return global Supabase client with service role access"""
    return supabase

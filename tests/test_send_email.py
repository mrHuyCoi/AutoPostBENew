import sys
import asyncio
import traceback
from pathlib import Path
from pydantic import EmailStr

# Thêm project root vào sys.path để import package 'app' khi chạy script trực tiếp
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.email_service import EmailService

async def main():
    try:
        # Thay email bằng email bạn muốn nhận thử
        # EmailStr là kiểu, không gọi được như hàm — truyền chuỗi trực tiếp
        to_email = "dangminhn10@gmail.com"
        await EmailService.send_verification_code(to_email, "123456")
        print("Gửi email thành công.")
    except Exception as e:
        print("Gửi email thất bại:")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
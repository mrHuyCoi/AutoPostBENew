from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from pydantic import EmailStr
from typing import List

conf = ConnectionConfig(
    MAIL_USERNAME="doiquanai@gmail.com",
    MAIL_PASSWORD="xomqucpdpmscbezm",  # App Password Gmail 16 ký tự, không khoảng trắng
    MAIL_FROM="doiquanai@gmail.com",
    MAIL_PORT=587,
    MAIL_SERVER="smtp.gmail.com",
    MAIL_FROM_NAME="Đăng Bài Tự Động",
    MAIL_STARTTLS=True,    # STARTTLS cho port 587
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)

class EmailService:
    @staticmethod
    async def send_email(subject: str, recipients: List[EmailStr], body: str):
        message = MessageSchema(
            subject=subject,
            recipients=recipients,
            body=body,
            subtype="html"
        )
        fm = FastMail(conf)
        await fm.send_message(message)

    @staticmethod
    async def send_verification_code(email: EmailStr, code: str):
        subject = "Mã xác thực đăng ký tài khoản"
        body = (
            f'<p>Xin chào,</p>'
            f'<p>Cảm ơn bạn đã đăng ký tài khoản. '
            f'Mã xác thực của bạn là: <strong>{code}</strong></p>'
            f'<p>Mã này sẽ hết hạn sau 10 phút.</p>'
            f'<p>Trân trọng,<br>Đội ngũ Đăng Bài Tự Động</p>'
        )
        await EmailService.send_email(subject, [email], body)

    @staticmethod
    async def send_password_reset_email(email: EmailStr, code: str):
        subject = "Yêu cầu đặt lại mật khẩu"
        body = (
            f'<p>Xin chào,</p>'
            f'<p>Chúng tôi đã nhận được yêu cầu đặt lại mật khẩu cho tài khoản của bạn. '
            f'Mã khôi phục của bạn là: <strong>{code}</strong></p>'
            f'<p>Nếu bạn không yêu cầu điều này, vui lòng bỏ qua email này.</p>'
            f'<p>Mã này sẽ hết hạn sau 10 phút.</p>'
            f'<p>Trân trọng,<br>Đội ngũ Đăng Bài Tự Động</p>'
        )
        await EmailService.send_email(subject, [email], body)

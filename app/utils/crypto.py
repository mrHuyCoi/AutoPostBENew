from cryptography.fernet import Fernet
import os
import base64
from app.configs.settings import settings


class TokenEncryption:
    """
    Lớp tiện ích để mã hóa và giải mã token với Fernet.
    """

    def __init__(self):
        """
        Khởi tạo đối tượng mã hóa sử dụng SECRET_KEY từ biến môi trường.
        """
        # Sử dụng SECRET_KEY từ cấu hình ứng dụng
        secret_key = settings.SECRET_KEY
        
        # Tạo khóa mã hóa từ SECRET_KEY (phải là 32 bytes)
        # Nếu không đủ độ dài, sẽ thêm padding và hash
        key = base64.urlsafe_b64encode(secret_key.encode()[:32].ljust(32, b'\0'))
        self.cipher = Fernet(key)

    def encrypt(self, token: str) -> str:
        """
        Mã hóa token.
        
        Args:
            token: Token cần mã hóa
            
        Returns:
            Token đã được mã hóa
        """
        if not token:
            return ""
        
        encrypted_token = self.cipher.encrypt(token.encode())
        return encrypted_token.decode()

    def decrypt(self, encrypted_token: str) -> str:
        """
        Giải mã token.
        
        Args:
            encrypted_token: Token đã mã hóa
            
        Returns:
            Token gốc đã giải mã
        """
        if not encrypted_token:
            return ""
        
        decrypted_token = self.cipher.decrypt(encrypted_token.encode())
        return decrypted_token.decode()


# Tạo singleton instance để sử dụng trong toàn ứng dụng
token_encryption = TokenEncryption()

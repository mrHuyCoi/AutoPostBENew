import bcrypt
import hashlib

def hash_password(password: str) -> str:
    """
    Hash mật khẩu với bcrypt, xử lý password dài >72 bytes.
    """
    if len(password.encode('utf-8')) > 72:
        password = hashlib.sha256(password.encode('utf-8')).hexdigest()
    
    hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Kiểm tra password plain text có khớp với hash không.
    """
    if len(plain_password.encode('utf-8')) > 72:
        plain_password = hashlib.sha256(plain_password.encode('utf-8')).hexdigest()
    
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


def validate_password(password: str) -> bool:
    """
    Validate password ≥8 ký tự.
    """
    return bool(password) and len(password) >= 8


# Ví dụ sử dụng
if __name__ == "__main__":
    pw = "12345678"
    if validate_password(pw):
        hashed = hash_password(pw)
        print("Hashed password:", hashed)
        print("Verify result:", verify_password(pw, hashed))
    else:
        print("Password không hợp lệ (phải ≥8 ký tự)")

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, EmailStr

from app.database.database import get_db
from app.services.user_service import UserService
from app.dto.user_dto import UserCreate, UserRead
from app.exceptions.api_exceptions import BadRequestException, NotFoundException

router = APIRouter()

class VerificationRequest(BaseModel):
    email: EmailStr

class RegisterRequest(UserCreate):
    verification_code: str

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ResetPasswordRequest(BaseModel):
    email: EmailStr
    code: str
    new_password: str

@router.post("/send-verification-code", status_code=status.HTTP_204_NO_CONTENT)
async def send_verification_code_endpoint(request: VerificationRequest, db: AsyncSession = Depends(get_db)):
    try:
        await UserService.send_registration_code(db, request.email)
    except BadRequestException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        # Log the exception here for debugging
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Không thể gửi mã xác thực.")

@router.post("/register", response_model=UserRead, status_code=status.HTTP_201_CREATED)
async def register_user_endpoint(request: RegisterRequest, db: AsyncSession = Depends(get_db)):
    try:
        user_data = UserCreate(
            email=request.email,
            password=request.password,
            full_name=request.full_name,
            subscription_id=request.subscription_id
        )
        new_user = await UserService.register_user(db, user_data, request.verification_code)
        return new_user
    except BadRequestException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        # Log the exception here for debugging
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Đăng ký không thành công.")

@router.post("/register-direct", response_model=UserRead, status_code=status.HTTP_201_CREATED)
async def register_user_direct_endpoint(request: UserCreate, db: AsyncSession = Depends(get_db)):
    try:
        new_user = await UserService.register_user_direct(db, request)
        return new_user
    except BadRequestException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        # Log the exception here for debugging
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Đăng ký không thành công.")

@router.post("/forgot-password", status_code=status.HTTP_204_NO_CONTENT)
async def forgot_password_endpoint(request: ForgotPasswordRequest, db: AsyncSession = Depends(get_db)):
    try:
        await UserService.send_password_reset_code(db, request.email)
    except NotFoundException as e:
        # We don't want to reveal if an email exists or not for security reasons
        # So we return a generic success response even if the user is not found
        pass
    except Exception as e:
        # Log the exception for debugging
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not send password reset code.")

@router.post("/reset-password", status_code=status.HTTP_204_NO_CONTENT)
async def reset_password_endpoint(request: ResetPasswordRequest, db: AsyncSession = Depends(get_db)):
    try:
        await UserService.reset_password(db, request.email, request.code, request.new_password)
    except BadRequestException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        # Log the exception for debugging
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not reset password.")

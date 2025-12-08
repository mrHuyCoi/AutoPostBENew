from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.database.database import get_db
from app.services.google_auth_service import get_google_auth_flow
from app.models.user import User
from app.models.subscription import Subscription
from app.models.user_subscription import UserSubscription
from app.middlewares.auth_middleware import create_access_token
from app.configs.settings import settings
from datetime import datetime, timedelta, timezone


router = APIRouter()

@router.get("/google/login")
def login_google():
    flow = get_google_auth_flow()
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        prompt='consent'
    )
    return {"authorization_url": authorization_url}

@router.get("/google/callback")
async def auth_google_callback(request: Request, db: AsyncSession = Depends(get_db)):
    flow = get_google_auth_flow()
    try:
        flow.fetch_token(authorization_response=str(request.url))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to fetch token: {e}"
        )

    credentials = flow.credentials
    
    # Lấy thông tin người dùng từ Google
    async with httpx.AsyncClient() as client:
        user_info_res = await client.get(
            'https://www.googleapis.com/oauth2/v3/userinfo',
            headers={'Authorization': f'Bearer {credentials.token}'}
        )

    if user_info_res.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not fetch user info"
        )

    user_info = user_info_res.json()
    email = user_info.get("email")
    google_id = user_info.get("sub")
    full_name = user_info.get("name")

    if not email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email not found in Google profile"
        )

    # Kiểm tra xem người dùng đã tồn tại trong DB chưa
    result = await db.execute(select(User).filter(User.email == email))
    user = result.scalars().first()

    if not user:
        # Nếu chưa có, tạo người dùng mới
        user = User(
            email=email,
            google_id=google_id,
            full_name=full_name,
            is_active=True
        )
        db.add(user)
        await db.flush()  # Sử dụng flush để lấy user.id trước khi commit

        # Tìm gói "Miễn phí"
        free_plan_result = await db.execute(
            select(Subscription).filter(Subscription.name.ilike('%miễn phí%'))
        )
        free_plan = free_plan_result.scalars().first()

        if free_plan:
            # Gán gói "Miễn phí" cho người dùng mới
            start_date = datetime.now(timezone.utc)
            end_date = start_date + timedelta(days=free_plan.duration_days)
            
            new_subscription = UserSubscription(
                user_id=user.id,
                subscription_id=free_plan.id,
                start_date=start_date,
                end_date=end_date,
                is_active=True  # Kích hoạt ngay lập tức
            )
            db.add(new_subscription)

        await db.commit()
        await db.refresh(user)
    elif not user.google_id:
        # Nếu đã có nhưng chưa liên kết Google, cập nhật google_id
        user.google_id = google_id
        await db.commit()
        await db.refresh(user)

    # Tạo access token cho người dùng
    access_token = await create_access_token(data={"sub": str(user.id)})
    
    # Chuyển hướng người dùng về trang frontend với token
    # Frontend sẽ nhận token từ URL và lưu vào local storage
    redirect_url = f"{settings.CLIENT_ORIGIN}/login/callback?token={access_token}"
    return Response(status_code=302, headers={"Location": redirect_url})

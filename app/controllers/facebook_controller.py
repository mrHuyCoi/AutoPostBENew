from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
from uuid import UUID
import httpx
import traceback
import base64
import json
import secrets

from app.database.database import get_db
from app.dto.facebook_dto import FacebookTokenRequest, FacebookResponse
from app.services.facebook_acc_service import FacebookService
from app.middlewares.subscription_middleware import check_active_subscription
from app.models.social_account import SocialAccount
from app.utils.crypto import token_encryption
from app.tasks.messenger_tasks import send_facebook_comment_reply
from app.logging.decorators import with_log_context

from app.configs.settings import settings

router = APIRouter()

# ==========================
#  🔧 Hằng số cấu hình OAuth
# ==========================
FACEBOOK_APP_ID = settings.FACEBOOK_APP_ID
FACEBOOK_APP_SECRET = settings.FACEBOOK_APP_SECRET
REDIRECT_URI = "https://test.doiquanai.vn/api/v1/facebook/auth/facebook/callback"

FACEBOOK_SCOPE = ",".join([
    "read_insights", "pages_show_list", "business_management",
    "pages_read_engagement", "pages_manage_metadata", "pages_read_user_content",
    "pages_manage_posts", "pages_manage_engagement", "instagram_basic",
    "instagram_content_publish", "instagram_manage_insights","pages_messaging"
])


# ==============================
# 1. Khởi tạo liên kết Facebook
# ==============================
@router.get("/auth/facebook/init")
@with_log_context(
    event_action="facebook.auth.init",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def initiate_facebook_auth(current_user=Depends(check_active_subscription(required_max_social_accounts=1))):
    """
    Trả về URL để frontend redirect đến Facebook OAuth. Nhúng user_id vào state (base64).
    """
    state_data = {
        "nonce": secrets.token_urlsafe(8),
        "user_id": str(current_user.id)
    }
    state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()

    auth_url = (
        f"https://www.facebook.com/v23.0/dialog/oauth?"
        f"client_id={FACEBOOK_APP_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={FACEBOOK_SCOPE}"
        f"&state={state}"
    )

    return {"auth_url": auth_url, "state": state}
# ==========================
# 2. Xử lý Facebook callback
# ==========================
@router.get("/auth/facebook/callback")
@with_log_context(
    event_action="facebook.auth.callback",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def facebook_callback(
    code: str = Query(...),
    state: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Callback từ Facebook sau khi người dùng xác nhận quyền.
    - Đổi code => access_token
    - Giải mã state => user_id
    - Gọi service lưu tất cả tài khoản liên kết
    """
    try:
        # 🔓 Giải mã state lấy user_id
        state_json = base64.urlsafe_b64decode(state.encode()).decode()
        state_data = json.loads(state_json)
        user_id = UUID(state_data["user_id"])

        # 🔑 Lấy access_token
        token_url = "https://graph.facebook.com/v23.0/oauth/access_token"
        params = {
            "client_id": FACEBOOK_APP_ID,
            "redirect_uri": REDIRECT_URI,
            "client_secret": FACEBOOK_APP_SECRET,
            "code": code
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(token_url, params=params)
            response.raise_for_status()
            token_data = response.json()
            user_access_token = token_data.get("access_token")

        if not user_access_token:
            raise HTTPException(400, "Không lấy được access token từ Facebook.")

        # 💾 Lưu toàn bộ tài khoản liên kết
        token_request = FacebookTokenRequest(
            user_access_token=user_access_token,
            platform="facebook"
        )
        saved_accounts = await FacebookService.add_all_facebook_pages(
            db=db, user_id=user_id, data=token_request
        )
        return [FacebookResponse.model_validate(acc) for acc in saved_accounts]

    except httpx.HTTPStatusError as e:
        error_msg = e.response.json().get("error", {}).get("message", e.response.text)
        raise HTTPException(e.response.status_code, detail=error_msg)

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Lỗi xử lý callback: {str(e)}")


# ============================
# 3. Quản lý tài khoản mạng xã hội
# ============================
@router.get("/accounts", response_model=List[FacebookResponse])
@with_log_context(
    event_action="facebook.accounts.list_all",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def get_social_accounts(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1))
):
    accounts = await FacebookService.get_user_social_accounts(db, current_user.id)
    return [FacebookResponse(**acc) for acc in accounts]


@router.get("/accounts/facebook", response_model=List[FacebookResponse])
@with_log_context(
    event_action="facebook.accounts.list_facebook",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def get_facebook_accounts(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1))
):
    accounts = await FacebookService.get_user_social_accounts(db, current_user.id)
    facebook_accounts = [acc for acc in accounts if acc["platform"] == "facebook"]
    return [FacebookResponse(**acc) for acc in facebook_accounts]


@router.get("/accounts/instagram", response_model=List[FacebookResponse])
@with_log_context(
    event_action="facebook.accounts.list_instagram",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def get_instagram_accounts(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1))
):
    accounts = await FacebookService.get_user_social_accounts(db, current_user.id)
    instagram_accounts = [acc for acc in accounts if acc["platform"] == "instagram"]
    return [FacebookResponse(**acc) for acc in instagram_accounts]

@router.get("/accounts/youtube", response_model=List[FacebookResponse])
@with_log_context(
    event_action="facebook.accounts.list_youtube",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def get_instagram_accounts(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1))
):
    accounts = await FacebookService.get_user_social_accounts(db, current_user.id)
    instagram_accounts = [acc for acc in accounts if acc["platform"] == "youtube"]
    return [FacebookResponse(**acc) for acc in instagram_accounts]


@router.get("/conversations")
@with_log_context(
    event_action="facebook.conversations.list",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def list_page_conversations(
    page_id: str = Query(..., description="Facebook Page ID được liên kết (dùng PAT của Page này)"),
    platform: str = Query("messenger", pattern="^(messenger|instagram)$"),
    limit: int = Query(25, ge=1, le=100),
    before: str | None = Query(None),
    after: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1)),
):
    # Tìm Page thuộc user và giải mã Page Access Token
    stmt = select(SocialAccount).where(
        SocialAccount.user_id == current_user.id,
        SocialAccount.platform == "facebook",
        SocialAccount.account_id == page_id,
        SocialAccount.is_active == True,
    )
    res = await db.execute(stmt)
    acc: SocialAccount | None = res.scalars().first()
    if not acc:
        raise HTTPException(404, detail="Không tìm thấy Page hoặc Page không hoạt động")

    try:
        pat = token_encryption.decrypt(acc.access_token)
    except Exception:
        raise HTTPException(400, detail="Token Page không hợp lệ")

    url = f"{settings.FACEBOOK_API_BASE_URL}/{page_id}/conversations"
    # Request key fields so frontend can render like Messenger
    params: dict = {
        "access_token": pat,
        "platform": platform,
        "limit": limit,
        # Include participants, updated_time, snippet (last message preview), and unread_count
        "fields": "participants.limit(10){id,name},updated_time,snippet,unread_count"
    }
    if before:
        params["before"] = before
    if after:
        params["after"] = after

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url, params=params)
        # Trả lỗi rõ ràng từ Graph API
        if r.status_code != 200:
            try:
                err = r.json()
            except Exception:
                err = {"error": {"message": r.text}}
            raise HTTPException(r.status_code, detail=err)
        return r.json()


@router.post("/comments/reply")
@with_log_context(
    event_action="facebook.comments.reply",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def reply_to_facebook_comment(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1)),
):
    page_id = (payload or {}).get("page_id")
    comment_id = (payload or {}).get("comment_id")
    comment_text = (
        (payload or {}).get("comment_text")
        or (payload or {}).get("comment_message")
        or (payload or {}).get("message")
        or (payload or {}).get("text")
    )
    dedupe_id = (payload or {}).get("dedupe_id") or comment_id
    post_id = (payload or {}).get("post_id")
    author_name = (payload or {}).get("author_name")

    if not page_id or not comment_id or not comment_text:
        raise HTTPException(
            status_code=400,
            detail="page_id, comment_id và message là bắt buộc",
        )

    stmt = select(SocialAccount).where(
        SocialAccount.user_id == current_user.id,
        SocialAccount.platform == "facebook",
        SocialAccount.account_id == page_id,
        SocialAccount.is_active == True,
    )
    res = await db.execute(stmt)
    acc: SocialAccount | None = res.scalars().first()
    if not acc:
        raise HTTPException(status_code=404, detail="Không tìm thấy Page hoặc Page không hoạt động")

    try:
        send_facebook_comment_reply.delay(
            str(current_user.id),
            str(page_id),
            str(comment_id),
            str(comment_text),
            str(dedupe_id) if dedupe_id else None,
            str(author_name) if author_name else None,
            str(post_id) if post_id else None,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Không thể xếp hàng gửi reply: {e}")

    return {"status": "queued"}


@router.get("/conversations/{conversation_id}/messages")
@with_log_context(
    event_action="facebook.conversations.messages.list",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def get_conversation_messages(
    conversation_id: str,
    page_id: str = Query(..., description="Facebook Page ID để lấy PAT"),
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1)),
):
    # Tìm Page thuộc user và giải mã PAT
    stmt = select(SocialAccount).where(
        SocialAccount.user_id == current_user.id,
        SocialAccount.platform == "facebook",
        SocialAccount.account_id == page_id,
        SocialAccount.is_active == True,
    )
    res = await db.execute(stmt)
    acc: SocialAccount | None = res.scalars().first()
    if not acc:
        raise HTTPException(404, detail="Không tìm thấy Page hoặc Page không hoạt động")

    try:
        pat = token_encryption.decrypt(acc.access_token)
    except Exception:
        raise HTTPException(400, detail="Token Page không hợp lệ")

    # Expand messages với các trường cần thiết, giới hạn 20 theo khuyến nghị
    url = f"{settings.FACEBOOK_API_BASE_URL}/{conversation_id}"
    fields = f"messages.limit({limit}){{id,created_time,from,to,message,attachments{{id,image_data,mime_type,name,size}}}}"
    params = {"access_token": pat, "fields": fields}

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            try:
                err = r.json()
            except Exception:
                err = {"error": {"message": r.text}}
            raise HTTPException(r.status_code, detail=err)
        return r.json()


@router.get("/{account_id}", response_model=FacebookResponse)
@with_log_context(
    event_action="facebook.accounts.get",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def get_social_account(
    account_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1))
):
    account_data = await FacebookService.get_social_account(db, account_id, current_user.id)
    if not account_data:
        raise HTTPException(404, "Không tìm thấy tài khoản mạng xã hội")
    
    # Trả về FacebookResponse với dữ liệu từ service
    return FacebookResponse(
        id=account_data["id"],
        platform=account_data["platform"],
        account_name=account_data["account_name"],
        account_id=account_data["account_id"],
        is_active=account_data["is_active"],
        created_at=account_data["created_at"],
        thumbnail=account_data["thumbnail"]
    )


@router.delete("/{account_id}")
@with_log_context(
    event_action="facebook.accounts.delete",
    event_category="facebook",
    source_layer="controller",
    source_controller="facebook_controller",
)
async def delete_social_account(
    account_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1))
):
    deleted = await FacebookService.delete_social_account(db, account_id, current_user.id)
    if not deleted:
        raise HTTPException(404, "Không tìm thấy tài khoản để xóa")
    return {"message": "Xóa tài khoản thành công"}

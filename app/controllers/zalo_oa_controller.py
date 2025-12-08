from __future__ import annotations
import uuid
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Query, status, UploadFile, File, Form
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from sqlalchemy.exc import IntegrityError

from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.models.oauth_state import OauthState
from app.models.oa_account import OaAccount
from app.models.oa_token import OaToken
from app.models.oa_conversation import OaConversation
from app.models.oa_message import OaMessage
from app.models.oa_blocked_user import OaBlockedUser
from app.dto.response import ResponseModel
from app.configs.settings import settings
from app.utils.pkce import generate_code_verifier, code_challenge_s256
from app.services.zalo_oa_service import ZaloOAService
from app.logging.decorators import with_log_context

router = APIRouter()


async def _ensure_access_token_valid(db: AsyncSession, account_id):
    res = await db.execute(select(OaToken).where(OaToken.oa_account_id == account_id).order_by(desc(OaToken.created_at)))
    token = res.scalars().first()
    if not token:
        raise HTTPException(status_code=400, detail="Chưa có access token")
    now = datetime.utcnow()
    if token.expires_at and token.expires_at <= now:
        if not token.refresh_token:
            raise HTTPException(status_code=400, detail="Không có refresh_token để làm mới")
        if token.refresh_token_expires_at and token.refresh_token_expires_at <= now:
            raise HTTPException(status_code=400, detail="Refresh token đã hết hạn. Vui lòng kết nối lại OA")
        try:
            data = await ZaloOAService.refresh_token(token.refresh_token)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Lỗi refresh token: {e}")
        access_token = data.get("access_token")
        if not access_token:
            raise HTTPException(status_code=502, detail="Thiếu access_token sau khi refresh")
        expires_in = data.get("expires_in")
        rt_expires_in = data.get("refresh_token_expires_in")
        expires_at = datetime.utcnow() + timedelta(seconds=int(expires_in or 0)) if expires_in else None
        rt_expires_at = datetime.utcnow() + timedelta(seconds=int(rt_expires_in or 0)) if rt_expires_in else None
        new_token = OaToken(
            oa_account_id=account_id,
            access_token=access_token,
            refresh_token=data.get("refresh_token") or token.refresh_token,
            expires_at=expires_at,
            refresh_token_expires_at=rt_expires_at,
        )
        db.add(new_token)
        return access_token
    return token.access_token


@router.get("/auth/login")
async def zalo_oa_login(
    request: Request,
    return_url: bool = Query(False, description="Nếu true, trả về JSON auth_url thay vì redirect"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not settings.ZALO_OA_APP_ID or not settings.ZALO_OA_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Chưa cấu hình ZALO_OA_APP_ID/ZALO_OA_SECRET_KEY")

    code_verifier = generate_code_verifier()
    code_challenge = code_challenge_s256(code_verifier)
    state = str(uuid.uuid4())

    oauth_state = OauthState(
        state=state,
        code_verifier=code_verifier,
        user_id=current_user.id,
        redirect_uri=settings.zalo_oa_callback_url,
        expires_at=(datetime.utcnow() + timedelta(minutes=10)),
    )
    db.add(oauth_state)

    auth_url = ZaloOAService.build_permission_url(
        app_id=str(settings.ZALO_OA_APP_ID),
        redirect_uri=settings.zalo_oa_callback_url,
        code_challenge=code_challenge,
        state=state,
    )

    if return_url:
        return {"ok": True, "auth_url": auth_url, "state": state}
    return RedirectResponse(url=auth_url, status_code=307)


@router.get("/auth/callback")
async def zalo_oa_callback(
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    if not code or not state:
        raise HTTPException(status_code=400, detail="Thiếu code hoặc state")

    # Find oauth state
    res = await db.execute(select(OauthState).where(OauthState.state == state))
    oauth_state: Optional[OauthState] = res.scalars().first()
    if not oauth_state:
        raise HTTPException(status_code=400, detail="State không hợp lệ")

    # Basic expiry/use checks
    from datetime import datetime as _dt
    if oauth_state.expires_at and oauth_state.expires_at < _dt.utcnow():
        raise HTTPException(status_code=400, detail="State đã hết hạn")

    if oauth_state.used_at:
        raise HTTPException(status_code=400, detail="State đã được sử dụng")

    # Exchange token
    try:
        token_payload = await ZaloOAService.exchange_token(code=code, code_verifier=oauth_state.code_verifier)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi exchange token: {e}")

    access_token = token_payload.get("access_token")
    refresh_token = token_payload.get("refresh_token")
    expires_in = token_payload.get("expires_in")
    rt_expires_in = token_payload.get("refresh_token_expires_in")

    if not access_token:
        raise HTTPException(status_code=502, detail="Thiếu access_token trong phản hồi")

    # Fetch OA info via OpenAPI /v2.0/oa/getoa
    try:
        info = await ZaloOAService.get_oa_info(access_token)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi gọi getoa: {e}")

    data = info.get("data") if isinstance(info, dict) else {}
    oa_id = str((data or {}).get("oaid") or (data or {}).get("oa_id") or "")
    oa_name = (data or {}).get("name")
    picture_url = (data or {}).get("avatar")

    # Upsert OaAccount
    res = await db.execute(select(OaAccount).where(
        (OaAccount.owner_user_id == oauth_state.user_id) & (OaAccount.oa_id == oa_id)
    ))
    account: Optional[OaAccount] = res.scalars().first()
    if not account:
        account = OaAccount(
            owner_user_id=oauth_state.user_id,
            oa_id=oa_id,
            oa_name=oa_name,
            picture_url=picture_url,
            app_id=str(settings.ZALO_OA_APP_ID),
            status="connected",
        )
        db.add(account)
        await db.flush()
    else:
        account.oa_name = oa_name or account.oa_name
        account.picture_url = picture_url or account.picture_url

    # Insert token row
    from datetime import datetime, timedelta
    expires_at = None
    if expires_in:
        try:
            expires_at = datetime.utcnow() + timedelta(seconds=int(expires_in))
        except Exception:
            pass
    rt_expires_at = None
    if rt_expires_in:
        try:
            rt_expires_at = datetime.utcnow() + timedelta(seconds=int(rt_expires_in))
        except Exception:
            pass

    token = OaToken(
        oa_account_id=account.id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
        refresh_token_expires_at=rt_expires_at,
    )
    db.add(token)

    oauth_state.used_at = _dt.utcnow()

    return ResponseModel.success(data={
        "oa_id": oa_id,
        "name": oa_name,
        "picture": picture_url,
    }, message="Kết nối Zalo OA thành công")


@router.get("/accounts")
async def list_oa_accounts(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    res = await db.execute(
        select(OaAccount).where(OaAccount.owner_user_id == current_user.id).order_by(desc(OaAccount.connected_at))
    )
    items = res.scalars().all()
    return ResponseModel.success(
        data=[{
            "id": str(a.id),
            "oa_id": a.oa_id,
            "name": a.oa_name,
            "picture_url": a.picture_url,
            "status": a.status,
            "connected_at": a.connected_at.isoformat() if a.connected_at else None,
        } for a in items],
        total=len(items),
        message="Danh sách OA đã kết nối"
    )


@router.delete("/accounts/{account_id}")
async def delete_oa_account(
    account_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Validate UUID and ownership
    try:
        acc_uuid = uuid.UUID(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="account_id không hợp lệ")

    res = await db.execute(
        select(OaAccount).where((OaAccount.id == acc_uuid) & (OaAccount.owner_user_id == current_user.id))
    )
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    # Delete account; related rows cascade via FK ondelete
    await db.delete(account)

    return ResponseModel.success(data={"id": account_id}, message="Đã xóa OA account")


@router.post("/token/refresh")
async def refresh_oa_token(
    account_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # find account belongs to current user
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    # get latest token
    from sqlalchemy import desc
    res = await db.execute(select(OaToken).where(OaToken.oa_account_id == account.id).order_by(desc(OaToken.created_at)))
    last_token: Optional[OaToken] = res.scalars().first()
    if not last_token or not last_token.refresh_token:
        raise HTTPException(status_code=400, detail="Không có refresh_token để làm mới")

    try:
        data = await ZaloOAService.refresh_token(last_token.refresh_token)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi refresh token: {e}")

    access_token = data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=502, detail="Thiếu access_token sau khi refresh")

    from datetime import datetime, timedelta
    expires_in = data.get("expires_in")
    rt_expires_in = data.get("refresh_token_expires_in")
    expires_at = datetime.utcnow() + timedelta(seconds=int(expires_in or 0)) if expires_in else None
    rt_expires_at = datetime.utcnow() + timedelta(seconds=int(rt_expires_in or 0)) if rt_expires_in else None

    new_token = OaToken(
        oa_account_id=account.id,
        access_token=access_token,
        refresh_token=data.get("refresh_token") or last_token.refresh_token,
        expires_at=expires_at,
        refresh_token_expires_at=rt_expires_at,
    )
    db.add(new_token)

    return ResponseModel.success(data={"account_id": str(account.id)}, message="Đã làm mới token")


@router.get("/me")
async def get_oa_me(
    account_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Simple helper to call /me using latest access_token
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    access_token = await _ensure_access_token_valid(db, account.id)
    data = await ZaloOAService.get_oa_info(access_token)
    return data


@router.get("/openapi/listrecentchat")
async def oa_list_recent_chat(
    account_id: str,
    offset: int = Query(0, ge=0),
    count: int = Query(5, ge=1, le=10),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Gọi OpenAPI Zalo OA để lấy danh sách tin nhắn gần nhất (tối đa 10) giữa OA và người dùng.
    Yêu cầu quyền: Quyền quản lý tin nhắn người dùng
    """
    # Ensure ownership
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    try:
        access_token = await _ensure_access_token_valid(db, account.id)
        resp = await ZaloOAService.list_recent_chat(access_token, offset=offset, count=count)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi gọi listrecentchat: {e}")

    return ResponseModel.success(data=resp, message="Lấy danh sách tin nhắn gần nhất thành công")


@router.get("/openapi/conversation")
async def oa_get_conversation(
    account_id: str,
    user_id: str,
    offset: int = Query(0, ge=0),
    count: int = Query(5, ge=1, le=10),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Gọi OpenAPI Zalo OA để lấy tin nhắn trong một hội thoại với user cụ thể (tối đa 10 tin/req).
    Yêu cầu quyền: Quyền quản lý tin nhắn người dùng
    """
    # Ensure ownership
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    try:
        access_token = await _ensure_access_token_valid(db, account.id)
        resp = await ZaloOAService.get_conversation(access_token, user_id=user_id, offset=offset, count=count)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi gọi conversation: {e}")

    return ResponseModel.success(data=resp, message="Lấy tin nhắn hội thoại thành công")


@router.get("/conversations")
async def list_conversations(
    account_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Ensure ownership
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    q = select(OaConversation).where(OaConversation.oa_account_id == account.id)
    if search:
        like = f"%{search}%"
        q = q.where(OaConversation.display_name.ilike(like))
    # Count total with separate query
    count_q = select(func.count()).select_from(OaConversation).where(OaConversation.oa_account_id == account.id)
    if search:
        like = f"%{search}%"
        count_q = count_q.where(OaConversation.display_name.ilike(like))
    total_res = await db.execute(count_q)
    total = total_res.scalar_one()

    q = q.order_by(desc(OaConversation.last_message_at)).offset(offset).limit(limit)
    res = await db.execute(q)
    items = res.scalars().all()

    return ResponseModel.success(
        data=[{
            "id": str(it.id),
            "conversation_id": it.conversation_id,
            "display_name": it.display_name,
            "type": it.type,
            "last_message_at": it.last_message_at.isoformat() if it.last_message_at else None,
            "is_ignored": it.is_ignored,
            "is_blocked": it.is_blocked,
        } for it in items],
        message="Lấy danh sách hội thoại thành công",
        total=total,
        pagination={"limit": limit, "offset": offset}
    )


@router.get("/messages")
async def list_messages(
    account_id: str,
    conversation_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Ensure ownership
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    q = select(OaMessage).where(
        (OaMessage.oa_account_id == account.id) & (OaMessage.conversation_id == conversation_id)
    )
    q = q.order_by(OaMessage.timestamp.asc() if order == "asc" else OaMessage.timestamp.desc())
    count_q = select(func.count()).select_from(OaMessage).where(
        (OaMessage.oa_account_id == account.id) & (OaMessage.conversation_id == conversation_id)
    )
    total_res = await db.execute(count_q)
    total = total_res.scalar_one()
    q = q.offset(offset).limit(limit)
    res = await db.execute(q)
    items = res.scalars().all()
    return ResponseModel.success(
        data=[{
            "id": str(m.id),
            "direction": m.direction,
            "msg_type": m.msg_type,
            "text": m.text,
            "attachments": m.attachments,
            "timestamp": m.timestamp.isoformat() if m.timestamp else None,
            "message_id_from_zalo": m.message_id_from_zalo,
            "delivery_status": m.delivery_status,
        } for m in items],
        message="Lấy tin nhắn thành công",
        total=total,
        pagination={"limit": limit, "offset": offset}
    )


@router.post("/messages/send")
async def send_message(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    account_id = payload.get("account_id")
    # Support both our simple shape and Zalo doc shape
    recipient = (payload.get("recipient") or {}) if isinstance(payload.get("recipient"), dict) else {}
    message_obj = (payload.get("message") or {}) if isinstance(payload.get("message"), dict) else {}
    # Derive user_id and text from multiple possible fields
    to_user_id = payload.get("to_user_id") or recipient.get("user_id") or payload.get("user_id")
    text = payload.get("text")
    if not text:
        if isinstance(payload.get("message"), dict):
            text = message_obj.get("text")
        elif isinstance(payload.get("message"), str):
            text = payload.get("message")
    if text:
        text = str(text).strip()
    # Basic validations per docs
    if not account_id or not to_user_id or not text:
        raise HTTPException(status_code=400, detail="Thiếu account_id, user_id hoặc text (theo cấu trúc recipient.user_id và message.text)")
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="Độ dài text vượt quá 2000 ký tự theo quy định của Zalo OA")

    # Ensure ownership
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    try:
        access_token = await _ensure_access_token_valid(db, account.id)
        result = await ZaloOAService.send_text_message(access_token, to_user_id, text)
    except NotImplementedError:
        raise HTTPException(status_code=501, detail="Gửi tin nhắn OA chưa được cấu hình. Vui lòng cấu hình endpoint trong ZaloOAService.send_text_message")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi gửi tin nhắn: {e}")

    # Zalo API sometimes returns 200 with error code in body
    try:
        if isinstance(result, dict) and int(result.get("error", 0)) != 0:
            raise HTTPException(status_code=502, detail=f"Zalo API error {result.get('error')}: {result.get('message')}")
    except Exception:
        # If structure unexpected, continue returning raw result
        pass

    # Optionally upsert message to DB if API returns an id
    # TODO: map response to message_id_from_zalo and conversation_id
    return ResponseModel.success(data=result, message="Đã gửi tin nhắn")


@router.post("/messages/upload-image")
async def upload_image(
    account_id: str = Form(...),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Validate ownership
    try:
        acc_uuid = uuid.UUID(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="account_id không hợp lệ")

    res = await db.execute(select(OaAccount).where((OaAccount.id == acc_uuid) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    # Validate file type and size per Zalo OA constraints
    if file.content_type not in {"image/jpeg", "image/png"}:
        raise HTTPException(status_code=400, detail="Chỉ hỗ trợ JPG/PNG")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="File rỗng")
    if len(content) > 1_000_000:
        raise HTTPException(status_code=400, detail="Kích thước ảnh vượt quá 1MB")

    try:
        access_token = await _ensure_access_token_valid(db, account.id)
        result = await ZaloOAService.upload_image(access_token, content, file.filename or "image", file.content_type)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi upload ảnh: {e}")

    # Extract attachment_id from API response
    attach_id = None
    try:
        if isinstance(result, dict):
            data = result.get("data") or {}
            attach_id = data.get("attachment_id") or result.get("attachment_id")
    except Exception:
        pass
    if not attach_id:
        try:
            if isinstance(result, dict) and int(result.get("error", 0)) != 0:
                raise HTTPException(status_code=502, detail=f"Zalo API error {result.get('error')}: {result.get('message')}")
        except Exception:
            pass
        raise HTTPException(status_code=502, detail="Không lấy được attachment_id từ phản hồi Zalo")

    return ResponseModel.success(data={"attachment_id": attach_id}, message="Upload ảnh thành công")


@router.post("/messages/send-image")
async def send_image_message(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    account_id = payload.get("account_id")
    to_user_id = payload.get("to_user_id") or payload.get("user_id")
    attachment_id = payload.get("attachment_id")

    if not account_id or not to_user_id or not attachment_id:
        raise HTTPException(status_code=400, detail="Thiếu account_id, user_id hoặc attachment_id")

    # Ensure ownership
    res = await db.execute(select(OaAccount).where((OaAccount.id == uuid.UUID(account_id)) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    try:
        access_token = await _ensure_access_token_valid(db, account.id)
        result = await ZaloOAService.send_image_message(access_token, to_user_id, attachment_id)
    except NotImplementedError:
        raise HTTPException(status_code=501, detail="Gửi ảnh OA chưa được cấu hình")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lỗi gửi ảnh: {e}")

    try:
        if isinstance(result, dict) and int(result.get("error", 0)) != 0:
            raise HTTPException(status_code=502, detail=f"Zalo API error {result.get('error')}: {result.get('message')}")
    except Exception:
        pass

    return ResponseModel.success(data=result, message="Đã gửi ảnh")

@router.patch("/conversations/{conv_id}")
async def update_conversation_flags(
    conv_id: str,
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Cập nhật cờ is_ignored/is_blocked của hội thoại thuộc OA account của user.
    Body: { is_ignored?: bool, is_blocked?: bool }
    """
    # Load conversation and ensure ownership through its account
    res = await db.execute(select(OaConversation).where(OaConversation.id == uuid.UUID(conv_id)))
    conv = res.scalars().first()
    if not conv:
        raise HTTPException(status_code=404, detail="Không tìm thấy hội thoại")

    res = await db.execute(select(OaAccount).where(OaAccount.id == conv.oa_account_id))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    if account.owner_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Không có quyền")

    if "is_ignored" in payload:
        conv.is_ignored = bool(payload.get("is_ignored"))
    if "is_blocked" in payload:
        conv.is_blocked = bool(payload.get("is_blocked"))

    return ResponseModel.success(data={"id": conv_id}, message="Cập nhật hội thoại thành công")


# ---------------- Blocked users (per OA account) ----------------
@router.get("/blocked-users")
async def list_blocked_users(
    account_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Ensure ownership
    try:
        acc_uuid = uuid.UUID(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="account_id không hợp lệ")
    res = await db.execute(select(OaAccount).where((OaAccount.id == acc_uuid) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    total_q = select(func.count()).select_from(OaBlockedUser).where(OaBlockedUser.oa_account_id == account.id)
    total_res = await db.execute(total_q)
    total = total_res.scalar_one()

    q = (
        select(OaBlockedUser)
        .where(OaBlockedUser.oa_account_id == account.id)
        .order_by(desc(OaBlockedUser.created_at))
        .offset(offset)
        .limit(limit)
    )
    res = await db.execute(q)
    items = res.scalars().all()

    return ResponseModel.success(
        data=[
            {
                "id": str(it.id),
                "blocked_user_id": it.blocked_user_id,
                "note": it.note,
                "created_at": it.created_at.isoformat() if it.created_at else None,
            }
            for it in items
        ],
        total=total,
        message="Danh sách user bị chặn của OA",
    )


@router.post("/blocked-users")
async def create_blocked_user(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    account_id = payload.get("account_id")
    blocked_user_id = payload.get("blocked_user_id")
    note = payload.get("note")

    if not account_id or not blocked_user_id:
        raise HTTPException(status_code=400, detail="Thiếu account_id hoặc blocked_user_id")

    try:
        acc_uuid = uuid.UUID(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="account_id không hợp lệ")

    res = await db.execute(select(OaAccount).where((OaAccount.id == acc_uuid) & (OaAccount.owner_user_id == current_user.id)))
    account = res.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Không tìm thấy OA account")

    # Create record
    rec = OaBlockedUser(oa_account_id=account.id, blocked_user_id=str(blocked_user_id), note=note)
    db.add(rec)
    try:
        await db.flush()
    except IntegrityError:
        # Already exists per unique constraint
        raise HTTPException(status_code=409, detail="Người dùng đã nằm trong danh sách chặn của OA")

    return ResponseModel.success(
        data={"id": str(rec.id), "blocked_user_id": rec.blocked_user_id, "note": rec.note},
        message="Đã chặn người dùng cho OA",
    )


@router.delete("/blocked-users/{blocked_id}")
async def delete_blocked_user(
    blocked_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Find record
    try:
        blk_uuid = uuid.UUID(blocked_id)
    except Exception:
        raise HTTPException(status_code=400, detail="blocked_id không hợp lệ")

    res = await db.execute(select(OaBlockedUser).where(OaBlockedUser.id == blk_uuid))
    rec = res.scalars().first()
    if not rec:
        raise HTTPException(status_code=404, detail="Không tìm thấy bản ghi")

    # Ensure ownership via its OA account
    res = await db.execute(select(OaAccount).where(OaAccount.id == rec.oa_account_id))
    account = res.scalars().first()
    if not account or account.owner_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Không có quyền")

    await db.delete(rec)
    return ResponseModel.success(data={"id": blocked_id}, message="Đã bỏ chặn người dùng")


@router.patch("/blocked-users/{blocked_id}")
async def update_blocked_user(
    blocked_id: str,
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Only support updating note for now
    try:
        blk_uuid = uuid.UUID(blocked_id)
    except Exception:
        raise HTTPException(status_code=400, detail="blocked_id không hợp lệ")

    res = await db.execute(select(OaBlockedUser).where(OaBlockedUser.id == blk_uuid))
    rec = res.scalars().first()
    if not rec:
        raise HTTPException(status_code=404, detail="Không tìm thấy bản ghi")

    res = await db.execute(select(OaAccount).where(OaAccount.id == rec.oa_account_id))
    account = res.scalars().first()
    if not account or account.owner_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Không có quyền")

    if "note" in payload:
        rec.note = payload.get("note")

    return ResponseModel.success(data={"id": blocked_id}, message="Đã cập nhật ghi chú")

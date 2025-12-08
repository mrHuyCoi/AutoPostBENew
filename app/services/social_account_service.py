import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Dict, Any
from app.models.social_account import SocialAccount
from app.dto.facebook_dto import FacebookTokenRequest

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
import uuid
from sqlalchemy.ext.asyncio import AsyncSession

class ISocialAccountService(ABC):
    """
    Interface cho các thao tác liên quan đến tài khoản mạng xã hội.
    """

    @abstractmethod
    async def add_facebook_account(
        self, db: AsyncSession, user_id: uuid.UUID, data: FacebookTokenRequest
    ) -> SocialAccount:
        """
        Thêm tài khoản Facebook Page vào hệ thống chỉ sử dụng User Token.
        """
        pass

    @abstractmethod
    def _verify_facebook_token(self, access_token: str, page_id: str) -> Dict[str, Any]:
        """
        Xác thực Facebook Page Access Token bằng cách gọi API Facebook.
        """
        pass

    @abstractmethod
    async def get_user_social_accounts(self, db: AsyncSession, user_id: uuid.UUID) -> List[SocialAccount]:
        """
        Lấy danh sách tài khoản mạng xã hội của người dùng.
        """
        pass

    @abstractmethod
    async def get_social_account(self, db: AsyncSession, account_id: uuid.UUID, user_id: uuid.UUID) -> Optional[SocialAccount]:
        """
        Lấy thông tin một tài khoản mạng xã hội cụ thể.
        """
        pass

    @abstractmethod
    async def delete_social_account(self, db: AsyncSession, account_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """
        Xóa một tài khoản mạng xã hội.
        """
        pass

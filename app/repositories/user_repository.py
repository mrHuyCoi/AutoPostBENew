# app/repositories/user_repository.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text # SỬA: Import 'text'
from typing import Optional, List
import uuid
from sqlalchemy.orm import selectinload

from app.models.user import User
from app.dto.user_dto import UserCreate, UserUpdate
from app.utils.security import hash_password



class UserRepository:
    """Repository xử lý các thao tác CRUD cho đối tượng User."""
    
    @staticmethod
    async def create(db: AsyncSession, data: UserCreate) -> User:
        """Tạo một người dùng mới."""
        try:
            # Hash mật khẩu trước khi lưu
            hashed_password = hash_password(data.password)
            
            # Tạo đối tượng User
            db_user = User(
                email=data.email.lower(),  # Lưu email dưới dạng lowercase
                hashed_password=hashed_password,
                full_name=data.full_name,
                # Bỏ role_id, role có giá trị mặc định là 'user'
            )
            
            # Lưu vào database
            db.add(db_user)
            await db.commit()
            await db.refresh(db_user)
            
            return db_user
        except Exception as e:
            print(f"Error creating user: {str(e)}")
            await db.rollback()
            raise e
    
    @staticmethod
    async def get_by_id(db: AsyncSession, user_id: uuid.UUID) -> Optional[User]:
        """
        Lấy thông tin người dùng bằng ID.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            
        Returns:
            Đối tượng User hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(User)
            .where(User.id == user_id)
        )
        return result.scalars().first()
    
    @staticmethod
    async def get_by_email(db: AsyncSession, email: str) -> Optional[User]:
        """
        Lấy thông tin người dùng bằng email.
        
        Args:
            db: Database session
            email: Email của người dùng
            
        Returns:
            Đối tượng User hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(User).where(User.email.ilike(email))
        )
        return result.scalars().first()
    
    @staticmethod
    async def update(db: AsyncSession, user_id: uuid.UUID, data: UserUpdate) -> Optional[User]:
        """Cập nhật thông tin người dùng."""
        try:
            db_user = await UserRepository.get_by_id(db, user_id)
            
            if not db_user:
                return None
            
            # Cập nhật các trường
            if data.full_name is not None:
                db_user.full_name = data.full_name
            if data.custom_system_prompt is not None:
                db_user.custom_system_prompt = data.custom_system_prompt
            if data.role is not None:
                db_user.role = data.role
                db_user.is_superuser = (data.role == 'admin')
            
            # Lưu thay đổi
            await db.commit()
            await db.refresh(db_user)
            
            return db_user
        except Exception as e:
            print(f"Error updating user: {str(e)}")
            await db.rollback()
            raise e
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
        """
        Lấy danh sách người dùng với phân trang, sắp xếp theo thứ tự mới nhất đứng đầu.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa
            
        Returns:
            Danh sách các đối tượng User sắp xếp theo created_at giảm dần
        """
        result = await db.execute(select(User).order_by(User.created_at.desc()).offset(skip).limit(limit))
        return result.scalars().all()
    
    @staticmethod
    async def delete(db: AsyncSession, user_id: uuid.UUID) -> bool:
        """
        Xóa người dùng.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        try:
            db_user = await UserRepository.get_by_id(db, user_id)
            
            if not db_user:
                return False
            
            # SỬA: Dùng raw SQL DELETE để bypass lỗi 'relationship'
            # Nó sẽ không cố gắng SELECT từ 'user_bot_controls' nữa
            await db.execute(
                text("DELETE FROM users WHERE id = :user_id")
                .params(user_id=user_id)
            )
            
            # await db.delete(db_user) # <-- Bỏ dòng này
            await db.commit()
            
            return True
        except Exception as e:
            print(f"Error deleting user: {str(e)}")
            await db.rollback()
            raise e

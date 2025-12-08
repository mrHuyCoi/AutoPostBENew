from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from app.utils.time import get_vn_now


class SoftDeleteMixin:
    """
    Mixin class cung cấp chức năng soft delete cho các model.
    """
    
    @staticmethod
    async def soft_delete(db: AsyncSession, model_class, record_id, days_to_purge: int = 1):
        """
        Thực hiện soft delete cho một bản ghi.
        
        Args:
            db: Database session
            model_class: Class của model cần xóa
            record_id: ID của bản ghi cần xóa
            days_to_purge: Số ngày sau khi sẽ xóa vĩnh viễn (mặc định 1 ngày)
            
        Returns:
            bool: True nếu thành công
        """
        # Use offset-naive datetimes because DB columns are TIMESTAMP WITHOUT TIME ZONE
        now = get_vn_now().replace(tzinfo=None)
        purge_time = now + timedelta(days=days_to_purge)
        
        stmt = update(model_class).where(
            model_class.id == record_id
        ).values(
            trashed_at=now,
            purge_after=purge_time
        )
        
        result = await db.execute(stmt)
        await db.commit()
        
        return result.rowcount > 0
    
    @staticmethod
    async def restore(db: AsyncSession, model_class, record_id):
        """
        Khôi phục một bản ghi đã bị soft delete.
        
        Args:
            db: Database session
            model_class: Class của model cần khôi phục
            record_id: ID của bản ghi cần khôi phục
            
        Returns:
            bool: True nếu thành công
        """
        stmt = update(model_class).where(
            model_class.id == record_id
        ).values(
            trashed_at=None,
            purge_after=None
        )
        
        result = await db.execute(stmt)
        await db.commit()
        
        return result.rowcount > 0
    
    @staticmethod
    async def hard_delete_expired(db: AsyncSession, model_class):
        """
        Xóa vĩnh viễn các bản ghi đã hết hạn khôi phục.
        
        Args:
            db: Database session
            model_class: Class của model cần xóa
            
        Returns:
            int: Số lượng bản ghi đã xóa vĩnh viễn
        """
        from sqlalchemy import delete
        
        now = get_vn_now().replace(tzinfo=None)
        
        # Xóa các bản ghi có purge_after <= now
        stmt = delete(model_class).where(
            model_class.purge_after <= now,
            model_class.trashed_at.isnot(None)
        )
        
        result = await db.execute(stmt)
        await db.commit()
        
        return result.rowcount

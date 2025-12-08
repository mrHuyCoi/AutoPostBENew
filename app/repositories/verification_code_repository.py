from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.verification_code import VerificationCode
from app.dto.verification_code_dto import VerificationCodeCreate
from datetime import datetime, timezone

class VerificationCodeRepository:
    @staticmethod
    async def create(db: AsyncSession, data: VerificationCodeCreate) -> VerificationCode:
        try:
            # Ensure the email is stored in lowercase to prevent case-sensitivity issues
            new_code = VerificationCode(
                email=data.email.lower(),
                code=data.code,
                expires_at=data.expires_at
            )
            db.add(new_code)
            await db.commit()
            await db.refresh(new_code)
            return new_code
        except Exception as e:
            print(f"Error creating verification code: {str(e)}")
            await db.rollback()
            raise e

    @staticmethod
    async def get_by_email_and_code(db: AsyncSession, email: str, code: str) -> VerificationCode | None:
        # Compare email in lowercase and check for expiration
        # Ensure we use timezone-aware datetime for comparison
        current_time = datetime.now(timezone.utc)
        result = await db.execute(
            select(VerificationCode).where(
                VerificationCode.email == email.lower(),
                VerificationCode.code == code,
                VerificationCode.expires_at > current_time
            )
        )
        return result.scalars().first()

    @staticmethod
    async def delete(db: AsyncSession, code: VerificationCode):
        await db.delete(code)
        await db.commit()

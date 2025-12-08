import uuid
from datetime import datetime
from sqlalchemy import Column, String, Text, Integer, Table, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from app.database.database import Base

# Association Table for the many-to-many relationship between ChatbotPlan and ChatbotService
chatbot_plan_service_association = Table(
    'chatbot_plan_service_association',
    Base.metadata,
    Column('plan_id', UUID(as_uuid=True), ForeignKey('chatbot_plans.id', ondelete="CASCADE")),
    Column('service_id', UUID(as_uuid=True), ForeignKey('chatbot_services.id', ondelete="CASCADE"))
)

class ChatbotPlan(Base):
    __tablename__ = 'chatbot_plans'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False, unique=True, index=True)
    description = Column(Text, nullable=True)
    monthly_price = Column(Integer, nullable=False) # Giá của gói theo tháng (đã giảm giá combo)
    max_api_calls = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    services = relationship(
        "ChatbotService",
        secondary=chatbot_plan_service_association,
        backref="plans"
    )

    def __repr__(self):
        return f"<ChatbotPlan(name='{self.name}')>" 
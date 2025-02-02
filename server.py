import logging

from fastapi import FastAPI, HTTPException, Depends, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import (
    AsyncSession, create_async_engine, async_sessionmaker
)
from sqlalchemy import String, func, select, text
from sqlalchemy.orm import (
    DeclarativeBase, declared_attr, Mapped, mapped_column
)
from datetime import datetime
from contextlib import asynccontextmanager
from typing import AsyncGenerator, List


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# URL для подключения к базе данных SQLite
DATABASE_URL = "sqlite+aiosqlite:///./dummy_messenger.db"

# Создание асинхронного движка для работы с базой данных
engine = create_async_engine(
    DATABASE_URL,
    echo=False,  # Отключаем вывод SQL-запросов в консоль
    connect_args={"timeout": 100}  # Увеличиваем таймаут для SQLite
)

# Создание асинхронной сессии для работы с базой данных
SessionLocal = async_sessionmaker(bind=engine)


# Генератор для получения асинхронной сессии базы данных
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as db:
        try:
            yield db
        finally:
            await db.close()


# Базовый класс для всех моделей SQLAlchemy
class Base(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    id: Mapped[int] = mapped_column(
        primary_key=True, index=True, autoincrement=True
    )


# Модель для сообщений
class Message(Base):
    sender: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    text: Mapped[str] = mapped_column(String(100), nullable=False)
    message_count: Mapped[int] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        default=func.now(), server_default=func.now()
    )


# Базовый класс Pydantic для валидации входящих данных
class MessageBase(BaseModel):
    sender: str
    text: str


# Pydantic модель для запроса сообщения
class MessageRequest(MessageBase):
    pass


# Pydantic модель для ответа с сообщением
class MessageResponse(MessageBase):
    id: int
    created_at: datetime
    message_count: int


# Инициализация базы данных
async def init_database():
    async with engine.begin() as conn:
        # Включаем режим WAL для SQLite
        await conn.execute(text("PRAGMA journal_mode=WAL;"))
        # Создаем таблицы, если они не существуют
        await conn.run_sync(Base.metadata.create_all)
        logger.info("База данных успешно инициализирована.")


# Контекстный менеджер для управления жизненным циклом приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_database()
    yield
    await engine.dispose()


# Создание FastAPI приложения
app = FastAPI(lifespan=lifespan)


# Эндпоинт для отправки сообщения
@app.post("/send", response_model=List[MessageResponse])
async def send_message(
    message: MessageRequest, db: AsyncSession = Depends(get_db)
) -> List[MessageResponse]:
    """
    Обрабатывает отправку сообщения и возвращает последние 10 сообщений.
    """
    sender = message.sender
    text = message.text

    # Валидация входных данных
    if not sender or not text:
        logger.warning("Отсутствует отправитель или текст.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid input"
        )

    try:
        async with db.begin():
            # Гарантируем уникальный `message_count`
            result = await db.execute(
                select(func.coalesce(func.max(Message.message_count), 0) + 1)
                .filter(Message.sender == sender)
                .with_for_update()
            )
            new_message_count = result.scalar_one()

            # Создание нового сообщения
            new_message = Message(
                sender=sender,
                text=text,
                message_count=new_message_count
            )
            db.add(new_message)
            await db.flush()

            # Получение последних 10 сообщений
            last_messages = await db.execute(
                select(Message).order_by(Message.id.desc()).limit(10)
            )
            messages_list = [
                MessageResponse(
                    id=msg.id,
                    sender=msg.sender,
                    text=msg.text,
                    created_at=msg.created_at,
                    message_count=msg.message_count
                ) for msg in last_messages.scalars()
            ]

            return messages_list

    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

import aiohttp
import asyncio
import random
import time
import logging
from typing import Dict, Any


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Список реплик сервера
SERVER_URLS = ["http://127.0.0.1:8000/send", "http://127.0.0.1:8001/send"]

# Список пользователей
USERS = [f"User_{i}" for i in range(10)]

# Количество параллельных корутин
NUM_COROUTINES = 50

# Количество запросов на каждую корутину
REQUESTS_PER_COROUTINE = 100


async def send_message(
    session: aiohttp.ClientSession, url: str
) -> Dict[str, Any]:
    """Отправляет сообщение на сервер и возвращает ответ."""
    sender = random.choice(USERS)
    text = f"Message from {sender}"
    payload = {"sender": sender, "text": text}

    try:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error(f"Ошибка {response.status} при отправке на {url}")
                return {"error": f"HTTP {response.status}"}
    except Exception as e:
        logger.error(f"Ошибка при отправке: {e}")
        return {"error": str(e)}


async def worker(session: aiohttp.ClientSession):
    """Одна корутина, которая отправляет REQUESTS_PER_COROUTINE запросов."""
    for _ in range(REQUESTS_PER_COROUTINE):
        url = random.choice(SERVER_URLS)
        await send_message(session, url)


async def load_test():
    """Запускает нагрузочное тестирование сервера."""
    async with aiohttp.ClientSession() as session:
        start_time = time.perf_counter()

        # Создаём 50 асинхронных задач (каждая делает 100 запросов)
        tasks = [
            asyncio.create_task(worker(session)) for _ in range(NUM_COROUTINES)
        ]

        # Ожидаем завершения всех задач
        await asyncio.gather(*tasks)

        end_time = time.perf_counter()
        duration = end_time - start_time

        total_requests = NUM_COROUTINES * REQUESTS_PER_COROUTINE
        avg_time_per_request = duration / total_requests
        throughput = total_requests / duration

        logger.info(f"Всего запросов: {total_requests}")
        logger.info(f"Время выполнения: {duration:.2f} секунд")
        logger.info(f"Среднее время на запрос: {avg_time_per_request:.4f} сек")
        logger.info(f"Пропускная способность: {throughput:.2f} запросов/сек")


if __name__ == "__main__":
    logger.info("Запуск нагрузочного тестирования...")
    asyncio.run(load_test())

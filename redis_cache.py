import aioredis
from fastapi import HTTPException

class RedisCache:
    def __init__(self, redis_url="redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis = None

    async def init(self):
        """соединение с Redis"""
        self.redis = await aioredis.create_redis_pool(self.redis_url)

    async def get(self, key: str):
        """получить данные из кеша."""
        value = await self.redis.get(key)
        return value.decode("utf-8") if value else None

    async def set(self, key: str, value: str, ttl: int = 60):
        """данные в кеш с TTL """
        await self.redis.setex(key, ttl, value)

    async def close(self):
        """закрыть соединение с Redis   """
        self.redis.close()
        await self.redis.wait_closed()
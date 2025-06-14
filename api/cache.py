import asyncio

import aioredis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend


def custom_key_builder(
        func, namespace, request, response=None, *args, **kwargs):
    return f'{namespace}:{request.url.path}?{request.url.query}'


async def setup_redis_cache(app):

    def task():
        asyncio.run_coroutine_threadsafe(redis.flushdb(), loop)

    redis = aioredis.from_url('redis://localhost:6379')
    FastAPICache.init(
        RedisBackend(redis),
        prefix='spimex-cache',
        key_builder=custom_key_builder
    )

    scheduler = AsyncIOScheduler()
    loop = asyncio.get_event_loop()
    scheduler.add_job(task, CronTrigger(hour=14, minute=11))
    scheduler.start()

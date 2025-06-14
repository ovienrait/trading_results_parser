from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.cache import setup_redis_cache
from api.routers import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await setup_redis_cache(app)
    yield

app = FastAPI(
    title='Spimex Trading API',
    description='API для получения результатов торгов SPIMEX',
    version='1.0.0',
    lifespan=lifespan
)

app.include_router(router)

import aiohttp
from contextlib import asynccontextmanager


class CustomSession(aiohttp.ClientSession):
    def __init__(self, *args, **kwargs):
        super().__init__(
            headers={
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                "accept": "application/json",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "ru,uk;q=0.9,en;q=0.8",
            },
            *args,
            **kwargs,
        )

    async def close(self):
        # Здесь можно добавить логику перед закрытием сессии, если нужно
        await super().close()


@asynccontextmanager
async def customSession(*args, **kwargs):
    session = CustomSession(*args, **kwargs)
    try:
        yield session
    finally:
        await session.close()

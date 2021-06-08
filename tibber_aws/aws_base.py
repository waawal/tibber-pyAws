from __future__ import annotations

try:
    from contextlib import AsyncExitStack
except ImportError:  # To support python 3.6
    from async_exit_stack import AsyncExitStack
import logging

import aiobotocore

_LOGGER = logging.getLogger(__name__)


class AwsBase:
    def __init__(self, service_name, region_name="eu-west-1") -> None:
        self._client = None
        self._context_stack = AsyncExitStack()
        self._region_name = region_name
        self._service_name = service_name

    async def close(self) -> None:
        await self._context_stack.aclose()

    async def _init_client_if_required(
        self, session: aiobotocore.AioSession | None = None
    ) -> None:
        if self._client is not None:
            return
        session = session or aiobotocore.get_session()
        self._client = await self._context_stack.enter_async_context(
            session.create_client(self._service_name, region_name=self._region_name)
        )

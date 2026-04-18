from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable

from kafka_demo.common.envelope import EventEnvelope


MessageHandler = Callable[[EventEnvelope], Awaitable[None]]
ErrorHandler = Callable[[str, str, EventEnvelope, Exception], Awaitable[None]]


@dataclass
class _Subscription:
    consumer_name: str
    queue: asyncio.Queue[EventEnvelope]
    task: asyncio.Task


class SimulatedKafkaBroker:
    def __init__(self) -> None:
        self._subs_by_topic: dict[str, list[_Subscription]] = defaultdict(list)
        self._closed = False

    def subscribe(
        self,
        *,
        topic: str,
        consumer_name: str,
        handler: MessageHandler,
        on_error: ErrorHandler | None = None,
    ) -> None:
        queue: asyncio.Queue[EventEnvelope] = asyncio.Queue()
        task = asyncio.create_task(self._consume_loop(topic, consumer_name, queue, handler, on_error))
        self._subs_by_topic[topic].append(_Subscription(consumer_name=consumer_name, queue=queue, task=task))

    async def produce(self, event: EventEnvelope) -> None:
        if self._closed:
            return
        for sub in self._subs_by_topic.get(event.topic, []):
            await sub.queue.put(event)

    async def close(self) -> None:
        self._closed = True
        tasks: list[asyncio.Task] = []
        for subs in self._subs_by_topic.values():
            for sub in subs:
                sub.task.cancel()
                tasks.append(sub.task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _consume_loop(
        self,
        topic: str,
        consumer_name: str,
        queue: asyncio.Queue[EventEnvelope],
        handler: MessageHandler,
        on_error: ErrorHandler | None,
    ) -> None:
        while True:
            event = await queue.get()
            try:
                await handler(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if on_error:
                    await on_error(topic, consumer_name, event, exc)

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

from shared.schemas.events import EventEnvelope, KafkaTopic

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode(),
            # idempotent delivery — exactly-once producer semantics
            enable_idempotence=True,
            acks="all",
            compression_type="gzip",
        )
        await self._producer.start()
        logger.info("Kafka producer started, servers=%s", self._bootstrap_servers)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def publish(
        self,
        topic: KafkaTopic,
        event: EventEnvelope,
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("Producer not started — call start() first")

        raw_headers: list[tuple[str, bytes]] = []
        if headers:
            raw_headers = [(k, v.encode()) for k, v in headers.items()]

        # always propagate trace_id in headers for OTel context
        raw_headers.append(("trace_id", event.trace_id.encode()))
        raw_headers.append(("event_type", event.event_type.encode()))

        encoded_key = key.encode() if key else None
        payload = event.model_dump(mode="json")

        try:
            await self._producer.send_and_wait(
                topic.value,
                value=payload,
                key=encoded_key,
                headers=raw_headers,
            )
            logger.debug("Published %s → %s", event.event_type, topic.value)
        except KafkaError:
            logger.exception("Failed to publish event %s", event.event_id)
            raise


class KafkaEventConsumer:
    def __init__(
        self,
        topics: list[KafkaTopic],
        group_id: str,
        bootstrap_servers: str,
    ) -> None:
        self._topics = topics
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        topic_names = [t.value for t in self._topics]
        self._consumer = AIOKafkaConsumer(
            *topic_names,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        logger.info(
            "Kafka consumer started, group=%s topics=%s",
            self._group_id,
            topic_names,
        )

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka consumer stopped, group=%s", self._group_id)

    async def messages(self) -> AsyncIterator[tuple[dict[str, Any], dict[str, str]]]:
        """Yield (payload_dict, headers_dict) for each consumed message."""
        if self._consumer is None:
            raise RuntimeError("Consumer not started — call start() first")

        async for msg in self._consumer:
            headers = {k: v.decode() for k, v in (msg.headers or [])}
            yield msg.value, headers

    async def consume_with_handler(
        self,
        handler: Callable[[dict[str, Any], dict[str, str]], Any],
    ) -> None:
        """Drive the consumer loop, calling handler for each message."""
        async for payload, headers in self.messages():
            try:
                await handler(payload, headers)
            except Exception:
                logger.exception(
                    "Handler error for event %s", payload.get("event_id", "?")
                )

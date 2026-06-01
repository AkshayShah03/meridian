from __future__ import annotations

import asyncio
import logging
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from shared.schemas.events import KafkaTopic, NotificationEvent
from shared.schemas.kafka import KafkaEventConsumer
from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("notify-service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

app = FastAPI(title="notify-service")
tracer = None
consumer: KafkaEventConsumer | None = None
_consumer_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup() -> None:
    global tracer, consumer, _consumer_task
    setup_telemetry("notify-service")
    tracer = get_tracer("notify-service")

    consumer = KafkaEventConsumer(
        topics=[KafkaTopic.NOTIFICATIONS],
        group_id="notify-service",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer.start()
    _consumer_task = asyncio.create_task(_consume_loop())
    logger.info("notify-service ready")


@app.on_event("shutdown")
async def shutdown() -> None:
    if _consumer_task:
        _consumer_task.cancel()
    if consumer:
        await consumer.stop()


async def _consume_loop() -> None:
    async for payload, _headers in consumer.messages():
        try:
            event = NotificationEvent.model_validate(payload)
            asyncio.create_task(_dispatch(event))
        except Exception:
            logger.exception("Failed to parse NotificationEvent: %s", payload)


async def _dispatch(event: NotificationEvent) -> None:
    with span(
        tracer,
        "notify.dispatch",
        trace_id=event.trace_id,
        attributes={"channel": event.channel, "user_id": event.user_id},
    ):
        # Plug real delivery logic here (SMTP, webhook, push, etc.)
        logger.info(
            "Dispatching notification channel=%s user=%s payload=%s",
            event.channel,
            event.user_id,
            event.payload,
        )


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "notify-service"})


@app.get("/ready")
async def ready() -> JSONResponse:
    return JSONResponse({"status": "ready"})

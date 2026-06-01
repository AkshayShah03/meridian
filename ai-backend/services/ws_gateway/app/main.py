from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncGenerator

import jwt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import JSONResponse

from shared.schemas.events import (
    KafkaTopic,
    LLMResponseChunkEvent,
    UserMessageEvent,
)
from shared.schemas.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ws-gateway")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-prod")
JWT_ALGORITHM = "HS256"

app = FastAPI(title="ws-gateway")
tracer = None

# session_id → asyncio.Queue of serialised chunk dicts
_session_queues: dict[str, asyncio.Queue] = {}

producer: KafkaEventProducer | None = None
response_consumer: KafkaEventConsumer | None = None
_router_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup() -> None:
    global tracer, producer, response_consumer, _router_task
    setup_telemetry("ws-gateway")
    tracer = get_tracer("ws-gateway")

    producer = KafkaEventProducer(KAFKA_BOOTSTRAP)
    await producer.start()

    response_consumer = KafkaEventConsumer(
        topics=[KafkaTopic.LLM_RESPONSES],
        group_id="ws-gateway-response-router",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await response_consumer.start()

    _router_task = asyncio.create_task(_response_router())
    logger.info("ws-gateway ready")


@app.on_event("shutdown")
async def shutdown() -> None:
    if _router_task:
        _router_task.cancel()
    if producer:
        await producer.stop()
    if response_consumer:
        await response_consumer.stop()


# ── response router ──────────────────────────────────────────────────────────

async def _response_router() -> None:
    """Consume llm-responses and fan-out chunks to per-session queues."""
    async for payload, _headers in response_consumer.messages():
        try:
            chunk = LLMResponseChunkEvent.model_validate(payload)
            q = _session_queues.get(chunk.session_id)
            if q:
                await q.put(chunk)
            else:
                logger.debug("No active session for session_id=%s", chunk.session_id)
        except Exception:
            logger.exception("Router error for payload=%s", payload.get("event_id"))


# ── auth helper ──────────────────────────────────────────────────────────────

def _verify_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


# ── WebSocket endpoint ───────────────────────────────────────────────────────

@app.websocket("/ws/{session_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    session_id: str,
    token: str = Query(...),
) -> None:
    claims = _verify_token(token)
    user_id = claims["sub"]
    tenant_id = claims["tenant_id"]

    await websocket.accept()
    logger.info("WS connected session=%s user=%s", session_id, user_id)

    q: asyncio.Queue = asyncio.Queue(maxsize=1000)
    _session_queues[session_id] = q

    async def _sender() -> None:
        while True:
            chunk: LLMResponseChunkEvent = await q.get()
            await websocket.send_json(chunk.model_dump(mode="json"))
            if chunk.is_final:
                break

    try:
        while True:
            data = await websocket.receive_json()
            message_text: str = data.get("message", "")
            model: str = data.get("model", "mistral:7b")
            agent_mode: bool = bool(data.get("agent_mode", False))
            tenant_tier: str = claims.get("tier", "free")

            with span(
                tracer,
                "ws.user_message",
                attributes={"session_id": session_id, "user_id": user_id,
                            "agent_mode": str(agent_mode)},
            ) as s:
                ctx = s.get_span_context()
                trace_id_str = format(ctx.trace_id, "032x") if ctx.trace_id else None
                event = UserMessageEvent(
                    **({"trace_id": trace_id_str} if trace_id_str else {}),
                    session_id=session_id,
                    user_id=user_id,
                    tenant_id=tenant_id,
                    message=message_text,
                    model=model,
                    agent_mode=agent_mode,
                    tenant_tier=tenant_tier,
                )
                await producer.publish(
                    KafkaTopic.USER_EVENTS,
                    event,
                    key=session_id,
                )
                logger.debug("Published user message session=%s", session_id)

            # drive sender until this response finishes
            await asyncio.create_task(_sender())

    except WebSocketDisconnect:
        logger.info("WS disconnected session=%s", session_id)
    finally:
        _session_queues.pop(session_id, None)


# ── health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "ws-gateway"})


@app.get("/ready")
async def ready() -> JSONResponse:
    connected = len(_session_queues)
    return JSONResponse({"status": "ready", "active_sessions": connected})

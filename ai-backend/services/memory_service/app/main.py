from __future__ import annotations

"""
Memory Service — replaces thin context-service with three memory layers:

  Short-term  Redis list  recent N turns, TTL 24h
  Semantic    pgvector    cosine-similar past turns retrieved per query
  Episodic    Redis str   auto-generated conversation summaries

Flow:
  user-events → assemble context → llm-requests (direct) | agent-tasks (agent_mode)
  llm-responses (final chunks only) → store assistant reply in all layers
"""

import asyncio
import json
import logging
import os
from typing import Any

import asyncpg
import httpx
import redis.asyncio as aioredis
import tiktoken
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from shared.schemas.events import (
    AgentTaskEvent,
    KafkaTopic,
    LLMRequestEvent,
    LLMResponseChunkEvent,
    UserMessageEvent,
)
from shared.schemas.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("memory-service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/aibackend")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
MAX_CONTEXT_TOKENS = int(os.getenv("MAX_CONTEXT_TOKENS", "8000"))
HISTORY_TTL = 60 * 60 * 24       # 24h for short-term
SUMMARY_TTL = 60 * 60 * 24 * 7   # 7d for episodic summaries
SEMANTIC_TOP_K = 4                 # similar memories to inject
EMBED_MODEL = "nomic-embed-text"   # Ollama embedding model (free)
EMBED_DIM = 768                    # nomic-embed-text output dimension

app = FastAPI(title="memory-service")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
tracer = None
redis_client: aioredis.Redis | None = None
db_pool: asyncpg.Pool | None = None
consumer: KafkaEventConsumer | None = None
response_consumer: KafkaEventConsumer | None = None
producer: KafkaEventProducer | None = None
_enc = tiktoken.get_encoding("cl100k_base")
_consumer_task: asyncio.Task | None = None
_response_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup() -> None:
    global tracer, redis_client, db_pool, consumer, response_consumer, producer
    global _consumer_task, _response_task

    setup_telemetry("memory-service")
    tracer = get_tracer("memory-service")

    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)

    await _ensure_pgvector(db_pool)

    producer = KafkaEventProducer(KAFKA_BOOTSTRAP)
    await producer.start()

    consumer = KafkaEventConsumer(
        topics=[KafkaTopic.USER_EVENTS],
        group_id="memory-service-user",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer.start()

    response_consumer = KafkaEventConsumer(
        topics=[KafkaTopic.LLM_RESPONSES],
        group_id="memory-service-responses",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await response_consumer.start()

    _consumer_task = asyncio.create_task(_consume_user_events())
    _response_task = asyncio.create_task(_consume_llm_responses())
    logger.info("memory-service ready, max_tokens=%d embed_model=%s", MAX_CONTEXT_TOKENS, EMBED_MODEL)


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in (_consumer_task, _response_task):
        if task:
            task.cancel()
    if producer:
        await producer.stop()
    for c in (consumer, response_consumer):
        if c:
            await c.stop()
    if redis_client:
        await redis_client.aclose()
    if db_pool:
        await db_pool.close()


# ── pgvector setup ────────────────────────────────────────────────────────────

async def _ensure_pgvector(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS memory_embeddings (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                session_id  TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                role        TEXT NOT NULL,
                content     TEXT NOT NULL,
                embedding   vector({EMBED_DIM}),
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mem_user ON memory_embeddings(user_id)"
        )
    logger.info("pgvector table ready, dim=%d", EMBED_DIM)


# ── embedding via Ollama ──────────────────────────────────────────────────────

async def _embed(text: str) -> list[float] | None:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{OLLAMA_BASE_URL}/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text},
            )
            resp.raise_for_status()
            return resp.json()["embedding"]
    except Exception:
        logger.warning("Embedding failed (Ollama unavailable?), skipping semantic layer")
        return None


def _vec_str(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


# ── short-term memory (Redis) ─────────────────────────────────────────────────

def _history_key(session_id: str) -> str:
    return f"history:{session_id}"


def _summary_key(session_id: str) -> str:
    return f"summary:{session_id}"


async def _append_to_history(session_id: str, role: str, content: str) -> None:
    key = _history_key(session_id)
    await redis_client.rpush(key, json.dumps({"role": role, "content": content}))
    await redis_client.expire(key, HISTORY_TTL)


async def _load_history(session_id: str) -> list[dict[str, Any]]:
    raw = await redis_client.lrange(_history_key(session_id), 0, -1)
    return [json.loads(r) for r in raw]


# ── semantic memory (pgvector) ────────────────────────────────────────────────

async def _store_embedding(session_id: str, user_id: str, role: str, content: str) -> None:
    vec = await _embed(content)
    if vec is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO memory_embeddings (session_id, user_id, role, content, embedding)
               VALUES ($1, $2, $3, $4, $5::vector)""",
            session_id, user_id, role, content, _vec_str(vec),
        )


async def _semantic_recall(user_id: str, query: str, limit: int = SEMANTIC_TOP_K) -> list[dict[str, Any]]:
    vec = await _embed(query)
    if vec is None:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT role, content, 1 - (embedding <=> $1::vector) AS similarity
               FROM memory_embeddings
               WHERE user_id = $2
               ORDER BY embedding <=> $1::vector
               LIMIT $3""",
            _vec_str(vec), user_id, limit,
        )
    return [{"role": r["role"], "content": r["content"], "similarity": float(r["similarity"])} for r in rows]


# ── episodic memory: auto-summarise long sessions ─────────────────────────────

async def _maybe_summarise(session_id: str, history: list[dict[str, Any]]) -> str | None:
    if len(history) < 20:
        return None
    summary = await redis_client.get(_summary_key(session_id))
    if summary:
        return summary
    # Build a cheap extractive summary (first + last 3 turns)
    head = history[:3]
    tail = history[-3:]
    summary_text = "Earlier context summary: " + " | ".join(
        f"{m['role']}: {m['content'][:80]}" for m in head + tail
    )
    await redis_client.setex(_summary_key(session_id), SUMMARY_TTL, summary_text)
    return summary_text


# ── token budget truncation ───────────────────────────────────────────────────

def _truncate(messages: list[dict[str, Any]], max_tokens: int) -> list[dict[str, Any]]:
    budget = max_tokens
    kept: list[dict[str, Any]] = []
    for msg in reversed(messages):
        cost = len(_enc.encode(msg["content"]))
        if cost > budget:
            break
        kept.append(msg)
        budget -= cost
    kept.reverse()
    return kept


# ── user-event consumer ───────────────────────────────────────────────────────

async def _consume_user_events() -> None:
    async for payload, _headers in consumer.messages():
        try:
            event = UserMessageEvent.model_validate(payload)
            asyncio.create_task(_handle_user_message(event))
        except Exception:
            logger.exception("Parse error: %s", payload.get("event_id"))


async def _handle_user_message(event: UserMessageEvent) -> None:
    with span(tracer, "memory.assemble", trace_id=event.trace_id,
              attributes={"session_id": event.session_id}):

        # 1. Store incoming turn in all memory layers
        await _append_to_history(event.session_id, "user", event.message)
        asyncio.create_task(
            _store_embedding(event.session_id, event.user_id, "user", event.message)
        )

        # 2. Load and truncate short-term history
        history = await _load_history(event.session_id)
        messages = _truncate(history, MAX_CONTEXT_TOKENS)

        # 3. Inject semantic memories as a system preamble (if Ollama is up)
        similar = await _semantic_recall(event.user_id, event.message)
        if similar:
            mem_text = "\n".join(
                f"[Memory] {m['role']}: {m['content'][:200]}"
                for m in similar
                if m["similarity"] > 0.75
            )
            if mem_text:
                messages = [{"role": "system", "content": f"Relevant past context:\n{mem_text}"}] + messages

        # 4. Inject episodic summary for long sessions
        summary = await _maybe_summarise(event.session_id, history)
        if summary:
            messages = [{"role": "system", "content": summary}] + messages

        # 5. Route to agent-orchestrator or inference-gateway
        if event.agent_mode:
            await producer.publish(
                KafkaTopic.AGENT_TASKS,
                AgentTaskEvent(
                    trace_id=event.trace_id,
                    session_id=event.session_id,
                    user_id=event.user_id,
                    tenant_id=event.tenant_id,
                    task=event.message,
                    model=event.model,
                ),
                key=event.session_id,
            )
        else:
            await producer.publish(
                KafkaTopic.LLM_REQUESTS,
                LLMRequestEvent(
                    trace_id=event.trace_id,
                    session_id=event.session_id,
                    user_id=event.user_id,
                    tenant_id=event.tenant_id,
                    tenant_tier=event.tenant_tier,
                    messages=messages,
                    model=event.model,
                ),
                key=event.session_id,
            )
        logger.info("Assembled context session=%s turns=%d semantic_hits=%d",
                    event.session_id, len(messages), len(similar))


# ── llm-response consumer: store assistant reply ──────────────────────────────

_pending_responses: dict[str, list[str]] = {}


async def _consume_llm_responses() -> None:
    async for payload, _headers in response_consumer.messages():
        try:
            chunk = LLMResponseChunkEvent.model_validate(payload)
            sid = chunk.session_id
            if not chunk.is_final:
                _pending_responses.setdefault(sid, []).append(chunk.token)
            else:
                tokens = _pending_responses.pop(sid, [])
                full = "".join(tokens)
                if full:
                    asyncio.create_task(
                        _store_assistant_reply(sid, chunk.user_id, full)
                    )
        except Exception:
            logger.exception("Response parse error")


async def _store_assistant_reply(session_id: str, user_id: str, content: str) -> None:
    await _append_to_history(session_id, "assistant", content)
    await _store_embedding(session_id, user_id, "assistant", content)


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "memory-service"})


@app.get("/history/{session_id}")
async def get_history(session_id: str) -> JSONResponse:
    """Return Redis conversation history for a session — used by the demo UI."""
    history = await _load_history(session_id)
    summary = await redis_client.get(_summary_key(session_id))
    embedding_count = 0
    try:
        async with db_pool.acquire() as conn:
            embedding_count = await conn.fetchval(
                "SELECT COUNT(*) FROM memory_embeddings WHERE session_id = $1", session_id
            )
    except Exception:
        pass
    return JSONResponse({
        "session_id": session_id,
        "turns": len(history),
        "messages": history,
        "has_summary": summary is not None,
        "embedding_count": int(embedding_count or 0),
    })


@app.get("/sessions")
async def list_sessions() -> JSONResponse:
    """Return all active session IDs in Redis — used by the demo UI."""
    keys = await redis_client.keys("history:*")
    sessions = []
    for k in keys[:20]:
        sid = k.replace("history:", "")
        length = await redis_client.llen(k)
        ttl = await redis_client.ttl(k)
        sessions.append({"session_id": sid, "turns": length, "ttl_seconds": ttl})
    return JSONResponse({"sessions": sessions, "total": len(keys)})


@app.get("/ready")
async def ready() -> JSONResponse:
    try:
        await redis_client.ping()
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return JSONResponse({"status": "ready"})
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

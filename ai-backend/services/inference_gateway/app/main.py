from __future__ import annotations

"""
Inference Gateway — replaces thin llm-proxy with:
  - Multi-model routing: Ollama (free/local) → Anthropic (premium fallback)
  - Exact prompt cache via Redis (SHA-256 of messages+model)
  - Concurrency gate via asyncio.Semaphore
  - Structured output via instructor + Ollama's OpenAI-compat endpoint
  - TTFT measurement, cache-hit tagging, routing decision events
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any, AsyncIterator

import anthropic
import httpx
import instructor
import openai
import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from shared.schemas.events import (
    EvalRequestEvent,
    KafkaTopic,
    LLMRequestEvent,
    LLMResponseChunkEvent,
    ModelRoutingEvent,
)
from shared.schemas.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("inference-gateway")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_LLM_REQUESTS", "20"))
CACHE_TTL = int(os.getenv("PROMPT_CACHE_TTL", "3600"))  # 1 hour

# Model tiers — all Ollama models are free/local
ROUTING_TABLE: dict[str, dict[str, str]] = {
    "fast":      {"provider": "ollama", "model": "llama3.2:3b"},
    "code":      {"provider": "ollama", "model": "deepseek-coder:6.7b"},
    "reasoning": {"provider": "ollama", "model": "mistral:7b"},
    "premium":   {"provider": "anthropic", "model": "claude-sonnet-4-6"},
}

app = FastAPI(title="inference-gateway")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
tracer = None
_semaphore: asyncio.Semaphore | None = None
redis_client: aioredis.Redis | None = None
consumer: KafkaEventConsumer | None = None
producer: KafkaEventProducer | None = None
_consumer_task: asyncio.Task | None = None
_anthropic: anthropic.AsyncAnthropic | None = None
_structured_client: instructor.AsyncInstructor | None = None


@app.on_event("startup")
async def startup() -> None:
    global tracer, _semaphore, redis_client, consumer, producer, _consumer_task
    global _anthropic, _structured_client

    setup_telemetry("inference-gateway")
    tracer = get_tracer("inference-gateway")
    _semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)

    if ANTHROPIC_API_KEY:
        _anthropic = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

    # instructor wraps Ollama's OpenAI-compatible endpoint for structured output
    _structured_client = instructor.from_openai(
        openai.AsyncOpenAI(base_url=f"{OLLAMA_BASE_URL}/v1", api_key="ollama"),
        mode=instructor.Mode.JSON,
    )

    producer = KafkaEventProducer(KAFKA_BOOTSTRAP)
    await producer.start()

    consumer = KafkaEventConsumer(
        topics=[KafkaTopic.LLM_REQUESTS],
        group_id="inference-gateway",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer.start()
    _consumer_task = asyncio.create_task(_consume_loop())
    logger.info("inference-gateway ready, ollama=%s max_concurrent=%d", OLLAMA_BASE_URL, MAX_CONCURRENT)


@app.on_event("shutdown")
async def shutdown() -> None:
    if _consumer_task:
        _consumer_task.cancel()
    if producer:
        await producer.stop()
    if consumer:
        await consumer.stop()
    if redis_client:
        await redis_client.aclose()
    if _anthropic:
        await _anthropic.close()


# ── model routing ─────────────────────────────────────────────────────────────

async def _available_model(preferred: str) -> str:
    """Return preferred model if Ollama has it, else fall back to llama3.2:3b."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
            names = {m["name"] for m in resp.json().get("models", [])}
        if preferred in names:
            return preferred
        for fallback in ["llama3.2:3b", "llama3.2:1b"]:
            if fallback in names:
                logger.warning("Model %s not available, falling back to %s", preferred, fallback)
                return fallback
    except Exception:
        pass
    return preferred  # try anyway — Ollama will error with a clear message


def _detect_task_type(messages: list[dict[str, Any]], tenant_tier: str) -> str:
    if tenant_tier == "premium" and ANTHROPIC_API_KEY:
        return "premium"
    # exclude system-role messages (injected by memory-service) from the count
    user_msgs = [m for m in messages if m.get("role") != "system"]
    last = user_msgs[-1]["content"].lower() if user_msgs else ""
    code_kws = {"code", "function", "implement", "debug", "class", "script", "algorithm", "def ", "sql"}
    if any(kw in last for kw in code_kws):
        return "code"
    if len(user_msgs) <= 1 and len(last) < 150:
        return "fast"
    return "reasoning"


def _cache_key(messages: list[dict[str, Any]], model: str) -> str:
    payload = json.dumps({"messages": messages, "model": model}, sort_keys=True)
    return "prompt_cache:" + hashlib.sha256(payload.encode()).hexdigest()


# ── Ollama streaming ──────────────────────────────────────────────────────────

async def _stream_ollama(model: str, messages: list[dict[str, Any]]) -> AsyncIterator[str]:
    async with httpx.AsyncClient(timeout=120.0) as client:
        async with client.stream(
            "POST",
            f"{OLLAMA_BASE_URL}/api/chat",
            json={"model": model, "messages": messages, "stream": True},
        ) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                data = json.loads(line)
                token = data.get("message", {}).get("content", "")
                if token:
                    yield token
                if data.get("done"):
                    break


# ── Anthropic streaming ───────────────────────────────────────────────────────

async def _stream_anthropic(model: str, messages: list[dict[str, Any]], max_tokens: int) -> AsyncIterator[str]:
    if not _anthropic:
        raise RuntimeError("Anthropic key not configured")
    async with _anthropic.messages.stream(
        model=model, max_tokens=max_tokens, messages=messages
    ) as stream:
        async for token in stream.text_stream:
            yield token


# ── cache helpers ─────────────────────────────────────────────────────────────

async def _get_cached(key: str) -> str | None:
    return await redis_client.get(key)


async def _set_cached(key: str, response: str) -> None:
    await redis_client.setex(key, CACHE_TTL, response)


# ── main inference loop ───────────────────────────────────────────────────────

async def _consume_loop() -> None:
    async for payload, _headers in consumer.messages():
        try:
            event = LLMRequestEvent.model_validate(payload)
            asyncio.create_task(_handle(event))
        except Exception:
            logger.exception("Parse error: %s", payload.get("event_id"))


async def _handle(event: LLMRequestEvent) -> None:
    async with _semaphore:
        with span(tracer, "inference.serve", trace_id=event.trace_id,
                  attributes={"session_id": event.session_id, "model": event.model}):
            await _infer_and_publish(event)


async def _infer_and_publish(event: LLMRequestEvent) -> None:
    cache_key = _cache_key(event.messages, event.model)
    cached = await _get_cached(cache_key)
    if cached:
        # replay cached response as a token stream
        logger.info("Cache HIT session=%s", event.session_id)
        await _publish_stream_from_string(event, cached, cache_hit=True, provider="cache", model_used=event.model)
        return

    task_type = _detect_task_type(event.messages, event.tenant_tier)
    route = ROUTING_TABLE[task_type]
    provider = route["provider"]
    # check model availability and fall back if not yet downloaded
    model = route["model"] if provider == "anthropic" else await _available_model(route["model"])

    await producer.publish(
        KafkaTopic.AUDIT_LOG,
        ModelRoutingEvent(
            trace_id=event.trace_id,
            session_id=event.session_id,
            requested_model=event.model,
            routed_to_model=model,
            provider=provider,
            reason=task_type,
        ),
        key=event.session_id,
    )

    chunk_index = 0
    start = time.monotonic()
    ttft_ms: float | None = None
    full_response: list[str] = []

    try:
        stream = (
            _stream_anthropic(model, event.messages, event.max_tokens)
            if provider == "anthropic"
            else _stream_ollama(model, event.messages)
        )

        async for token in stream:
            if ttft_ms is None:
                ttft_ms = (time.monotonic() - start) * 1000

            full_response.append(token)
            await producer.publish(
                KafkaTopic.LLM_RESPONSES,
                LLMResponseChunkEvent(
                    trace_id=event.trace_id,
                    session_id=event.session_id,
                    user_id=event.user_id,
                    chunk_index=chunk_index,
                    token=token,
                    is_final=False,
                    provider=provider,
                    model_used=model,
                ),
                key=event.session_id,
                headers={"trace_id": event.trace_id},
            )
            chunk_index += 1

    except Exception as exc:
        logger.exception("Inference error session=%s", event.session_id)
        error_token = f"[ERROR: {exc}]"
        full_response.append(error_token)
        await producer.publish(
            KafkaTopic.LLM_RESPONSES,
            LLMResponseChunkEvent(
                trace_id=event.trace_id,
                session_id=event.session_id,
                user_id=event.user_id,
                chunk_index=chunk_index,
                token=error_token,
                is_final=True,
                provider=provider,
                model_used=model,
            ),
            key=event.session_id,
        )
        return

    # final sentinel
    await producer.publish(
        KafkaTopic.LLM_RESPONSES,
        LLMResponseChunkEvent(
            trace_id=event.trace_id,
            session_id=event.session_id,
            user_id=event.user_id,
            chunk_index=chunk_index,
            token="",
            is_final=True,
            ttft_ms=ttft_ms,
            provider=provider,
            model_used=model,
        ),
        key=event.session_id,
    )

    complete = "".join(full_response)
    await _set_cached(cache_key, complete)

    # fire evaluation asynchronously
    await producer.publish(
        KafkaTopic.EVAL_REQUESTS,
        EvalRequestEvent(
            trace_id=event.trace_id,
            session_id=event.session_id,
            user_id=event.user_id,
            tenant_id=event.tenant_id,
            messages=event.messages,
            response=complete,
            model=model,
            provider=provider,
            ttft_ms=ttft_ms,
            total_tokens=len(complete.split()),
        ),
        key=event.session_id,
    )
    logger.info("Served session=%s provider=%s model=%s chunks=%d ttft_ms=%.1f",
                event.session_id, provider, model, chunk_index, ttft_ms or 0)


async def _publish_stream_from_string(
    event: LLMRequestEvent,
    text: str,
    *,
    cache_hit: bool,
    provider: str,
    model_used: str,
) -> None:
    words = text.split(" ")
    for i, word in enumerate(words):
        token = word if i == len(words) - 1 else word + " "
        await producer.publish(
            KafkaTopic.LLM_RESPONSES,
            LLMResponseChunkEvent(
                trace_id=event.trace_id,
                session_id=event.session_id,
                user_id=event.user_id,
                chunk_index=i,
                token=token,
                is_final=False,
                provider=provider,
                model_used=model_used,
                cache_hit=cache_hit,
            ),
            key=event.session_id,
        )
    await producer.publish(
        KafkaTopic.LLM_RESPONSES,
        LLMResponseChunkEvent(
            trace_id=event.trace_id,
            session_id=event.session_id,
            user_id=event.user_id,
            chunk_index=len(words),
            token="",
            is_final=True,
            provider=provider,
            model_used=model_used,
            cache_hit=cache_hit,
        ),
        key=event.session_id,
    )


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "inference-gateway"})


@app.get("/cache")
async def cache_stats() -> JSONResponse:
    """Return Redis prompt-cache statistics — used by the demo UI."""
    keys = await redis_client.keys("prompt_cache:*")
    entries = []
    for k in keys[:10]:
        ttl = await redis_client.ttl(k)
        val = await redis_client.get(k)
        entries.append({
            "key_suffix": k[-12:],
            "ttl_seconds": ttl,
            "response_chars": len(val) if val else 0,
            "preview": (val or "")[:80] + ("…" if val and len(val) > 80 else ""),
        })
    return JSONResponse({
        "total_cached": len(keys),
        "cache_ttl_seconds": CACHE_TTL,
        "entries": entries,
    })


@app.get("/routing-table")
async def routing_table() -> JSONResponse:
    return JSONResponse({"routing_table": ROUTING_TABLE})


@app.get("/ready")
async def ready() -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
            models = [m["name"] for m in resp.json().get("models", [])]
        return JSONResponse({"status": "ready", "ollama_models": models})
    except Exception as exc:
        return JSONResponse({"status": "degraded", "reason": str(exc)}, status_code=200)

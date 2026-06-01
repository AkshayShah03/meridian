from __future__ import annotations

"""
Eval Service — automated quality measurement for every LLM response.

Scoring dimensions (all heuristic/free, no LLM judge required):
  coherence     — response is not empty, not a refusal, not an error token
  relevance     — keyword overlap between last user message and response
  completeness  — response length relative to question complexity
  latency       — normalised TTFT score (lower is better)
  safety        — absence of known toxic patterns

Scores stored in Postgres llm_usage table.
EvalResultEvent published to eval-results topic for downstream consumers.
"""

import asyncio
import logging
import os
import re
import time
from typing import Any

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from shared.schemas.events import EvalRequestEvent, EvalResultEvent, KafkaTopic
from shared.schemas.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("eval-service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/aibackend")

# Refusal / error patterns reduce coherence score
_REFUSAL_RE = re.compile(
    r"\b(I can't|I cannot|I'm unable|I am unable|As an AI|I don't have access|I'm sorry, but)\b",
    re.IGNORECASE,
)
_ERROR_RE = re.compile(r"\[ERROR:", re.IGNORECASE)

# Ideal TTFT threshold in ms — responses faster than this get full score
IDEAL_TTFT_MS = 500.0
WORST_TTFT_MS = 5000.0

app = FastAPI(title="eval-service")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
tracer = None
db_pool: asyncpg.Pool | None = None
consumer: KafkaEventConsumer | None = None
producer: KafkaEventProducer | None = None
_consumer_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup() -> None:
    global tracer, db_pool, consumer, producer, _consumer_task

    setup_telemetry("eval-service")
    tracer = get_tracer("eval-service")

    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await _ensure_eval_table(db_pool)

    producer = KafkaEventProducer(KAFKA_BOOTSTRAP)
    await producer.start()

    consumer = KafkaEventConsumer(
        topics=[KafkaTopic.EVAL_REQUESTS],
        group_id="eval-service",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer.start()
    _consumer_task = asyncio.create_task(_consume_loop())
    logger.info("eval-service ready")


@app.on_event("shutdown")
async def shutdown() -> None:
    if _consumer_task:
        _consumer_task.cancel()
    if producer:
        await producer.stop()
    if consumer:
        await consumer.stop()
    if db_pool:
        await db_pool.close()


# ── DB setup ──────────────────────────────────────────────────────────────────

async def _ensure_eval_table(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS response_evals (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                session_id      TEXT NOT NULL,
                user_id         TEXT NOT NULL,
                tenant_id       TEXT NOT NULL,
                model           TEXT NOT NULL,
                provider        TEXT NOT NULL,
                coherence       FLOAT,
                relevance       FLOAT,
                completeness    FLOAT,
                latency_score   FLOAT,
                safety_score    FLOAT,
                overall_score   FLOAT,
                issues          TEXT[],
                ttft_ms         FLOAT,
                total_tokens    INTEGER,
                evaluated_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)


# ── scoring functions ─────────────────────────────────────────────────────────

def _score_coherence(response: str) -> tuple[float, list[str]]:
    issues = []
    if not response.strip():
        return 0.0, ["empty_response"]
    if _ERROR_RE.search(response):
        issues.append("contains_error_token")
        return 0.1, issues
    if _REFUSAL_RE.search(response):
        issues.append("refusal_detected")
        return 0.5, issues
    return 1.0, issues


def _score_relevance(messages: list[dict[str, Any]], response: str) -> float:
    last_user = next(
        (m["content"] for m in reversed(messages) if m.get("role") == "user"),
        "",
    )
    if not last_user or not response:
        return 0.0
    query_words = set(re.findall(r"\w+", last_user.lower())) - {"the", "a", "an", "is", "are", "i", "to", "of"}
    response_words = set(re.findall(r"\w+", response.lower()))
    if not query_words:
        return 1.0
    overlap = len(query_words & response_words) / len(query_words)
    return min(overlap * 2, 1.0)  # scale: 50% overlap → 1.0


def _score_completeness(messages: list[dict[str, Any]], response: str) -> float:
    last_user = next(
        (m["content"] for m in reversed(messages) if m.get("role") == "user"),
        "",
    )
    question_words = len(last_user.split())
    response_words = len(response.split())
    # expect ~3× more words in response than question for a complete answer
    ratio = response_words / max(question_words * 3, 1)
    return min(ratio, 1.0)


def _score_latency(ttft_ms: float | None) -> float:
    if ttft_ms is None:
        return 0.5  # unknown — neutral
    if ttft_ms <= IDEAL_TTFT_MS:
        return 1.0
    if ttft_ms >= WORST_TTFT_MS:
        return 0.0
    return 1.0 - (ttft_ms - IDEAL_TTFT_MS) / (WORST_TTFT_MS - IDEAL_TTFT_MS)


def _score_safety(response: str) -> tuple[float, list[str]]:
    issues = []
    # Basic toxicity heuristic — extend with a real classifier in production
    dangerous = ["exec(", "eval(", "__import__", "os.system", "subprocess"]
    for d in dangerous:
        if d in response:
            issues.append(f"suspicious_code_pattern:{d}")
    return (0.2 if issues else 1.0), issues


def _evaluate(event: EvalRequestEvent) -> tuple[dict[str, float], float, list[str]]:
    coherence, c_issues = _score_coherence(event.response)
    relevance = _score_relevance(event.messages, event.response)
    completeness = _score_completeness(event.messages, event.response)
    latency = _score_latency(event.ttft_ms)
    safety, s_issues = _score_safety(event.response)

    scores = {
        "coherence": round(coherence, 3),
        "relevance": round(relevance, 3),
        "completeness": round(completeness, 3),
        "latency": round(latency, 3),
        "safety": round(safety, 3),
    }
    weights = {"coherence": 0.30, "relevance": 0.25, "completeness": 0.20, "latency": 0.15, "safety": 0.10}
    overall = sum(scores[k] * weights[k] for k in scores)
    issues = c_issues + s_issues
    return scores, round(overall, 3), issues


# ── consumer ──────────────────────────────────────────────────────────────────

async def _consume_loop() -> None:
    async for payload, _headers in consumer.messages():
        try:
            event = EvalRequestEvent.model_validate(payload)
            asyncio.create_task(_handle(event))
        except Exception:
            logger.exception("Parse error: %s", payload.get("event_id"))


async def _handle(event: EvalRequestEvent) -> None:
    with span(tracer, "eval.score", trace_id=event.trace_id,
              attributes={"session_id": event.session_id, "model": event.model}):

        scores, overall, issues = _evaluate(event)

        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO response_evals
                   (session_id, user_id, tenant_id, model, provider,
                    coherence, relevance, completeness, latency_score, safety_score,
                    overall_score, issues, ttft_ms, total_tokens)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
                event.session_id, event.user_id, event.tenant_id,
                event.model, event.provider,
                scores["coherence"], scores["relevance"], scores["completeness"],
                scores["latency"], scores["safety"],
                overall, issues, event.ttft_ms, event.total_tokens,
            )

        await producer.publish(
            KafkaTopic.EVAL_RESULTS,
            EvalResultEvent(
                trace_id=event.trace_id,
                session_id=event.session_id,
                scores=scores,
                overall_score=overall,
                issues=issues,
            ),
            key=event.session_id,
        )
        logger.info("Eval session=%s overall=%.2f issues=%s model=%s",
                    event.session_id, overall, issues, event.model)


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "eval-service"})


@app.get("/evals")
async def get_evals(limit: int = 20) -> JSONResponse:
    """Return recent evaluation scores — used by the demo UI."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT session_id, user_id, tenant_id, model, provider,
                      coherence, relevance, completeness, latency_score,
                      safety_score, overall_score, issues, ttft_ms, total_tokens,
                      evaluated_at
               FROM response_evals
               ORDER BY evaluated_at DESC LIMIT $1""",
            limit,
        )
    return JSONResponse([
        {
            "session_id": str(r["session_id"]),
            "user_id": str(r["user_id"]),
            "tenant_id": r["tenant_id"],
            "model": r["model"],
            "provider": r["provider"],
            "scores": {
                "coherence":    round(float(r["coherence"] or 0), 3),
                "relevance":    round(float(r["relevance"] or 0), 3),
                "completeness": round(float(r["completeness"] or 0), 3),
                "latency":      round(float(r["latency_score"] or 0), 3),
                "safety":       round(float(r["safety_score"] or 0), 3),
            },
            "overall_score": round(float(r["overall_score"] or 0), 3),
            "issues":   list(r["issues"] or []),
            "ttft_ms":  round(float(r["ttft_ms"]), 1) if r["ttft_ms"] else None,
            "total_tokens": r["total_tokens"],
            "evaluated_at": r["evaluated_at"].isoformat(),
        }
        for r in rows
    ])


@app.get("/ready")
async def ready() -> JSONResponse:
    try:
        async with db_pool.acquire() as conn:
            avg = await conn.fetchval("SELECT AVG(overall_score) FROM response_evals")
        return JSONResponse({"status": "ready", "avg_quality_score": float(avg or 0)})
    except Exception as exc:
        return JSONResponse({"status": "degraded", "reason": str(exc)})

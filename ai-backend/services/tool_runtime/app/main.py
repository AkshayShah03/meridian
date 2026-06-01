from __future__ import annotations

"""
Tool Runtime — executes tools on behalf of the agent-orchestrator.

Tools (all free, no API keys):
  calculate   — safe AST-based math evaluator (no exec/eval)
  search      — DuckDuckGo Instant Answers (free, no key)
  fetch_url   — HTTP GET with SSRF protection
  get_datetime — current UTC datetime

Security:
  - calculate: AST whitelist, no builtins
  - fetch_url: blocks private/loopback IPs (SSRF guard)
  - All tools run with asyncio.wait_for timeout
"""

import ast
import asyncio
import ipaddress
import logging
import operator
import os
import socket
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from shared.schemas.events import KafkaTopic, ToolRequestEvent, ToolResultEvent
from shared.schemas.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("tool-runtime")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FETCH_TIMEOUT = 10.0
SEARCH_MAX_RESULTS = 3

app = FastAPI(title="tool-runtime")
tracer = None
consumer: KafkaEventConsumer | None = None
producer: KafkaEventProducer | None = None
_consumer_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup() -> None:
    global tracer, consumer, producer, _consumer_task

    setup_telemetry("tool-runtime")
    tracer = get_tracer("tool-runtime")

    producer = KafkaEventProducer(KAFKA_BOOTSTRAP)
    await producer.start()

    consumer = KafkaEventConsumer(
        topics=[KafkaTopic.TOOL_REQUESTS],
        group_id="tool-runtime",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer.start()
    _consumer_task = asyncio.create_task(_consume_loop())
    logger.info("tool-runtime ready")


@app.on_event("shutdown")
async def shutdown() -> None:
    if _consumer_task:
        _consumer_task.cancel()
    if producer:
        await producer.stop()
    if consumer:
        await consumer.stop()


# ── consumer ──────────────────────────────────────────────────────────────────

async def _consume_loop() -> None:
    async for payload, _headers in consumer.messages():
        try:
            req = ToolRequestEvent.model_validate(payload)
            asyncio.create_task(_execute(req))
        except Exception:
            logger.exception("Parse error: %s", payload)


async def _execute(req: ToolRequestEvent) -> None:
    start = time.monotonic()
    output = ""
    error: str | None = None

    with span(tracer, f"tool.{req.tool_name}", trace_id=req.trace_id,
              attributes={"tool": req.tool_name, "task_id": req.task_id}):
        try:
            output = await asyncio.wait_for(
                _dispatch(req.tool_name, req.tool_input),
                timeout=FETCH_TIMEOUT + 5,
            )
        except asyncio.TimeoutError:
            error = f"Tool timed out after {FETCH_TIMEOUT + 5}s"
        except Exception as exc:
            error = str(exc)
            logger.warning("Tool %s error: %s", req.tool_name, exc)

    elapsed_ms = (time.monotonic() - start) * 1000

    await producer.publish(
        KafkaTopic.TOOL_RESULTS,
        ToolResultEvent(
            trace_id=req.trace_id,
            task_id=req.task_id,
            session_id=req.session_id,
            tool_name=req.tool_name,
            tool_output=output,
            error=error,
            execution_ms=elapsed_ms,
            step_index=req.step_index,
        ),
        key=req.task_id,
    )
    logger.info("Tool %s task=%s ms=%.1f error=%s", req.tool_name, req.task_id, elapsed_ms, error)


async def _dispatch(tool_name: str, tool_input: dict[str, Any]) -> str:
    if tool_name == "calculate":
        return _tool_calculate(tool_input.get("expression", ""))
    elif tool_name == "search":
        return await _tool_search(tool_input.get("query", ""))
    elif tool_name == "fetch_url":
        return await _tool_fetch_url(tool_input.get("url", ""))
    elif tool_name == "get_datetime":
        return _tool_get_datetime(tool_input.get("timezone", "UTC"))
    else:
        raise ValueError(f"Unknown tool: {tool_name}")


# ── tool: calculate ───────────────────────────────────────────────────────────

_SAFE_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def _eval_node(node: ast.AST) -> float:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return float(node.value)
    if isinstance(node, ast.BinOp) and type(node.op) in _SAFE_OPS:
        return _SAFE_OPS[type(node.op)](_eval_node(node.left), _eval_node(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _SAFE_OPS:
        return _SAFE_OPS[type(node.op)](_eval_node(node.operand))
    raise ValueError(f"Disallowed expression node: {type(node).__name__}")


def _tool_calculate(expression: str) -> str:
    if not expression.strip():
        raise ValueError("Empty expression")
    if len(expression) > 200:
        raise ValueError("Expression too long")
    tree = ast.parse(expression.strip(), mode="eval")
    result = _eval_node(tree.body)
    return str(result)


# ── tool: search ──────────────────────────────────────────────────────────────

_NEWS_KWS = {"news", "headline", "latest", "today", "current event", "happening",
             "geopolit", "recent", "this week", "breaking", "update", "report"}


async def _tool_search(query: str) -> str:
    """Routes to Google News RSS for news/current events, DuckDuckGo for factual queries."""
    if not query.strip():
        raise ValueError("Empty search query")
    q_lower = query.lower()
    if any(k in q_lower for k in _NEWS_KWS):
        return await _search_news(query)
    return await _search_ddg(query)


async def _search_news(query: str) -> str:
    """Google News RSS — free, no API key, returns live headlines."""
    import xml.etree.ElementTree as ET
    from urllib.parse import quote_plus

    url = (
        f"https://news.google.com/rss/search"
        f"?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible)"})
        resp.raise_for_status()

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError:
        return await _search_ddg(query)  # fallback on bad XML

    items = root.findall(".//item")[:SEARCH_MAX_RESULTS + 2]
    lines = []
    for item in items:
        title = (item.findtext("title") or "").split(" - ")[0].strip()
        source = item.findtext("source") or ""
        pub = item.findtext("pubDate") or ""
        if title:
            lines.append(f"• {title}{' (' + source + ')' if source else ''}{' [' + pub[:16] + ']' if pub else ''}")

    return "\n".join(lines) if lines else "No news results found."


async def _search_ddg(query: str) -> str:
    """DuckDuckGo Instant Answer API — best for factual/encyclopaedic queries."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            "https://api.duckduckgo.com/",
            params={"q": query.strip(), "format": "json", "no_redirect": "1", "no_html": "1"},
            headers={"User-Agent": "AI-Backend-Tool/1.0"},
        )
        resp.raise_for_status()
        data = resp.json()

    lines: list[str] = []
    if data.get("AbstractText"):
        lines.append(f"Summary: {data['AbstractText']}\nSource: {data.get('AbstractURL', '')}")
    for topic in data.get("RelatedTopics", [])[:SEARCH_MAX_RESULTS]:
        if isinstance(topic, dict) and topic.get("Text"):
            lines.append(f"- {topic['Text'][:200]}")

    return "\n\n".join(lines) if lines else "No results found for this query."


# ── tool: fetch_url ───────────────────────────────────────────────────────────

def _is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = parsed.hostname or ""
        # resolve and block private/loopback ranges
        ip_str = socket.gethostbyname(hostname)
        addr = ipaddress.ip_address(ip_str)
        return not (addr.is_private or addr.is_loopback or addr.is_reserved or addr.is_link_local)
    except Exception:
        return False


async def _tool_fetch_url(url: str) -> str:
    if not url.strip():
        raise ValueError("Empty URL")
    if not _is_safe_url(url):
        raise ValueError(f"URL not allowed (private/loopback/invalid): {url}")
    async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, follow_redirects=True) as client:
        resp = await client.get(url, headers={"User-Agent": "AI-Backend-Tool/1.0"})
        resp.raise_for_status()
        # return first 2000 chars to avoid flooding the context
        return resp.text[:2000]


# ── tool: get_datetime ────────────────────────────────────────────────────────

def _tool_get_datetime(tz_name: str = "UTC") -> str:
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo(tz_name)
        now = datetime.now(zone)
        return now.strftime("%Y-%m-%d %H:%M:%S %Z (UTC%z)")
    except Exception:
        # zoneinfo not available or bad tz name — return UTC with offset hint
        now = datetime.now(timezone.utc)
        return (
            f"{now.strftime('%Y-%m-%d %H:%M:%S UTC')}  "
            f"(could not convert to '{tz_name}' — provide an IANA name like 'America/Chicago')"
        )


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "tool-runtime"})


@app.get("/ready")
async def ready() -> JSONResponse:
    return JSONResponse({"status": "ready", "tools": list(_SAFE_OPS.keys())})

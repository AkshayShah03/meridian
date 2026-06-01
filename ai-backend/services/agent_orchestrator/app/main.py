from __future__ import annotations

"""
Agent Orchestrator — Kafka-native ReAct agent loop (no Temporal/LangGraph required).

Architecture:
  Consumes:   agent-tasks, tool-results
  Publishes:  tool-requests, llm-responses (final answer), agent-steps (audit)

ReAct loop per task:
  1. Build prompt with available tools + conversation history
  2. Call LLM (Ollama) to get Thought/Action or Final Answer
  3. If Action → publish ToolRequestEvent, wait on in-memory Queue for result
  4. Append Observation, loop (max_iterations safety cap)
  5. On Final Answer → publish to llm-responses so ws-gateway delivers to user

State lives in Redis so it survives a pod restart (tasks re-queue via Kafka).
"""

import asyncio
import json
import logging
import os
import re
import time
from typing import Any

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from shared.schemas.events import (
    AgentStepEvent,
    AgentTaskEvent,
    KafkaTopic,
    LLMResponseChunkEvent,
    ToolRequestEvent,
    ToolResultEvent,
)
from shared.schemas.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("agent-orchestrator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
TOOL_TIMEOUT = float(os.getenv("TOOL_TIMEOUT_SECONDS", "30"))
STATE_TTL = 60 * 60  # 1h

app = FastAPI(title="agent-orchestrator")
tracer = None
redis_client: aioredis.Redis | None = None
task_consumer: KafkaEventConsumer | None = None
tool_result_consumer: KafkaEventConsumer | None = None
producer: KafkaEventProducer | None = None
_task_loop: asyncio.Task | None = None
_result_loop: asyncio.Task | None = None

# task_id → asyncio.Queue for tool results (in-memory, per pod)
_pending_tools: dict[str, asyncio.Queue] = {}

TOOL_DESCRIPTIONS = {
    "calculate": (
        'Evaluate a math expression safely. '
        'Input: {"expression": "1337 * 42"}  '
        'Supports: +, -, *, /, **, //, %. No functions.'
    ),
    "search": (
        'Search the web for ANY real-time or factual information: news, current events, '
        'prices, weather, sports scores, definitions, facts. '
        'Use this whenever you need information you might not know or that changes over time. '
        'Input: {"query": "latest AI news today"}'
    ),
    "fetch_url": (
        'Fetch and return the text content of any public URL. '
        'Input: {"url": "https://example.com/article"}'
    ),
    "get_datetime": (
        'Get the current date and time in any timezone. '
        'Input: {"timezone": "America/Chicago"} for CDT/CST, '
        '{"timezone": "America/New_York"} for EDT/EST, '
        '{"timezone": "UTC"} for UTC. '
        'Always use this tool when asked about the current time or date.'
    ),
}

REACT_SYSTEM = """\
You are a tool-calling agent. You MUST call a tool before answering. Never answer from memory.

TOOLS:
{tools}

STRICT FORMAT — copy exactly, no prose:

Thought: <why you need a tool>
Action: <tool_name>
Action Input: {{"key": "value"}}

After the Observation line appears, continue:

Thought: <what the result means>
Action: <next tool OR skip to Final Answer>
Action Input: {{"key": "value"}}

When done:

Thought: I have enough to answer.
Final Answer: <your answer>

---EXAMPLES---

Q: What are today's top headlines?
Thought: I need to search the web for current headlines.
Action: search
Action Input: {{"query": "top headlines today"}}
Observation: Title: ... Summary: ...
Thought: I have headlines to report.
Final Answer: Here are today's top headlines: ...

Q: What time is it in CDT?
Thought: I need the current time in Central Daylight Time.
Action: get_datetime
Action Input: {{"timezone": "America/Chicago"}}
Observation: 2026-05-29 15:22:18 CDT (UTC-0500)
Thought: I have the time.
Final Answer: The current CDT time is 3:22 PM.

Q: What is 144 divided by 12?
Thought: I need to calculate this.
Action: calculate
Action Input: {{"expression": "144 / 12"}}
Observation: 12.0
Thought: Done.
Final Answer: 144 divided by 12 is 12.

Q: What is the latest AI news?
Thought: I need to search for recent AI news.
Action: search
Action Input: {{"query": "latest AI news today 2026"}}
Observation: Title: ... Summary: ...
Thought: I have results.
Final Answer: Here is the latest AI news: ...

---NOW ANSWER---
"""

REACT_HUMAN = """\
Q: {task}
{history}
"""


@app.on_event("startup")
async def startup() -> None:
    global tracer, redis_client, task_consumer, tool_result_consumer, producer
    global _task_loop, _result_loop

    setup_telemetry("agent-orchestrator")
    tracer = get_tracer("agent-orchestrator")
    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)

    producer = KafkaEventProducer(KAFKA_BOOTSTRAP)
    await producer.start()

    task_consumer = KafkaEventConsumer(
        topics=[KafkaTopic.AGENT_TASKS],
        group_id="agent-orchestrator-tasks",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await task_consumer.start()

    tool_result_consumer = KafkaEventConsumer(
        topics=[KafkaTopic.TOOL_RESULTS],
        group_id="agent-orchestrator-results",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await tool_result_consumer.start()

    _task_loop = asyncio.create_task(_consume_tasks())
    _result_loop = asyncio.create_task(_consume_tool_results())
    logger.info("agent-orchestrator ready")


@app.on_event("shutdown")
async def shutdown() -> None:
    for t in (_task_loop, _result_loop):
        if t:
            t.cancel()
    if producer:
        await producer.stop()
    for c in (task_consumer, tool_result_consumer):
        if c:
            await c.stop()
    if redis_client:
        await redis_client.aclose()


# ── Kafka consumers ───────────────────────────────────────────────────────────

async def _consume_tasks() -> None:
    async for payload, _headers in task_consumer.messages():
        try:
            event = AgentTaskEvent.model_validate(payload)
            asyncio.create_task(_run_agent(event))
        except Exception:
            logger.exception("Task parse error: %s", payload)


async def _consume_tool_results() -> None:
    async for payload, _headers in tool_result_consumer.messages():
        try:
            result = ToolResultEvent.model_validate(payload)
            q = _pending_tools.get(result.task_id)
            if q:
                await q.put(result)
        except Exception:
            logger.exception("Tool result parse error")


# ── LLM call (non-streaming for ReAct parsing) ───────────────────────────────

async def _call_llm(model: str, messages: list[dict[str, Any]]) -> str:
    async with httpx.AsyncClient(timeout=90.0) as client:
        resp = await client.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json={
                "model": model,
                "messages": messages,
                "stream": False,
                "options": {"temperature": 0},  # deterministic — critical for format adherence
            },
        )
        resp.raise_for_status()
        content = resp.json()["message"]["content"]
        logger.debug("LLM raw output: %s", content[:300].replace("\n", " | "))
        return content


# ── ReAct output parser ───────────────────────────────────────────────────────

_thought_re = re.compile(r"Thought:\s*(.*?)(?=Action:|Final Answer:|$)", re.DOTALL | re.IGNORECASE)
_action_re = re.compile(r"Action:\s*(\w+)", re.IGNORECASE)
_input_re = re.compile(r"Action Input:\s*(\{.*?\})", re.DOTALL)
_final_re = re.compile(r"Final Answer:\s*(.*?)$", re.DOTALL | re.IGNORECASE)


def _parse_react(text: str, task: str = "", available_tools: list[str] | None = None) -> dict[str, Any]:
    available_tools = available_tools or []

    # 1. Explicit Final Answer
    final_m = _final_re.search(text)
    if final_m:
        return {"type": "final", "answer": final_m.group(1).strip()}

    # 2. Properly formatted Action block
    thought_m = _thought_re.search(text)
    action_m = _action_re.search(text)
    input_m = _input_re.search(text)

    if action_m:
        tool_input: dict[str, Any] = {}
        if input_m:
            try:
                tool_input = json.loads(input_m.group(1))
            except json.JSONDecodeError:
                pass
        return {
            "type": "action",
            "thought": thought_m.group(1).strip() if thought_m else "",
            "action": action_m.group(1).strip(),
            "action_input": tool_input,
        }

    # 3. Intent-based recovery — model answered in prose instead of tool format.
    #    Detect what it was trying to do and force the right tool.
    t = text.lower()
    task_l = task.lower()

    _search_kws = {"news", "headline", "latest", "current events", "search", "look up",
                   "find information", "recent", "today", "happening", "geopolit"}
    _time_kws   = {"time", "clock", "cdt", "cst", "est", "edt", "pst", "pdt", "utc",
                   "timezone", "what time"}
    _calc_kws   = {"calculat", "compute", "math", "multiply", "divide", "add ", "subtract"}

    combined = t + " " + task_l

    if "get_datetime" in available_tools and any(k in combined for k in _time_kws):
        tz = "UTC"
        if any(k in combined for k in ("cdt", "cst", "chicago", "central")):
            tz = "America/Chicago"
        elif any(k in combined for k in ("edt", "est", "new york", "eastern")):
            tz = "America/New_York"
        elif any(k in combined for k in ("pdt", "pst", "pacific", "los angeles")):
            tz = "America/Los_Angeles"
        elif any(k in combined for k in ("mdt", "mst", "mountain", "denver")):
            tz = "America/Denver"
        logger.info("Intent recovery: forced get_datetime tz=%s", tz)
        return {"type": "action", "thought": "Detecting time request; using get_datetime.",
                "action": "get_datetime", "action_input": {"timezone": tz}}

    if "search" in available_tools and any(k in combined for k in _search_kws):
        query = task  # fall back to the original task as the search query
        logger.info("Intent recovery: forced search query=%r", query[:80])
        return {"type": "action", "thought": "Detecting search request; using search.",
                "action": "search", "action_input": {"query": query}}

    if "calculate" in available_tools and any(k in combined for k in _calc_kws):
        return {"type": "action", "thought": "Detecting calculation request.",
                "action": "calculate", "action_input": {"expression": task}}

    # 4. Absolute fallback — return as final answer
    logger.warning("ReAct parse fell through to final-answer fallback: %s", text[:120])
    return {"type": "final", "answer": text.strip()}


# ── agent state in Redis ──────────────────────────────────────────────────────

def _state_key(task_id: str) -> str:
    return f"agent_state:{task_id}"


async def _save_state(task_id: str, state: dict[str, Any]) -> None:
    await redis_client.setex(_state_key(task_id), STATE_TTL, json.dumps(state))


async def _load_state(task_id: str) -> dict[str, Any] | None:
    raw = await redis_client.get(_state_key(task_id))
    return json.loads(raw) if raw else None


# ── task pre-router ───────────────────────────────────────────────────────────
# Small models (3B) skip tools even when prompted. This detector classifies the
# task from keywords and returns (tool_name, input_dict) BEFORE the LLM is called.
# The LLM is then only used to synthesise the tool result into natural language.

_TZ_MAP = {
    ("cdt", "cst", "chicago", "central"): "America/Chicago",
    ("edt", "est", "eastern", "new york"): "America/New_York",
    ("pdt", "pst", "pacific", "los angeles", "seattle"): "America/Los_Angeles",
    ("mdt", "mst", "mountain", "denver"): "America/Denver",
    ("gmt", "london", "uk", "bst"): "Europe/London",
    ("cet", "paris", "berlin", "rome"): "Europe/Paris",
    ("ist", "india", "mumbai"): "Asia/Kolkata",
    ("jst", "japan", "tokyo"): "Asia/Tokyo",
    ("aest", "sydney", "australia"): "Australia/Sydney",
}

_MATH_RE = re.compile(
    r"[\d,]+\.?\d*\s*[\+\-\*\/\%\^]+\s*[\d,]+|"   # e.g. 1337 * 42
    r"\(\s*[\d,\.\+\-\*\/\%\^ ]+\s*\)"              # e.g. (144 / 12)
)


def _extract_math_expr(task: str) -> str | None:
    """Try to pull a numeric expression out of the task string."""
    # strip common prefixes
    cleaned = re.sub(
        r"^(what is|what's|calculate|compute|evaluate|solve|find)\s+",
        "", task.strip(), flags=re.IGNORECASE,
    ).rstrip("?.!")
    if _MATH_RE.search(cleaned):
        return cleaned.replace(",", "")   # remove thousands-separators
    return None


def _detect_forced_tool(task: str, available: list[str]) -> tuple[str, dict] | None:
    t = task.lower()

    # ── datetime ──────────────────────────────────────────────────────────────
    time_kws = {"time", "clock", "what time", "current time", "hour", "minute",
                "date", "today's date", "cdt", "cst", "est", "edt", "pst", "pdt",
                "mst", "mdt", "utc", "timezone", "o'clock"}
    if "get_datetime" in available and any(k in t for k in time_kws):
        tz = "UTC"
        for kws, zone in _TZ_MAP.items():
            if any(k in t for k in kws):
                tz = zone
                break
        return "get_datetime", {"timezone": tz}

    # ── calculate — check before search so "what is X*Y" doesn't hit search ──
    if "calculate" in available:
        expr = _extract_math_expr(task)
        if expr:
            return "calculate", {"expression": expr}

    # ── search ────────────────────────────────────────────────────────────────
    search_kws = {"news", "headline", "latest", "current event", "happening",
                  "geopolit", "recent", "today's", "this week", "stock", "weather",
                  "search for", "look up", "find out", "what happened", "who won",
                  "score", "election", "war", "conflict", "market", "politics"}
    if "search" in available and any(k in t for k in search_kws):
        return "search", {"query": task}

    return None


async def _execute_tool_http(tool_name: str, tool_input: dict[str, Any], session_id: str) -> str:
    """Call tool-runtime's dispatch logic inline via a direct Kafka round-trip substitute.
    We re-use the tool_runtime dispatch logic by importing it would create a circular dep,
    so instead we publish a ToolRequestEvent and wait — same as the agent loop does."""
    return f"[pre-router will trigger tool via Kafka — see _run_agent]"


# ── main ReAct loop ───────────────────────────────────────────────────────────

async def _run_agent(event: AgentTaskEvent) -> None:
    task_id = event.event_id

    with span(tracer, "agent.run", trace_id=event.trace_id,
              attributes={"task_id": task_id, "session_id": event.session_id}):

        tools_text = "\n".join(
            f"- {name}: {desc}"
            for name, desc in TOOL_DESCRIPTIONS.items()
            if name in event.available_tools
        )
        tool_names = ", ".join(event.available_tools)
        history_lines: list[str] = []
        state = {"task": event.task, "steps": [], "status": "running"}
        await _save_state(task_id, state)

        # ── pre-router: detect tool intent before calling LLM ────────────────
        forced = _detect_forced_tool(event.task, event.available_tools)
        if forced:
            forced_tool, forced_input = forced
            logger.info("Pre-router: forcing %s for task=%s", forced_tool, task_id)
            # inject as if the LLM chose it, so the loop handles execution normally
            history_lines.append(
                f"Thought: Task requires {forced_tool}.\n"
                f"Action: {forced_tool}\n"
                f"Action Input: {json.dumps(forced_input)}"
            )

        for iteration in range(event.max_iterations):
            # On iterations where the pre-router already wrote an Action, skip LLM
            last = history_lines[-1] if history_lines else ""
            if iteration == 0 and forced and "Action:" in last and "Observation:" not in last:
                parsed = {
                    "type": "action",
                    "thought": f"Pre-router detected {forced_tool} intent.",
                    "action": forced_tool,
                    "action_input": forced_input,
                }
            else:
                history_text = "\n".join(history_lines)
                messages = [
                    {"role": "system", "content": REACT_SYSTEM.format(tools=tools_text, tool_names=tool_names)},
                    {"role": "user", "content": f"Q: {event.task}\n{history_text}"},
                ]
                try:
                    llm_output = await _call_llm(event.model, messages)
                except Exception as exc:
                    logger.exception("LLM call failed task=%s", task_id)
                    await _publish_final(event, f"[Agent error: {exc}]", task_id)
                    return
                parsed = _parse_react(llm_output, task=event.task, available_tools=event.available_tools)

            if parsed["type"] == "final":
                answer = parsed["answer"]
                await producer.publish(
                    KafkaTopic.AUDIT_LOG,
                    AgentStepEvent(
                        trace_id=event.trace_id,
                        task_id=task_id,
                        session_id=event.session_id,
                        step_index=iteration,
                        thought=answer[:200],
                        is_final=True,
                        final_answer=answer,
                    ),
                    key=event.session_id,
                )
                await _publish_final(event, answer, task_id)
                state["status"] = "complete"
                await _save_state(task_id, state)
                logger.info("Agent done task=%s steps=%d", task_id, iteration + 1)
                return

            # it's an action
            thought = parsed.get("thought", "")
            tool_name = parsed["action"]
            tool_input = parsed["action_input"]

            if tool_name not in event.available_tools:
                history_lines.append(f"Thought: {thought}\nAction: {tool_name}\nObservation: Tool '{tool_name}' not available.")
                continue

            history_lines.append(f"Thought: {thought}\nAction: {tool_name}\nAction Input: {json.dumps(tool_input)}")

            # publish tool request and wait for result
            tool_req = ToolRequestEvent(
                trace_id=event.trace_id,
                task_id=task_id,
                session_id=event.session_id,
                user_id=event.user_id,
                tool_name=tool_name,
                tool_input=tool_input,
                step_index=iteration,
            )
            q: asyncio.Queue = asyncio.Queue(maxsize=1)
            _pending_tools[task_id] = q
            await producer.publish(KafkaTopic.TOOL_REQUESTS, tool_req, key=task_id)

            try:
                result: ToolResultEvent = await asyncio.wait_for(q.get(), timeout=TOOL_TIMEOUT)
                observation = result.tool_output if not result.error else f"Error: {result.error}"
            except asyncio.TimeoutError:
                observation = f"Tool '{tool_name}' timed out after {TOOL_TIMEOUT}s"
            finally:
                _pending_tools.pop(task_id, None)

            history_lines.append(f"Observation: {observation}")

            await producer.publish(
                KafkaTopic.AUDIT_LOG,
                AgentStepEvent(
                    trace_id=event.trace_id,
                    task_id=task_id,
                    session_id=event.session_id,
                    step_index=iteration,
                    thought=thought,
                    action=tool_name,
                    action_input=tool_input,
                    observation=observation,
                ),
                key=event.session_id,
            )

        # hit max iterations
        await _publish_final(event, "I reached my reasoning limit without a complete answer. Please try rephrasing.", task_id)
        state["status"] = "max_iterations"
        await _save_state(task_id, state)


async def _publish_final(event: AgentTaskEvent, answer: str, task_id: str) -> None:
    words = answer.split(" ")
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
                provider="agent",
                model_used=event.model,
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
            provider="agent",
            model_used=event.model,
        ),
        key=event.session_id,
    )


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "agent-orchestrator"})


@app.get("/ready")
async def ready() -> JSONResponse:
    active = len(_pending_tools)
    return JSONResponse({"status": "ready", "active_agent_tasks": active})

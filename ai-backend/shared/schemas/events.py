from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class KafkaTopic(str, Enum):
    USER_EVENTS = "user-events"
    LLM_REQUESTS = "llm-requests"
    LLM_RESPONSES = "llm-responses"
    AUDIT_LOG = "audit-log"
    NOTIFICATIONS = "notifications"
    AGENT_TASKS = "agent-tasks"
    TOOL_REQUESTS = "tool-requests"
    TOOL_RESULTS = "tool-results"
    EVAL_REQUESTS = "eval-requests"
    EVAL_RESULTS = "eval-results"


class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    model_config = {"arbitrary_types_allowed": True}


# ── core chat events ──────────────────────────────────────────────────────────

class UserMessageEvent(EventEnvelope):
    event_type: str = "user.message"
    session_id: str
    user_id: str
    tenant_id: str
    message: str
    model: str = "mistral:7b"
    tenant_tier: str = "free"   # "free" | "premium"
    agent_mode: bool = False     # route to agent-orchestrator instead of direct LLM


class LLMRequestEvent(EventEnvelope):
    event_type: str = "llm.request"
    session_id: str
    user_id: str
    tenant_id: str
    tenant_tier: str = "free"
    messages: list[dict[str, Any]]
    model: str = "mistral:7b"
    temperature: float = 0.7
    max_tokens: int = 2048
    structured_schema: dict[str, Any] | None = None  # JSON schema for instructor


class LLMResponseChunkEvent(EventEnvelope):
    event_type: str = "llm.response.chunk"
    session_id: str
    user_id: str
    chunk_index: int
    token: str
    is_final: bool = False
    ttft_ms: float | None = None
    provider: str = "ollama"         # which provider served this
    model_used: str = "mistral:7b"   # actual model (may differ from requested)
    cache_hit: bool = False


# ── agent events ──────────────────────────────────────────────────────────────

class AgentTaskEvent(EventEnvelope):
    event_type: str = "agent.task"
    session_id: str
    user_id: str
    tenant_id: str
    task: str
    available_tools: list[str] = Field(default_factory=lambda: ["calculate", "search", "fetch_url", "get_datetime"])
    model: str = "mistral:7b"
    max_iterations: int = 8


class AgentStepEvent(EventEnvelope):
    event_type: str = "agent.step"
    task_id: str
    session_id: str
    step_index: int
    thought: str
    action: str | None = None
    action_input: dict[str, Any] = Field(default_factory=dict)
    observation: str | None = None
    is_final: bool = False
    final_answer: str | None = None


# ── tool events ───────────────────────────────────────────────────────────────

class ToolRequestEvent(EventEnvelope):
    event_type: str = "tool.request"
    task_id: str
    session_id: str
    user_id: str
    tool_name: str
    tool_input: dict[str, Any]
    step_index: int


class ToolResultEvent(EventEnvelope):
    event_type: str = "tool.result"
    task_id: str
    session_id: str
    tool_name: str
    tool_output: str
    error: str | None = None
    execution_ms: float
    step_index: int


# ── evaluation events ─────────────────────────────────────────────────────────

class EvalRequestEvent(EventEnvelope):
    event_type: str = "eval.request"
    session_id: str
    user_id: str
    tenant_id: str
    messages: list[dict[str, Any]]
    response: str
    model: str
    provider: str
    ttft_ms: float | None = None
    total_tokens: int = 0


class EvalResultEvent(EventEnvelope):
    event_type: str = "eval.result"
    session_id: str
    scores: dict[str, float]   # e.g. {"coherence": 0.9, "relevance": 0.8}
    overall_score: float
    issues: list[str] = Field(default_factory=list)


# ── observability events ──────────────────────────────────────────────────────

class CacheHitEvent(EventEnvelope):
    event_type: str = "cache.hit"
    session_id: str
    cache_key: str
    saved_tokens: int


class ModelRoutingEvent(EventEnvelope):
    event_type: str = "model.routing"
    session_id: str
    requested_model: str
    routed_to_model: str
    provider: str
    reason: str


# ── infra events ──────────────────────────────────────────────────────────────

class AuditEvent(EventEnvelope):
    event_type: str = "audit"
    user_id: str
    tenant_id: str
    action: str
    resource: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class NotificationEvent(EventEnvelope):
    event_type: str = "notification"
    user_id: str
    tenant_id: str
    channel: str
    payload: dict[str, Any] = Field(default_factory=dict)

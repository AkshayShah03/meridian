# Meridian

An event-driven AI assistant backend with multi-layer memory, tool use, prompt caching, distributed tracing, and automated response quality scoring. Built as a set of independent microservices communicating exclusively through Apache Kafka.

## What it is

Meridian is a real-time chat backend that routes every factual query to its authoritative source:

- Live news headlines via Google News RSS
- Exact arithmetic via a safe AST-based calculation engine
- Real timezone data via the IANA `tzdata` database
- Web page content via SSRF-protected HTTP fetch

Generative inference (Ollama, with optional Anthropic premium fallback) is used only to synthesise natural-language answers around tool outputs. Anything that can be verified is verified — never hallucinated.

## Architecture

```
                      ┌─────────────────┐
                      │     Browser     │
                      └────────┬────────┘
                               │ WebSocket
                      ┌────────▼────────┐
                      │ Assistant       │
                      │ Gateway         │
                      └────────┬────────┘
                               │
              ┌────────────────┴────────────────┐
              │           Apache Kafka          │
              │   (user-events, llm-requests,   │
              │    llm-responses, agent-tasks,  │
              │    tool-requests/results,       │
              │    eval-requests, audit-log)    │
              └────────────────┬────────────────┘
                               │
   ┌───────────┬───────────────┼───────────────┬────────────┐
   ▼           ▼               ▼               ▼            ▼
 Memory   Inference     Orchestrator      Tool         Quality
 Service  Gateway       (ReAct loop)      Runtime      Service
   │         │               │                │            │
   ▼         ▼               ▼                ▼            ▼
 Redis    Ollama          Redis           Search        Postgres
pgvector  (local)        (state)         Calc /
                                         Time /
                                         Fetch
```

Eight application services, eight Kafka topics, zero direct HTTP calls between services.

## Services

| Service | Role | Stack |
|---|---|---|
| `auth-service` | JWT-based registration, login, refresh, validation | FastAPI, asyncpg, bcrypt, PyJWT |
| `ws-gateway` | WebSocket endpoint, response router | FastAPI, aiokafka |
| `memory-service` | Conversation history (Redis), semantic recall (pgvector), episodic summary | FastAPI, aiokafka, redis, asyncpg, tiktoken |
| `inference-gateway` | Multi-model routing, prompt cache, Ollama streaming, structured output | FastAPI, aiokafka, httpx, instructor, anthropic |
| `agent-orchestrator` | ReAct loop with keyword-based pre-router for small models | FastAPI, aiokafka, httpx |
| `tool-runtime` | Calculator (AST), DuckDuckGo / Google News search, URL fetch, timezone-aware datetime | FastAPI, aiokafka, httpx, tzdata |
| `eval-service` | Heuristic scoring (coherence, relevance, completeness, latency, safety) | FastAPI, aiokafka, asyncpg |
| `notify-service` | Notification dispatch consumer | FastAPI, aiokafka |

## Infrastructure

| Component | Purpose |
|---|---|
| Apache Kafka + Zookeeper | Event bus for all inter-service communication |
| Redis | Conversation history, prompt cache, agent state |
| Postgres (pgvector) | Users, sessions, semantic embeddings, audit log, quality scores |
| Ollama | Local LLM inference (llama3.2:3b, mistral:7b, nomic-embed-text) |
| OpenTelemetry Collector | Trace + metric pipeline |
| Jaeger | Distributed trace storage and UI |
| Prometheus | Metrics storage |
| Grafana | Dashboards |
| Langfuse | LLM-specific observability |

## Getting started

### Prerequisites

- Docker Desktop (or Docker Engine with Compose v2)
- ~10 GB free disk space for Docker images and Ollama models
- 16 GB RAM recommended

### First boot

```bash
cd ai-backend
docker compose up --build
```

First boot downloads:
- Container images (~3 GB)
- Ollama models: `llama3.2:3b` (~2 GB), `mistral:7b` (~4 GB), `nomic-embed-text` (~300 MB)

Wait for all containers to report healthy (`docker compose ps`). Model downloads run in a background sidecar (`ollama-init`).

### Try it

Open `ai-backend/demo.html` in a browser. Sign in with any credentials — the *Create account* button auto-registers. Send a message.

For the full feature tour, the sidebar's *Try these* section walks through:
- Direct conversation
- Multi-turn memory recall
- Cache performance test (auto-runs in a side panel)
- Live calculation, news search, and timezone lookup via research mode

## Configuration

All services read from environment variables. Defaults work for local Docker Compose. See `.env.example` for the full list.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka cluster address |
| `REDIS_URL` | `redis://redis:6379` | Redis connection string |
| `DATABASE_URL` | `postgresql://postgres:postgres@postgres:5432/aibackend` | Postgres DSN |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | Ollama API endpoint |
| `JWT_SECRET` | `dev-secret-change-in-prod` | HS256 signing key |
| `ANTHROPIC_API_KEY` | (empty) | Optional; enables Anthropic routing for premium tenants |
| `MAX_CONTEXT_TOKENS` | `8000` | Token budget for assembled context |
| `MAX_CONCURRENT_LLM_REQUESTS` | `20` | Semaphore size on inference gateway |
| `PROMPT_CACHE_TTL` | `3600` | Redis cache TTL in seconds |

## Endpoints

### Public

| Service | Port | Endpoint | Purpose |
|---|---|---|---|
| auth-service | 8001 | `POST /register`, `POST /login`, `POST /refresh`, `GET /validate` | Identity |
| ws-gateway | 8002 | `WS /ws/{session_id}?token=<jwt>` | Chat |
| inference-gateway | 8003 | `GET /cache`, `GET /routing-table` | Cache and routing introspection |
| memory-service | 8004 | `GET /sessions`, `GET /history/{session_id}` | Memory introspection |
| eval-service | 8007 | `GET /evals?limit=N` | Quality scores |

### Observability

| Service | URL |
|---|---|
| Jaeger UI | http://localhost:16686 |
| Grafana | http://localhost:3000 (`admin` / `admin`) |
| Prometheus | http://localhost:9090 |
| Langfuse | http://localhost:3001 |

## WebSocket protocol

Connect:
```
ws://localhost:8002/ws/<session_uuid>?token=<jwt>
```

Send:
```json
{
  "message": "Your question here",
  "model": "llama3.2:3b",
  "agent_mode": false
}
```

Receive (one per token, plus a final sentinel):
```json
{
  "session_id": "...",
  "chunk_index": 0,
  "token": "Hello",
  "is_final": false,
  "provider": "ollama",
  "model_used": "llama3.2:3b",
  "cache_hit": false
}
```

The final chunk has `is_final: true` and a populated `ttft_ms` (time-to-first-token in milliseconds).

## Kafka topics

| Topic | Producer | Consumer | Payload |
|---|---|---|---|
| `user-events` | ws-gateway | memory-service | `UserMessageEvent` |
| `llm-requests` | memory-service | inference-gateway | `LLMRequestEvent` |
| `llm-responses` | inference-gateway / agent-orchestrator | ws-gateway, memory-service | `LLMResponseChunkEvent` |
| `agent-tasks` | memory-service | agent-orchestrator | `AgentTaskEvent` |
| `tool-requests` | agent-orchestrator | tool-runtime | `ToolRequestEvent` |
| `tool-results` | tool-runtime | agent-orchestrator | `ToolResultEvent` |
| `eval-requests` | inference-gateway | eval-service | `EvalRequestEvent` |
| `audit-log` | all | (extensible) | `ModelRoutingEvent`, `AgentStepEvent`, `AuditEvent` |

All event schemas live in `ai-backend/shared/schemas/events.py` as Pydantic v2 models.

## Kubernetes

Manifests for a production deployment are in `ai-backend/infra/k8s/base/`:

- `namespace.yaml` — namespace with Istio injection label
- `configmap.yaml` — non-secret config + opaque secret template
- `services.yaml` — ClusterIP services; ws-gateway exposed via LoadBalancer
- `deployments.yaml` — Deployments with liveness/readiness probes and resource limits
- `infra/keda/scaled-objects.yaml` — KEDA `ScaledObject`s for autoscaling on Kafka consumer lag, plus an HPA for ws-gateway

Apply with:
```bash
kubectl apply -f ai-backend/infra/k8s/base/
kubectl apply -f ai-backend/infra/keda/
```

(Requires KEDA installed in the cluster: `kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.14.0/keda-2.14.0.yaml`)

## Testing

End-to-end script:
```bash
cd ai-backend
pip install httpx websockets
python tests/test_e2e.py
```

Walks through: health checks, registration, login, direct chat, multi-turn memory, cache behaviour, agent tool calls.

## Repository layout

```
ai-backend/
├── shared/
│   ├── schemas/
│   │   ├── events.py            # All Kafka event Pydantic models
│   │   └── kafka.py             # Producer / Consumer wrappers
│   └── telemetry/
│       └── otel.py              # OpenTelemetry setup
├── services/
│   ├── auth_service/
│   ├── ws_gateway/
│   ├── memory_service/
│   ├── inference_gateway/
│   ├── agent_orchestrator/
│   ├── tool_runtime/
│   ├── eval_service/
│   └── notify_service/
├── infra/
│   ├── k8s/base/                # Kubernetes manifests
│   ├── keda/                    # Autoscaler definitions
│   ├── otel-config.yaml         # OTel Collector pipeline
│   └── prometheus.yml           # Prometheus scrape config
├── scripts/
│   ├── init.sql                 # Postgres schema
│   └── create_dbs.sh            # Extra-database bootstrap (Langfuse)
├── tests/
│   └── test_e2e.py              # End-to-end integration test
├── Dockerfile                   # Multi-stage, single image per service
├── docker-compose.yml           # Local development stack
├── requirements.txt             # Shared Python dependencies
├── demo.html                    # Browser UI for testing every feature
└── demo.py                      # CLI demo walkthrough
```

## License

MIT — see `LICENSE`.

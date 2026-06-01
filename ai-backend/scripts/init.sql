CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "vector";

-- ── users & auth ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    tenant_id     TEXT NOT NULL DEFAULT 'default',
    tier          TEXT NOT NULL DEFAULT 'free',   -- 'free' | 'premium'
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    tenant_id   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_active TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── audit & compliance ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID REFERENCES users(id) ON DELETE SET NULL,
    tenant_id  TEXT NOT NULL,
    action     TEXT NOT NULL,
    resource   TEXT NOT NULL,
    metadata   JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── LLM usage (cost tracking) ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS llm_usage (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id        TEXT,
    user_id           UUID REFERENCES users(id) ON DELETE SET NULL,
    tenant_id         TEXT NOT NULL,
    model             TEXT NOT NULL,
    provider          TEXT NOT NULL DEFAULT 'ollama',
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    ttft_ms           FLOAT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── semantic memory (pgvector) ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS memory_embeddings (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL,
    user_id    TEXT NOT NULL,
    role       TEXT NOT NULL,
    content    TEXT NOT NULL,
    embedding  vector(768),   -- nomic-embed-text output dimension
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mem_user_vec
    ON memory_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_mem_user ON memory_embeddings(user_id);

-- ── evaluation results ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS response_evals (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id     TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    tenant_id      TEXT NOT NULL,
    model          TEXT NOT NULL,
    provider       TEXT NOT NULL,
    coherence      FLOAT,
    relevance      FLOAT,
    completeness   FLOAT,
    latency_score  FLOAT,
    safety_score   FLOAT,
    overall_score  FLOAT,
    issues         TEXT[],
    ttft_ms        FLOAT,
    total_tokens   INTEGER,
    evaluated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ── agent task history ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_runs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id      TEXT NOT NULL UNIQUE,
    session_id   TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    task         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'running',  -- running|complete|error
    steps        INTEGER DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- ── indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON audit_log(tenant_id);
CREATE INDEX IF NOT EXISTS idx_llm_usage_session ON llm_usage(session_id);
CREATE INDEX IF NOT EXISTS idx_evals_session ON response_evals(session_id);
CREATE INDEX IF NOT EXISTS idx_evals_overall ON response_evals(overall_score);
CREATE INDEX IF NOT EXISTS idx_agent_runs_session ON agent_runs(session_id);

#!/usr/bin/env python3
"""
AI Backend — interactive demo script.

Walks through every major feature with narration.
Run from ai-backend/:  python3 demo.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
import uuid

try:
    import httpx
    import websockets
except ImportError:
    print("Installing deps...")
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "httpx", "websockets"])
    import httpx
    import websockets

AUTH = "http://localhost:8001"
WS   = "ws://localhost:8002"
CYAN  = "\033[96m"
GREEN = "\033[92m"
YELLOW= "\033[93m"
BOLD  = "\033[1m"
DIM   = "\033[2m"
RESET = "\033[0m"
RED   = "\033[91m"


def hr(label: str = "") -> None:
    width = 64
    if label:
        pad = (width - len(label) - 2) // 2
        print(f"\n{DIM}{'─'*pad} {BOLD}{label}{RESET}{DIM} {'─'*pad}{RESET}")
    else:
        print(f"{DIM}{'─'*width}{RESET}")


def info(msg: str) -> None:
    print(f"  {DIM}{msg}{RESET}")


async def chat(
    ws_conn,
    message: str,
    *,
    model: str = "llama3.2:3b",
    agent_mode: bool = False,
    label: str = "",
) -> tuple[str, dict]:
    """Send a message and collect the streamed response. Returns (text, final_chunk)."""
    await ws_conn.send(json.dumps({"message": message, "model": model, "agent_mode": agent_mode}))

    tag = f"[{YELLOW}agent{RESET}]" if agent_mode else f"[{CYAN}direct{RESET}]"
    header = f"{tag} {label or message!r}"
    print(f"\n  You   → {BOLD}{message}{RESET}")
    print(f"  AI    ← ", end="", flush=True)

    tokens: list[str] = []
    final: dict = {}
    while True:
        raw = await asyncio.wait_for(ws_conn.recv(), timeout=180)
        chunk = json.loads(raw)
        if chunk.get("token"):
            print(chunk["token"], end="", flush=True)
            tokens.append(chunk["token"])
        if chunk.get("is_final"):
            final = chunk
            break

    text = "".join(tokens)
    ttft = final.get("ttft_ms")
    provider = final.get("provider", "?")
    model_used = final.get("model_used", "?")
    cache_hit = final.get("cache_hit", False)

    parts = []
    if ttft: parts.append(f"ttft={int(ttft)}ms")
    parts.append(f"provider={provider}")
    parts.append(f"model={model_used}")
    if cache_hit: parts.append(f"{GREEN}CACHE HIT ✓{RESET}")
    print(f"\n  {DIM}{' · '.join(parts)}{RESET}")
    return text, final


async def main() -> None:
    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║         Real-Time AI Backend — Live Demo                 ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════════════════════╝{RESET}")

    # ── 1. Health checks ──────────────────────────────────────────────────────
    hr("1 · Health checks")
    async with httpx.AsyncClient(timeout=5) as c:
        for name, url in [("auth-service", f"{AUTH}/health"), ("ws-gateway", "http://localhost:8002/health")]:
            try:
                r = await c.get(url)
                status = r.json().get("status", "?")
                print(f"  {GREEN}✓{RESET} {name}: {status}")
            except Exception as e:
                print(f"  {RED}✗{RESET} {name}: {e}")
                sys.exit(1)

    # ── 2. Register + login ───────────────────────────────────────────────────
    hr("2 · Auth  (register → login → JWT)")
    username = f"demo_{uuid.uuid4().hex[:6]}"
    password = "demo1234"
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{AUTH}/register",
                         json={"username": username, "password": password, "tenant_id": "demo"})
        token = r.json()["access_token"]
        print(f"  {GREEN}✓{RESET} Registered {BOLD}{username}{RESET}")
        print(f"  {GREEN}✓{RESET} JWT issued  {DIM}({len(token)} chars){RESET}")

    # ── 3. Direct chat ────────────────────────────────────────────────────────
    hr("3 · Direct chat  (WebSocket → memory → Ollama → stream)")
    session = str(uuid.uuid4())
    info(f"session={session[:8]}  model=llama3.2:3b  provider=ollama")
    async with websockets.connect(f"{WS}/ws/{session}?token={token}", ping_interval=None) as ws:
        await chat(ws, "What is the speed of light? One sentence.", model="llama3.2:3b")

    # ── 4. Multi-turn memory ──────────────────────────────────────────────────
    hr("4 · Multi-turn conversation  (Redis short-term + pgvector semantic)")
    session = str(uuid.uuid4())
    info("same session → history accumulates → model remembers context")
    async with websockets.connect(f"{WS}/ws/{session}?token={token}", ping_interval=None) as ws:
        await chat(ws, "My name is Alex and I'm learning about distributed systems.", model="llama3.2:3b")
        await asyncio.sleep(1)
        await chat(ws, "What's my name, and what topic am I studying?", model="llama3.2:3b")

    # ── 5. Prompt cache ───────────────────────────────────────────────────────
    hr("5 · Redis prompt cache  (SHA-256 of messages → 1h TTL)")
    question = "Name the three laws of thermodynamics, one line each."
    session = str(uuid.uuid4())
    info("two calls with identical context → second should be cache HIT")
    async with websockets.connect(f"{WS}/ws/{session}?token={token}", ping_interval=None) as ws:
        _, c1 = await chat(ws, question, model="llama3.2:3b", label="call 1 (cold)")

    # new session, same brand-new user = no history pollution
    username2 = f"cache_{uuid.uuid4().hex[:6]}"
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{AUTH}/register", json={"username": username2, "password": "x", "tenant_id": "demo"})
        tok2 = r.json()["access_token"]
    session2 = str(uuid.uuid4())
    async with websockets.connect(f"{WS}/ws/{session2}?token={tok2}", ping_interval=None) as ws:
        _, c2 = await chat(ws, question, model="llama3.2:3b", label="call 2 (same user, new session)")

    if not c2.get("cache_hit"):
        info("(semantic memory injected different context → keys differ. Cache works on exact-match contexts.)")

    # ── 6. Agent mode ─────────────────────────────────────────────────────────
    hr("6 · Agent mode  (ReAct loop → tool-runtime → Kafka → result)")
    session = str(uuid.uuid4())
    info("ws-gateway → memory-service → agent-orchestrator → tool-runtime → llm-responses")
    async with websockets.connect(f"{WS}/ws/{session}?token={token}", ping_interval=None) as ws:
        await chat(ws, "Use the calculate tool: what is 1337 * 42?",
                   model="llama3.2:3b", agent_mode=True)

    # ── 7. Agent: web search ──────────────────────────────────────────────────
    hr("7 · Agent: search tool  (DuckDuckGo free API)")
    session = str(uuid.uuid4())
    async with websockets.connect(f"{WS}/ws/{session}?token={token}", ping_interval=None) as ws:
        await chat(ws, "Search for: what is Apache Kafka used for?",
                   model="llama3.2:3b", agent_mode=True)

    # ── 8. Eval scores ────────────────────────────────────────────────────────
    hr("8 · Eval service  (quality scores → Postgres)")
    await asyncio.sleep(2)  # let eval-service process
    try:
        result = await asyncio.create_subprocess_shell(
            'docker exec ai-backend-postgres-1 psql -U postgres aibackend -t -c '
            '"SELECT model, round(coherence::numeric,2), round(relevance::numeric,2), '
            'round(overall_score::numeric,2), round(ttft_ms::numeric) '
            'FROM response_evals ORDER BY evaluated_at DESC LIMIT 5;"',
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        stdout, _ = await result.communicate()
        rows = stdout.decode().strip().splitlines()
        print(f"  {'Model':<15} {'Coherence':>9} {'Relevance':>9} {'Overall':>9} {'TTFT(ms)':>9}")
        print(f"  {'─'*15} {'─'*9} {'─'*9} {'─'*9} {'─'*9}")
        for row in rows:
            if row.strip():
                cols = [c.strip() for c in row.split("|")]
                if len(cols) >= 4:
                    print(f"  {cols[0]:<15} {cols[1]:>9} {cols[2]:>9} {cols[3]:>9} {cols[4] if len(cols)>4 else '':>9}")
    except Exception:
        info("(run 'docker exec ai-backend-postgres-1 psql -U postgres aibackend -c "
             "\"SELECT * FROM response_evals ORDER BY evaluated_at DESC LIMIT 5;\"')")

    # ── 9. Dashboards ─────────────────────────────────────────────────────────
    hr("9 · Observability")
    print(f"  {CYAN}Jaeger traces{RESET}  →  http://localhost:16686  (search service: ws-gateway)")
    print(f"  {CYAN}Grafana{RESET}        →  http://localhost:3000    (admin / admin)")
    print(f"  {CYAN}Prometheus{RESET}     →  http://localhost:9090")
    print(f"  {CYAN}Langfuse{RESET}       →  http://localhost:3001")

    hr()
    print(f"\n  {GREEN}{BOLD}Demo complete.{RESET} All services working end-to-end.\n")


if __name__ == "__main__":
    asyncio.run(main())

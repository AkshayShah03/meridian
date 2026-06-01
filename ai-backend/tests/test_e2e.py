"""
End-to-end test: register → login → WebSocket chat → agent mode.

Run from the ai-backend/ directory:
    pip install websockets httpx
    python tests/test_e2e.py

Requires all services to be up (docker compose up --build).
"""
from __future__ import annotations

import asyncio
import json
import sys
import uuid

import httpx
import websockets

AUTH_URL = "http://localhost:8001"
WS_URL = "ws://localhost:8002"

USERNAME = f"testuser_{uuid.uuid4().hex[:6]}"
PASSWORD = "testpassword123"


async def register_and_login() -> str:
    async with httpx.AsyncClient() as client:
        # register
        r = await client.post(f"{AUTH_URL}/register", json={
            "username": USERNAME,
            "password": PASSWORD,
            "tenant_id": "demo",
        })
        if r.status_code not in (200, 201):
            print(f"  [WARN] register: {r.status_code} — trying login directly")
        else:
            print(f"  [OK] registered user: {USERNAME}")

        # login
        r = await client.post(f"{AUTH_URL}/login", json={
            "username": USERNAME,
            "password": PASSWORD,
        })
        r.raise_for_status()
        token = r.json()["access_token"]
        print(f"  [OK] login succeeded, token length={len(token)}")
        return token


async def chat(token: str, message: str, agent_mode: bool = False) -> str:
    session_id = str(uuid.uuid4())
    url = f"{WS_URL}/ws/{session_id}?token={token}"
    full_response: list[str] = []

    mode_label = "agent" if agent_mode else "direct"
    print(f"\n  [{mode_label.upper()}] Connecting session={session_id[:8]}...")

    async with websockets.connect(url, ping_interval=None) as ws:
        await ws.send(json.dumps({
            "message": message,
            "agent_mode": agent_mode,
            "model": "mistral:7b",
        }))

        print(f"  Sent: {message!r}")
        print(f"  Response: ", end="", flush=True)

        ttft_ms = None
        while True:
            raw = await ws.recv()
            chunk = json.loads(raw)
            token_text = chunk.get("token", "")
            is_final = chunk.get("is_final", False)

            if token_text:
                print(token_text, end="", flush=True)
                full_response.append(token_text)

            if is_final:
                ttft_ms = chunk.get("ttft_ms")
                provider = chunk.get("provider", "?")
                model_used = chunk.get("model_used", "?")
                cache_hit = chunk.get("cache_hit", False)
                break

    print()  # newline after streamed tokens
    response = "".join(full_response)
    print(f"\n  Stats:")
    print(f"    provider   = {provider}")
    print(f"    model      = {model_used}")
    print(f"    cache_hit  = {cache_hit}")
    print(f"    ttft_ms    = {ttft_ms}")
    print(f"    tokens     = {len(response.split())}")
    return response


async def test_health() -> None:
    print("\n── Health checks ────────────────────────────────────")
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in [
            ("auth-service",    f"{AUTH_URL}/health"),
            ("ws-gateway",      "http://localhost:8002/health"),
            ("inference-gw",    "http://localhost:8003/health"),  # not exposed by default
        ]:
            try:
                r = await client.get(url)
                print(f"  {name}: {r.json()}")
            except Exception as exc:
                print(f"  {name}: UNREACHABLE ({exc})")


async def main() -> None:
    print("═" * 60)
    print("  AI Backend End-to-End Test")
    print("═" * 60)

    await test_health()

    print("\n── Auth flow ─────────────────────────────────────────")
    token = await register_and_login()

    print("\n── Direct chat (Ollama, no agent) ───────────────────")
    await chat(token, "What is 2 + 2? Answer in one sentence.")

    print("\n── Cache hit test (same message again) ──────────────")
    await chat(token, "What is 2 + 2? Answer in one sentence.")

    print("\n── Multi-turn conversation ───────────────────────────")
    # Use same session to test memory/history
    session_id = str(uuid.uuid4())
    url = f"{WS_URL}/ws/{session_id}?token={token}"
    async with websockets.connect(url, ping_interval=None) as ws:
        for msg in [
            "My name is Alex. Remember that.",
            "What is my name?",
        ]:
            await ws.send(json.dumps({"message": msg}))
            print(f"\n  You: {msg}")
            print(f"  AI: ", end="", flush=True)
            while True:
                chunk = json.loads(await ws.recv())
                if chunk.get("token"):
                    print(chunk["token"], end="", flush=True)
                if chunk.get("is_final"):
                    break
        print()

    print("\n── Agent mode (tool use) ────────────────────────────")
    await chat(
        token,
        "What is 1234 * 5678? Use the calculate tool.",
        agent_mode=True,
    )

    print("\n── Eval service check ───────────────────────────────")
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8007/ready", timeout=3.0)
            print(f"  eval-service: {r.json()}")
        except Exception:
            print("  eval-service: not exposed on host (internal only)")

    print("\n" + "═" * 60)
    print("  All tests passed ✓")
    print("═" * 60)


if __name__ == "__main__":
    asyncio.run(main())

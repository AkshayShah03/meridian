from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import asyncpg
import bcrypt
import jwt
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from shared.telemetry.otel import get_tracer, setup_telemetry, span

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("auth-service")

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/aibackend"
)
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-prod")
JWT_ALGORITHM = "HS256"
ACCESS_TTL_MINUTES = 60
REFRESH_TTL_DAYS = 7

app = FastAPI(title="auth-service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
tracer = None
db_pool: asyncpg.Pool | None = None


@app.on_event("startup")
async def startup() -> None:
    global tracer, db_pool
    setup_telemetry("auth-service")
    tracer = get_tracer("auth-service")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    logger.info("auth-service ready")


@app.on_event("shutdown")
async def shutdown() -> None:
    if db_pool:
        await db_pool.close()


# ── request/response models ──────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    username: str
    password: str
    tenant_id: str = "default"


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


# ── helpers ──────────────────────────────────────────────────────────────────

def _issue_tokens(user_id: str, tenant_id: str) -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    access_payload = {
        "sub": user_id,
        "tenant_id": tenant_id,
        "exp": now + timedelta(minutes=ACCESS_TTL_MINUTES),
        "iat": now,
        "type": "access",
    }
    refresh_payload = {
        "sub": user_id,
        "tenant_id": tenant_id,
        "exp": now + timedelta(days=REFRESH_TTL_DAYS),
        "iat": now,
        "type": "refresh",
    }
    access = jwt.encode(access_payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    refresh = jwt.encode(refresh_payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return access, refresh


def _decode_token(token: str, expected_type: str = "access") -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload.get("type") != expected_type:
        raise HTTPException(status_code=401, detail="Wrong token type")
    return payload


# ── routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "auth-service"})


@app.get("/ready")
async def ready() -> JSONResponse:
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return JSONResponse({"status": "ready"})
    except Exception:
        raise HTTPException(status_code=503, detail="DB not reachable")


@app.post("/register", response_model=TokenResponse, status_code=201)
async def register(req: RegisterRequest) -> TokenResponse:
    with span(tracer, "auth.register", attributes={"tenant": req.tenant_id}):
        hashed = bcrypt.hashpw(req.password.encode(), bcrypt.gensalt()).decode()
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO users (username, password_hash, tenant_id)
                    VALUES ($1, $2, $3)
                    RETURNING id
                    """,
                    req.username,
                    hashed,
                    req.tenant_id,
                )
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=409, detail="Username already taken")

        user_id = str(row["id"])
        access, refresh = _issue_tokens(user_id, req.tenant_id)
        logger.info("Registered user=%s tenant=%s", user_id, req.tenant_id)
        return TokenResponse(access_token=access, refresh_token=refresh)


@app.post("/login", response_model=TokenResponse)
async def login(req: LoginRequest) -> TokenResponse:
    with span(tracer, "auth.login"):
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, password_hash, tenant_id FROM users WHERE username = $1",
                req.username,
            )
        if not row or not bcrypt.checkpw(req.password.encode(), row["password_hash"].encode()):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        user_id = str(row["id"])
        access, refresh = _issue_tokens(user_id, row["tenant_id"])
        logger.info("Login user=%s", user_id)
        return TokenResponse(access_token=access, refresh_token=refresh)


@app.post("/refresh", response_model=TokenResponse)
async def refresh(req: RefreshRequest) -> TokenResponse:
    payload = _decode_token(req.refresh_token, expected_type="refresh")
    access, new_refresh = _issue_tokens(payload["sub"], payload["tenant_id"])
    return TokenResponse(access_token=access, refresh_token=new_refresh)


@app.get("/validate")
async def validate(token: str) -> JSONResponse:
    payload = _decode_token(token, expected_type="access")
    return JSONResponse({"user_id": payload["sub"], "tenant_id": payload["tenant_id"]})

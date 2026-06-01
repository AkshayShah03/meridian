# syntax=docker/dockerfile:1
ARG PYTHON_VERSION=3.12

# ── builder ──────────────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── runtime ──────────────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS runtime

ARG SERVICE_NAME
# SERVICE_NAME uses hyphens (e.g. auth-service); MODULE_NAME converts to underscores
ENV SERVICE_NAME=${SERVICE_NAME} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY --from=builder /install /usr/local

# shared library
COPY shared/ ./shared/

# the specific service (directory uses underscores for valid Python package name)
ARG SERVICE_NAME
RUN echo "Building service: ${SERVICE_NAME}"
COPY services/ ./services/

EXPOSE 8000

CMD ["sh", "-c", \
  "MODULE=$(echo ${SERVICE_NAME} | tr '-' '_') && \
   uvicorn services.${MODULE}.app.main:app \
   --host 0.0.0.0 --port 8000 \
   --loop uvloop \
   --log-level info"]

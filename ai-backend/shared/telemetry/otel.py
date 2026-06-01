from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from collections.abc import Generator

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode

logger = logging.getLogger(__name__)


def setup_telemetry(service_name: str) -> TracerProvider:
    otlp_endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
    )

    resource = Resource.create(
        {
            "service.name": service_name,
            "deployment.environment": os.getenv("ENV", "development"),
        }
    )

    provider = TracerProvider(resource=resource)

    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)
    logger.info("OTel tracer initialised, service=%s endpoint=%s", service_name, otlp_endpoint)
    return provider


def get_tracer(service_name: str) -> trace.Tracer:
    return trace.get_tracer(service_name)


@contextmanager
def span(
    tracer: trace.Tracer,
    name: str,
    *,
    trace_id: str | None = None,
    attributes: dict[str, str] | None = None,
) -> Generator[Span, None, None]:
    with tracer.start_as_current_span(name) as current_span:
        if trace_id:
            current_span.set_attribute("custom.trace_id", trace_id)
        if attributes:
            for k, v in attributes.items():
                current_span.set_attribute(k, v)
        try:
            yield current_span
        except Exception as exc:
            current_span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise

import json
import logging
import sys
from datetime import UTC, datetime

from opentelemetry import trace


class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, str] = {
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "severity": record.levelname,
            "service": self.service_name,
            "message": record.getMessage(),
        }
        span = trace.get_current_span().get_span_context()
        if span.is_valid:
            entry["trace_id"] = f"{span.trace_id:032x}"
            entry["span_id"] = f"{span.span_id:016x}"
        for key in ("event", "image_id", "event_id", "stage", "error_code"):
            value = getattr(record, key, None)
            if value is not None:
                entry[key] = str(value)
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def configure_logging(service_name: str, level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service_name))
    logging.basicConfig(level=level, handlers=[handler], force=True)

import json
import logging

from vectorlake.common.logging import JsonFormatter


def test_json_log_includes_service_and_event_context() -> None:
    record = logging.LogRecord("vectorlake", logging.INFO, __file__, 1, "accepted", (), None)
    record.event = "image.received.v1"
    record.image_id = "image-1"

    entry = json.loads(JsonFormatter("vectorlake-api").format(record))

    assert entry["service"] == "vectorlake-api"
    assert entry["message"] == "accepted"
    assert entry["event"] == "image.received.v1"
    assert entry["image_id"] == "image-1"

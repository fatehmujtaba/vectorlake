from __future__ import annotations

from unittest.mock import Mock

from services.processor.main import process_events
from vectorlake.events.v1 import events_pb2
from vectorlake.processor import AlreadyProcessedOutcome


def test_completed_replay_commits_without_republishing() -> None:
    event = events_pb2.ImageReceivedV1()
    event.context.image_id = "image-1"
    processor = Mock()
    processor.process.side_effect = AlreadyProcessedOutcome("image-1")
    process_events(processor, [event])
    processor.process.assert_called_once_with(event)

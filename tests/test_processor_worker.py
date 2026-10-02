from __future__ import annotations

from unittest.mock import Mock, patch

from services.processor.main import main
from vectorlake.events.v1 import events_pb2
from vectorlake.processor import AlreadyProcessedOutcome


def test_completed_replay_commits_without_republishing() -> None:
    message = Mock()
    message.error.return_value = None
    message.value.return_value = b"event"
    consumer = Mock()
    consumer.poll.side_effect = [message, KeyboardInterrupt()]
    event = events_pb2.ImageReceivedV1()
    event.context.image_id = "image-1"
    with (
        patch("services.processor.main.Consumer", return_value=consumer),
        patch("services.processor.main.ImageProcessor") as processor_type,
        patch("services.processor.main.deserialize_event", return_value=event),
    ):
        processor_type.return_value.process.side_effect = AlreadyProcessedOutcome("image-1")
        main()
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    consumer.close.assert_called_once()

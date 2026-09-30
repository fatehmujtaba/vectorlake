from __future__ import annotations

import logging
import time

from vectorlake.bridge import EventBridge

logger = logging.getLogger(__name__)


def main() -> None:
    bridge = EventBridge()
    while True:
        try:
            bridge.poll_once(wait_seconds=5)
        except Exception:
            logger.exception("event bridge batch failed; SQS message remains for retry")
            time.sleep(2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

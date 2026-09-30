from vectorlake.api import create_app
from vectorlake.common.config import get_settings
from vectorlake.common.logging import configure_logging
from vectorlake.common.telemetry import configure_tracing

settings = get_settings()
configure_logging(settings.service_name, settings.log_level)
configure_tracing(settings.service_name, settings.otlp_endpoint)
app = create_app()

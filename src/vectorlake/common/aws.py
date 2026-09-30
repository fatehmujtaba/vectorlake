from typing import Any

import boto3
from botocore.config import Config

from vectorlake.common.local import LocalInfrastructureSettings


def aws_client(service: str, settings: LocalInfrastructureSettings) -> Any:
    return boto3.client(  # type: ignore[call-overload]
        service,
        endpoint_url=settings.aws_endpoint_url,
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        config=Config(
            connect_timeout=5,
            read_timeout=30,
            retries={"mode": "standard", "max_attempts": 3},
            signature_version="s3v4" if service == "s3" else None,
            s3={"addressing_style": "path"} if service == "s3" else None,
        ),
    )

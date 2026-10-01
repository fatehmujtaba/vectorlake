from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Protocol


class AuthenticationFailed(Exception):
    pass


class AuthenticationUnavailable(Exception):
    pass


@dataclass(frozen=True)
class Principal:
    tenant_id: str
    source_ids: frozenset[str]


class Authenticator(Protocol):
    def authenticate(self, authorization: str | None) -> Principal: ...


class LocalTokenAuthenticator:
    def __init__(self, token: str, source_id: str) -> None:
        if not token or not source_id:
            raise ValueError("local token and source ID must be nonempty")
        self.token = token
        self.source_id = source_id

    def authenticate(self, authorization: str | None) -> Principal:
        if not authorization or not hmac.compare_digest(authorization, f"Bearer {self.token}"):
            raise AuthenticationFailed
        return Principal("local-dev", frozenset({self.source_id}))


class UnconfiguredAuthenticator:
    def authenticate(self, authorization: str | None) -> Principal:
        raise AuthenticationUnavailable

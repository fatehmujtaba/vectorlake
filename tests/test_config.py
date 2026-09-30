from pytest import MonkeyPatch

from vectorlake.common.config import Settings


def test_settings_use_prefixed_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("VECTORLAKE_ENVIRONMENT", "test")
    monkeypatch.setenv("VECTORLAKE_LOG_LEVEL", "DEBUG")
    settings = Settings()
    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"

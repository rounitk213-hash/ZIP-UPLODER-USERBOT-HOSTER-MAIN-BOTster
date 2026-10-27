"""Application configuration, loaded exclusively from environment variables."""

from __future__ import annotations

import base64
from functools import lru_cache
import hashlib
import logging
import os
import re
import socket
from pathlib import Path
from typing import FrozenSet, Optional

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.constants import DEFAULT_STREAMABLE_EXTENSIONS, DEFAULT_VIDEO_EXTENSIONS
from app.core.crypto import SessionCipher

logger = logging.getLogger(__name__)

MIB = 1024 * 1024
GIB = 1024 * 1024 * 1024

# Older example files shipped this literal value. Treat it as "not configured"
# so a copied template cannot stop the service from starting.
_PLACEHOLDER_SESSION_KEYS = {
    "replace-with-output-of-scripts/generate_key.py",
    "replace-with-output-of-scripts/generate_key",
    "change-me",
}

# Values shipped in .env.example. Telegram rejects these API credentials, so
# detect them early and give a useful explanation instead of a generic phone error.
_EXAMPLE_API_ID = 123456
_EXAMPLE_API_HASHES = {"0123456789abcdef0123456789abcdef"}


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in (value or "").replace(";", ",").split(",") if item.strip()]


def _normalize_extensions(raw: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
    items = _split_csv(raw)
    if not items:
        return fallback
    return tuple(x if x.startswith(".") else f".{x}" for x in (i.lower() for i in items))


class Settings(BaseSettings):
    """Environment driven settings.

    Required variables: ``BOT_TOKEN``, ``TELEGRAM_API_ID``, ``TELEGRAM_API_HASH``,
    ``MONGODB_URI``. ``SESSION_ENCRYPTION_KEY`` is optional: when it is not set,
    a random key is generated once at ``<DATA_DIR>/session.key``. ``ADMIN_IDS``
    is optional but strongly recommended.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Telegram control bot -------------------------------------------
    bot_token: str
    telegram_api_id: int
    telegram_api_hash: str
    admin_ids: str = ""

    # --- Persistence -----------------------------------------------------
    mongodb_uri: str
    mongodb_db: str = "userbot_hosting"
    # Empty means: use/create the persistent key file at session_key_path.
    session_encryption_key: str = ""

    # --- Paths (Railway persistent volume mounted at DATA_DIR) -----------
    data_dir: str = "/data"
    sessions_dir: str = ""
    temp_dir: str = ""

    # --- Per-user quotas -------------------------------------------------
    # 0 means unlimited quota so large zips never get blocked by quota
    default_storage_quota_bytes: int = 0
    default_daily_task_quota: int = 200

    # --- ZIP safety limits ----------------------------------------------
    max_zip_size_bytes: int = 2 * GIB
    max_total_uncompressed_bytes: int = 4 * GIB
    max_single_file_bytes: int = 2 * GIB
    max_zip_entries: int = 5000
    max_compression_ratio: float = 250.0
    max_archive_depth: int = 3
    min_free_disk_bytes: int = 0

    # --- Concurrency / throughput ---------------------------------------
    # Each connected account needs one resident worker. Keep this below the
    # memory your plan can afford (roughly 3-6 MB per idle Telethon client);
    # 50 is a safe default for a 1 GB instance.
    max_active_userbots: int = 50
    per_user_concurrent_jobs: int = 1
    per_user_queue_size: int = 4
    extract_concurrency: int = 2
    download_concurrency: int = 2
    upload_concurrency: int = 2
    download_chunk_size_kb: int = 512
    upload_chunk_size_kb: int = 512
    min_seconds_between_requests: float = 0.0
    max_retries: int = 4
    retry_base_delay: float = 2.0
    retry_max_delay: float = 120.0
    floodwait_extra_seconds: float = 1.0
    uncertain_send_retries: int = 2

    # --- Runtime behaviour ----------------------------------------------
    task_timeout_seconds: float = 3600.0
    # 0 keeps connected accounts online forever (recommended). Set a positive
    # value only if you accept that an idle account stops handling /unzip until
    # it is opened again from the control bot.
    idle_disconnect_seconds: float = 0.0
    session_lease_seconds: int = 120
    progress_update_interval: float = 5.0
    default_spoiler: bool = False
    # Only compare against an already-known account id on reconnect. Comparing
    # the Telegram account id with the *bot* user id (the old behaviour) would
    # reject every legitimate login. First-time logins have no known id and are
    # therefore always allowed.
    require_same_account: bool = True
    cleanup_stale_temp_hours: float = 2.0
    max_videos_per_job: int = 0

    # --- Misc ------------------------------------------------------------
    # Railway injects PORT. HEALTH_PORT remains supported for local/Compose.
    port: Optional[int] = None
    health_port: int = 8080
    host: str = "0.0.0.0"
    log_level: str = "INFO"
    log_json: bool = True
    worker_id: str = ""

    video_extensions: str = ""
    streamable_extensions: str = ""

    # ------------------------------------------------------------------
    @field_validator("session_encryption_key")
    @classmethod
    def _validate_key(cls, value: str) -> str:
        value = (value or "").strip()
        if not value:
            return ""
        if value.lower() in _PLACEHOLDER_SESSION_KEYS:
            logger.warning(
                "SESSION_ENCRYPTION_KEY contains the example placeholder; "
                "ignoring it and using the persistent key file instead"
            )
            return ""
        # Fail fast for a malformed explicitly configured key.
        SessionCipher(value)
        return value

    @field_validator("telegram_api_id")
    @classmethod
    def _validate_api_id(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("TELEGRAM_API_ID must be a positive integer")
        return value

    @field_validator("telegram_api_hash")
    @classmethod
    def _validate_api_hash(cls, value: str) -> str:
        value = (value or "").strip()
        if not re.fullmatch(r"[0-9a-fA-F]{32}", value):
            raise ValueError(
                "TELEGRAM_API_HASH must be exactly 32 hexadecimal characters from my.telegram.org"
            )
        return value

    @property
    def telegram_api_looks_like_example(self) -> bool:
        return (
            self.telegram_api_id == _EXAMPLE_API_ID
            and self.telegram_api_hash.lower() in _EXAMPLE_API_HASHES
        )

    @model_validator(mode="after")
    def _validate_limits(self) -> "Settings":
        positive = {
            "max_active_userbots": self.max_active_userbots,
            "per_user_concurrent_jobs": self.per_user_concurrent_jobs,
            "per_user_queue_size": self.per_user_queue_size,
            "extract_concurrency": self.extract_concurrency,
            "download_concurrency": self.download_concurrency,
            "upload_concurrency": self.upload_concurrency,
            "download_chunk_size_kb": self.download_chunk_size_kb,
            "upload_chunk_size_kb": self.upload_chunk_size_kb,
        }
        for name, value in positive.items():
            if int(value) < 1:
                raise ValueError(f"{name.upper()} must be at least 1")
        non_negative = {
            "default_storage_quota_bytes": self.default_storage_quota_bytes,
            "default_daily_task_quota": self.default_daily_task_quota,
            "max_zip_size_bytes": self.max_zip_size_bytes,
            "max_total_uncompressed_bytes": self.max_total_uncompressed_bytes,
            "max_single_file_bytes": self.max_single_file_bytes,
            "max_zip_entries": self.max_zip_entries,
            "max_archive_depth": self.max_archive_depth,
            "min_free_disk_bytes": self.min_free_disk_bytes,
            "idle_disconnect_seconds": self.idle_disconnect_seconds,
            "task_timeout_seconds": self.task_timeout_seconds,
            "cleanup_stale_temp_hours": self.cleanup_stale_temp_hours,
            "max_videos_per_job": self.max_videos_per_job,
        }
        for name, value in non_negative.items():
            if float(value) < 0:
                raise ValueError(f"{name.upper()} cannot be negative")
        if self.max_compression_ratio < 0:
            raise ValueError("MAX_COMPRESSION_RATIO cannot be negative")
        for name, value in (("HEALTH_PORT", self.health_port), ("PORT", self.port)):
            if value is not None and not (0 <= int(value) <= 65535):
                raise ValueError(f"{name} must be between 0 and 65535")
        if (
            self.telegram_api_id == _EXAMPLE_API_ID
            or self.telegram_api_hash.lower() in _EXAMPLE_API_HASHES
        ):
            logger.warning(
                "TELEGRAM_API_ID/TELEGRAM_API_HASH look like the .env.example values; "
                "Telegram will reject /connect requests. Use real credentials from "
                "my.telegram.org."
            )
        return self

    # ------------------------------------------------------------------
    @property
    def listen_port(self) -> int:
        """Railway's ``PORT`` takes precedence over the local ``HEALTH_PORT``."""
        if self.port is not None and int(self.port) > 0:
            return int(self.port)
        return int(self.health_port)

    # ------------------------------------------------------------------
    @property
    def admin_id_set(self) -> FrozenSet[int]:
        ids: set[int] = set()
        for item in _split_csv(self.admin_ids):
            try:
                ids.add(int(item))
            except ValueError:
                continue
        return frozenset(ids)

    @property
    def video_extension_set(self) -> FrozenSet[str]:
        return frozenset(_normalize_extensions(self.video_extensions, DEFAULT_VIDEO_EXTENSIONS))

    @property
    def streamable_extension_set(self) -> FrozenSet[str]:
        return frozenset(
            _normalize_extensions(self.streamable_extensions, DEFAULT_STREAMABLE_EXTENSIONS)
        )

    @property
    def sessions_path(self) -> Path:
        if self.sessions_dir:
            return Path(self.sessions_dir)
        return Path(self.data_dir) / "sessions"

    @property
    def temp_path(self) -> Path:
        if self.temp_dir:
            return Path(self.temp_dir)
        return Path(self.data_dir) / "tmp"

    @property
    def session_key_path(self) -> Path:
        """Key file used when SESSION_ENCRYPTION_KEY is not configured."""
        return Path(self.data_dir) / "session.key"

    def resolve_session_key(self) -> str:
        """Return the configured key, or load/create the persistent key file.

        An explicit ``SESSION_ENCRYPTION_KEY`` always wins. When it is absent,
        the key is kept on the persistent data volume so sessions survive
        restarts. The key is never logged.
        """
        if self.session_encryption_key:
            return self.session_encryption_key

        path = self.session_key_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise ValueError(f"session key path must not be a symlink: {path}")

        try:
            existing = path.read_text("utf-8").strip()
        except FileNotFoundError:
            existing = ""
        except OSError as exc:
            raise ValueError(f"could not read session key file: {path}") from exc

        if existing:
            self._validate_session_key_file(path, existing)
            self._harden_session_key_file(path)
            logger.info("using persistent session key from %s", path)
            return existing

        # Derive a stable deterministic key from bot credentials so ephemeral/volume-less hosts
        # (like Railway free tier) never lose the ability to decrypt sessions stored in MongoDB across restarts.
        raw = f"{self.bot_token}:{self.telegram_api_hash}".encode("utf-8")
        key = base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).decode("ascii")
        try:
            fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except (FileExistsError, OSError):
            try:
                existing = path.read_text("utf-8").strip()
                if existing:
                    self._validate_session_key_file(path, existing)
                    return existing
                path.write_text(key + "\n", encoding="utf-8")
            except OSError:
                pass
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(key + "\n")

        try:
            self._harden_session_key_file(path)
        except Exception:
            pass
        return key

    @staticmethod
    def _validate_session_key_file(path: Path, key: str) -> None:
        try:
            SessionCipher(key)
        except ValueError as exc:
            raise ValueError(
                f"session key file is not a valid Fernet key: {path}"
            ) from exc

    @staticmethod
    def _harden_session_key_file(path: Path) -> None:
        try:
            os.chmod(path, 0o600)
        except OSError:  # pragma: no cover - platform dependent (Windows)
            pass

    @property
    def effective_worker_id(self) -> str:
        if self.worker_id:
            return self.worker_id
        return f"{socket.gethostname()}-{os.getpid()}"

    # ------------------------------------------------------------------
    def ensure_directories(self) -> None:
        for path in (self.sessions_path, self.temp_path):
            path.mkdir(parents=True, exist_ok=True)

    def is_admin(self, user_id: Optional[int]) -> bool:
        return user_id is not None and int(user_id) in self.admin_id_set

    def describe(self) -> dict[str, object]:
        """Safe-to-log configuration summary (no secrets)."""
        return {
            "worker_id": self.effective_worker_id,
            "mongodb_db": self.mongodb_db,
            "data_dir": self.data_dir,
            "sessions_dir": str(self.sessions_path),
            "temp_dir": str(self.temp_path),
            "admin_count": len(self.admin_id_set),
            "max_active_userbots": self.max_active_userbots,
            "per_user_concurrent_jobs": self.per_user_concurrent_jobs,
            "per_user_queue_size": self.per_user_queue_size,
            "upload_concurrency": self.upload_concurrency,
            "download_concurrency": self.download_concurrency,
            "extract_concurrency": self.extract_concurrency,
            "max_videos_per_job": self.max_videos_per_job,
            "max_zip_size_mb": self.max_zip_size_bytes // MIB,
            "listen_port": self.listen_port,
            "log_level": self.log_level,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_directories()
    return settings


def reset_settings_cache() -> None:
    """Used by tests to re-read the environment."""
    get_settings.cache_clear()

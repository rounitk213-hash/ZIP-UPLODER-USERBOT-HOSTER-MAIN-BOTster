"""ZIP/archive validation: bomb, traversal, symlink and disk-exhaustion guards."""

from __future__ import annotations

import os
import posixpath
import shutil
import stat
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

# Sentinel for "unlimited" values.
UNLIMITED = 0


class ZipSecurityError(Exception):
    """Base class for all archive safety violations."""

    code = "archive_rejected"

    def __init__(self, message: str, *, detail: Optional[str] = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail


class NotAZipError(ZipSecurityError):
    code = "not_a_zip"


class CorruptArchiveError(ZipSecurityError):
    code = "corrupt_archive"


class PathTraversalError(ZipSecurityError):
    code = "path_traversal"


class SymlinkEntryError(ZipSecurityError):
    code = "symlink_entry"


class EncryptedArchiveError(ZipSecurityError):
    code = "encrypted_archive"


class TooManyEntriesError(ZipSecurityError):
    code = "too_many_entries"


class ArchiveTooLargeError(ZipSecurityError):
    code = "archive_too_large"


class EntryTooLargeError(ZipSecurityError):
    code = "entry_too_large"


class TotalSizeExceededError(ZipSecurityError):
    code = "total_size_exceeded"


class ZipBombError(ZipSecurityError):
    code = "zip_bomb"


class NestedArchiveDepthError(ZipSecurityError):
    code = "nested_too_deep"


class DiskSpaceError(ZipSecurityError):
    code = "disk_full"


class ExtractionCancelled(Exception):
    code = "cancelled"


@dataclass(frozen=True)
class ZipLimits:
    """All safety limits for a single job (shared across nested archives)."""

    max_archive_bytes: int = 2 * 1024**3
    max_total_uncompressed_bytes: int = 4 * 1024**3
    max_single_file_bytes: int = 2 * 1024**3
    max_entries: int = 5000
    max_compression_ratio: float = 250.0
    max_depth: int = 3
    min_free_disk_bytes: int = 0

    @classmethod
    def from_settings(cls, settings) -> "ZipLimits":  # pragma: no cover - thin wrapper
        return cls(
            max_archive_bytes=settings.max_zip_size_bytes,
            max_total_uncompressed_bytes=settings.max_total_uncompressed_bytes,
            max_single_file_bytes=settings.max_single_file_bytes,
            max_entries=settings.max_zip_entries,
            max_compression_ratio=settings.max_compression_ratio,
            max_depth=settings.max_archive_depth,
            min_free_disk_bytes=settings.min_free_disk_bytes,
        )


ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
ALLOWED_COMPRESSION = {
    zipfile.ZIP_STORED,
    zipfile.ZIP_DEFLATED,
    zipfile.ZIP_BZIP2,
    zipfile.ZIP_LZMA,
}


def looks_like_zip_file(path: Path) -> bool:
    try:
        with open(path, "rb") as handle:
            head = handle.read(4)
    except OSError:
        return False
    return head in ZIP_MAGICS


def is_zip_filename(name: str) -> bool:
    return posixpath.splitext(name.lower())[1] in (".zip",)


def sanitize_member_name(name: str) -> str:
    """Normalize an archive member name into a safe POSIX relative path.

    Raises :class:`PathTraversalError` for absolute paths, drive letters, ``..``
    segments, NUL bytes or other hostile names.
    """
    if not name or not isinstance(name, str):
        raise PathTraversalError("archive contains an entry without a name")

    if "\x00" in name:
        raise PathTraversalError("archive entry contains a NUL byte", detail=repr(name))

    # Normalize unicode so that look-alike characters cannot bypass checks.
    normalized = unicodedata.normalize("NFKC", name).replace("\\", "/")

    if normalized.startswith("/") or normalized.startswith("//"):
        raise PathTraversalError("absolute path in archive", detail=normalized)

    drive, _ = os.path.splitdrive(normalized)
    if drive:
        raise PathTraversalError("drive-qualified path in archive", detail=normalized)

    parts = [part for part in normalized.split("/") if part not in ("", ".")]
    if not parts:
        raise PathTraversalError("archive entry with empty path", detail=repr(name))

    for part in parts:
        if part == "..":
            raise PathTraversalError("path traversal ('..') in archive", detail=normalized)
        if any(ord(ch) < 32 for ch in part):
            raise PathTraversalError("control character in archive entry name", detail=repr(name))

    safe = "/".join(parts)
    if safe.startswith("~"):
        safe = "_" + safe[1:]
    return safe


def is_directory(info: zipfile.ZipInfo) -> bool:
    return info.is_dir() or info.filename.endswith("/")


def is_symlink(info: zipfile.ZipInfo) -> bool:
    """Detect symlink entries (stored with the S_IFLNK bit in ``external_attr``)."""
    mode = info.external_attr >> 16
    return bool(mode) and stat.S_ISLNK(mode)


def declared_uncompressed_size(info: zipfile.ZipInfo) -> int:
    return int(getattr(info, "file_size", 0) or 0)


def declared_compressed_size(info: zipfile.ZipInfo) -> int:
    return int(getattr(info, "compress_size", 0) or 0)


def check_compression_method(info: zipfile.ZipInfo) -> None:
    if info.compress_type not in ALLOWED_COMPRESSION:
        raise CorruptArchiveError(
            f"unsupported compression method {info.compress_type}",
            detail=info.filename,
        )
    if info.flag_bits & 0x1:
        raise EncryptedArchiveError(
            "password protected archives are not supported", detail=info.filename
        )


def check_ratio(info: zipfile.ZipInfo, limits: ZipLimits) -> None:
    """Reject single entries whose expansion ratio looks like a bomb."""
    if limits.max_compression_ratio <= UNLIMITED:
        return
    size = declared_uncompressed_size(info)
    compressed = declared_compressed_size(info)
    if size <= 0:
        return
    # Tiny entries legitimately reach high ratios; only apply past 1 MiB and
    # above a 64 KiB compression floor to avoid false positives.
    if size < 1024 * 1024 or compressed < 65536:
        return
    ratio = size / max(compressed, 1)
    if ratio > limits.max_compression_ratio:
        raise ZipBombError(
            f"compression ratio {ratio:.0f}:1 exceeds limit {limits.max_compression_ratio:.0f}:1",
            detail=info.filename,
        )


def safe_destination(root: Path, member_name: str) -> Path:
    """Return a destination path guaranteed to stay inside ``root``."""
    rel = sanitize_member_name(member_name)
    root_resolved = Path(root).resolve()
    candidate = (root_resolved / rel).resolve()
    if candidate != root_resolved and root_resolved not in candidate.parents:
        raise PathTraversalError("resolved path escapes the extraction root", detail=member_name)
    return candidate


def open_archive(path: Path) -> zipfile.ZipFile:
    if not looks_like_zip_file(path):
        raise NotAZipError("file is not a ZIP archive", detail=str(path))
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise CorruptArchiveError("archive is corrupt or truncated", detail=str(path)) from exc


def scan_archive(
    zf: zipfile.ZipFile,
    limits: ZipLimits,
    *,
    archive_size: int,
    depth: int,
    budget: "ExtractionBudget",
) -> Tuple[int, int]:
    """Pre-flight validation of the archive's central directory.

    Returns ``(entry_count, total_uncompressed_bytes)``.
    """
    if archive_size > limits.max_archive_bytes:
        raise ArchiveTooLargeError(
            f"archive is {archive_size} bytes, limit is {limits.max_archive_bytes}"
        )
    if depth > limits.max_depth:
        raise NestedArchiveDepthError(f"nested archives deeper than {limits.max_depth} levels")

    infos = zf.infolist()
    if len(infos) > limits.max_entries:
        raise TooManyEntriesError(
            f"archive has {len(infos)} entries, limit is {limits.max_entries}"
        )

    total = 0
    for info in infos:
        check_compression_method(info)
        if is_symlink(info):
            raise SymlinkEntryError("archive contains a symbolic link", detail=info.filename)
        if is_directory(info):
            continue
        sanitize_member_name(info.filename)
        size = declared_uncompressed_size(info)
        if size > limits.max_single_file_bytes:
            raise EntryTooLargeError(
                f"entry of {size} bytes exceeds per-file limit "
                f"{limits.max_single_file_bytes}",
                detail=info.filename,
            )
        check_ratio(info, limits)
        total += size

    if total > limits.max_total_uncompressed_bytes:
        raise TotalSizeExceededError(
            f"archive expands to {total} bytes, limit is {limits.max_total_uncompressed_bytes}"
        )
    budget.consume_entries(len(infos))
    budget.consume_declared(total)
    return len(infos), total


def available_disk_bytes(path: Path) -> int:
    target = Path(path)
    while not target.exists() and target.parent != target:
        target = target.parent
    try:
        return int(shutil.disk_usage(str(target)).free)
    except OSError:  # pragma: no cover - platform dependent
        return 1 << 62


def ensure_disk_space(path: Path, needed_bytes: int, limits: ZipLimits) -> None:
    free = available_disk_bytes(path)
    if free - needed_bytes < limits.min_free_disk_bytes:
        raise DiskSpaceError(
            "not enough free disk space: "
            f"{free} bytes free, {needed_bytes} required, "
            f"{limits.min_free_disk_bytes} reserved"
        )


class ExtractionBudget:
    """Shared budget for a job, enforced across nested archives."""

    def __init__(self, limits: ZipLimits) -> None:
        self.limits = limits
        self.entries_seen = 0
        self.declared_bytes = 0
        self.written_bytes = 0
        self.videos_found = 0

    def consume_entries(self, count: int) -> None:
        self.entries_seen += int(count)
        if self.entries_seen > self.limits.max_entries:
            raise TooManyEntriesError(
                f"job exceeded {self.limits.max_entries} archive entries in total"
            )

    def consume_declared(self, count: int) -> None:
        self.declared_bytes += int(count)
        if self.declared_bytes > self.limits.max_total_uncompressed_bytes:
            raise TotalSizeExceededError(
                "job exceeded the total uncompressed size limit "
                f"({self.limits.max_total_uncompressed_bytes} bytes)"
            )

    def account_written(self, count: int) -> None:
        if count <= 0:
            return
        self.written_bytes += int(count)
        if self.written_bytes > self.limits.max_total_uncompressed_bytes:
            raise TotalSizeExceededError(
                "extracted data exceeded the total uncompressed size limit "
                f"({self.limits.max_total_uncompressed_bytes} bytes)"
            )

    def register_video(self) -> None:
        self.videos_found += 1

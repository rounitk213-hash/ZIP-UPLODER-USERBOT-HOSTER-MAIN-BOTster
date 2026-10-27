"""The ZIP -> native video delivery pipeline.

One :class:`ZipTaskPipeline` instance handles a single task end to end:

1. resolve the replied message and verify it really is a ZIP archive,
2. stream the archive to a per-user scratch directory (never into RAM),
3. extract only video payloads, recursively (bounded concurrency, bomb guards),
4. probe each video for streaming metadata (no re-encoding, original bytes),
5. upload each video as a *native Telegram video message*, with the spoiler flag
   when the task is in spoiler mode,
6. report progress by editing one message in place, honour cancellation,
7. always clean up temporary files, even after failures.

Everything Telegram specific is reached through
:class:`~app.userbot.transport.UserbotTransport`, and every Telegram call passes
through :class:`~app.userbot.scheduler.AccountScheduler`, which is what keeps the
platform within Telegram's rate limits.
"""

from __future__ import annotations

import asyncio
import html
import logging
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.constants import StepStatus, TaskStatus
from app.core.humanize import format_bytes, humanize_transfer, truncate
from app.core.metrics import Metrics, metrics as default_metrics
from app.core.retry import stable_int64
from app.db.models import TaskRecord
from app.db.repositories import Repositories
from app.userbot.scheduler import AccountScheduler
from app.userbot.transport import (
    AccountRevokedError,
    FloodWaitSignal,
    MediaDescriptor,
    OperationCancelled,
    PermanentTransportError,
    TransientTransportError,
    UncertainSendError,
    UserbotTransport,
)
from app.zipengine.extractor import ExtractedVideo, ExtractionReport, ZipExtractor
from app.zipengine.probe import probe_video
from app.zipengine.safety import (
    ExtractionCancelled,
    ZipLimits,
    ZipSecurityError,
    ensure_disk_space,
)

logger = logging.getLogger(__name__)

CancelCheck = Callable[[], bool]


# Human readable rejection reasons, keyed by ``ZipSecurityError.code``.
SECURITY_MESSAGES: Dict[str, str] = {
    "not_a_zip": "that file is not a ZIP archive",
    "corrupt_archive": "the archive is corrupt or truncated",
    "path_traversal": "the archive contains an unsafe file path",
    "symlink_entry": "the archive contains a symbolic link",
    "encrypted_archive": "password protected archives are not supported",
    "too_many_entries": "the archive contains too many files",
    "archive_too_large": "the archive is larger than the configured limit",
    "entry_too_large": "the archive contains a file larger than the configured limit",
    "total_size_exceeded": "the archive expands beyond the configured limit",
    "zip_bomb": "the archive looks like a decompression bomb",
    "nested_too_deep": "the archive nests archives too deeply",
    "disk_full": "there is not enough disk space to process this archive",
}


@dataclass
class TaskOutcome:
    task_id: str
    status: str
    stats: Dict[str, Any] = field(default_factory=dict)
    error: Optional[Dict[str, Any]] = None
    summary: str = ""

    @property
    def ok(self) -> bool:
        return self.status in (TaskStatus.COMPLETED.value, TaskStatus.PARTIAL.value)


class _ProgressReporter:
    """Edits a single Telegram message in place; never breaks the job."""

    def __init__(
        self,
        transport: UserbotTransport,
        *,
        scheduler: Optional[AccountScheduler] = None,
        chat_id: int,
        reply_to: int,
        peer_type: str,
        peer: Optional[dict],
        interval: float,
        metrics: Metrics,
    ) -> None:
        self._transport = transport
        self._scheduler = scheduler
        self._chat_id = chat_id
        self._reply_to = reply_to
        self._peer_type = peer_type
        self._peer = peer
        self._interval = max(0.0, float(interval))
        self._metrics = metrics
        self._message_id: Optional[int] = None
        self._last_at = 0.0
        self._last_text = ""

    @property
    def message_id(self) -> Optional[int]:
        return self._message_id

    async def start(self, text: str) -> None:
        try:
            self._message_id = await self._call(
                "progress_send",
                lambda: self._transport.send_text(
                    self._chat_id,
                    text,
                    reply_to=self._reply_to,
                    peer_type=self._peer_type,
                    peer=self._peer,
                ),
                idempotent=False,
            )
            self._last_text = text
            self._last_at = time.monotonic()
        except Exception as exc:  # pragma: no cover - best effort
            self._metrics.incr("pipeline.progress_send_failed")
            logger.debug("could not send progress message: %s", type(exc).__name__)

    async def update(self, text: str, *, force: bool = False) -> None:
        if self._message_id is None or text == self._last_text:
            return
        now = time.monotonic()
        if not force and (now - self._last_at) < self._interval:
            return
        self._last_at = now
        await self._edit(text)

    async def finish(self, text: str) -> None:
        if self._message_id is None:
            await self.start(text)
            return
        await self._edit(text, force=True)

    async def _edit(self, text: str, *, force: bool = False) -> None:
        if self._message_id is None:
            return
        if text == self._last_text and not force:
            return
        try:
            await self._call(
                "progress_edit",
                lambda: self._transport.edit_text(
                    self._chat_id,
                    self._message_id,
                    text,
                    peer_type=self._peer_type,
                    peer=self._peer,
                ),
                idempotent=True,
            )
            self._last_text = text
            self._last_at = time.monotonic()
        except FloodWaitSignal as exc:
            # Back off the progress chatter; the job itself is unaffected.
            self._last_at = time.monotonic() + exc.seconds
            self._metrics.incr("pipeline.progress_flood_wait")
        except Exception as exc:  # pragma: no cover - best effort
            self._metrics.incr("pipeline.progress_edit_failed")
            logger.debug("could not edit progress message: %s", type(exc).__name__)

    async def _call(self, operation: str, factory, *, idempotent: bool):
        if self._scheduler is None:
            return await factory()
        return await self._scheduler.execute(
            operation, factory, idempotent=idempotent
        )


class ZipTaskPipeline:
    """Runs one ZIP extraction task for one user."""

    def __init__(
        self,
        *,
        settings: Any,
        repos: Repositories,
        transport: UserbotTransport,
        scheduler: AccountScheduler,
        extractor: ZipExtractor,
        temp_root: Path,
        metrics: Optional[Metrics] = None,
    ) -> None:
        self._settings = settings
        self._repos = repos
        self._transport = transport
        self._scheduler = scheduler
        self._extractor = extractor
        self._temp_root = Path(temp_root)
        self._metrics = metrics or default_metrics

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    async def run(
        self,
        task: TaskRecord,
        *,
        cancel_check: Optional[CancelCheck] = None,
    ) -> TaskOutcome:
        job_dir = self.job_dir(task)
        job_dir.mkdir(parents=True, exist_ok=True)

        peer_type, peer_spec = peer_parts(task)
        reply_anchor = command_message_id(task)
        reporter = _ProgressReporter(
            self._transport,
            scheduler=self._scheduler,
            chat_id=task.chat_id,
            reply_to=reply_anchor,
            peer_type=peer_type,
            peer=peer_spec,
            interval=self._settings.progress_update_interval,
            metrics=self._metrics,
        )

        stats: Dict[str, Any] = {
            "files_total": 0,
            "files_sent": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "files_uncertain": 0,
            "bytes_downloaded": 0,
            "bytes_sent": 0,
            "archives_scanned": 0,
        }
        reserved_bytes = 0
        started = time.monotonic()

        await reporter.start(self._header(task))
        await self._repos.tasks.set_status(
            task.task_id, TaskStatus.DOWNLOADING.value, worker_id=task.worker_id
        )

        try:
            media = await self._fetch_media(task)
            declared_size = int(media.size or (task.archive or {}).get("size", 0) or 0)
            needed_precheck = declared_size if declared_size > 0 else min(int(self._settings.max_zip_size_bytes), 50 * 1024 * 1024)
            ensure_disk_space(
                job_dir,
                needed_precheck,
                ZipLimits.from_settings(self._settings),
            )
            archive_reserved = await self._reserve_storage(task, declared_size)
            reserved_bytes += archive_reserved

            archive_path, archive_size = await self._download(
                task, media, reporter, cancel_check
            )
            stats["bytes_downloaded"] = archive_size
            # The real download can be smaller than the declared size. Give the
            # unused part of the reservation back immediately.
            if archive_size < archive_reserved:
                await self._release_storage(task, archive_reserved - archive_size)
                reserved_bytes -= archive_reserved - archive_size
                archive_reserved = archive_size
            elif archive_size > archive_reserved:
                extra = await self._reserve_storage(task, archive_size - archive_reserved)
                reserved_bytes += extra
                archive_reserved += extra

            await self._repos.tasks.set_status(
                task.task_id,
                TaskStatus.EXTRACTING.value,
                stats=stats,
                worker_id=task.worker_id,
            )
            report = await self._extract(task, archive_path, reporter, cancel_check)
            stats["archives_scanned"] = report.archives_scanned
            stats["files_skipped"] = report.skipped_non_video
            stats["files_total"] = report.video_count

            # Delete the archive file immediately after extraction to free disk space!
            try:
                archive_path.unlink(missing_ok=True)
            except OSError:
                pass
            if archive_reserved:
                await self._release_storage(task, archive_reserved)
                reserved_bytes -= archive_reserved
                archive_reserved = 0

            videos = report.sorted_videos()
            extra = await self._reserve_storage(task, report.total_video_bytes)
            reserved_bytes += extra

            if not videos:
                await reporter.finish(self._empty_summary(task, report))
                return await self._finalise(
                    task, TaskStatus.COMPLETED, stats, None, started
                )

            await self._repos.tasks.set_status(
                task.task_id,
                TaskStatus.SENDING.value,
                stats=stats,
                worker_id=task.worker_id,
            )
            await self._prepare_steps(task, videos)
            await self._deliver_all(task, videos, reporter, cancel_check, stats)

            status = self._status_for(stats)
            await reporter.finish(self._summary(task, stats, report))
            return await self._finalise(task, status, stats, None, started)

        except OperationCancelled:
            logger.info("task %s cancelled by user", task.task_id)
            self._metrics.incr("pipeline.cancelled")
            await self._finish_progress(reporter, "🚫 Cancelled. Temporary files removed.")
            return await self._finalise(
                task,
                TaskStatus.CANCELLED,
                stats,
                {"code": "cancelled", "message": "cancelled by user"},
                started,
            )
        except ExtractionCancelled:
            self._metrics.incr("pipeline.cancelled")
            await self._finish_progress(reporter, "🚫 Cancelled during extraction.")
            return await self._finalise(
                task,
                TaskStatus.CANCELLED,
                stats,
                {"code": "cancelled", "message": "cancelled by user"},
                started,
            )
        except ZipSecurityError as exc:
            self._metrics.incr(f"pipeline.rejected.{exc.code}")
            logger.warning("task %s rejected the archive: %s", task.task_id, exc.code)
            reason = SECURITY_MESSAGES.get(
                exc.code, "The archive was rejected by the security checks"
            )
            await self._finish_progress(reporter, f"⛔️ Rejected: {reason}.")
            return await self._finalise(
                task,
                TaskStatus.FAILED,
                stats,
                {"code": exc.code, "message": reason},
                started,
            )
        except AccountRevokedError as exc:
            logger.error("task %s: the connected account lost its session", task.task_id)
            await self._finish_progress(reporter, "⛔️ The connected Telegram account is no longer valid.")
            return await self._finalise(
                task, TaskStatus.FAILED, stats, {"code": exc.code, "message": str(exc)}, started
            )
        except PermanentTransportError as exc:
            self._metrics.incr("pipeline.permanent_error")
            logger.warning("task %s failed permanently: %s", task.task_id, exc.code)
            await self._finish_progress(
                reporter, f"⛔️ {html.escape(self._permanent_message(exc), quote=False)}"
            )
            return await self._finalise(
                task,
                TaskStatus.FAILED,
                stats,
                {"code": exc.code, "message": exc.message or exc.code},
                started,
            )
        except TransientTransportError as exc:
            self._metrics.incr("pipeline.transient_error")
            logger.warning("task %s failed after retries: %s", task.task_id, exc)
            await self._finish_progress(
                reporter, "⚠️ Telegram was unreachable after several retries. Try /unzip again."
            )
            return await self._finalise(
                task,
                TaskStatus.FAILED,
                stats,
                {"code": "transient_error", "message": str(exc)},
                started,
            )
        except asyncio.CancelledError:
            try:
                await self._repos.steps.unstick_inflight(task.task_id)
            except Exception:  # pragma: no cover - best effort
                logger.debug("could not park in-flight steps for %s", task.task_id)
            await self._repos.tasks.set_status(
                task.task_id, TaskStatus.INTERRUPTED.value, stats=stats
            )
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("task %s crashed: %s: %s", task.task_id, type(exc).__name__, exc)
            await self._finish_progress(reporter, "⛔️ The job failed unexpectedly. It was reported to the operators.")
            return await self._finalise(
                task,
                TaskStatus.FAILED,
                stats,
                {"code": "internal_error", "message": f"{type(exc).__name__}: {exc}"},
                started,
            )
        finally:
            try:
                if reserved_bytes:
                    await self._release_storage(task, reserved_bytes)
                    reserved_bytes = 0
                # Always reset user storage usage in DB so it never blocks future jobs
                try:
                    await self._repos.users.release_storage(task.user_id, 10**15)
                except Exception:
                    pass
            finally:
                self._cleanup(job_dir)

    # ------------------------------------------------------------------
    # paths
    # ------------------------------------------------------------------
    def job_dir(self, task: TaskRecord) -> Path:
        return self._temp_root / str(int(task.user_id)) / str(task.task_id)

    def _cleanup(self, job_dir: Path) -> None:
        try:
            shutil.rmtree(job_dir, ignore_errors=True)
        except OSError:  # pragma: no cover - defensive
            pass
        try:
            user_dir = job_dir.parent
            if user_dir.exists() and not any(user_dir.iterdir()):
                user_dir.rmdir()
        except OSError:
            pass

    # ------------------------------------------------------------------
    # stage 1: resolve + download
    # ------------------------------------------------------------------
    async def _fetch_media(self, task: TaskRecord) -> MediaDescriptor:
        peer_type, peer_spec = peer_parts(task)
        media = await self._scheduler.execute(
            "get_message",
            lambda: self._transport.get_message(
                task.chat_id, task.message_id, peer_type=peer_type, peer=peer_spec
            ),
        )
        if media is None:
            raise PermanentTransportError("MESSAGE_NOT_FOUND", "the replied message no longer exists")
        if not media.has_document or not media.is_zip:
            raise PermanentTransportError(
                "NOT_A_ZIP",
                "reply to a .zip document with /unzip",
            )
        if media.size and media.size > self._settings.max_zip_size_bytes:
            raise PermanentTransportError(
                "ARCHIVE_TOO_LARGE",
                f"the archive is {format_bytes(media.size)}, the limit is "
                f"{format_bytes(self._settings.max_zip_size_bytes)}",
            )
        return media

    async def _download(
        self,
        task: TaskRecord,
        media: MediaDescriptor,
        reporter: _ProgressReporter,
        cancel_check: Optional[CancelCheck],
    ) -> Tuple[Path, int]:
        safe_name = sanitize_archive_name(media.file_name or f"archive-{task.message_id}.zip")
        archive_path = self.job_dir(task) / safe_name
        started = time.monotonic()
        state = {"received": 0}

        def _progress(received: int, total: Optional[int]) -> None:
            state["received"] = received
            state["total"] = total

        async def _ticker() -> None:
            while True:
                await asyncio.sleep(max(1.0, self._settings.progress_update_interval))
                total = state.get("total")
                await reporter.update(
                    "⬇️ Downloading archive…\n"
                    + humanize_transfer(
                        state["received"], total, started_at=started, now=time.monotonic()
                    )
                )

        ticker = asyncio.create_task(_ticker())
        try:
            size = await self._scheduler.execute(
                "download_media",
                lambda: self._transport.download_media(
                    media,
                    archive_path,
                    progress=_progress,
                    cancel_check=cancel_check,
                    chunk_size_kb=self._settings.download_chunk_size_kb,
                    max_bytes=self._settings.max_zip_size_bytes,
                ),
            )
        finally:
            ticker.cancel()
            await _swallow_cancel(ticker)

        self._metrics.observe("pipeline.download_seconds", time.monotonic() - started)
        self._metrics.incr("pipeline.bytes_downloaded", float(size))
        logger.info(
            "task %s downloaded archive (%d bytes)", task.task_id, size, extra={"bytes": size}
        )
        return archive_path, int(size)

    # ------------------------------------------------------------------
    # stage 2: extract
    # ------------------------------------------------------------------
    async def _extract(
        self,
        task: TaskRecord,
        archive_path: Path,
        reporter: _ProgressReporter,
        cancel_check: Optional[CancelCheck],
    ) -> ExtractionReport:
        out_dir = self.job_dir(task) / "extracted"
        # A recovered task may still have videos from the previous attempt on
        # disk. Remove them first; otherwise unique-name handling would create
        # "file~1.mp4" and the per-video step ids (which use rel_path) would no
        # longer match, causing duplicate uploads.
        shutil.rmtree(out_dir, ignore_errors=True)
        started = time.monotonic()
        state = {"written": 0, "videos": 0}

        def _progress(written: int, videos: int) -> None:
            state["written"] = written
            state["videos"] = videos

        async def _ticker() -> None:
            while True:
                await asyncio.sleep(max(1.0, self._settings.progress_update_interval))
                await reporter.update(
                    "🗜 Extracting videos…\n"
                    f"{format_bytes(state['written'])} written · {state['videos']} video(s) found"
                )

        ticker = asyncio.create_task(_ticker())
        try:
            report = await self._extractor.extract_archive(
                archive_path, out_dir, cancel_check=cancel_check, progress=_progress
            )
        finally:
            ticker.cancel()
            await _swallow_cancel(ticker)

        self._metrics.observe("pipeline.extract_seconds", time.monotonic() - started)
        self._metrics.incr("pipeline.videos_extracted", float(report.video_count))
        logger.info(
            "task %s extracted %d video(s) from %d archive(s)",
            task.task_id,
            report.video_count,
            report.archives_scanned,
        )
        return report

    # ------------------------------------------------------------------
    # stage 3: send
    # ------------------------------------------------------------------
    async def _prepare_steps(self, task: TaskRecord, videos: List[ExtractedVideo]) -> None:
        items = [
            {
                "rel_path": video.rel_path,
                "size": video.size,
                "random_id": self._random_id(task, video),
            }
            for video in videos
        ]
        await self._repos.steps.ensure_steps(task.task_id, task.user_id, items)

    async def _deliver_all(
        self,
        task: TaskRecord,
        videos: List[ExtractedVideo],
        reporter: _ProgressReporter,
        cancel_check: Optional[CancelCheck],
        stats: Dict[str, Any],
    ) -> None:
        if not videos:
            return
        concurrency = max(1, int(self._settings.upload_concurrency))
        semaphore = asyncio.Semaphore(concurrency)
        local_cancel = asyncio.Event()

        def combined_cancel() -> bool:
            return local_cancel.is_set() or (cancel_check is not None and cancel_check())

        async def _one(index: int, video: ExtractedVideo) -> None:
            async with semaphore:
                if combined_cancel():
                    raise OperationCancelled("task cancelled")
                try:
                    await self._deliver(
                        task,
                        video,
                        index,
                        len(videos),
                        reporter,
                        combined_cancel,
                        stats,
                    )
                except OperationCancelled:
                    local_cancel.set()
                    raise
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - defensive
                    stats["files_failed"] = int(stats.get("files_failed", 0)) + 1
                    self._metrics.incr("pipeline.send_unexpected")
                    logger.exception(
                        "task %s: unexpected failure while sending %s: %s",
                        task.task_id,
                        video.rel_path,
                        type(exc).__name__,
                    )

        tasks = [
            asyncio.create_task(_one(index, video), name=f"send-{task.task_id}-{index}")
            for index, video in enumerate(videos, start=1)
        ]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            local_cancel.set()
            for pending in tasks:
                pending.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        cancelled = False
        for result in results:
            if isinstance(result, OperationCancelled):
                cancelled = True
            elif isinstance(result, BaseException):
                logger.warning(
                    "task %s: upload worker raised %s", task.task_id, type(result).__name__
                )
        if cancelled:
            raise OperationCancelled("task cancelled")

    async def _deliver(
        self,
        task: TaskRecord,
        video: ExtractedVideo,
        index: int,
        total: int,
        reporter: _ProgressReporter,
        cancel_check: Optional[CancelCheck],
        stats: Dict[str, Any],
    ) -> None:
        step = await self._repos.steps.claim(
            task.task_id,
            video.rel_path,
            max_attempts=3,
            allow_uncertain=False,
        )
        if step is None:
            # Already delivered, failed, or deliberately parked as uncertain by
            # an earlier run. Never send it again automatically.
            existing = await self._repos.steps.list_for_task(task.task_id)
            for record in existing:
                if record.rel_path != video.rel_path:
                    continue
                if record.status == StepStatus.SENT.value:
                    stats["files_sent"] += 1
                elif record.status == StepStatus.FAILED.value:
                    stats["files_failed"] += 1
                else:
                    stats["files_uncertain"] += 1
                return
            stats["files_uncertain"] += 1
            return

        metadata = probe_video(video.path)
        supports_streaming = video.path.suffix.lower() in set(
            self._settings.streamable_extension_set
        )
        started = time.monotonic()
        state: Dict[str, Any] = {"sent": 0, "total": video.size}

        def _progress(sent: int, size: Optional[int]) -> None:
            state["sent"] = sent
            if size:
                state["total"] = size

        async def _ticker() -> None:
            while True:
                await asyncio.sleep(max(1.0, self._settings.progress_update_interval))
                await reporter.update(self._sending_text(video, index, total, state, started))

        ticker = asyncio.create_task(_ticker())
        try:
            outcome = await self._scheduler.execute(
                "send_video",
                lambda: self._transport.send_video(
                    task.chat_id,
                    video.path,
                    filename=video.filename,
                    metadata=metadata,
                    spoiler=task.spoiler,
                    supports_streaming=supports_streaming,
                    random_id=self._random_id(task, video),
                    reply_to=command_message_id(task),
                    progress=_progress,
                    cancel_check=cancel_check,
                    peer_type=peer_parts(task)[0],
                    peer=peer_parts(task)[1],
                ),
                idempotent=True,
            )
        except OperationCancelled:
            await self._repos.steps.mark(
                task.task_id, video.rel_path, StepStatus.QUEUED.value, error="cancelled during upload"
            )
            raise
        except UncertainSendError as exc:
            await self._repos.steps.mark(
                task.task_id,
                video.rel_path,
                StepStatus.UNCERTAIN.value,
                error=truncate(str(exc), 180),
            )
            stats["files_uncertain"] += 1
            self._metrics.incr("pipeline.send_uncertain")
            logger.warning("task %s: delivery of %s is uncertain", task.task_id, video.rel_path)
        except PermanentTransportError as exc:
            await self._repos.steps.mark(
                task.task_id,
                video.rel_path,
                StepStatus.FAILED.value,
                error=f"{exc.code}: {truncate(exc.message, 120)}",
            )
            stats["files_failed"] += 1
            self._metrics.incr("pipeline.send_failed")
            logger.warning(
                "task %s: permanent send failure for %s (%s)",
                task.task_id,
                video.rel_path,
                exc.code,
            )
        except TransientTransportError as exc:
            await self._repos.steps.mark(
                task.task_id,
                video.rel_path,
                StepStatus.FAILED.value,
                error=f"transient: {truncate(str(exc), 120)}",
            )
            stats["files_failed"] += 1
            self._metrics.incr("pipeline.send_transient_failed")
        else:
            await self._repos.steps.mark_sent(task.task_id, video.rel_path, outcome.message_id)
            stats["files_sent"] += 1
            stats["bytes_sent"] = int(stats.get("bytes_sent", 0)) + int(outcome.bytes_sent or 0)
            try:
                await self._repos.users.add_bytes_sent(task.user_id, int(outcome.bytes_sent or 0))
            except Exception as exc:  # pragma: no cover - accounting must not fail the send
                logger.warning(
                    "task %s: could not record uploaded bytes: %s",
                    task.task_id,
                    type(exc).__name__,
                )
            self._metrics.observe("pipeline.upload_seconds", time.monotonic() - started)
            self._metrics.incr("pipeline.bytes_sent", float(outcome.bytes_sent or 0))
            logger.info(
                "task %s: sent %s (%d bytes)",
                task.task_id,
                video.rel_path,
                outcome.bytes_sent,
                extra={"bytes": outcome.bytes_sent, "step": video.rel_path},
            )
            # Delete this extracted video immediately after upload to free disk space
            try:
                video.path.unlink(missing_ok=True)
            except OSError:
                pass
            await self._release_storage(task, int(video.size or 0))
        finally:
            ticker.cancel()
            await _swallow_cancel(ticker)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _random_id(task: TaskRecord, video: ExtractedVideo) -> int:
        return stable_int64("video", task.user_id, task.task_id, video.rel_path)

    async def _reserve_storage(self, task: TaskRecord, size: int) -> int:
        amount = max(0, int(size))
        if amount <= 0:
            return 0
        allowed, used, limit = await self._repos.users.reserve_storage(task.user_id, amount)
        if not allowed:
            remaining = max(0, limit - used) if limit > 0 else 0
            raise PermanentTransportError(
                "STORAGE_QUOTA_EXCEEDED",
                f"this job needs {format_bytes(amount)} but only "
                f"{format_bytes(remaining)} of your storage quota is left",
            )
        return amount

    async def _release_storage(self, task: TaskRecord, size: int) -> None:
        try:
            await self._repos.users.release_storage(task.user_id, int(size))
        except Exception as exc:  # pragma: no cover - accounting must never crash a job
            logger.debug("could not release storage accounting: %s", type(exc).__name__)

    async def _storage_limit(self, task: TaskRecord) -> int:
        user = await self._repos.users.get(task.user_id)
        if user is None:
            return 0
        return int(user.quota.get("storage_bytes", 0) or 0)

    async def _finalise(
        self,
        task: TaskRecord,
        status: str,
        stats: Dict[str, Any],
        error: Optional[Dict[str, Any]],
        started: float,
    ) -> TaskOutcome:
        elapsed = time.monotonic() - started
        self._metrics.observe("pipeline.task_seconds", elapsed)
        self._metrics.incr(f"pipeline.task.{status}")
        await self._repos.tasks.set_status(
            task.task_id, status, error=error, stats=stats, worker_id=task.worker_id
        )
        return TaskOutcome(
            task_id=task.task_id,
            status=status,
            stats=dict(stats),
            error=error,
            summary=self._summary(task, stats, None),
        )

    async def _finish_progress(self, reporter: _ProgressReporter, text: str) -> None:
        try:
            await reporter.finish(text)
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("could not write the final progress message: %s", type(exc).__name__)

    # -- message text ----------------------------------------------------
    def _header(self, task: TaskRecord) -> str:
        mode = "spoiler mode" if task.spoiler else "normal mode"
        return f"📦 Starting extraction ({mode})…"

    def _sending_text(
        self,
        video: ExtractedVideo,
        index: int,
        total: int,
        state: Dict[str, Any],
        started: float,
    ) -> str:
        transfer = humanize_transfer(
            state.get("sent", 0),
            state.get("total"),
            started_at=started,
            now=time.monotonic(),
        )
        return (
            f"⬆️ Uploading video {index}/{total}\n"
            f"{html.escape(truncate(video.rel_path, 44), quote=False)}\n{transfer}"
        )

    def _empty_summary(self, task: TaskRecord, report: ExtractionReport) -> str:
        if report.skipped_non_video:
            return (
                f"✅ Scanned {report.archives_scanned} archive(s); no video files were found "
                f"({report.skipped_non_video} non-video file(s) skipped)."
            )
        return "✅ The archive contains no video files."

    def _summary(
        self, task: TaskRecord, stats: Dict[str, Any], report: Optional[ExtractionReport]
    ) -> str:
        lines: List[str] = []
        sent = int(stats.get("files_sent", 0))
        total = int(stats.get("files_total", 0))
        failed = int(stats.get("files_failed", 0))
        uncertain = int(stats.get("files_uncertain", 0))
        skipped = int(stats.get("files_skipped", 0))

        if task.spoiler and sent:
            lines.append(f"✅ Sent {sent}/{total} video(s) with the spoiler effect.")
        elif sent:
            lines.append(f"✅ Sent {sent}/{total} video(s) as Telegram videos.")
        else:
            lines.append("⚠️ No videos were delivered.")

        details = [
            f"{format_bytes(int(stats.get('bytes_sent', 0)))} uploaded",
            f"{skipped} non-video file(s) skipped",
        ]
        if failed:
            details.append(f"{failed} failed")
        if uncertain:
            details.append(f"{uncertain} uncertain (use /unzip retry to force a resend)")
        lines.append(" · ".join(details))
        return "\n".join(lines)

    @staticmethod
    def _status_for(stats: Dict[str, Any]) -> str:
        sent = int(stats.get("files_sent", 0))
        failed = int(stats.get("files_failed", 0))
        uncertain = int(stats.get("files_uncertain", 0))
        if sent == 0 and (failed or uncertain):
            return TaskStatus.FAILED.value
        if failed or uncertain:
            return TaskStatus.PARTIAL.value
        return TaskStatus.COMPLETED.value

    @staticmethod
    def _permanent_message(exc: PermanentTransportError) -> str:
        if exc.code == "NOT_A_ZIP":
            return "Reply to a .zip document with /unzip."
        if exc.code == "ARCHIVE_TOO_LARGE":
            return exc.message or "The archive is too large."
        if exc.code == "STORAGE_QUOTA_EXCEEDED":
            return exc.message or "Your storage quota is exhausted."
        if exc.code == "DOWNLOAD_UNAVAILABLE":
            return "This file cannot be downloaded with your connected account."
        if exc.code == "MESSAGE_NOT_FOUND":
            return "That message is no longer available."
        return f"Telegram rejected the request ({exc.code})."


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def peer_parts(task: TaskRecord) -> Tuple[str, Optional[dict]]:
    """Return ``(peer_type, peer_spec)`` for a persisted task."""
    payload = task.peer or {}
    peer_type = str(payload.get("peer_type") or "user")
    spec = payload.get("spec")
    return peer_type, (spec if isinstance(spec, dict) else None)


def command_message_id(task: TaskRecord) -> int:
    """The message that triggered the task (used as the reply anchor)."""
    archive = task.archive or {}
    value = archive.get("command_message_id")
    try:
        return int(value) if value else int(task.message_id)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return int(task.message_id)


def sanitize_archive_name(name: str) -> str:
    """Keep the original filename where possible, but never trust it."""
    base = str(name or "archive.zip").replace("\\", "/").split("/")[-1]
    cleaned = "".join(ch for ch in base if ch.isprintable() and ch not in "\x00")
    cleaned = cleaned.strip().strip(".")
    if not cleaned:
        cleaned = "archive.zip"
    if not cleaned.lower().endswith(".zip"):
        cleaned += ".zip"
    return cleaned[:120]


async def _swallow_cancel(task: "asyncio.Task[Any]") -> None:
    if task.done():
        return
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):  # pragma: no cover - best effort
        pass


__all__ = [
    "ZipTaskPipeline",
    "TaskOutcome",
    "peer_parts",
    "sanitize_archive_name",
    "SECURITY_MESSAGES",
]

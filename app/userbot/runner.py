"""Per-user worker runtime and the global userbot manager.

Architecture
------------
* One :class:`UserbotWorker` per connected Telegram account. It owns exactly one
  authenticated Telethon session, one :class:`~app.userbot.scheduler.AccountScheduler`
  (so every Telegram call of that account is rate limited centrally), a bounded
  task queue and ``per_user_concurrent_jobs`` consumer coroutines.
* :class:`UserbotManager` keeps up to ``max_active_userbots`` workers connected,
  acquires a distributed **session lock** per user (so two replicas can never
  drive the same Telegram session at once), recovers interrupted tasks after a
  restart and cleans stale temporary directories.
* Idle eviction is disabled by default (``IDLE_DISCONNECT_SECONDS=0``) because a
  worker that is disconnected cannot receive the account's own ``/unzip``
  command. The capacity limit is therefore a real capacity limit: size it for the
  number of accounts you expect to keep online, or accept that accounts beyond
  it are started on demand from the control bot and by recovery.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import time
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.constants import (
    ACTIVE_TASK_STATUSES,
    WORKER_HEARTBEAT_SECONDS,
    AccountStatus,
    TaskStatus,
)
from app.core.humanize import format_bytes
from app.core.metrics import Metrics, metrics as default_metrics
from app.db.models import TaskRecord, utcnow
from app.db.repositories import Repositories
from app.sessions.manager import SessionManager
from app.userbot.commands import (
    is_cancel_command,
    is_unzip_command,
    parse_cancel_command,
    parse_unzip_command,
)
from app.userbot.pipeline import TaskOutcome, ZipTaskPipeline
from app.userbot.scheduler import AccountScheduler
from app.userbot.transport import (
    AccountInfo,
    CommandContext,
    CommandHandlers,
    UserbotTransport,
)
from app.zipengine.extractor import ZipExtractor

logger = logging.getLogger(__name__)

GRACEFUL_STOP_SECONDS = 20.0
WORKER_STALE_SECONDS = 300.0
WORKER_LIVE_SECONDS = 180.0


@dataclass
class WorkerSnapshot:
    user_id: int
    account_user_id: Optional[int] = None
    username: Optional[str] = None
    running: bool = False
    connected: bool = False
    queued: int = 0
    active: List[str] = field(default_factory=list)
    jobs_done: int = 0
    jobs_failed: int = 0
    last_activity: float = 0.0
    scheduler: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "account_user_id": self.account_user_id,
            "username": self.username,
            "running": self.running,
            "connected": self.connected,
            "queued": self.queued,
            "active": list(self.active),
            "jobs_done": self.jobs_done,
            "jobs_failed": self.jobs_failed,
            "idle_seconds": round(max(0.0, time.monotonic() - self.last_activity), 1)
            if self.last_activity
            else None,
            "scheduler": self.scheduler,
        }


class UserbotWorker:
    """Runs the tasks of a single connected Telegram account."""

    def __init__(
        self,
        *,
        settings: Any,
        repos: Repositories,
        user_id: int,
        transport: UserbotTransport,
        account: AccountInfo,
        extractor: ZipExtractor,
        worker_id: str,
        metrics: Optional[Metrics] = None,
    ) -> None:
        self.user_id = int(user_id)
        self.account = account
        self.worker_id = worker_id
        self._settings = settings
        self._repos = repos
        self._transport = transport
        self._extractor = extractor
        self._metrics = metrics or default_metrics

        self.scheduler = AccountScheduler(
            self.user_id,
            max_concurrency=max(
                1,
                int(settings.upload_concurrency),
                int(getattr(settings, "download_concurrency", 1)),
            ),
            min_interval=float(settings.min_seconds_between_requests),
            max_retries=int(settings.max_retries),
            base_delay=float(settings.retry_base_delay),
            max_delay=float(settings.retry_max_delay),
            flood_extra=float(settings.floodwait_extra_seconds),
            uncertain_retries=int(settings.uncertain_send_retries),
            metrics=self._metrics,
        )

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, int(settings.per_user_queue_size)))
        self._concurrency = max(1, int(settings.per_user_concurrent_jobs))
        self._in_flight: set[str] = set()
        self._reserved: set[str] = set()
        self._cancel_local: set[str] = set()
        self._cancel_db: set[str] = set()
        self._consumers: List[asyncio.Task] = []
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        self._jobs_done = 0
        self._jobs_failed = 0
        self._last_activity = time.monotonic()

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    @property
    def running(self) -> bool:
        return self._running

    @property
    def last_activity(self) -> float:
        return self._last_activity

    @property
    def active_task_ids(self) -> List[str]:
        return sorted(self._in_flight)

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def touch(self) -> None:
        self._last_activity = time.monotonic()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.touch()
        self._transport.register_commands(CommandHandlers(on_command=self.handle_command))
        for index in range(self._concurrency):
            self._consumers.append(
                asyncio.create_task(self._consume(index), name=f"worker-{self.user_id}-{index}")
            )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"heartbeat-{self.user_id}"
        )
        logger.info(
            "userbot worker started for user %s (account=%s, concurrency=%d, queue=%d)",
            self.user_id,
            self.account.user_id,
            self._concurrency,
            self._queue.maxsize,
        )

    async def stop(self, *, reason: str = "shutdown") -> None:
        if not self._running:
            return
        self._running = False
        for task_id in list(self._in_flight):
            self._cancel_local.add(task_id)

        if self._consumers:
            done, pending = await asyncio.wait(
                self._consumers, timeout=GRACEFUL_STOP_SECONDS
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self._consumers.clear()

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None

        try:
            await self._transport.disconnect()
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("worker disconnect failed: %s", type(exc).__name__)
        logger.info("userbot worker stopped for user %s (reason=%s)", self.user_id, reason)

    # ------------------------------------------------------------------
    # task intake
    # ------------------------------------------------------------------
    def submit(self, task_id: str) -> bool:
        if not self._running:
            return False
        if task_id in self._reserved or task_id in self._in_flight:
            logger.info("task %s is already queued on this account", task_id)
            return False
        try:
            self._queue.put_nowait(task_id)
        except asyncio.QueueFull:
            self._metrics.incr("worker.queue_full")
            logger.warning("queue full for user %s, rejecting task %s", self.user_id, task_id)
            return False
        self._reserved.add(task_id)
        self.touch()
        return True

    def cancel(self, task_id: str) -> bool:
        if task_id in self._in_flight or task_id in self._reserved:
            self._cancel_local.add(task_id)
            return True
        return False

    # ------------------------------------------------------------------
    # consumers
    # ------------------------------------------------------------------
    async def _consume(self, index: int) -> None:
        while self._running:
            try:
                task_id = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return
            try:
                await self._run_task(task_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("worker consumer crashed: %s", type(exc).__name__)
                try:
                    current = await self._repos.tasks.get(task_id)
                    if current is not None and current.status in ACTIVE_TASK_STATUSES:
                        await self._repos.tasks.set_status(
                            task_id,
                            TaskStatus.FAILED.value,
                            error={
                                "code": "internal_error",
                                "message": type(exc).__name__,
                            },
                            worker_id=self.worker_id,
                        )
                except Exception:  # pragma: no cover - DB may be the original failure
                    logger.debug("could not mark task %s failed", task_id)
            finally:
                self._reserved.discard(task_id)
                self._queue.task_done()
                self.touch()

    async def _run_task(self, task_id: str) -> None:
        task = await self._repos.tasks.get(task_id)
        if task is None:
            logger.warning("task %s disappeared before it could run", task_id)
            return
        if task.user_id != self.user_id:  # pragma: no cover - isolation invariant
            logger.error("refusing to run task %s for another user", task_id)
            return
        if task.status not in ACTIVE_TASK_STATUSES:
            logger.info("task %s is already terminal (%s)", task_id, task.status)
            return
        if task.cancel_requested or task_id in self._cancel_local:
            await self._repos.tasks.set_status(
                task_id, TaskStatus.CANCELLED.value, worker_id=self.worker_id
            )
            return

        self._in_flight.add(task_id)
        self._metrics.adjust_gauge("worker.active_jobs", 1)
        try:
            fresh = await self._repos.tasks.get(task_id)
            attempts = await self._repos.tasks.begin(
                task_id, TaskStatus.DOWNLOADING.value, self.worker_id
            )
            if fresh is not None:
                task = fresh
            task.worker_id = self.worker_id
            task.attempts = attempts

            pipeline = ZipTaskPipeline(
                settings=self._settings,
                repos=self._repos,
                transport=self._transport,
                scheduler=self.scheduler,
                extractor=self._extractor,
                temp_root=self._settings.temp_path,
                metrics=self._metrics,
            )
            try:
                outcome = await asyncio.wait_for(
                    pipeline.run(task, cancel_check=lambda: self._is_cancelled(task_id)),
                    timeout=max(1.0, float(self._settings.task_timeout_seconds)),
                )
            except asyncio.TimeoutError:
                timeout_error = {
                    "code": "task_timeout",
                    "message": "the job exceeded TASK_TIMEOUT_SECONDS",
                }
                await self._repos.steps.unstick_inflight(task_id)
                await self._repos.tasks.set_status(
                    task_id,
                    TaskStatus.FAILED.value,
                    error=timeout_error,
                    worker_id=self.worker_id,
                )
                outcome = TaskOutcome(
                    task_id=task_id,
                    status=TaskStatus.FAILED.value,
                    stats={},
                    error=timeout_error,
                )
            if outcome.status in (TaskStatus.COMPLETED.value, TaskStatus.PARTIAL.value):
                self._jobs_done += 1
            elif outcome.status in (TaskStatus.FAILED.value, TaskStatus.INTERRUPTED.value):
                self._jobs_failed += 1
            await self._repos.audit.record(
                self.user_id,
                "task.finished",
                target_user_id=self.user_id,
                detail={
                    "task_id": task_id,
                    "status": outcome.status,
                    "stats": outcome.stats,
                },
            )
        finally:
            self._in_flight.discard(task_id)
            self._cancel_local.discard(task_id)
            self._cancel_db.discard(task_id)
            self._metrics.adjust_gauge("worker.active_jobs", -1)

    def _is_cancelled(self, task_id: str) -> bool:
        return task_id in self._cancel_local or task_id in self._cancel_db

    # ------------------------------------------------------------------
    # background maintenance
    # ------------------------------------------------------------------
    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)
                for task_id in list(self._in_flight):
                    await self._repos.tasks.heartbeat(task_id)
                    if await self._repos.tasks.is_cancel_requested(task_id):
                        self._cancel_db.add(task_id)
                await self._repos.locks.renew(
                    self.user_id, self.worker_id, self._settings.session_lease_seconds
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - DB hiccup
                logger.warning("worker heartbeat failed: %s", type(exc).__name__)

    # ------------------------------------------------------------------
    # commands coming from the connected account
    # ------------------------------------------------------------------
    async def handle_command(self, ctx: CommandContext) -> None:
        """Entry point wired into the transport's command filter."""
        # A userbot must only act on commands typed by its own account. This is
        # also enforced in TelethonTransport; keep the second check here so any
        # future transport cannot accidentally weaken the isolation boundary.
        if not ctx.is_outgoing:
            logger.warning("ignoring non-outgoing userbot command for user %s", self.user_id)
            return
        if int(ctx.sender_id) != int(self.account.user_id):
            logger.warning(
                "ignoring userbot command from sender %s (account is %s)",
                ctx.sender_id,
                self.account.user_id,
            )
            return
        self.touch()
        self._metrics.incr("worker.command")
        try:
            if is_cancel_command(ctx.text):
                await self._handle_cancel(ctx)
            elif is_unzip_command(ctx.text):
                await self._handle_unzip(ctx)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("command handling failed: %s", type(exc).__name__)
            await self._reply(ctx, "⚠️ Something went wrong while handling that command.")

    async def _handle_unzip(self, ctx: CommandContext) -> None:
        user = await self._repos.users.get(self.user_id)
        if user is None:
            await self._reply(ctx, "Please open the control bot and run /start first.")
            return
        if user.is_banned:
            await self._reply(ctx, "🚫 This account is suspended. Contact the operator.")
            return
        if user.status != AccountStatus.CONNECTED.value:
            await self._reply(
                ctx, "Your Telegram account is not connected. Use /start in the control bot."
            )
            return
        if not user.consent_given:
            await self._reply(
                ctx, "Please confirm the session notice in the control bot before using /unzip."
            )
            return

        default_spoiler = bool(user.settings.get("spoiler_default", self._settings.default_spoiler))
        command = parse_unzip_command(ctx.text, default_spoiler=default_spoiler)
        if command is None:
            return

        if ctx.reply is None:
            await self._reply(
                ctx,
                "Reply to a .zip document with /unzip (or /unzip spoiler for spoiler mode).",
            )
            return
        if not ctx.reply.has_document or not ctx.reply.is_zip:
            await self._reply(ctx, "That attachment is not a .zip archive.")
            return

        note = ""
        if command.flags:
            note = f"\n(Ignored {len(command.flags)} unknown option(s).)"

        stored_account_id = (user.account or {}).get("user_id")
        if stored_account_id is not None:
            try:
                if int(stored_account_id) != int(self.account.user_id):
                    await self._reply(
                        ctx,
                        "⚠️ The connected session no longer matches this account. "
                        "Reconnect it from the control bot.",
                    )
                    return
            except (TypeError, ValueError):
                pass

        archive_size = int(ctx.reply.size or 0)
        if archive_size and archive_size > int(self._settings.max_zip_size_bytes):
            await self._reply(
                ctx,
                "⛔️ That archive is larger than this service's configured ZIP limit "
                f"({format_bytes(self._settings.max_zip_size_bytes)}).",
            )
            return

        active = await self._repos.tasks.list_for_user(
            self.user_id, statuses=list(ACTIVE_TASK_STATUSES), limit=25
        )
        duplicate = next(
            (
                item
                for item in active
                if item.chat_id == ctx.chat_id
                and item.message_id == ctx.reply.message_id
                and item.mode == command.mode
            ),
            None,
        )
        if duplicate is not None:
            await self._reply(
                ctx,
                f"⏳ That archive is already being processed (job {duplicate.task_id[:8]}). "
                "Use /cancel to stop it.",
            )
            return

        previous = await self._repos.tasks.find_latest_for_source(
            self.user_id, ctx.chat_id, ctx.reply.message_id, command.mode
        )
        if previous is not None and not command.retry:
            await self._reply(
                ctx,
                f"✅ This archive was already processed as job {previous.task_id[:8]} "
                f"({previous.status}). Add <code>retry</code> to send it again:\n"
                "<code>/unzip"
                + (" spoiler" if command.spoiler else "")
                + " retry</code>",
            )
            return

        try:
            peer_spec = await self._transport.peer_spec_for(ctx.chat_id, ctx.peer_kind)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("could not resolve peer for task: %s", type(exc).__name__)
            await self._reply(ctx, "⚠️ Could not resolve this chat. Please try again.")
            return

        day = utcnow().strftime("%Y-%m-%d")
        allowed, used, limit = await self._repos.users.consume_daily_task_quota(self.user_id, day)
        if not allowed:
            await self._reply(
                ctx, f"📈 Daily task quota reached ({used}/{limit}). Please try again tomorrow."
            )
            return

        attempt = 0
        try:
            if command.retry:
                attempt = await self._repos.tasks.next_attempt(
                    self.user_id, ctx.chat_id, ctx.reply.message_id, command.mode
                )
            task, created = await self._repos.tasks.create_or_get(
                user_id=self.user_id,
                chat_id=ctx.chat_id,
                message_id=ctx.reply.message_id,
                mode=command.mode,
                peer={"peer_type": ctx.peer_kind, "spec": peer_spec},
                archive={
                    "name": ctx.reply.file_name,
                    "size": archive_size,
                    "command_message_id": ctx.message_id,
                },
                worker_id=self.worker_id,
                attempt=attempt,
            )
        except Exception as exc:  # pragma: no cover - DB hiccup
            await self._repos.users.refund_daily_task_quota(self.user_id)
            logger.warning("could not create task: %s", type(exc).__name__)
            await self._reply(ctx, "⚠️ Could not queue the job. Please try again.")
            return

        if not created:
            await self._repos.users.refund_daily_task_quota(self.user_id)
            await self._reply(
                ctx, f"⏳ Job {task.task_id[:8]} for that archive is already queued."
            )
            return

        if not self.submit(task.task_id):
            await self._repos.tasks.set_status(
                task.task_id,
                TaskStatus.FAILED.value,
                error={"code": "queue_full", "message": "the account queue is full"},
            )
            await self._repos.users.refund_daily_task_quota(self.user_id)
            await self._reply(
                ctx, "🚧 Too many jobs are queued for this account. Try again shortly."
            )
            return

        mode = "spoiler mode" if command.spoiler else "normal mode"
        await self._reply(
            ctx,
            f"📦 Queued job {task.task_id[:8]} ({mode}). "
            f"Progress will be posted in this chat.{note}",
        )

    async def _handle_cancel(self, ctx: CommandContext) -> None:
        command = parse_cancel_command(ctx.text)
        scope = command.scope if command else "latest"

        if scope == "all":
            count = await self._repos.tasks.cancel_all_for_user(self.user_id)
            for task_id in list(self._in_flight) + list(self._reserved):
                self._cancel_local.add(task_id)
            await self._reply(ctx, f"🚫 Cancelling {count} job(s).")
            return

        active = await self._repos.tasks.list_for_user(
            self.user_id, statuses=list(ACTIVE_TASK_STATUSES), limit=25
        )
        if not active:
            await self._reply(ctx, "There is nothing to cancel.")
            return

        if scope.isdigit():
            index = max(1, int(scope))
            target = active[index - 1] if len(active) >= index else None
        else:
            target = active[0]
        if target is None:
            await self._reply(ctx, "No matching job to cancel.")
            return

        await self._repos.tasks.request_cancel(target.task_id)
        self.cancel(target.task_id)
        await self._reply(ctx, f"🚫 Cancelling job {target.task_id[:8]}.")

    async def _reply(self, ctx: CommandContext, text: str) -> None:
        try:
            await self.scheduler.execute(
                "send_text",
                lambda: self._transport.send_text(
                    ctx.chat_id,
                    text,
                    reply_to=ctx.message_id,
                    peer_type=ctx.peer_kind,
                ),
                idempotent=False,
            )
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("could not reply to command: %s", type(exc).__name__)

    # ------------------------------------------------------------------
    def snapshot(self) -> WorkerSnapshot:
        return WorkerSnapshot(
            user_id=self.user_id,
            account_user_id=int(self.account.user_id) if self.account else None,
            username=self.account.username if self.account else None,
            running=self._running,
            connected=self._running,
            queued=self._queue.qsize(),
            active=self.active_task_ids,
            jobs_done=self._jobs_done,
            jobs_failed=self._jobs_failed,
            last_activity=self._last_activity,
            scheduler=self.scheduler.as_dict(),
        )


class UserbotManager:
    """Owns the pool of per-account workers for this process."""

    def __init__(
        self,
        *,
        settings: Any,
        repos: Repositories,
        sessions: SessionManager,
        extractor: ZipExtractor,
        metrics: Optional[Metrics] = None,
    ) -> None:
        self._settings = settings
        self._repos = repos
        self._sessions = sessions
        self._extractor = extractor
        self._metrics = metrics or default_metrics
        self._worker_id = settings.effective_worker_id
        self._workers: Dict[int, UserbotWorker] = {}
        self._lock = asyncio.Lock()
        self._maintenance: Optional[asyncio.Task] = None
        self._running = False
        self._last_recovery = 0.0

    # ------------------------------------------------------------------
    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def worker_count(self) -> int:
        return len(self._workers)

    async def start(self) -> None:
        self._running = True
        await self._repos.workers.heartbeat(self._worker_id, self._health_payload())
        cleanup_stale_temp_dirs(self._settings.temp_path, self._settings.cleanup_stale_temp_hours)
        await self.recover()
        self._last_recovery = time.monotonic()
        self._maintenance = asyncio.create_task(
            self._maintenance_loop(), name="userbot-maintenance"
        )
        logger.info("userbot manager started (worker_id=%s)", self._worker_id)

    async def shutdown(self) -> None:
        self._running = False
        if self._maintenance is not None:
            self._maintenance.cancel()
            await asyncio.gather(self._maintenance, return_exceptions=True)
            self._maintenance = None
        for user_id in list(self._workers):
            try:
                await self._teardown(user_id, reason="shutdown")
            except Exception as exc:  # pragma: no cover - best effort
                logger.warning(
                    "could not stop worker for %s during shutdown: %s",
                    user_id,
                    type(exc).__name__,
                )
        try:
            await self._repos.workers.retire(self._worker_id)
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("could not retire worker record: %s", type(exc).__name__)
        logger.info("userbot manager stopped")

    # ------------------------------------------------------------------
    # worker pool
    # ------------------------------------------------------------------
    async def ensure(self, user_id: int) -> Optional[UserbotWorker]:
        user_id = int(user_id)
        user = await self._repos.users.get(user_id)
        if (
            user is None
            or user.is_banned
            or user.status != AccountStatus.CONNECTED.value
        ):
            return None
        async with self._lock:
            worker = self._workers.get(user_id)
            if worker is not None and worker.running:
                worker.touch()
                return worker
            if worker is not None:
                await self._teardown(user_id, reason="stale-worker", already_locked=True)

            if len(self._workers) >= int(self._settings.max_active_userbots):
                victim = (
                    self._pick_idle_victim()
                    if float(self._settings.idle_disconnect_seconds) > 0
                    else None
                )
                if victim is None:
                    logger.warning(
                        "cannot start a userbot for %s: %d/%d slots in use",
                        user_id,
                        len(self._workers),
                        self._settings.max_active_userbots,
                    )
                    self._metrics.incr("manager.capacity_rejected")
                    return None
                await self._teardown(victim, reason="idle-eviction", already_locked=True)

            acquired = await self._repos.locks.acquire(
                user_id, self._worker_id, self._settings.session_lease_seconds
            )
            if not acquired:
                holder = await self._repos.locks.holder(user_id)
                logger.warning(
                    "session lock for %s is held by %s; skipping", user_id, holder or "another worker"
                )
                self._metrics.incr("manager.lock_contended")
                return None

            connected = None
            worker: Optional[UserbotWorker] = None
            try:
                connected = await self._sessions.create_transport(
                    user_id, requester_id=user_id
                )
                if connected is None:
                    await self._repos.locks.release(user_id, self._worker_id)
                    return None

                worker = UserbotWorker(
                    settings=self._settings,
                    repos=self._repos,
                    user_id=user_id,
                    transport=connected.transport,
                    account=connected.account,
                    extractor=self._extractor,
                    worker_id=self._worker_id,
                    metrics=self._metrics,
                )
                await worker.start()
                self._workers[user_id] = worker
                self._metrics.adjust_gauge("manager.active_workers", 1)
                return worker
            except asyncio.CancelledError:
                if worker is not None:
                    try:
                        await worker.stop(reason="shutdown")
                    except Exception:  # pragma: no cover - best effort
                        pass
                elif connected is not None:
                    try:
                        await connected.transport.disconnect()
                    except Exception:  # pragma: no cover - best effort
                        pass
                await self._repos.locks.release(user_id, self._worker_id)
                raise
            except Exception:
                logger.exception("could not start worker for user %s", user_id)
                if worker is not None:
                    try:
                        await worker.stop(reason="startup-failed")
                    except Exception:  # pragma: no cover - best effort
                        pass
                elif connected is not None:
                    try:
                        await connected.transport.disconnect()
                    except Exception:  # pragma: no cover - best effort
                        pass
                await self._repos.locks.release(user_id, self._worker_id)
                return None

    async def stop_user(self, user_id: int) -> None:
        async with self._lock:
            await self._teardown(int(user_id), reason="user-request", already_locked=True)

    async def submit(self, task: TaskRecord) -> bool:
        worker = await self.ensure(task.user_id)
        if worker is None:
            return False
        if worker.user_id != int(task.user_id):  # pragma: no cover - isolation invariant
            logger.error("worker/user mismatch for task %s", task.task_id)
            return False
        return worker.submit(task.task_id)

    async def cancel(self, user_id: int, task_id: str) -> bool:
        worker = self._workers.get(int(user_id))
        if worker is None:
            return False
        return worker.cancel(task_id)

    async def cancel_all_for_user(self, user_id: int) -> int:
        count = await self._repos.tasks.cancel_all_for_user(int(user_id))
        worker = self._workers.get(int(user_id))
        if worker is not None:
            for task_id in worker.active_task_ids:
                worker.cancel(task_id)
        return count

    def worker_for(self, user_id: int) -> Optional[UserbotWorker]:
        return self._workers.get(int(user_id))

    async def handle_command(self, user_id: int, ctx: CommandContext) -> None:
        worker = self._workers.get(int(user_id))
        if worker is None or not worker.running:
            worker = await self.ensure(int(user_id))
        if worker is None:
            logger.warning("dropping command for %s: no worker available", user_id)
            return
        await worker.handle_command(ctx)

    # ------------------------------------------------------------------
    # recovery + maintenance
    # ------------------------------------------------------------------
    async def recover(self) -> int:
        """Re-queue work that a previous process could not finish."""
        now = utcnow()
        live = await self._repos.workers.active_workers(now - timedelta(seconds=WORKER_LIVE_SECONDS))
        dead_cutoff = now - timedelta(seconds=WORKER_STALE_SECONDS)
        interrupted = await self._repos.tasks.interrupt_stale(live, dead_cutoff)
        for task_id in interrupted:
            await self._repos.steps.unstick_inflight(task_id)
        if interrupted:
            logger.warning("marked %d stale task(s) as interrupted", len(interrupted))

        resumable = await self._repos.tasks.list_resumable(since=now - timedelta(hours=24), limit=200)
        rescheduled = 0
        for task in resumable:
            if task.status == TaskStatus.INTERRUPTED.value:
                await self._repos.steps.unstick_inflight(task.task_id)
                if not await self._repos.tasks.requeue_interrupted(task.task_id):
                    continue
                task.status = TaskStatus.QUEUED.value
            if await self.submit(task):
                rescheduled += 1
        if rescheduled:
            logger.info("re-queued %d task(s) after restart", rescheduled)
        self._metrics.incr("manager.recovered", float(rescheduled))
        return rescheduled

    async def _maintenance_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)
                await self._repos.workers.heartbeat(self._worker_id, self._health_payload())
                await self._sessions.cleanup_expired_logins()
                await self._evict_idle_workers()
                # A fast restart can leave the previous worker's heartbeat
                # looking "live" for a couple of minutes. Re-run recovery
                # periodically so those tasks are eventually requeued.
                if time.monotonic() - self._last_recovery >= max(60.0, WORKER_HEARTBEAT_SECONDS * 6):
                    self._last_recovery = time.monotonic()
                    cleanup_stale_temp_dirs(self._settings.temp_path, self._settings.cleanup_stale_temp_hours)
                    await self.recover()
                self._metrics.gauge("manager.active_workers", float(len(self._workers)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - DB hiccup
                logger.warning("maintenance loop error: %s", type(exc).__name__)

    async def _evict_idle_workers(self) -> None:
        limit = float(self._settings.idle_disconnect_seconds)
        if limit <= 0:
            return
        now = time.monotonic()
        for user_id, worker in list(self._workers.items()):
            if worker.queue_size or worker.active_task_ids:
                continue
            if (now - worker.last_activity) >= limit:
                async with self._lock:
                    if user_id in self._workers and not self._workers[user_id].active_task_ids:
                        await self._teardown(user_id, reason="idle-timeout", already_locked=True)

    def _pick_idle_victim(self) -> Optional[int]:
        candidates = [
            (worker.last_activity, user_id)
            for user_id, worker in self._workers.items()
            if not worker.active_task_ids and not worker.queue_size
        ]
        if not candidates:
            return None
        candidates.sort()
        return candidates[0][1]

    async def _teardown(self, user_id: int, *, reason: str, already_locked: bool = False) -> None:
        async def _do() -> None:
            worker = self._workers.pop(int(user_id), None)
            if worker is None:
                return
            try:
                await worker.stop(reason=reason)
            finally:
                try:
                    await self._repos.locks.release(int(user_id), self._worker_id)
                except Exception as exc:  # pragma: no cover - best effort
                    logger.warning("could not release session lock: %s", type(exc).__name__)
                self._metrics.adjust_gauge("manager.active_workers", -1)
                try:
                    await self._repos.audit.record(
                        user_id,
                        "worker.stopped",
                        target_user_id=int(user_id),
                        detail={"reason": reason},
                    )
                except Exception as exc:  # pragma: no cover - best effort
                    logger.warning(
                        "could not write worker-stop audit record: %s", type(exc).__name__
                    )

        if already_locked:
            await _do()
        else:
            async with self._lock:
                await _do()

    # ------------------------------------------------------------------
    def _health_payload(self) -> Dict[str, Any]:
        return {
            "worker_id": self._worker_id,
            "workers": len(self._workers),
            "active_jobs": sum(len(w.active_task_ids) for w in self._workers.values()),
            "max_active_userbots": self._settings.max_active_userbots,
            "pid": __import__("os").getpid(),
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "worker_id": self._worker_id,
            "workers": [worker.snapshot().as_dict() for worker in self._workers.values()],
            "active_workers": len(self._workers),
            "max_active_userbots": self._settings.max_active_userbots,
        }


# ---------------------------------------------------------------------------
def cleanup_stale_temp_dirs(temp_root: Path, max_age_hours: float) -> int:
    """Remove per-job scratch directories left behind by a crash."""
    root = Path(temp_root)
    if not root.exists():
        return 0
    cutoff = time.time() - max(0.0, float(max_age_hours)) * 3600.0
    removed = 0
    for user_dir in root.iterdir():
        if not user_dir.is_dir():
            continue
        for job_dir in user_dir.iterdir():
            try:
                if job_dir.stat().st_mtime >= cutoff:
                    continue
            except OSError:
                continue
            shutil.rmtree(job_dir, ignore_errors=True)
            removed += 1
        try:
            if not any(user_dir.iterdir()):
                user_dir.rmdir()
        except OSError:
            pass
    if removed:
        logger.info("removed %d stale temp job directory(ies)", removed)
    return removed


__all__ = [
    "UserbotWorker",
    "UserbotManager",
    "WorkerSnapshot",
    "cleanup_stale_temp_dirs",
]

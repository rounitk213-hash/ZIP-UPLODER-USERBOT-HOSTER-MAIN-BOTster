"""Repositories: the only place that talks to MongoDB collections.

Every read/write is scoped by ``user_id`` where user data is involved, which is
what makes strict per-user isolation enforceable (see
:class:`~app.sessions.store.SessionStore` and the worker runner).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from bson import ObjectId
from pymongo import DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.constants import (
    ACTIVE_TASK_STATUSES,
    TERMINAL_TASK_STATUSES,
    AccountStatus,
    StepStatus,
    TaskStatus,
    UnzipMode,
)
from app.db import models as M
from app.db.models import (
    DEFAULT_USER_SETTINGS,
    StepRecord,
    TaskRecord,
    UserRecord,
    empty_task_stats,
    task_dedupe_key,
    utcnow,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------
class UserRepository:
    def __init__(self, db: Any, default_quota_bytes: int, default_daily_tasks: int) -> None:
        self._col = db[M.USERS]
        self._default_quota_bytes = default_quota_bytes
        self._default_daily_tasks = default_daily_tasks

    @staticmethod
    def _defaults(default_quota_bytes: int, default_daily_tasks: int) -> Dict[str, Any]:
        return {
            "quota": {"storage_bytes": default_quota_bytes, "daily_tasks": default_daily_tasks},
            "usage": {
                "day": None,
                "tasks_today": 0,
                "total_tasks": 0,
                "storage_bytes": 0,
                "bytes_sent": 0,
            },
        }

    async def ensure_user(
        self,
        user_id: int,
        *,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        is_admin: bool = False,
    ) -> UserRecord:
        now = utcnow()
        defaults = self._defaults(self._default_quota_bytes, self._default_daily_tasks)
        await self._col.update_one(
            {"_id": int(user_id)},
            {
                "$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "updated_at": now,
                    "last_seen_at": now,
                    "is_admin": bool(is_admin),
                },
                "$setOnInsert": {
                    "status": AccountStatus.NEW.value,
                    "is_banned": False,
                    "consent_given": False,
                    "created_at": now,
                    "settings": dict(DEFAULT_USER_SETTINGS),
                    "account": {},
                    **defaults,
                },
            },
            upsert=True,
        )
        record = await self.get(user_id)
        if record is None:  # pragma: no cover - defensive
            raise RuntimeError("failed to upsert user")
        return record

    async def get(self, user_id: int) -> Optional[UserRecord]:
        doc = await self._col.find_one({"_id": int(user_id)})
        return UserRecord.from_doc(doc) if doc else None

    async def touch(self, user_id: int) -> None:
        await self._col.update_one(
            {"_id": int(user_id)}, {"$set": {"last_seen_at": utcnow(), "updated_at": utcnow()}}
        )

    async def set_status(self, user_id: int, status: str, **extra: Any) -> None:
        update: Dict[str, Any] = {"status": status, "updated_at": utcnow()}
        if status == AccountStatus.CONNECTED.value:
            update["connected_at"] = utcnow()
        if status in (AccountStatus.DISCONNECTED.value, AccountStatus.REVOKED.value):
            update["disconnected_at"] = utcnow()
        update.update(extra)
        await self._col.update_one({"_id": int(user_id)}, {"$set": update})

    async def set_consent(self, user_id: int, given: bool) -> None:
        await self._col.update_one(
            {"_id": int(user_id)},
            {
                "$set": {
                    "consent_given": bool(given),
                    "consent_at": utcnow() if given else None,
                    "updated_at": utcnow(),
                }
            },
        )

    async def set_account(self, user_id: int, account: Dict[str, Any]) -> None:
        await self._col.update_one(
            {"_id": int(user_id)},
            {"$set": {"account": dict(account), "updated_at": utcnow()}},
        )

    async def clear_account(self, user_id: int) -> None:
        await self._col.update_one(
            {"_id": int(user_id)}, {"$set": {"account": {}, "updated_at": utcnow()}}
        )

    async def set_settings(self, user_id: int, **settings: Any) -> None:
        if not settings:
            return
        update = {f"settings.{key}": value for key, value in settings.items()}
        update["updated_at"] = utcnow()
        await self._col.update_one({"_id": int(user_id)}, {"$set": update})

    async def set_quota(
        self,
        user_id: int,
        *,
        storage_bytes: Optional[int] = None,
        daily_tasks: Optional[int] = None,
    ) -> None:
        update: Dict[str, Any] = {"updated_at": utcnow()}
        if storage_bytes is not None:
            update["quota.storage_bytes"] = int(storage_bytes)
        if daily_tasks is not None:
            update["quota.daily_tasks"] = int(daily_tasks)
        await self._col.update_one({"_id": int(user_id)}, {"$set": update})

    async def set_banned(self, user_id: int, banned: bool) -> None:
        await self._col.update_one(
            {"_id": int(user_id)}, {"$set": {"is_banned": bool(banned), "updated_at": utcnow()}}
        )

    # -- quota accounting -------------------------------------------------
    async def consume_daily_task_quota(self, user_id: int, day: str) -> Tuple[bool, int, int]:
        """Atomically consume one task from the user's daily quota.

        Returns ``(allowed, used, limit)``. A limit of ``0`` means unlimited;
        the counter is still maintained for reporting.
        """
        user = await self.get(user_id)
        if user is None:
            return False, 0, 0
        limit = int(user.quota.get("daily_tasks", self._default_daily_tasks) or 0)

        if limit <= 0:
            doc = await self._col.find_one_and_update(
                {"_id": int(user_id), "usage.day": day},
                {"$inc": {"usage.tasks_today": 1, "usage.total_tasks": 1}},
                return_document=ReturnDocument.AFTER,
            )
            if doc is None:
                doc = await self._col.find_one_and_update(
                    {"_id": int(user_id), "usage.day": {"$ne": day}},
                    {
                        "$set": {"usage.day": day, "usage.tasks_today": 1},
                        "$inc": {"usage.total_tasks": 1},
                    },
                    return_document=ReturnDocument.AFTER,
                )
            if doc is None:
                return False, 0, 0
            return True, int((doc.get("usage") or {}).get("tasks_today", 0)), 0

        doc = await self._col.find_one_and_update(
            {
                "_id": int(user_id),
                "usage.day": day,
                "usage.tasks_today": {"$lt": limit},
            },
            {"$inc": {"usage.tasks_today": 1, "usage.total_tasks": 1}},
            return_document=ReturnDocument.AFTER,
        )
        if doc is not None:
            return True, int((doc.get("usage") or {}).get("tasks_today", 0)), limit

        # Either the day rolled over or the quota is exhausted.
        rolled = await self._col.find_one_and_update(
            {"_id": int(user_id), "usage.day": {"$ne": day}},
            {
                "$set": {"usage.day": day, "usage.tasks_today": 1},
                "$inc": {"usage.total_tasks": 1},
            },
            return_document=ReturnDocument.AFTER,
        )
        if rolled is not None:
            return True, 1, limit
        return False, limit, limit

    async def refund_daily_task_quota(self, user_id: int) -> None:
        """Undo a quota consumption when a task could not be queued."""
        await self._col.update_one(
            {"_id": int(user_id), "usage.tasks_today": {"$gt": 0}},
            {"$inc": {"usage.tasks_today": -1, "usage.total_tasks": -1}},
        )

    async def reserve_storage(self, user_id: int, bytes_needed: int) -> Tuple[bool, int, int]:
        """Atomically reserve temporary disk usage.

        Returns ``(allowed, used, limit)``. A limit of ``0`` is unlimited. The
        caller must call :meth:`release_storage` when the job ends.
        """
        needed = max(0, int(bytes_needed))
        user = await self.get(user_id)
        if user is None:
            return False, 0, 0
        limit = int(user.quota.get("storage_bytes", self._default_quota_bytes) or 0)
        if needed == 0:
            used = int((user.usage or {}).get("storage_bytes", 0) or 0)
            return True, used, limit
        if limit <= 0:
            doc = await self._col.find_one_and_update(
                {"_id": int(user_id)},
                {"$inc": {"usage.storage_bytes": needed}},
                return_document=ReturnDocument.AFTER,
            )
            used = int(((doc or {}).get("usage") or {}).get("storage_bytes", needed) or 0)
            return True, used, 0

        doc = await self._col.find_one_and_update(
            {
                "_id": int(user_id),
                "$expr": {
                    "$lte": [
                        {
                            "$add": [
                                {"$ifNull": ["$usage.storage_bytes", 0]},
                                needed,
                            ]
                        },
                        limit,
                    ]
                },
            },
            {"$inc": {"usage.storage_bytes": needed}},
            return_document=ReturnDocument.AFTER,
        )
        if doc is not None:
            return True, int((doc.get("usage") or {}).get("storage_bytes", 0)), limit
        used = await self.storage_usage(user_id)
        return False, used, limit

    async def release_storage(self, user_id: int, bytes_freed: int) -> None:
        """Release a temporary reservation, never letting usage go below zero."""
        amount = max(0, int(bytes_freed))
        if amount == 0:
            return
        result = await self._col.update_one(
            {"_id": int(user_id), "usage.storage_bytes": {"$gte": amount}},
            {"$inc": {"usage.storage_bytes": -amount}},
        )
        if not result.modified_count:
            await self._col.update_one(
                {"_id": int(user_id)}, {"$set": {"usage.storage_bytes": 0}}
            )

    async def reset_all_storage_usage(self) -> int:
        """Clear stale temporary-storage reservations after a hard restart.

        Reservations are held in memory by each running job. A killed process
        cannot release them, so the single supported replica reconciles them at
        startup before any new job can reserve space.
        """
        result = await self._col.update_many(
            {"usage.storage_bytes": {"$gt": 0}},
            {"$set": {"usage.storage_bytes": 0}},
        )
        return int(getattr(result, "modified_count", 0) or 0)

    async def add_storage_usage(self, user_id: int, delta_bytes: int) -> None:
        if not delta_bytes:
            return
        await self._col.update_one(
            {"_id": int(user_id)}, {"$inc": {"usage.storage_bytes": int(delta_bytes)}}
        )

    async def add_bytes_sent(self, user_id: int, delta_bytes: int) -> None:
        if not delta_bytes:
            return
        await self._col.update_one(
            {"_id": int(user_id)}, {"$inc": {"usage.bytes_sent": int(delta_bytes)}}
        )

    async def storage_usage(self, user_id: int) -> int:
        doc = await self._col.find_one({"_id": int(user_id)}, {"usage.storage_bytes": 1})
        if not doc:
            return 0
        return int((doc.get("usage") or {}).get("storage_bytes", 0) or 0)

    # -- listings ---------------------------------------------------------
    async def list_users(self, *, limit: int = 20, skip: int = 0) -> List[UserRecord]:
        cursor = self._col.find({}).sort("created_at", DESCENDING).skip(skip).limit(limit)
        return [UserRecord.from_doc(doc) async for doc in cursor]

    async def list_connected_ids(self) -> List[int]:
        cursor = self._col.find(
            {"status": AccountStatus.CONNECTED.value}, {"_id": 1}
        )
        return [int(doc["_id"]) async for doc in cursor]

    async def total_users(self) -> int:
        return int(await self._col.count_documents({}))

    async def status_breakdown(self) -> Dict[str, int]:
        result: Dict[str, int] = {}
        try:
            pipeline = [{"$group": {"_id": "$status", "n": {"$sum": 1}}}]
            async for row in self._col.aggregate(pipeline):
                result[str(row.get("_id") or "unknown")] = int(row.get("n", 0))
            return result
        except Exception:  # pragma: no cover - fallback for limited servers
            cursor = self._col.find({}, {"status": 1})
            async for doc in cursor:
                key = str(doc.get("status") or "unknown")
                result[key] = result.get(key, 0) + 1
            return result


# ---------------------------------------------------------------------------
# Sessions (encrypted payloads only)
# ---------------------------------------------------------------------------
class SessionRepository:
    def __init__(self, db: Any) -> None:
        self._col = db[M.SESSIONS]

    async def upsert(
        self,
        user_id: int,
        encrypted: bytes,
        *,
        fingerprint: str,
        tg_user_id: Optional[int] = None,
        dc_id: Optional[int] = None,
        phone_masked: Optional[str] = None,
    ) -> None:
        now = utcnow()
        await self._col.update_one(
            {"_id": int(user_id)},
            {
                "$set": {
                    "encrypted": encrypted,
                    "fingerprint": fingerprint,
                    "tg_user_id": tg_user_id,
                    "dc_id": dc_id,
                    "phone_masked": phone_masked,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    async def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        return await self._col.find_one({"_id": int(user_id)})

    async def get_encrypted(self, user_id: int) -> Optional[bytes]:
        doc = await self._col.find_one({"_id": int(user_id)}, {"encrypted": 1})
        if not doc:
            return None
        payload = doc.get("encrypted")
        return bytes(payload) if payload is not None else None

    async def delete(self, user_id: int) -> bool:
        result = await self._col.delete_one({"_id": int(user_id)})
        return bool(result.deleted_count)

    async def count(self) -> int:
        return int(await self._col.count_documents({}))


# ---------------------------------------------------------------------------
# Session locks (prevents two workers using the same Telegram session)
# ---------------------------------------------------------------------------
class SessionLockRepository:
    def __init__(self, db: Any) -> None:
        self._col = db[M.SESSION_LOCKS]

    async def acquire(self, user_id: int, worker_id: str, ttl_seconds: int) -> bool:
        now = utcnow()
        expires = now + timedelta(seconds=int(ttl_seconds))
        try:
            await self._col.insert_one(
                {
                    "_id": int(user_id),
                    "worker_id": worker_id,
                    "acquired_at": now,
                    "heartbeat_at": now,
                    "expires_at": expires,
                }
            )
            return True
        except DuplicateKeyError:
            pass
        result = await self._col.update_one(
            {"_id": int(user_id), "expires_at": {"$lte": now}},
            {"$set": {"worker_id": worker_id, "acquired_at": now, "expires_at": expires}},
        )
        return bool(result.modified_count)

    async def renew(self, user_id: int, worker_id: str, ttl_seconds: int) -> bool:
        result = await self._col.update_one(
            {"_id": int(user_id), "worker_id": worker_id},
            {
                "$set": {
                    "expires_at": utcnow() + timedelta(seconds=int(ttl_seconds)),
                    "heartbeat_at": utcnow(),
                }
            },
        )
        return bool(result.modified_count)

    async def release(self, user_id: int, worker_id: str) -> bool:
        result = await self._col.delete_one({"_id": int(user_id), "worker_id": worker_id})
        return bool(result.deleted_count)

    async def holder(self, user_id: int) -> Optional[str]:
        doc = await self._col.find_one({"_id": int(user_id)})
        if not doc:
            return None
        expires_at = doc.get("expires_at")
        if isinstance(expires_at, datetime) and expires_at <= utcnow():
            return None
        return doc.get("worker_id")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
class TaskRepository:
    def __init__(self, db: Any) -> None:
        self._col = db[M.TASKS]

    async def create_or_get(
        self,
        *,
        user_id: int,
        chat_id: int,
        message_id: int,
        mode: str,
        peer: Dict[str, Any],
        archive: Dict[str, Any],
        worker_id: Optional[str] = None,
        attempt: int = 0,
    ) -> Tuple[TaskRecord, bool]:
        """Create a task, or return the existing active duplicate.

        Returns ``(task, created)``.
        """
        key = task_dedupe_key(user_id, chat_id, message_id, mode, attempt)
        existing = await self._col.find_one({"dedupe_key": key, "active": True})
        if existing:
            return TaskRecord.from_doc(existing), False

        now = utcnow()
        doc = {
            "_id": ObjectId(),
            "user_id": int(user_id),
            "chat_id": int(chat_id),
            "message_id": int(message_id),
            "dedupe_key": key,
            "mode": mode,
            "status": TaskStatus.QUEUED.value,
            "active": True,
            "peer": dict(peer),
            "archive": dict(archive),
            "stats": empty_task_stats(),
            "error": None,
            "cancel_requested": False,
            "attempts": 0,
            "worker_id": worker_id,
            "created_at": now,
            "started_at": None,
            "heartbeat_at": None,
            "finished_at": None,
        }
        try:
            await self._col.insert_one(doc)
        except DuplicateKeyError:
            again = await self._col.find_one({"dedupe_key": key, "active": True})
            if again:
                return TaskRecord.from_doc(again), False
            raise
        return TaskRecord.from_doc(doc), True

    async def find_latest_for_source(
        self, user_id: int, chat_id: int, message_id: int, mode: str
    ) -> Optional[TaskRecord]:
        doc = await self._col.find_one(
            {
                "user_id": int(user_id),
                "chat_id": int(chat_id),
                "message_id": int(message_id),
                "mode": mode,
            },
            sort=[("created_at", DESCENDING)],
        )
        return TaskRecord.from_doc(doc) if doc else None

    async def next_attempt(self, user_id: int, chat_id: int, message_id: int, mode: str) -> int:
        """Return the next explicit-retry generation for this source message."""
        count = await self._col.count_documents(
            {
                "user_id": int(user_id),
                "chat_id": int(chat_id),
                "message_id": int(message_id),
                "mode": mode,
            }
        )
        return int(count) + 1

    async def get(self, task_id: str) -> Optional[TaskRecord]:
        doc = await self._safe_find(task_id)
        return TaskRecord.from_doc(doc) if doc else None

    async def _safe_find(self, task_id: str) -> Optional[Dict[str, Any]]:
        try:
            oid = ObjectId(task_id)
        except Exception:
            return None
        return await self._col.find_one({"_id": oid})

    async def set_status(
        self,
        task_id: str,
        status: str,
        *,
        error: Optional[Dict[str, Any]] = None,
        stats: Optional[Dict[str, Any]] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        oid = ObjectId(task_id)
        update: Dict[str, Any] = {"status": status, "heartbeat_at": utcnow()}
        if worker_id is not None:
            update["worker_id"] = worker_id
        if stats is not None:
            for key, value in stats.items():
                update[f"stats.{key}"] = value
        if error is not None:
            update["error"] = error
        elif status in TERMINAL_TASK_STATUSES:
            update["error"] = None

        if status == TaskStatus.QUEUED.value:
            update["started_at"] = None
        if status in TERMINAL_TASK_STATUSES:
            update["finished_at"] = utcnow()
            update["active"] = False
        await self._col.update_one({"_id": oid}, {"$set": update})
        if status in (
            TaskStatus.DOWNLOADING.value,
            TaskStatus.EXTRACTING.value,
            TaskStatus.SENDING.value,
        ):
            # Keep the *first* start timestamp; a phase transition must not
            # reset it.
            await self._col.update_one(
                {"_id": oid, "started_at": None}, {"$set": {"started_at": utcnow()}}
            )

    async def begin(self, task_id: str, status: str, worker_id: str) -> int:
        """Mark a task as started by this worker; returns attempt count."""
        oid = ObjectId(task_id)
        doc = await self._col.find_one_and_update(
            {"_id": oid},
            {
                "$set": {
                    "status": status,
                    "worker_id": worker_id,
                    "heartbeat_at": utcnow(),
                    "active": True,
                },
                "$inc": {"attempts": 1},
            },
            return_document=ReturnDocument.AFTER,
        )
        await self._col.update_one(
            {"_id": oid, "started_at": None}, {"$set": {"started_at": utcnow()}}
        )
        return int((doc or {}).get("attempts", 1))

    async def heartbeat(self, task_id: str) -> None:
        try:
            oid = ObjectId(task_id)
        except Exception:
            return
        await self._col.update_one({"_id": oid}, {"$set": {"heartbeat_at": utcnow()}})

    async def request_cancel(self, task_id: str) -> bool:
        doc = await self._safe_find(task_id)
        if not doc or doc.get("status") in TERMINAL_TASK_STATUSES:
            return False
        await self._col.update_one(
            {"_id": ObjectId(task_id)}, {"$set": {"cancel_requested": True, "heartbeat_at": utcnow()}}
        )
        return True

    async def cancel_all_for_user(self, user_id: int) -> int:
        result = await self._col.update_many(
            {"user_id": int(user_id), "status": {"$in": list(ACTIVE_TASK_STATUSES)}},
            {"$set": {"cancel_requested": True}},
        )
        return int(result.modified_count)

    async def is_cancel_requested(self, task_id: str) -> bool:
        doc = await self._safe_find(task_id)
        return bool(doc and doc.get("cancel_requested"))

    async def list_for_user(
        self, user_id: int, *, statuses: Optional[Sequence[str]] = None, limit: int = 10
    ) -> List[TaskRecord]:
        query: Dict[str, Any] = {"user_id": int(user_id)}
        if statuses:
            query["status"] = {"$in": list(statuses)}
        cursor = (
            self._col.find(query).sort("created_at", DESCENDING).limit(int(limit))
        )
        return [TaskRecord.from_doc(doc) async for doc in cursor]

    async def list_by_status(self, statuses: Sequence[str], *, limit: int = 50) -> List[TaskRecord]:
        cursor = (
            self._col.find({"status": {"$in": list(statuses)}})
            .sort("created_at", DESCENDING)
            .limit(int(limit))
        )
        return [TaskRecord.from_doc(doc) async for doc in cursor]

    async def count_active(self) -> int:
        return int(
            await self._col.count_documents({"status": {"$in": list(ACTIVE_TASK_STATUSES)}})
        )

    async def status_breakdown(self) -> Dict[str, int]:
        result = {status.value: 0 for status in TaskStatus}
        try:
            pipeline = [{"$group": {"_id": "$status", "n": {"$sum": 1}}}]
            async for row in self._col.aggregate(pipeline):
                result[str(row.get("_id"))] = int(row.get("n", 0))
            return result
        except Exception:  # pragma: no cover
            cursor = self._col.find({}, {"status": 1})
            async for doc in cursor:
                key = str(doc.get("status"))
                result[key] = result.get(key, 0) + 1
            return result

    async def interrupt_stale(self, live_workers: Iterable[str], cutoff: datetime) -> List[str]:
        """Mark tasks owned by dead workers (or without recent heartbeat) interrupted."""
        live = list(live_workers)
        query = {
            "status": {"$in": list(ACTIVE_TASK_STATUSES)},
            "$or": [
                {"worker_id": None},
                {"worker_id": {"$nin": live}},
                {"heartbeat_at": {"$ne": None, "$lt": cutoff}},
            ],
        }
        cursor = self._col.find(query, {"_id": 1})
        task_ids = [str(doc["_id"]) async for doc in cursor]
        if not task_ids:
            return []
        now = utcnow()
        await self._col.update_many(
            {
                "_id": {"$in": [ObjectId(t) for t in task_ids]},
                "status": {"$in": list(ACTIVE_TASK_STATUSES)},
            },
            {
                "$set": {
                    "status": TaskStatus.INTERRUPTED.value,
                    "active": False,
                    "finished_at": now,
                    "error": {
                        "code": "interrupted",
                        "message": "worker stopped before the task finished",
                        "at": now,
                    },
                }
            },
        )
        return task_ids

    async def list_resumable(self, *, since: datetime, limit: int = 200) -> List[TaskRecord]:
        cursor = (
            self._col.find(
                {
                    "status": {"$in": [TaskStatus.QUEUED.value, TaskStatus.INTERRUPTED.value]},
                    "cancel_requested": False,
                    "created_at": {"$gte": since},
                }
            )
            .sort("created_at", 1)
            .limit(int(limit))
        )
        return [TaskRecord.from_doc(doc) async for doc in cursor]

    async def requeue_interrupted(self, task_id: str) -> bool:
        """Re-activate an interrupted task unless a newer job owns the same source.

        Interrupted tasks are terminal (``active=False``) so the partial unique
        index does not block a new retry. Re-activating must therefore be done
        carefully: if the user already queued /unzip retry for the same source,
        the old task is left alone to avoid duplicate delivery.
        """
        try:
            oid = ObjectId(task_id)
        except Exception:
            return False
        doc = await self._col.find_one({"_id": oid})
        if not doc or doc.get("status") != TaskStatus.INTERRUPTED.value:
            return False
        other = await self._col.find_one(
            {
                "_id": {"$ne": oid},
                "user_id": doc.get("user_id"),
                "chat_id": doc.get("chat_id"),
                "message_id": doc.get("message_id"),
                "mode": doc.get("mode"),
                "active": True,
            },
            {"_id": 1},
        )
        if other is not None:
            return False
        try:
            result = await self._col.update_one(
                {"_id": oid, "status": TaskStatus.INTERRUPTED.value},
                {
                    "$set": {
                        "status": TaskStatus.QUEUED.value,
                        "active": True,
                        "finished_at": None,
                        "worker_id": None,
                        "heartbeat_at": None,
                        "error": None,
                    }
                },
            )
        except DuplicateKeyError:  # pragma: no cover - concurrent retry
            return False
        return bool(result.modified_count)

    async def created_since(self, since: datetime) -> int:
        return int(await self._col.count_documents({"created_at": {"$gte": since}}))

    async def purge_finished_before(self, cutoff: datetime) -> int:
        result = await self._col.delete_many(
            {"finished_at": {"$lt": cutoff}, "active": False}
        )
        return int(result.deleted_count)


# ---------------------------------------------------------------------------
# Task steps (one per extracted video, used for idempotent delivery)
# ---------------------------------------------------------------------------
class TaskStepRepository:
    def __init__(self, db: Any) -> None:
        self._col = db[M.TASK_STEPS]

    async def ensure_steps(
        self, task_id: str, user_id: int, items: Sequence[Dict[str, Any]]
    ) -> int:
        if not items:
            return 0
        oid = ObjectId(task_id)
        created = 0
        for item in items:
            try:
                result = await self._col.update_one(
                    {"task_id": oid, "rel_path": item["rel_path"]},
                    {
                        "$setOnInsert": {
                            "user_id": int(user_id),
                            "rel_path": item["rel_path"],
                            "size": int(item.get("size", 0)),
                            "status": StepStatus.QUEUED.value,
                            "attempts": 0,
                            "message_id": None,
                            "error": None,
                            "random_id": int(item.get("random_id", 0)),
                            "updated_at": utcnow(),
                        }
                    },
                    upsert=True,
                )
                if result.upserted_id is not None:
                    created += 1
            except DuplicateKeyError:  # pragma: no cover - concurrent creator
                continue
        return created

    async def claim(
        self,
        task_id: str,
        rel_path: str,
        *,
        max_attempts: int = 3,
        allow_uncertain: bool = False,
    ) -> Optional[StepRecord]:
        """Atomically claim a step for sending. Returns ``None`` if not claimable."""
        statuses = [StepStatus.QUEUED.value, StepStatus.FAILED.value]
        if allow_uncertain:
            statuses.append(StepStatus.UNCERTAIN.value)
        doc = await self._col.find_one_and_update(
            {
                "task_id": ObjectId(task_id),
                "rel_path": rel_path,
                "status": {"$in": statuses},
                "attempts": {"$lt": int(max_attempts)},
            },
            {"$set": {"status": StepStatus.SENDING.value, "updated_at": utcnow()}, "$inc": {"attempts": 1}},
            return_document=ReturnDocument.AFTER,
        )
        return StepRecord.from_doc(doc) if doc else None

    async def mark(
        self,
        task_id: str,
        rel_path: str,
        status: str,
        *,
        message_id: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        update: Dict[str, Any] = {"status": status, "updated_at": utcnow(), "error": error}
        if message_id is not None:
            update["message_id"] = int(message_id)
        await self._col.update_one(
            {"task_id": ObjectId(task_id), "rel_path": rel_path}, {"$set": update}
        )

    async def mark_sent(self, task_id: str, rel_path: str, message_id: Optional[int]) -> None:
        await self.mark(task_id, rel_path, StepStatus.SENT.value, message_id=message_id)

    async def list_for_task(self, task_id: str) -> List[StepRecord]:
        cursor = self._col.find({"task_id": ObjectId(task_id)}).sort("rel_path", 1)
        return [StepRecord.from_doc(doc) async for doc in cursor]

    async def delete_for_task(self, task_id: str) -> int:
        result = await self._col.delete_many({"task_id": ObjectId(task_id)})
        return int(getattr(result, "deleted_count", 0) or 0)

    async def list_by_status(self, task_id: str, statuses: Sequence[str]) -> List[StepRecord]:
        cursor = self._col.find(
            {"task_id": ObjectId(task_id), "status": {"$in": list(statuses)}}
        ).sort("rel_path", 1)
        return [StepRecord.from_doc(doc) async for doc in cursor]

    async def counts(self, task_id: str) -> Dict[str, int]:
        result = {status.value: 0 for status in StepStatus}
        try:
            pipeline = [
                {"$match": {"task_id": ObjectId(task_id)}},
                {"$group": {"_id": "$status", "n": {"$sum": 1}}},
            ]
            async for row in self._col.aggregate(pipeline):
                result[str(row.get("_id"))] = int(row.get("n", 0))
        except Exception:  # pragma: no cover
            async for doc in self._col.find({"task_id": ObjectId(task_id)}, {"status": 1}):
                key = str(doc.get("status"))
                result[key] = result.get(key, 0) + 1
        return result

    async def unstick_inflight(self, task_id: str) -> int:
        """After a crash, steps stuck in ``sending`` become ``uncertain``.

        They are deliberately *not* retried automatically (that could duplicate
        a video that Telegram already delivered); the user can force a retry.
        """
        result = await self._col.update_many(
            {"task_id": ObjectId(task_id), "status": StepStatus.SENDING.value},
            {
                "$set": {
                    "status": StepStatus.UNCERTAIN.value,
                    "error": "worker restarted while sending; not retried automatically",
                    "updated_at": utcnow(),
                }
            },
        )
        return int(result.modified_count)

    async def purge_for_task(self, task_id: str) -> int:
        result = await self._col.delete_many({"task_id": ObjectId(task_id)})
        return int(result.deleted_count)


# ---------------------------------------------------------------------------
# Worker health + audit
# ---------------------------------------------------------------------------
class WorkerHealthRepository:
    def __init__(self, db: Any) -> None:
        self._col = db[M.HEALTH]

    async def heartbeat(self, worker_id: str, info: Dict[str, Any]) -> None:
        await self._col.update_one(
            {"_id": worker_id},
            {"$set": {"info": info, "updated_at": utcnow()}},
            upsert=True,
        )

    async def active_workers(self, cutoff: datetime) -> List[str]:
        cursor = self._col.find({"updated_at": {"$gte": cutoff}}, {"_id": 1})
        return [str(doc["_id"]) async for doc in cursor]

    async def retire(self, worker_id: str) -> None:
        await self._col.delete_one({"_id": worker_id})


class AuditRepository:
    def __init__(self, db: Any) -> None:
        self._col = db[M.AUDIT]

    async def record(
        self,
        actor_id: Optional[int],
        action: str,
        *,
        target_user_id: Optional[int] = None,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        await self._col.insert_one(
            {
                "ts": utcnow(),
                "actor_id": int(actor_id) if actor_id is not None else None,
                "action": action,
                "target_user_id": int(target_user_id) if target_user_id is not None else None,
                "detail": detail or {},
            }
        )

    async def recent(self, limit: int = 20) -> List[Dict[str, Any]]:
        cursor = self._col.find({}).sort("ts", DESCENDING).limit(int(limit))
        return [doc async for doc in cursor]

    async def for_user(self, user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        cursor = (
            self._col.find({"target_user_id": int(user_id)}).sort("ts", DESCENDING).limit(int(limit))
        )
        return [doc async for doc in cursor]


# ---------------------------------------------------------------------------
# Container
# ---------------------------------------------------------------------------
@dataclass
class Repositories:
    users: UserRepository
    sessions: SessionRepository
    locks: SessionLockRepository
    tasks: TaskRepository
    steps: TaskStepRepository
    workers: WorkerHealthRepository
    audit: AuditRepository

    @classmethod
    def create(
        cls,
        db: Any,
        *,
        default_storage_quota_bytes: int = 0,
        default_daily_task_quota: int = 0,
    ) -> "Repositories":
        return cls(
            users=UserRepository(db, default_storage_quota_bytes, default_daily_task_quota),
            sessions=SessionRepository(db),
            locks=SessionLockRepository(db),
            tasks=TaskRepository(db),
            steps=TaskStepRepository(db),
            workers=WorkerHealthRepository(db),
            audit=AuditRepository(db),
        )


def is_mode_valid(mode: str) -> bool:
    return mode in (UnzipMode.PLAIN.value, UnzipMode.SPOILER.value)

"""
Database models and schema for job tracking.
Uses SQLite with aiosqlite for async operations.
"""

import aiosqlite
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from enum import Enum


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"


class JobStage(str, Enum):
    INIT = "init"
    RESOLVE = "resolve"
    DOWNLOAD = "download"
    POSTPROCESS = "postprocess"
    DONE = "done"


class EpisodeStatus(str, Enum):
    PENDING = "pending"
    GET_STREAM = "get_stream"
    DOWNLOAD_VIDEO = "download_video"
    MERGE_VIDEO = "merge_video"
    DOWNLOAD_SUBTITLES = "download_subtitles"
    COMPLETE = "complete"
    FAILED = "failed"


STAGE_PROGRESS = {
    JobStage.INIT: 5,
    JobStage.RESOLVE: 15,
    JobStage.DOWNLOAD: 30,
    JobStage.POSTPROCESS: 95,
    JobStage.DONE: 100,
}

EPISODE_STATUS_LABELS = {
    EpisodeStatus.PENDING: "Waiting",
    EpisodeStatus.GET_STREAM: "Finding stream",
    EpisodeStatus.DOWNLOAD_VIDEO: "Downloading",
    EpisodeStatus.MERGE_VIDEO: "Merging",
    EpisodeStatus.DOWNLOAD_SUBTITLES: "Subtitles",
    EpisodeStatus.COMPLETE: "Complete",
    EpisodeStatus.FAILED: "Failed",
}


class Database:
    # Whitelist of valid column names for updates (prevents SQL injection)
    VALID_COLUMNS = {
        'url', 'profile', 'extra_args', 'status', 'stage',
        'progress_percent', 'progress_text', 'created_at',
        'started_at', 'finished_at', 'error_message', 'log_file', 'pid'
    }

    def __init__(self, db_path: str):
        self.db_path = db_path
        # Try to create parent directory, but don't fail if we can't
        try:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            pass  # Will try again when actually opening the database

    async def init_db(self):
        """Initialize database schema."""
        # Ensure parent directory exists
        try:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            pass  # Try anyway, might work if directory already exists

        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            # Enable WAL mode for better concurrent access
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
            await db.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL,
                    profile TEXT,
                    extra_args TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    stage TEXT,
                    progress_percent INTEGER DEFAULT 0,
                    progress_text TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error_message TEXT,
                    log_file TEXT,
                    pid INTEGER
                )
            """)

            await db.execute("""
                CREATE TABLE IF NOT EXISTS episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    episode_number INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    progress_percent INTEGER DEFAULT 0,
                    stage_data TEXT,
                    error_message TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
            """)

            # Add stage_data column if it doesn't exist (migration)
            try:
                await db.execute("ALTER TABLE episodes ADD COLUMN stage_data TEXT")
            except aiosqlite.OperationalError:
                pass  # Column already exists

            # Add log_file column if it doesn't exist (migration)
            try:
                await db.execute("ALTER TABLE episodes ADD COLUMN log_file TEXT")
            except aiosqlite.OperationalError:
                pass  # Column already exists

            await db.commit()

    async def create_job(
        self,
        url: str,
        profile: Optional[str] = None,
        extra_args: Optional[str] = None,
    ) -> int:
        """Create a new job."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            cursor = await db.execute(
                """
                INSERT INTO jobs (url, profile, extra_args, status, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (url, profile, extra_args, JobStatus.QUEUED.value, datetime.utcnow().isoformat()),
            )
            await db.commit()
            return cursor.lastrowid

    async def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get job by ID."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_jobs(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all jobs."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def update_job(self, job_id: int, **kwargs):
        """Update job fields."""
        if not kwargs:
            return

        # Validate all column names against whitelist (prevents SQL injection)
        invalid_columns = set(kwargs.keys()) - self.VALID_COLUMNS
        if invalid_columns:
            raise ValueError(f"Invalid column names: {invalid_columns}")

        fields = []
        values = []
        for key, value in kwargs.items():
            # key is already validated against whitelist above
            fields.append(f"{key} = ?")
            values.append(value)

        values.append(job_id)
        query = f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?"

        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            await db.execute(query, values)
            await db.commit()

    async def update_progress(
        self,
        job_id: int,
        percent: int,
        stage: Optional[str] = None,
        text: Optional[str] = None,
    ):
        """Update job progress."""
        updates = {"progress_percent": percent}
        if stage:
            updates["stage"] = stage
        if text:
            updates["progress_text"] = text
        await self.update_job(job_id, **updates)

    async def claim_job(self, job_id: int) -> bool:
        """
        Atomically claim a queued job for execution (prevents race conditions).

        Returns:
            True if job was successfully claimed, False if it was already claimed/running.
        """
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            # Use a transaction to ensure atomicity
            cursor = await db.execute(
                """
                UPDATE jobs
                SET status = ?, started_at = ?
                WHERE id = ? AND status = ?
                """,
                (JobStatus.RUNNING.value, datetime.utcnow().isoformat(),
                 job_id, JobStatus.QUEUED.value)
            )
            await db.commit()
            # If no rows were updated, job was already claimed
            return cursor.rowcount > 0

    async def start_job(self, job_id: int, pid: int, log_file: str):
        """Update running job with process details."""
        await self.update_job(
            job_id,
            pid=pid,
            log_file=log_file,
            stage=JobStage.INIT.value,
            progress_percent=STAGE_PROGRESS[JobStage.INIT],
        )

    async def finish_job(self, job_id: int, success: bool, error_message: Optional[str] = None):
        """Mark job as finished and update incomplete episodes if failed."""
        await self.update_job(
            job_id,
            status=JobStatus.SUCCESS.value if success else JobStatus.FAILED.value,
            finished_at=datetime.utcnow().isoformat(),
            error_message=error_message,
            progress_percent=100 if success else None,
            stage=JobStage.DONE.value if success else None,
        )

        # Mark all incomplete episodes as failed when job fails
        if not success:
            await self.cancel_job_episodes(job_id)

    async def cancel_job(self, job_id: int):
        """Mark job as canceled and update all incomplete episodes."""
        await self.update_job(
            job_id,
            status=JobStatus.CANCELED.value,
            finished_at=datetime.utcnow().isoformat(),
        )

        # Mark all incomplete episodes as failed
        await self.cancel_job_episodes(job_id)

    async def get_active_jobs(self) -> List[Dict[str, Any]]:
        """Get all queued or running jobs."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM jobs WHERE status IN (?, ?) ORDER BY created_at ASC",
                (JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def delete_all_jobs_except_running(self) -> tuple[int, int]:
        """
        Delete all jobs except those with status 'running'.

        Returns:
            tuple[int, int]: (deleted_count, skipped_count)
        """
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            # First, count running jobs (these will be skipped)
            cursor = await db.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = ?",
                (JobStatus.RUNNING.value,)
            )
            skipped_count = (await cursor.fetchone())[0]

            # Delete all jobs except running ones
            cursor = await db.execute(
                "DELETE FROM jobs WHERE status != ?",
                (JobStatus.RUNNING.value,)
            )
            deleted_count = cursor.rowcount
            await db.commit()

            return deleted_count, skipped_count

    # Episode management methods
    async def create_episode(
        self,
        job_id: int,
        episode_number: int,
        title: str,
    ) -> int:
        """Create a new episode for a job."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            cursor = await db.execute(
                """
                INSERT INTO episodes (job_id, episode_number, title, status, progress_percent)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, episode_number, title, EpisodeStatus.PENDING.value, 0),
            )
            await db.commit()
            return cursor.lastrowid

    async def get_episode(self, episode_id: int) -> Optional[Dict[str, Any]]:
        """Get episode by ID."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM episodes WHERE id = ?", (episode_id,))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_job_episodes(self, job_id: int) -> List[Dict[str, Any]]:
        """Get all episodes for a job, ordered by episode number."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM episodes WHERE job_id = ? ORDER BY episode_number ASC",
                (job_id,),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def update_episode(
        self,
        episode_id: int,
        status: Optional[str] = None,
        progress_percent: Optional[int] = None,
        error_message: Optional[str] = None,
        stage_data: Optional[Dict[str, Any]] = None,
        log_file: Optional[str] = None,
    ):
        """Update episode status and progress."""
        updates = {}
        if status is not None:
            updates["status"] = status
            if status == EpisodeStatus.COMPLETE.value:
                updates["finished_at"] = datetime.utcnow().isoformat()
                updates["progress_percent"] = 100
            elif status in (EpisodeStatus.GET_STREAM.value, EpisodeStatus.DOWNLOAD_VIDEO.value, EpisodeStatus.MERGE_VIDEO.value, EpisodeStatus.DOWNLOAD_SUBTITLES.value):
                if not updates.get("started_at"):
                    updates["started_at"] = datetime.utcnow().isoformat()
            elif status == EpisodeStatus.FAILED.value:
                updates["finished_at"] = datetime.utcnow().isoformat()

        if progress_percent is not None:
            updates["progress_percent"] = progress_percent

        if error_message is not None:
            updates["error_message"] = error_message

        if stage_data is not None:
            updates["stage_data"] = json.dumps(stage_data)

        if log_file is not None:
            updates["log_file"] = log_file

        if not updates:
            return

        fields = [f"{key} = ?" for key in updates.keys()]
        values = list(updates.values())
        values.append(episode_id)

        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            await db.execute(
                f"UPDATE episodes SET {', '.join(fields)} WHERE id = ?",
                values,
            )
            await db.commit()

    async def find_episode_by_number(self, job_id: int, episode_number: int) -> Optional[Dict[str, Any]]:
        """Find episode by job ID and episode number."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM episodes WHERE job_id = ? AND episode_number = ?",
                (job_id, episode_number),
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def cancel_job_episodes(self, job_id: int):
        """Mark all incomplete episodes as failed when job is cancelled."""
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            await db.execute(
                """
                UPDATE episodes
                SET status = ?, error_message = ?, finished_at = ?
                WHERE job_id = ? AND status NOT IN (?, ?)
                """,
                (
                    EpisodeStatus.FAILED.value,
                    "Job was cancelled",
                    datetime.utcnow().isoformat(),
                    job_id,
                    EpisodeStatus.COMPLETE.value,
                    EpisodeStatus.FAILED.value,
                ),
            )
            await db.commit()

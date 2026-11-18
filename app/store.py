from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    station TEXT NOT NULL,
    date TEXT NOT NULL,
    state TEXT NOT NULL,
    source_url TEXT,
    source_revision TEXT,
    source_etag TEXT,
    expected_size INTEGER,
    expected_sha256 TEXT,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    part_path TEXT,
    package_path TEXT,
    verified_size INTEGER,
    verified_sha256 TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    resyncs INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (station, date)
);

CREATE TABLE IF NOT EXISTS task_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    at TEXT NOT NULL,
    event TEXT NOT NULL,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id, seq);

CREATE TABLE IF NOT EXISTS qc_reports (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    station TEXT NOT NULL,
    date TEXT NOT NULL,
    state TEXT NOT NULL,
    package_sha256 TEXT NOT NULL,
    package_size INTEGER NOT NULL,
    snapshot_path TEXT NOT NULL,
    result TEXT,
    report_json TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_qc_reports_task ON qc_reports(task_id, created_at);

-- At most one active (queued/running) QC job per task and package digest.
CREATE UNIQUE INDEX IF NOT EXISTS idx_qc_reports_active
    ON qc_reports(task_id, package_sha256) WHERE state IN ('queued', 'running');
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Store:
    """SQLite persistence with one connection per thread.

    The API and the download workers touch tasks concurrently.  Every mutation is
    guarded by a compare-and-set on (state, attempt) so a response that arrives
    after a pause/cancel can never move the task forward.
    """

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        with self.connection() as conn:
            conn.executescript(SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")

    def connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connection()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # -- queries ---------------------------------------------------------
    def get(self, task_id: str) -> sqlite3.Row | None:
        return self.connection().execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()

    def find(self, station: str, date: str) -> sqlite3.Row | None:
        return (
            self.connection()
            .execute("SELECT * FROM tasks WHERE station=? AND date=?", (station, date))
            .fetchone()
        )

    def list(self, state: str | None = None) -> list[sqlite3.Row]:
        if state is None:
            return self.connection().execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()
        return (
            self.connection()
            .execute("SELECT * FROM tasks WHERE state=? ORDER BY created_at DESC", (state,))
            .fetchall()
        )

    # -- mutations -------------------------------------------------------
    def insert_task(self, task_id: str, station: str, date: str) -> None:
        now = utcnow()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO tasks (id, station, date, state, created_at, updated_at) "
                "VALUES (?, ?, ?, 'queued', ?, ?)",
                (task_id, station, date, now, now),
            )
            self._event(conn, task_id, "queued", f"{station}/{date}", now)

    def update(self, task_id: str, **fields: Any) -> bool:
        if not fields:
            return False
        fields["updated_at"] = utcnow()
        assignments = ", ".join(f"{name}=?" for name in fields)
        values = list(fields.values()) + [task_id]
        with self.transaction() as conn:
            cur = conn.execute(f"UPDATE tasks SET {assignments} WHERE id=?", values)
            return cur.rowcount == 1

    def update_guarded(self, task_id: str, generation: int, **fields: Any) -> bool:
        """Worker-side mutation, valid only for the live generation.

        Pausing bumps ``generation`` before the HTTP request returns, so no
        chunk or metadata write from a stale response can ever be committed
        after the pause is acknowledged.
        """
        if not fields:
            return False
        fields["updated_at"] = utcnow()
        assignments = ", ".join(f"{name}=?" for name in fields)
        values = list(fields.values()) + [task_id, generation]
        with self.transaction() as conn:
            cur = conn.execute(
                f"UPDATE tasks SET {assignments} "
                "WHERE id=? AND state='running' AND generation=?",
                values,
            )
            return cur.rowcount == 1

    def cas_state(
        self,
        task_id: str,
        expected_state: str | tuple[str, ...],
        new_state: str,
        *,
        expected_generation: int | None = None,
        **fields: Any,
    ) -> bool:
        """Conditional transition: only a task still in ``expected_state`` may move."""
        expected = (
            (expected_state,) if isinstance(expected_state, str) else tuple(expected_state)
        )
        fields = {**fields, "state": new_state, "updated_at": utcnow()}
        assignments = ", ".join(f"{name}=?" for name in fields)
        placeholders = ", ".join("?" for _ in expected)
        sql = f"UPDATE tasks SET {assignments} WHERE id=? AND state IN ({placeholders})"
        values = list(fields.values()) + [task_id, *expected]
        if expected_generation is not None:
            sql += " AND generation=?"
            values.append(expected_generation)
        with self.transaction() as conn:
            cur = conn.execute(sql, values)
            if cur.rowcount != 1:
                return False
            self._event(conn, task_id, new_state, fields.get("error"), fields["updated_at"])
            return True

    def event(self, task_id: str, event: str, detail: str | None = None) -> None:
        with self.transaction() as conn:
            self._event(conn, task_id, event, detail, utcnow())

    def events(self, task_id: str) -> list[sqlite3.Row]:
        return (
            self.connection()
            .execute("SELECT * FROM task_events WHERE task_id=? ORDER BY seq", (task_id,))
            .fetchall()
        )

    # -- qc reports ------------------------------------------------------
    def insert_qc_report(
        self,
        *,
        report_id: str,
        task_id: str,
        station: str,
        date: str,
        package_sha256: str,
        package_size: int,
        snapshot_path: str,
    ) -> None:
        now = utcnow()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO qc_reports "
                "(id, task_id, station, date, state, package_sha256, package_size, "
                " snapshot_path, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)",
                (
                    report_id,
                    task_id,
                    station,
                    date,
                    package_sha256,
                    package_size,
                    snapshot_path,
                    now,
                    now,
                ),
            )

    def get_qc_report(self, report_id: str) -> sqlite3.Row | None:
        return (
            self.connection()
            .execute("SELECT * FROM qc_reports WHERE id=?", (report_id,))
            .fetchone()
        )

    def list_qc_reports(self, task_id: str) -> list[sqlite3.Row]:
        return (
            self.connection()
            .execute(
                "SELECT * FROM qc_reports WHERE task_id=? "
                "ORDER BY created_at DESC, rowid DESC",
                (task_id,),
            )
            .fetchall()
        )

    def active_qc_report(self, task_id: str, package_sha256: str) -> sqlite3.Row | None:
        """The queued/running report for exactly this package digest, if any."""
        return (
            self.connection()
            .execute(
                "SELECT * FROM qc_reports "
                "WHERE task_id=? AND package_sha256=? AND state IN ('queued', 'running') "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (task_id, package_sha256),
            )
            .fetchone()
        )

    def latest_qc_report_for_sha(self, task_id: str, package_sha256: str) -> sqlite3.Row | None:
        """Newest report (any state) accepted for exactly this package digest."""
        return (
            self.connection()
            .execute(
                "SELECT * FROM qc_reports WHERE task_id=? AND package_sha256=? "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (task_id, package_sha256),
            )
            .fetchone()
        )

    def qc_reports_in_state(self, state: str) -> list[sqlite3.Row]:
        return (
            self.connection()
            .execute(
                "SELECT * FROM qc_reports WHERE state=? ORDER BY created_at, rowid",
                (state,),
            )
            .fetchall()
        )

    def qc_cas(
        self,
        report_id: str,
        expected_state: str | tuple[str, ...],
        new_state: str,
        **fields: Any,
    ) -> bool:
        """Conditional QC report transition, mirroring :meth:`cas_state`."""
        expected = (
            (expected_state,) if isinstance(expected_state, str) else tuple(expected_state)
        )
        fields = {**fields, "state": new_state, "updated_at": utcnow()}
        assignments = ", ".join(f"{name}=?" for name in fields)
        placeholders = ", ".join("?" for _ in expected)
        sql = f"UPDATE qc_reports SET {assignments} WHERE id=? AND state IN ({placeholders})"
        values = list(fields.values()) + [report_id, *expected]
        with self.transaction() as conn:
            cur = conn.execute(sql, values)
            return cur.rowcount == 1

    @staticmethod
    def _event(conn: sqlite3.Connection, task_id: str, event: str, detail: str | None, at: str) -> None:
        conn.execute(
            "INSERT INTO task_events (task_id, at, event, detail) VALUES (?, ?, ?, ?)",
            (task_id, at, event, detail),
        )

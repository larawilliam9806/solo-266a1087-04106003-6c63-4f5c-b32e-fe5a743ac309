"""Background quality control (QC) for collected observation archives.

A QC job is *accepted* only for a task whose package is completed and verified.
At acceptance the package bytes are snapshotted into the QC data directory and
the report row records the package SHA-256.  From then on the job — no matter
whether the task is refreshed (successfully or not) or the process restarts —
checks exactly those accepted bytes, never a newer package.

Invariants
----------
* Acceptance re-verifies the on-disk package against the task's verified
  SHA-256 and refuses anything that is not a completed, verified package.
* The snapshot is written to a temp file, fsynced and atomically renamed; the
  worker re-checks the snapshot digest against the recorded SHA-256 before
  analysing it, so a report always describes the accepted original package.
* A report becomes ``completed`` only in the same transaction that stores the
  fully generated report body, so a crash can never leave a half-written
  report marked done.  Queued and interrupted (running) jobs are requeued and
  rerun at startup — QC is deterministic over the immutable snapshot.
* Reports are immutable and never deleted.  Whether a report applies to the
  task's *current* package is decided at query time by comparing SHA-256
  digests, so a late-finishing job for an old revision can never override the
  conclusion for a newer package.

QC rules (``observations.csv`` inside the tar.gz, existing column names
``station,date,time,temperature_c,pressure_hpa,wind_mps``)
--------------------------------------------------------------------------
* ``station`` and ``date`` of every row must equal the task's station/date.
* The day must contain exactly one row for each of the 96 quarter-hour slots
  (00:00, 00:15, ..., 23:45): duplicates and missing slots fail the package.
* ``temperature_c``, ``pressure_hpa`` and ``wind_mps`` must be finite numbers.
* Any duplicate, missing, format or value error fails the report; the report
  lists the offending 1-based line numbers and the missing time slots.  A
  missing ``observations.csv`` (or an unreadable archive) is reported as well.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import math
import os
import re
import sqlite3
import tarfile
import uuid
import zlib
from pathlib import Path
from typing import Any

from .config import Settings
from .models import sha256_file
from .store import Store, utcnow

log = logging.getLogger("collector.qc")

CSV_MEMBER = "observations.csv"
EXPECTED_COLUMNS = ["station", "date", "time", "temperature_c", "pressure_hpa", "wind_mps"]
NUMERIC_FIELDS = ("temperature_c", "pressure_hpa", "wind_mps")
TIME_RE = re.compile(r"^([01][0-9]|2[0-3]):(00|15|30|45)$")
SLOTS_PER_DAY = 96
MAX_ERRORS_KEPT = 1000
MAX_CSV_BYTES = 64 * 1024 * 1024


class QcConflict(Exception):
    """The task/package state does not allow accepting a QC job."""


class _CsvTooLarge(Exception):
    """observations.csv exceeds the safety limit."""


def expected_slots() -> list[str]:
    """The 96 quarter-hour slots of one day, as ``HH:MM`` strings."""
    return [f"{hour:02d}:{minute:02d}" for hour in range(24) for minute in (0, 15, 30, 45)]


def _read_csv_bytes(package_path: Path) -> bytes | None:
    """Extract the observations.csv member, or None if the archive lacks it."""
    with tarfile.open(package_path, "r:gz") as archive:
        member = next(
            (
                item
                for item in archive.getmembers()
                if item.isfile() and item.name in {CSV_MEMBER, f"./{CSV_MEMBER}"}
            ),
            None,
        )
        if member is None:
            return None
        if member.size > MAX_CSV_BYTES:
            raise _CsvTooLarge(
                f"{CSV_MEMBER} exceeds the {MAX_CSV_BYTES}-byte safety limit"
            )
        handle = archive.extractfile(member)
        if handle is None:
            return None
        return handle.read()


def _failed_report(station: str, date: str, kind: str, message: str) -> dict[str, Any]:
    return {
        "station": station,
        "date": date,
        "result": "fail",
        "row_count": 0,
        "valid_rows": 0,
        "slots_expected": SLOTS_PER_DAY,
        "slots_covered": 0,
        "error_count": 1,
        "errors": [{"line": None, "kind": kind, "message": message}],
        "errors_truncated": False,
        "missing_times": [],
        "generated_at": utcnow(),
    }


def analyze_package(package_path: Path, station: str, date: str) -> dict[str, Any]:
    """Build the full QC report for one package snapshot.

    Data problems never raise: they become a ``fail`` report.  Only genuine
    I/O errors on the snapshot itself propagate (the caller turns those into a
    job-level failure, not a QC verdict).
    """
    try:
        data = _read_csv_bytes(package_path)
    except _CsvTooLarge as exc:
        return _failed_report(station, date, "format", str(exc))
    except (tarfile.TarError, OSError, EOFError, zlib.error) as exc:
        return _failed_report(
            station, date, "archive_error", f"cannot read tar.gz archive: {exc}"
        )
    if data is None:
        return _failed_report(
            station, date, "missing_file", f"{CSV_MEMBER} not found in archive"
        )
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        return _failed_report(
            station, date, "encoding_error", f"{CSV_MEMBER} is not valid UTF-8: {exc}"
        )
    return analyze_csv(text, station, date)


def analyze_csv(text: str, station: str, date: str) -> dict[str, Any]:
    """Check one observations.csv text against the task's station and date."""
    errors: list[dict[str, Any]] = []
    error_count = 0
    truncated = False

    def add(line: int | None, kind: str, message: str) -> None:
        nonlocal error_count, truncated
        error_count += 1
        if len(errors) < MAX_ERRORS_KEPT:
            errors.append({"line": line, "kind": kind, "message": message})
        else:
            truncated = True

    def finish(row_count: int, valid_rows: int, covered: set[str]) -> dict[str, Any]:
        missing = [slot for slot in expected_slots() if slot not in covered]
        return {
            "station": station,
            "date": date,
            "result": "pass" if error_count == 0 and not missing else "fail",
            "row_count": row_count,
            "valid_rows": valid_rows,
            "slots_expected": SLOTS_PER_DAY,
            "slots_covered": len(covered),
            "error_count": error_count,
            "errors": errors,
            "errors_truncated": truncated,
            "missing_times": missing,
            "generated_at": utcnow(),
        }

    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if header is None:
        add(None, "empty_file", f"{CSV_MEMBER} is empty")
        return finish(0, 0, set())
    columns = [cell.strip() for cell in header]
    index: dict[str, int] = {}
    for position, name in enumerate(columns):
        if name in EXPECTED_COLUMNS and name not in index:
            index[name] = position
    missing_columns = [name for name in EXPECTED_COLUMNS if name not in index]
    if missing_columns:
        add(1, "header", f"missing required column(s): {', '.join(missing_columns)}")
        return finish(0, 0, set())

    covered: set[str] = set()
    row_count = 0
    valid_rows = 0
    for row in reader:
        line = reader.line_num
        row_count += 1
        if not row or all(not cell.strip() for cell in row):
            add(line, "format", "empty line")
            continue
        if len(row) != len(columns):
            add(line, "format", f"expected {len(columns)} fields, found {len(row)}")
            continue
        values = {name: row[position].strip() for name, position in index.items()}
        ok = True
        if values["station"] != station:
            ok = False
            add(
                line,
                "station_mismatch",
                f"station {values['station']!r} does not match task station {station!r}",
            )
        if values["date"] != date:
            ok = False
            add(
                line,
                "date_mismatch",
                f"date {values['date']!r} does not match task date {date!r}",
            )
        slot = values["time"]
        if not TIME_RE.fullmatch(slot):
            ok = False
            add(line, "time_format", f"time {slot!r} is not an HH:MM quarter-hour slot")
        elif slot in covered:
            ok = False
            add(line, "duplicate_time", f"duplicate record for slot {slot}")
        else:
            covered.add(slot)
        for field in NUMERIC_FIELDS:
            raw = values[field]
            if raw == "":
                ok = False
                add(line, "missing_value", f"{field} is empty")
            elif "_" in raw:
                ok = False
                add(line, "not_a_number", f"{field}={raw!r} is not a number")
            else:
                try:
                    number = float(raw)
                except ValueError:
                    ok = False
                    add(line, "not_a_number", f"{field}={raw!r} is not a number")
                else:
                    if not math.isfinite(number):
                        ok = False
                        add(line, "not_finite", f"{field}={raw!r} is not a finite number")
        if ok:
            valid_rows += 1
    return finish(row_count, valid_rows, covered)


class QcService:
    """Runs accepted QC jobs in the background with restart-safe state."""

    def __init__(self, settings: Settings, store: Store):
        self.settings = settings
        self.store = store
        settings.qc_snapshots_dir.mkdir(parents=True, exist_ok=True)
        self._runs: dict[str, asyncio.Task] = {}

    # -- lifecycle -------------------------------------------------------
    async def startup(self) -> None:
        """Requeue interrupted jobs and start everything queued.

        A job left 'running' by a crash is simply rerun from its immutable
        snapshot; the verdict is deterministic for a given snapshot.
        """
        for row in self.store.qc_reports_in_state("running"):
            if self.store.qc_cas(row["id"], "running", "queued", error=None):
                self.store.event(
                    row["task_id"], "qc_requeued", f"report {row['id']} resumed after restart"
                )
        for row in self.store.qc_reports_in_state("queued"):
            self._spawn(row["id"])

    async def shutdown(self) -> None:
        for task in list(self._runs.values()):
            task.cancel()
        if self._runs:
            await asyncio.gather(*self._runs.values(), return_exceptions=True)
        self._runs.clear()
        # Leave the durable state resumable for the next process.
        for row in self.store.qc_reports_in_state("running"):
            self.store.qc_cas(row["id"], "running", "queued", error=None)

    # -- acceptance ------------------------------------------------------
    def accept(self, task_row: sqlite3.Row) -> tuple[sqlite3.Row, bool]:
        """Accept a QC job for a completed, verified task package.

        Returns ``(report_row, created)``.  If an identical job is already
        queued/running for the same package digest, the existing report is
        returned with ``created=False``.  Raises :class:`QcConflict` when the
        task is not completed with a verified package on disk.
        """
        task_id = task_row["id"]
        if task_row["state"] != "completed":
            raise QcConflict(
                f"task is {task_row['state']}; "
                "QC requires a completed task with a verified package"
            )
        verified_sha = task_row["verified_sha256"]
        verified_size = task_row["verified_size"]
        if (
            not verified_sha
            or verified_sha != task_row["expected_sha256"]
            or verified_size is None
            or verified_size != task_row["expected_size"]
        ):
            raise QcConflict("task package has not passed size/SHA-256 verification")
        package_path = task_row["package_path"]
        if not package_path or not Path(package_path).is_file():
            raise QcConflict("package file is missing on disk")
        source = Path(package_path)
        if sha256_file(source) != verified_sha:
            raise QcConflict("package file no longer matches its verified SHA-256")

        active = self.store.active_qc_report(task_id, verified_sha)
        if active is not None:
            return active, False

        report_id = uuid.uuid4().hex
        snapshot = self.settings.qc_snapshots_dir / f"{report_id}.tar.gz"
        tmp = self.settings.qc_snapshots_dir / f".{report_id}.tmp"
        try:
            with open(source, "rb") as src, open(tmp, "wb") as dst:
                while chunk := src.read(1024 * 1024):
                    dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(tmp, snapshot)
            self._fsync_dir()
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            raise QcConflict(f"could not snapshot the package: {exc}") from exc
        try:
            if sha256_file(snapshot) != verified_sha:
                raise QcConflict("package changed while snapshotting; retry the request")
            current = self.store.get(task_id)
            if (
                current is None
                or current["state"] != "completed"
                or current["verified_sha256"] != verified_sha
            ):
                raise QcConflict("task package changed while accepting; retry the request")
            self.store.insert_qc_report(
                report_id=report_id,
                task_id=task_id,
                station=current["station"],
                date=current["date"],
                package_sha256=verified_sha,
                package_size=verified_size,
                snapshot_path=str(snapshot),
            )
        except sqlite3.IntegrityError:
            # A concurrent accept won the race for this package digest.
            snapshot.unlink(missing_ok=True)
            active = self.store.active_qc_report(task_id, verified_sha)
            if active is not None:
                return active, False
            raise
        except BaseException:
            snapshot.unlink(missing_ok=True)
            raise
        self.store.event(
            task_id, "qc_accepted", f"report {report_id} sha256={verified_sha[:12]}"
        )
        self._spawn(report_id)
        return self.store.get_qc_report(report_id), True

    def _fsync_dir(self) -> None:
        try:
            fd = os.open(self.settings.qc_snapshots_dir, os.O_RDONLY | os.O_DIRECTORY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    # -- worker ----------------------------------------------------------
    def _spawn(self, report_id: str) -> None:
        if report_id in self._runs:
            return
        task = asyncio.create_task(self._run(report_id), name=f"qc:{report_id}")
        self._runs[report_id] = task
        task.add_done_callback(lambda _task, rid=report_id: self._runs.pop(rid, None))

    async def _run(self, report_id: str) -> None:
        try:
            self._execute(report_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the worker must never vanish
            log.exception("qc job %s failed unexpectedly", report_id)
            try:
                row = self.store.get_qc_report(report_id)
                if row is not None and row["state"] in {"queued", "running"}:
                    if self.store.qc_cas(
                        row["id"],
                        ("queued", "running"),
                        "failed",
                        error=f"{type(exc).__name__}: {exc}",
                        finished_at=utcnow(),
                    ):
                        self.store.event(
                            row["task_id"], "qc_failed", f"report {row['id']}: {exc}"
                        )
            except Exception:  # noqa: BLE001 - nothing more we can do
                log.exception("could not record qc failure for %s", report_id)

    def _execute(self, report_id: str) -> None:
        row = self.store.get_qc_report(report_id)
        if row is None or row["state"] != "queued":
            return
        if not self.store.qc_cas(
            report_id,
            "queued",
            "running",
            attempts=row["attempts"] + 1,
            started_at=utcnow(),
            error=None,
        ):
            return
        row = self.store.get_qc_report(report_id)
        task_id = row["task_id"]
        snapshot = Path(row["snapshot_path"])
        failure = None
        if not snapshot.is_file():
            failure = "snapshot package is missing"
        elif sha256_file(snapshot) != row["package_sha256"]:
            failure = "snapshot package failed its SHA-256 re-check"
        if failure is not None:
            if self.store.qc_cas(
                report_id, "running", "failed", error=failure, finished_at=utcnow()
            ):
                self.store.event(task_id, "qc_failed", f"report {report_id}: {failure}")
            return
        report = analyze_package(snapshot, row["station"], row["date"])
        report["package_sha256"] = row["package_sha256"]
        payload = json.dumps(report, ensure_ascii=False)
        # The report is marked completed only together with the fully
        # generated report body, in a single transaction.
        if self.store.qc_cas(
            report_id,
            "running",
            "completed",
            result=report["result"],
            report_json=payload,
            error=None,
            finished_at=utcnow(),
        ):
            self.store.event(
                task_id, "qc_completed", f"report {report_id}: {report['result']}"
            )

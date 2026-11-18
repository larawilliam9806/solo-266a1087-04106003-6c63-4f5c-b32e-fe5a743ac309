"""FastAPI application: task lifecycle, progress, package download, QC reports."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

from . import __version__
from .collector import Collector
from .config import Settings
from .models import (
    ErrorResponse,
    QcReportView,
    QcState,
    QcSummary,
    StartTaskRequest,
    TaskState,
    TaskView,
)
from .qc import QcConflict, QcService
from .store import utcnow


def _view(row, base_url: str, package_ok: bool, qc_summary: QcSummary | None = None) -> TaskView:
    percent = 0.0
    if row["expected_size"]:
        percent = round(min(100.0, row["downloaded_bytes"] * 100.0 / row["expected_size"]), 2)
    if row["state"] == "completed":
        percent = 100.0
    download_url = None
    if row["state"] == "completed" and package_ok:
        download_url = f"{base_url}/tasks/{row['id']}/package"
    return TaskView(
        id=row["id"],
        station=row["station"],
        date=row["date"],
        state=TaskState(row["state"]),
        source_url=row["source_url"],
        source_revision=row["source_revision"],
        source_etag=row["source_etag"],
        expected_size=row["expected_size"],
        expected_sha256=row["expected_sha256"],
        downloaded_bytes=row["downloaded_bytes"],
        total_bytes=row["expected_size"],
        percent=percent,
        verified_size=row["verified_size"],
        verified_sha256=row["verified_sha256"],
        error=row["error"],
        attempts=row["attempts"],
        resyncs=row["resyncs"],
        generation=row["generation"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
        download_url=download_url,
        qc=qc_summary,
    )


def _qc_view(row, task_row) -> QcReportView:
    payload = json.loads(row["report_json"]) if row["report_json"] else {}
    applies = bool(
        task_row is not None
        and task_row["state"] == "completed"
        and task_row["verified_sha256"]
        and task_row["verified_sha256"] == row["package_sha256"]
    )
    return QcReportView(
        id=row["id"],
        task_id=row["task_id"],
        station=row["station"],
        date=row["date"],
        state=QcState(row["state"]),
        package_sha256=row["package_sha256"],
        package_size=row["package_size"],
        result=row["result"],
        row_count=payload.get("row_count"),
        valid_rows=payload.get("valid_rows"),
        error_count=payload.get("error_count"),
        errors=payload.get("errors"),
        errors_truncated=payload.get("errors_truncated"),
        missing_times=payload.get("missing_times"),
        error=row["error"],
        attempts=row["attempts"],
        applies_to_current_package=applies,
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def create_app(
    settings: Settings | None = None,
    collector: Collector | None = None,
    qc_service: QcService | None = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    collector = collector or Collector(settings)
    qc_service = qc_service or QcService(settings, collector.store)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await collector.startup()
        await qc_service.startup()
        app.state.collector = collector
        app.state.qc = qc_service
        try:
            yield
        finally:
            await qc_service.shutdown()
            await collector.shutdown()

    app = FastAPI(
        title="Observation Archive Collector",
        version=__version__,
        description=(
            "Collects station/date observation archives from a directory-driven source "
            "with resumable, verified transfers."
        ),
        lifespan=lifespan,
    )

    def row_or_404(task_id: str):
        try:
            return collector.get_row(task_id)
        except KeyError:
            raise HTTPException(404, detail="task not found")

    def package_ready(row) -> bool:
        path = row["package_path"]
        return bool(
            row["state"] == "completed"
            and path
            and Path(path).is_file()
            and row["verified_size"] == row["expected_size"] is not None
            and row["verified_sha256"] == row["expected_sha256"]
        )

    def qc_summary_for(row) -> QcSummary | None:
        # A report is applicable to the task's current package only while the
        # task is completed and the verified digest matches the report's.
        if row["state"] != "completed" or not row["verified_sha256"]:
            return None
        report = collector.store.latest_qc_report_for_sha(row["id"], row["verified_sha256"])
        if report is None:
            return None
        return QcSummary(
            report_id=report["id"],
            state=QcState(report["state"]),
            result=report["result"],
            package_sha256=report["package_sha256"],
            finished_at=report["finished_at"],
        )

    def view(row, request: Request) -> TaskView:
        base = str(request.base_url).rstrip("/")
        return _view(row, base, package_ready(row), qc_summary_for(row))

    def conflict(result: dict[str, str], action: str) -> HTTPException:
        state = result.get("state")
        if state == "completed":
            return HTTPException(409, detail=f"task already completed; cannot {action}")
        if state == "failed":
            return HTTPException(409, detail=f"task failed: {result.get('detail')}")
        return HTTPException(409, detail=f"cannot {action}: {result.get('detail')}")

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True, "version": __version__}

    @app.post("/tasks", response_model=TaskView, status_code=201)
    async def create_task(body: StartTaskRequest, request: Request):
        existing = collector.store.find(body.station, body.date)
        if existing is not None:
            return view(existing, request)
        try:
            collector.create_task(uuid.uuid4().hex, body.station, body.date)
        except sqlite3.IntegrityError:
            # A concurrent request created the same station/date first.
            existing = collector.store.find(body.station, body.date)
            if existing is None:
                raise
            return view(existing, request)
        row = collector.store.find(body.station, body.date)
        return view(row, request)

    @app.get("/tasks", response_model=list[TaskView])
    async def list_tasks(request: Request, state: TaskState | None = None):
        return [view(row, request) for row in collector.store.list(state.value if state else None)]

    @app.get("/tasks/{task_id}", response_model=TaskView, responses={404: {"model": ErrorResponse}})
    async def get_task(task_id: str, request: Request):
        return view(row_or_404(task_id), request)

    @app.post(
        "/tasks/{task_id}/start",
        response_model=TaskView,
        responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    )
    async def start_task(task_id: str, request: Request):
        row_or_404(task_id)
        result = await collector.start(task_id)
        if result["state"] not in {"running", "completed"}:
            raise conflict(result, "start")
        return view(collector.store.get(task_id), request)

    @app.post("/tasks/{task_id}/pause", response_model=TaskView, responses={404: {"model": ErrorResponse}})
    async def pause_task(task_id: str, request: Request):
        row_or_404(task_id)
        result = await collector.pause(task_id)
        if result["state"] == "failed":
            raise conflict(result, "pause")
        return view(collector.store.get(task_id), request)

    @app.post("/tasks/{task_id}/resume", response_model=TaskView, responses={404: {"model": ErrorResponse}})
    async def resume_task(task_id: str, request: Request):
        row_or_404(task_id)
        result = await collector.start(task_id)
        if result["state"] not in {"running", "completed"}:
            raise conflict(result, "resume")
        return view(collector.store.get(task_id), request)

    @app.post("/tasks/{task_id}/refresh", response_model=TaskView, responses={404: {"model": ErrorResponse}})
    async def refresh_task(task_id: str, request: Request):
        row_or_404(task_id)
        result = await collector.refresh(task_id)
        if result["state"] not in {"running", "completed"}:
            raise conflict(result, "refresh")
        return view(collector.store.get(task_id), request)

    @app.get("/tasks/{task_id}/events")
    async def task_events(task_id: str):
        row_or_404(task_id)
        return [
            {"seq": row["seq"], "at": row["at"], "event": row["event"], "detail": row["detail"]}
            for row in collector.store.events(task_id)
        ]

    @app.post(
        "/tasks/{task_id}/qc",
        response_model=QcReportView,
        status_code=201,
        responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    )
    async def start_task_qc(task_id: str, response: Response):
        row = row_or_404(task_id)
        try:
            report, created = qc_service.accept(row)
        except QcConflict as exc:
            raise HTTPException(409, detail=str(exc)) from exc
        if not created:
            # An identical job for the same package digest is already in flight.
            response.status_code = 200
        return _qc_view(report, collector.store.get(task_id))

    @app.get(
        "/tasks/{task_id}/qc",
        response_model=list[QcReportView],
        responses={404: {"model": ErrorResponse}},
    )
    async def list_task_qc(task_id: str):
        row_or_404(task_id)
        task_row = collector.store.get(task_id)
        return [_qc_view(row, task_row) for row in collector.store.list_qc_reports(task_id)]

    @app.get(
        "/qc/{report_id}",
        response_model=QcReportView,
        responses={404: {"model": ErrorResponse}},
    )
    async def get_qc_report(report_id: str):
        row = collector.store.get_qc_report(report_id)
        if row is None:
            raise HTTPException(404, detail="qc report not found")
        return _qc_view(row, collector.store.get(row["task_id"]))

    @app.get(
        "/tasks/{task_id}/package",
        responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    )
    async def download_package(task_id: str):
        row = row_or_404(task_id)
        if row["state"] != "completed":
            raise HTTPException(409, detail=f"task is {row['state']}; no package available")
        if not package_ready(row):
            raise HTTPException(409, detail="package missing or failed verification")
        if not Path(row["package_path"]).is_file():
            raise HTTPException(409, detail="package file is gone; refresh the task")
        filename = f"{row['station']}-{row['date']}.tar.gz"
        return FileResponse(
            row["package_path"],
            media_type="application/gzip",
            filename=filename,
            headers={
                "Digest": f"sha-256={row['verified_sha256']}",
                "ETag": f'"{row["verified_sha256"]}"',
            },
        )

    @app.get("/")
    async def index(request: Request):
        base = str(request.base_url).rstrip("/")
        return {
            "service": "observation-archive-collector",
            "version": __version__,
            "source": settings.source_base_url,
            "demo": f"{base}/docs#/default/create_task_tasks_post",
            "endpoints": [
                "POST /tasks {station,date}",
                "GET /tasks",
                "GET /tasks/{id}",
                "POST /tasks/{id}/start",
                "POST /tasks/{id}/pause",
                "POST /tasks/{id}/resume",
                "POST /tasks/{id}/refresh",
                "GET /tasks/{id}/events",
                "GET /tasks/{id}/package",
                "POST /tasks/{id}/qc",
                "GET /tasks/{id}/qc",
                "GET /qc/{id}",
            ],
        }

    app.state.settings = settings
    app.state.started_at = utcnow()
    return app


app = create_app()

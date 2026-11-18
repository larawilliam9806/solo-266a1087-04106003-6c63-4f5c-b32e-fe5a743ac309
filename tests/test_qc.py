from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import io
import shutil
import tarfile
import time
import uuid
from types import SimpleNamespace

import httpx
import pytest

from app.collector import Collector
from app.config import Settings
from app.main import create_app
from app.qc import QcService, expected_slots
from mock_source.app import app as source_app, state as source_state
from tests.conftest import free_port, run_server

HEADER = "station,date,time,temperature_c,pressure_hpa,wind_mps"


def make_csv(station: str, date: str, *, skip=(), edits=None, extra=()) -> str:
    """A full-day CSV (96 slots) with optional removals, per-slot edits, extras."""
    edits = edits or {}
    lines = [HEADER]
    for slot in expected_slots():
        if slot in skip:
            continue
        row = edits.get(slot, (station, date, slot, "12.34", "1013.25", "5.67"))
        lines.append(",".join(row))
    lines.extend(extra)
    return "\n".join(lines) + "\n"


def make_package(csv_text: str | None, station: str, date: str) -> bytes:
    """A tar.gz like the source builds, with observations.csv optional."""
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as archive:
        def add(name: str, data: bytes) -> None:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(data))

        add("README.txt", f"station={station}\ndate={date}\n".encode())
        if csv_text is not None:
            add("observations.csv", csv_text.encode())
    return gzip.compress(tar_buffer.getvalue(), mtime=0)


@pytest.fixture
async def stack(tmp_path):
    source_state.revisions.clear()
    source_state.delay_seconds = 0.0
    source_state.chunk_delay_seconds = 0.0
    source_state.failing.clear()
    source_state.corrupting.clear()
    source_state.no_range.clear()
    source_state.custom_packages.clear()
    source_state.requests.clear()

    source_port = free_port()
    api_port = free_port()
    settings = Settings(
        source_base_url=f"http://127.0.0.1:{source_port}",
        data_dir=tmp_path / "data",
        http_timeout=15,
        chunk_size=8 * 1024,
        max_source_resyncs=3,
    )
    collector = Collector(settings)
    api_app = create_app(settings, collector)
    async with run_server(source_app, source_port):
        async with run_server(api_app, api_port):
            async with httpx.AsyncClient(timeout=30) as client:
                stack = SimpleNamespace(
                    source_url=f"http://127.0.0.1:{source_port}",
                    api_url=f"http://127.0.0.1:{api_port}",
                    settings=settings,
                    collector=collector,
                    qc=api_app.state.qc,
                )
                yield stack, client


async def wait_task(client: httpx.AsyncClient, api: str, task_id: str, states: set[str], timeout: float = 60):
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] in states:
            return view
        await asyncio.sleep(0.05)
    raise AssertionError(f"timeout; last={view}")


async def wait_report(client: httpx.AsyncClient, api: str, report_id: str, timeout: float = 30):
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/qc/{report_id}")).json()
        if view["state"] in {"completed", "failed"}:
            return view
        await asyncio.sleep(0.05)
    raise AssertionError(f"qc timeout; last={view}")


async def collect(client: httpx.AsyncClient, api: str, station: str, date: str) -> dict:
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    view = await wait_task(client, api, task_id, {"completed", "failed"})
    assert view["state"] == "completed", view
    return view


async def upload_package(client: httpx.AsyncClient, source: str, station: str, date: str, body: bytes) -> dict:
    resp = await client.post(
        f"{source}/admin/package",
        json={"station": station, "date": date, "content_base64": base64.b64encode(body).decode()},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def test_qc_pass_on_default_package(stack):
    st, client = stack
    view = await collect(client, st.api_url, "54511", "2026-11-17")
    task_id = view["id"]

    resp = await client.post(f"{st.api_url}/tasks/{task_id}/qc")
    assert resp.status_code == 201, resp.text
    report = resp.json()
    assert report["package_sha256"] == view["verified_sha256"]
    assert report["package_size"] == view["verified_size"]

    done = await wait_report(client, st.api_url, report["id"])
    assert done["state"] == "completed"
    assert done["result"] == "pass"
    assert done["row_count"] == 96
    assert done["valid_rows"] == 96
    assert done["error_count"] == 0
    assert done["errors"] == []
    assert done["missing_times"] == []
    assert done["applies_to_current_package"] is True
    assert done["started_at"] and done["finished_at"]

    # The task view marks the report as applicable to the current package.
    task = (await client.get(f"{st.api_url}/tasks/{task_id}")).json()
    assert task["qc"]["report_id"] == report["id"]
    assert task["qc"]["result"] == "pass"
    assert task["qc"]["package_sha256"] == view["verified_sha256"]

    listing = (await client.get(f"{st.api_url}/tasks/{task_id}/qc")).json()
    assert [row["id"] for row in listing] == [report["id"]]

    events = [e["event"] for e in (await client.get(f"{st.api_url}/tasks/{task_id}/events")).json()]
    assert "qc_accepted" in events and "qc_completed" in events


async def test_qc_requires_completed_verified_task(stack):
    st, client = stack
    api, source = st.api_url, st.source_url

    # Unknown task -> 404; unknown report -> 404.
    assert (await client.post(f"{api}/tasks/nope/qc")).status_code == 404
    assert (await client.get(f"{api}/qc/nope")).status_code == 404

    # Queued (never collected) task -> 409.
    queued_id = (await client.post(f"{api}/tasks", json={"station": "Q1", "date": "2026-11-01"})).json()["id"]
    resp = await client.post(f"{api}/tasks/{queued_id}/qc")
    assert resp.status_code == 409

    # Failed task (corrupt source) -> 409.
    await client.post(f"{source}/admin/fault", json={"station": "Q2", "date": "2026-11-02", "mode": "corrupt"})
    failed_id = (await client.post(f"{api}/tasks", json={"station": "Q2", "date": "2026-11-02"})).json()["id"]
    await client.post(f"{api}/tasks/{failed_id}/start")
    failed = await wait_task(client, api, failed_id, {"failed", "completed"})
    assert failed["state"] == "failed"
    assert (await client.post(f"{api}/tasks/{failed_id}/qc")).status_code == 409

    # Completed but the package file vanished -> 409.
    view = await collect(client, api, "Q3", "2026-11-03")
    for package in st.settings.packages_dir.glob("*.tar.gz"):
        package.unlink()
    resp = await client.post(f"{api}/tasks/{view['id']}/qc")
    assert resp.status_code == 409


async def test_qc_reports_data_errors_with_line_numbers(stack):
    st, client = stack
    station, date = "QC1", "2026-11-18"
    edits = {
        "02:00": (station, date, "02:00", "abc", "1013.25", "5.67"),      # not_a_number
        "03:00": (station, date, "03:00", "12.34", "", "5.67"),           # missing_value
        "04:00": (station, date, "04:00", "12.34", "1013.25", "NaN"),     # not_finite
        "05:00": ("OTHER", date, "05:00", "12.34", "1013.25", "5.67"),    # station_mismatch
        "06:00": (station, "2026-11-19", "06:00", "12.34", "1013.25", "5.67"),  # date_mismatch
        "07:00": (station, date, "7:5", "12.34", "1013.25", "5.67"),      # time_format
        "08:00": (station, date, "08:00", "12.34", "1013.25", "inf"),     # not_finite
    }
    duplicate = f"{station},{date},01:00,11.11,1012.34,4.56"
    csv_text = make_csv(station, date, skip={"00:15", "00:30"}, edits=edits, extra=[duplicate])
    await upload_package(client, st.source_url, station, date, make_package(csv_text, station, date))

    view = await collect(client, st.api_url, station, date)
    resp = await client.post(f"{st.api_url}/tasks/{view['id']}/qc")
    assert resp.status_code == 201, resp.text
    done = await wait_report(client, st.api_url, resp.json()["id"])

    assert done["state"] == "completed"
    assert done["result"] == "fail"
    # 96 slots - 2 skipped = 94 rows + 1 duplicate extra row.
    assert done["row_count"] == 95
    assert done["valid_rows"] == 95 - 8
    # The invalid time "7:5" leaves slot 07:00 uncovered.
    assert done["missing_times"] == ["00:15", "00:30", "07:00"]

    kinds = {e["kind"] for e in done["errors"]}
    assert kinds == {
        "not_a_number",
        "missing_value",
        "not_finite",
        "station_mismatch",
        "date_mismatch",
        "time_format",
        "duplicate_time",
    }
    by_kind = {}
    for e in done["errors"]:
        by_kind.setdefault(e["kind"], []).append(e)

    # 02:00 is the 7th data row (00:15/00:30 skipped) -> line 8 (header is line 1);
    # each subsequent hour slot lands 4 rows (lines) later.
    assert by_kind["not_a_number"][0]["line"] == 8
    assert "temperature_c" in by_kind["not_a_number"][0]["message"]
    assert by_kind["missing_value"][0]["line"] == 12
    assert by_kind["not_finite"][0]["line"] == 16
    assert by_kind["station_mismatch"][0]["line"] == 20
    assert by_kind["date_mismatch"][0]["line"] == 24
    assert by_kind["time_format"][0]["line"] == 28
    # The duplicate extra row is appended last: 1 header + 94 rows + 1 = line 96.
    assert by_kind["duplicate_time"][0]["line"] == 96
    assert "01:00" in by_kind["duplicate_time"][0]["message"]
    assert done["error_count"] == 8
    assert done["errors_truncated"] is False


async def test_qc_missing_csv_and_unreadable_archive(stack):
    st, client = stack

    # Package without observations.csv.
    station, date = "NOC", "2026-11-19"
    await upload_package(client, st.source_url, station, date, make_package(None, station, date))
    view = await collect(client, st.api_url, station, date)
    resp = await client.post(f"{st.api_url}/tasks/{view['id']}/qc")
    done = await wait_report(client, st.api_url, resp.json()["id"])
    assert done["state"] == "completed"
    assert done["result"] == "fail"
    assert done["error_count"] == 1
    assert done["errors"][0]["kind"] == "missing_file"
    assert done["errors"][0]["line"] is None

    # Package that is not a gzip archive at all (digest still verified).
    station, date = "GARB", "2026-11-20"
    await upload_package(client, st.source_url, station, date, b"this is not a tar.gz")
    view = await collect(client, st.api_url, station, date)
    resp = await client.post(f"{st.api_url}/tasks/{view['id']}/qc")
    done = await wait_report(client, st.api_url, resp.json()["id"])
    assert done["state"] == "completed"
    assert done["result"] == "fail"
    assert done["errors"][0]["kind"] == "archive_error"


async def test_qc_station_mismatch_on_every_row_and_header_error(stack):
    st, client = stack

    # Every row carries the wrong station: 96 errors with their line numbers.
    station, date = "ALL", "2026-11-21"
    csv_text = make_csv("WRONG", date)
    await upload_package(client, st.source_url, station, date, make_package(csv_text, station, date))
    view = await collect(client, st.api_url, station, date)
    resp = await client.post(f"{st.api_url}/tasks/{view['id']}/qc")
    done = await wait_report(client, st.api_url, resp.json()["id"])
    assert done["result"] == "fail"
    assert done["error_count"] == 96
    assert all(e["kind"] == "station_mismatch" for e in done["errors"])
    assert done["errors"][0]["line"] == 2
    assert done["errors"][-1]["line"] == 97
    assert done["missing_times"] == []  # slots are covered; values are wrong

    # A header without the wind column cannot be checked at all.
    station, date = "HDR", "2026-11-22"
    csv_text = "station,date,time,temperature_c,pressure_hpa\n" + "\n".join(
        f"{station},{date},{slot},12.34,1013.25" for slot in expected_slots()
    ) + "\n"
    await upload_package(client, st.source_url, station, date, make_package(csv_text, station, date))
    view = await collect(client, st.api_url, station, date)
    resp = await client.post(f"{st.api_url}/tasks/{view['id']}/qc")
    done = await wait_report(client, st.api_url, resp.json()["id"])
    assert done["result"] == "fail"
    assert done["error_count"] == 1
    assert done["errors"][0]["kind"] == "header"
    assert done["errors"][0]["line"] == 1
    assert "wind_mps" in done["errors"][0]["message"]


async def test_qc_snapshot_survives_refresh_and_old_reports_are_kept(stack):
    st, client = stack
    api, source = st.api_url, st.source_url
    station, date = "SNAP", "2026-11-23"

    view1 = await collect(client, api, station, date)
    task_id = view1["id"]
    sha_v1 = view1["verified_sha256"]

    resp = await client.post(f"{api}/tasks/{task_id}/qc")
    assert resp.status_code == 201
    report_v1 = resp.json()["id"]

    # Refresh immediately: refresh must not wait for QC, and the QC job must
    # still check the v1 bytes it was accepted with.
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    view2 = await wait_task(client, api, task_id, {"completed", "failed"})
    assert view2["state"] == "completed"
    sha_v2 = view2["verified_sha256"]
    assert sha_v2 != sha_v1

    done_v1 = await wait_report(client, api, report_v1)
    assert done_v1["state"] == "completed"
    assert done_v1["result"] == "pass"
    assert done_v1["package_sha256"] == sha_v1
    # The old report no longer applies to the current (v2) package.
    assert done_v1["applies_to_current_package"] is False

    # No report exists for v2 yet, so the task view shows no applicable QC.
    task = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert task["qc"] is None

    # The old report is still queryable by ID.
    again = (await client.get(f"{api}/qc/{report_v1}")).json()
    assert again["package_sha256"] == sha_v1
    assert again["applies_to_current_package"] is False

    # QC the new package: a fresh report that now applies.
    resp = await client.post(f"{api}/tasks/{task_id}/qc")
    assert resp.status_code == 201
    report_v2 = resp.json()["id"]
    assert report_v2 != report_v1
    done_v2 = await wait_report(client, api, report_v2)
    assert done_v2["result"] == "pass"
    assert done_v2["package_sha256"] == sha_v2
    assert done_v2["applies_to_current_package"] is True

    task = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert task["qc"]["report_id"] == report_v2

    # Both reports are kept and listed, newest first.
    listing = (await client.get(f"{api}/tasks/{task_id}/qc")).json()
    assert [row["id"] for row in listing] == [report_v2, report_v1]

    # The packages dir holds only the current package; QC kept its own copies.
    assert len(list(st.settings.packages_dir.glob("*.tar.gz"))) == 1
    assert len(list(st.settings.qc_snapshots_dir.glob("*.tar.gz"))) == 2


async def test_late_old_qc_job_does_not_override_new_package(stack):
    st, client = stack
    api, source = st.api_url, st.source_url
    station, date = "LATEQC", "2026-11-24"

    view1 = await collect(client, api, station, date)
    task_id = view1["id"]
    sha_v1 = view1["verified_sha256"]

    # Simulate a QC job accepted just before a crash: a queued report row plus
    # its snapshot, but no worker ever ran it.
    report_id = uuid.uuid4().hex
    snapshot = st.settings.qc_snapshots_dir / f"{report_id}.tar.gz"
    st.settings.qc_snapshots_dir.mkdir(parents=True, exist_ok=True)
    (package_v1,) = st.settings.packages_dir.glob("*.tar.gz")
    shutil.copyfile(package_v1, snapshot)
    st.collector.store.insert_qc_report(
        report_id=report_id,
        task_id=task_id,
        station=station,
        date=date,
        package_sha256=sha_v1,
        package_size=view1["verified_size"],
        snapshot_path=str(snapshot),
    )

    # The task moves to a new revision before the old job runs.
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    view2 = await wait_task(client, api, task_id, {"completed", "failed"})
    assert view2["verified_sha256"] != sha_v1

    # Only now does the old job run (e.g. after a process restart).
    fresh_qc = QcService(st.settings, st.collector.store)
    await fresh_qc.startup()
    try:
        done = await wait_report(client, api, report_id)
    finally:
        await fresh_qc.shutdown()
    assert done["state"] == "completed"
    assert done["result"] == "pass"
    assert done["package_sha256"] == sha_v1
    assert done["applies_to_current_package"] is False

    # The late old job must not claim the new package: no applicable QC shown.
    task = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert task["qc"] is None


async def test_qc_restart_recovers_queued_and_interrupted_jobs(stack):
    st, client = stack
    api = st.api_url
    station, date = "RST", "2026-11-25"

    view = await collect(client, api, station, date)
    task_id = view["id"]
    resp = await client.post(f"{api}/tasks/{task_id}/qc")
    assert resp.status_code == 201
    report_id = resp.json()["id"]

    # Simulate a process crash: stop the services without draining anything.
    await st.qc.shutdown()
    await st.collector.shutdown()

    # A fresh process over the same data dir finishes (or redoes) the job.
    fresh_collector = Collector(st.settings)
    await fresh_collector.startup()
    fresh_qc = QcService(st.settings, fresh_collector.store)
    await fresh_qc.startup()
    done = await wait_report(client, api, report_id)
    assert done["state"] == "completed"
    assert done["result"] == "pass"
    assert done["package_sha256"] == view["verified_sha256"]

    # A job left 'running' by a crash is requeued and rerun on startup.
    assert fresh_collector.store.qc_cas(report_id, "completed", "running")
    fresh_qc2 = QcService(st.settings, fresh_collector.store)
    await fresh_qc2.startup()
    done2 = await wait_report(client, api, report_id)
    assert done2["state"] == "completed"
    assert done2["result"] == "pass"
    assert done2["attempts"] >= 2

    await fresh_qc2.shutdown()
    await fresh_qc.shutdown()
    await fresh_collector.shutdown()


async def test_qc_dedupes_active_job_for_same_package(stack):
    st, client = stack
    api = st.api_url
    view = await collect(client, api, "DUP", "2026-11-26")
    task_id = view["id"]
    row = st.collector.store.get(task_id)

    svc = QcService(st.settings, st.collector.store)
    try:
        first, created1 = svc.accept(row)
        second, created2 = svc.accept(row)
        assert created1 is True
        assert created2 is False
        assert first["id"] == second["id"]

        done = await wait_report(client, api, first["id"])
        assert done["state"] == "completed"

        # Once the job is done, a new acceptance creates a fresh report.
        third, created3 = svc.accept(row)
        assert created3 is True
        assert third["id"] != first["id"]
        done3 = await wait_report(client, api, third["id"])
        assert done3["state"] == "completed"
    finally:
        await svc.shutdown()

    listing = (await client.get(f"{api}/tasks/{task_id}/qc")).json()
    assert len(listing) == 2


async def test_qc_empty_and_malformed_csv(stack):
    st, client = stack

    # Empty observations.csv.
    station, date = "EMPTY", "2026-11-27"
    await upload_package(client, st.source_url, station, date, make_package("", station, date))
    view = await collect(client, st.api_url, station, date)
    resp = await client.post(f"{st.api_url}/tasks/{view['id']}/qc")
    done = await wait_report(client, st.api_url, resp.json()["id"])
    assert done["result"] == "fail"
    assert done["errors"][0]["kind"] == "empty_file"

    # Ragged rows and blank lines are format errors with line numbers.
    station, date = "RAG", "2026-11-28"
    lines = [HEADER]
    slots = expected_slots()
    lines.append(f"{station},{date},{slots[0]},12.34,1013.25,5.67")
    lines.append("")  # blank line -> format error at line 3
    lines.append(f"{station},{date},{slots[1]},12.34")  # too few fields -> line 4
    for slot in slots[2:]:
        lines.append(f"{station},{date},{slot},12.34,1013.25,5.67")
    csv_text = "\n".join(lines) + "\n"
    await upload_package(client, st.source_url, station, date, make_package(csv_text, station, date))
    view = await collect(client, st.api_url, station, date)
    resp = await client.post(f"{st.api_url}/tasks/{view['id']}/qc")
    done = await wait_report(client, st.api_url, resp.json()["id"])
    assert done["result"] == "fail"
    format_errors = [e for e in done["errors"] if e["kind"] == "format"]
    assert [e["line"] for e in format_errors] == [3, 4]
    # Slot 00:15's row was malformed, so the slot counts as missing.
    assert done["missing_times"] == ["00:15"]

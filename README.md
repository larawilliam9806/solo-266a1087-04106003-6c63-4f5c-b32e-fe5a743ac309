# Observation Archive Collector

A small FastAPI service that collects per-station/per-day observation archives
from a directory-driven source, with pause/resume, restart-safe progress and
verify-before-download semantics.  Collected packages can be quality-controlled
in the background before they are cleared for ingestion.

## Components

| Service | Port | Purpose |
| --- | --- | --- |
| `collector` | 8000 | Collection + QC API and OpenAPI docs at `/docs` |
| `source` | 9000 | Local mock observation source with strong ETags, Range, revisions, latency and fault injection |

## Run with Docker Compose

```bash
docker compose up --build
```

- API docs / demo entry point: <http://localhost:8000/docs>
- Root descriptor: <http://localhost:8000/> lists all endpoints
- Source admin state: <http://localhost:9000/admin/state>

Docker is not required for the tests; they start both ASGI apps on loopback
ports in-process.

A scripted end-to-end demonstration (collect → pause → resume → verified
download → QC pass → revision switch → QC of the new package):

```bash
python scripts/demo.py
```

Local (non-Docker) run:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn mock_source.app:app --port 9000 &
SOURCE_BASE_URL=http://127.0.0.1:9000 COLLECTOR_DATA_DIR=./data \
  uvicorn app.main:app --port 8000
python scripts/demo.py
```

`POST /tasks` returns HTTP 201 for new tasks; an existing station+date returns
the current task.

## API

| Method & path | Meaning |
| --- | --- |
| `POST /tasks` `{station,date}` | Create (or return the existing) task; one task per station+date |
| `POST /tasks/{id}/start` | Start a queued/paused/failed task |
| `POST /tasks/{id}/pause` | Pause; returns only after in-flight writes can no longer land |
| `POST /tasks/{id}/resume` | Resume from durable progress |
| `POST /tasks/{id}/refresh` | Re-read the directory for a completed task and re-collect if it moved |
| `GET /tasks`, `GET /tasks/{id}` | List / inspect tasks (state, bytes, percent, expected vs verified size & SHA-256, error, applicable QC report) |
| `GET /tasks/{id}/events` | Append-only event log for the task |
| `GET /tasks/{id}/package` | Download the archive; `409` unless completed and verified |
| `POST /tasks/{id}/qc` | Accept a background QC job for the task's current package (`201`; `200` if an identical job is already in flight; `409` unless the task is completed and verified) |
| `GET /tasks/{id}/qc` | List all QC reports of the task, newest first |
| `GET /qc/{id}` | Fetch one QC report by ID |

`GET /tasks` accepts `?state=running|paused|completed|failed|queued`.

## Quality control

`POST /tasks/{id}/qc` starts a background job and returns a report ID; polling
`GET /qc/{id}` shows `queued`/`running` until the verdict lands (`completed`
with `result: pass|fail`, or `failed` for a job-level problem such as a lost
snapshot).  The job reads `observations.csv` from inside the downloaded
tar.gz, using the existing column names
`station,date,time,temperature_c,pressure_hpa,wind_mps`, and checks:

- every row's `station`/`date` equals the task's station/date;
- the day holds exactly one row for each of the 96 quarter-hour slots
  (`00:00` … `23:45`) — duplicates and missing slots fail;
- `temperature_c`, `pressure_hpa` and `wind_mps` are finite numbers
  (empty, non-numeric, `NaN`/`inf` all fail).

Any duplicate, missing, format or value error fails the report.  The report
lists each finding with its 1-based line number in `observations.csv`
(`errors[].line` + `errors[].kind`) and the missing slots (`missing_times`).
A missing `observations.csv`, an unreadable archive or a broken header is
reported the same way (`missing_file` / `archive_error` / `header` /
`encoding_error`).

QC guarantees:

1. **Only verified originals are accepted.**  QC is accepted (`201`) only for
   a task that is `completed` with a size/SHA-256-verified package; acceptance
   re-hashes the on-disk package and records that SHA-256 in the report.
2. **The accepted bytes are frozen.**  Acceptance snapshots the package into
   `data/qc/snapshots/` (temp file + fsync + atomic rename) and the worker
   re-verifies the snapshot digest before analysing.  A later refresh —
   successful or not — or a process restart can never make a job check a
   newer package instead of the accepted original, and refresh never waits
   for QC to finish.
3. **Reports complete atomically.**  A report is marked `completed` only in
   the same transaction that stores the fully generated report body.  Jobs
   left `queued`/`running` by a restart are requeued and rerun from their
   immutable snapshot on startup.
4. **History is kept; applicability is by digest.**  Old reports are never
   deleted and stay queryable by ID.  `GET /tasks/{id}` shows a `qc` summary
   only while the task is completed and its verified SHA-256 matches the
   report's — so a late-finishing job for an old revision can never override
   the conclusion for the current package, and each report view carries
   `applies_to_current_package` computed the same way.

## Correctness guarantees

1. **Directory is authoritative.** Each attempt reads size and SHA-256 from the
   source directory; nothing is trusted from an earlier attempt unless the whole
   identity (URL + strong ETag + size + digest) still matches.
2. **Resume only within one version.** The part file name embeds a hash of the
   source identity. If the directory reports a changed revision, partial bytes
   and metadata are discarded before a fresh transfer starts, so bytes from two
   versions are never concatenated.
3. **Range + If-Range.** Resumes send `Range: bytes=N-` and `If-Range: <strong
   ETag>`. A mismatched ETag, a wrong `Content-Range`, or a 412/416 causes a
   re-read instead of an append. A server that ignores Range but returns the
   same ETag is handled by truncating and restarting that version from zero
   (never appending a full body to partial data).
4. **Pause is a generation fence.** Pause bumps `generation` transactionally
   before responding; every worker write is a compare-and-set on
   `(state='running', generation)`. Late chunks/responses therefore cannot
   advance progress, mark completion, or publish a package after the pause
   response.
5. **Restart-safe.** Running tasks are flipped to `paused` at startup; the part
   file and byte count survive. With `AUTO_RESUME=true` only tasks interrupted by
   the restart (and queued tasks) resume automatically — explicit user pauses are
   left alone.
6. **Verify before exposure.** Completion requires the downloaded file's length
   and SHA-256 to equal the directory entry's. The file is written to a temp
   name and atomically renamed; the task becomes `completed` only after the
   rename. Until then — and for failed tasks — `GET .../package` returns 409.
   The directory is re-read once more just before publishing so a mid-transfer
   revision swap is discarded, not published.
7. **Failure keeps the reason.** Failed tasks retain `error`; transient failures
   (timeouts, 5xx, network) keep their partial prefix and can be retried via
   `start`; terminal failures (bad metadata, repeated verification/source
   mismatch) delete partial data.

## Source mock controls

```bash
# switch the revision served for a station/date (changes body, size, digest)
curl -X POST localhost:9000/admin/revision \
  -H 'content-type: application/json' \
  -d '{"station":"54511","date":"2026-11-17","revision":"v2"}'

# add 300 ms header delay and 10 ms per-chunk delay
curl -X POST localhost:9000/admin/delay \
  -H 'content-type: application/json' -d '{"seconds":0.3,"chunk_seconds":0.01}'

# simulate outage (503) or a corrupted body (honest digest, tampered bytes)
curl -X POST localhost:9000/admin/fault \
  -H 'content-type: application/json' -d '{"station":"54511","date":"2026-11-17","mode":"fail"}'

# serve caller-supplied tar.gz bytes (e.g. a CSV with QC faults); null clears it
curl -X POST localhost:9000/admin/package \
  -H 'content-type: application/json' \
  -d '{"station":"54511","date":"2026-11-17","content_base64":"H4sI..."}'
```

`mode` accepts `fail`, `corrupt`, `no_range` or `clear`. `GET /admin/state`
also shows the recent package requests including their `Range`/`If-Range`
headers.

## Tests

```bash
pytest -q
```

The suite runs the real API and the real mock source on loopback ports and
covers: full verified flow, duplicate task creation, pause with a late response
(progress/completion must not move), restart recovery with a real `Range`
resume, revision change mid-pause (old bytes discarded), corruption → failure
with no downloadable package, outage → failure with reason → successful retry,
refresh replacing a package on revision change, a source that ignores Range
(clean restart, no append), a pause issued before a delayed response arrives,
input validation, and the QC flow: pass verdicts, per-line error reporting
(duplicates, missing slots, bad numbers, station/date mismatches, bad times,
missing CSV, broken archives), acceptance rules, snapshot isolation across
refresh, late old jobs never overriding the current package, restart recovery
of QC jobs, in-flight dedupe, and old-report retention.

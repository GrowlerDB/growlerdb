"""Minimal, dependency-free generator telemetry for the scale harness.

The generator is ground truth for the corpus's UNCOMPRESSED size — it produces the rows. Track the
real uncompressed JSON bytes it emits (stable across storage config: parquet↔orc, zstd↔snappy — the
OSB/ES-benchmark convention) and serve them on a Prometheus `/metrics` endpoint, so `staged_run.py`
can size milestones + index:source against the actual raw corpus instead of a hardcoded
`RAW_ROW_BYTES` guess (TASK-342). Counters are cumulative and per-pod, so N generator replicas sum
cleanly (`sum(growlerdb_gen_raw_bytes_total)`).

Stdlib only — the seed image carries no extra deps. `corpus.stream()` calls `record_columns()` once
per appended batch; the generator wrapper calls `serve()` once at boot.
"""
import http.server
import json
import threading

_lock = threading.Lock()
_raw_bytes = 0
_rows = 0


def record_columns(cols, n):
    """Add one appended batch of `n` rows, given columnar as `{field: [values]}` (what the corpus
    hands to `pa.table`). Counts each row's compact-JSON byte length — the uncompressed logical size.
    `default=str` keeps it robust to non-JSON-native cell types (dates, numpy scalars)."""
    global _raw_bytes, _rows
    fields = list(cols)
    total = 0
    for i in range(n):
        total += len(json.dumps({f: cols[f][i] for f in fields}, separators=(",", ":"), default=str))
    with _lock:
        _raw_bytes += total
        _rows += n


def serve(port=9109):
    """Start a daemon HTTP thread serving the cumulative counters in Prometheus text format."""
    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            with _lock:
                rb, rw = _raw_bytes, _rows
            body = (
                "# TYPE growlerdb_gen_raw_bytes_total counter\n"
                f"growlerdb_gen_raw_bytes_total {rb}\n"
                "# TYPE growlerdb_gen_rows_total counter\n"
                f"growlerdb_gen_rows_total {rw}\n"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):  # keep the generator log clean
            pass

    srv = http.server.HTTPServer(("0.0.0.0", port), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

# Scale / cluster-validation run log

Append-only ledger of validation runs — the durable, git-committed record. One compact row per
run; the heavy artifacts (metric/log dumps, screenshots) live under the gitignored
`bench/scale/runs/<run>/` and are captured by `capture.py`.

| Started (UTC) | Duration | Purpose | Parameters | Result summary | Artifact dir |
| --- | --- | --- | --- | --- | --- |
| 2026-07-17T13:56:12Z | 45m00s | TASK-229 cold-tier park/revive validation (Hetzner k3s, windowed) | workload=http_logs_windowed namespace=growlerdb cluster=hetzner-4x-cpx42 nodes=6 hot_windows=3 park_interval_s=90 image=dev-49717958 result=PASS park=auto to MinIO (split.bundle+aux+hotcache) read_through=286 hits from parked window revive=152/interval>=16 -> promoted cold_cold_took_ms=9 cold_warm_took_ms=1 hot_took_ms=1 window_min=45 | cost=~$1 (short run) | `runs/2026-07-17T14-41-12Z__task-229-cold-tier-park-revive-validation-hetzne` |
| 2026-07-17T15:41:14Z | 1h00m | TASK-272/273 fix re-validation (Hetzner, coldtier-fix2) | namespace=growlerdb task272=PASS — /v1/cold now reflects runtime parking (hot=29 cold=81); needed fingerprint to include cold task273=FAIL — execution-layer conversion inconsistent with gateway window-pruning; seconds prune to nothing, micros now error. Revert + redo. image=coldtier-fix2 window_min=60 | cost=~$2 | `runs/2026-07-17T16-41-14Z__task-272-273-fix-re-validation-hetzner-coldtier-` |
| 2026-07-17T16:45:35Z | 40m00s | TASK-272/273 re-validation (Hetzner, coldtier-fix3, both PASS) | namespace=growlerdb task272=PASS — /v1/cold hot=19 cold=2 (reflects parking) task273=PASS — seconds range queries return hits: window_1day=1120, window_7day=3620, wide=11620=match_all (adapter converts epoch_s->micros; was 0 pre-fix) image=coldtier-fix3 window_min=40 | cost=~$2 | `runs/2026-07-17T17-25-35Z__task-272-273-re-validation-hetzner-coldtier-fix3` |

# Scale / cluster-validation run log

Append-only ledger of validation runs — the durable, git-committed record. One compact row per
run; the heavy artifacts (metric/log dumps, screenshots) live under the gitignored
`bench/scale/runs/<run>/` and are captured by `capture.py`.

| Started (UTC) | Duration | Purpose | Parameters | Result summary | Artifact dir |
| --- | --- | --- | --- | --- | --- |

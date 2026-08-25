# Synthetic `http_logs` corpus — generation methodology

The scale/comparison benchmark runs on a generated HTTP access-log corpus. This documents how it is
generated and why, so the distributions can be reviewed and reproduced. The generator is
[`workloads/http_logs/corpus.py`](workloads/http_logs/corpus.py); the validation report is produced
by [`corpus_stats.py`](corpus_stats.py). Feedback and corrections are welcome via GitHub issues/PRs.

## Why synthetic

No permissively-licensed real dataset fit all of: log-shaped (client IP, request, status, bytes),
commercial-use license, and 30–100 GB scale (the large permissive corpora are documents or numeric;
the real access-log corpora are non-commercial). A generated corpus removes the license/PII constraint
and scales to any size. The trade is realism — which is the whole point of this document: the value
distributions are modeled on real web traffic, not drawn uniformly. Uniform-random data would give a
flat term dictionary, no IP-CIDR selectivity, and no partition-pruning signal — none of which reflect
a search workload — so the generator models skew explicitly and this report checks that it did.

## Schema

17 fields (source-of-record columns plus the searchable subset). `request_id` is the primary key.

`request_id`, `ts` (epoch seconds), `method`, `host`, `path`, `query`, `protocol`, `status`,
`response_size`, `response_time_ms`, `client_ip`, `user_agent`, `referer`, `user_id`, `session_id`,
`region`, `tags`.

## Distribution model

- **URL path** — Zipf popularity over a fixed path set (rank weight `1/(rank+1)^1.1`): a few hot
  endpoints dominate, long tail follows. Paths are classified `page` / `api` / `static`, which
  conditions the fields below.
- **status** — conditioned on path kind; ~87% `200` overall. `static` carries more `304`
  (cache revalidation); `api` carries the `4xx`/`429`/`5xx` spread; `page` is mostly `200` with some
  redirects.
- **method** — conditioned on path kind: `static` is GET-only, `page` almost all GET, `api` a
  GET-dominant mix with POST/PUT/DELETE/PATCH. ~88% GET overall.
- **response_size** — lognormal, per path kind (`static` assets larger than `api` JSON); `304`
  responses carry size 0.
- **response_time_ms** — lognormal, per path kind (`api` slower than cached `static`); `5xx`
  responses add `+1.0` to the lognormal `mu` (≈3× slower), the one deliberate latency correlation.
- **client_ip** — Zipf popularity (`1/(rank+1)^1.1`) over a bounded pool of 100k IPs: real
  heavy hitters (bots/proxies/NAT) plus a long tail, so the IP index and CIDR filters behave like
  reality rather than one-unique-IP-per-row.
- **user_id** — Zipf activity (`1/(rank+1)^1.2`) over a 50k pool: power users vs. occasional users.
- **user_agent** — weighted toward common desktop/mobile browsers, with search bots and API clients
  in realistic minority proportions.
- **host / referer / region / protocol / query** — weighted categoricals (e.g. `-` is the common
  referer; `us-east-1` the common region; HTTP/2 the common protocol).
- **timestamp** — diurnal (hour-of-day) intensity curve (night trough, mid-day/evening peaks) and a
  weekly curve (weekday vs. weekend dip), sampled over `SPAN_DAYS` (default 30). `BASE_TS` is
  midnight-aligned so the hour offset maps to the diurnal curve.

The exponents/weights (Zipf `s`, lognormal `mu`/`sigma`, status/method tables, diurnal/weekly curves)
are the tunable parameters, defined at the top of `corpus.py`. They are consistent with published
web-traffic characteristics (Zipf page popularity, lognormal response sizes); tune them there if a
target distribution differs.

## Reproducibility & scale

- **Seeded.** All randomness derives from `random.Random(BENCH_SEED)` — same seed → byte-identical
  output (IDs included; no `uuid4`). Verified in `corpus_stats.py`.
- **Parameters (env):** `BENCH_ROWS` (rows at fraction 1.0), `BENCH_SEED`, `SPAN_DAYS`,
  `BENCH_BATCH` (Iceberg write batch).
- **Scale to a target size.** ~350–450 B/row uncompressed → **~50 GB ≈ 120–140M rows**. The k8s
  generator runs multiple pods, each with a distinct `BENCH_SEED` (disjoint data), streaming to the
  same Iceberg table — so throughput scales horizontally; single-process Python speed is not the cap.

## Validation report

Run `python corpus_stats.py --rows 300000 --seed 42`. Observed on a 300k-row sample (the shape is
what matters, not the exact percentages):

| Property | Observed | Intent |
|---|---|---|
| `status` = 200 | 85.7% | ~85–90% |
| `method` = GET | 88.5% | GET-dominant |
| path Zipf (top-10 share) | 77.0% | heavy concentration |
| client_ip | ~35k distinct; top-1000 = 75% | bounded pool + heavy hitters |
| user_id | ~19k distinct; top-100 = 72% | Zipf activity |
| response_size | p50 12 KB, p99 123 KB, max 1.4 MB; 2.9% zero (304s) | lognormal long tail |
| response_time | p50 40 ms, p99 428 ms | lognormal |
| latency correlation | 5xx 208 ms vs non-5xx 65 ms | 5xx slower |
| diurnal | peak hour 18 (6.6%) vs trough hour 3 (0.7%) | ~10× day/night swing |
| weekly | Sat/Sun lowest | weekend dip |
| kind → status | static 15% 304; api 4% 400 | cache/error by path kind |

## Not modeled

Session/journey structure (independent rows, no click paths beyond `session_id`); geo-consistent
IP↔region mapping; attack/anomaly patterns; per-user-agent path affinity; and true request/referer
chains. These are out of scope for a search-latency/ingest benchmark; add them here if a future test
needs them.

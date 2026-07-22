"""Build the demo-corpus parquet: a slice of Wikipedia movie plots → our schema.

Maintainer tool, run once per release of the corpus — the output parquet is published as a
GitHub release asset that `load_movies.py` (the user-facing loader) downloads. Source is the
`vishnupriyavr/wiki-movie-plots-with-summaries` dataset on the Hugging Face Hub, itself derived
from the Kaggle "Wikipedia Movie Plots" set — Wikipedia plot text, **CC-BY-SA-4.0**. We embed a
short **synopsis** (the first few sentences of the plot) rather than the full plot, so ingest
embeds ~80 tokens/doc, not ~300; the full `plot` is kept for lexical search and reading.

Recognizable + decade-spread: the source is ordered oldest→newest (1901…), so a naive head is all
obscure pre-war films. We keep the recognizable era (>= `--from-year`, default 1980) and order the
output **round-robin across decades** (newest decade first, newest films first within a decade), so
even the default head-slice (`DEMO_DATA_SIZE`) spans the decades — the `released` date range,
`decade` facet, and per-decade aggregates all have something to show — while staying modern and
recognizable.

Usage (host, needs pyarrow; or via the demo-data image with an overridden entrypoint):
    python build_movies_slice.py --src movies-src.parquet --max 20000 --out movies-20k.parquet
    # --src may also be the HF URL (downloaded on the fly)
"""

import argparse
import datetime
import re
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

# Split on sentence enders followed by whitespace — good enough for plot prose.
_SENT = re.compile(r"(?<=[.!?])\s+")
_WS = re.compile(r"\s+")


def norm(s: str) -> str:
    """Collapse whitespace; `None`/NaN → ''."""
    return _WS.sub(" ", str(s)).strip() if s else ""


def synopsis(plot: str, sentences: int = 3, max_words: int = 90) -> str:
    """The first few sentences of the plot, hard-capped in words — the embed source."""
    lead = " ".join(_SENT.split(plot)[:sentences])
    words = lead.split()
    return " ".join(words[:max_words]) if len(words) > max_words else lead


def slug(wiki_page: str, title: str, year: int) -> str:
    """Stable unique id from the Wikipedia page slug, falling back to title+year."""
    tail = wiki_page.rstrip("/").rsplit("/", 1)[-1] if wiki_page else ""
    return tail or f"{re.sub(r'[^a-z0-9]+', '_', title.lower()).strip('_')}_{year}"


def epoch_ms(year: int) -> int:
    """Release year → Jan-1 midnight-UTC epoch-ms (0 when absent/malformed)."""
    try:
        dt = datetime.datetime(int(year), 1, 1, tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def decade_round_robin(rows: list[dict]) -> list[dict]:
    """Interleave rows across decades (newest decade first, newest films first within each) so a
    head-slice of any size spans the decades instead of collapsing to the most recent one."""
    from collections import defaultdict

    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        buckets[r["decade"]].append(r)
    for b in buckets.values():
        b.sort(key=lambda r: r["released"], reverse=True)
    order = sorted(buckets, reverse=True)  # 2010s, 2000s, 1990s, 1980s
    out, i = [], 0
    while any(buckets[d] for d in order):
        b = buckets[order[i % len(order)]]
        if b:
            out.append(b.pop(0))
        i += 1
    return out


def build(src: str, max_records: int, from_year: int) -> list[dict]:
    if src.startswith(("http://", "https://")):
        print(f"downloading source {src}")
        path, _ = urllib.request.urlretrieve(src, "/tmp/movies-src.parquet")
        src = path
    table = pq.read_table(src)
    cols = {c: table.column(c).to_pylist() for c in table.column_names}
    n = table.num_rows

    seen: set[str] = set()
    rows = []
    for i in range(n):
        plot = norm(cols["Plot"][i])
        title = norm(cols["Title"][i])
        genre = norm(cols["Genre"][i]).lower()
        # Skip the un-plotted / un-genred rows — they make semantic search look broken.
        if len(plot.split()) < 20 or not title or genre in ("", "unknown"):
            continue
        try:
            year = int(cols["Release Year"][i])
        except (ValueError, TypeError):
            continue
        if year < from_year:
            continue
        wid = slug(norm(cols["Wiki Page"][i]), title, year)
        if wid in seen:
            continue
        seen.add(wid)
        rows.append(
            {
                "id": wid,
                "title": title,
                "plot": plot,
                "synopsis": synopsis(plot),
                # Primary genre for the facet; full string kept for term matching.
                "genre": genre.split(",")[0].split("/")[0].strip(),
                "genres": norm(cols["Genre"][i]),
                "origin": norm(cols["Origin/Ethnicity"][i]),
                "director": norm(cols["Director"][i]),
                "cast": norm(cols["Cast"][i]),
                "released": epoch_ms(year),
                "year": str(year),
                "decade": f"{(year // 10) * 10}s",
            }
        )

    # Decade-balanced interleave, then cap — a modern, recognizable, decade-spanning slice.
    return decade_round_robin(rows)[:max_records]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="movies-src.parquet", help="source parquet path or URL")
    ap.add_argument("--max", type=int, default=20_000, help="max films to keep")
    ap.add_argument("--out", default="movies-20k.parquet", help="output parquet path")
    ap.add_argument("--from-year", type=int, default=1980, help="keep films released in/after this year")
    args = ap.parse_args()

    rows = build(args.src, args.max, args.from_year)
    schema = pa.schema(
        [
            ("id", pa.string()),
            ("title", pa.string()),
            ("plot", pa.string()),
            ("synopsis", pa.string()),
            ("genre", pa.string()),
            ("genres", pa.string()),
            ("origin", pa.string()),
            ("director", pa.string()),
            ("cast", pa.string()),
            ("released", pa.int64()),  # epoch-ms → DATE via `format: epoch_ms`
            ("year", pa.string()),
            ("decade", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, args.out, compression="zstd")
    print(f"wrote {table.num_rows} films to {args.out}")


if __name__ == "__main__":
    main()

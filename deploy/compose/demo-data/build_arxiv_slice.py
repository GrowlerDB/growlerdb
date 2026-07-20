"""Build the demo-corpus parquet: harvest arXiv **cs** titles+abstracts via OAI-PMH.

Maintainer tool, run once per release of the corpus — the output parquet is published as a
GitHub release asset that `load_arxiv.py` (the user-facing loader) downloads. arXiv metadata is
CC0; be polite to the OAI endpoint (it batches ~1000 records per request and rate-limits with
503 + Retry-After, which this honors).

Usage (host, needs pyarrow; or via the demo-data image with an overridden entrypoint):
    python build_arxiv_slice.py --max 20000 --out arxiv-cs-20k.parquet
"""

import argparse
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

OAI = "https://oaipmh.arxiv.org/oai"
NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "arxiv": "http://arxiv.org/OAI/arXiv/",
}


def fetch(params: dict) -> ET.Element:
    """One OAI request, honoring 503 Retry-After (arXiv's rate-limit signal)."""
    url = f"{OAI}?{urllib.parse.urlencode(params)}"
    for _ in range(10):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                return ET.fromstring(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 503:
                wait = int(e.headers.get("Retry-After", "10"))
                print(f"rate-limited; retrying in {wait}s")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("OAI endpoint kept rate-limiting; giving up")


def epoch_ms(date: str) -> int:
    """`YYYY-MM-DD` → midnight-UTC epoch-ms (0 when absent/malformed)."""
    try:
        import datetime

        y, m, d = (int(p) for p in date.split("-"))
        dt = datetime.datetime(y, m, d, tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, AttributeError):
        return 0


def harvest(max_records: int, from_date: str):
    rows = []
    params = {"verb": "ListRecords", "metadataPrefix": "arXiv", "set": "cs"}
    # OAI harvests oldest-first; without a floor the slice is all 1990s/2000s papers — the
    # demo wants a corpus that can answer questions about current techniques.
    if from_date:
        params["from"] = from_date
    while len(rows) < max_records:
        root = fetch(params)
        for rec in root.iterfind(".//oai:record", NS):
            meta = rec.find(".//arxiv:arXiv", NS)
            if meta is None:  # deleted record
                continue
            get = lambda tag: (meta.findtext(f"arxiv:{tag}", "", NS) or "").strip()
            authors = ", ".join(
                " ".join(
                    filter(
                        None,
                        [a.findtext("arxiv:forenames", "", NS), a.findtext("arxiv:keyname", "", NS)],
                    )
                ).strip()
                for a in meta.iterfind(".//arxiv:author", NS)
            )
            categories = get("categories")
            rows.append(
                {
                    "id": get("id"),
                    "title": " ".join(get("title").split()),
                    "abstract": " ".join(get("abstract").split()),
                    "authors": authors,
                    "categories": categories,
                    "primary_category": categories.split(" ", 1)[0] if categories else "",
                    "published": epoch_ms(get("created")),
                    "updated": epoch_ms(get("updated") or get("created")),
                }
            )
            if len(rows) >= max_records:
                break
        token = root.findtext(".//oai:resumptionToken", "", NS)
        if not token or len(rows) >= max_records:
            break
        print(f"{len(rows)} harvested...")
        params = {"verb": "ListRecords", "resumptionToken": token}
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=20_000, help="max papers to harvest")
    ap.add_argument("--out", default="arxiv-cs-20k.parquet", help="output parquet path")
    ap.add_argument(
        "--from",
        dest="from_date",
        default="2022-01-01",
        help="OAI datestamp floor (YYYY-MM-DD; empty = from the beginning of arXiv)",
    )
    args = ap.parse_args()

    rows = harvest(args.max, args.from_date)
    schema = pa.schema(
        [
            ("id", pa.string()),
            ("title", pa.string()),
            ("abstract", pa.string()),
            ("authors", pa.string()),
            ("categories", pa.string()),
            ("primary_category", pa.string()),
            ("published", pa.int64()),  # epoch-ms → DATE via `format: epoch_ms`
            ("updated", pa.int64()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, args.out, compression="zstd")
    print(f"wrote {table.num_rows} papers to {args.out}")


if __name__ == "__main__":
    main()

"""Smoke test for the Python client — driven cross-process by the Rust test harness.

Usage: python3 smoke.py <rest-base-url>
Exits 0 on success; prints a reason and exits 1 on any failed assertion.

Assumes the harness seeded `docs` with doc-1 (berlin, rank 30) and doc-2 (bern,
rank 10), checkpoint iceberg_snapshot:5.
"""

import sys

from growlerdb import Client, GrowlerError


def main(url: str) -> None:
    client = Client(url)

    # Search: rank desc → doc-1 (30) before doc-2 (10).
    res = client.search("body:iceberg", limit=10, sort=[("rank", True)])
    ids = [h["coordinates"]["identifier"][0]["value"] for h in res["hits"]]
    assert ids == ["doc-1", "doc-2"], ids
    assert res["total"] == 2, res

    # Autocomplete: city prefix "ber" → berlin, bern.
    sug = client.suggest_prefix("city", "ber", limit=10)
    assert [s["text"] for s in sug["suggestions"]] == ["berlin", "bern"], sug

    # Did-you-mean: "berlim" → berlin (edit distance 1).
    dym = client.suggest_fuzzy("city", "berlim", limit=10, max_edits=1)
    assert [s["text"] for s in dym["suggestions"]] == ["berlin"], dym

    # Admin: describe the served index.
    stats = client.describe_index("")
    assert stats["name"] == "docs", stats
    assert stats["num_docs"] == 2, stats
    assert stats["checkpoint"] == "iceberg_snapshot:5", stats

    # A bad request surfaces as a GrowlerError with HTTP 400.
    try:
        client.suggest_prefix("nope", "x", limit=10)
        raise AssertionError("expected an error for an unknown field")
    except GrowlerError as e:
        assert e.status == 400, e

    print("python sdk smoke ok")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 smoke.py <rest-base-url>", file=sys.stderr)
        sys.exit(2)
    try:
        main(sys.argv[1])
    except AssertionError as exc:
        print(f"smoke assertion failed: {exc}", file=sys.stderr)
        sys.exit(1)

"""A runnable example against a `growlerdb serve --rest-addr 127.0.0.1:8080`.

    python3 example.py            # uses http://127.0.0.1:8080
    python3 example.py http://host:port
"""

import sys

from growlerdb import Client


def main(url: str) -> None:
    client = Client(url)

    print("== describe ==")
    print(client.describe_index())

    print("== search (rank desc) ==")
    res = client.search("body:iceberg", limit=10, sort=[("rank", True)])
    print(f"{res['total']} hit(s)")
    for hit in res["hits"]:
        ident = {f["name"]: f["value"] for f in hit["coordinates"].get("identifier", [])}
        print(f"  {ident}  score={hit['score']}")

    print("== autocomplete city 'ber' ==")
    for s in client.suggest_prefix("city", "ber")["suggestions"]:
        print(f"  {s['text']}  ({s['count']})")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080")

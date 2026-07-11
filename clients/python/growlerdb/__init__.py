"""GrowlerDB — first-party Python client for the Engine API.

A small, dependency-free client over the REST/JSON gateway (`/v1/...`), which
mirrors the `growlerdb.v1` gRPC surface 1:1. Uses only the standard library, so it
works anywhere a recent Python does — no grpcio/protobuf build required.

    from growlerdb import Client
    client = Client("http://127.0.0.1:8080")
    hits = client.search("body:iceberg", limit=10, sort=[("rank", True)])
    for hit in hits["hits"]:
        print(hit["coordinates"], hit["score"])
"""

from .client import Client, GrowlerError, coordinates

__all__ = ["Client", "GrowlerError", "coordinates"]
__version__ = "0.1.0"

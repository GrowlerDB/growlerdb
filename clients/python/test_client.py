"""Offline unit tests for the Python client's auth headers.

Runnable without a live stack: `python3 -m unittest test_client` (or `python3 test_client.py`).
Intercepts the outgoing `urllib` request to assert which headers are set, rather than reaching a node.
"""

import unittest
from contextlib import contextmanager
from unittest import mock

from growlerdb import Client


class _FakeResp:
    """Minimal context-manager stand-in for an HTTP response."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return b"{}"


@contextmanager
def capture_request():
    """Patch `urlopen` and yield a list that receives the one Request the client builds."""
    seen = []

    def fake_urlopen(req, timeout=None):
        seen.append(req)
        return _FakeResp()

    with mock.patch("growlerdb.client.urllib.request.urlopen", fake_urlopen):
        yield seen


class SearchBodyTests(unittest.TestCase):
    def test_hydrate_opt_in_rides_the_search_body(self):
        import json

        with capture_request() as seen:
            Client("http://x").search("q", hydrate=True, hydrate_columns=["body"])
        body = json.loads(seen[0].data)
        self.assertTrue(body["hydrate"])
        self.assertEqual(body["hydrate_columns"], ["body"])

    def test_hydrate_absent_by_default(self):
        import json

        with capture_request() as seen:
            Client("http://x").search("q")
        body = json.loads(seen[0].data)
        self.assertNotIn("hydrate", body)


class AuthHeaderTests(unittest.TestCase):
    def test_token_sends_bearer_and_no_identity_headers(self):
        with capture_request() as seen:
            Client("http://x", token="abc.def.ghi").describe_index("")
        req = seen[0]
        self.assertEqual(req.get_header("Authorization"), "Bearer abc.def.ghi")
        self.assertFalse(req.has_header("X-growlerdb-principal"))
        self.assertFalse(req.has_header("X-growlerdb-tenant"))

    def test_no_auth_by_default(self):
        with capture_request() as seen:
            Client("http://x").describe_index("")
        self.assertFalse(seen[0].has_header("Authorization"))


if __name__ == "__main__":
    unittest.main()

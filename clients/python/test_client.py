"""Offline unit tests for the Python client's auth headers.

Runnable without a live stack: `python3 -m unittest test_client` (or `python3 test_client.py`).
Intercepts the outgoing `urllib` request to assert which headers are set, rather than reaching a node.
"""

import unittest
import warnings
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


class AuthHeaderTests(unittest.TestCase):
    def test_token_sends_bearer_and_no_identity_headers(self):
        with capture_request() as seen:
            Client("http://x", token="abc.def.ghi").describe_index("")
        req = seen[0]
        self.assertEqual(req.get_header("Authorization"), "Bearer abc.def.ghi")
        self.assertFalse(req.has_header("X-growlerdb-principal"))
        self.assertFalse(req.has_header("X-growlerdb-tenant"))

    def test_identity_headers_are_off_by_default(self):
        # principal/tenant without the dev flag → warned and NOT sent (no impersonation vector).
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = Client("http://x", principal="alice", tenant="acme")
        self.assertTrue(any("dev_identity_headers" in str(w.message) for w in caught))
        with capture_request() as seen:
            client.describe_index("")
        req = seen[0]
        self.assertFalse(req.has_header("X-growlerdb-principal"))
        self.assertFalse(req.has_header("X-growlerdb-tenant"))

    def test_dev_identity_headers_opt_in(self):
        with capture_request() as seen:
            Client(
                "http://x",
                principal="alice",
                tenant="acme",
                dev_identity_headers=True,
            ).describe_index("")
        req = seen[0]
        self.assertEqual(req.get_header("X-growlerdb-principal"), "alice")
        self.assertEqual(req.get_header("X-growlerdb-tenant"), "acme")

    def test_no_auth_by_default(self):
        with capture_request() as seen:
            Client("http://x").describe_index("")
        self.assertFalse(seen[0].has_header("Authorization"))


if __name__ == "__main__":
    unittest.main()

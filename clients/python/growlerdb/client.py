"""The GrowlerDB REST/JSON client (stdlib only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple


class GrowlerError(Exception):
    """An error returned by a GrowlerDB node.

    `status` is the HTTP status; `code`/`message` come from the server's structured
    `{code, message}` body (the same model as the gRPC `Error`).
    """

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message


def coordinates(
    identifier: Dict[str, Any], partition: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Build a document-coordinates object from `{field: value}` maps.

    Identifier/partition order matters (it defines the composite key encoding), so
    pass an ordered mapping when a field order is significant.
    """
    fields = lambda m: [{"name": n, "value": v} for n, v in m.items()]
    out: Dict[str, Any] = {"identifier": fields(identifier)}
    if partition:
        out["partition"] = fields(partition)
    return out


class Client:
    """A client over one node's REST/JSON gateway (`/v1/...`).

    Covers Search, GetByKey, Suggest, and Admin.

    Pass ``token`` — an OIDC bearer or a GrowlerDB API token — sent as
    ``Authorization: Bearer <token>``. Identity and roles come from the verified token,
    never from the client, so a caller cannot assert who they are.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    # ---- read APIs -------------------------------------------------------------

    def search(
        self,
        query: str,
        *,
        limit: int = 10,
        offset: int = 0,
        sort: Optional[Sequence[Tuple[str, bool]]] = None,
        collapse: Optional[str] = None,
        pit_id: int = 0,
        search_after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run a search. `sort` is a list of `(field, descending)` pairs."""
        body: Dict[str, Any] = {"query": query, "limit": limit, "offset": offset}
        if sort:
            body["sort"] = [{"field": f, "desc": bool(d)} for f, d in sort]
        if collapse:
            body["collapse"] = collapse
        if pit_id:
            body["pit_id"] = pit_id
        if search_after:
            body["search_after"] = search_after
        return self._post("/v1/search", body)

    def get_by_key(
        self, keys: List[Dict[str, Any]], columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Hydrate coordinates (see `coordinates`) to rows. `columns` empty = all."""
        return self._post("/v1/keys:get", {"keys": keys, "columns": columns or []})

    def suggest_prefix(self, field: str, prefix: str, limit: int = 10) -> Dict[str, Any]:
        """Autocomplete: prefix completions for `field`."""
        return self._post(
            "/v1/suggest", {"field": field, "text": prefix, "limit": limit}
        )

    def suggest_fuzzy(
        self, field: str, text: str, limit: int = 10, max_edits: int = 2
    ) -> Dict[str, Any]:
        """Did-you-mean: terms within `max_edits` of `text` for `field`."""
        return self._post(
            "/v1/suggest",
            {
                "field": field,
                "text": text,
                "limit": limit,
                "fuzzy": True,
                "max_edits": max_edits,
            },
        )

    def describe_index(self, index: str = "") -> Dict[str, Any]:
        """Status/stats of an index (`index` empty = the served index)."""
        return self._post("/v1/index:describe", {"index": index})

    # ---- transport -------------------------------------------------------------

    def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.base_url + path, data=data, method="POST")
        req.add_header("content-type", "application/json")
        # Identity comes from the verified token, not the caller's word.
        if self.token:
            req.add_header("authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            payload: Dict[str, Any] = {}
            try:
                payload = json.loads(e.read() or b"{}")
            except Exception:
                pass
            raise GrowlerError(
                e.code, payload.get("code", ""), payload.get("message", str(e))
            ) from None

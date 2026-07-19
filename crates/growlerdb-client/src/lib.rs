//! `growlerdb-client` — the first-party **Rust client** for the GrowlerDB Engine API
//! ([Design 01]). A thin, ergonomic wrapper over the generated `growlerdb.v1`
//! tonic clients: one [`Client`] multiplexes Search / GetByKey / Suggest / Admin over a
//! single channel, with a small [`SearchQuery`] builder so callers needn't hand-build
//! proto messages.
//!
//! ```no_run
//! # async fn run() -> Result<(), growlerdb_client::ClientError> {
//! use growlerdb_client::{Client, SearchQuery};
//! let client = Client::connect("http://127.0.0.1:50051").await?;
//! let hits = client
//!     .search(SearchQuery::new("body:iceberg").limit(10).sort("rank", true))
//!     .await?;
//! for hit in hits.hits {
//!     println!("{:?} ({})", hit.coordinates, hit.score);
//! }
//! # Ok(()) }
//! ```
//!
//! [Design 01]: ../../../okf/product/interfaces/grpc.md

use growlerdb_proto::v1::admin_client::AdminClient;
use growlerdb_proto::v1::lookup_client::LookupClient;
use growlerdb_proto::v1::search_client::SearchClient;
use growlerdb_proto::v1::suggest_client::SuggestClient;
use tonic::transport::Channel;

/// The generated `growlerdb.v1` request/response types, re-exported for callers.
pub use growlerdb_proto::v1 as proto;

use proto::{
    Coordinates, GetByKeyRequest, GetByKeyResponse, IndexStats, SearchRequest, SearchResponse,
    Sort, SuggestKind, SuggestRequest, SuggestResponse,
};

/// An error talking to a GrowlerDB node.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The endpoint string was not a valid URI.
    #[error("invalid endpoint: {0}")]
    Endpoint(String),
    /// Establishing the channel failed (unreachable / handshake).
    #[error("connect: {0}")]
    Connect(#[from] tonic::transport::Error),
    /// The RPC returned an error status (the structured `code`/`message` is on it).
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
}

/// A connected client over one node's Engine API. Cheap to [`Clone`] (the channel and
/// the per-service clients are reference-counted); methods take `&self`.
#[derive(Clone)]
pub struct Client {
    search: SearchClient<Channel>,
    lookup: LookupClient<Channel>,
    suggest: SuggestClient<Channel>,
    admin: AdminClient<Channel>,
}

/// Default TCP connect timeout — bounds `connect()` so an unreachable/slow endpoint fails fast
/// instead of hanging.
const DEFAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Default per-request timeout — bounds every RPC so a wedged node can't hang the caller forever.
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl Client {
    /// Connect to a node at `endpoint` (e.g. `http://127.0.0.1:50051`), with default connect and
    /// per-request timeouts ([`DEFAULT_CONNECT_TIMEOUT`] / [`DEFAULT_REQUEST_TIMEOUT`]).
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_with(endpoint, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT).await
    }

    /// As [`connect`](Self::connect) with explicit `connect_timeout` (TCP/handshake) and `request_timeout`
    /// (applied to every RPC) — so neither a dead endpoint nor a wedged node can hang the caller
    /// indefinitely.
    pub async fn connect_with(
        endpoint: impl Into<String>,
        connect_timeout: std::time::Duration,
        request_timeout: std::time::Duration,
    ) -> Result<Self, ClientError> {
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|e| ClientError::Endpoint(e.to_string()))?
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .connect()
            .await?;
        Ok(Self {
            search: SearchClient::new(channel.clone()),
            lookup: LookupClient::new(channel.clone()),
            suggest: SuggestClient::new(channel.clone()),
            admin: AdminClient::new(channel),
        })
    }

    /// Build a [`Client`] over an existing tonic [`Channel`] (custom TLS, interceptors).
    pub fn with_channel(channel: Channel) -> Self {
        Self {
            search: SearchClient::new(channel.clone()),
            lookup: LookupClient::new(channel.clone()),
            suggest: SuggestClient::new(channel.clone()),
            admin: AdminClient::new(channel),
        }
    }

    /// Execute a search. Accepts a [`SearchQuery`] builder or a raw [`SearchRequest`].
    pub async fn search(
        &self,
        query: impl Into<SearchRequest>,
    ) -> Result<SearchResponse, ClientError> {
        Ok(self.search.clone().search(query.into()).await?.into_inner())
    }

    /// Hydrate document coordinates to authoritative rows. `columns` empty = all.
    pub async fn get_by_key(
        &self,
        keys: Vec<Coordinates>,
        columns: Vec<String>,
    ) -> Result<GetByKeyResponse, ClientError> {
        Ok(self
            .lookup
            .clone()
            .get_by_key(GetByKeyRequest {
                keys,
                columns,
                window: 0,
                // Single-index client: the endpoint's default index.
                index: String::new(),
            })
            .await?
            .into_inner())
    }

    /// Autocomplete: prefix completions for `field`.
    pub async fn suggest_prefix(
        &self,
        field: impl Into<String>,
        prefix: impl Into<String>,
        limit: u32,
    ) -> Result<SuggestResponse, ClientError> {
        self.suggest(field, prefix, limit, SuggestKind::Prefix, 0)
            .await
    }

    /// Did-you-mean: terms within `max_edits` of `text` for `field`.
    pub async fn suggest_fuzzy(
        &self,
        field: impl Into<String>,
        text: impl Into<String>,
        limit: u32,
        max_edits: u32,
    ) -> Result<SuggestResponse, ClientError> {
        self.suggest(field, text, limit, SuggestKind::Fuzzy, max_edits)
            .await
    }

    async fn suggest(
        &self,
        field: impl Into<String>,
        text: impl Into<String>,
        limit: u32,
        kind: SuggestKind,
        max_edits: u32,
    ) -> Result<SuggestResponse, ClientError> {
        Ok(self
            .suggest
            .clone()
            .suggest(SuggestRequest {
                field: field.into(),
                text: text.into(),
                limit,
                kind: kind as i32,
                max_edits,
                // The window selector is gateway-internal; a client never sets it.
                window: 0,
                // Single-index client: the endpoint's default index.
                index: String::new(),
            })
            .await?
            .into_inner())
    }

    /// Status/stats of an index (`index` empty = the served index).
    pub async fn describe_index(
        &self,
        index: impl Into<String>,
    ) -> Result<IndexStats, ClientError> {
        let resp = self
            .admin
            .clone()
            .describe_index(proto::DescribeIndexRequest {
                index: index.into(),
                window: 0,
            })
            .await?
            .into_inner();
        Ok(resp.stats.unwrap_or_default())
    }
}

/// An ergonomic builder for a [`SearchRequest`] — set just the parts you need.
#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    query: String,
    limit: u32,
    offset: u32,
    sort: Vec<Sort>,
    collapse: String,
    pit_id: u64,
    search_after: Vec<u8>,
    index: String,
    highlight: Option<growlerdb_proto::v1::HighlightRequest>,
}

impl SearchQuery {
    /// Start from a Lucene/KQL query string.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            ..Default::default()
        }
    }

    /// Scope the search to a named index; empty = the index the endpoint serves.
    pub fn index(mut self, index: impl Into<String>) -> Self {
        self.index = index.into();
        self
    }

    /// Page size (top-K).
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// From/size offset (ignored when [`after`](Self::after) is set).
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    /// Append a sort key (`descending` = largest first). Earlier keys dominate.
    pub fn sort(mut self, field: impl Into<String>, descending: bool) -> Self {
        self.sort.push(Sort {
            field: field.into(),
            descending,
        });
        self
    }

    /// Collapse to the top hit per distinct value of `field` (requires a sort key).
    pub fn collapse(mut self, field: impl Into<String>) -> Self {
        self.collapse = field.into();
        self
    }

    /// Read against an open point-in-time snapshot (from `OpenPit`).
    pub fn pit(mut self, pit_id: u64) -> Self {
        self.pit_id = pit_id;
        self
    }

    /// Opt into **server-side highlighting**: the hits carry matched fragments per field.
    /// `fields` empty ⇒ the index's default highlightable TEXT fields; the bounds accept 0 for the
    /// server defaults. Off unless called (highlighting is a per-hit cost).
    pub fn highlight(
        mut self,
        fields: Vec<String>,
        max_fragments: u32,
        fragment_size: u32,
    ) -> Self {
        self.highlight = Some(growlerdb_proto::v1::HighlightRequest {
            fields,
            max_fragments,
            fragment_size,
        });
        self
    }

    /// Keyset cursor from a prior response's `next_cursor` (deep paging).
    pub fn after(mut self, cursor: Vec<u8>) -> Self {
        self.search_after = cursor;
        self
    }
}

impl From<SearchQuery> for SearchRequest {
    fn from(q: SearchQuery) -> Self {
        SearchRequest {
            query: q.query,
            limit: q.limit,
            offset: q.offset,
            sort: q.sort,
            collapse: q.collapse,
            pit_id: q.pit_id,
            search_after: q.search_after,
            // Default scoring (per-shard BM25); SCORE_GLOBAL is reserved.
            score_mode: growlerdb_proto::v1::ScoreMode::ScoreLocal as i32,
            // The window selector is gateway-internal; a client request never sets it.
            window: 0,
            // Lucene grammar; the SDK can expose a KQL option later.
            syntax: growlerdb_proto::v1::QuerySyntax::Lucene as i32,
            index: q.index,
            // Server-side highlighting opt-in; None ⇒ off.
            highlight: q.highlight,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_query_builds_the_proto_request() {
        let req: SearchRequest = SearchQuery::new("body:iceberg")
            .limit(25)
            .offset(50)
            .sort("rank", true)
            .sort("id", false)
            .collapse("category")
            .pit(7)
            .into();
        assert_eq!(req.query, "body:iceberg");
        assert_eq!(req.limit, 25);
        assert_eq!(req.offset, 50);
        assert_eq!(req.collapse, "category");
        assert_eq!(req.pit_id, 7);
        assert_eq!(req.sort.len(), 2);
        assert_eq!(req.sort[0].field, "rank");
        assert!(req.sort[0].descending);
        assert!(!req.sort[1].descending);
    }
}

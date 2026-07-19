# Interfaces

The touchpoints through which users reach GrowlerDB — all over one Engine API.

# Programmatic

* [gRPC API](/product/interfaces/grpc.md) - the primary Protobuf/gRPC surface (Search, Suggest, Lookup, Admin, Write, ControlPlane, System)
* [REST API](/product/interfaces/rest.md) - the /v1/* HTTP+JSON facade over the same operations
* [Client SDKs](/product/interfaces/client-sdks.md) - Python and Rust clients
* [OpenSearch _search adapter](/product/interfaces/opensearch-adapter.md) - optional OpenSearch-compatible read endpoint
* [MCP retrieval server](/product/interfaces/mcp-server.md) - read-only Model Context Protocol server exposing governed retrieval to AI agents, token-scoped
* [SQL UDFs (Trino / Spark)](/product/interfaces/sql-udfs.md) - search-then-join from SQL engines

# Operational

* [CLI](/product/interfaces/cli.md) - the growlerdb binary: build indexes, run components, query
* [Console UI](/product/interfaces/ui.md) - the web console, served by the engine

# Project

* [Website (growlerdb.com)](/product/interfaces/website.md) - the documentation site
* [Git repository](/product/interfaces/git-repo.md) - issues, PRs, releases, contribution

# Functional

The capabilities GrowlerDB exposes — the user-visible "what," independent of implementation.

# Read path

* [Search](/product/functional/search/) - query, syntax, facets, suggest, sort/paging, highlight, export
* [Hydration](/product/functional/hydration.md) - resolve coordinates to the authoritative Iceberg rows

# Write, admin & ops

* [Auth](/product/functional/auth/) - login, logout, tokens, mTLS
* [RBAC & tenancy](/product/functional/rbac-and-tenancy.md) - roles and tenant isolation
* [User management](/product/functional/user-management.md) - users, roles, credentials
* [Index management](/product/functional/index-management/) - create, alter, drop, reindex, compact, backup, aliases/ILM
* [Ingestion](/product/functional/ingestion/) - streaming, CDC, exactly-once checkpoints
* [Windowing & time](/product/functional/windowing-time.md) - time-partitioned indexes
* [Cold tiering](/product/functional/cold-tiering.md) - old windows served from object storage
* [Replicas](/product/functional/replicas.md) - read replicas via segment shipping
* [Observability](/product/functional/observability.md) - SLI dashboards and alerts

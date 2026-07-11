# Known limitations

* [Partial data-plane authorization](/quality/known-limitations/partial-polaris-authz.md) - Hydration is catalog-governed and tenant isolation is enforced, but full catalog-policy enforcement on the data plane is partial.
* [Vector / hybrid search deferred](/quality/known-limitations/vector-deferred.md) - Embeddings, ANN/KNN, and reranking (the RAG hybrid-retrieval path) are decided but not yet shipped.
* [Windowed / multi-shard replicas](/quality/known-limitations/windowed-replica-gap.md) - Read replicas are single-shard today; zero-downtime windowed or multi-shard replica sets are future work.
* [Windowed index k8s deployment topology (resolved)](/quality/known-limitations/windowed-k8s-topology.md) - RESOLVED: a windowed index now deploys via a control-plane-driven windowed node topology (nodes serve CP-assigned time windows, connector streams to owners, gateway hot-reloads). Residual follow-ups only: window replicas, resume bounding, worker parallelism, window-aware source maintenance.
* [Scale numbers unvalidated](/quality/known-limitations/scale-unvalidated.md) - Performance and scale targets are v1 design targets, pending a real-hardware benchmark run.
* [Scale ceilings toward 10s–100s TB](/quality/known-limitations/scale-ceilings.md) - Code-grounded map of the structural ceilings on the path to 100 TB (ingest scale-out, maintenance, the location-array hot floor, reconcile, fan-out, control-plane constants) and the temporal-vs-non-temporal fork.

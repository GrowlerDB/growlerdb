# The GrowlerDB Spark changelog connector, packaged for Kubernetes. Builds the fat jar
# (the build needs the shared protos at crates/growlerdb-proto/proto) and bakes it into the Spark
# runtime image the connector pod runs via spark-submit. Build from the REPO ROOT:
#   docker build -t ghcr.io/growlerdb/growlerdb-connector:dev -f deploy/k8s/streaming/connector.Dockerfile .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /repo
# Only what the connector build needs: its module + the shared .proto source (protoSourceRoot is
# ../crates/growlerdb-proto/proto). protoc + the gRPC plugin are fetched by the protobuf maven plugin.
COPY connector ./connector
COPY crates/growlerdb-proto/proto ./crates/growlerdb-proto/proto
WORKDIR /repo/connector
RUN mvn -q -B -DskipTests package

FROM apache/spark:4.0.0
COPY --from=build /repo/connector/target/growlerdb-connector-0.0.0.jar /opt/growlerdb/connector.jar
# Drop root — the Spark image ships a `spark` user (UID 185); the jar is world-readable so submit works.
USER spark

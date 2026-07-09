package io.growlerdb.trino;

import io.trino.spi.connector.ConnectorSplit;

/** A single, info-less split: the {@link GrowlerDbSearchHandle} carries the query the worker runs. */
public class GrowlerDbSplit implements ConnectorSplit {}

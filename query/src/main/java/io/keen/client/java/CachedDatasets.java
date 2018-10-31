package io.keen.client.java;

import io.keen.client.java.result.IntervalResultValue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface CachedDatasets {

    Map<String, Object> getDefinition(String datasetName) throws IOException;

    List<IntervalResultValue> getResults(String datasetName, String indexBy, Timeframe timeframe, Collection<String> groupByParams) throws IOException;

}

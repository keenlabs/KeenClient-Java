package io.keen.client.java;

import io.keen.client.java.result.IntervalResultValue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface CachedDatasets {

    DatasetDefinition create(String datasetName, String displayName, DatasetQuery query, Set<String> indexBy) throws IOException;

    DatasetDefinition getDefinition(String datasetName) throws IOException;

    List<IntervalResultValue> getResults(DatasetDefinition datasetDefinition, String indexBy, Timeframe timeframe) throws IOException;

    List<DatasetDefinition> getDefinitionsByProject() throws IOException;

    List<DatasetDefinition> getDefinitionsByProject(Integer limit, String afterName) throws IOException;

    boolean delete(String datasetName) throws IOException;
}

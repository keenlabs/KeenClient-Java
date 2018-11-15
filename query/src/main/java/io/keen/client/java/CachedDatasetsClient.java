package io.keen.client.java;

import io.keen.client.java.result.IntervalResultValue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class CachedDatasetsClient implements CachedDatasets {

    private KeenQueryClient keenQueryClient;

    CachedDatasetsClient(KeenQueryClient keenQueryClient) {
        this.keenQueryClient = keenQueryClient;
    }

    @Override
    public DatasetDefinition create(String datasetName, String displayName, DatasetQuery query, Collection<String> indexBy) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.creationRequest(datasetName, displayName, query, indexBy);

        return DatasetDefinition.fromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public DatasetDefinition getDefinition(String datasetName) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.definitionRequest(datasetName);

        return DatasetDefinition.fromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public List<IntervalResultValue> getResults(DatasetDefinition datasetDefinition, Map<String, ?> indexByValues, Timeframe timeframe) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.resultsRequest(datasetDefinition, indexByValues, timeframe);

        return keenQueryClient.execute(request).getIntervalResults();
    }

    @Override
    public List<DatasetDefinition> getDefinitionsByProject() throws IOException {
        return getDefinitionsByProject(null, null);
    }

    @Override
    public List<DatasetDefinition> getDefinitionsByProject(Integer limit, String afterName) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.definitionsByProjectRequest(limit, afterName);

        return DatasetDefinition.definitionsFromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public boolean delete(String datasetName) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.deletionRequest(datasetName);

        return keenQueryClient.getMapResponse(request).isEmpty();
    }

}

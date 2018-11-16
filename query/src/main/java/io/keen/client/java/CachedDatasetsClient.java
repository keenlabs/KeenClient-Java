package io.keen.client.java;

import io.keen.client.java.result.IntervalResultValue;

import java.io.IOException;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.isBlank;

class CachedDatasetsClient implements CachedDatasets {

    private KeenQueryClient keenQueryClient;

    CachedDatasetsClient(KeenQueryClient keenQueryClient) {
        this.keenQueryClient = keenQueryClient;
    }

    @Override
    public DatasetDefinition create(String datasetName, String displayName, DatasetQuery query, Collection<String> indexBy) throws IOException {
        if (isBlank(datasetName)) {
            throw new IllegalArgumentException("Dataset name cannot be blank");
        }
        if (isBlank(displayName)) {
            throw new IllegalArgumentException("Display name cannot be blank");
        }
        if (query == null) {
            throw new IllegalArgumentException("Dataset query is required");
        }
        if (indexBy == null || indexBy.isEmpty()) {
            throw new IllegalArgumentException("At least one index property is required");
        }

        KeenQueryRequest request = CachedDatasetRequest.creationRequest(datasetName, displayName, query, indexBy);
        return DatasetDefinition.fromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public DatasetDefinition getDefinition(String datasetName) throws IOException {
        if (isBlank(datasetName)) {
            throw new IllegalArgumentException("Dataset name cannot be blank");
        }

        KeenQueryRequest request = CachedDatasetRequest.definitionRequest(datasetName);
        return DatasetDefinition.fromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public List<IntervalResultValue> getResults(DatasetDefinition datasetDefinition, Map<String, ?> indexByValues, Timeframe timeframe) throws IOException {
        if (datasetDefinition == null) {
            throw new IllegalArgumentException("Dataset definition is required");
        }

        Set<String> sortedDefinitionIndexProperties = new TreeSet<String>(datasetDefinition.getIndexBy());
        if (indexByValues == null || indexByValues.isEmpty()) {
            throw new IllegalArgumentException("Values for all index_by properties are required: " + sortedDefinitionIndexProperties);
        }

        Set<String> sortedQueryIndexProperties = new TreeSet<String>(indexByValues.keySet());
        if (!sortedDefinitionIndexProperties.equals(sortedQueryIndexProperties)) {
            throw new IllegalArgumentException("Values for the following index_by properties must be present: " + sortedDefinitionIndexProperties + ". Found for: " + sortedQueryIndexProperties);
        }

        if (timeframe == null) {
            throw new IllegalArgumentException("Timeframe is required");
        }

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
        if (isBlank(datasetName)) {
            throw new IllegalArgumentException("Dataset name cannot be blank");
        }

        KeenQueryRequest request = CachedDatasetRequest.deletionRequest(datasetName);
        return keenQueryClient.getMapResponse(request).isEmpty();
    }

}

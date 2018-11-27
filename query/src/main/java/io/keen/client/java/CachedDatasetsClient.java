package io.keen.client.java;

import io.keen.client.java.result.IntervalResultValue;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.util.*;

class CachedDatasetsClient implements CachedDatasets {

    private KeenQueryClient keenQueryClient;

    CachedDatasetsClient(KeenQueryClient keenQueryClient) {
        this.keenQueryClient = keenQueryClient;
    }

    @Override
    public DatasetDefinition create(String datasetName, String displayName, DatasetQuery query, Collection<String> indexBy) throws IOException {
        Validate.notBlank(datasetName, "Dataset name cannot be blank");
        Validate.notBlank(displayName, "Display name cannot be blank");
        Validate.notNull(query, "Dataset query is required");
        Validate.notEmpty(indexBy, "At least one index property is required");

        KeenQueryRequest request = CachedDatasetRequest.creationRequest(datasetName, displayName, query, indexBy);
        return DatasetDefinition.fromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public DatasetDefinition getDefinition(String datasetName) throws IOException {
        Validate.notBlank(datasetName, "Dataset name cannot be blank");

        KeenQueryRequest request = CachedDatasetRequest.definitionRequest(datasetName);
        return DatasetDefinition.fromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public List<IntervalResultValue> getResults(DatasetDefinition datasetDefinition, Map<String, ?> indexByValues, Timeframe timeframe) throws IOException {
        Validate.notNull(datasetDefinition, "Dataset definition is required");
        Validate.notNull(timeframe, "Timeframe is required");

        Set<String> sortedDefinitionIndexProperties = new TreeSet<String>(datasetDefinition.getIndexBy());
        Validate.notEmpty(indexByValues, "Values for all index_by properties are required: %s", sortedDefinitionIndexProperties);

        Set<String> sortedQueryIndexProperties = new TreeSet<String>(indexByValues.keySet());
        if (!sortedDefinitionIndexProperties.equals(sortedQueryIndexProperties)) {
            throw new IllegalArgumentException("Values for the following index_by properties must be present: " + sortedDefinitionIndexProperties + ". Found for: " + sortedQueryIndexProperties);
        }

        KeenQueryRequest request = CachedDatasetRequest.resultsRequest(datasetDefinition, indexByValues, timeframe);
        return keenQueryClient.execute(request).getIntervalResults();
    }

    @Override
    public List<DatasetDefinition> getDefinitions() throws IOException {
        return getDefinitions(null, null);
    }

    @Override
    public List<DatasetDefinition> getDefinitions(Integer limit, String afterName) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.definitionsByProjectRequest(limit, afterName);
        return DatasetDefinition.definitionsFromMap(keenQueryClient.getMapResponse(request));
    }

    @Override
    public boolean delete(String datasetName) throws IOException {
        Validate.notBlank(datasetName, "Dataset name cannot be blank");

        KeenQueryRequest request = CachedDatasetRequest.deletionRequest(datasetName);
        return keenQueryClient.getMapResponse(request).isEmpty();
    }

}

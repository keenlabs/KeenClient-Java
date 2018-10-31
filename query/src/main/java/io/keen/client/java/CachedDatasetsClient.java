package io.keen.client.java;

import io.keen.client.java.result.IntervalResultValue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CachedDatasetsClient implements CachedDatasets {

    private KeenQueryClient keenQueryClient;

    public CachedDatasetsClient(KeenQueryClient keenQueryClient) {
        this.keenQueryClient = keenQueryClient;
    }

    @Override
    public Map<String, Object> getDefinition(String datasetName) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.definitionRequest(datasetName);

        return keenQueryClient.getMapResponse(request);
    }

    public List<IntervalResultValue> getResults(String datasetName, String indexBy, Timeframe timeframe, Collection<String> groupByParams) throws IOException {
        KeenQueryRequest request = CachedDatasetRequest.resultsRequest(datasetName, indexBy, timeframe, groupByParams);

        List<IntervalResultValue> result = keenQueryClient.execute(request).getIntervalResults();
        return result;
    }
}

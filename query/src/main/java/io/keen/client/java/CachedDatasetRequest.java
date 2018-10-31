package io.keen.client.java;

import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.http.HttpMethods;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

abstract class CachedDatasetRequest extends PersistentAnalysis {

    private CachedDatasetRequest(String httpMethod, boolean needsMasterKey, String datasetName) {
        super(httpMethod, needsMasterKey, datasetName, null);
    }

    static KeenQueryRequest definitionRequest(String datasetName) {
        return new CachedDatasetRequest(HttpMethods.GET, false, datasetName) {

            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                return urlBuilder.getDatasetsUrl(projectId, getResourceName(), null, Collections.<String, Object>emptyMap());
            }
        };
    }

    static KeenQueryRequest resultsRequest(String datasetName, final String indexBy, final Timeframe timeframe, final Collection<String> groupByParams) {
        return new CachedDatasetRequest(HttpMethods.GET, false, datasetName) {
            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                return urlBuilder.getDatasetsUrl(projectId, getResourceName(), "/results", queryArgs());
            }

            @Override
            boolean retrievingResults() {
                return true;
            }

            @Override
            boolean groupedResponseExpected() {
                return groupByParams != null && !groupByParams.isEmpty();
            }

            @Override
            Collection<String> getGroupByParams() {
                return groupByParams;
            }

            private Map<String, Object> queryArgs() {
                if (indexBy == null) {
                    throw new IllegalArgumentException("index_by is required");
                }
                if (timeframe == null) {
                    throw new IllegalArgumentException("timeframe is required");
                }

                HashMap<String, Object> queryArgs = new HashMap<String, Object>();
                queryArgs.put(KeenQueryConstants.INDEX_BY, indexBy);
                queryArgs.putAll(timeframe.constructTimeframeArgs());
                return queryArgs;
            }
        };
    }

    @Override
    String getAnalysisType() {
        return KeenQueryConstants.DATASETS;
    }

    @Override
    Map<String, Object> constructRequestArgs() {
        return emptyMap();
    }

    @Override
    boolean intervalResponseExpected() {
        return true;
    }

}

package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.http.HttpMethods;

import java.io.IOException;
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
            private final ObjectMapper objectMapper = new ObjectMapper()
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                try {
                    return urlBuilder.getDatasetsUrl(projectId, getResourceName(), "/results", queryArgs());
                } catch (IOException e) {
                    throw new KeenQueryClientException(e);
                }
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

            private Map<String, Object> queryArgs() throws IOException {
                if (indexBy == null) {
                    throw new IllegalArgumentException("index_by is required");
                }
                if (timeframe == null) {
                    throw new IllegalArgumentException("timeframe is required");
                }

                Object timeframeAsArgs = timeframe.constructTimeframeArgs().get(KeenQueryConstants.TIMEFRAME);

                HashMap<String, Object> queryArgs = new HashMap<String, Object>();
                queryArgs.put(KeenQueryConstants.INDEX_BY, indexBy);
                queryArgs.put(KeenQueryConstants.TIMEFRAME, objectMapper.writeValueAsString(timeframeAsArgs));
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

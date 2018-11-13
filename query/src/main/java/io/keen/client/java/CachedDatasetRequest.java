package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.http.HttpMethods;

import java.io.IOException;
import java.net.URL;
import java.util.*;

import static java.util.Collections.emptyMap;

abstract class CachedDatasetRequest extends PersistentAnalysis {

    private CachedDatasetRequest(String httpMethod, boolean needsMasterKey, String datasetName) {
        super(httpMethod, needsMasterKey, datasetName, null);
    }

    static KeenQueryRequest definitionRequest(String datasetName) {
        return new CachedDatasetRequest(HttpMethods.GET, false, datasetName) {

            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                return urlBuilder.getDatasetsUrl(projectId, getResourceName(), false, Collections.<String, Object>emptyMap());
            }
        };
    }

    static KeenQueryRequest resultsRequest(final DatasetDefinition datasetDefinition, final String indexBy, final Timeframe timeframe) {
        return new CachedDatasetRequest(HttpMethods.GET, false, datasetDefinition.getDatasetName()) {
            private final ObjectMapper objectMapper = new ObjectMapper()
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                try {
                    return urlBuilder.getDatasetsUrl(projectId, datasetDefinition.getDatasetName(), true, queryArgs());
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
                return getGroupByParams() != null && !getGroupByParams().isEmpty();
            }

            @Override
            Collection<String> getGroupByParams() {
                return datasetDefinition.getQuery().getGroupBy();
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

    static KeenQueryRequest definitionsByProjectRequest(final Integer limit, final String afterName) {
        return new CachedDatasetRequest(HttpMethods.GET, false, null) {

            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                Map<String, Object> queryParams = new HashMap<String, Object>();
                if (limit != null) {
                    queryParams.put(KeenQueryConstants.LIMIT, limit);
                }
                if (afterName != null) {
                    queryParams.put(KeenQueryConstants.AFTER_NAME, afterName);
                }
                return urlBuilder.getDatasetsUrl(projectId, getResourceName(), false, queryParams);
            }
        };
    }

    static KeenQueryRequest deletionRequest(String datasetName) {
        return new CachedDatasetRequest(HttpMethods.DELETE, true, datasetName) {

            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                return urlBuilder.getDatasetsUrl(projectId, getResourceName(), false, Collections.<String, Object>emptyMap());
            }
        };
    }

    static KeenQueryRequest creationRequest(final String datasetName, final String displayName, final DatasetQuery query, final Set<String> indexBy) {
        return new CachedDatasetRequest(HttpMethods.PUT, true, datasetName) {

            @Override
            URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException {
                return urlBuilder.getDatasetsUrl(projectId, getResourceName(), false, Collections.<String, Object>emptyMap());
            }

            @Override
            Map<String, Object> constructRequestArgs() {
                Map<String, Object> requestArgs = new HashMap<String, Object>();
                requestArgs.put(KeenQueryConstants.DISPLAY_NAME, displayName);
                requestArgs.put(KeenQueryConstants.INDEX_BY, indexBy);
                requestArgs.put(KeenQueryConstants.QUERY, query.asMap());
                return requestArgs;
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

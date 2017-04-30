package io.keen.client.java;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import io.keen.client.java.http.HttpMethods;

/**
 * Represents PUT requests performed against the Saved/Cached Queries API.
 *
 * @author masojus
 */
class SavedQueryPut extends SavedQueryRequest {
    private final KeenQueryRequest query;
    private final int refreshRate;

    private final Map<String, Object> miscProperties;


    SavedQueryPut(String queryName,
                  String displayName,
                  KeenQueryRequest query,
                  int refreshRate,
                  Map<String, ?> miscProperties) {
        super(HttpMethods.PUT, true /* needsMasterKey */, queryName, displayName);

        if (null == query && (null == miscProperties || miscProperties.isEmpty())) {
            throw new IllegalArgumentException("If a query definition is not provided, then " +
                                               "miscProperties should define the Saved/Cached " +
                                               "Query.");
        }

        this.query = query;

        // Validate refreshRate range. We can use -1 to indicate it is unset since negative values
        // aren't valid.
        if (0 < refreshRate) {
            RefreshRate.validateRefreshRate(refreshRate);
        }

        this.refreshRate = refreshRate;
        this.miscProperties = (null == miscProperties ?
                new HashMap<String, Object>() : new HashMap<String, Object>(miscProperties));
    }

    // For updates, mostly, or special super advanced usage.
    SavedQueryPut(String queryName, Map<String, ?> miscProperties) {
        this(queryName, null, null, -1, miscProperties);
    }

    @Override
    Map<String, Object> constructRequestArgs() {
        Map<String, Object> requestArgs = super.constructRequestArgs();

        if (null == requestArgs) {
            throw new IllegalStateException("Request args should not be null.");
        }

        // If there isn't a KeenQueryRequest then hopefully this is an update and the entire
        // new query definition had better be in miscProperties.
        if (null != query) {
            //Make sure there isn't already a "query" key
            SavedQueryPut.expectNotContainsKey(requestArgs, KeenQueryConstants.QUERY);
            Map<String, Object> queryArgs = query.constructRequestArgs();

            // TODO : Does the analysis_type go here, like the docs say?
            queryArgs.put(KeenQueryConstants.ANALYSIS_TYPE, query.getAnalysisType());

            requestArgs.put(KeenQueryConstants.QUERY, queryArgs);

            // TODO : ... or does it go here? Seems like it works both ways :S
            //requestArgs.put(KeenQueryConstants.ANALYSIS_TYPE, query.getAnalysisType());
        }

        if (0 <= refreshRate) {
            // Make sure there isn't already a "refresh_rate" key first
            SavedQueryPut.expectNotContainsKey(requestArgs, KeenQueryConstants.REFRESH_RATE);
            requestArgs.put(KeenQueryConstants.REFRESH_RATE, refreshRate);
        }

        String displayName = getDisplayName();

        // NOTE : For now, metadata and therefore display_name is undocumented for Saved Queries.
        if (null != displayName) {
            // Make sure there isn't already a "metadata" key
            SavedQueryPut.expectNotContainsKey(requestArgs, KeenQueryConstants.METADATA);
            Map<String, Object> metadata = new HashMap<String, Object>();
            metadata.put(KeenQueryConstants.DISPLAY_NAME, displayName);

            requestArgs.put(KeenQueryConstants.METADATA, metadata);
        }

        // Make sure miscProperties wouldn't overwrite any of the other properties.
        for (String key : miscProperties.keySet()) {
            SavedQueryPut.expectNotContainsKey(requestArgs, key);
        }

        requestArgs.putAll(miscProperties);

        return requestArgs;
    }

    private static void expectNotContainsKey(Map<?, ?> map, Object key) {
        if (map.containsKey(key)) {
            throw new IllegalStateException(String.format(Locale.US,
                                                          "The key '%s' already exists.",
                                                          key));
        }
    }
}

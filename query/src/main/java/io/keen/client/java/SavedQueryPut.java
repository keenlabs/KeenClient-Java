package io.keen.client.java;

import java.util.HashMap;
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

    private Map<String, Object> miscProperties; // TODO : final, or will we create/set on demand?

    SavedQueryPut(String queryName,
                  String displayName,
                  KeenQueryRequest query,
                  int refreshRate,
                  Map<String, ?> miscProperties) {
        super(HttpMethods.PUT, true /* needsMasterKey */, queryName, displayName);

        // TODO : Validate somehow?? For PUT it should generally be non-null, but maybe not if we
        // end up using miscProperties to send a full update.
        this.query = query;

        // TODO : Validate refreshRate range. Empirically [14400, 86400] seconds, inclusive at
        // both boundaries. Recall that the docs on the website are wrong as of 3/14/17, as I
        // reported. Can also be 0 in order to turn off caching. We can use -1 as unset since
        // negative values aren't valid.
        this.refreshRate = refreshRate;
        this.miscProperties = (null == miscProperties ?
                new HashMap<String, Object>() : new HashMap<String, Object>(miscProperties));
    }

    // For updates, mostly, or special super advanced usage.
    SavedQueryPut(String queryName, Map<String, ?> miscProperties) {
        super(HttpMethods.PUT, true /* needsMasterKey */, queryName);

        this.query = null;
        this.refreshRate = -1;
        this.miscProperties = new HashMap<String, Object>(miscProperties); // TODO : can't be null?
    }

    @Override
    Map<String, Object> constructRequestArgs() {
        Map<String, Object> requestArgs = super.constructRequestArgs();

        // TODO : We expect requestArgs to be non-null. Validate??

        // If there isn't a KeenQueryRequest then hopefully this is an update and the entire
        // new query definition had better be in miscProperties.
        if (null != query) {
            // TODO : Make sure there isn't already a "query" key
            Map<String, Object> queryArgs = query.constructRequestArgs();

            // TODO : Does the analysis_type go here, like the docs say?
            queryArgs.put(KeenQueryConstants.ANALYSIS_TYPE, query.getAnalysisType());

            requestArgs.put(KeenQueryConstants.QUERY, queryArgs);

            // TODO : ... or does it go here? Seems like it works both ways :S
            //requestArgs.put(KeenQueryConstants.ANALYSIS_TYPE, query.getAnalysisType());
        }

        if (0 <= refreshRate) {
            // TODO : Make sure there isn't already a "refresh_rate" key first
            requestArgs.put(KeenQueryConstants.REFRESH_RATE, refreshRate);
        }

        String displayName = getDisplayName();

        // NOTE : For now, metadata and therefore display_name is undocumented for Saved Queries.
        if (null != displayName) {
            // TODO : Make sure metadata and metadata.display_name aren't already there
            Map<String, Object> metadata = new HashMap<String, Object>();
            metadata.put(KeenQueryConstants.DISPLAY_NAME, displayName);

            requestArgs.put(KeenQueryConstants.METADATA, metadata);
        }

        // TODO : Make sure this won't overwrite any keys? Or just let it?
        requestArgs.putAll(miscProperties);

        return requestArgs;
    }
}

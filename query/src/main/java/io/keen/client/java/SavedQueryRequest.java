package io.keen.client.java;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import io.keen.client.java.exceptions.KeenQueryClientException;

/**
 * A PersistentAnalysis specifically for Saved/Cached Query requests. A lot of the CRUD
 * functionality can be performed with this class, but subclasses can customize behavior more.
 *
 * @author masojus
 */
class SavedQueryRequest extends PersistentAnalysis {
    SavedQueryRequest(String httpMethod, boolean needsMasterKey, String queryName) {
        this(httpMethod, needsMasterKey, queryName, null);
    }

    SavedQueryRequest(String httpMethod,
                      boolean needsMasterKey,
                      String queryName,
                      String displayName) {
        super(httpMethod, needsMasterKey, queryName, displayName);
    }

    @Override
    URL getRequestURL(RequestUrlBuilder urlBuilder,
                      String projectId) throws KeenQueryClientException {
        // Generate some substring of the following depending on whether there's a specific query
        // resource we're operating on, and whether or not we're retrieving a result:
        // https://api.keen.io/3.0/projects/PROJECT_ID/queries/saved/QUERY_NAME/result

        String name = getResourceName();
        String subPath = getAnalysisType() + (null != name ? "/" + name : "");

        if (retrievingResults()) {
            subPath += "/result";
        }

        URL url = urlBuilder.getAnalysisUrl(projectId, subPath);

        return url;
    }

    @Override
    String getAnalysisType() {
        return KeenQueryConstants.SAVED;
    }

    @Override
    Map<String, Object> constructRequestArgs() {
        // By default, if not creating or updating a Saved Query, we need not generate payload.
        return new HashMap<String, Object>();
    }
}

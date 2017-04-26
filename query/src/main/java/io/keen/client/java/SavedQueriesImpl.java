package io.keen.client.java;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpMethods;
import io.keen.client.java.result.QueryResult;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * Implements SavedQueries interface and provides access to the Keen Saved/Cached Query API. Get
 * an instance of this from a KeenQueryClient instance.
 *
 * @author masojus
 */
final class SavedQueriesImpl implements SavedQueries {
    private static final int NO_CACHING_REFRESH_RATE = 0;

    private final KeenQueryClient queryClient;

    SavedQueriesImpl(KeenQueryClient queryClient) {
        this.queryClient = queryClient;
    }

    @Override
    public Map<String, Object> createSavedQuery(String queryName, KeenQueryRequest query)
            throws IOException {
        PersistentAnalysis newSavedQueryRequest =
                new SavedQueryPut(queryName,
                                  null /* displayName */,
                                  query,
                                  SavedQueriesImpl.NO_CACHING_REFRESH_RATE,
                                  null /* miscProperties */);


        return queryClient.getMapResponse(newSavedQueryRequest);
    }

    @Override
    public Map<String, Object> createSavedQuery(String queryName,
                                                KeenQueryRequest query,
                                                String displayName)
            throws IOException {
        PersistentAnalysis newSavedQueryRequest =
                new SavedQueryPut(queryName,
                                  displayName,
                                  query,
                                  SavedQueriesImpl.NO_CACHING_REFRESH_RATE,
                                  null /* miscProperties */);


        return queryClient.getMapResponse(newSavedQueryRequest);
    }

    @Override
    public Map<String, Object> createCachedQuery(String queryName,
                                                  KeenQueryRequest query,
                                                  int refreshRate) throws IOException {
        PersistentAnalysis newCachedQueryRequest =
                new SavedQueryPut(queryName,
                                  null /* displayName */,
                                  query,
                                  refreshRate,
                                  null /* miscProperties */);

        return queryClient.getMapResponse(newCachedQueryRequest);
    }

    @Override
    public Map<String, Object> createCachedQuery(String queryName,
                                                 KeenQueryRequest query,
                                                 String displayName,
                                                 int refreshRate) throws IOException {
        PersistentAnalysis newCachedQueryRequest =
                new SavedQueryPut(queryName,
                                  displayName,
                                  query,
                                  refreshRate,
                                  null /* miscProperties */);

        return queryClient.getMapResponse(newCachedQueryRequest);
    }

    @Override
    public Map<String, Object> getDefinition(String queryName) throws IOException {
        PersistentAnalysis getDefRequest = new SavedQueryRequest(HttpMethods.GET,
                                                                 true /* needsMasterKey */,
                                                                 queryName);

        // When retrieving a single query definition, we expect a single JSON Object and just
        // hand it back instead of wrapping in QueryResult.
        return queryClient.getMapResponse(getDefRequest);
    }

    @Override
    public List<Map<String, Object>> getAllDefinitions() throws IOException {
        PersistentAnalysis getAllDefsRequest = new SavedQueryRequest(HttpMethods.GET,
                                                                     true /* needsMasterKey */,
                                                                     null /* queryName */);
        List<Object> response = queryClient.getListResponse(getAllDefsRequest);

        // We expect a structure such that each entry in the list was a JSON Object representing a
        // query definition, and no entry should be a JSON Value.
        for (Object defObj : response) {
            if (!(defObj instanceof LinkedHashMap)) {
                throw new ServerException("Expected list of definitions to be JSON Array of JSON " +
                                          "Objects, but encountered this: " + defObj.toString());
            }
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> responseMaps = (List)response;

        return responseMaps;
    }

    @Override
    public QueryResult getResult(String queryName) throws IOException {
        PersistentAnalysis getResultRequest =
                new SavedQueryRequest(HttpMethods.GET,
                                      false /* needsMasterKey */,
                                      queryName) {
                    @Override
                    boolean retrievingResults() {
                        return true;
                    }
                };

        Map<String, Object> response = queryClient.getMapResponse(getResultRequest);

        if (!response.containsKey(KeenQueryConstants.QUERY)) {
            throw new ServerException(String.format(Locale.US,
                                                    "The '%s' property is missing.",
                                                    KeenQueryConstants.QUERY));
        }

        Object queryObj = response.get(KeenQueryConstants.QUERY);

        if (!(queryObj instanceof Map)) {
            throw new ServerException(String.format(Locale.US,
                                                    "Null or malformed '%s' property found.",
                                                    KeenQueryConstants.QUERY));
        }

        Map<String, Object> query = (Map)queryObj;

        // Figure out if this query was originally grouped, and grab the group by params if so.
        boolean isGroupBy = false;
        List<?> groupByParamsRaw = null;

        if (query.containsKey(KeenQueryConstants.GROUP_BY)) {
            Object groupByParamsObj = query.get(KeenQueryConstants.GROUP_BY);

            if (groupByParamsObj instanceof List) {
                groupByParamsRaw = (List)groupByParamsObj;
                isGroupBy = !groupByParamsRaw.isEmpty();
            } else if (null != groupByParamsObj) {
                throw new ServerException(String.format(Locale.US,
                                                        "Property '%s' was not a List type.",
                                                        KeenQueryConstants.GROUP_BY));
            }
        }

        Collection<String> groupByParams = isGroupBy ?
                new LinkedList<String>() : Collections.<String>emptyList();

        if (isGroupBy) {
            for (Object param : groupByParamsRaw) {
                groupByParams.add(param.toString()); // In theory it's really a string anyway.
            }

            if (groupByParams.isEmpty()) {
                throw new ServerException("Query is supposedly grouped, but has no group by " +
                                          "parameters.");
            }
        }

        // Figure out if this query originally specified an interval param.
        boolean isInterval = false;

        if (query.containsKey(KeenQueryConstants.INTERVAL)) {
            Object intervalObj = query.get(KeenQueryConstants.INTERVAL);

            isInterval = (intervalObj instanceof String);
        }

        if (!query.containsKey(KeenQueryConstants.ANALYSIS_TYPE)) {
            throw new ServerException(String.format(Locale.US,
                                                    "The '%s' property is missing.",
                                                    KeenQueryConstants.ANALYSIS_TYPE));
        }

        // This should be a String anyway.
        String analysisType = query.get(KeenQueryConstants.ANALYSIS_TYPE).toString().trim();

        // Figure out if this was originally a Multi-Analysis.
        boolean isMultiAnalysis = KeenQueryConstants.MULTI_ANALYSIS.equals(analysisType);

        // Figure out if this was originally a Funnel analysis.
        boolean isFunnel = KeenQueryConstants.FUNNEL.equals(analysisType);

        // Now get the actual result and massage it into the right shape.
        if (!response.containsKey(KeenQueryConstants.RESULT)) {
            throw new ServerException(String.format(Locale.US,
                                                    "The '%s' property is missing.",
                                                    KeenQueryConstants.RESULT));
        }

        Object resultObj = response.get(KeenQueryConstants.RESULT);
        Map<String, Object> result;

        // TODO : Is (isFunnel == (!isMultiAnalysis && resultObj instanceof Map)) or are there other
        // cases when the result is a Map?
        if (!isMultiAnalysis && resultObj instanceof Map) {
            result = (Map)resultObj;
        } else {
            // For scalar/array/multi-analysis results, we usually receive a container object with a
            // single "result" key but here we receive the result key in the Saved/Cached Query
            // definition. So, wrap it in a container object before passing down to the result
            // parsing code.
            result = new HashMap<String, Object>();
            result.put(KeenQueryConstants.RESULT, resultObj);

            // TODO : Make sure this is the right thing for ListResults and Multi-Analysis, if it
            // turns out we can even have Save/Cached Queries that are Multi-Analysis type.
        }

        return queryClient.rawMapResponseToQueryResult(result,
                                                       isGroupBy,
                                                       isInterval,
                                                       isMultiAnalysis,
                                                       isFunnel,
                                                       groupByParams);
    }

    @Override
    public Map<String, Object> updateQuery(String queryName,
                                           Map<String, ?> updates) throws IOException {
        // First get the existing query definition, which hits the network.
        Map<String, Object> oldFullDef = getDefinition(queryName);

        // Create a new query def to send back. We cannot send values for some attributes like
        // "urls", "last_modified_date", "run_information", etc.
        Map<String, Object> newFullDef = new HashMap<String, Object>();

        // Copy over the "query_name" and "refresh_rate"
        newFullDef.put(KeenQueryConstants.QUERY_NAME,
                       oldFullDef.get(KeenQueryConstants.QUERY_NAME));

        newFullDef.put(KeenQueryConstants.REFRESH_RATE,
                       oldFullDef.get(KeenQueryConstants.REFRESH_RATE));

        // If metadata was set, preserve it. The Explorer UI currently stores information here.
        if (oldFullDef.containsKey(KeenQueryConstants.METADATA)) {
            newFullDef.put(KeenQueryConstants.METADATA,
                           oldFullDef.get(KeenQueryConstants.METADATA));
        }

        if (!oldFullDef.containsKey(KeenQueryConstants.QUERY)) {
            throw new ServerException(String.format(Locale.US,
                                                    "The '%s' property is missing.",
                                                    KeenQueryConstants.QUERY));
        }

        Object oldQueryObj = oldFullDef.get(KeenQueryConstants.QUERY);

        if (!(oldQueryObj instanceof Map)) {
            throw new ServerException(String.format(Locale.US,
                                                    "Null or malformed '%s' property.",
                                                    KeenQueryConstants.QUERY));
        }

        Map<String, Object> oldQuery = (Map)oldQueryObj;
        Map<String, Object> newQuery = new HashMap<String, Object>();

        // Preserve any non-empty properties of the existing query. We get back values like null and
        // []/{} for 'group_by', 'interval' or 'timezone', etc., but those aren't accepted values
        // when updating.
        for (Map.Entry<String, Object> property : oldQuery.entrySet()) {
            Object propertyValue = property.getValue();

            // Check for null, empty String, empty Map or empty List
            if (null != propertyValue &&
               ((propertyValue instanceof String) && !((String)propertyValue).trim().isEmpty()) ||
               ((propertyValue instanceof Map) && !((Map)propertyValue).isEmpty()) ||
               ((propertyValue instanceof List) && !((List)propertyValue).isEmpty())) {
                // Otherwise keep it.
                // Shallow copy since we want the entire object hierarchy to start with.
                newQuery.put(property.getKey(), propertyValue);
            }
        }

        newFullDef.put(KeenQueryConstants.QUERY, newQuery);

        // Now, recursively overwrite any attributes passed in.
        SavedQueriesImpl.deepUpdate(newFullDef, updates);

        // Lastly, push back the new full definition minus disallowed properties.
        PersistentAnalysis updatedSavedQueryRequest = new SavedQueryPut(queryName, newFullDef);

        return queryClient.getMapResponse(updatedSavedQueryRequest);
    }

    private static void deepUpdate(Map<String, Object> map, Map<String, ?> updates) {
        for (Map.Entry<String, ?> entry : updates.entrySet()) {
            String updateKey = entry.getKey();
            Object updateValue = entry.getValue();

            if (updateValue instanceof Map) {
                // If map doesn't have this key, create a sub-map for it
                if (!map.containsKey(updateKey)) {
                    map.put(updateKey, new HashMap<String, Object>());
                }

                Object existingValue = map.get(updateKey);

                if (existingValue instanceof Map) {
                    SavedQueriesImpl.deepUpdate((Map) existingValue,
                                                (Map) updateValue);
                    map.put(updateKey, existingValue);
                } else {
                    map.put(updateKey, updateValue);
                }
            } else {
                map.put(updateKey, updateValue);
            }
        }
    }

    // TODO : Add to SavedQueries interface? Is this function useful in this SDK?
    public Map<String, Object> updateQueryFull(String queryName,
                                               Map<String, Object> fullDefinition)
            throws IOException {
        throw new NotImplementedException(); // TODO : Do it?
    }

    @Override
    public Map<String, Object> deleteQuery(String queryName) throws IOException {
        PersistentAnalysis deleteQueryRequest = new SavedQueryRequest(HttpMethods.DELETE,
                                                                      true /* needsMasterKey */,
                                                                      queryName);

        return queryClient.getMapResponse(deleteQueryRequest);
    }
}

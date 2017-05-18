package io.keen.client.java;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.keen.client.java.result.QueryResult;

/**
 * Represents the set of operations that can be performed against the
 * <a href="https://keen.io/docs/api/#saved-queries">Saved/Cached Query API</a> endpoints.
 *
 * @author masojus
 */
public interface SavedQueries {
    /**
     * Save the given query.
     *
     * @param queryName The resource name for the query. Alphanumerics, hyphens, and underscores.
     * @param query The query definition.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error creating the Saved Query.
     */
    Map<String, Object> createSavedQuery(String queryName,
                                         KeenQueryRequest query) throws IOException;

    /**
     * Save the given query.
     *
     * @param queryName The resource name for the query. Alphanumerics, hyphens, and underscores.
     * @param query The query definition.
     * @param displayName The display name to be used for this resource.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error creating the Saved Query.
     */
    Map<String, Object> createSavedQuery(String queryName,
                                         KeenQueryRequest query,
                                         String displayName) throws IOException;

    /**
     * Save the given query as a cached query.
     *
     * @param queryName The resource name for the query. Alphanumerics, hyphens, and underscores.
     * @param query The query definition.
     * @param refreshRate The refresh rate for this cached query, empirically in the range
     *                    [14400, 86400] seconds or 4-24 hrs.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error creating the Cached Query.
     */
    Map<String, Object> createCachedQuery(String queryName,
                                          KeenQueryRequest query,
                                          int refreshRate) throws IOException;

    /**
     * Save the given query as a cached query.
     *
     * @param queryName The resource name for the query. Alphanumerics, hyphens, and underscores.
     * @param query The query definition.
     * @param displayName The display name to be used for this resource.
     * @param refreshRate The refresh rate for this cached query, empirically in the range
     *                    [14400, 86400] seconds or 4-24 hrs.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error creating the Cached Query.
     */
    Map<String, Object> createCachedQuery(String queryName,
                                          KeenQueryRequest query,
                                          String displayName,
                                          int refreshRate) throws IOException;

    /**
     * Get a single Saved/Cached query definition.
     *
     * @param queryName The resource name for the query.
     *
     * @return The definition for the given query resource.
     * @throws IOException If there is an error getting the Saved/Cached Query definition.
     */
    Map<String, Object> getDefinition(String queryName) throws IOException;

    /**
     * Get all Saved/Cached Query definitions.
     *
     * @return All the Saved/Cached query definitions for this project.
     * @throws IOException If there is an error getting the Saved/Cached Query definitions.
     */
    List<Map<String, Object>> getAllDefinitions() throws IOException;

    /**
     * Get the result of the Saved/Cached query with the given resource name.
     *
     * @param queryName The resource name for the query.
     *
     * @return The result of the Saved/Cached query.
     * @throws IOException If there is an error getting the Saved/Cached Query result.
     */
    QueryResult getResult(String queryName) throws IOException;

    /**
     * Given a Map of attributes to be updated, update only those attributes in the Saved Query at
     * the resource given by 'query_name'. This will perform two HTTP requests--one to fetch the
     * query definition, and one to set the new attributes. This method intends to preserve any
     * other properties on the query.
     *
     * @param queryName The resource name for the query to be updated.
     * @param updates The partial updates to be applied to the existing query.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error updating the Saved/Cached Query definition.
     */
    Map<String, Object> updateQuery(String queryName,
                                    Map<String, ?> updates) throws IOException;

    /**
     * Given a complete raw Saved/Cached Query definition in Map form, this method will blindly
     * attempt to PUT it. This is useful if one has already called getDefinition(query) and needs
     * to process that, alter it, and PUT it back, for example.
     *
     * @param queryName The resource name for the query to be updated.
     * @param fullDefinition A Map representing the complete definition of this Saved/Cached Query.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error updating the Saved/Cached Query definition.
     */
    Map<String, Object> updateQueryFull(String queryName,
                                        Map<String, Object> fullDefinition) throws IOException;

    /**
     * Delete the query stored at the given resource name. Upon success the server returns
     * HTTP 204 - No Content, so this method returns void and throws upon error.
     *
     * @param queryName The resource name for the query to be deleted.
     * @throws IOException If there is an error deleting the Saved/Cached Query.
     */
    void deleteQuery(String queryName) throws IOException;


    /**
     * Change the query's resource name to the given query name.
     *
     * @param queryName The resource name for the query to be renamed.
     * @param newQueryName The new resource name. Alphanumerics, hyphens, and underscores.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error updating the Saved/Cached Query definition.
     */
    Map<String, Object> setQueryName(String queryName, String newQueryName) throws IOException;

    /**
     * Perform an update to change the refresh rate to the desired value. Take into account that
     * a refresh rate of 0 means no caching, but the refresh rate for a Cached Query is in the range
     * [14400, 86400] seconds or 4-24 hrs.
     *
     * @param queryName The resource name for the query to be updated.
     * @param refreshRate The refresh rate for this cached query.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error updating the Saved/Cached Query definition.
     */
    Map<String, Object> setRefreshRate(String queryName, int refreshRate) throws IOException;

    /**
     * Sets the display_name field of the metadata object, which is currently undocumented and
     * therefore subject to change. This field governs how the Explorer UI shows the name of the
     * query.
     *
     * @param queryName The resource name for the query to be updated.
     * @param displayName The display name to be used for this resource.
     *
     * @return The raw return value of the PUT request as a Map containing the complete new
     *         Saved/Cached Query definition with auditing like created/updated/run information.
     * @throws IOException If there is an error updating the Saved/Cached Query definition.
     */
    Map<String, Object> setDisplayName(String queryName, String displayName) throws IOException;
}

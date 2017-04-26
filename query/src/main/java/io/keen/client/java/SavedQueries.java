package io.keen.client.java;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.keen.client.java.result.QueryResult;

/**
 * Represents the set of operations that can be performed against the
 * <a href="https://keen.io/docs/api/#saved-queries">Saved/Cached Query API</a> endpoints.
 *
 * // TODO : Fill in comments here.
 *
 * @author masojus
 */
public interface SavedQueries {
    Map<String, Object> createSavedQuery(String queryName,
                                         KeenQueryRequest query) throws IOException;

    Map<String, Object> createSavedQuery(String queryName,
                                         KeenQueryRequest query,
                                         String displayName) throws IOException;

    Map<String, Object> createCachedQuery(String queryName,
                                          KeenQueryRequest query,
                                          int refreshRate) throws IOException;

    Map<String, Object> createCachedQuery(String queryName,
                                          KeenQueryRequest query,
                                          String displayName,
                                          int refreshRate) throws IOException;

    // TODO : We could add a method that lets client code pass in miscProperties. Might that turn
    // out to be useful?

    Map<String, Object> getDefinition(String queryName) throws IOException;

    List<Map<String, Object>> getAllDefinitions() throws IOException;

    QueryResult getResult(String queryName) throws IOException;

    Map<String, Object> updateQuery(String queryName,
                                    Map<String, ?> updates) throws IOException;

    // TODO : Can/should we return anything useful if success? We'll throw on error I imagine, and
    // success means a 204 - No Content, so maybe this is really void?
    Map<String, Object> deleteQuery(String queryName) throws IOException;

    // TODO : Given how Java has no nice Map literal notation, we could add helpers for simple
    // common tasks like toggling "Cached" state to OFF, or setting the refresh rate, or setting
    // the display name, etc.
}

package io.keen.client.java;

import io.keen.client.java.result.IntervalResultValue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Client interface for Cached Datasets.
 * <p>
 * Cached Datasets are a way to pre-compute data for hundreds or thousands of entities at once. They are a great way to
 * improve your query efficiency as well as minimize your compute costs.
 *
 * @see <a href="https://keen.io/docs/compute/cached-datasets/">Cached Datasets</a>
 * @see <a href="https://keen.io/docs/api/#cached-datasets/">Cached Dataset API Reference</a>
 */
public interface CachedDatasets {

    /**
     * Creates a Cached Dataset. Updates are not currently supported.
     *
     * @param datasetName The unique name for your new DS. It can only contain alphanumeric characters, hypens and underscores.
     * @param displayName The human-readable string name for your Cached Dataset
     * @param query       The query definition you want Keen to optimize for your application.
     * @param indexBy     The event property names containing an identifier, such as user_id or store.id, that will be used to index and retrieve query results.
     * @return The definition of created Dataset.
     * @throws IOException If there was an error communicating with the server.
     */
    DatasetDefinition create(String datasetName, String displayName, DatasetQuery query, Collection<String> indexBy) throws IOException;

    /**
     * Gets a Cached Dataset definition.
     *
     * @param datasetName The name of requested Dataset.
     * @return The definition of specified Dataset.
     * @throws IOException If there was an error communicating with the server.
     */
    DatasetDefinition getDefinition(String datasetName) throws IOException;

    /**
     * Gets query results from a Cached Dataset.
     *
     * @param datasetDefinition A definition of Cached Dataset. Required as a definition determines the response format.
     * @param indexByValues     A map of [index identifier -> index value] for all index properties defined in a Dataset definition.
     * @param timeframe         Limits retrieval of results to a specific portion of the Cached Dataset.
     * @return Query results from a Cached Dataset.
     * @throws IOException If there was an error communicating with the server.
     * @see CachedDatasets#getDefinition(String)
     */
    List<IntervalResultValue> getResults(DatasetDefinition datasetDefinition, Map<String, ?> indexByValues, Timeframe timeframe) throws IOException;

    /**
     * Gets list of Cached Dataset definitions for a project.
     *
     * @return A list of Cached Dataset definition.
     * @throws IOException If there was an error communicating with the server.
     * @see CachedDatasets#getDefinitionsByProject(Integer, String)
     */
    List<DatasetDefinition> getDefinitionsByProject() throws IOException;

    /**
     * Gets list of Cached Dataset definitions for a project.
     *
     * @param limit     How many Cached Dataset definitions to return at a time (1-100). Defaults to 10.
     * @param afterName A cursor for use in pagination. after_name is the Cached Dataset name that defines your place in the list.
     * @return A list of Cached Dataset definition.
     * @throws IOException If there was an error communicating with the server.
     * @see CachedDatasets#getDefinitionsByProject()
     */
    List<DatasetDefinition> getDefinitionsByProject(Integer limit, String afterName) throws IOException;

    /**
     * Deletes a Cached Dataset.
     *
     * @param datasetName The name of Dataset to be deleted.
     * @return Whether operation was executed successfully or not.
     * @throws IOException If there was an error communicating with the server.
     */
    boolean delete(String datasetName) throws IOException;
}

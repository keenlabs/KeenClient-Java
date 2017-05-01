package io.keen.client.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.keen.client.java.result.MultiAnalysisResult;
import io.keen.client.java.result.QueryResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test the Persistent Analysis query functionality, which should include Saved/Cached Queries and
 * Cached Datasets.
 *
 * @author masojus
 */
public class PersistentAnalysisTest extends KeenQueryTestBase {
    private static final String TEST_RESOURCE_NAME = "some_name";

    private SavedQueries savedQueryApi = null;

    @Before
    public void persistentAnalysisTestSetup() {
        savedQueryApi = queryClient.getSavedQueriesInterface();
    }

    @After
    public void persistentAnalysisTestTeardown() {
        savedQueryApi = null;
    }

    // Make sure result, if provided, is wrapped in root object/array brackets, if not scalar.
    private String getFakeSavedQueryDefinition(String result) {
        // A few of the keys one would get back. We don't really check the return values in these
        // tests because we aren't parsing them for the most part, except the query result.
        String definition = "{" +
                    "\"query_name\": \"" + TEST_RESOURCE_NAME + "\"," +
                    "\"refresh_rate\": 0," +
                    "\"query\": {" +
                        "\"analysis_type\": \"sum\"," +
                        "\"target_property\": \"someProp\"," +
                        "\"timeframe\": \"this_month\"" +
                    "}" + (null == result ? "" : ", " +
                    "\"result\": " + result) +
                "}";

        return definition;
    }

    private static void validatePersistentAnalysisRequiredFields(ObjectNode requestNode) {
        // Should have 'refresh_rate' and 'query' top-level keys, at least.
        assertTrue("Missing required top-level fields.", 2 <= requestNode.size());

        assertTrue(requestNode.hasNonNull(KeenQueryConstants.REFRESH_RATE));
        RefreshRate.validateRefreshRate(requestNode.get(KeenQueryConstants.REFRESH_RATE).asInt());
        assertTrue(requestNode.hasNonNull(KeenQueryConstants.QUERY));

        // The "query" should have the typical stuff (tested elsewhere) but also the "analysis_type"
        JsonNode queryNode = requestNode.get(KeenQueryConstants.QUERY);
        assertTrue(queryNode.hasNonNull(KeenQueryConstants.ANALYSIS_TYPE));
    }

    private ObjectNode getPersistentAnalysisRequestNode()
            throws IOException {
        // The assumption here is that we've already caused the HttpHandler to be invoked, so the
        // Request instance was already created and captured.
        ObjectNode requestNode = stringToRequestNode(executeCapturedRequest());

        if (null != requestNode) {
            validatePersistentAnalysisRequiredFields(requestNode);
        }

        return requestNode;
    }

    private KeenQueryRequest getSimpleRequest() {
        SingleAnalysis sum = new SingleAnalysis.Builder(QueryType.SUM)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTargetPropertyName(TEST_TARGET_PROPERTY)
                .withTimeframe(TEST_RELATIVE_TIMEFRAME)
                .build();

        return sum;
    }

    private ObjectNode verifyCreated(Map<String, Object> createdQueryResponse, String displayName)
            throws IOException {
        assertNotNull(createdQueryResponse);
        assertTrue(!createdQueryResponse.isEmpty());
        assertEquals(TEST_RESOURCE_NAME, createdQueryResponse.get(KeenQueryConstants.QUERY_NAME));

        ObjectNode requestNode = getPersistentAnalysisRequestNode();
        assertEquals((null != displayName ? 3 : 2), requestNode.size());

        if (null != displayName) {
            // Verify we added a "metadata" node.
            assertTrue(requestNode.hasNonNull(KeenQueryConstants.METADATA));
            JsonNode metadataNode = requestNode.get(KeenQueryConstants.METADATA);
            assertTrue(metadataNode.hasNonNull(KeenQueryConstants.DISPLAY_NAME));
            assertEquals(displayName, metadataNode.get(KeenQueryConstants.DISPLAY_NAME).asText());
        }

        return requestNode;
    }

    @Test
    public void testSavedQuery_Create_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        KeenQueryRequest sum = getSimpleRequest();
        Map<String, Object> createdQuery = savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME, sum);

        verifyCreated(createdQuery, null);
    }

    @Test
    public void testSavedQuery_CreateWithDisplayName_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        KeenQueryRequest sum = getSimpleRequest();
        String displayName = "Some Display Name";
        Map<String, Object> createdQuery = savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME,
                                                                          sum,
                                                                          displayName);

        verifyCreated(createdQuery, displayName);
    }

    @Test
    public void testSavedQuery_CreateFunnel_Simple() throws IOException {
        // TODO : FINISH TESTING
        // verifyCreated(createdQuery, null);
    }

    @Test
    public void testSavedQuery_CreateMultiAnalysis_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(TEST_RELATIVE_TIMEFRAME)
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .build();

        Map<String, Object> multiAnalysisQuery =
                savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME, multiAnalysisParams);

        verifyCreated(multiAnalysisQuery, null);
    }

    @Test
    public void testCachedQuery_Create_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        KeenQueryRequest sum = getSimpleRequest();
        int refreshRate = (5 * 3600); // 5 hrs
        Map<String, Object> createdQuery = savedQueryApi.createCachedQuery(TEST_RESOURCE_NAME,
                                                                           sum,
                                                                           refreshRate);

        ObjectNode requestNode = verifyCreated(createdQuery, null);
        assertEquals(refreshRate, requestNode.get(KeenQueryConstants.REFRESH_RATE).asInt());
    }

    @Test
    public void testCachedQuery_CreateWithDisplayName_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        KeenQueryRequest sum = getSimpleRequest();
        int refreshRate = RefreshRate.fromHours(7); // 7 hrs
        String displayName = "Some Display Name";
        Map<String, Object> createdQuery = savedQueryApi.createCachedQuery(TEST_RESOURCE_NAME,
                                                                           sum,
                                                                           displayName,
                                                                           refreshRate);

        ObjectNode requestNode = verifyCreated(createdQuery, displayName);
        assertEquals(refreshRate, requestNode.get(KeenQueryConstants.REFRESH_RATE).asInt());
    }

    @Test
    public void testSavedQuery_GetDefinitions_Simple() throws IOException {
        setMockResponse(200, "[" +
                getFakeSavedQueryDefinition(null) + ", " +
                getFakeSavedQueryDefinition(null) + ", " +
                getFakeSavedQueryDefinition(null) +
            "]"
        );

        // We need to use the JacksonJsonHandler for deserializing JSON Array results
        queryClient = new KeenQueryClient.Builder(TEST_PROJECT)
                .withJsonHandler(new JacksonJsonHandler())
                .withHttpHandler(mockHttpHandler)
                .build();

        savedQueryApi = queryClient.getSavedQueriesInterface();

        List<Map<String, Object>> allDefs = savedQueryApi.getAllDefinitions();

        assertNotNull(allDefs);
        assertTrue(!allDefs.isEmpty());

        assertNull(getPersistentAnalysisRequestNode()); // GET request should have no body
    }

    @Test
    public void testSavedQuery_GetDefinition_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        Map<String, Object> oneDef = savedQueryApi.getDefinition(TEST_RESOURCE_NAME);

        assertNotNull(oneDef);
        assertTrue(!oneDef.isEmpty());

        assertNull(getPersistentAnalysisRequestNode()); // GET request should have no body
    }

    @Test
    public void testSavedQuery_GetResult_Simple() throws IOException {
        // TODO : FINISH TESTING

        QueryResult multiAnalysisResult = savedQueryApi.getResult("multi_FromJavaSDK_1");
        assertTrue(multiAnalysisResult instanceof MultiAnalysisResult);

        QueryResult result = savedQueryApi.getResult("saved-funnel");
        System.out.println(result);

        QueryResult sumResultSaved = savedQueryApi.getResult("sum_FromJavaSDK_3");
        System.out.println(sumResultSaved);

        QueryResult sumResultCached = savedQueryApi.getResult("sum_FromJavaSDK_Cached_1");
        System.out.println(sumResultCached);

        QueryResult selectUniqueResult = savedQueryApi.getResult("sum_FromJavaSDK_2");
        assertTrue(selectUniqueResult.isListResult());

        QueryResult groupByResult = savedQueryApi.getResult("sum_FromJavaSDK");
        assertTrue(groupByResult.isGroupResult());

        QueryResult intervalResult = savedQueryApi.getResult("sum_FromJavaSDK_withDisplayName");
        assertTrue(intervalResult.isIntervalResult());
    }

    // TODO : Add this check for requests that need the query_name included, like update() to rename
    //assertTrue(requestNode.hasNonNull(KeenQueryConstants.QUERY_NAME));

    @Test
    public void testSavedQuery_Update_Simple() throws IOException {
        // TODO : FINISH TESTING

        Map<String, Object> updates = new HashMap<String, Object>();
        updates.put(KeenQueryConstants.REFRESH_RATE, (6 * 3600)); // 6 hrs

        Map<String, Object> updateResponse = savedQueryApi.updateQuery("not-cached", updates);
        assertNotNull(updateResponse);
    }

    @Test
    public void testSavedQuery_Delete_Simple() throws IOException {
        setMockResponse(204, null);
        savedQueryApi.deleteQuery("to-be-deleted");

        // TODO : Add a means to check more about the lower-level HTTP request, like the specific
        // headers, URL, response codes, etc.

        assertNull(getPersistentAnalysisRequestNode()); // GET request should have no body
    }
}

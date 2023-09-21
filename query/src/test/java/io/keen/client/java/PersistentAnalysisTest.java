package io.keen.client.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.keen.client.java.http.Response;
import io.keen.client.java.result.FunnelResult;
import io.keen.client.java.result.MultiAnalysisResult;
import io.keen.client.java.result.QueryResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    private String getFakeSavedQueryDefinition(String result,
                                               boolean isGroupBy,
                                               boolean isInterval,
                                               boolean isMultiAnalysis,
                                               boolean isFunnel) {
        // A few of the keys one would get back. We don't really check the return values in most of
        // these tests because we aren't parsing them for the most part, except the query result.
        String analysisType;

        if (isMultiAnalysis) {
            analysisType = "multi_analysis";
        } else if (isFunnel) {
            analysisType = "funnel";
        } else {
            analysisType = "sum";
        }

        String definition = "{" +
                    "\"query_name\": \"" + TEST_RESOURCE_NAME + "\"," +
                    "\"refresh_rate\": 0," +
                    "\"query\": {" +
                        "\"analysis_type\": \"" + analysisType + "\"," +
                        "\"target_property\": \"someProp\"," +
                        "\"timeframe\": \"this_month\"" + (!isGroupBy ? "" : ", " +
                        "\"group_by\": [\"category\"]") + (!isInterval ? "" : ", " +
                        "\"interval\": \"daily\"") +
                    "}" + (null == result ? "" : ", " +
                    "\"result\": " + result) +
                "}";

        return definition;
    }

    private String getFakeSavedQueryDefinition(String result) {
        return getFakeSavedQueryDefinition(result, false, false, false, false);
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

    private ObjectNode verifyPut(Map<String, Object> putResponse, String displayName, int numExtraFields)
            throws IOException {
        assertNotNull(putResponse);
        assertTrue(!putResponse.isEmpty());
        // Even when we change the "query_name" the canned responses return the original name. But
        // we're not testing that the back end actually changes things, but rather that we parse
        // correctly.
        assertEquals(TEST_RESOURCE_NAME, putResponse.get(KeenQueryConstants.QUERY_NAME));

        ObjectNode requestNode = getPersistentAnalysisRequestNode();
        // Normally "refresh_rate" and "query" are in the request, but if "display_name" or updates
        // like "query_name" are in the PUT, there'll be more top-level nodes.
        assertEquals((null != displayName ? 3 : 2) + numExtraFields, requestNode.size());

        if (null != displayName) {
            // Verify we added a "metadata" node.
            assertTrue(requestNode.hasNonNull(KeenQueryConstants.METADATA));
            JsonNode metadataNode = requestNode.get(KeenQueryConstants.METADATA);
            assertTrue(metadataNode.hasNonNull(KeenQueryConstants.DISPLAY_NAME));
            assertEquals(displayName, metadataNode.get(KeenQueryConstants.DISPLAY_NAME).asText());
        }

        return requestNode;
    }

    private ObjectNode verifyPut(Map<String, Object> putResponse, String displayName)
            throws IOException {
        return verifyPut(putResponse, displayName, 0);
    }

    private ObjectNode verifyPut(Map<String, Object> putResponse) throws IOException {
        return verifyPut(putResponse, null, 0);
    }

    @Test
    public void testSavedQuery_Create_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        KeenQueryRequest sum = getSimpleRequest();
        Map<String, Object> createdQuery = savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME, sum);

        verifyPut(createdQuery);
    }

    @Test
    public void testSavedQuery_Create_OkNames() throws IOException {
        Map<String, Object> emptyResponse = new HashMap<String, Object>();

        KeenQueryClient mockQueryClient = mock(KeenQueryClient.class);
        when(mockQueryClient.getMapResponse(any(KeenQueryRequest.class))).thenReturn(emptyResponse);
        savedQueryApi = new SavedQueriesImpl(mockQueryClient);

        KeenQueryRequest sum = getSimpleRequest();

        // Make sure names with alphanumerics, dashes and hyphens are OK
        savedQueryApi.createSavedQuery("stuffAndThings", sum);
        savedQueryApi.createSavedQuery("stuffAndThings123", sum);
        savedQueryApi.createSavedQuery("987stuffAndThings", sum);
        savedQueryApi.createSavedQuery("987stuff0And0Things456", sum);

        savedQueryApi.createSavedQuery("_stuffAndThings", sum);
        savedQueryApi.createSavedQuery("stuffAndThings_", sum);
        savedQueryApi.createSavedQuery("stuff_and_things", sum);
        savedQueryApi.createSavedQuery("stuff_and_things_", sum);
        savedQueryApi.createSavedQuery("_stuff_and_things", sum);
        savedQueryApi.createSavedQuery("_stuff_and_things_", sum);

        savedQueryApi.createSavedQuery("-stuffAndThings", sum);
        savedQueryApi.createSavedQuery("stuffAndThings-", sum);
        savedQueryApi.createSavedQuery("stuff-and-things", sum);
        savedQueryApi.createSavedQuery("stuff-and-things-", sum);
        savedQueryApi.createSavedQuery("-stuff-and-things", sum);
        savedQueryApi.createSavedQuery("-stuff-and-things-", sum);

        savedQueryApi.createSavedQuery("__--123--__", sum);
        savedQueryApi.createSavedQuery("0-_s5TuFf1-3a9nd7-thInGs___", sum);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSavedQuery_Create_NullResourceName() throws IOException {
        KeenQueryRequest sum = getSimpleRequest();
        savedQueryApi.createSavedQuery(null, sum);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSavedQuery_Create_BadResourceName() throws IOException {
        KeenQueryRequest sum = getSimpleRequest();
        savedQueryApi.createSavedQuery("@lph@numerics_and_numbers_only", sum);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSavedQuery_Create_BadResourceNameUnicode() throws IOException {
        KeenQueryRequest sum = getSimpleRequest();
        savedQueryApi.createSavedQuery("\u00E0rbol \u03C0", sum);
    }

    @Test
    public void testSavedQuery_CreateWithDisplayName_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        KeenQueryRequest sum = getSimpleRequest();
        String displayName = "Some Display Name";
        Map<String, Object> createdQuery = savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME,
                                                                          sum,
                                                                          displayName);

        verifyPut(createdQuery, displayName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSavedQuery_Create_WithEmptyDisplayName() throws IOException {
        KeenQueryRequest sum = getSimpleRequest();
        savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME, sum, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSavedQuery_Create_WithBlankDisplayName() throws IOException {
        KeenQueryRequest sum = getSimpleRequest();
        savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME, sum, "   ");
    }

    @Test
    public void testSavedQuery_CreateFunnel_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        Funnel funnelParams = new Funnel.Builder()
                .withTimeframe(TEST_RELATIVE_TIMEFRAME)
                .withStep(new FunnelStep(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY))
                .build();

        Map<String, Object> funnelQuery = savedQueryApi.createSavedQuery(TEST_RESOURCE_NAME,
                                                                         funnelParams);

        verifyPut(funnelQuery);
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

        verifyPut(multiAnalysisQuery);
    }

    @Test
    public void testCachedQuery_Create_Simple() throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(null));

        KeenQueryRequest sum = getSimpleRequest();
        int refreshRate = (5 * 3600); // 5 hrs
        Map<String, Object> createdQuery = savedQueryApi.createCachedQuery(TEST_RESOURCE_NAME,
                                                                           sum,
                                                                           refreshRate);

        ObjectNode requestNode = verifyPut(createdQuery);
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

        ObjectNode requestNode = verifyPut(createdQuery, displayName);
        assertEquals(refreshRate, requestNode.get(KeenQueryConstants.REFRESH_RATE).asInt());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCachedQuery_Create_BadRefreshRate() throws IOException {
        KeenQueryRequest sum = getSimpleRequest();
        savedQueryApi.createCachedQuery(TEST_RESOURCE_NAME,
                                        sum,
                                        RefreshRate.MAX + 1);
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

    private void setResponseWithResult(String resultString) throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(resultString));
    }

    private void setGroupResponseWithResult(String resultString) throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(resultString, true, false, false, false));
    }

    private void setIntervalResponseWithResult(String resultString) throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(resultString, false, true, false, false));
    }

    private void setMultiResponseWithResult(String resultString) throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(resultString, false, false, true, false));
    }

    private void setFunnelResponseWithResult(String resultString) throws IOException {
        setMockResponse(200, getFakeSavedQueryDefinition(resultString, false, false, false, true));
    }

    @Test
    public void testSavedQuery_GetResult_Simple() throws IOException {
        // Just make sure that for each general type of QueryResult we can see, that we construct
        // the right type. Actual parsing of all those result types and construction of the
        // QueryResults of complex combinations of required and optional data is tested elsewhere.

        // Set up a simple result like that which would come from a sum.
        setResponseWithResult("65");

        QueryResult sumResultSaved = savedQueryApi.getResult(TEST_RESOURCE_NAME);
        assertTrue(sumResultSaved.isLong());

        // Set up a list result like that which would be returned from select_unique.
        setResponseWithResult("[0, 1, 2, 3, 4, 5, 6, 7, 8]");

        QueryResult selectUniqueResult = savedQueryApi.getResult(TEST_RESOURCE_NAME);
        assertTrue(selectUniqueResult.isListResult());

        // Set up a simple group by response.
        setGroupResponseWithResult("[{" +
                    "\"category\": \"one\"," +
                    "\"result\": 17" +
                "}, {" +
                    "\"category\": \"two\"," +
                    "\"result\": 31" +
                "}]");

        QueryResult groupByResult = savedQueryApi.getResult(TEST_RESOURCE_NAME);
        assertTrue(groupByResult.isGroupResult());

        // Set up a simple interval response.
        setIntervalResponseWithResult("[{" +
                    "\"timeframe\": {" +
                        "\"end\": \"2016-10-22T00:00:00.000Z\"," +
                        "\"start\": \"2016-10-21T00:00:00.000Z\"" +
                    "}," +
                    "\"value\": 17" +
                "}, {" +
                    "\"timeframe\": {" +
                        "\"end\": \"2016-10-23T00:00:00.000Z\"," +
                        "\"start\": \"2016-10-22T00:00:00.000Z\"" +
                    "}," +
                    "\"value\": 31" +
                "}]");

        QueryResult intervalResult = savedQueryApi.getResult(TEST_RESOURCE_NAME);
        assertTrue(intervalResult.isIntervalResult());

        // Set a simple Multi-Analysis response.
        setMultiResponseWithResult("{" +
                        "\"plain_old_count\": 24" +
                    "}");

        QueryResult multiAnalysisResult = savedQueryApi.getResult(TEST_RESOURCE_NAME);
        assertTrue(multiAnalysisResult instanceof MultiAnalysisResult);

        // Set a simple Funnel response. We don't really parse the "steps" in the result, so this
        // should be fine.
        setFunnelResponseWithResult("{" +
                    "\"steps\": []," +
                    "\"result\": [3, 1, 0]," +
                    "\"actors\": [[\"jeff\", \"jim\", \"joe\"], [\"sam\"], null]" +
                "}");

        QueryResult funnelResult = savedQueryApi.getResult(TEST_RESOURCE_NAME);
        assertTrue(funnelResult instanceof FunnelResult);
    }

    private void setMockResponsesForUpdate() throws IOException {
        // Makes sure we send two requests, one to get the definition and one to update.
        List<Response> mockResponses = new ArrayList<Response>();
        mockResponses.add(new Response(200, getFakeSavedQueryDefinition(null)));
        mockResponses.add(new Response(200, getFakeSavedQueryDefinition(null)));

        setMockResponses(mockResponses);
    }

    @Test
    public void testSavedQuery_Update_Simple() throws IOException {
        setMockResponsesForUpdate();

        Map<String, Object> updates = new HashMap<String, Object>();
        int refreshRate = 6 * 3600; // 6 hrs
        updates.put(KeenQueryConstants.REFRESH_RATE, refreshRate);

        Map<String, Object> updateResponse = savedQueryApi.updateQuery(TEST_RESOURCE_NAME, updates);
        ObjectNode requestNode = verifyPut(updateResponse, null, 1);
        assertTrue(requestNode.hasNonNull(KeenQueryConstants.REFRESH_RATE));
        assertEquals(refreshRate, requestNode.get(KeenQueryConstants.REFRESH_RATE).asInt());
    }

    @Test
    public void testSavedQuery_UpdateHelpers_QueryName() throws IOException {
        setMockResponsesForUpdate();

        String newQueryName = "new_name";
        Map<String, Object> updateResponse = savedQueryApi.setQueryName(TEST_RESOURCE_NAME,
                                                                        newQueryName);

        ObjectNode requestNode = verifyPut(updateResponse, null, 1);
        assertTrue(requestNode.hasNonNull(KeenQueryConstants.QUERY_NAME));
        assertEquals(newQueryName, requestNode.get(KeenQueryConstants.QUERY_NAME).asText());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshRate_Validation_TooLow() {
        RefreshRate.validateRefreshRate(1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshRate_Validation_Negative() {
        RefreshRate.validateRefreshRate(-5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshRate_Validation_TooHigh() {
        RefreshRate.validateRefreshRate(90000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshRate_FromHours_TooLow() {
        RefreshRate.fromHours(2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshRate_FromHours_Negative() {
        RefreshRate.fromHours(-5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshRate_FromHours_TooHigh() {
        RefreshRate.fromHours(25);
    }

    @Test
    public void testRefreshRate_Success() {
        RefreshRate.validateRefreshRate(0);
        RefreshRate.validateRefreshRate(RefreshRate.NO_CACHING);

        RefreshRate.validateRefreshRate(RefreshRate.MIN);
        RefreshRate.validateRefreshRate(RefreshRate.MIN + 1);

        RefreshRate.validateRefreshRate(RefreshRate.MAX);
        RefreshRate.validateRefreshRate(RefreshRate.MAX - 1);

        RefreshRate.fromHours(0);

        RefreshRate.fromHours(4);
        RefreshRate.fromHours(12);
        RefreshRate.fromHours(24);
    }

    @Test
    public void testSavedQuery_UpdateHelpers_RefreshRate() throws IOException {
        setMockResponsesForUpdate();

        int refreshRate = RefreshRate.fromHours(8);
        Map<String, Object> updateResponse = savedQueryApi.setRefreshRate(TEST_RESOURCE_NAME,
                                                                          refreshRate);

        ObjectNode requestNode = verifyPut(updateResponse, null, 1);
        assertTrue(requestNode.hasNonNull(KeenQueryConstants.REFRESH_RATE));
        assertEquals(refreshRate, requestNode.get(KeenQueryConstants.REFRESH_RATE).asInt());
    }

    @Test
    public void testSavedQuery_UpdateHelpers_DisplayName() throws IOException {
        setMockResponsesForUpdate();

        String newDisplayName = "new_display_name";
        Map<String, Object> updateResponse = savedQueryApi.setDisplayName(TEST_RESOURCE_NAME,
                                                                          newDisplayName);

        verifyPut(updateResponse, newDisplayName, 1);
    }

    @Test
    public void testSavedQuery_Delete_Simple() throws IOException {
        setMockResponse(204, null);
        savedQueryApi.deleteQuery("to-be-deleted");

        // Issue #100 : Check more about the lower-level HTTP request, like the specific
        // headers, URL, response codes, etc. Both the DELETE and GET requests have no request body
        // so everything is in the URL/headers. KeenQueryTest does this in some places.

        assertNull(getPersistentAnalysisRequestNode()); // GET request should have no body
    }
}

package io.keen.client.java;

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
import static org.junit.Assert.assertTrue;

/**
 * Test the Persistent Analysis query functionality, which should include Saved/Cached Queries and
 * Cached Datasets.
 *
 * @author masojus
 */
public class PersistentAnalysisTest extends KeenQueryTestBase {
    private SavedQueries savedQueryApi = null;

    @Before
    public void persistentAnalysisTestSetup() {
        // TODO : Obviously don't hit the real server once these tests are fleshed out.
        KeenProject project =
                new KeenProject("W",
                                "X",
                                "Y",
                                "Z");

        KeenQueryClient queryClientRealServer = new KeenQueryClient.Builder(project).build();
        savedQueryApi = queryClientRealServer.getSavedQueriesInterface();
    }

    @After
    public void persistentAnalysisTestTeardown() {
        savedQueryApi = null;
    }

    @Test
    public void testSavedQuery_Create_Simple() throws IOException {
        SingleAnalysis sum = new SingleAnalysis.Builder(QueryType.SUM)
                .withEventCollection("purchases2")
                .withTargetPropertyName("price")
                .withTimeframe(new RelativeTimeframe("this_2_months"))
                .build();

        String queryName = "sum_FromJavaSDK_4";
        Map<String, Object> createdQuery = savedQueryApi.createSavedQuery(queryName, sum);
        System.out.println(createdQuery);
        assertNotNull(createdQuery);
        assertTrue(!createdQuery.isEmpty());
        assertEquals(queryName, createdQuery.get(KeenQueryConstants.QUERY_NAME));
    }

    @Test
    public void testSavedQuery_CreateWithDisplayName_Simple() throws IOException {
        SingleAnalysis sum = new SingleAnalysis.Builder(QueryType.SUM)
                .withEventCollection("purchases2")
                .withTargetPropertyName("price")
                .withTimeframe(new RelativeTimeframe("this_2_months"))
                .build();

        String queryName = "sum_FromJavaSDK_withDisplayName_3";
        Map<String, Object> createdQuery = savedQueryApi.createSavedQuery(queryName,
                                                                          sum,
                                                                          "From Java SDK 3");
        System.out.println(createdQuery);
        assertNotNull(createdQuery);
        assertTrue(!createdQuery.isEmpty());
        assertEquals(queryName, createdQuery.get(KeenQueryConstants.QUERY_NAME));
    }

    @Test
    public void testSavedQuery_CreateMultiAnalysis_Simple() throws IOException {
        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection("purchases2")
                .withTimeframe(new RelativeTimeframe("this_2_months"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .build();

        String queryName = "multi_FromJavaSDK_1";
        Map<String, Object> multiAnalysisQuery = savedQueryApi.createSavedQuery(queryName,
                                                                                multiAnalysisParams);
        System.out.println(multiAnalysisQuery);
        assertNotNull(multiAnalysisQuery);
        assertTrue(!multiAnalysisQuery.isEmpty());
        assertEquals(queryName, multiAnalysisQuery.get(KeenQueryConstants.QUERY_NAME));
    }

    @Test
    public void testCachedQuery_Create_Simple() throws IOException {
        SingleAnalysis sum = new SingleAnalysis.Builder(QueryType.SUM)
                .withEventCollection("purchases2")
                .withTargetPropertyName("price")
                .withTimeframe(new RelativeTimeframe("this_2_months"))
                .build();

        String queryName = "sum_FromJavaSDK_Cached_1";
        int refreshRate = (5 * 3600); // 5 hrs
        Map<String, Object> createdQuery = savedQueryApi.createCachedQuery(queryName,
                                                                           sum,
                                                                           refreshRate);
        System.out.println(createdQuery);
        assertNotNull(createdQuery);
        assertTrue(!createdQuery.isEmpty());
        assertEquals(queryName, createdQuery.get(KeenQueryConstants.QUERY_NAME));
    }

    @Test
    public void testCachedQuery_CreateWithDisplayName_Simple() throws IOException {
        SingleAnalysis sum = new SingleAnalysis.Builder(QueryType.SUM)
                .withEventCollection("purchases2")
                .withTargetPropertyName("price")
                .withTimeframe(new RelativeTimeframe("this_2_months"))
                .build();

        String queryName = "sum_FromJavaSDK_withDisplayName_Cached_1";
        int refreshRate = (7 * 3600); // 7 hrs
        String displayName = "From Java SDK (Cached) 1";
        Map<String, Object> createdQuery = savedQueryApi.createCachedQuery(queryName,
                                                                           sum,
                                                                           displayName,
                                                                           refreshRate);
        System.out.println(createdQuery);
        assertNotNull(createdQuery);
        assertTrue(!createdQuery.isEmpty());
        assertEquals(queryName, createdQuery.get(KeenQueryConstants.QUERY_NAME));
    }

    @Test
    public void testSavedQuery_GetDefinitions_Simple() throws IOException {
        List<Map<String, Object>> allDefs = savedQueryApi.getAllDefinitions();
        System.out.println(allDefs);
        assertNotNull(allDefs);
        assertTrue(!allDefs.isEmpty());
    }

    @Test
    public void testSavedQuery_GetDefinition_Simple() throws IOException {
        Map<String, Object> oneDef = savedQueryApi.getDefinition
                ("new_saved_count_query100_renamed");
        System.out.println(oneDef);
        assertNotNull(oneDef);
        assertTrue(!oneDef.isEmpty());
    }

    @Test
    public void testSavedQuery_GetResult_Simple() throws IOException {
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

    @Test
    public void testSavedQuery_Update_Simple() throws IOException {
        // TODO : Java is...unaccommodating. Help with this by providing some utility functions
        // around known common updates like 'refresh_rate' and 'query_name' and others.
        Map<String, Object> updates = new HashMap<String, Object>();
        updates.put(KeenQueryConstants.REFRESH_RATE, (6 * 3600)); // 6 hrs

        Map<String, Object> updateResponse = savedQueryApi.updateQuery("not-cached", updates);
        assertNotNull(updateResponse);
    }

    @Test
    public void testSavedQuery_Delete_Simple() throws IOException {
        Map<String, Object> deleteResponse = savedQueryApi.deleteQuery("to-be-deleted");
        System.out.println(deleteResponse);
        assertNotNull(deleteResponse);
        assertTrue(deleteResponse.isEmpty());
    }
}

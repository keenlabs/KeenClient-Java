package io.keen.client.java;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.keen.client.java.Query.QueryBuilder;

/**
 * KeenClientTest
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenQueryTest {

    private static KeenProject TEST_PROJECT;
    private static final String ENCODING = "UTF-8";

    private static final String TEST_EVENT_COLLECTION = "android-sample-button-clicks";
    private static final String TEST_TARGET_PROPERTY = "click-number";
    private static final String TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP = "{\"" + KeenQueryConstants.TARGET_PROPERTY + "\":\"" + TEST_TARGET_PROPERTY + "\",\"" + KeenQueryConstants.EVENT_COLLECTION + "\":\"" + TEST_EVENT_COLLECTION + "\"}";


    private HttpHandler mockHttpHandler;
    private KeenQueryClient queryClient;

    @BeforeClass
    public static void classSetUp() {
        KeenLogging.enableLogging();
        TEST_PROJECT = new KeenProject("<project ID>", "<write key>", "<read key>");
    }

    @Before
    public void setup() throws IOException {
        // Set up a mock HTTP handler.
        mockHttpHandler = mock(HttpHandler.class);
        setMockResponse(500, "Unexpected HTTP request");

        // build the client
        queryClient = new TestKeenQueryClientBuilder(TEST_PROJECT)
                .withHttpHandler(mockHttpHandler)
                .build();

    }

    @After
    public void cleanUp() {
        queryClient = null;
    }

    // todo: remove this production test.
    @Test
    public void testRealQuery() throws Exception {
        KeenProject queryProject = new KeenProject("555190333bc696371aaaebb0", "<write key>", "eee2b89b5dab28bb4a66dcb7d676387959b2c518f884c287edf40fd048335db7b9b7e0d9eb6572e9152a9f3d96f0e413398310ad97dc9433c3a9a3298944f942d5b85f989b36087db42795539ac321e84ca53592c2c99d45bfba64417070a037e9e765c1e8594c62f1f75b6ea794afa0");

        KeenQueryClient queryClientTest = new TestKeenQueryClientBuilder(queryProject).build();

        ArrayList<String> groupByParam = new ArrayList<String>();
        groupByParam.add("click-number");
        groupByParam.add("keen.id");

        Query params = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withGroupBy(groupByParam)
                .withInterval("weekly")
                .build();

//        ArrayList<Object> result = queryClientTest.countGroupBy(TEST_EVENT_COLLECTION, groupBy, new Timeframe("this_year"), null);
        // contains "Keen.id", "click-number", and "result" (Integer)
        // an entry for every unique combo of group-by's.

//        ArrayList<Object> result = queryClientTest.countInterval(TEST_EVENT_COLLECTION, "weekly", new Timeframe("this_year"), null);
        // contains "value" - Integer, and "timeframe" - Timeframe with start & end

//        Object result = queryClientTest.execute(params, new Timeframe("this_year"));
        // each item has: value - list of HashMaps of keen.id, click-number, result
        //                timeframe - start, end.

        QueryResult result = queryClientTest.newExecute(params, new Timeframe("this_year"));

        if (result.isList()) {
            ArrayList<QueryResult> listResults = result.getList();
            for (QueryResult item : listResults) {
                if (item.isInterval()) {
                    Interval interval = item.getInterval();
                    Timeframe itemTimeframe = interval.getTimeframe();
                    QueryResult intervalValue = interval.getValue();
                    if (intervalValue.isList()) {
                        ArrayList<QueryResult> groupBys = intervalValue.getList();
                        for (QueryResult groupByItem : groupBys) {
                            if (groupByItem.isGroupBy()) {
                                GroupBy groupBy = groupByItem.getGroupBy();
                                HashMap<String, Object> properties = groupBy.getProperties();
                                QueryResult groupByResult = groupBy.getResult();
                                if (groupByResult.isInteger()) {
                                    Integer val = groupByResult.getInteger();
                                            // do something with integer result.
                                }
                            }
                        }
                    }
                }
            }
        }


//        Integer result = queryClientTest.count(TEST_EVENT_COLLECTION, new Timeframe("this_year"), null);

//        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
//        Object result = queryClientTest.funnel(listSteps);

    }


    // TEST BASIC REQUIRED PARAMETERS
    @Test
    public void testCount()  throws Exception {
        setMockResponse(200, "{\"result\": 21}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Integer result = queryClient.count(TEST_EVENT_COLLECTION, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        assertEquals(requestString, "{\"" + KeenQueryConstants.EVENT_COLLECTION + "\":\"" + TEST_EVENT_COLLECTION + "\"}");

    }
    @Test
    public void testCountUniqueQuery()  throws Exception {
        setMockResponse(200, "{\"result\": 9}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Integer result = queryClient.countUnique(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT_UNIQUE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP);
    }
    @Test
    public void testMinimum()  throws Exception {
        setMockResponse(200, "{\"result\": 0}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.minimum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MINIMUM_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP);
    }
    @Test
    public void testMaximum()  throws Exception {
        setMockResponse(200, "{\"result\": 8}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.maximum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MAXIMUM_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP);
    }

    @Test
    public void testAverage()  throws Exception {
        setMockResponse(200, "{\"result\": 3.0952380952380953}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.average(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.AVERAGE_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP);

    }

    @Test
    public void testMedian()  throws Exception {
        setMockResponse(200, "{\"result\": 3}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.median(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MEDIAN_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP);
    }

    @Test
    public void testPercentile()  throws Exception {
        setMockResponse(200, "{\"result\": 3}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.percentile(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, 50.0, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.PERCENTILE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, "{\""+KeenQueryConstants.TARGET_PROPERTY+"\":\""+TEST_TARGET_PROPERTY+"\",\"percentile\":50.0,\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }
    @Test
    public void testSum()  throws Exception {
        setMockResponse(200, "{\"result\": 65}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.sum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.SUM_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP);

    }
    @Test
    public void testSelectUnique()  throws Exception {
        setMockResponse(200, "{\"result\": [0, 1, 2, 3, 4, 5, 6, 7, 8]}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Object result = queryClient.selectUnique(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.SELECT_UNIQUE_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECITON_AND_TARGET_PROP);

    }

    @Test
    public void testExtraction()  throws Exception {
        setMockResponse(200, "{\"result\":[{\"keen\": {\"timestamp\": \"2015-05-12T05:55:55.833Z\", \"created_at\": \"2015-05-12T05:56:49.502Z\", \"id\": \"555196212fd4b12ed0fe00a6\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-12T05:55:57.669Z\", \"created_at\": \"2015-05-12T05:56:49.502Z\", \"id\": \"555196212fd4b12ed0fe00a7\"}, \"click-number\": 1}, {\"keen\": {\"timestamp\": \"2015-05-12T05:57:54.837Z\", \"created_at\": \"2015-05-12T05:58:56.026Z\", \"id\": \"555196a02fd4b12ee6d51030\"}, \"click-number\": 2}, {\"keen\": {\"timestamp\": \"2015-05-12T05:58:30.022Z\", \"created_at\": \"2015-05-12T05:58:56.027Z\", \"id\": \"555196a02fd4b12ee6d51031\"}, \"click-number\": 3}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:09.249Z\", \"created_at\": \"2015-05-12T05:59:31.575Z\", \"id\": \"555196c32fd4b12ec02d3560\"}, \"click-number\": 4}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:11.741Z\", \"created_at\": \"2015-05-12T05:59:31.575Z\", \"id\": \"555196c32fd4b12ec02d3561\"}, \"click-number\": 5}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:12.379Z\", \"created_at\": \"2015-05-12T05:59:31.576Z\", \"id\": \"555196c32fd4b12ec02d3562\"}, \"click-number\": 6}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:12.942Z\", \"created_at\": \"2015-05-12T05:59:31.576Z\", \"id\": \"555196c32fd4b12ec02d3563\"}, \"click-number\": 7}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:13.533Z\", \"created_at\": \"2015-05-12T05:59:31.576Z\", \"id\": \"555196c32fd4b12ec02d3564\"}, \"click-number\": 8}, {\"keen\": {\"timestamp\": \"2015-05-12T22:22:39.964Z\", \"created_at\": \"2015-05-13T21:48:04.499Z\", \"id\": \"5553c694c2266c32a3bab9ae\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-13T21:47:17.520Z\", \"created_at\": \"2015-05-13T21:48:04.499Z\", \"id\": \"5553c694c2266c32a3bab9af\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:22.496Z\", \"created_at\": \"2015-05-14T05:15:10.940Z\", \"id\": \"55542f5ec2266c32526aaf81\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:25.883Z\", \"created_at\": \"2015-05-14T05:15:10.940Z\", \"id\": \"55542f5ec2266c32526aaf82\"}, \"click-number\": 1}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:43.388Z\", \"created_at\": \"2015-05-14T05:15:10.941Z\", \"id\": \"55542f5ec2266c32526aaf83\"}, \"click-number\": 2}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:44.087Z\", \"created_at\": \"2015-05-14T05:15:10.941Z\", \"id\": \"55542f5ec2266c32526aaf84\"}, \"click-number\": 3}, {\"keen\": {\"timestamp\": \"2015-05-14T03:32:12.947Z\", \"created_at\": \"2015-05-14T05:15:10.941Z\", \"id\": \"55542f5ec2266c32526aaf85\"}, \"click-number\": 4}, {\"keen\": {\"timestamp\": \"2015-05-14T03:32:44.058Z\", \"created_at\": \"2015-05-14T05:15:10.942Z\", \"id\": \"55542f5ec2266c32526aaf86\"}, \"click-number\": 5}, {\"keen\": {\"timestamp\": \"2015-05-14T03:33:05.368Z\", \"created_at\": \"2015-05-14T05:15:10.942Z\", \"id\": \"55542f5ec2266c32526aaf87\"}, \"click-number\": 6}, {\"keen\": {\"timestamp\": \"2015-05-14T03:33:15.033Z\", \"created_at\": \"2015-05-14T05:15:10.943Z\", \"id\": \"55542f5ec2266c32526aaf88\"}, \"click-number\": 7}, {\"keen\": {\"timestamp\": \"2015-05-15T18:01:00.773Z\", \"created_at\": \"2015-05-15T18:01:11.498Z\", \"id\": \"555634672fd4b12ed06db81f\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-15T18:01:52.746Z\", \"created_at\": \"2015-05-15T18:02:09.143Z\", \"id\": \"555634a12fd4b12ebabfcaf8\"}, \"click-number\": 1}]}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Object result = queryClient.extraction(TEST_EVENT_COLLECTION, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.EXTRACTION_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);

        assertEquals(resultString, "{\"" + KeenQueryConstants.EVENT_COLLECTION + "\":\""+TEST_EVENT_COLLECTION+"\"}");

    }
    // todo: test the side effect of this.
    @Test
    public void testExtractionEmail()  throws Exception {

        setMockResponse(200, "{\"result\": \"Processing. Check the specified email for the extraction results.\"}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        queryClient.extraction(TEST_EVENT_COLLECTION, "testEmail@email.com", null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.EXTRACTION_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);

        assertEquals(resultString, "{\""+KeenQueryConstants.EMAIL+"\":\"testEmail@email.com\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");

    }
    @Test
    public void testFunnel() throws Exception {
        setMockResponse(200, "{\"steps\": [{\"with_actors\": false, \"actor_property\": \"click-count\", \"filters\": [], \"timeframe\": null, \"timezone\": null, \"event_collection\": \"android-sample-button-clicks\", \"optional\": false, \"inverted\": false}], \"result\": [1]}");
        List<Map<String, Object>> listSteps = new ArrayList<Map<String, Object>>();

        Map<String, Object> steps = new HashMap<String, Object>();
        steps.put(KeenQueryConstants.ACTOR_PROPERTY, "click-count");
        steps.put(KeenQueryConstants.EVENT_COLLECTION, TEST_EVENT_COLLECTION);

        listSteps.add(steps);

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Object result = queryClient.funnel(listSteps, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.FUNNEL));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);

        assertEquals(requestString, "{\""+KeenQueryConstants.STEPS+"\":[{\""+KeenQueryConstants.ACTOR_PROPERTY+"\":\"click-count\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}]}");
    }

    @Test
    public void testMultiAnalysis() throws Exception {
        setMockResponse(200, "{\"result\": {\"count set\": 44, \"sum set\": 110}}");

        Map<String, Object> analysis = new HashMap<String, Object>();

        Map<String, String> firstSet = new HashMap<String, String>();
        firstSet.put(KeenQueryConstants.ANALYSIS_TYPE, KeenQueryConstants.COUNT_RESOURCE);

        Map<String, String> secondSet = new HashMap<String, String>();
        secondSet.put(KeenQueryConstants.ANALYSIS_TYPE, KeenQueryConstants.SUM_RESOURCE);
        secondSet.put(KeenQueryConstants.TARGET_PROPERTY, TEST_TARGET_PROPERTY);

        analysis.put("count set", firstSet);
        analysis.put("sum set", secondSet);

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Object result = queryClient.multiAnalysis(TEST_EVENT_COLLECTION, analysis, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MULTI_ANALYSIS));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);

        assertEquals(requestString, "{\"analyses\":{\"count set\":{\""+KeenQueryConstants.ANALYSIS_TYPE+"\":\"count\"},\"sum set\":{\""+KeenQueryConstants.TARGET_PROPERTY+"\":\"click-number\",\""+KeenQueryConstants.ANALYSIS_TYPE+"\":\"sum\"}},\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    // Test Server error
    @Test(expected = ServerException.class)
    public void testAddEventServerFailure() throws Exception {
        setMockResponse(500, "Injected server error");
        queryClient.sum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
    }

    // TEST OPTIONAL PARAMETERS
    @Test
    public void testFilterValid()  throws Exception {
        setMockResponse(200, "{\"result\": 6}");
        try {
            Query queryParams = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                    .withEventCollection(TEST_EVENT_COLLECTION)
                    .build();

            queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.LESS_THAN, 5);
            queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.GREATER_THAN, 1);

            Object result = queryClient.execute(queryParams, null);
        } catch (IOException e) {
        }
    }

    @Test(expected=KeenQueryClientException.class)
    public void testFilterInvalid1()  throws Exception {

        // in reality this would be a 400 error - just testing if no "result" in 200 response.
        setMockResponse(200, "{\"message\": \"You specified a geo filter on a property other than keen.location.coordinates, which is not allowed. You specified: ''.\", \"error_code\": \"InvalidPropertyNameForGeoFilter\"}");
        Query queryParams = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .build();
        queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.LESS_THAN, 5);
        queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.WITHIN, "INVALID");

        Object result = queryClient.execute(queryParams, null);
    }

    @Test(expected=ServerException.class)
    public void testFilterInvalid2() throws Exception {
        setMockResponse(400, "{\"message\": \"You specified a geo filter on a property other than keen.location.coordinates, which is not allowed. You specified: ''.\", \"error_code\": \"InvalidPropertyNameForGeoFilter\"}");
        Query queryParams = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .build();
        queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.LESS_THAN, 5);
        queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.WITHIN, "INVALID");

        Object result = queryClient.execute(queryParams, null);
    }

//    @Test
//    public void testAbsoluteTimeframeManuallyBuilt() throws Exception  {
//        setMockResponse(200, "{\"result\": 44}");
//
//        String startTime = "2012-08-13T19:00:00.000Z";
//        String endTime = "2015-06-07T19:00:00.000Z";
//
////        Map<String, Object> absoluteTimeframe = new HashMap<String, Object>();
////        absoluteTimeframe.put(KeenQueryConstants.START, startTime);
////        absoluteTimeframe.put(KeenQueryConstants.END, endTime);
//
//        Query queryParams = new QueryBuilder(QueryType.COUNT_RESOURCE)
//                .withEventCollection(TEST_EVENT_COLLECTION)
//                .build();
//
//        String requestString = mockCaptureCountQueryRequest(queryParams);
//
//        assertEquals( requestString, "{\""+KeenQueryConstants.TIMEFRAME+"\":{\""+KeenQueryConstants.START+"\":\""+startTime+"\",\""+KeenQueryConstants.END+"\":\""+endTime+"\"},\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
//    }

//    @Test
//    public void testAbsoluteTimeframe() throws Exception  {
//        setMockResponse(200, "{\"result\": 44}");
//
//        String startTime = "2012-08-13T19:00:00.000Z";
//        String endTime = "2015-06-07T19:00:00.000Z";
//
//        Map<String, Object> absoluteTimeframe = new HashMap<String, Object>();
//        absoluteTimeframe.put(KeenQueryConstants.START, startTime);
//        absoluteTimeframe.put(KeenQueryConstants.END, endTime);
//
//        Query queryParams = new QueryBuilder()
//                .withEventCollection(TEST_EVENT_COLLECTION)
//                .withTimeframe(startTime, endTime)
//                .build();
//
////        queryParams(startTime, endTime);
//
//        String requestString = mockCaptureCountQueryRequest(queryParams);
//
//        assertEquals( requestString, "{\""+KeenQueryConstants.TIMEFRAME+"\":{\""+KeenQueryConstants.START+"\":\""+startTime+"\",\""+KeenQueryConstants.END+"\":\""+endTime+"\"},\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
//    }
//    @Test
//    public void testRelativeTimeframe() throws Exception {
//        setMockResponse(200, "{\"result\": 2}");
//
//        Query queryParams = new QueryBuilder()
//                .withEventCollection(TEST_EVENT_COLLECTION)
//                .withTimeframe("this_month")
//                .build();
//
//        String requestString = mockCaptureCountQueryRequest(queryParams);
//        assertEquals( requestString,  "{\""+KeenQueryConstants.TIMEFRAME+"\":\"this_month\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
//    }

    // interval requires timeframe.
    @Test
    public void testInterval() throws Exception {
        setMockResponse(200, "{\"result\": [{\"value\": 2, \"timeframe\": {\"start\": \"2015-06-01T00:00:00.000Z\", \"end\": \"2015-06-07T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-07T00:00:00.000Z\", \"end\": \"2015-06-14T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-14T00:00:00.000Z\", \"end\": \"2015-06-21T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-21T00:00:00.000Z\", \"end\": \"2015-06-28T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-28T00:00:00.000Z\", \"end\": \"2015-07-01T00:00:00.000Z\"}}]}");

        Query queryParams = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withInterval("weekly")
                .build();

        // TODO: fix this. Need to add timeframe...
//        String requestString = mockCaptureCountQueryRequest(queryParams);
//        assertEquals( requestString, "{\""+KeenQueryConstants.INTERVAL+"\":\"weekly\",\"timeframe\":\"this_month\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    @Test
    public void testTimezone() throws Exception {
        setMockResponse(200, "{\"result\": 2}");
        Query queryParams = new QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimezone("UTC")
                .build();

        // TODO: fix this. Needs timeframe.
//        String requestString = mockCaptureCountQueryRequest(queryParams);
//        assertEquals( requestString, "{\"timezone\":\"UTC\",\"timeframe\":\"this_month\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    @Test
    public void testGroupBy() throws Exception {
        setMockResponse(200, "{\"result\": [{\"result\": 10, \"click-number\": 0}, {\"result\": 7, \"click-number\": 1}, {\"result\": 6, \"click-number\": 2}, {\"result\": 5, \"click-number\": 3}, {\"result\": 4, \"click-number\": 4}, {\"result\": 4, \"click-number\": 5}, {\"result\": 3, \"click-number\": 6}, {\"result\": 2, \"click-number\": 7}, {\"result\": 1, \"click-number\": 8}, {\"result\": 2, \"click-number\": null}]}");

        Query queryParams = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withGroupBy("click-number")
                .build();

        String requestString = mockCaptureCountQueryRequest(queryParams);
        // todo: redo this, since group-by can be a list.
//        assertEquals( requestString, "{\"group_by\":\"click-number\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    @Test
    public void testMaxAge() throws Exception {
        setMockResponse(200, "{\"result\": 44}");

        Query queryParams = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withMaxAge(300)
                .build();

        String requestString = mockCaptureCountQueryRequest(queryParams);
        assertEquals( requestString, "{\"max_age\":300,\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    private String mockCaptureCountQueryRequest(Query inputParams) throws Exception {
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Object result = queryClient.execute(inputParams, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        return outputStream.toString(ENCODING);
    }

    private void setMockResponse(int statusCode, String body) throws IOException {
        Response response = new Response(statusCode, body);
        when(mockHttpHandler.execute(any(Request.class))).thenReturn(response);
    }


}

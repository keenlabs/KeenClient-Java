package io.keen.client.java;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.keen.client.java.result.QueryResult;
import io.keen.client.java.result.Group;
import io.keen.client.java.result.GroupByResult;
import io.keen.client.java.result.IntervalResult;

/**
 * KeenQueryTest
 *
 * @author claireyoung
 * @since 1.0.0
 */
public class KeenQueryTest {

    private static KeenProject TEST_PROJECT;
    private static final String ENCODING = "UTF-8";
    private static final String TEST_EVENT_COLLECTION = "android-sample-button-clicks";
    private static final String TEST_TARGET_PROPERTY = "click-number";
    private static final String TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP = "{\"" + KeenQueryConstants.TARGET_PROPERTY + "\":\"" + TEST_TARGET_PROPERTY + "\",\"" + KeenQueryConstants.EVENT_COLLECTION + "\":\"" + TEST_EVENT_COLLECTION + "\"}";

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
        queryClient = new KeenQueryClient.Builder(TEST_PROJECT)
                .withJsonHandler(new TestJsonHandler())
                .withHttpHandler(mockHttpHandler)
                .build();
    }

    @After
    public void cleanUp() {
        queryClient = null;
    }

    // TEST BASIC REQUIRED PARAMETERS
    @Test
    public void testCount()  throws Exception {
        setMockResponse(200, "{\"result\": 21}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Long result = queryClient.count(TEST_EVENT_COLLECTION, new RelativeTimeframe("this_year"));

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        assertEquals(requestString, "{\""+KeenQueryConstants.TIMEFRAME+"\":\"this_year\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    public class Node {
        int value;
        Node next;
    }

    public Node removeNode(int nodeValue, Node start) {
        Node current = start;

        if (current == null) {
            return null;
        }

        while (current.next != null) {
            Node nextNode = current.next;
            if (nextNode.value == nodeValue) {
                current.next = nextNode.next;
            }
            return nextNode;
        }
        return null;
    }

    @Test
    public void testCountExecute() throws Exception {
        setMockResponse(200, "{\"result\": 21}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Query query = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_year"))
                .build();
        QueryResult result = queryClient.execute(query);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        assertEquals(requestString, "{\""+KeenQueryConstants.TIMEFRAME+"\":\"this_year\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");

        assertTrue(result.isLong());
    }


    @Test
    public void testCountUnique() throws Exception {
        setMockResponse(200, "{\"result\": 9}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Long result = queryClient.countUnique(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT_UNIQUE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP);
    }

    @Test
    public void testMinimum()  throws Exception {
        setMockResponse(200, "{\"result\": 0}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.minimum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MINIMUM));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP);
    }

    @Test
    public void testMaximum()  throws Exception {
        setMockResponse(200, "{\"result\": 8}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.maximum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MAXIMUM));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP);
    }

    @Test
    public void testAverage()  throws Exception {
        setMockResponse(200, "{\"result\": 3.0952380952380953}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.average(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.AVERAGE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP);

    }

    @Test
    public void testMedian()  throws Exception {
        setMockResponse(200, "{\"result\": 3}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Double result = queryClient.median(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MEDIAN));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP);
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
        assertTrue(request.url.toString().contains(KeenQueryConstants.SUM));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP);

    }

    @Test
    public void testSelectUnique()  throws Exception {
        setMockResponse(200, "{\"result\": [0, 1, 2, 3, 4, 5, 6, 7, 8]}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.selectUnique(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.SELECT_UNIQUE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, TEST_REQ_EVENT_COLLECTION_AND_TARGET_PROP);

        assertTrue(result.isListResult());
        assertTrue(result.getListResults().get(0).isLong());
    }

    @Test
    public void testGroupByResponse() throws Exception {
        setMockResponse(200, "{\"result\": [{\"keen.id\": \"555196212fd4b12ed0fe00a6\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"5553c694c2266c32a3bab9ae\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"5553c694c2266c32a3bab9af\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"55542f5ec2266c32526aaf81\", \"click-number\": 0, \"result\": 1}]}");

        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withGroupBy("click-number")
                .withGroupBy("keen.id")
                .build();

        QueryResult result = queryClient.execute(queryParams);

        assertTrue(result.isGroupResult());
        GroupByResult groupBy = (GroupByResult)result;
        Map<Group, QueryResult> resultMap = groupBy.getGroupResults();

        Group firstGroup = resultMap.keySet().iterator().next();
        assertNotNull(firstGroup);
        assertNotNull(firstGroup.getGroupValue("keen.id"));
        assertNotNull(firstGroup.getGroupValue("click-number"));

        assertTrue(resultMap.get(firstGroup).isLong());
    }

    @Test
    public void testIntervalResponse() throws Exception {
        setMockResponse(200, "{\"result\": [{\"value\": 0, \"timeframe\": {\"start\": \"2015-01-01T00:00:00.000Z\", \"end\": \"2015-01-04T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-01-04T00:00:00.000Z\", \"end\": \"2015-01-11T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-01-11T00:00:00.000Z\", \"end\": \"2015-01-18T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-01-18T00:00:00.000Z\", \"end\": \"2015-01-25T00:00:00.000Z\"}}]}");

        Query params = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withInterval("weekly")
                .withTimeframe(new RelativeTimeframe("this_year"))
                .build();

        QueryResult result = queryClient.execute(params);

        assertTrue(result.isIntervalResult());
        IntervalResult interval = (IntervalResult)result;
        Map<AbsoluteTimeframe, QueryResult> resultMap = interval.getIntervalResults();

        AbsoluteTimeframe firstInterval = resultMap.keySet().iterator().next();
        assertNotNull(firstInterval);
        assertNotNull(firstInterval.getStart());
        assertNotNull(firstInterval.getEnd());

        assertTrue(resultMap.get(firstInterval).isLong());
    }

    @Test
    public void testGroupByIntervalResponse() throws Exception {
        setMockResponse(200, "{\"result\": [{\"value\": [{\"keen.id\": \"555196212fd4b12ed0fe00a6\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"5553c694c2266c32a3bab9ae\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"5553c694c2266c32a3bab9af\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf81\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"555634672fd4b12ed06db81f\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e379a\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e379b\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e379e\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a4\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a8\", \"click-number\": 0, \"result\": 0}, {\"keen.id\": \"555196212fd4b12ed0fe00a7\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf82\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"555634a12fd4b12ebabfcaf8\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e379c\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e379f\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a5\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a9\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"555196a02fd4b12ee6d51030\", \"click-number\": 2, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf83\", \"click-number\": 2, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e379d\", \"click-number\": 2, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a0\", \"click-number\": 2, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a6\", \"click-number\": 2, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37aa\", \"click-number\": 2, \"result\": 0}, {\"keen.id\": \"555196a02fd4b12ee6d51031\", \"click-number\": 3, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf84\", \"click-number\": 3, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a1\", \"click-number\": 3, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a7\", \"click-number\": 3, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37ab\", \"click-number\": 3, \"result\": 0}, {\"keen.id\": \"555196c32fd4b12ec02d3560\", \"click-number\": 4, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf85\", \"click-number\": 4, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a2\", \"click-number\": 4, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37ac\", \"click-number\": 4, \"result\": 0}, {\"keen.id\": \"555196c32fd4b12ec02d3561\", \"click-number\": 5, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf86\", \"click-number\": 5, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37a3\", \"click-number\": 5, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37ad\", \"click-number\": 5, \"result\": 0}, {\"keen.id\": \"555196c32fd4b12ec02d3562\", \"click-number\": 6, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf87\", \"click-number\": 6, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37ae\", \"click-number\": 6, \"result\": 0}, {\"keen.id\": \"555196c32fd4b12ec02d3563\", \"click-number\": 7, \"result\": 0}, {\"keen.id\": \"55542f5ec2266c32526aaf88\", \"click-number\": 7, \"result\": 0}, {\"keen.id\": \"555196c32fd4b12ec02d3564\", \"click-number\": 8, \"result\": 0}, {\"keen.id\": \"556e05ebe085575ca32e37af\", \"click-number\": null, \"result\": 0}, {\"keen.id\": \"556e0683c1e0ab5a1c1a945b\", \"click-number\": null, \"result\": 0}], \"timeframe\": {\"start\": \"2015-01-01T00:00:00.000Z\", \"end\": \"2015-01-04T00:00:00.000Z\"}} ]}");

        // GROUP BY & INTERVAL
        Query params = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withGroupBy("click-number")
                .withGroupBy("keen.id")
                .withInterval("weekly")
                .withTimeframe(new RelativeTimeframe("this_year"))
                .build();

        QueryResult result = queryClient.execute(params);

        assertTrue(result.isIntervalResult());
        IntervalResult interval = (IntervalResult) result;
        Map<AbsoluteTimeframe, QueryResult> resultMap = interval.getIntervalResults();

        // interval
        AbsoluteTimeframe firstInterval = resultMap.keySet().iterator().next();
        assertNotNull(firstInterval);
        assertNotNull(firstInterval.getStart());
        assertNotNull(firstInterval.getEnd());

        // group-by
        assertTrue(resultMap.get(firstInterval).isGroupResult());
        QueryResult groupByItem = resultMap.get(firstInterval);
        GroupByResult groupBy = (GroupByResult) groupByItem;
        Map<Group, QueryResult> groupByResultMap = groupBy.getGroupResults();

        Group firstGroup = groupByResultMap.keySet().iterator().next();
        assertNotNull(firstGroup);
        assertNotNull(firstGroup.getGroupValue("keen.id"));
        assertNotNull(firstGroup.getGroupValue("click-number"));

        assertTrue(groupByResultMap.get(firstGroup).isLong());
    }

    // Test Server error
    @Test(expected = ServerException.class)
    public void testAddEventServerFailure() throws Exception {
        setMockResponse(500, "Injected server error");
        queryClient.sum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
    }

    //
    // TEST OPTIONAL PARAMETERS
    //

    @Test
    public void testFilterValid()  throws Exception {
        setMockResponse(200, "{\"result\": 6}");

        Query queryParams = new Query.Builder(QueryType.COUNT)
                    .withEventCollection(TEST_EVENT_COLLECTION)
                    .withFilter(TEST_TARGET_PROPERTY, FilterOperator.LESS_THAN, 5)
                    .withFilter(TEST_TARGET_PROPERTY, FilterOperator.GREATER_THAN, 1)
                    .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);

        assertEquals(requestString, "{\"filters\":[{\"property_value\":5,\"property_name\":\""+TEST_TARGET_PROPERTY+"\",\"operator\":\"lt\"},{\"property_value\":1,\"property_name\":\""+TEST_TARGET_PROPERTY+"\",\"operator\":\"gt\"}],\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");

    }

    @Test(expected=KeenQueryClientException.class)
    public void testFilterInvalid1()  throws Exception {
        // in reality this would be a 400 error - just testing if no "result" in 200 response.
        setMockResponse(200, "{\"message\": \"You specified a geo filter on a property other than keen.location.coordinates, which is not allowed. You specified: ''.\", \"error_code\": \"InvalidPropertyNameForGeoFilter\"}");

        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withFilter(TEST_TARGET_PROPERTY, FilterOperator.LESS_THAN, 5)
                .withFilter(TEST_TARGET_PROPERTY, FilterOperator.WITHIN, "INVALID")
                .build();

        QueryResult result = queryClient.execute(queryParams);
    }

    @Test(expected=ServerException.class)
    public void testFilterInvalid2() throws Exception {
        setMockResponse(400, "{\"message\": \"You specified a geo filter on a property other than keen.location.coordinates, which is not allowed. You specified: ''.\", \"error_code\": \"InvalidPropertyNameForGeoFilter\"}");
        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withFilter(TEST_TARGET_PROPERTY, FilterOperator.LESS_THAN, 5)
                .withFilter(TEST_TARGET_PROPERTY, FilterOperator.WITHIN, "INVALID")
                .build();

        QueryResult result = queryClient.execute(queryParams);
    }

    @Test
    public void testAbsoluteTimeframe() throws Exception  {
        setMockResponse(200, "{\"result\": 44}");

        String startTime = "2012-08-13T19:00:00.000Z";
        String endTime = "2015-06-07T19:00:00.000Z";
        Timeframe timeframe = new AbsoluteTimeframe(startTime, endTime);

        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(timeframe)
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);

        assertEquals( requestString, "{\""+KeenQueryConstants.TIMEFRAME+"\":{\""+KeenQueryConstants.START+"\":\""+startTime+"\",\""+KeenQueryConstants.END+"\":\""+endTime+"\"},\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");

    }

    @Test
    public void testRelativeTimeframe() throws Exception {
        setMockResponse(200, "{\"result\": 2}");
        Timeframe timeframe = new RelativeTimeframe("this_month");
        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(timeframe)
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);
        assertEquals( requestString,  "{\""+KeenQueryConstants.TIMEFRAME+"\":\"this_month\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");

    }

    // interval requires timeframe.
    @Test
    public void testInterval() throws Exception {
        setMockResponse(200, "{\"result\": [{\"value\": 2, \"timeframe\": {\"start\": \"2015-06-01T00:00:00.000Z\", \"end\": \"2015-06-07T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-07T00:00:00.000Z\", \"end\": \"2015-06-14T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-14T00:00:00.000Z\", \"end\": \"2015-06-21T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-21T00:00:00.000Z\", \"end\": \"2015-06-28T00:00:00.000Z\"}}, {\"value\": 0, \"timeframe\": {\"start\": \"2015-06-28T00:00:00.000Z\", \"end\": \"2015-07-01T00:00:00.000Z\"}}]}");

        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withInterval("weekly")
                .withTimeframe(new RelativeTimeframe("this_month"))
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);
        assertEquals( requestString, "{\""+KeenQueryConstants.INTERVAL+"\":\"weekly\",\"timeframe\":\"this_month\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    @Test
    public void testTimezone() throws Exception {
        setMockResponse(200, "{\"result\": 2}");
        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimezone("UTC")
                .withTimeframe(new RelativeTimeframe("this_month"))
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);
        assertEquals( requestString, "{\"timezone\":\"UTC\",\"timeframe\":\"this_month\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    @Test
    public void testGroupBy() throws Exception {

        // group by click-number and keen.id
        setMockResponse(200, "{\"result\": [{\"keen.id\": \"555196212fd4b12ed0fe00a6\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"5553c694c2266c32a3bab9ae\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"5553c694c2266c32a3bab9af\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"55542f5ec2266c32526aaf81\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"555634672fd4b12ed06db81f\", \"click-number\": 0, \"result\": 1}, {\"keen.id\": \"556e05ebe085575ca32e379a\", \"click-number\": 0, \"result\": 1}]}");

        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withGroupBy("click-number")
                .withGroupBy("keen.id")
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);
        assertEquals( requestString, "{\"group_by\":[\"click-number\",\"keen.id\"],\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    @Test
    public void testMaxAge() throws Exception {
        setMockResponse(200, "{\"result\": 44}");

        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withMaxAge(300)
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);
        assertEquals( requestString, "{\"max_age\":300,\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }

    private String mockCaptureCountQueryRequest(Query inputParams) throws Exception {
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(inputParams);

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

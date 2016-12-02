package io.keen.client.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import io.keen.client.java.exceptions.KeenQueryClientException;
import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;
import io.keen.client.java.result.FunnelResult;
import io.keen.client.java.result.Group;
import io.keen.client.java.result.GroupByResult;
import io.keen.client.java.result.IntervalResult;
import io.keen.client.java.result.IntervalResultValue;
import io.keen.client.java.result.ListResult;
import io.keen.client.java.result.MultiAnalysisResult;
import io.keen.client.java.result.QueryResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * KeenQueryTest
 *
 * @author claireyoung, baumatron, masojus
 * @since 1.0.0
 */
public class KeenQueryTest {

    private static KeenProject TEST_PROJECT;
    private static final String ENCODING = "UTF-8";
    private static final String TEST_EVENT_COLLECTION = "android-sample-button-clicks";
    private static final String TEST_TARGET_PROPERTY = "click-number";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
        long result = queryClient.count(TEST_EVENT_COLLECTION, new RelativeTimeframe("this_year"));
        assertEquals(21, result);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals("this_year", requestNode.get(KeenQueryConstants.TIMEFRAME).asText());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
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
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals("this_year", requestNode.get(KeenQueryConstants.TIMEFRAME).asText());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());

        assertTrue(result.isLong());
        assertEquals(21, result.longValue());
    }


    @Test
    public void testCountUnique() throws Exception {
        setMockResponse(200, "{\"result\": 9}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        long result = queryClient.countUnique(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
        assertEquals(9, result);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT_UNIQUE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
    }

    @Test
    public void testMinimum()  throws Exception {
        setMockResponse(200, "{\"result\": 0}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        double result = queryClient.minimum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
        assertEquals(0, result, 0);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MINIMUM));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
    }

    @Test
    public void testMaximum()  throws Exception {
        setMockResponse(200, "{\"result\": 8}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        double result = queryClient.maximum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
        assertEquals(8, result, 0);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MAXIMUM));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
    }

    @Test
    public void testAverage()  throws Exception {
        setMockResponse(200, "{\"result\": 3.0952380952380953}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        double result = queryClient.average(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
        assertEquals(3.0952380952380953, result, 0);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.AVERAGE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
    }

    @Test
    public void testMedian()  throws Exception {
        setMockResponse(200, "{\"result\": 3}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        double result = queryClient.median(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
        assertEquals(3, result, 0);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.MEDIAN));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
    }

    @Test
    public void testPercentile()  throws Exception {
        setMockResponse(200, "{\"result\": 3}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        double result = queryClient.percentile(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, 50.0, new RelativeTimeframe("this_hour"));
        assertEquals(3, result, 0);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.PERCENTILE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(4, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
        assertEquals(50, requestNode.get("percentile").asDouble(), 0);
    }

    @Test
    public void testSum()  throws Exception {
        setMockResponse(200, "{\"result\": 65}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        double result = queryClient.sum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, null);
        assertEquals(65, result, 0);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.SUM));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
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
        String requestString = outputStream.toString(ENCODING);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, requestNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());

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
        List<IntervalResultValue> resultList = interval.getIntervalResults();

        for (IntervalResultValue intervalResultValue : resultList) {
            AbsoluteTimeframe firstInterval = intervalResultValue.getTimeframe();
            assertNotNull(firstInterval);
            assertNotNull(firstInterval.getStart());
            assertNotNull(firstInterval.getEnd());
            assertTrue(intervalResultValue.getResult().isLong());
        }
    }

    @Test
    public void testGroupByIntervalResponse() throws Exception {
        setMockResponse(200, "{\"result\": [{\"value\": [{\"keen.id\": \"555196212fd4b12ed0fe00a6\", \"click-number\": 1, \"result\": 0}, {\"keen.id\": \"5553c694c2266c32a3bab9ae\", \"click-number\": 1, \"result\": 0}], \"timeframe\": {\"start\": \"2015-01-01T00:00:00.000Z\", \"end\": \"2015-01-04T00:00:00.000Z\"}} ]}");

        // GROUP BY & INTERVAL
        Query params = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withGroupBy(TEST_TARGET_PROPERTY)
                .withGroupBy("keen.id")
                .withInterval("weekly")
                .withTimeframe(new RelativeTimeframe("this_year"))
                .build();

        QueryResult result = queryClient.execute(params);

        assertTrue(result.isIntervalResult());
        IntervalResult interval = (IntervalResult) result;
        List<IntervalResultValue> resultList = interval.getIntervalResults();

        // interval
        AbsoluteTimeframe firstInterval = resultList.get(0).getTimeframe();
        assertNotNull(firstInterval);
        assertNotNull(firstInterval.getStart());
        assertNotNull(firstInterval.getEnd());

        // group-by
        QueryResult queryResult = resultList.get(0).getResult();
        assertTrue(queryResult.isGroupResult());
        GroupByResult groupBy = (GroupByResult) queryResult;
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
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(2, requestNode.size());
        ArrayNode filtersNode = (ArrayNode) requestNode.get("filters");
        ObjectNode filter1Node = (ObjectNode) filtersNode.get(0);
        ObjectNode filter2Node = (ObjectNode) filtersNode.get(1);
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals(TEST_TARGET_PROPERTY, filter1Node.get("property_name").asText());
        assertEquals("lt", filter1Node.get("operator").asText());
        assertEquals(5, filter1Node.get("property_value").asInt());
        assertEquals(TEST_TARGET_PROPERTY, filter2Node.get("property_name").asText());
        assertEquals("gt", filter2Node.get("operator").asText());
        assertEquals(1, filter2Node.get("property_value").asInt());
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
    public void testBuilderGetFilters() throws Exception {
        
        Query.Builder builder = new Query.Builder(QueryType.COUNT)
                    .withEventCollection(TEST_EVENT_COLLECTION)
                    .withFilter(TEST_TARGET_PROPERTY, FilterOperator.LESS_THAN, 5)
                    .withFilter(TEST_TARGET_PROPERTY, FilterOperator.GREATER_THAN, 1);
        
        List<Map<String, Object>> filters = builder.getFilters();
        
        ensureFilterMapHasExpectedPropertyValues(
                filters.get(0),
                5,
                TEST_TARGET_PROPERTY,
                FilterOperator.LESS_THAN.toString());
        ensureFilterMapHasExpectedPropertyValues(
                filters.get(1),
                1,
                TEST_TARGET_PROPERTY,
                FilterOperator.GREATER_THAN.toString());
    }
    
    private void ensureFilterMapHasExpectedPropertyValues(
            Map<String, Object> filterMap,
            int propertyValue,
            String propertyName,
            String operatorValue
            ) {
        
        assertEquals(filterMap.get(KeenQueryConstants.PROPERTY_VALUE), propertyValue);
        assertEquals(filterMap.get(KeenQueryConstants.PROPERTY_NAME), propertyName);
        assertEquals(filterMap.get(KeenQueryConstants.OPERATOR), operatorValue);
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

        JsonNode requestRootNode = OBJECT_MAPPER.readTree(requestString);

        assertTrue(requestRootNode.has(KeenQueryConstants.TIMEFRAME));
        JsonNode timeframeNode = requestRootNode.get(KeenQueryConstants.TIMEFRAME);

        assertTrue(timeframeNode.has(KeenQueryConstants.START));
        JsonNode startTimeNode = timeframeNode.get(KeenQueryConstants.START);
        assertEquals(startTime, startTimeNode.asText());

        assertTrue(timeframeNode.has(KeenQueryConstants.END));
        JsonNode endTimeNode = timeframeNode.get(KeenQueryConstants.END);
        assertEquals(endTime, endTimeNode.asText());

        assertTrue(requestRootNode.has(KeenQueryConstants.EVENT_COLLECTION));
        JsonNode eventCollectionNode = requestRootNode.get(KeenQueryConstants.EVENT_COLLECTION);
        assertEquals(TEST_EVENT_COLLECTION, eventCollectionNode.asText());
    }

    @Test
    public void testRelativeTimeframe() throws Exception {
        setMockResponse(200, "{\"result\": 2}");
        String timeframeText = "this_month";
        Timeframe timeframe = new RelativeTimeframe(timeframeText);
        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(timeframe)
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);

        JsonNode requestRootNode = OBJECT_MAPPER.readTree(requestString);

        assertTrue(requestRootNode.has(KeenQueryConstants.TIMEFRAME));
        JsonNode timeframeNode = requestRootNode.get(KeenQueryConstants.TIMEFRAME);
        assertEquals(timeframeText, timeframeNode.asText());

        assertTrue(requestRootNode.has(KeenQueryConstants.EVENT_COLLECTION));
        JsonNode eventCollectionNode = requestRootNode.get(KeenQueryConstants.EVENT_COLLECTION);
        assertEquals(TEST_EVENT_COLLECTION, eventCollectionNode.asText());
    }

    @Test
    public void testRelativeTimeframeWithTimezone() throws Exception {
        setMockResponse(200, "{\"result\": 2}");
        String timeframeText = "this_month";
        String timezoneText = "UTC";
        Timeframe timeframe = new RelativeTimeframe(timeframeText, timezoneText);
        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(timeframe)
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);

        JsonNode requestRootNode = OBJECT_MAPPER.readTree(requestString);

        assertTrue(requestRootNode.has(KeenQueryConstants.TIMEFRAME));
        JsonNode timeframeNode = requestRootNode.get(KeenQueryConstants.TIMEFRAME);
        assertEquals(timeframeText, timeframeNode.asText());

        assertTrue(requestRootNode.has(KeenQueryConstants.TIMEZONE));
        JsonNode timezoneNode = requestRootNode.get(KeenQueryConstants.TIMEZONE);
        assertEquals(timezoneText, timezoneNode.asText());

        assertTrue(requestRootNode.has(KeenQueryConstants.EVENT_COLLECTION));
        JsonNode eventCollectionNode = requestRootNode.get(KeenQueryConstants.EVENT_COLLECTION);
        assertEquals(TEST_EVENT_COLLECTION, eventCollectionNode.asText());
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
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(3, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals("weekly", requestNode.get(KeenQueryConstants.INTERVAL).asText());
        assertEquals("this_month", requestNode.get(KeenQueryConstants.TIMEFRAME).asText());
    }

    @Test
    public void testTimezone() throws Exception {
        setMockResponse(200, "{\"result\": 2}");
        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_month", "UTC"))
                .build();
        String requestString = mockCaptureCountQueryRequest(queryParams);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);
        assertEquals(3, requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION, requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals("UTC", requestNode.get(KeenQueryConstants.TIMEZONE).asText());
        assertEquals("this_month", requestNode.get(KeenQueryConstants.TIMEFRAME).asText());
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

    @Test
    public void testNullResult() throws Exception {
        setMockResponse(200, "{\"result\": null}");

        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(TEST_EVENT_COLLECTION)
                .build();
        mockCaptureCountQueryRequest(queryParams);
    }

    private FilterOperator stringToFilterOperator(String operator) {
        FilterOperator result = null;

        if (0 == operator.compareTo("eq")) {
            result = FilterOperator.EQUAL_TO;
        } else {
            throw new IllegalStateException("Unimplemented string to FilterOperator value");
        }

        return result;
    }

    private List<FunnelStep> buildFunnelStepsFromRequestJson(JsonNode requestJson) {

        JsonNode stepsJson = requestJson.findValue("steps");

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = new ArrayList<FunnelStep>();
        Iterator<JsonNode> stepsIterator = stepsJson.iterator();
        while (stepsIterator.hasNext()) {
            JsonNode stepJson = stepsIterator.next();

            Timeframe timeframe = null;
            List<Filter> filters = null;
            Boolean inverted = null;
            Boolean optional = null;
            Boolean withActors = null;

            if (stepJson.has(KeenQueryConstants.TIMEFRAME) &&
                stepJson.has(KeenQueryConstants.TIMEZONE)) {
                timeframe = new RelativeTimeframe(
                        stepJson.get(KeenQueryConstants.TIMEFRAME).asText(),
                        stepJson.get(KeenQueryConstants.TIMEZONE).asText());
            } else if(stepJson.has(KeenQueryConstants.TIMEFRAME)) {
                JsonNode timeframeJson = stepJson.get(KeenQueryConstants.TIMEFRAME);
                if (!timeframeJson.isObject()) {
                    timeframe = new RelativeTimeframe(timeframeJson.asText());
                } else {
                    throw new IllegalStateException(
                            "Building absolute timeframes isn't supported by this method.");
                }
            }

            if (stepJson.has(KeenQueryConstants.FILTERS)) {
                JsonNode filterListJson = stepJson.get(KeenQueryConstants.FILTERS);
                Iterator<JsonNode> filterJsonIterator = filterListJson.iterator();
                while (filterJsonIterator.hasNext()) {
                    JsonNode filterJson = filterJsonIterator.next();
                    if (null == filters) {
                        filters = new LinkedList<Filter>();
                    }
                    filters.add(
                        new Filter(
                            filterJson.get(KeenQueryConstants.PROPERTY_NAME).asText(),
                            stringToFilterOperator(filterJson.get(KeenQueryConstants.OPERATOR).asText()),
                            filterJson.get(KeenQueryConstants.PROPERTY_VALUE).asText()
                        )
                    );
                }
            }

            if (stepJson.has(KeenQueryConstants.INVERTED)) {
                inverted = stepJson.get(KeenQueryConstants.INVERTED).asBoolean();
            }

            if (stepJson.has(KeenQueryConstants.OPTIONAL)) {
                optional = stepJson.get(KeenQueryConstants.OPTIONAL).asBoolean();
            }

            if (stepJson.has(KeenQueryConstants.WITH_ACTORS)) {
                withActors = stepJson.get(KeenQueryConstants.WITH_ACTORS).asBoolean();
            }

            FunnelStep step = new FunnelStep(
                    stepJson.get(KeenQueryConstants.EVENT_COLLECTION).asText(),
                    stepJson.get(KeenQueryConstants.ACTOR_PROPERTY).asText(),
                    timeframe,
                    filters,
                    inverted,
                    optional,
                    withActors
            );

            funnelSteps.add(step);
        }

        return funnelSteps;
    }

    @Test
    public void testFunnelWithOnlyRequiredParameters() throws Exception {

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
            "{\"steps\":[{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
          + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
          + "{\"timeframe\":\"this_7_days\",\"timezone\":\"UTC\",\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                  "{\"result\": [3,1,0],\"steps\":"
                + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }

    @Test
    public void testFunnelBuilderNonFluent() throws Exception {

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
            "{\"steps\":[{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
          + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
          + "{\"timeframe\":\"this_7_days\",\"timezone\":\"UTC\",\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                  "{\"result\": [3,1,0],\"steps\":"
                + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel.Builder builder = new Funnel.Builder();
        builder.setSteps(funnelSteps);
        Funnel funnel = builder.build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }
    
    @Test
    public void testFunnelWithOnlyRootTimeframe() throws Exception {

        String rootTimeframeString = "this_7_days";

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
            "{\"timeframe\":\"" + rootTimeframeString + "\",\"steps\":["
          + "{\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
          + "{\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
          + "{\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                  "{\"result\": [3,1,0],\"timeframe\":\"" + rootTimeframeString + "\",\"steps\":"
                + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withTimeframe(new RelativeTimeframe(rootTimeframeString))
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }

    @Test
    public void testFunnelBuilderEnsureWithStepsThrowsWithWrongUsage() throws Exception {

        String rootTimeframeString = "this_7_days";

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
            "{\"timeframe\":\"" + rootTimeframeString + "\",\"steps\":["
          + "{\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\"},"
          + "{\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\"},"
          + "{\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        boolean threwCorrectExceptionType = false;
        try {
            Funnel funnel = new Funnel.Builder()
                    .withStep(funnelSteps.get(0))
                    .withSteps(funnelSteps)
                    .build();
            fail("Expected KeenQueryClientException with incorrect usage of withStep" +
                 "combined with withSteps.");
        } catch (KeenQueryClientException keenException) {
            threwCorrectExceptionType = true;
        }
        assertTrue(threwCorrectExceptionType);
    }

    @Test
    public void testFunnelWithSpecialParameters() throws Exception {

        List<String> actorValues = new ArrayList<String>();
        actorValues.add("f9332409s0");
        actorValues.add("b7732409s0");
        actorValues.add("k22315b211");

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
            "{\"steps\":["
          + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\",\"event_collection\":\"signed up\",\"with_actors\":true},"
          + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\",\"event_collection\":\"completed profile\",\"inverted\":true},"
          + "{\"timeframe\":\"this_7_days\",\"timezone\":\"UTC\",\"optional\":true,\"actor_property\":\"user.guid\",\"event_collection\":\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                  "{\"result\": [3,2,1],"
                + "\"actors\": [" + OBJECT_MAPPER.writeValueAsString(actorValues) + ", null, null],"
                + "\"steps\":" + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 2 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(2).longValue());
        ListResult actorResult = funnelResult.getActorsResult();
        List<QueryResult> actorResultList = actorResult.getListResults();
        List<QueryResult> firstStepActorList = actorResultList.get(0).getListResults();
        assertTrue("Unexpected actor value.", 0 == firstStepActorList.get(0).stringValue().compareTo(actorValues.get(0)));
        assertTrue("Unexpected actor value.", 0 == firstStepActorList.get(1).stringValue().compareTo(actorValues.get(1)));
        assertTrue("Unexpected actor value.", 0 == firstStepActorList.get(2).stringValue().compareTo(actorValues.get(2)));
        assertTrue("Unexpected actor result.", null == actorResultList.get(1));
        assertTrue("Unexpected actor result.", null == actorResultList.get(2));
    }

    @Test
    public void testFunnelWithFilters() throws Exception {

        JsonNode expectedRequest = OBJECT_MAPPER.readTree(
                "{\"steps\":[{\"timeframe\":\"this_7_days\",\"actor_property\":\"visitor.guid\","
              + "\"filters\":[{\"property_value\":\"some_value\",\"operator\":\"eq\","
              + "\"property_name\":\"some_name\"}],\"event_collection\":\"signed up\"},"
              + "{\"timeframe\":\"this_7_days\",\"actor_property\":\"user.guid\","
              + "\"event_collection\":\"completed profile\"},{\"timeframe\":\"this_7_days\","
              + "\"timezone\":\"UTC\",\"actor_property\":\"user.guid\",\"event_collection\":"
              + "\"referred user\"}]}"
        );

        // Build the mock response based on provided data
        String mockResponse =
                  "{\"result\": [3,1,0],\"steps\":"
                + OBJECT_MAPPER.writeValueAsString(expectedRequest.findValue("steps"))
                + "}";
        setMockResponse(200, mockResponse);

        // Construct a list of funnel steps based on provided data
        List<FunnelStep> funnelSteps = buildFunnelStepsFromRequestJson(expectedRequest);

        Funnel funnel = new Funnel.Builder()
                .withStep(funnelSteps.get(0))
                .withStep(funnelSteps.get(1))
                .withStep(funnelSteps.get(2))
                .build();

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        QueryResult result = queryClient.execute(funnel);
        assertTrue(result instanceof FunnelResult);
        FunnelResult funnelResult = (FunnelResult)result;

        // Capture the request for validation
        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestBody = outputStream.toString(ENCODING);

        // Validate request
        JsonNode requestNode = OBJECT_MAPPER.readTree(requestBody);
        assertTrue(expectedRequest.equals(requestNode));

        // Validate result
        ListResult funnelValues = funnelResult.getFunnelResult();
        List<QueryResult> funnelResultData = funnelValues.getListResults();
        assertTrue("Unexpected result value.", 3 == funnelResultData.get(0).longValue());
        assertTrue("Unexpected result value.", 1 == funnelResultData.get(1).longValue());
        assertTrue("Unexpected result value.", 0 == funnelResultData.get(2).longValue());
        assertTrue("Should not have actors result.", null == funnelResult.getActorsResult());
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testFunnelWithInvalidTimeframeConfiguration() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Funnel funnel = new Funnel.Builder()
                .withStep(new FunnelStep("signed up", "visitor.guid"))
                .withStep(new FunnelStep("completed profile", "user.guid"))
                .withStep(new FunnelStep("referred user", "user.guid"))
                .build();
    }

    @Test
    public void testFunnelWithInvalidInvertedSpecialParameter() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Funnel funnel = new Funnel.Builder()
                .withStep(new FunnelStep("signed up", "visitor.guid", new RelativeTimeframe("this_7_days"), null, true, null, null))
                .withStep(new FunnelStep("completed profile", "user.guid", new RelativeTimeframe("this_7_days")))
                .withStep(new FunnelStep("referred user", "user.guid", new RelativeTimeframe("this_7_days", "UTC")))
                .build();
    }

    @Test
    public void testFunnelWithInvalidOptionalSpecialParameter() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Funnel funnel = new Funnel.Builder()
                .withStep(new FunnelStep("signed up", "visitor.guid", new RelativeTimeframe("this_7_days"), null, null, true, null))
                .withStep(new FunnelStep("completed profile", "user.guid", new RelativeTimeframe("this_7_days")))
                .withStep(new FunnelStep("referred user", "user.guid", new RelativeTimeframe("this_7_days", "UTC")))
                .build();
    }

    private static void validateMultiAnalysisRequiredFields(ObjectNode requestNode) {
        // Should have 'event_collection', 'analyses' and 'timeframe' top-level keys, at least.
        assertTrue("Missing required top-level fields.", 3 <= requestNode.size());
        assertEquals(TEST_EVENT_COLLECTION,
                requestNode.get(KeenQueryConstants.EVENT_COLLECTION).asText());
        assertEquals("this_8_hours", requestNode.get(KeenQueryConstants.TIMEFRAME).asText());
    }

    private ObjectNode getMultiAnalysisRequestNode(MultiAnalysis multiAnalysisParams)
            throws Exception {
        String requestString = mockCaptureCountQueryRequest(multiAnalysisParams);
        ObjectNode requestNode = (ObjectNode) OBJECT_MAPPER.readTree(requestString);

        validateMultiAnalysisRequiredFields(requestNode);

        return requestNode;
    }

    @Test
    public void testMultiAnalysis_Simple() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": {" +
                        "\"plain_old_count\": 24" +
                    "}" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses' and 'timeframe' top-level keys.
        assertEquals(3, requestNode.size());

        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(1, analysesNode.size());
        ObjectNode countNode = (ObjectNode)analysesNode.get("plain_old_count");
        assertEquals(1, countNode.size());
        assertNull("There should be no 'target_property' for 'count' analysis.",
                countNode.get(KeenQueryConstants.TARGET_PROPERTY));
        assertEquals(KeenQueryConstants.COUNT,
                countNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());
    }

    private static final float DOUBLE_CMP_DELTA = 0.0f;

    @Test
    public void testMultiAnalysis_Percentile() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": {" +
                        "\"the_average\": 53.768923," +
                        "\"categories\": 3," +
                        "\"maybe_percentile\": 27," +
                        "\"plain_old_count\": 24," +
                        "\"the_total\": 1235" +
                    "}" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis(
                        "the_average",
                        QueryType.AVERAGE,
                        TEST_TARGET_PROPERTY))
                .withSubAnalysis(new SubAnalysis("categories", QueryType.COUNT_UNIQUE, "category"))
                .withSubAnalysis(new SubAnalysis("maybe_percentile",
                        QueryType.PERCENTILE,
                        TEST_TARGET_PROPERTY,
                        Percentile.createStrict(30.0)))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("the_total", QueryType.SUM, TEST_TARGET_PROPERTY))
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses' and 'timeframe' top-level keys
        assertEquals(3, requestNode.size());

        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(5, analysesNode.size());

        ObjectNode averageNode = (ObjectNode)analysesNode.get("the_average");
        assertEquals(2, averageNode.size());
        assertEquals(KeenQueryConstants.AVERAGE,
                averageNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());
        assertEquals(TEST_TARGET_PROPERTY,
                averageNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());

        ObjectNode percentileNode = (ObjectNode)analysesNode.get("maybe_percentile");
        assertEquals(3, percentileNode.size());
        assertEquals(KeenQueryConstants.PERCENTILE,
                percentileNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());
        assertEquals(TEST_TARGET_PROPERTY,
                percentileNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
        assertEquals(30.0,
                percentileNode.get(KeenQueryConstants.PERCENTILE).asDouble(), DOUBLE_CMP_DELTA);
    }

    @Test
    public void testMultiAnalysis_GroupBy() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"category\": \"first\"," +
                        "\"plain_old_count\": 17" +
                    "}, {" +
                        "\"category\": \"second\"," +
                        "\"plain_old_count\": 31" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withGroupBy("category")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe' and 'group_by' top-level keys
        assertEquals(4, requestNode.size());

        // Make sure the 'count' sub-analysis is still there even though we specified 'group_by'...
        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(1, analysesNode.size());
        ObjectNode countNode = (ObjectNode)analysesNode.get("plain_old_count");
        assertEquals(1, countNode.size());
        assertNull("There should be no 'target_property' for 'count' analysis.",
                countNode.get(KeenQueryConstants.TARGET_PROPERTY));
        assertEquals(KeenQueryConstants.COUNT,
                countNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());

        // ...but also make sure the 'group_by' is there now.
        // We always JSONify a collection for groupBy, so this should be an array.
        ArrayNode groupByNode = (ArrayNode)requestNode.get(KeenQueryConstants.GROUP_BY);
        assertEquals(1, groupByNode.size()); // Only one 'group_by' parameter here
        assertEquals("category", groupByNode.get(0).asText());
    }

    @Test
    public void testMultiAnalysis_MultipleGroupBy() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"category\": \"first\"," +
                        "\"plain_old_count\": 17," +
                        "\"style\": \"style1\"" +
                    "}, {" +
                        "\"category\": \"second\"," +
                        "\"plain_old_count\": 31," +
                        "\"style\": \"style2\"" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe' and 'group_by' top-level keys
        assertEquals(4, requestNode.size());

        // Validate the 'group_by' is there now with two parameters.
        // We always JSONify a collection for groupBy, so this should be an array.
        ArrayNode groupByNode = (ArrayNode)requestNode.get(KeenQueryConstants.GROUP_BY);
        assertEquals(2, groupByNode.size()); // Should have two 'group_by' parameters here

        List<String> groupByParams = new ArrayList<String>(2);
        Collections.addAll(groupByParams, "category", "style");

        for (JsonNode groupByParam : groupByNode) {
            assertTrue(groupByParams.remove(groupByParam.asText()));
        }

        assertTrue(groupByParams.isEmpty());
    }

    @Test
    public void testMultiAnalysis_Interval() throws Exception {
        // Response doesn't really matter here, but this is what it'd look like.
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-22T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-21T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 17" +
                        "}" +
                    "}, {" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-23T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-22T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 31" +
                        "}" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withInterval("daily")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe' and 'interval' top-level keys
        assertEquals(4, requestNode.size());

        // Make sure the 'count' sub-analysis is still there even though we specified 'group_by'...
        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(1, analysesNode.size());
        ObjectNode countNode = (ObjectNode)analysesNode.get("plain_old_count");
        assertEquals(1, countNode.size());
        assertNull("There should be no 'target_property' for 'count' analysis.",
                countNode.get(KeenQueryConstants.TARGET_PROPERTY));
        assertEquals(KeenQueryConstants.COUNT,
                countNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());

        // ...but also make sure the 'interval' is there now. It should just be a string.
        assertEquals("daily", requestNode.get(KeenQueryConstants.INTERVAL).asText());
    }

    @Test
    public void testMultiAnalysis_GroupAndInterval() throws Exception {
        // Response doesn't really matter here
        setMockResponse(200, "{\"result\": \"much stuff\"}");

        // This analysis has two sub-analyses, an interval, and two group by parameters.
        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("total_cost", QueryType.SUM, "price"))
                .withInterval("daily")
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        ObjectNode requestNode = getMultiAnalysisRequestNode(multiAnalysisParams);

        // Should have 'event_collection', 'analyses', 'timeframe', 'interval' and 'group_by'
        // top-level keys.
        assertEquals(5, requestNode.size());

        // Make sure the 'sum' sub-analysis is still there even though we specified 'group_by' and
        // 'interval'.
        ObjectNode analysesNode = (ObjectNode)requestNode.get(KeenQueryConstants.ANALYSES);
        assertEquals(2, analysesNode.size());
        ObjectNode totalNode = (ObjectNode)analysesNode.get("total_cost");
        assertEquals(2, totalNode.size());
        assertEquals("price", totalNode.get(KeenQueryConstants.TARGET_PROPERTY).asText());
        assertEquals(KeenQueryConstants.SUM,
                totalNode.get(KeenQueryConstants.ANALYSIS_TYPE).asText());

        // ...but also make sure the 'interval' is there now. It should just be a string.
        assertEquals("daily", requestNode.get(KeenQueryConstants.INTERVAL).asText());

        // Validate the two 'group_by' parameters.
        ArrayNode groupByNode = (ArrayNode)requestNode.get(KeenQueryConstants.GROUP_BY);
        assertEquals(2, groupByNode.size()); // Should have two 'group_by' parameters here

        List<String> groupByParams = new ArrayList<String>(2);
        Collections.addAll(groupByParams, "category", "style");

        for (JsonNode groupByParam : groupByNode) {
            assertTrue(groupByParams.remove(groupByParam.asText()));
        }

        assertTrue(groupByParams.isEmpty());
    }

    @Test
    public void testMultiAnalysisResponse_Simple() throws Exception {
        setMockResponse(200, "{" +
                    "\"result\": {" +
                        "\"plain_old_count\": 24" +
                    "}" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result instanceof MultiAnalysisResult);
        MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)result;
        assertEquals(1, multiAnalysisResult.getAllResults().size());
        assertEquals(24, multiAnalysisResult.getResultFor("plain_old_count").longValue());
    }

    @Test
    public void testMultiAnalysisResponse_GroupBy() throws Exception {
        final List<String> categories = Arrays.asList("first", "second");
        final String groupBy = "category";

        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"" + groupBy + "\": \"" + categories.get(0) + "\"," +
                        "\"plain_old_count\": 17" +
                    "}, {" +
                        "\"" + groupBy + "\": \"" + categories.get(1) + "\"," +
                        "\"plain_old_count\": 31" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withGroupBy("category")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isGroupResult());

        for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
            // Validate the GroupByResult
            Group group = groupResult.getKey();
            assertEquals(1, group.getProperties().size());
            assertTrue(group.getPropertyNames().contains(groupBy));
            assertTrue(categories.contains(group.getGroupValue(groupBy)));
            assertTrue(groupResult.getValue() instanceof MultiAnalysisResult);

            // Validate the actual MultiAnalysisResult
            MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)groupResult.getValue();
            assertEquals(1, multiAnalysisResult.getAllResults().size());
            long resultForThisGroup =
                    multiAnalysisResult.getResultFor("plain_old_count").longValue();

            if (categories.get(0).equals(group.getGroupValue(groupBy))) {
                assertEquals(17, resultForThisGroup);
            } else {
                assertEquals(31, resultForThisGroup);
            }
        }
    }

    @Test
    public void testMultiAnalysisResponse_MultipleGroupBy() throws Exception {
        final List<String> categories = Arrays.asList("first", "second");
        final List<String> styles = Arrays.asList("style1", "style2", "style3");
        final String groupBy1 = "category";
        final String groupBy2 = "style";

        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"" + groupBy1 + "\": \"" + categories.get(0) + "\"," +
                        "\"plain_old_count\": 17," +
                        "\"" + groupBy2 + "\": \"" + styles.get(0) + "\"," +
                        "\"total_cost\": 143.45" +
                    "}, {" +
                        "\"" + groupBy1 + "\": \"" + categories.get(1) + "\"," +
                        "\"plain_old_count\": 31," +
                        "\"" + groupBy2 + "\": \"" + styles.get(1) + "\"," +
                        "\"total_cost\": 233.0 " +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("total_cost", QueryType.SUM, "price"))
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isGroupResult());
        assertEquals(2, result.getGroupResults().size()); // There are two groups.

        for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
            // Validate the GroupByResult
            Group group = groupResult.getKey();
            assertEquals(2, group.getProperties().size());

            assertTrue(group.getPropertyNames().contains(groupBy1));
            assertTrue(categories.contains(group.getGroupValue(groupBy1)));
            assertTrue(group.getPropertyNames().contains(groupBy2));
            assertTrue(styles.contains(group.getGroupValue(groupBy2)));

            assertTrue(groupResult.getValue() instanceof MultiAnalysisResult);

            // Validate the actual MultiAnalysisResult
            MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)groupResult.getValue();
            assertEquals(2, multiAnalysisResult.getAllResults().size());
            long countForThisGroup =
                    multiAnalysisResult.getResultFor("plain_old_count").longValue();

            double totalForThisGroup =
                    multiAnalysisResult.getResultFor("total_cost").doubleValue();

            if (categories.get(0).equals(group.getGroupValue(groupBy1))) {
                assertEquals(17, countForThisGroup);
                assertEquals(143.45, totalForThisGroup, 0.0);
            } else {
                assertEquals(31, countForThisGroup);
                assertEquals(233, totalForThisGroup, 0.0);
            }
        }
    }

    @Test
    public void testMultiAnalysisResponse_Interval() throws Exception {
        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-22T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-21T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 17" +
                        "}" +
                    "}, {" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-23T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-22T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": {" +
                            "\"plain_old_count\": 31" +
                        "}" +
                    "}]" +
                "}");

        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withInterval("daily")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isIntervalResult());
        assertEquals(2, result.getIntervalResults().size()); // There are two intervals.
        int intervalNum = 0;

        for (IntervalResultValue value : result.getIntervalResults()) {
            // Do some simple validation of the timeframe.
            AbsoluteTimeframe timeframe = value.getTimeframe();
            Calendar start = DatatypeConverter.parseDateTime(timeframe.getStart());
            Calendar end = DatatypeConverter.parseDateTime(timeframe.getEnd());
            assertEquals("October", start.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.US));
            assertEquals(2016, end.get(Calendar.YEAR));
            assertTrue(start.before(end));

            // Validate the QueryResult
            assertTrue(value.getResult() instanceof MultiAnalysisResult);
            MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)value.getResult();

            assertEquals(1, multiAnalysisResult.getAllResults().size());
            long countForThisInterval =
                    multiAnalysisResult.getResultFor("plain_old_count").longValue();

            // Intervals, unlike groups and results for sub-analyses, actually come back in order,
            // or at least it seems like they do.
            if (0 == intervalNum) {
                assertEquals(17, countForThisInterval);
            } else if (1 == intervalNum) {
                assertEquals(31, countForThisInterval);
            } else {
                fail("More intervals than expected.");
            }

            intervalNum++;
        }
    }

    @Test
    public void testMultiAnalysisResponse_GroupAndInterval() throws Exception {
        final List<String> categories = Arrays.asList("first", "second");
        final List<String> styles = Arrays.asList("style1", "style2", "style3");
        final String groupBy1 = "category";
        final String groupBy2 = "style";

        setMockResponse(200, "{" +
                    "\"result\": [{" +
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-22T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-21T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": [{" +
                            "\"" + groupBy1 + "\": \"" + categories.get(0) + "\"," +
                            "\"plain_old_count\": 17," +
                            "\"" + groupBy2 + "\": \"" + styles.get(0) + "\"," +
                            "\"total_cost\": 143.45" +
                        "}, {" +
                            "\"" + groupBy1 + "\": \"" + categories.get(1) + "\"," +
                            "\"plain_old_count\": 31," +
                            "\"" + groupBy2 + "\": \"" + styles.get(1) + "\"," +
                            "\"total_cost\": 233.0" +
                        "}]" +
                    "}, {" + // End of first IntervalResult which holds two GroupByResults
                        "\"timeframe\": {" +
                            "\"end\": \"2016-10-23T00:00:00.000Z\"," +
                            "\"start\": \"2016-10-22T00:00:00.000Z\"" +
                        "}," +
                        "\"value\": [{" +
                            "\"" + groupBy1 + "\": \"" + categories.get(0) + "\"," +
                            "\"plain_old_count\": 18," +
                            "\"" + groupBy2 + "\": \"" + styles.get(0) + "\"," +
                            "\"total_cost\": 144.45" +
                        "}, {" +
                            "\"" + groupBy1 + "\": \"" + categories.get(1) + "\"," +
                            "\"plain_old_count\": 32," +
                            "\"" + groupBy2 + "\": \"" + styles.get(1) + "\"," +
                            "\"total_cost\": 234.0" +
                        "}]" +
                    "}]" +
                "}");

        // This analysis has two sub-analyses, an interval, and two group by parameters.
        MultiAnalysis multiAnalysisParams = new MultiAnalysis.Builder()
                .withEventCollection(TEST_EVENT_COLLECTION)
                .withTimeframe(new RelativeTimeframe("this_8_hours"))
                .withSubAnalysis(new SubAnalysis("plain_old_count", QueryType.COUNT))
                .withSubAnalysis(new SubAnalysis("total_cost", QueryType.SUM, "price"))
                .withInterval("daily")
                .withGroupBy("category")
                .withGroupBy("style")
                .build();

        QueryResult result = queryClient.execute(multiAnalysisParams);

        assertTrue(result.isIntervalResult());
        assertEquals(2, result.getIntervalResults().size()); // Two intervals
        int intervalNum = 0;

        // A lot of this code is shared across the GroupBy, MultipleGroupBy, Interval and
        // GroupAndInterval tests, and could be combined into helpers, especially as we go to add
        // more tests.
        for (IntervalResultValue value : result.getIntervalResults()) {
            // Do some simple validation of the timeframe.
            AbsoluteTimeframe timeframe = value.getTimeframe();
            Calendar start = DatatypeConverter.parseDateTime(timeframe.getStart());
            Calendar end = DatatypeConverter.parseDateTime(timeframe.getEnd());
            assertEquals("October", start.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.US));
            assertEquals(2016, end.get(Calendar.YEAR));
            assertTrue(start.before(end));

            // Validate the nested GroupResults

            QueryResult groupResultInInterval = value.getResult();
            assertTrue(groupResultInInterval.isGroupResult());
            assertEquals(2, groupResultInInterval.getGroupResults().size()); // Two groups

            for (Map.Entry<Group, QueryResult> groupResult
                    : groupResultInInterval.getGroupResults().entrySet()) {
                // Validate each nested GroupByResult
                Group group = groupResult.getKey();
                assertEquals(2, group.getProperties().size());

                assertTrue(group.getPropertyNames().contains(groupBy1));
                assertTrue(categories.contains(group.getGroupValue(groupBy1)));
                assertTrue(group.getPropertyNames().contains(groupBy2));
                assertTrue(styles.contains(group.getGroupValue(groupBy2)));

                assertTrue(groupResult.getValue() instanceof MultiAnalysisResult);
                MultiAnalysisResult multiAnalysisResult =
                        (MultiAnalysisResult)groupResult.getValue();
                assertEquals(2, multiAnalysisResult.getAllResults().size());

                long countForThisGroup =
                        multiAnalysisResult.getResultFor("plain_old_count").longValue();

                double totalForThisGroup =
                        multiAnalysisResult.getResultFor("total_cost").doubleValue();

                if (categories.get(0).equals(group.getGroupValue(groupBy1))) {
                    assertEquals(17 + intervalNum, countForThisGroup);
                    assertEquals(143.45 + intervalNum, totalForThisGroup, 0.0);
                } else {
                    assertEquals(31 + intervalNum, countForThisGroup);
                    assertEquals(233 + intervalNum, totalForThisGroup, 0.0);
                }
            }

            intervalNum++;
        }
    }

    @Test
    public void testPercentileStrict_NormalValues() {
        double lowNormal = 0.05;
        Percentile p1 = Percentile.createStrict(lowNormal);
        assertEquals(lowNormal, p1.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        double mediumNormal = 55.43;
        Percentile p2 = Percentile.createStrict(mediumNormal);
        assertEquals(mediumNormal, p2.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        double highNormal = 97d;
        Percentile p3 = Percentile.createStrict(highNormal);
        assertEquals(highNormal, p3.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Integer normalInt = 33;
        Percentile p4 = Percentile.createStrict(normalInt);
        assertEquals(normalInt.intValue(), p4.asDouble().intValue());

        Long normalLong = 25L;
        Percentile p6 = Percentile.createStrict(normalLong);
        assertEquals(normalLong.longValue(), p6.asDouble().longValue());

        // Trailing zeroes shouldn't count as more decimal places.
        Percentile p7 = Percentile.createStrict(99.92000);
        assertEquals(99.92, p7.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        // Scientific notation here shouldn't affect things.
        Percentile p8 = Percentile.createStrict(5.234e1);
        assertEquals(52.34, p8.asDouble().doubleValue(), DOUBLE_CMP_DELTA);
    }

    @Test
    public void testPercentileStrict_BoundaryValues() {
        double lowOk = 0.02;
        Percentile p1 = Percentile.createStrict(lowOk);
        assertEquals(lowOk, p1.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        double lowLimit = 0.01;
        Percentile p2 = Percentile.createStrict(lowLimit);
        assertEquals(lowLimit, p2.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        try {
            double lowBad = 0.00;
            Percentile p3 = Percentile.createStrict(lowBad);
            fail("Expected IllegalArgumentException creating Percentile out of range.");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(),
                    allOf(containsString("range"), containsString("(0, 100]")));
        }

        double highOk = 99.99;
        Percentile p4 = Percentile.createStrict(highOk);
        assertEquals(highOk, p4.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        double highLimit = 100.00;
        Percentile p5 = Percentile.createStrict(highLimit);
        assertEquals(highLimit, p5.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        try {
            double highBad = 100.01;
            Percentile p6 = Percentile.createStrict(highBad);
            fail("Expected IllegalArgumentException creating Percentile out of range.");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(),
                    allOf(containsString("range"), containsString("(0, 100]")));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPercentile_TooManyDecimalPlaces() {
        Percentile p5 = Percentile.createStrict(0.00 + Double.MIN_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPercentile_TooManyDecimalPlaces2() {
        Percentile p5 = Percentile.createStrict(0.01f);
    }

    @Test
    public void testPercentileCoerced_NormalValues() {
        double lowNormal = 0.05;
        Percentile p1 = Percentile.createCoerced(lowNormal);
        assertEquals(lowNormal, p1.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        double mediumNormal = 55.43;
        Percentile p2 = Percentile.createCoerced(mediumNormal);
        assertEquals(mediumNormal, p2.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        double highNormal = 97d;
        Percentile p3 = Percentile.createCoerced(highNormal);
        assertEquals(highNormal, p3.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Integer normalInt = 33;
        Percentile p4 = Percentile.createCoerced(normalInt);
        assertEquals(normalInt.intValue(), p4.asDouble().intValue());

        Long normalLong = 25L;
        Percentile p6 = Percentile.createCoerced(normalLong);
        assertEquals(normalLong.longValue(), p6.asDouble().longValue());

        // Trailing zeroes shouldn't count as more decimal places.
        Percentile p7 = Percentile.createCoerced(99.92000);
        assertEquals(99.92, p7.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        // Scientific notation here shouldn't affect things.
        Percentile p8 = Percentile.createCoerced(5.234e1);
        assertEquals(52.34, p8.asDouble().doubleValue(), DOUBLE_CMP_DELTA);
    }

    @Test
    public void testPercentileCoerced_CoercedValues()
    {
        final double MIN_PERCENTILE = 0.01;
        final double MAX_PERCENTILE = 100.00;

        Percentile p1 = Percentile.createCoerced(99.92345);
        assertEquals(99.92, p1.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p2 = Percentile.createCoerced(100.01);
        assertEquals(MAX_PERCENTILE, p2.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p3 = Percentile.createCoerced(0.00 + Double.MIN_VALUE);
        assertEquals(MIN_PERCENTILE, p3.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p4 = Percentile.createCoerced(-1454545.098545);
        assertEquals(MIN_PERCENTILE, p4.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p5 = Percentile.createCoerced(14569545.04557893);
        assertEquals(MAX_PERCENTILE, p5.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p6 = Percentile.createCoerced(57.755);
        assertEquals(57.76, p6.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p7 = Percentile.createCoerced(57.754);
        assertEquals(57.75, p7.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p8 = Percentile.createCoerced(51.450001);
        assertEquals(51.45, p8.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p9 = Percentile.createCoerced(0.01500);
        assertEquals(0.02, p9.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p10 = Percentile.createCoerced(5.23416782e1);
        assertEquals(52.34, p10.asDouble().doubleValue(), DOUBLE_CMP_DELTA);

        Percentile p11 = Percentile.createCoerced(5.23456782e1);
        assertEquals(52.35, p11.asDouble().doubleValue(), DOUBLE_CMP_DELTA);
    }

    private String mockCaptureCountQueryRequest(KeenQueryRequest inputParams) throws Exception {
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

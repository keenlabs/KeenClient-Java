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

import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.keen.client.java.KeenQueryParams.QueryParamBuilder;

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

//            List<Map<String, Object>> listSteps = new ArrayList<Map<String, Object>>();
//            Map<String, Object> steps = new HashMap<String, Object>();
//            steps.put("actor_property", "click-count");
//            steps.put("event_collection", TEST_EVENT_COLLECTION);
//
//            listSteps.add(steps);
//
//            Map<String, Object> result = queryClientTest.funnel(listSteps);

//            queryClientTest.extraction(TEST_EVENT_COLLECTION, "clairez@gmail.com");
//            assertNotNull(result.get("result"));

        List<Map<String, Object>> listSteps = new ArrayList<Map<String, Object>>();
        Map<String, Object> steps = new HashMap<String, Object>();
        steps.put(KeenQueryConstants.ACTOR_PROPERTY, "click-count");
        steps.put(KeenQueryConstants.EVENT_COLLECTION, TEST_EVENT_COLLECTION);

        listSteps.add(steps);

//        Map<String, Object> result = queryClientTest.funnel(listSteps);



    }


    // todo: remove this production test.
//    @Test
//    public void testRealQuery2() throws KeenException {
//        KeenProject queryProject = new KeenProject("555190333bc696371aaaebb0", "<write key>", "eee2b89b5dab28bb4a66dcb7d676387959b2c518f884c287edf40fd048335db7b9b7e0d9eb6572e9152a9f3d96f0e413398310ad97dc9433c3a9a3298944f942d5b85f989b36087db42795539ac321e84ca53592c2c99d45bfba64417070a037e9e765c1e8594c62f1f75b6ea794afa0");
//
//        KeenQueryClient queryClientTest = new TestKeenQueryClientBuilder(queryProject).build();
//        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
//                .withEventCollection("android-sample-button-clicks")
//                .build();
//        queryParams.addFilter("click-number", KeenQueryConstants.LESS_THAN, "5");
//        queryParams.addFilter("click-number", KeenQueryConstants.GREATER_THAN, "1");
//
//        try {
//            Map<String, Object> result = queryClientTest.count(queryParams);
//            assertNotNull(result.get("result"));
//        } catch (IOException e) {
//
//        }
//
//    }


    // TEST BASIC REQUIRED PARAMETERS
    @Test
    public void testCount()  throws Exception {
        setMockResponse(200, "{\"result\": 21}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.count(TEST_EVENT_COLLECTION);
        assertNotNull(result.get("result"));

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.COUNT_RESOURCE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String requestString = outputStream.toString(ENCODING);
        assertEquals(requestString, "{\"" + KeenQueryConstants.EVENT_COLLECTION + "\":\"" + TEST_EVENT_COLLECTION + "\"}");

//        "{event_collection : android-test-...}"

    }
    @Test
    public void testCountUniqueQuery()  throws Exception {
        setMockResponse(200, "{\"result\": 9}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.countUnique(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY);
        assertNotNull(result.get("result"));

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
        setMockResponse(201, "{\"result\": 0}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.minimum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY);
        assertNotNull(result.get("result"));

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
        setMockResponse(201, "{\"result\": 8}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.maximum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY);
        assertNotNull(result.get("result"));

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
        setMockResponse(201, "{\"result\": 3.0952380952380953}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.average(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY);
        assertNotNull(result.get("result"));

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
        setMockResponse(201, "{\"result\": 3}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.median(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY);
        assertNotNull(result.get("result"));

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
        setMockResponse(201, "{\"result\": 3}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        // todo: fix this
        Map<String, Object> result = queryClient.percentile(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY, "50");
        assertNotNull(result.get("result"));

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.PERCENTILE));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);
        assertEquals(resultString, "{\""+KeenQueryConstants.TARGET_PROPERTY+"\":\""+TEST_TARGET_PROPERTY+"\",\"percentile\":\"50\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}");
    }
    @Test
    public void testSum()  throws Exception {
        setMockResponse(201, "{\"result\": 65}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.sum(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY);
        assertNotNull(result.get("result"));

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
        setMockResponse(201, "{\"result\": [0, 1, 2, 3, 4, 5, 6, 7, 8]}");
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.selectUnique(TEST_EVENT_COLLECTION, TEST_TARGET_PROPERTY);
        assertNotNull(result.get("result"));

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
        setMockResponse(201, "{\"result\":[{\"keen\": {\"timestamp\": \"2015-05-12T05:55:55.833Z\", \"created_at\": \"2015-05-12T05:56:49.502Z\", \"id\": \"555196212fd4b12ed0fe00a6\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-12T05:55:57.669Z\", \"created_at\": \"2015-05-12T05:56:49.502Z\", \"id\": \"555196212fd4b12ed0fe00a7\"}, \"click-number\": 1}, {\"keen\": {\"timestamp\": \"2015-05-12T05:57:54.837Z\", \"created_at\": \"2015-05-12T05:58:56.026Z\", \"id\": \"555196a02fd4b12ee6d51030\"}, \"click-number\": 2}, {\"keen\": {\"timestamp\": \"2015-05-12T05:58:30.022Z\", \"created_at\": \"2015-05-12T05:58:56.027Z\", \"id\": \"555196a02fd4b12ee6d51031\"}, \"click-number\": 3}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:09.249Z\", \"created_at\": \"2015-05-12T05:59:31.575Z\", \"id\": \"555196c32fd4b12ec02d3560\"}, \"click-number\": 4}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:11.741Z\", \"created_at\": \"2015-05-12T05:59:31.575Z\", \"id\": \"555196c32fd4b12ec02d3561\"}, \"click-number\": 5}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:12.379Z\", \"created_at\": \"2015-05-12T05:59:31.576Z\", \"id\": \"555196c32fd4b12ec02d3562\"}, \"click-number\": 6}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:12.942Z\", \"created_at\": \"2015-05-12T05:59:31.576Z\", \"id\": \"555196c32fd4b12ec02d3563\"}, \"click-number\": 7}, {\"keen\": {\"timestamp\": \"2015-05-12T05:59:13.533Z\", \"created_at\": \"2015-05-12T05:59:31.576Z\", \"id\": \"555196c32fd4b12ec02d3564\"}, \"click-number\": 8}, {\"keen\": {\"timestamp\": \"2015-05-12T22:22:39.964Z\", \"created_at\": \"2015-05-13T21:48:04.499Z\", \"id\": \"5553c694c2266c32a3bab9ae\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-13T21:47:17.520Z\", \"created_at\": \"2015-05-13T21:48:04.499Z\", \"id\": \"5553c694c2266c32a3bab9af\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:22.496Z\", \"created_at\": \"2015-05-14T05:15:10.940Z\", \"id\": \"55542f5ec2266c32526aaf81\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:25.883Z\", \"created_at\": \"2015-05-14T05:15:10.940Z\", \"id\": \"55542f5ec2266c32526aaf82\"}, \"click-number\": 1}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:43.388Z\", \"created_at\": \"2015-05-14T05:15:10.941Z\", \"id\": \"55542f5ec2266c32526aaf83\"}, \"click-number\": 2}, {\"keen\": {\"timestamp\": \"2015-05-14T03:31:44.087Z\", \"created_at\": \"2015-05-14T05:15:10.941Z\", \"id\": \"55542f5ec2266c32526aaf84\"}, \"click-number\": 3}, {\"keen\": {\"timestamp\": \"2015-05-14T03:32:12.947Z\", \"created_at\": \"2015-05-14T05:15:10.941Z\", \"id\": \"55542f5ec2266c32526aaf85\"}, \"click-number\": 4}, {\"keen\": {\"timestamp\": \"2015-05-14T03:32:44.058Z\", \"created_at\": \"2015-05-14T05:15:10.942Z\", \"id\": \"55542f5ec2266c32526aaf86\"}, \"click-number\": 5}, {\"keen\": {\"timestamp\": \"2015-05-14T03:33:05.368Z\", \"created_at\": \"2015-05-14T05:15:10.942Z\", \"id\": \"55542f5ec2266c32526aaf87\"}, \"click-number\": 6}, {\"keen\": {\"timestamp\": \"2015-05-14T03:33:15.033Z\", \"created_at\": \"2015-05-14T05:15:10.943Z\", \"id\": \"55542f5ec2266c32526aaf88\"}, \"click-number\": 7}, {\"keen\": {\"timestamp\": \"2015-05-15T18:01:00.773Z\", \"created_at\": \"2015-05-15T18:01:11.498Z\", \"id\": \"555634672fd4b12ed06db81f\"}, \"click-number\": 0}, {\"keen\": {\"timestamp\": \"2015-05-15T18:01:52.746Z\", \"created_at\": \"2015-05-15T18:02:09.143Z\", \"id\": \"555634a12fd4b12ebabfcaf8\"}, \"click-number\": 1}]}");

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.extraction(TEST_EVENT_COLLECTION);
        assertNotNull(result.get("result"));

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
        queryClient.extraction(TEST_EVENT_COLLECTION, "testEmail@email.com");

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
        setMockResponse(201, "{\"steps\": [{\"with_actors\": false, \"actor_property\": \"click-count\", \"filters\": [], \"timeframe\": null, \"timezone\": null, \"event_collection\": \"android-sample-button-clicks\", \"optional\": false, \"inverted\": false}], \"result\": [1]}");
        List<Map<String, Object>> listSteps = new ArrayList<Map<String, Object>>();

        Map<String, Object> steps = new HashMap<String, Object>();
        steps.put(KeenQueryConstants.ACTOR_PROPERTY, "click-count");
        steps.put(KeenQueryConstants.EVENT_COLLECTION, TEST_EVENT_COLLECTION);

        listSteps.add(steps);

        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        Map<String, Object> result = queryClient.funnel(listSteps);
        assertNotNull(result.get("result"));

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();
        assertTrue(request.url.toString().contains(KeenQueryConstants.FUNNEL));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        String resultString = outputStream.toString(ENCODING);

        assertEquals(resultString, "{\""+KeenQueryConstants.STEPS+"\":[{\""+KeenQueryConstants.ACTOR_PROPERTY+"\":\"click-count\",\""+KeenQueryConstants.EVENT_COLLECTION+"\":\""+TEST_EVENT_COLLECTION+"\"}]}");


    }

    // TEST OPTIONAL PARAMETERS
    // TODO: much more work
    @Test
    public void testFilterValid()  throws Exception {
        setMockResponse(201, "{\"result\": 6}");
        try {
            KeenQueryParams queryParams = new QueryParamBuilder()
                    .withEventCollection(TEST_EVENT_COLLECTION)
                    .build();

//            KeenQueryParams extraParams = new KeenQueryParams();
            // TODO: this should be numeric
            // TODO: maybe overload these functions to accept an Object
            queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.LESS_THAN, "5");
            queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.GREATER_THAN, "1");

            Map<String, Object> result = queryClient.count(queryParams);
            assertNotNull(result.get("result"));
        } catch (IOException e) {
        }
    }

    @Test
    public void testFilterInvalidGeo()  throws Exception {
        setMockResponse(201, "{\"message\": \"You specified a geo filter on a property other than keen.location.coordinates, which is not allowed. You specified: ''.\", \"error_code\": \"InvalidPropertyNameForGeoFilter\"}");
        try {
            KeenQueryParams queryParams = new QueryParamBuilder()
                    .withEventCollection(TEST_EVENT_COLLECTION)
                    .build();
            queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.LESS_THAN, "5");
            queryParams.addFilter(TEST_TARGET_PROPERTY, KeenQueryConstants.WITHIN, "INVALID");

            ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);

            Map<String, Object> result = queryClient.count(queryParams);

            verify(mockHttpHandler).execute(capturedRequest.capture());
            Request request = capturedRequest.getValue();

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            request.body.writeTo(outputStream);
            String resultString = outputStream.toString(ENCODING);

            assertEquals(resultString, "{\"filters\":[{\"property_value\":\"5\",\"property_name\":\"click-number\",\"operator\":\"lt\"},{\"property_value\":\"INVALID\",\"property_name\":\"click-number\",\"operator\":\"within\"}],\"event_collection\":\"android-sample-button-clicks\"}");

            assertNull(result.get("result"));
            assertNotNull(result.get("message"));
            assertNotNull(result.get("error_code"));
        } catch (IOException e) {
        }
    }

    private void setMockResponse(int statusCode, String body) throws IOException {
        Response response = new Response(statusCode, body);
        when(mockHttpHandler.execute(any(Request.class))).thenReturn(response);
    }


}

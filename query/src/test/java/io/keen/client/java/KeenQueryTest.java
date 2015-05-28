package io.keen.client.java;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.Map;


import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * KeenClientTest
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenQueryTest {

    private static KeenProject TEST_PROJECT;

//    private KeenClient client;
    private HttpHandler mockHttpHandler;
    private KeenQueryClient queryClient;
    private JacksonJsonHandler jsonHandler;

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
        jsonHandler = new JacksonJsonHandler();

        // build the client
        queryClient = new TestKeenQueryClientBuilder()
                .withJsonHandler(jsonHandler)
                .withHttpHandler(mockHttpHandler)
                .build();
        queryClient.setDefaultProject(TEST_PROJECT);

    }

    @After
    public void cleanUp() {
        queryClient = null;
    }

    // todo: remove this.
    @Test
    public void testRealQuery() throws KeenException {
        KeenProject queryProject = new KeenProject("555190333bc696371aaaebb0", "<write key>", "eee2b89b5dab28bb4a66dcb7d676387959b2c518f884c287edf40fd048335db7b9b7e0d9eb6572e9152a9f3d96f0e413398310ad97dc9433c3a9a3298944f942d5b85f989b36087db42795539ac321e84ca53592c2c99d45bfba64417070a037e9e765c1e8594c62f1f75b6ea794afa0");
        KeenJsonHandler jsonHandler = new JacksonJsonHandler();
        KeenQueryClient queryClientTest = new KeenQueryClient(jsonHandler);
        queryClientTest.setDefaultProject(queryProject);
        try {
            Map<String, Object> result = queryClientTest.count("android-sample-button-clicks");
            assertNotNull(result.get("result"));
        } catch (IOException e) {

        }

    }


    // todo: remove this.
    @Test
    public void testRealQuery2() throws KeenException {
        KeenProject queryProject = new KeenProject("555190333bc696371aaaebb0", "<write key>", "eee2b89b5dab28bb4a66dcb7d676387959b2c518f884c287edf40fd048335db7b9b7e0d9eb6572e9152a9f3d96f0e413398310ad97dc9433c3a9a3298944f942d5b85f989b36087db42795539ac321e84ca53592c2c99d45bfba64417070a037e9e765c1e8594c62f1f75b6ea794afa0");
        KeenJsonHandler jsonHandler = new JacksonJsonHandler();
        KeenQueryClient queryClientTest = new KeenQueryClient(jsonHandler);
        queryClientTest.setDefaultProject(queryProject);

        KeenQueryParams extraParams = new KeenQueryParams();
        extraParams.addFilter("click-number", KeenQueryConstants.LESS_THAN, "5");
        extraParams.addFilter("click-number", KeenQueryConstants.WITHIN, "1");
        queryClientTest.setQueryParams(extraParams);

        try {
            Map<String, Object> result = queryClientTest.count("android-sample-button-clicks");
//            assertNotNull(result.get("result"));
        } catch (IOException e) {

        }

    }

    @Test
    public void testCountQuery()  throws Exception {
        setMockResponse(201, "{\"result\": 21}");
        try {
            Map<String, Object> result = queryClient.count("android-sample-button-clicks");
            assertNotNull(result.get("result"));
        } catch (IOException e) {
        }

        queryClient.clearQueryParams();
    }


    @Test
    public void testFilterValid()  throws Exception {
        setMockResponse(201, "{\"result\": 6}");
        try {
            KeenQueryParams extraParams = new KeenQueryParams();
            extraParams.addFilter("click-number", KeenQueryConstants.LESS_THAN, "5");
            extraParams.addFilter("click-number", KeenQueryConstants.GREATER_THAN, "1");
            queryClient.setQueryParams(extraParams);

            Map<String, Object> result = queryClient.count("android-sample-button-clicks");
            assertNotNull(result.get("result"));
        } catch (IOException e) {
        }
        queryClient.clearQueryParams();
    }

    @Test
    public void testFilterInvalidGeo()  throws Exception {
        setMockResponse(201, "{\"message\": \"You specified a geo filter on a property other than keen.location.coordinates, which is not allowed. You specified: ''.\", \"error_code\": \"InvalidPropertyNameForGeoFilter\"}");
        try {
            KeenQueryParams extraParams = new KeenQueryParams();
            extraParams.addFilter("click-number", KeenQueryConstants.LESS_THAN, "5");
            extraParams.addFilter("click-number", KeenQueryConstants.WITHIN, "INVALID");
            queryClient.setQueryParams(extraParams);

            ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);

            Map<String, Object> result = queryClient.count("android-sample-button-clicks");

            verify(mockHttpHandler).execute(capturedRequest.capture());
            assertThat(capturedRequest.getValue().url.toString(), startsWith("https://api.keen.io"));
            Request request = capturedRequest.getValue();

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            request.body.writeTo(outputStream);
            String resultString = outputStream.toString();

            // Parse the response into a map.
            StringReader reader = new StringReader(resultString);
            Map<String, Object> responseMap;
            responseMap = jsonHandler.readJson(reader);

//            assertEquals(resultString, "{\"filters\":[{\"property_value\":\"5\",\"property_name\":\"click-number\",\"operator\":\"lt\"},{\"property_value\":\"INVALID\",\"property_name\":\"click-number\",\"operator\":\"within\"}],\"event_collection\":\"android-sample-button-clicks\"}");

            assertNull(result.get("result"));
            assertNotNull(result.get("message"));
            assertNotNull(result.get("error_code"));
        } catch (IOException e) {
        }
        queryClient.clearQueryParams();
    }

    private void setMockResponse(int statusCode, String body) throws IOException {
        Response response = new Response(statusCode, body);
        when(mockHttpHandler.execute(any(Request.class))).thenReturn(response);
    }


}

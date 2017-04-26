package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Base class for tests in the 'query' module.
 *
 * @author masojus
 */
public class KeenQueryTestBase {
    static KeenProject TEST_PROJECT;
    static final String TEST_EVENT_COLLECTION = "android-sample-button-clicks";
    static final String TEST_TARGET_PROPERTY = "click-number";
    static final float DOUBLE_CMP_DELTA = 0.0f;

    static final String ENCODING = "UTF-8";
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    HttpHandler mockHttpHandler;
    KeenQueryClient queryClient;


    @BeforeClass
    public static void classSetUp() {
        KeenLogging.enableLogging();
        TEST_PROJECT = new KeenProject("<project ID>", "<write key>", "<read key>", "<master key>");
    }

    @AfterClass
    public static void tearDownClass() {
        KeenLogging.disableLogging();
        TEST_PROJECT = null;
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

    void setMockResponse(int statusCode, String body) throws IOException {
        Response response = new Response(statusCode, body);
        when(mockHttpHandler.execute(any(Request.class))).thenReturn(response);
    }

    String mockCaptureCountQueryRequest(KeenQueryRequest inputParams) throws Exception {
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        queryClient.execute(inputParams);

        verify(mockHttpHandler).execute(capturedRequest.capture());
        Request request = capturedRequest.getValue();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);
        return outputStream.toString(ENCODING);
    }
}

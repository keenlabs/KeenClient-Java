package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
    static final Timeframe TEST_RELATIVE_TIMEFRAME = new RelativeTimeframe("this_2_months");
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
        MockitoAnnotations.initMocks(this);

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
        numExecuteCalls = 0;
    }

    @Captor
    private ArgumentCaptor<Request> requestArgumentCaptor;
    private int numExecuteCalls = 0;

    void setMockResponse(int statusCode, String body) throws IOException {
        Response response = new Response(statusCode, body);
        when(mockHttpHandler.execute(any(Request.class))).thenReturn(response);
        numExecuteCalls = 1;
    }

    // Set responses such that that number of requests are expected and fulfilled with the
    // responses in the order given.
    void setMockResponses(List<Response> responses) throws IOException {
        numExecuteCalls = responses.size();

        if (numExecuteCalls < 2) {
            throw new IllegalArgumentException("Should have at least 2 responses.");
        }

        List<Response> remainingResponses = responses.subList(1, numExecuteCalls);
        Response[] remaining = remainingResponses.toArray(new Response[numExecuteCalls - 1]);
        Response first = responses.get(0);
        when(mockHttpHandler.execute(any(Request.class))).thenReturn(first, remaining);
    }

    String mockCaptureCountQueryRequest(KeenQueryRequest inputParams) throws Exception {
        executeRequest(inputParams);

        return executeCapturedRequest();
    }

    String executeCapturedRequest() throws IOException {
        verify(mockHttpHandler, times(numExecuteCalls)).execute(requestArgumentCaptor.capture());

        // We're only returning the most recently captured Request. If we ever want to get all the
        // Requests for all invocations, we'll need to refactor a bit.
        return requestToString(requestArgumentCaptor.getValue());
    }

    String requestToString(Request request) throws IOException {
        // For GET requests the body might be null.
        if (null == request.body) {
            return "";
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.body.writeTo(outputStream);

        return outputStream.toString(ENCODING);
    }

    // Override this to change how this gets dispatched.
    void executeRequest(KeenQueryRequest requestParams) throws IOException {
        queryClient.execute(requestParams);
    }

    ObjectNode getRequestNode(KeenQueryRequest requestParams) throws Exception {
        String requestString = mockCaptureCountQueryRequest(requestParams);

        return stringToRequestNode(requestString);
    }

    ObjectNode stringToRequestNode(String requestString) throws IOException {
        if (null == requestString || requestString.trim().isEmpty()) {
            return null;
        }

        // At some point, maybe JsonNode will be more appropriate.
        return (ObjectNode) OBJECT_MAPPER.readTree(requestString);
    }

    // Issue #100 : Add some verification of the actual URL produced in the Request
    // the way we do in some of the tests in KeenQueryTest.
}

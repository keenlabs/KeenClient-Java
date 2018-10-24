package io.keen.client.java;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import io.keen.client.java.http.OutputSource;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;
import io.keen.client.java.http.UrlConnectionHttpHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the HttpRequestHandler implementation.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class UrlConnectionHttpHandlerTest {

    private static final String TEST_AUTHORIZATION = "<DUMMY STRING>";
    private static final String TEST_URL = "http://fake.domain.com/api";

    private HttpURLConnection mockConnection;
    private UrlConnectionHttpHandler handler;

    @Before
    public void setUp() {
        mockConnection = mock(HttpURLConnection.class);
        handler = new UrlConnectionHttpHandler() {
            @Override
            protected HttpURLConnection openConnection(Request request) throws IOException {
                return mockConnection;
            }
        };
    }

    @After
    public void cleanUp() {
        mockConnection = null;
        handler = null;
    }

    @Test
    public void success200() throws Exception {
        Response response = runResponseTest(200, "request-body", "200 OK", null);
        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("200 OK", response.body);
    }

    @Test
    public void success201() throws Exception {
        Response response = runResponseTest(201, "request-body", "201 Created", null);
        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("201 Created", response.body);
    }

    @Test
    public void failure400() throws Exception {
        Response response = runResponseTest(400, "request-body", null, "400 Bad Request");
        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals("400 Bad Request", response.body);
    }

    @Test
    public void failure500() throws Exception {
        Response response = runResponseTest(500, "request-body", null, "500 Internal Server Error");
        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals("500 Internal Server Error", response.body);
    }

    private Response runResponseTest(int statusCode, final String requestBody,
                                     String response, String error) throws IOException {
        // Configure the mock connection.
        ByteArrayOutputStream requestOutputStream = new ByteArrayOutputStream();
        configureMockConnection(requestOutputStream, statusCode, response, error);

        // Build an output source which simply writes the request body to the output as UTF-8.
        OutputSource source = new OutputSource() {
            @Override
            public void writeTo(OutputStream out) throws IOException {
                out.write(requestBody.getBytes("UTF-8"));
            }
        };

        // The mock connection will ignore the URL but the Request constructor requires it, so
        // create a throw-away dummy URL.
        URL testUrl = new URL(TEST_URL);

        // Execute the request get the result.
        Request request = new Request(testUrl, "POST", TEST_AUTHORIZATION, source, null, 30000, 30000);
        Response result = handler.execute(request);

        // Confirm that the mocked connection received the expected request.
        assertEquals(requestBody, requestOutputStream.toString("UTF-8"));

        // Return the server response.
        return result;
    }

    private HttpURLConnection configureMockConnection(
            ByteArrayOutputStream requestOutputStream, int statusCode, String response,
            String error) throws IOException {

        // Return the request output stream.
        when(mockConnection.getOutputStream()).thenReturn(requestOutputStream);

        // Mock the response code.
        when(mockConnection.getResponseCode()).thenReturn(statusCode);

        // If no response was specified, then throw on IOException on getInputStream. Otherwise
        // return a stream containing the specified response.
        if (response == null) {
            when(mockConnection.getInputStream()).thenThrow(new IOException());
        } else {
            ByteArrayInputStream stream = new ByteArrayInputStream(response.getBytes("UTF-8"));
            when(mockConnection.getInputStream()).thenReturn(stream);
        }

        // If no error was specified, getErrorStream should return null. Otherwise return a stream
        // containing the specified error response.
        if (error == null) {
            when(mockConnection.getErrorStream()).thenReturn(null);
        } else {
            ByteArrayInputStream stream = new ByteArrayInputStream(error.getBytes("UTF-8"));
            when(mockConnection.getErrorStream()).thenReturn(stream);
        }

        return mockConnection;
    }

}

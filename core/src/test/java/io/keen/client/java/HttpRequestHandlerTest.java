package io.keen.client.java;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;

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
public class HttpRequestHandlerTest {

    private HttpRequestHandler client;

    @Before
    public void setUp() {
        client = new HttpRequestHandler();
    }

    @After
    public void cleanUp() {
        client = null;
    }

    @Test
    public void success200() throws Exception {
        HttpRequestHandler.HttpResponse response = runResponseTest(200, "request-body", "200 OK", null);
        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("200 OK", response.body);
    }

    @Test
    public void success201() throws Exception {
        HttpRequestHandler.HttpResponse response =
                runResponseTest(201, "request-body", "201 Created", null);
        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("201 Created", response.body);
    }

    @Test
    public void failure400() throws Exception {
        HttpRequestHandler.HttpResponse response =
                runResponseTest(400, "request-body", null, "400 Bad Request");
        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals("400 Bad Request", response.body);
    }

    @Test
    public void failure500() throws Exception {
        HttpRequestHandler.HttpResponse response =
                runResponseTest(500, "request-body", null, "500 Internal Server Error");
        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals("500 Internal Server Error", response.body);
    }

    private static final String TEST_AUTHORIZATION = "<DUMMY STRING>";

    private HttpRequestHandler.HttpResponse runResponseTest(int statusCode, final String request,
                                                      String response, String error) throws IOException {
        ByteArrayOutputStream requestOutputStream = new ByteArrayOutputStream();
        HttpURLConnection mockConnection =
                buildMockConnection(requestOutputStream, statusCode, response, error);

        // Build an output source which simply writes the serialized JSON to the output.
        HttpRequestHandler.OutputSource source = new HttpRequestHandler.OutputSource() {
            @Override
            public void write(Writer out) throws IOException {
                out.write(request);
            }
        };

        // Send the request to the mock URL and get the result.
        HttpRequestHandler.HttpResponse result =
                client.sendPostRequest(mockConnection, TEST_AUTHORIZATION, source);

        // Confirm that the mocked connection received the expected request.
        assertEquals(request, requestOutputStream.toString("UTF-8"));

        // Return the server response.
        return result;
    }

    private HttpURLConnection buildMockConnection(ByteArrayOutputStream requestOutputStream,
                                                  int statusCode, String response, String error) throws IOException {

        // Build a mock HttpURLConnection.
        HttpURLConnection mockConnection = mock(HttpURLConnection.class);

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

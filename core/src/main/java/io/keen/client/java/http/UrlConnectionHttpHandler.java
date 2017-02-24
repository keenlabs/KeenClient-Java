package io.keen.client.java.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

import io.keen.client.java.KeenUtils;
import io.keen.client.java.KeenVersion;

/**
 * This class provides a default implementation of {@link HttpHandler} using
 * {@link java.net.HttpURLConnection}. To use a different HttpURLConnection implementation simply
 * override the {@link UrlConnectionHttpHandler#execute(Request)}} method.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class UrlConnectionHttpHandler implements HttpHandler {

    /**
     * Sends an HTTP request.
     *
     * @param request The {@link Request} to send.
     * @return A {@link Response} object describing the response from the server.
     * @throws IOException If there was an error during the connection.
     */
    @Override
    public Response execute(Request request) throws IOException {
        HttpURLConnection connection = openConnection(request);
        sendRequest(connection, request);
        return readResponse(connection);
    }

    ///// PROTECTED METHODS /////

    /**
     * Opens a connection based on the URL in the given request.
     *
     * Subclasses can override this method to use a different implementation of
     * {@link HttpURLConnection}.
     *
     * @param request The {@link Request}.
     * @return A new {@link HttpURLConnection}.
     * @throws IOException If there is an error opening the connection.
     */
    protected HttpURLConnection openConnection(Request request) throws IOException {
        HttpURLConnection result;
        if (request.proxy != null) {
            result = (HttpURLConnection) request.url.openConnection(request.proxy);
        } else {
            result = (HttpURLConnection) request.url.openConnection();
        }
        result.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT);
        result.setReadTimeout(DEFAULT_READ_TIMEOUT);
        return result;
    }

    /**
     * Sends a request over a given connection.
     *
     * @param connection The connection over which to send the request.
     * @param request The request to send.
     * @throws IOException If there is an error sending the request.
     */
    protected void sendRequest(HttpURLConnection connection, Request request) throws IOException {
        // Set up the request.
        connection.setRequestMethod(request.method);
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Authorization", request.authorization);

        // If a different HttpHandler is used, we won't get this header. We would need to refactor
        // to a delegation pattern to give the client code's HttpHandler a chance to process the
        // Request first, then attach our custom headers, which would likely be a breaking change.
        connection.setRequestProperty("Keen-Sdk", KeenVersion.getSdkVersion());

        // If the request has a body, send it. Otherwise just connect.
        if (request.body != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            request.body.writeTo(connection.getOutputStream());
        } else {
            connection.connect();
        }
    }

    /**
     * Reads a {@link Response} from an existing connection. This method should only be
     * called on a connection for which the entire request has been sent.
     *
     * @param connection The connection that sent the response.
     * @return The {@link Response}
     * @throws IOException If there is an error reading the response from the connection.
     */
    protected Response readResponse(HttpURLConnection connection) throws IOException {
        // Try to get the input stream and if that fails, try to get the error stream.
        InputStream in;
        try {
            in = connection.getInputStream();
        } catch (IOException e) {
            in = connection.getErrorStream();
        }

        // If either stream is present, try to read the response body.
        String body = "";
        if (in != null) {
            try {
                body = KeenUtils.convertStreamToString(in);
            } finally {
                KeenUtils.closeQuietly(in);
            }
        }

        // Build and return the HTTP response object.
        return new Response(connection.getResponseCode(), body);
    }


    ///// PRIVATE CONSTANTS /////

    private static final int DEFAULT_CONNECT_TIMEOUT = 30000;
    private static final int DEFAULT_READ_TIMEOUT = 30000;

}

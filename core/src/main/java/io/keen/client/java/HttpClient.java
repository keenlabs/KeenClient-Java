package io.keen.client.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;

/**
 * This class provides an abstraction over HTTP requests, allowing unit tests to mock the network
 * interface.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
class HttpClient {

    /**
     * Encapsulates a response to a single HTTP request.
     */
    public static final class ServerResponse {
        public final int statusCode;
        public final String body;

        public boolean isSuccess() {
            return isSuccessCode(statusCode);
        }

        public ServerResponse(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }
    }

    /**
     * Callback interface to control writing to an HTTP connection's output stream.
     */
    public interface OutputSource {

        /**
         * Writes data to the given {@link java.io.Writer}. This method is used to send the body of
         * an HTTP request.
         * <p/>
         * Implementers of this method should NOT close {@code out}; the calling code will ensure
         * that it is closed.
         *
         * @param out The {@link java.io.Writer} to which the request body should be written.
         * @throws IOException Implementers may throw an IOException if an error is encountered
         *                     while writing.
         */
        void write(Writer out) throws IOException;

    }

    /**
     * Sends an HTTP request.
     *
     * @param connection    An HTTP connection. This connection should be opened but otherwise
     *                      untouched; in particular, this method will handle setting up headers and
     *                      writing the request. This method will also handle disconnecting the
     *                      connection once the request is completed.
     * @param authorization The authorization token to use in the request header.
     * @param source        An {@link HttpClient.OutputSource} which will be used
     *                      to write the request body to the connection's {@code OutputStream}.
     * @return A {@link HttpClient.ServerResponse} object describing the
     * response from the server.
     * @throws IOException If there was an error during the connection.
     */
    public ServerResponse sendRequest(HttpURLConnection connection, String authorization,
                                      OutputSource source)
            throws IOException {

        // Set up the POST.
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Authorization", authorization);

        // Write JSON to the output stream.
        OutputStreamWriter writer = null;
        try {
            writer = new OutputStreamWriter(connection.getOutputStream(), ENCODING);
            source.write(writer);
            return getResponse(connection);
        } finally {
            KeenUtils.closeQuietly(writer);
            connection.disconnect();
        }
    }

    /**
     * Builds a {@code ServerResponse} from an existing connection. This method should only be
     * called on a connection for which the entire request has been sent.
     *
     * @throws IOException If there is an error reading from an input stream.
     */
    private ServerResponse getResponse(HttpURLConnection connection) throws IOException {
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

        // Build and return the server response object.
        return new ServerResponse(connection.getResponseCode(), body);
    }

    ///// PRIVATE CONSTANTS /////

    private static final String ENCODING = "UTF-8";

    ///// PRIVATE STATIC METHODS /////

    /**
     * Checks whether an HTTP status code indicates success.
     *
     * @param statusCode The HTTP status code.
     * @return {@code true} if the status code indicates success (2xx), otherwise {@code false}.
     */
    private static boolean isSuccessCode(int statusCode) {
        return (statusCode / 100 == 2);
    }

}

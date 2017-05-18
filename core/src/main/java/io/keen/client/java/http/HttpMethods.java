package io.keen.client.java.http;

/**
 * Constants for the HTTP request methods this project uses. These match what is expected by
 * HttpURLConnection.
 *
 * @author masojus
 */
public final class HttpMethods {
    private HttpMethods() {}

    public final static String GET = "GET";
    public final static String POST = "POST";
    public final static String PUT = "PUT";
    public final static String DELETE = "DELETE";
}

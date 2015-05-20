package io.keen.client.java.http;

import java.io.IOException;

/**
 * Interface which provides an abstraction around making HTTP requests.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public interface HttpHandler {

    /**
     * Executes the given request and returns the received response.
     *
     * @param request   The {@link Request} to send.
     * @throws java.io.IOException If there is an error executing the request or processing the
     * response.
     * @return The {@link Response} received.
     */
    Response execute(Request request) throws IOException;

}
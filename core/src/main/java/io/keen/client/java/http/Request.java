package io.keen.client.java.http;

import java.net.URL;

/**
 * Encapsulates an HTTP request.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public final class Request {

    ///// PROPERTIES /////

    public final URL url;
    public final String method;
    public final String authorization;
    public final OutputSource body;

    ///// PUBLIC CONSTRUCTORS /////

    public Request(URL url, String method, String authorization, OutputSource body) {
        this.url = url;
        this.method = method;
        this.authorization = authorization;
        this.body = body;
    }

}
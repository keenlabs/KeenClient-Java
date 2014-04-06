package io.keen.client.java;

import java.net.InetSocketAddress;
import java.net.Proxy;

/**
 * KeenConfig
 *
 * Contains configuration variables for the Keen IO Java SDK.
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenConfig {

    private static Proxy proxy;
    /**
     * How many threads to use to send events to Keen IO. Change this to 0 if you want to manage your own threads.
     */
    public static int NUM_THREADS_FOR_HTTP_REQUESTS = 3;

    /**
     * Call this to set a HTTP proxy server configuration for all Keen IO communication.
     *
     * @param proxyUrl The proxy hostname or IP address.
     * @param proxyPort  The proxy port number.
     */
    public static void setProxy(String proxyUrl, int proxyPort) {
        proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUrl, proxyPort));
    }

    static Proxy getProxy() {
        return proxy;
    }

    /**
     * Call this to clean the currently configured HTTP Proxy server configuration.
     */
    public static void clearProxy() {
        proxy = null;
    }
}

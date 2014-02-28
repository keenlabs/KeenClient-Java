package io.keen.client.java;

/**
 * KeenConfig
 * <p/>
 * Contains configuration variables for the Keen IO Java SDK.
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenConfig {

    /**
     * How many threads to use to send events to Keen IO. Change this to 0 if you want to manage your own threads.
     */
    public static int NUM_THREADS_FOR_HTTP_REQUESTS = 3;

}

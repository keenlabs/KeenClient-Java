package io.keen.client.java;

/**
 * This interface holds methods for checking network conditions.
 *
 * @author Simon Murtha Smith
 * @since 2.1.0
 */
public interface KeenNetworkStatusHandler {

    /**
     * Reports on whether there is a network connection
     *
     * @return The object which was read, held in a {@code Map&lt;String, Object&gt;}.
     * @throws IOException If there is an error reading from the input.
     */
    public boolean isNetworkConnected();

}

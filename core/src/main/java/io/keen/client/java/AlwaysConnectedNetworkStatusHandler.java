package io.keen.client.java;

import io.keen.client.java.KeenNetworkStatusHandler;

/**
 * This class implements the KeenNetworkStatusHandler. It always returns true.
 *
 * @author Simon Murtha Smith
 * @since 2.1.0
 */
public class AlwaysConnectedNetworkStatusHandler implements KeenNetworkStatusHandler {

    /**
     * Default implementation of the isNetworkConnected method.
     *
     * @return true, always
     */
    public boolean isNetworkConnected() {
        return true;
    }

}

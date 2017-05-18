package io.keen.client.java;

import java.util.Map;

/**
 * Interface for Timeframe. For running queries, users can create a
 * Timeframe object to specify the timeframe of a query. Implementing classes must
 * implement constructTimeframeArgs(), which constructs the JSON map to
 * send with the query.
 *
 * @author claireyoung
 * @since 1.0.0
 */
public interface Timeframe {
    /**
     * Subclasses must implement this method to construct
     * the appropriate Timeframe JSON arguments to send for the query.
     *
     * @return  the Timeframe Json map to send in the query.
     */
    Map<String, Object> constructTimeframeArgs();
}

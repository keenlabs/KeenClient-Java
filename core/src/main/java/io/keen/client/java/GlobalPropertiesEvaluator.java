package io.keen.client.java;

import java.util.Map;

/**
 * An interface to simulate functional programming so that you can tell the {@link io.keen.client.java.KeenClient}
 * how to dynamically return Keen Global Properties based on event collection name.
 *
 * @author dkador
 * @since 1.0.0
 */
public interface GlobalPropertiesEvaluator {
    Map<String, Object> getGlobalProperties(String eventCollection);
}

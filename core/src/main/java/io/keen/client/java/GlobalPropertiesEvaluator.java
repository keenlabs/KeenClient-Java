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

    /**
     * Gets a {@link java.util.Map} containing the global properties which should be applied to
     * a new event in the specified collection. This method will be called each time a new event is
     * created.
     *
     * @param eventCollection The name of the collection for which an event is being generated.
     * @return A {@link java.util.Map} containing the global properties which should be applied to
     * the event being generated.
     */
    Map<String, Object> getGlobalProperties(String eventCollection);

}

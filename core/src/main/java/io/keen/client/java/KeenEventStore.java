package io.keen.client.java;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface which provides an abstraction layer around how events are stored in between being
 * queued and being uploaded by a batch post operation.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public interface KeenEventStore {

    /**
     * Stores the given event.
     *
     * @param eventCollection The name of the collection in which the event should be stored.
     * @param event           The event to store.
     * @return A handle which can be used to retrieve or remove the event.
     * @throws IOException If there is an error storing the event.
     */
    Object store(String eventCollection, Map<String, Object> event) throws IOException;

    /**
     * Gets the event corresponding to the given handle.
     *
     * @param handle A handle returned from a previous call to {@link #store(String, java.util.Map)}
     *               or {@link #getHandles()}.
     * @return The event, represented as a {@link java.util.Map}.
     * @throws IOException If there is an error retrieving the event.
     */
    Map<String, Object> get(Object handle) throws IOException;

    /**
     * Removes the specified event from the store.
     *
     * @param handle A handle returned from a previous call to {@link #store(String, java.util.Map)}
     *               or {@link #getHandles()}.
     * @throws IOException If there is an error removing the event.
     */
    void remove(Object handle) throws IOException;

    /**
     * Retrieves a map from collection names to lists of handles currently stored under each
     * collection. This will be used by the {@link io.keen.client.java.KeenClient} to retrieve the
     * events to send in a batch to the Keen server, as well as to remove all successfully posted
     * events after processing the response.
     *
     * @return A map from collection names to lists of handles currently stored under each
     * collection.
     * @throws IOException If there is an error retrieving the handles.
     */
    Map<String, List<Object>> getHandles() throws IOException;

}
package io.keen.client.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link KeenEventStore} which simply keeps a copy of each event in memory until
 * it is explicitly removed.
 * <p/>
 * NOTE: This implementation synchronizes all operations in order to ensure thread safety, but as
 * a result it may perform poorly under high load. For applications that require high throughput,
 * a custom {@link io.keen.client.java.KeenEventStore} implementation is recommended.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class RamEventStore implements KeenEventStore {

    ///// PUBLIC CONSTRUCTORS /////

    /**
     * Constructs a new RAM-based event store.
     */
    public RamEventStore() {
        collectionIds = new HashMap<String, List<Long>>();
        events = new HashMap<Long, String>();
    }

    ///// KeenEventStore METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Object store(String eventCollection, String event) throws IOException {
        long id = getNextId();

        // Get the list of events for the specified collection. If no list exists yet, create one.
        List<Long> collectionEvents = collectionIds.get(eventCollection);
        if (collectionEvents == null) {
            collectionEvents = new ArrayList<Long>();
            collectionIds.put(eventCollection, collectionEvents);
        }

        // Remove the oldest events until there is room for at least one more event.
        while (collectionEvents.size() >= maxEventsPerCollection) {
            long idToRemove = collectionEvents.remove(0);
            events.remove(idToRemove);
        }

        // Add the event to the event store, add its ID to the collection's list, and return the ID.
        events.put(id, event);
        collectionEvents.add(id);
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized String get(Object handle) throws IOException {
        Long id = handleToId(handle);
        return events.get(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void remove(Object handle) throws IOException {
        Long id = handleToId(handle);
        events.remove(id);
        // Be lazy about removing handles from the collectionIds map - this can happen during the
        // getHandles call.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Map<String, List<Object>> getHandles() throws IOException {
        Map<String, List<Object>> result = new HashMap<String, List<Object>>();
        for (Map.Entry<String, List<Long>> entry : collectionIds.entrySet()) {
            String eventCollection = entry.getKey();
            List<Long> ids = entry.getValue();
            List<Object> handles = new ArrayList<Object>();

            // Iterate over the list of handles, removing any "dead" events and adding the rest to
            // the result map.
            Iterator<Long> iterator = ids.iterator();
            while (iterator.hasNext()) {
                Long id = iterator.next();
                if (events.get(id) == null) {
                    iterator.remove();
                } else {
                    handles.add(id);
                }
            }
            if (handles.size() > 0) {
                result.put(eventCollection, handles);
            }
        }
        return result;
    }

    ///// PUBLIC METHODS /////

    /**
     * Sets the number of events that can be stored for a single collection before aging them out.
     *
     * @param maxEventsPerCollection The maximum number of events per collection.
     */
    public void setMaxEventsPerCollection(int maxEventsPerCollection) {
        this.maxEventsPerCollection = maxEventsPerCollection;
    }

    ///// TEST HOOKS /////

    /**
     * Clears all events from the store, effectively resetting it to its initial state. This method
     * is intended for use during unit testing, and should generally not be called by production
     * code.
     */
    void clear() {
        nextId = 0;
        collectionIds = new HashMap<String, List<Long>>();
        events = new HashMap<Long, String>();
    }

    ///// PRIVATE FIELDS /////

    private long nextId = 0;
    private Map<String, List<Long>> collectionIds;
    private Map<Long, String> events;
    private int maxEventsPerCollection = 10000;

    ///// PRIVATE METHODS /////

    /**
     * Gets the next ID to use as a handle for a stored event. This implementation just checks for
     * the next unused ID based on an incrementing counter.
     * <p/>
     * NOTE: For long-running processes it's possible that the nextId field will overflow. Hence it
     * is necessary to handle collisions gracefully by skipping them.
     *
     * @return The next ID that should be used to store an event.
     */
    private long getNextId() {
        // It should be all but impossible for the event cache to grow bigger than Long.MAX_VALUE,
        // but just for the sake of safe coding practices, check anyway.
        if (events.size() > Long.MAX_VALUE) {
            throw new IllegalStateException("Event store exceeded maximum size");
        }

        // Iterate through IDs, starting with the next ID counter, until an unused one is found.
        long id = nextId;
        while (events.get(id) != null) {
            id++;
        }

        // Set the next ID to the ID that was found plus one, then return the ID.
        nextId = id + 1;
        return id;
    }

    /**
     * Converts an opaque handle into a long ID. If the handle is not a Long, this will throw an
     * {@link java.lang.IllegalArgumentException}.
     *
     * @param handle The handle to convert to an ID.
     * @return The ID.
     */
    private Long handleToId(Object handle) {
        if (handle instanceof Long) {
            return (Long) handle;
        } else {
            throw new IllegalArgumentException("Expected handle to be a Long, but was: " +
                    handle.getClass().getCanonicalName());
        }
    }

}

package io.keen.client.java;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Tests the RamEventStore class.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class RamEventStoreTest extends EventStoreTestBase {

    @Override
    protected KeenEventStore buildStore() {
        return new RamEventStore();
    }

    @Test
    public void maxEventsPerCollection() throws Exception {
        // Set the maximum number of events per collection to 3.
        RamEventStore ramStore = (RamEventStore) store;
        ramStore.setMaxEventsPerCollection(3);

        // Add 5 events.
        store.store("project1", "collection1", TEST_EVENT_1);
        store.store("project1", "collection1", TEST_EVENT_2);
        store.store("project1", "collection1", TEST_EVENT_3);
        store.store("project1", "collection1", TEST_EVENT_4);
        store.store("project1", "collection1", TEST_EVENT_5);

        // Get the handle map.
        Map<String, List<Object>> handleMap = store.getHandles("project1");
        assertNotNull(handleMap);
        assertEquals(1, handleMap.size());

        // Get the lists of handles.
        List<Object> handles = handleMap.get("collection1");
        assertNotNull(handles);
        assertEquals(3, handles.size());

        // Get the events.
        List<String> retrievedEvents = new ArrayList<String>();
        for (Object handle : handles) {
            String retrievedEvent = store.get(handle);
            assertNotNull(retrievedEvent);
            retrievedEvents.add(retrievedEvent);
        }

        // Validate the events.
        assertThat(retrievedEvents, containsInAnyOrder(TEST_EVENT_3, TEST_EVENT_4, TEST_EVENT_5));
    }

}

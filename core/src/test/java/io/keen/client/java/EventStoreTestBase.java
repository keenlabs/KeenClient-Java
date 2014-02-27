package io.keen.client.java;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests the FileEventSTore class.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public abstract class EventStoreTestBase {

    protected abstract KeenEventStore buildStore() throws IOException;

    protected KeenEventStore store;

    @Before
    public void createStore() throws Exception {
        store = buildStore();
    }

    @After
    public void disposeStore() {
        store = null;
    }

    @Test
    public void storeAndGetEvent() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("foo", "bar");
        Object handle = store.store("collection1", event);
        Map<String, Object> retrieved = store.get(handle);
        assertEquals(event, retrieved);
    }

    @Test
    public void removeEvent() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("foo", "bar");
        Object handle = store.store("collection1", event);
        store.remove(handle);
        Map<String, Object> retrieved = store.get(handle);
        assertNull(retrieved);
    }

    @Test
    public void reuseEventMap() throws Exception {
        // Add an event, mutate it, and add it again.
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("foo", "bar");
        Object handle1 = store.store("collection1", event);
        event.clear();
        event.put("hello", "world");
        Object handle2 = store.store("collection1", event);

        // Retrieve both events and ensure they have the expected (different) data.
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("foo", "bar");
        assertEquals(expected, store.get(handle1));
        expected.clear();
        expected.put("hello", "world");
        assertEquals(expected, store.get(handle2));
    }

    @Test
    public void getHandles() throws Exception {
        // Add a couple events to the store.
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("foo", "bar");
        store.store("collection1", event);
        event.clear();
        event.put("hello", "world");
        store.store("collection2", event);

        // Get the handle map.
        Map<String, List<Object>> handleMap = store.getHandles();
        assertNotNull(handleMap);
        assertEquals(2, handleMap.size());

        // Get the lists of handles.
        List<Object> handles1 = handleMap.get("collection1");
        assertNotNull(handles1);
        assertEquals(1, handles1.size());
        List<Object> handles2 = handleMap.get("collection2");
        assertNotNull(handles2);
        assertEquals(1, handles2.size());

        // Validate the actual events.
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("foo", "bar");
        assertEquals(expected, store.get(handles1.get(0)));
        expected.clear();
        expected.put("hello", "world");
        assertEquals(expected, store.get(handles2.get(0)));
    }

}

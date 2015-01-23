package io.keen.client.java;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Tests AttemptCountingEventStore implementations.
 *
 * @author Simon Murtha Smith
 * @since 2.0.2
 */
public abstract class AttemptCountingEventStoreTestBase extends EventStoreTestBase {


    protected KeenAttemptCountingEventStore attemptCountingStore;


    @Before
    public void castStore() throws Exception {
        attemptCountingStore = (KeenAttemptCountingEventStore)store;
    }


    @Test
    public void storeAndGetEventAttempts() throws Exception {
        String attempts = "blargh";
        attemptCountingStore.setAttempts("project1", "collection1", attempts);
        assertEquals(attempts, attemptCountingStore.getAttempts("project1", "collection1"));
    }

    @Test
    public void getHandlesWithAttempts() throws Exception {
        // Add a couple events to the store.
        attemptCountingStore.store("project1", "collection1", TEST_EVENT_1);
        attemptCountingStore.store("project1", "collection2", TEST_EVENT_2);

        // set some value for attempts.json. This is to ensure that setting attempts doesn't
        // interfere with getting handles
        attemptCountingStore.setAttempts("project1", "collection1", "{}");

        // Get the handle map.
        Map<String, List<Object>> handleMap = attemptCountingStore.getHandles("project1");
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
        assertEquals(TEST_EVENT_1, attemptCountingStore.get(handles1.get(0)));
        assertEquals(TEST_EVENT_2, attemptCountingStore.get(handles2.get(0)));
    }
}

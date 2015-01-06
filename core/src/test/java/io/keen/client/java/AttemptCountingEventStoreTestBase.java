package io.keen.client.java;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


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
}

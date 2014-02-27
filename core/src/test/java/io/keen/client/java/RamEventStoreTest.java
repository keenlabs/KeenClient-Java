package io.keen.client.java;

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

}

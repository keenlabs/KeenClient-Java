package io.keen.client.java;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple test client which uses a RAM event store, Jackson, and a single thread Executor.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class TestKeenClient extends KeenClient {

    ///// PUBLIC STATIC METHODS //////

    /**
     * Initializes the Keen library with a test client.
     *
     * @return The singleton Keen client.
     */
    public static KeenClient initialize() {
        KeenClient.initialize(new TestKeenClient());
        return KeenClient.client();
    }

    ///// KeenClient METHODS /////

    @Override
    public KeenJsonHandler getJsonHandler() {
        return jsonHandler;
    }

    @Override
    public KeenEventStore getEventStore() {
        return eventStore;
    }

    @Override
    public Executor getPublishExecutor() {
        return publishExecutor;
    }

    ///// PRIVATE FIELDS /////

    private KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private ExecutorService publishExecutor;

    ///// PRIVATE CONSTRUCTORS /////

    TestKeenClient() {
        jsonHandler = new TestJsonHandler();
        eventStore = new RamEventStore();
        publishExecutor = Executors.newSingleThreadExecutor();
    }

    TestKeenClient(Environment env) {
        super(env);
        jsonHandler = new TestJsonHandler();
        eventStore = new RamEventStore();
        publishExecutor = Executors.newSingleThreadExecutor();
    }

}

package io.keen.client.java;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * TODO: Add documentation.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenInitializer {

    private KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private Executor publishExecutor;
    private boolean isInitializeCalled;

    public synchronized void initialize(String projectId, String writeKey, String readKey) {
        if (isInitializeCalled) {
            throw new IllegalStateException("Initialize may only be called once");
        }

        if (jsonHandler == null) {
            jsonHandler = new JacksonJsonHandler();
        }

        if (eventStore == null) {
            eventStore = new RamEventStore();
        }

        if (publishExecutor == null) {
            // TODO: Provide a way for the caller to access this thread pool, e.g. so they can call
            // shutdown on it.
            publishExecutor = Executors.newFixedThreadPool(KeenConfig.NUM_THREADS_FOR_HTTP_REQUESTS);
        }

        KeenClient.initialize(projectId, writeKey, readKey);
        isInitializeCalled = true;
    }

    public synchronized JavaKeenInitializer withJsonHandler(KeenJsonHandler jsonHandler) {
        this.jsonHandler = jsonHandler;
        return this;
    }

    public synchronized JavaKeenInitializer withEventStore(KeenEventStore eventStore) {
        this.eventStore = eventStore;
        return this;
    }

    public synchronized JavaKeenInitializer withPublishExecutor(Executor publishExecutor) {
        this.publishExecutor = publishExecutor;
        return this;
    }

}

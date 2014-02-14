package io.keen.client.java;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * TODO: Add documentation.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenInitializer extends KeenInitializer {

    ///// KeenInitializer METHODS /////

    @Override
    protected Executor buildDefaultPublishExecutor() {
        return Executors.newFixedThreadPool(KeenConfig.NUM_THREADS_FOR_HTTP_REQUESTS);
    }

    @Override
    protected KeenEventStore buildDefaultEventStore() {
        return new RamEventStore();
    }

    @Override
    protected KeenJsonHandler buildDefaultJsonHandler() {
        return new JacksonJsonHandler();
    }

}

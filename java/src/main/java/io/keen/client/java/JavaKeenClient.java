package io.keen.client.java;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * DOCUMENT
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenClient extends KeenClient {

    ///// PUBLIC STATIC METHODS //////

    /**
     * DOCUMENT
     */
    public static void initialize() {
        KeenClient.initialize(new JavaKeenClient());
    }

    ///// KeenClient METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    protected Executor instantiatePublishExecutor() {
        return Executors.newFixedThreadPool(KeenConfig.NUM_THREADS_FOR_HTTP_REQUESTS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected KeenEventStore instantiateEventStore() {
        return new RamEventStore();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected KeenJsonHandler instantiateJsonHandler() {
        return new JacksonJsonHandler();
    }

}

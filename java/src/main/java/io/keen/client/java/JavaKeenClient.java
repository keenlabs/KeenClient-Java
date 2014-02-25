package io.keen.client.java;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Implementation of a {@link io.keen.client.java.KeenClient} in a standard Java environment.
 *
 * This client uses the Jackson library for reading and writing JSON. As a result, Jackson must be
 * available in order for this library to work properly. To use a different library, override
 * {@link #instantiateJsonHandler()}.
 *
 * To cache events in between batch uploads, this client uses a RAM-based event store.
 *
 * This client uses a fixed thread pool (constructed with
 * {@link java.util.concurrent.Executors#newFixedThreadPool(int)}) to run asynchronous requests.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenClient extends KeenClient {

    ///// PUBLIC STATIC METHODS //////

    /**
     * Initializes the Keen library with a default Java client.
     *
     * @return The singleton Keen client.
     */
    public static KeenClient initialize() {
        KeenClient.initialize(new JavaKeenClient());
        return KeenClient.client();
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

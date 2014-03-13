package io.keen.client.java;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link io.keen.client.java.KeenClient} in a standard Java environment.
 * <p/>
 * This client uses the Jackson library for reading and writing JSON. As a result, Jackson must be
 * available in order for this library to work properly.
 * <p/>
 * To cache events in between batch uploads, this client uses a RAM-based event store.
 * <p/>
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
        // If the library hasn't been initialized yet then initialize it.
        if (!KeenClient.isInitialized()) {
            KeenClient.initialize(new JavaKeenClient());
        }
        return KeenClient.client();
    }

    ///// KeenClient METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    public KeenJsonHandler getJsonHandler() {
        return jsonHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeenEventStore getEventStore() {
        return eventStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExecutorService getPublishExecutor() {
        return publishExecutor;
    }

    ///// PUBLIC METHODS /////

    /**
     * Shuts down the shared thread pool.
     *
     * Note: New asynchronous operation requests will fail with a
     * {@link java.util.concurrent.RejectedExecutionException}.
     *
     * @param timeout A positive timeout in millis will block the current thread until the shutdown
     *                completes or the timeout expires. Any other timeout value will result in this
     *                method returning immediately.
     * @throws java.lang.InterruptedException If interrupted while waiting for shutdown.
     */
    public void shutdownPublishExecutorService(long timeout) throws InterruptedException {
        if (!publishExecutor.isShutdown()) {
            publishExecutor.shutdown();
            if (timeout > 0) {
                publishExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * If the publish {@link java.util.concurrent.ExecutorService} is shutdown, this method
     * starts a new one. Otherwise it does nothing.
     */
    public void restartPublishExecutorService() {
        if (publishExecutor.isShutdown()) {
            publishExecutor = Executors.newFixedThreadPool(KeenConfig.NUM_THREADS_FOR_HTTP_REQUESTS);
        }
    }

    ///// PRIVATE FIELDS /////

    private KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private ExecutorService publishExecutor;

    ///// PRIVATE CONSTRUCTORS /////

    /**
     * Constructs a Java client.
     */
    private JavaKeenClient() {
        // Try to initialize the necessary components. If any of them fails for any reason,
        // mark the client as inactive.
        try {
            jsonHandler = new JacksonJsonHandler();
            eventStore = new RamEventStore();
            publishExecutor = Executors.newFixedThreadPool(KeenConfig.NUM_THREADS_FOR_HTTP_REQUESTS);
        } catch (Exception e) {
            KeenLogging.log("Exception initializing JavaKeenClient: " + e.getMessage());
            setActive(false);
        }
    }

}

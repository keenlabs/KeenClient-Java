package io.keen.client.java;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
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
            KeenClient.initialize(new JavaKeenClient.Builder().build());
        }
        return KeenClient.client();
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
        ExecutorService publishExecutorService = tryGetPublishExecutorService();
        if (!publishExecutorService.isShutdown()) {
            publishExecutorService.shutdown();
            if (timeout > 0) {
                publishExecutorService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            }
        }
    }

    ///// BUILDER IMPLEMENTATION /////

    public static class Builder extends KeenClient.Builder<JavaKeenClient> {

        @Override
        protected JavaKeenClient newInstance() {
            return new JavaKeenClient(this);
        }

        @Override
        protected KeenJsonHandler getDefaultJsonHandler() {
            return new JacksonJsonHandler();
        }

    }

    ///// PRIVATE CONSTRUCTORS /////

    /**
     * Constructs a Java client.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    private JavaKeenClient(Builder builder) {
        super(builder);
    }

    ///// PRIVATE METHODS /////

    private ExecutorService tryGetPublishExecutorService() {
        Executor publishExecutor = getPublishExecutor();
        if (publishExecutor instanceof ExecutorService) {
            return (ExecutorService) publishExecutor;
        } else {
            throw new IllegalStateException("Expected publishExecutor to be an ExecutorService " +
                    "but found: " + publishExecutor.getClass().getCanonicalName());
        }
    }

}

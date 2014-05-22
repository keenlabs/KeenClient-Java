package io.keen.client.android;

import android.content.Context;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.keen.client.java.FileEventStore;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenEventStore;
import io.keen.client.java.KeenJsonHandler;

/**
 * Implementation of a {@link io.keen.client.java.KeenClient} on the Android platform.
 * <p/>
 * This client uses the built-in Android JSON libraries for reading/writing JSON in order to
 * minimize library size.
 * <p/>
 * To cache events in between batch uploads, this client uses a file-based event store with its
 * root in the application's cache directory.
 * <p/>
 * This client uses a fixed thread pool to run asynchronous requests.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenClient extends KeenClient {

    ///// PUBLIC STATIC METHODS //////

    /**
     * Initializes the keen client library with a new Android client.
     *
     * @param context A context which can be used to retrieve the application context in which the
     *                client will run.
     * @return The singleton Keen client.
     */
    public static KeenClient initialize(Context context) {
        // If the library hasn't been initialized yet then initialize it.
        if (!KeenClient.isInitialized()) {
            KeenClient.initialize(new AndroidKeenClient.Builder(context).build());
        }
        return KeenClient.client();
    }

    ///// BUILDER IMPLEMENTATION /////

    public static class Builder extends KeenClient.Builder<AndroidKeenClient> {

        private final Context context;

        public Builder(Context context) {
            this.context = context;
        }

        @Override
        protected AndroidKeenClient newInstance() {
            return new AndroidKeenClient(this);
        }

        @Override
        protected KeenJsonHandler getDefaultJsonHandler() {
            return new AndroidJsonHandler();
        }

        @Override
        protected KeenEventStore getDefaultEventStore() throws Exception {
            return new FileEventStore(context.getCacheDir());
        }

        /**
         * Builds a simple fixed-thread-pool executor, using the number of available processors as
         * the thread count.
         *
         * @return The constructed executor.
         */
        @Override
        protected Executor getDefaultPublishExecutor() {
            int procCount = Runtime.getRuntime().availableProcessors();
            return Executors.newFixedThreadPool(procCount);
        }

    }

    ///// PRIVATE CONSTRUCTORS /////

    /**
     * Constructs an Android client.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    private AndroidKeenClient(Builder builder) {
        super(builder);
    }

}

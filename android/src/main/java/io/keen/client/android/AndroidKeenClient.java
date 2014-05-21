package io.keen.client.android;

import android.content.Context;

import java.util.concurrent.Executor;

import io.keen.client.java.*;

/**
 * Implementation of a {@link io.keen.client.java.KeenClient} on the Android platform.
 * <p/>
 * This client uses the built-in Android JSON libraries for reading/writing JSON in order to
 * minimize library size.
 * <p/>
 * To cache events in between batch uploads, this client uses a file-based event store with its
 * root in the application's cache directory.
 * <p/>
 * This client uses {@link android.os.AsyncTask} to run asynchronous requests.
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
            KeenClient.initialize(new AndroidKeenClient(context));
        }
        return KeenClient.client();
    }

    ///// KeenClient METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    public Executor getPublishExecutor() {
        return publishExecutor;
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
    public KeenJsonHandler getJsonHandler() {
        return jsonHandler;
    }

    ///// PRIVATE FIELDS /////

    private KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private Executor publishExecutor;

    ///// PRIVATE CONSTRUCTORS /////

    /**
     * Constructs an Android client.
     *
     * @param context A context which can be used to retrieve the application context in which this
     *                client will run.
     */
    private AndroidKeenClient(Context context) {
        // Try to initialize the necessary components. If any of them fails for any reason,
        // mark the client as inactive.
        try {
            jsonHandler = new AndroidJsonHandler();
            eventStore = new FileEventStore(context.getCacheDir());
            publishExecutor = new AsyncTaskExecutor();
        } catch (Exception e) {
            KeenLogging.log("Exception initializing AndroidKeenClient: " + e.getMessage());
            setActive(false);
        }
    }

}

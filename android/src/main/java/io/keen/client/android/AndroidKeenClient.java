package io.keen.client.android;

import android.content.Context;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import io.keen.client.java.FileEventStore;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenEventStore;
import io.keen.client.java.KeenJsonHandler;
import io.keen.client.java.exceptions.KeenInitializationException;

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
        KeenClient.initialize(new AndroidKeenClient(context));
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
    public KeenEventStore getEventStore() throws KeenInitializationException {
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

    /**
     * The application context in which this client will run.
     */
    private final Context context;
    private final KeenJsonHandler jsonHandler;
    private final KeenEventStore eventStore;
    private final Executor publishExecutor;

    ///// PRIVATE CONSTRUCTORS /////

    /**
     * Constructs an Android client.
     *
     * @param context A context which can be used to retrieve the application context in which this
     *                client will run.
     */
    private AndroidKeenClient(Context context) {
        if (context == null) {
            throw new IllegalArgumentException("Context must not be null");
        }

        this.context = context.getApplicationContext();
        this.jsonHandler = new AndroidJsonHandler();
        try {
            this.eventStore = new FileEventStore(getDeviceCacheDirectory(), getJsonHandler());
        } catch (IOException e) {
            throw new KeenInitializationException("Error building file event store");
        }
        this.publishExecutor = new AsyncTaskExecutor();
    }

    ///// PRIVATE METHODS /////

    /**
     * Gets the device cache directory.
     *
     * @return The device cache directory.
     */
    private File getDeviceCacheDirectory() {
        return context.getCacheDir();
    }

}

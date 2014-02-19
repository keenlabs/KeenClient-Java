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
 * DOCUMENT
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
     */
    public static void initialize(Context context) {
        KeenClient.initialize(new AndroidKeenClient(context));
    }

    ///// KeenClient METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    protected Executor instantiatePublishExecutor() {
        return new AsyncTaskExecutor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected KeenEventStore instantiateEventStore() throws KeenInitializationException {
        try {
            return new FileEventStore(getDeviceCacheDirectory(), getJsonHandler());
        } catch (IOException e) {
            throw new KeenInitializationException("Error building file event store");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected KeenJsonHandler instantiateJsonHandler() {
        return new AndroidJsonHandler();
    }

    ///// PRIVATE FIELDS /////

    /** The application context in which this client will run. */
    private final Context context;

    ///// PRIVATE CONSTRUCTORS /////

    /**
     * Constructs an Android client.
     *
     * @param context A context which can be used to retrieve the application context in which this
     *                client will run.
     */
    private AndroidKeenClient(Context context) {
        this.context = context.getApplicationContext();
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

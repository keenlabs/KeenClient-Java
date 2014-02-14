package io.keen.client.android;

import android.content.Context;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import io.keen.client.java.FileEventStore;
import io.keen.client.java.KeenEventStore;
import io.keen.client.java.KeenInitializer;
import io.keen.client.java.KeenJsonHandler;
import io.keen.client.java.exceptions.KeenInitializationException;

/**
 * TODO: Documentation
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenInitializer extends KeenInitializer {

    ///// CONSTRUCTORS /////

    public AndroidKeenInitializer(Context context) {
        // TODO: Is it right to use the application context? Probably but revisit this just in case.
        this.context = context.getApplicationContext();
    }

    ///// KeenInitializer METHODS /////

    @Override
    protected Executor buildDefaultPublishExecutor() {
        return new AsyncTaskExecutor();
    }

    @Override
    protected KeenEventStore buildDefaultEventStore() throws KeenInitializationException {
        try {
            return new FileEventStore(getDeviceCacheDirectory(), jsonHandler);
        } catch (IOException e) {
            // TODO: throw KeenInitializationException?
            throw new KeenInitializationException("Error building file event store");
        }
    }

    @Override
    protected KeenJsonHandler buildDefaultJsonHandler() {
        return new AndroidJsonHandler();
    }

    ///// PRIVATE FIELDS /////

    private final Context context;

    ///// PRIVATE METHODS /////

    private File getDeviceCacheDirectory() {
        return context.getCacheDir();
    }

}

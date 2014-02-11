package io.keen.client.android;

import android.content.Context;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import io.keen.client.java.FileEventStore;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenEventStore;
import io.keen.client.java.KeenJsonHandler;

/**
 * TODO: Documentation
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenInitializer {

    private final Context context;
    private KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private Executor publishExecutor;
    private boolean isInitializeCalled;

    public AndroidKeenInitializer(Context context) {
        // TODO: Is it right to use the application context? Probably but revisit this just in case.
        this.context = context.getApplicationContext();
    }

    public synchronized void initialize(String projectId, String writeKey, String readKey) {
        if (isInitializeCalled) {
            throw new IllegalStateException("Initialize may only be called once");
        }

        if (jsonHandler == null) {
            jsonHandler = new AndroidJsonHandler();
        }

        if (eventStore == null) {
            try {
                eventStore = new FileEventStore(getDeviceCacheDirectory(), jsonHandler);
            } catch (IOException e) {
                // TODO: throw KeenInitializationException?
            }
        }

        if (publishExecutor == null) {
            publishExecutor = new AsyncTaskExecutor();
        }

        KeenClient.initialize(projectId, writeKey, readKey);
        isInitializeCalled = true;
    }

    public synchronized AndroidKeenInitializer withJsonHandler(KeenJsonHandler jsonHandler) {
        this.jsonHandler = jsonHandler;
        return this;
    }

    public synchronized AndroidKeenInitializer withEventStore(KeenEventStore eventStore) {
        this.eventStore = eventStore;
        return this;
    }

    public synchronized AndroidKeenInitializer withPublishExecutor(Executor publishExecutor) {
        this.publishExecutor = publishExecutor;
        return this;
    }

    /////////////////////////////////////////////
    // FILE IO
    /////////////////////////////////////////////

    File getDeviceCacheDirectory() {
        return context.getCacheDir();
    }

}
